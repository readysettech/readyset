use std::collections::btree_map::IterMut;
use std::collections::{BTreeMap, BTreeSet};

use launchpad::redacted::RedactedString;
use nom_sql::{replicator_table_list, Dialect, SqlIdentifier};
use readyset::{ReadySetError, ReadySetResult};

/// A [`TableFilter`] keep a list of all the tables readyset-server is interested in as a mapping
/// from namespace to a list of tables for that schema. When a replication event happens, the event
/// is filtered based on its schema/table before being sent to readyset-server.
#[derive(Debug, Clone)]
pub(crate) struct TableFilter {
    /// A mapping between namespace to the list of tables to replicate from that namespace.
    /// A `BTreeMap` is used here, because the assumption is that the number of schemas in a
    /// database is usually small, and their names are short, so a small `BTreeMap` with inlined
    /// `SqlIdentifiers` would perform better than a `HashMap` where a hash is performed on every
    /// lookup.
    tables: BTreeMap<SqlIdentifier, ReplicateTableSpec>,
}

#[derive(Debug, Clone)]
pub(crate) enum ReplicateTableSpec {
    AllTablesInNamespace,
    /// Similarly to [`TableFilter`] a `BTreeMap` is used here, because the assumption is that
    /// table names are usually short and not that numerous that a `HashMap` would be slower.
    Tables(BTreeSet<SqlIdentifier>),
}

impl ReplicateTableSpec {
    /// Check if spec is for all tables (i.e. created with *)
    pub(crate) fn is_for_all_tables(&self) -> bool {
        matches!(self, ReplicateTableSpec::AllTablesInNamespace)
    }

    /// Replace the current spec with named tables
    pub(crate) fn set(&mut self, actual_tables: Vec<String>) {
        *self =
            ReplicateTableSpec::Tables(actual_tables.into_iter().map(SqlIdentifier::from).collect())
    }
}

impl TableFilter {
    pub(crate) fn try_new(
        dialect: Dialect,
        table_list: Option<RedactedString>,
        default_namespace: Option<&str>,
    ) -> ReadySetResult<TableFilter> {
        let default_namespace = default_namespace.map(SqlIdentifier::from);

        let mut tables = BTreeMap::new();

        let replicated = match table_list {
            None => {
                return match default_namespace {
                    Some(default) => {
                        // Will load all tables for the default namespace
                        tables.insert(default, ReplicateTableSpec::AllTablesInNamespace);
                        return Ok(TableFilter { tables });
                    }
                    None => Err(ReadySetError::ReplicationFailed(
                        "No tables and no default database specified for replication".to_string(),
                    )),
                };
            }
            Some(t) => t,
        };

        let replicate_list = match replicator_table_list(dialect)(replicated.as_bytes()) {
            Ok((rem, tables)) if rem.is_empty() => tables,
            _ => {
                return Err(ReadySetError::ReplicationFailed(
                    "Unable to parse replicated tables list".to_string(),
                ))
            }
        };

        for table in replicate_list {
            let table_name = table.name;
            let table_namespace = table
                .schema
                .or_else(|| default_namespace.clone())
                .ok_or_else(|| {
                    ReadySetError::ReplicationFailed(format!(
                        "No database and no default database for table {table_name}"
                    ))
                })?;

            if table_name == "*" {
                tables.insert(table_namespace, ReplicateTableSpec::AllTablesInNamespace);
            } else {
                match tables
                    .entry(table_namespace)
                    .or_insert_with(|| ReplicateTableSpec::Tables(BTreeSet::new()))
                {
                    // Replicating all tables for namespace, nothing to do */
                    ReplicateTableSpec::AllTablesInNamespace => true,
                    ReplicateTableSpec::Tables(tables) => tables.insert(table_name),
                };
            }
        }

        Ok(TableFilter { tables })
    }

    /// Iterator over all namespaces in this filter
    pub(crate) fn namespaces(&mut self) -> IterMut<'_, SqlIdentifier, ReplicateTableSpec> {
        self.tables.iter_mut()
    }

    /// Iterator over all the named tables in the filter along with their namespace
    pub(crate) fn tables(&self) -> impl Iterator<Item = (&SqlIdentifier, &SqlIdentifier)> {
        self.tables
            .iter()
            .filter_map(|(db, spec)| match spec {
                ReplicateTableSpec::AllTablesInNamespace => None,
                ReplicateTableSpec::Tables(tables) => Some(tables.iter().map(move |t| (db, t))),
            })
            .flatten()
    }

    /// Remove the given table from the list of tables
    pub(crate) fn remove(&mut self, namespace: &str, table: &str) {
        if let Some(ReplicateTableSpec::Tables(tables)) = self.tables.get_mut(namespace) {
            tables.remove(table);
        }
    }

    /// Check if a given table should be processed
    pub(crate) fn contains(&self, namespace: &str, table: &str) -> bool {
        if let Some(ns) = self.tables.get(namespace) {
            match ns {
                ReplicateTableSpec::AllTablesInNamespace => true,
                ReplicateTableSpec::Tables(tables) => tables.contains(table),
            }
        } else {
            false
        }
    }

    /// Check if the given namespace is mentioned at all in the filter
    pub(crate) fn contains_namespace(&self, namespace: &str) -> bool {
        self.tables.contains_key(namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::TableFilter;

    #[test]
    fn empty_list() {
        let filter = TableFilter::try_new(nom_sql::Dialect::MySQL, None, Some("noria")).unwrap();
        // By default should only allow all tables from the default namespace
        assert!(filter.contains("noria", "table"));
        assert!(!filter.contains("readyset", "table"));
    }

    #[test]
    fn regular_list() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("t1,t2,t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        // Tables with no namespace belong to the default namespace
        assert!(filter.contains("noria", "t1"));
        assert!(filter.contains("noria", "t2"));
        assert!(filter.contains("noria", "t3"));
        assert!(!filter.contains("noria", "t4"));
        assert!(!filter.contains("readyset", "table"));
    }

    #[test]
    fn mixed_list() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("t1,noria.t2,readyset.t4,t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        assert!(filter.contains("noria", "t1"));
        assert!(filter.contains("noria", "t2"));
        assert!(filter.contains("noria", "t3"));
        assert!(!filter.contains("noria", "t4"));
        assert!(filter.contains("readyset", "t4"));
        assert!(!filter.contains("readyset", "table"));
    }

    #[test]
    fn wildcard() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("noria.*, readyset.t4, t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        // Namespace with a wildcard contains all tables
        assert!(filter.contains("noria", "t1"));
        assert!(filter.contains("noria", "t2"));
        assert!(filter.contains("noria", "t3"));
        assert!(filter.contains("noria", "t4"));
        assert!(filter.contains("readyset", "t4"));
        assert!(!filter.contains("readyset", "table"));
    }
}
