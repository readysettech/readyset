use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};

use launchpad::redacted::RedactedString;
use nom_locate::LocatedSpan;
use nom_sql::{replicator_table_list, Dialect, SqlIdentifier};
use readyset::{ReadySetError, ReadySetResult};

/// A [`TableFilter`] keep a list of all the tables readyset-server is interested in as a mapping
/// from schema to a list of tables for that schema. When a replication event happens, the event
/// is filtered based on its schema/table before being sent to readyset-server.
#[derive(Debug, Clone)]
pub(crate) struct TableFilter {
    /// A mapping between schema to the list of tables to replicate from that schema.
    /// A `BTreeMap` is used here, because the assumption is that the number of schemas in a
    /// database is usually small, and their names are short, so a small `BTreeMap` with inlined
    /// `SqlIdentifiers` would perform better than a `HashMap` where a hash is performed on every
    /// lookup.
    /// If [`Option::None`], all tables in all schemas will be replicated.
    tables: Option<BTreeMap<SqlIdentifier, ReplicateTableSpec>>,
}

#[derive(Debug, Clone)]
pub(crate) enum ReplicateTableSpec {
    AllTablesInSchema,
    /// Similarly to [`TableFilter`] a `BTreeMap` is used here, because the assumption is that
    /// table names are usually short and not that numerous that a `HashMap` would be slower.
    Tables(BTreeSet<SqlIdentifier>),
}

impl ReplicateTableSpec {
    /// Check if spec is for all tables (i.e. created with *)
    pub(crate) fn is_for_all_tables(&self) -> bool {
        matches!(self, ReplicateTableSpec::AllTablesInSchema)
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
        default_schema: Option<&str>,
    ) -> ReadySetResult<TableFilter> {
        let default_schema = default_schema.map(SqlIdentifier::from);

        let mut tables = BTreeMap::new();

        let replicated = match table_list {
            None => {
                match default_schema {
                    Some(default) => {
                        // Will load all tables for the default schema
                        tables.insert(default, ReplicateTableSpec::AllTablesInSchema);
                        return Ok(TableFilter {
                            tables: Some(tables),
                        });
                    }
                    None => return Ok(TableFilter { tables: None }),
                };
            }
            Some(t) => t,
        };

        if replicated.as_str() == "*.*" {
            return Ok(Self::for_all_tables());
        }

        // TODO(alex) Propagate nom-sql error position info?
        let replicate_list =
            match replicator_table_list(dialect)(LocatedSpan::new(replicated.as_bytes())) {
                Ok((rem, tables)) if rem.is_empty() => tables,
                _ => {
                    return Err(ReadySetError::ReplicationFailed(
                        "Unable to parse replicated tables list".to_string(),
                    ))
                }
            };

        for table in replicate_list {
            let table_name = table.name;
            let table_schema =
                table
                    .schema
                    .or_else(|| default_schema.clone())
                    .ok_or_else(|| {
                        ReadySetError::ReplicationFailed(format!(
                            "No database and no default database for table {table_name}"
                        ))
                    })?;

            if table_name == "*" {
                tables.insert(table_schema, ReplicateTableSpec::AllTablesInSchema);
            } else {
                match tables
                    .entry(table_schema)
                    .or_insert_with(|| ReplicateTableSpec::Tables(BTreeSet::new()))
                {
                    // Replicating all tables for schema, nothing to do */
                    ReplicateTableSpec::AllTablesInSchema => true,
                    ReplicateTableSpec::Tables(tables) => tables.insert(table_name),
                };
            }
        }

        Ok(TableFilter {
            tables: Some(tables),
        })
    }

    /// Create a new filter that will pass all tables
    fn for_all_tables() -> Self {
        TableFilter { tables: None }
    }

    /// Iterator over all schemas in this filter
    pub(crate) fn schemas_mut(
        &mut self,
    ) -> impl Iterator<Item = (&SqlIdentifier, &mut ReplicateTableSpec)> {
        self.tables
            .as_mut()
            .map(|t| t.iter_mut())
            .into_iter()
            .flatten()
    }

    /// Iterator over all of the (schema, table) tuples in the filter
    pub(crate) fn tables(&self) -> impl Iterator<Item = (&SqlIdentifier, &SqlIdentifier)> {
        self.tables
            .as_ref()
            .map(|t| {
                t.iter()
                    .filter_map(|(db, spec)| match spec {
                        ReplicateTableSpec::AllTablesInSchema => None,
                        ReplicateTableSpec::Tables(tables) => {
                            Some(tables.iter().map(move |t| (db, t)))
                        }
                    })
                    .flatten()
            })
            .into_iter()
            .flatten()
    }

    /// Remove the given table from the list of tables
    pub(crate) fn remove(&mut self, schema: &str, table: &str) {
        if let Some(tables) = self.tables.as_mut() {
            if let Some(ReplicateTableSpec::Tables(tables)) = tables.get_mut(schema) {
                tables.remove(table);
            }
        }
    }

    /// Check if a given table should be processed
    pub(crate) fn contains<Q1, Q2>(&self, schema: &Q1, table: &Q2) -> bool
    where
        Q1: Ord + ?Sized,
        Q2: Ord + ?Sized,
        SqlIdentifier: Borrow<Q1> + Borrow<Q2>,
    {
        let tables = match self.tables.as_ref() {
            Some(tables) => tables,
            None => return true, // Empty filter processes all tables
        };

        if let Some(ns) = tables.get(schema) {
            match ns {
                ReplicateTableSpec::AllTablesInSchema => true,
                ReplicateTableSpec::Tables(tables) => tables.contains(table),
            }
        } else {
            false
        }
    }

    /// Check if the given schema is mentioned at all in the filter, or if the filter is for all
    /// schemas
    pub(crate) fn contains_schema(&self, schema: &str) -> bool {
        self.tables
            .as_ref()
            .map(|t| t.contains_key(schema))
            .unwrap_or(true)
    }

    /// Check if the filter is for all schemas and tables
    pub(crate) fn for_all_schemas(&self) -> bool {
        self.tables.is_none()
    }

    /// Insert a new schema into the filter that replicates all tables
    pub(crate) fn add_for_all_schema(&mut self, schema: SqlIdentifier) {
        self.tables
            .get_or_insert_with(BTreeMap::new)
            .insert(schema, ReplicateTableSpec::AllTablesInSchema);
    }
}

#[cfg(test)]
mod tests {
    use super::TableFilter;

    #[test]
    fn empty_list() {
        let filter = TableFilter::try_new(nom_sql::Dialect::MySQL, None, Some("noria")).unwrap();
        // By default should only allow all tables from the default schema
        assert!(filter.contains("noria", "table"));
        assert!(!filter.contains("readyset", "table"));
    }

    #[test]
    fn all_schemas_explicit() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("*.*".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        assert!(filter.contains("noria", "table"));
        assert!(filter.contains("readyset", "table"));
    }

    #[test]
    fn all_schemas_implicit() {
        let filter = TableFilter::try_new(nom_sql::Dialect::MySQL, None, None).unwrap();
        assert!(filter.contains("noria", "table"));
        assert!(filter.contains("readyset", "table"));
    }

    #[test]
    fn regular_list() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("t1,t2,t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        // Tables with no schema belong to the default schema
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
