use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};

use nom_locate::LocatedSpan;
use nom_sql::{replicator_table_list, Dialect, Relation, SqlIdentifier};
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_util::redacted::RedactedString;

/// A [`TableFilter`] keeps lists of all the tables readyset-server is interested in, as well as a
/// list of tables that we explicitly want to filter out of replication.
/// Tables may be filtered from replication in 2 ways:
/// 1. All tables will be filtered other than the ones provided to the option --replication_tables,
///    if it is used OR All tables will be replicated other than the ones provided to the option
///   --replication-tables-ignore, if it is used.
/// 2. If we encounter a unrecoverable failure in replication for a table, we can filter out the
///    table to keep the process running without that table, which is better than being stuck until
///    we fix why that table isn't replicating.
///
/// NOTE: 2. takes precedence over 1. above. So if a table is explicitly replicated with
/// --replication_tables, but then experiences an error in replication, we will stop replicating
/// that table.
///
/// When a replication event happens, the event is filtered based on its
/// schema/table before being sent to readyset-server.
///
/// `BTreeMap`s are used here, because the assumption is that the number of schemas in a
/// database is usually small, and their names are short, so a small `BTreeMap` with inlined
/// `SqlIdentifiers` would perform better than a `HashMap` where a hash is performed on every
/// lookup.
#[derive(Debug, Clone)]
pub(crate) struct TableFilter {
    /// A mapping between schema to the list of tables to replicate from that schema.
    /// Only the tables included in the map will be replicated.
    /// This is only populated by the --rplication-tables option
    explicitly_replicated: BTreeMap<SqlIdentifier, ReplicateTableSpec>,
    /// A mapping between schema to the list of tables to *NOT* replicate from that schema.
    /// Any other valid tables will be replicated, where a valid table is either one of the tables
    /// in `explicitly_replicated`, or all tables if that is empty.
    replication_denied: BTreeMap<SqlIdentifier, ReplicateTableSpec>,
}

#[derive(Debug, Clone)]
/// Within a particular schema, [`ReplicateTablesSpec`] tells us which tables to replicate or filter
/// out
///
/// If we start replicating all tables, adding tables to AllTablesExcept allows us to ignore them
/// If we start with a specific set of tables to replicate, removing them from Tables also allows us
/// to no longer replicate them.
///
/// Similarly to [`TableFilter`] a `BTreeSet` is used here, because the assumption is that
/// table names are usually short and not that numerous that a `HashMap` would be slower.
pub(crate) enum ReplicateTableSpec {
    AllTablesExcept(BTreeSet<SqlIdentifier>),
    Tables(BTreeSet<SqlIdentifier>),
}

impl ReplicateTableSpec {
    pub(crate) fn empty() -> Self {
        Self::Tables(BTreeSet::new())
    }

    pub(crate) fn empty_all_tables() -> Self {
        Self::AllTablesExcept(BTreeSet::new())
    }

    pub(crate) fn insert<S: Into<SqlIdentifier>>(&mut self, t: S) {
        match self {
            Self::AllTablesExcept(tables) => tables.remove(&t.into()),
            Self::Tables(tables) => tables.insert(t.into()),
        };
    }

    pub(crate) fn remove<S: Into<SqlIdentifier>>(&mut self, t: S) {
        match self {
            Self::AllTablesExcept(tables) => tables.insert(t.into()),
            Self::Tables(tables) => tables.remove(&t.into()),
        };
    }

    pub(crate) fn contains<S>(&self, t: &S) -> bool
    where
        S: Ord + ?Sized,
        SqlIdentifier: Borrow<S>,
    {
        match self {
            Self::AllTablesExcept(tables) => !tables.contains(t),
            Self::Tables(tables) => tables.contains(t),
        }
    }
}

impl TableFilter {
    pub(crate) fn try_new(
        dialect: Dialect,
        filter_table_list: Option<RedactedString>,
        filter_table_list_ignore: Option<RedactedString>,
        default_schema: Option<&str>,
    ) -> ReadySetResult<TableFilter> {
        let default_schema = default_schema.map(SqlIdentifier::from);

        if filter_table_list.is_some() && filter_table_list_ignore.is_some() {
            return Err(ReadySetError::ReplicationFailed(
                "Cannot use both --replication-tables and --replication-tables-ignore".to_string(),
            ));
        }

        let mut schemas: BTreeMap<SqlIdentifier, ReplicateTableSpec> = BTreeMap::new();
        let mut schemas_ignore: BTreeMap<SqlIdentifier, ReplicateTableSpec> = BTreeMap::new();

        let mut filter_list_ignore: Vec<Relation> = Vec::new();
        match filter_table_list_ignore {
            None => readyset_util::redacted::RedactedString("".to_string()),
            Some(t) => {
                if t.as_str() == "*.*" {
                    return Err(ReadySetError::ReplicationFailed(
                        "Cannot filter out all tables".to_string(),
                    ));
                }

                filter_list_ignore =
                    match replicator_table_list(dialect)(LocatedSpan::new(t.as_bytes())) {
                        Ok((rem, tables)) if rem.is_empty() => tables,
                        _ => {
                            return Err(ReadySetError::ReplicationFailed(
                                "Unable to parse filtered ignored tables list".to_string(),
                            ))
                        }
                    };
                t
            }
        };

        for table in filter_list_ignore {
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
                schemas_ignore.insert(table_schema, ReplicateTableSpec::empty_all_tables());
            } else {
                let tables = schemas_ignore
                    .entry(table_schema)
                    .or_insert_with(ReplicateTableSpec::empty);
                tables.insert(table_name);
            }
        }
        if !schemas_ignore.is_empty() {
            return Ok(TableFilter {
                explicitly_replicated: schemas,
                replication_denied: schemas_ignore,
            });
        }
        let filtered = match filter_table_list {
            None => {
                match default_schema {
                    Some(default) => {
                        // Will load all tables for the default schema
                        schemas.insert(default, ReplicateTableSpec::empty_all_tables());
                        return Ok(TableFilter {
                            explicitly_replicated: schemas,
                            replication_denied: schemas_ignore,
                        });
                    }
                    None => {
                        // We will learn what the tables are by `update_table_list` at snapshot
                        // time since `for_all_schemas` is true and not explicit exclude table.
                        return Ok(Self::for_all_tables());
                    }
                };
            }
            Some(t) => t,
        };

        if filtered.as_str() == "*.*" {
            return Ok(Self::for_all_tables());
        }

        let filter_list =
            match replicator_table_list(dialect)(LocatedSpan::new(filtered.as_bytes())) {
                Ok((rem, tables)) if rem.is_empty() => tables,
                _ => {
                    return Err(ReadySetError::ReplicationFailed(
                        "Unable to parse filtered tables list".to_string(),
                    ))
                }
            };

        for table in filter_list {
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
                schemas.insert(table_schema, ReplicateTableSpec::empty_all_tables());
            } else {
                let tables = schemas
                    .entry(table_schema)
                    .or_insert_with(ReplicateTableSpec::empty);
                tables.insert(table_name);
            }
        }

        Ok(TableFilter {
            explicitly_replicated: schemas,
            replication_denied: schemas_ignore,
        })
    }

    /// Create a new filter that will pass all tables
    fn for_all_tables() -> Self {
        Self {
            explicitly_replicated: BTreeMap::new(),
            replication_denied: BTreeMap::new(),
        }
    }

    /// Stop replicating the provided table
    pub(crate) fn deny_replication(&mut self, schema: &str, table: &str) {
        tracing::info!(%schema, %table, "denying replication");
        if let Some(tables) = self.explicitly_replicated.get_mut(schema) {
            tables.remove(table);
        }

        let tables = self
            .replication_denied
            .entry(schema.into())
            .or_insert_with(ReplicateTableSpec::empty);
        tables.insert(table);
    }

    /// Check if a given table should be processed
    pub(crate) fn should_be_processed<Q1, Q2>(&self, schema: &Q1, table: &Q2) -> bool
    where
        Q1: Ord + ?Sized,
        Q2: Ord + ?Sized,
        SqlIdentifier: Borrow<Q1> + Borrow<Q2>,
    {
        self.explicitly_replicated.is_empty() && !self.is_denied(schema, table)
            || self.is_explicitly_replicated(schema, table)
    }

    pub(crate) fn is_explicitly_replicated<Q1, Q2>(&self, schema: &Q1, table: &Q2) -> bool
    where
        Q1: Ord + ?Sized,
        Q2: Ord + ?Sized,
        SqlIdentifier: Borrow<Q1> + Borrow<Q2>,
    {
        let res = match self.explicitly_replicated.get(schema) {
            Some(tables) => tables.contains(table),
            None => false,
        };

        if res {
            debug_assert!(
                !self.is_denied(schema, table),
                "If a table is explicitly replicated, it should not also be denied"
            );
        }
        res
    }

    pub(crate) fn is_denied<Q1, Q2>(&self, schema: &Q1, table: &Q2) -> bool
    where
        Q1: Ord + ?Sized,
        Q2: Ord + ?Sized,
        SqlIdentifier: Borrow<Q1> + Borrow<Q2>,
    {
        let res = match self.replication_denied.get(schema) {
            Some(tables) => tables.contains(table),
            None => false,
        };

        if res {
            debug_assert!(
                !self.is_explicitly_replicated(schema, table),
                "If a table is denied, it should not also be explicitly_replicated"
            );
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::TableFilter;

    #[test]
    fn empty_list() {
        let filter =
            TableFilter::try_new(nom_sql::Dialect::MySQL, None, None, Some("noria")).unwrap();
        // By default should only allow all tables from the default schema
        assert!(filter.should_be_processed("noria", "table"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn all_schemas_explicit() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("*.*".to_string().into()),
            None,
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("noria", "table"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn all_schemas_implicit() {
        let filter = TableFilter::try_new(nom_sql::Dialect::MySQL, None, None, None).unwrap();
        assert!(filter.should_be_processed("noria", "table"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn regular_list() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("t1,t2,t3".to_string().into()),
            None,
            Some("noria"),
        )
        .unwrap();
        // Tables with no schema belong to the default schema
        assert!(filter.should_be_processed("noria", "t1"));
        assert!(filter.should_be_processed("noria", "t2"));
        assert!(filter.should_be_processed("noria", "t3"));
        assert!(!filter.should_be_processed("noria", "t4"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn mixed_list() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("t1,noria.t2,readyset.t4,t3".to_string().into()),
            None,
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("noria", "t1"));
        assert!(filter.should_be_processed("noria", "t2"));
        assert!(filter.should_be_processed("noria", "t3"));
        assert!(!filter.should_be_processed("noria", "t4"));
        assert!(filter.should_be_processed("readyset", "t4"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn wildcard() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("noria.*, readyset.t4, t3".to_string().into()),
            None,
            Some("noria"),
        )
        .unwrap();
        // Namespace with a wildcard contains all tables
        assert!(filter.should_be_processed("noria", "t1"));
        assert!(filter.should_be_processed("noria", "t2"));
        assert!(filter.should_be_processed("noria", "t3"));
        assert!(filter.should_be_processed("noria", "t4"));
        assert!(filter.should_be_processed("readyset", "t4"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn allowed_then_denied() {
        let mut filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            Some("noria.*, readyset.t4, t3".to_string().into()),
            None,
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("readyset", "t4"));
        filter.deny_replication("readyset", "t4");
        assert!(!filter.should_be_processed("readyset", "t4"));
    }

    #[test]
    fn all_allowed_then_one_denied() {
        let mut filter = TableFilter::for_all_tables();

        assert!(filter.should_be_processed("readyset", "t4"));
        filter.deny_replication("readyset", "t4");
        assert!(!filter.should_be_processed("readyset", "t4"));
    }

    #[test]
    fn regular_list_ignore() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            None,
            Some("t1,t2,t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        // Tables with no schema belong to the default schema
        assert!(!filter.should_be_processed("noria", "t1"));
        assert!(!filter.should_be_processed("noria", "t2"));
        assert!(!filter.should_be_processed("noria", "t3"));
        assert!(filter.should_be_processed("noria", "t4"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn mixed_list_ignore() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            None,
            Some("t1,noria.t2,readyset.t4,t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        assert!(!filter.should_be_processed("noria", "t1"));
        assert!(!filter.should_be_processed("noria", "t2"));
        assert!(!filter.should_be_processed("noria", "t3"));
        assert!(filter.should_be_processed("noria", "t4"));
        assert!(!filter.should_be_processed("readyset", "t4"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn wildcard_ignore() {
        let filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            None,
            Some("noria.*, readyset.t4, t3".to_string().into()),
            Some("noria"),
        )
        .unwrap();
        // Namespace with a wildcard contains all tables
        assert!(!filter.should_be_processed("noria", "t1"));
        assert!(!filter.should_be_processed("noria", "t2"));
        assert!(!filter.should_be_processed("noria", "t3"));
        assert!(!filter.should_be_processed("noria", "t4"));
        assert!(!filter.should_be_processed("readyset", "t4"));
        assert!(filter.should_be_processed("readyset", "table"));
    }
}
