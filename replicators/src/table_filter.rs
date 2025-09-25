use std::collections::{HashMap, HashSet};

use nom_locate::LocatedSpan;
use nom_sql::replicator_table_list;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_sql::Dialect;

/// A [`TableFilter`] keeps lists of all the tables readyset-server is interested in, as well as a
/// list of tables that we explicitly want to filter out of replication. Tables may be filtered
/// from replication in 2 ways:
///
/// 1. All tables will be filtered other than the ones provided to the option --replication-tables,
///    if it is used, or all tables will be replicated other than the ones provided to the option
///    --replication-tables-ignore, if it is used.
///
/// 2. If we encounter a unrecoverable failure in replication for a table, we can filter out the
///    table to keep the process running without that table, which is better than being stuck until
///    we fix why that table isn't replicating.
///
/// NOTE: 2 takes precedence over 1 above. So if a table is explicitly replicated with
/// --replication-tables, but then experiences an error in replication, we will stop replicating
/// that table.
///
/// When a replication event happens, the event is filtered based on its schema/table before being
/// sent to readyset-server.
#[derive(Debug, Clone)]
pub struct TableFilter {
    /// A mapping of schema -> replication strategy for tables in that schema.
    strategy_by_schema: HashMap<SqlIdentifier, ReplicationStrategy>,
    /// Whether or not to allow schemas not already in the map.
    allow_unregistered_schemas: bool,
}

#[derive(Debug, Clone)]
enum ReplicationStrategy {
    Allowlist(HashSet<SqlIdentifier>),
    Denylist(HashSet<SqlIdentifier>),
}

impl ReplicationStrategy {
    fn new(is_allowlist: bool) -> Self {
        match is_allowlist {
            true => ReplicationStrategy::Allowlist(HashSet::new()),
            false => ReplicationStrategy::Denylist(HashSet::new()),
        }
    }

    fn allow_table(&mut self, table: &SqlIdentifier) {
        match self {
            Self::Allowlist(s) => {
                s.insert(table.clone());
            }
            Self::Denylist(s) => {
                s.remove(table);
            }
        };
    }

    fn deny_table(&mut self, table: &SqlIdentifier) {
        match self {
            Self::Allowlist(s) => {
                s.remove(table);
            }
            Self::Denylist(s) => {
                s.insert(table.clone());
            }
        };
    }

    fn allows(&self, table: &str) -> bool {
        match self {
            Self::Allowlist(s) => s.contains(table),
            Self::Denylist(s) => !s.contains(table),
        }
    }
}

impl TableFilter {
    fn allow_all() -> Self {
        Self {
            strategy_by_schema: HashMap::new(),
            allow_unregistered_schemas: true,
        }
    }

    fn allow_one_schema(schema: &str, allow_unregistered_schemas: bool) -> Self {
        Self {
            strategy_by_schema: HashMap::from([(
                SqlIdentifier::from(schema),
                ReplicationStrategy::new(false),
            )]),
            allow_unregistered_schemas,
        }
    }

    fn allow_tables(
        default_schema: Option<&str>,
        tables: Vec<Relation>,
        allow_unregistered_schemas: bool,
        is_allowlist: bool,
    ) -> ReadySetResult<Self> {
        let mut strategy_by_schema = HashMap::new();
        for table in tables {
            let schema = table
                .schema
                .or_else(|| default_schema.map(SqlIdentifier::from))
                .ok_or_else(|| {
                    ReadySetError::ReplicationFailed(format!(
                        "No database and no default database for table {}",
                        table.name
                    ))
                })?;

            if table.name == "*" {
                // An empty allowlist blocks all tables; an empty denylist allows all tables.
                strategy_by_schema.insert(schema, ReplicationStrategy::new(!is_allowlist));
            } else {
                let strategy = strategy_by_schema
                    .entry(schema)
                    .or_insert(ReplicationStrategy::new(is_allowlist));
                if is_allowlist {
                    strategy.allow_table(&table.name);
                } else {
                    strategy.deny_table(&table.name);
                }
            }
        }

        if let Some(default_schema) = default_schema.map(SqlIdentifier::from) {
            // If none of the tables above are part of the default schema, also allow all tables in
            // the default schema by creating an empty denylist for it.
            strategy_by_schema
                .entry(default_schema)
                .or_insert(ReplicationStrategy::new(false));
        }

        Ok(Self {
            strategy_by_schema,
            allow_unregistered_schemas,
        })
    }

    fn deny_all() -> Self {
        Self {
            strategy_by_schema: HashMap::new(),
            allow_unregistered_schemas: false,
        }
    }

    pub fn try_new(
        dialect: Dialect,
        replication_tables: Option<&str>,
        replication_tables_ignore: Option<&str>,
        default_schema: Option<&str>,
    ) -> ReadySetResult<TableFilter> {
        if replication_tables.is_some() && replication_tables_ignore.is_some() {
            // XXX JCD we would need to decide the semantics for what it means to specify both.
            return Err(ReadySetError::ReplicationFailed(
                "Cannot use both --replication-tables and --replication-tables-ignore".to_string(),
            ));
        }
        if let Some("*.*") = replication_tables {
            return Ok(Self::allow_all());
        }
        if let Some("*.*") = replication_tables_ignore {
            return Ok(Self::deny_all());
        }

        let allow_unregistered_schemas = replication_tables.is_none() && default_schema.is_none();

        if let Some(arg) = replication_tables_ignore {
            let tables = match replicator_table_list(dialect)(LocatedSpan::new(arg.as_bytes())) {
                Ok((rem, tables)) if rem.is_empty() => tables,
                _ => {
                    return Err(ReadySetError::ReplicationFailed(
                        "Unable to parse --replication-tables-ignore".to_string(),
                    ))
                }
            };
            return Self::allow_tables(default_schema, tables, allow_unregistered_schemas, false);
        }

        if let Some(arg) = replication_tables {
            let tables = match replicator_table_list(dialect)(LocatedSpan::new(arg.as_bytes())) {
                Ok((rem, tables)) if rem.is_empty() => tables,
                _ => {
                    return Err(ReadySetError::ReplicationFailed(
                        "Unable to parse --replication-tables".to_string(),
                    ))
                }
            };
            return Self::allow_tables(default_schema, tables, allow_unregistered_schemas, true);
        }

        match default_schema {
            Some(schema) => Ok(Self::allow_one_schema(schema, allow_unregistered_schemas)),
            None => Ok(Self::allow_all()),
        }
    }

    /// Stop replicating the provided table.
    pub(crate) fn deny_replication(&mut self, schema: &str, table: &str) {
        tracing::info!(%schema, %table, "denying replication");
        if self.allow_unregistered_schemas {
            let strategy = self
                .strategy_by_schema
                .entry(SqlIdentifier::from(schema))
                .or_insert(ReplicationStrategy::new(false));
            strategy.deny_table(&SqlIdentifier::from(table));
        } else if let Some(strategy) = self.strategy_by_schema.get_mut(schema) {
            strategy.deny_table(&SqlIdentifier::from(table));
        }
        // Do nothing if allow_unregistered_schemas is false and we don't know about this schema.
    }

    /// Start replicating the provided table.
    pub(crate) fn allow_replication(&mut self, schema: &str, table: &str) {
        tracing::info!(%schema, %table, "allowing replication");
        let strategy = self
            .strategy_by_schema
            .entry(SqlIdentifier::from(schema))
            .or_insert(ReplicationStrategy::new(true));
        strategy.allow_table(&SqlIdentifier::from(table));
    }

    /// Check if a given table should be processed.
    pub(crate) fn should_be_processed(&self, schema: &str, table: &str) -> bool {
        if let Some(strategy) = self.strategy_by_schema.get(schema) {
            return strategy.allows(table);
        }
        self.allow_unregistered_schemas
    }
}

#[cfg(test)]
mod tests {
    use super::TableFilter;

    #[test]
    fn empty_list() {
        let filter =
            TableFilter::try_new(readyset_sql::Dialect::MySQL, None, None, Some("noria")).unwrap();
        // By default should only allow all tables from the default schema
        assert!(filter.should_be_processed("noria", "table"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn all_schemas_explicit() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            Some("*.*"),
            None,
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("noria", "table"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn all_schemas_implicit() {
        let filter = TableFilter::try_new(readyset_sql::Dialect::MySQL, None, None, None).unwrap();
        assert!(filter.should_be_processed("noria", "table"));
        assert!(filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn regular_list() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            Some("t1,t2,t3"),
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
            readyset_sql::Dialect::MySQL,
            Some("t1,noria.t2,readyset.t4,t3"),
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
            readyset_sql::Dialect::MySQL,
            Some("noria.*, readyset.t4, t3"),
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
            readyset_sql::Dialect::MySQL,
            Some("noria.*, readyset.t4, t3"),
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
        let mut filter = TableFilter::allow_all();

        assert!(filter.should_be_processed("readyset", "t4"));
        filter.deny_replication("readyset", "t4");
        assert!(!filter.should_be_processed("readyset", "t4"));
    }

    #[test]
    fn regular_list_ignore() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            None,
            Some("t1,t2,t3"),
            Some("noria"),
        )
        .unwrap();
        // Tables with no schema belong to the default schema
        assert!(!filter.should_be_processed("noria", "t1"));
        assert!(!filter.should_be_processed("noria", "t2"));
        assert!(!filter.should_be_processed("noria", "t3"));
        assert!(filter.should_be_processed("noria", "t4"));
        assert!(!filter.should_be_processed("readyset", "table"));
    }

    #[test]
    fn mixed_list_ignore() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            None,
            Some("t1,noria.t2,readyset.t4,t3"),
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
            readyset_sql::Dialect::MySQL,
            None,
            Some("noria.*, readyset.t4, t3"),
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

    #[test]
    fn wildcard_deny_list() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            None,
            Some("*.*"),
            Some("noria"),
        )
        .unwrap();
        assert!(!filter.should_be_processed("noria", "t1"));
    }

    #[test]
    fn default_schema_not_affected_by_irrelevant_ignore() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            None,
            Some("other_db.*"),
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("noria", "t1"));
        assert!(!filter.should_be_processed("other_db", "t2"));
        assert!(!filter.should_be_processed("other_other_db", "t3"));
    }

    #[test]
    fn default_schema_not_affected_by_irrelevant_include() {
        let filter = TableFilter::try_new(
            readyset_sql::Dialect::MySQL,
            Some("other_db.*"),
            None,
            Some("noria"),
        )
        .unwrap();
        assert!(filter.should_be_processed("noria", "t1"));
        assert!(filter.should_be_processed("other_db", "t2"));
        assert!(!filter.should_be_processed("other_other_db", "t3"));
    }
}
