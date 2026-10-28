mod connector;
mod ddl_replication;
mod snapshot;
mod wal;
mod wal_reader;

use pgsql::SimpleQueryMessage;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::ast::{CollationName, ColumnConstraint};
use tokio_postgres as pgsql;

pub use connector::{
    drop_publication, drop_readyset_schema, drop_replication_slot, PostgresWalConnector,
};
pub use snapshot::PostgresReplicator;

pub(crate) const REPLICATION_SLOT: &str = "readyset";
pub(crate) const PUBLICATION_NAME: &str = "readyset";

/// Read the locale provider, `datcollate`, and ICU or builtin locale of the current database.
/// The columns of `pg_database` differ between Postgres versions, so read them through JSON.
const DATABASE_COLLATION_QUERY: &str = "
    SELECT coalesce(d->>'datlocprovider', 'c'),
           d->>'datcollate',
           coalesce(d->>'datlocale', d->>'daticulocale')
    FROM (SELECT to_jsonb(db) AS d FROM pg_catalog.pg_database db
          WHERE db.datname = current_database()) s";

/// Return the name of the collation that columns with the `default` collation use.
pub(crate) async fn database_collation(client: &pgsql::Client) -> ReadySetResult<String> {
    let messages = client.simple_query(DATABASE_COLLATION_QUERY).await?;
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .ok_or_else(|| {
            ReadySetError::ReplicationFailed("pg_database has no current database".into())
        })?;
    let collate = row
        .get(1)
        .ok_or_else(|| ReadySetError::ReplicationFailed("pg_database has no datcollate".into()))?;
    Ok(database_collation_name(
        row.get(0).unwrap_or("c"),
        collate,
        row.get(2),
    ))
}

/// Return the collation name for a database's locale provider, `datcollate`, and ICU or builtin
/// locale.
fn database_collation_name(provider: &str, collate: &str, locale: Option<&str>) -> String {
    match provider {
        "i" => format!("{}-x-icu", locale.unwrap_or("und")),
        // Builtin collations sort by code point, which is the byte order of UTF-8.
        "b" => "C".to_owned(),
        _ => collate.to_owned(),
    }
}

/// Build the `ColumnConstraint`s for a snapshotted or DDL-replicated Postgres column from its
/// `NOT NULL` flag and the `pg_collation.collname` we read from the upstream catalog. A column
/// with the `default` collation gets `database_collation`.
pub(crate) fn pg_column_constraints(
    not_null: bool,
    collation_name: Option<&str>,
    database_collation: &str,
) -> Vec<ColumnConstraint> {
    let mut constraints = Vec::new();
    if not_null {
        constraints.push(ColumnConstraint::NotNull);
    }
    if let Some(name) = collation_name {
        let name = if name == "default" {
            database_collation
        } else {
            name
        };
        constraints.push(ColumnConstraint::Collation(CollationName {
            name: name.into(),
            quote_style: Some('"'),
        }));
    }
    constraints
}

#[cfg(test)]
mod tests {
    use readyset_sql::ast::SqlQuery;
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::parse_query;

    use super::*;

    fn quoted(name: &str) -> ColumnConstraint {
        ColumnConstraint::Collation(CollationName {
            name: name.into(),
            quote_style: Some('"'),
        })
    }

    #[test]
    fn database_collation_names() {
        assert_eq!(
            database_collation_name("c", "en_US.utf8", None),
            "en_US.utf8"
        );
        assert_eq!(database_collation_name("c", "C", None), "C");
        assert_eq!(
            database_collation_name("i", "C", Some("en-US")),
            "en-US-x-icu"
        );
        assert_eq!(database_collation_name("i", "C", None), "und-x-icu");
        assert_eq!(database_collation_name("b", "C", Some("C.UTF-8")), "C");
    }

    #[test]
    fn default_collation_is_database_collation() {
        assert_eq!(
            pg_column_constraints(false, Some("default"), "en_US.utf8"),
            vec![quoted("en_US.utf8")]
        );
        assert_eq!(
            pg_column_constraints(false, Some("C"), "en_US.utf8"),
            vec![quoted("C")]
        );
    }

    #[test]
    fn collation_roundtrip() {
        let constraints = pg_column_constraints(true, Some("en_US.utf8"), "C");
        let sql = format!(
            "CREATE TABLE t (s text {})",
            constraints
                .iter()
                .map(|c| c.display(Dialect::PostgreSQL).to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
        let SqlQuery::CreateTable(stmt) = parse_query(Dialect::PostgreSQL, &sql).unwrap() else {
            panic!("expected CREATE TABLE from {sql}");
        };
        assert_eq!(stmt.body.unwrap().fields[0].constraints, constraints);
    }
}
