mod connector;
mod ddl_replication;
mod snapshot;
mod wal;
mod wal_reader;

use readyset_sql::ast::{CollationName, ColumnConstraint};

pub use connector::{
    drop_publication, drop_readyset_schema, drop_replication_slot, PostgresWalConnector,
};
pub use snapshot::PostgresReplicator;

pub(crate) const REPLICATION_SLOT: &str = "readyset";
pub(crate) const PUBLICATION_NAME: &str = "readyset";

/// Build the `ColumnConstraint`s for a snapshotted or DDL-replicated Postgres column from its
/// `NOT NULL` flag and the `pg_collation.collname` we read from the upstream catalog.
pub(crate) fn pg_column_constraints(
    not_null: bool,
    collation_name: Option<&str>,
) -> Vec<ColumnConstraint> {
    let mut constraints = Vec::new();
    if not_null {
        constraints.push(ColumnConstraint::NotNull);
    }
    if let Some(name) = collation_name {
        constraints.push(ColumnConstraint::Collation(CollationName::from(name)));
    }
    constraints
}
