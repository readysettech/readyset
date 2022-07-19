use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use std::future;

use futures::stream::FuturesUnordered;
use futures::{pin_mut, StreamExt, TryFutureExt};
use launchpad::redacted::Sensitive;
use nom_sql::{parse_key_specification_string, Dialect, TableKey};
use noria::{ReadySetError, ReadySetResult};
use noria_data::DataType;
use postgres_types::{accepts, FromSql, Type};
use tokio_postgres as pgsql;
use tracing::{debug, error, info, info_span, trace, Instrument};

use super::PostgresPosition;
use crate::table_filter::TableFilter;

const BATCH_SIZE: usize = 1024; // How many queries to buffer before pushing to Noria

pub struct PostgresReplicator<'a> {
    /// This is the underlying (regular) PostgreSQL transaction
    pub(crate) transaction: pgsql::Transaction<'a>,
    pub(crate) noria: &'a mut noria::ControllerHandle,
    /// Filters out tables we are not interested in
    pub(crate) table_filter: TableFilter,
}

#[derive(Debug)]
enum TableKind {
    RegularTable,
    View,
}

#[derive(Debug)]
enum ConstraintKind {
    PrimaryKey,
    UniqueKey,
    ForeignKey,
    Other(u8),
}

#[derive(Debug)]
struct TableEntry {
    schema: String,
    name: String,
    oid: u32,
}

#[derive(Debug)]
struct TableDescription {
    #[allow(dead_code)]
    schema: String,
    name: String,
    columns: Vec<ColumnEntry>,
    constraints: Vec<ConstraintEntry>,
}

#[derive(Debug)]
struct ColumnEntry {
    name: String,
    definition: String,
    not_null: bool,
    type_oid: Type,
}

#[derive(Debug)]
struct ConstraintEntry {
    #[allow(dead_code)]
    name: String,
    definition: TableKey,
    #[allow(dead_code)]
    kind: Option<ConstraintKind>,
}

/// Newtype struct to allow converting TableKey from a SQL column in a way that lets us wrap the
/// error in a pgsql::Error
struct ConstraintDefinition(TableKey);

impl<'a> FromSql<'a> for ConstraintDefinition {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let s = String::from_sql(ty, raw)?;
        Ok(ConstraintDefinition(parse_key_specification_string(
            Dialect::PostgreSQL,
            s,
        )?))
    }

    accepts!(TEXT);
}

impl TryFrom<pgsql::Row> for ColumnEntry {
    type Error = ReadySetError;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        Ok(ColumnEntry {
            name: row.try_get(0)?,
            definition: row.try_get(1)?,
            not_null: row.try_get(2)?,
            type_oid: Type::from_oid(row.try_get(3)?).ok_or_else(|| {
                // Type::from_oid returns None when the oid does not refer to a Postgres type
                #[allow(clippy::unwrap_used)] // this just worked
                let definition: String = row.try_get(1).unwrap();
                ReadySetError::Unsupported(format!(
                    "ReadySet does not yet support custom types: {definition}"
                ))
            })?,
        })
    }
}

impl Display for ColumnEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` {}{}",
            self.name,
            self.definition,
            if self.not_null { " NOT NULL" } else { "" },
        )
    }
}

impl TryFrom<pgsql::Row> for ConstraintEntry {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        let ConstraintDefinition(definition) = row.try_get(1)?;

        Ok(ConstraintEntry {
            name: row.try_get(0)?,
            definition,
            kind: row.try_get::<_, Option<i8>>(2)?.map(|c| match c as u8 {
                b'f' => ConstraintKind::ForeignKey,
                b'p' => ConstraintKind::PrimaryKey,
                b'u' => ConstraintKind::UniqueKey,
                b => ConstraintKind::Other(b),
            }),
        })
    }
}

impl Display for ConstraintEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.definition)
    }
}

impl TryFrom<pgsql::Row> for TableEntry {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        Ok(TableEntry {
            schema: row.try_get(0)?,
            oid: row.try_get(1)?,
            name: row.try_get(2)?,
        })
    }
}

impl TableEntry {
    async fn get_columns<'a>(
        oid: u32,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<Vec<ColumnEntry>, ReadySetError> {
        let query = r"
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atttypid
            FROM pg_catalog.pg_attribute a WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
            ";

        let columns = transaction.query(query, &[&oid]).await?;
        columns.into_iter().map(TryInto::try_into).collect()
    }

    async fn get_constraints<'a>(
        oid: u32,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<Vec<ConstraintEntry>, pgsql::Error> {
        let query = r"
            SELECT c2.relname, pg_catalog.pg_get_constraintdef(con.oid, true), contype
            FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
            LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('f','p','u'))
            WHERE c.oid = $1
            AND c.oid = i.indrelid
            AND i.indexrelid = c2.oid
            AND pg_catalog.pg_get_constraintdef(con.oid, true) IS NOT NULL
            ORDER BY i.indisprimary DESC, c2.relname;
            ";

        let constraints = transaction.query(query, &[&oid]).await?;
        constraints.into_iter().map(TryInto::try_into).collect()
    }

    async fn get_table<'a>(
        self,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<TableDescription, ReadySetError> {
        let columns = Self::get_columns(self.oid, transaction).await?;
        let constraints = Self::get_constraints(self.oid, transaction).await?;

        Ok(TableDescription {
            schema: self.schema,
            name: self.name,
            columns,
            constraints,
        })
    }

    async fn get_create_view<'a>(
        self,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<String, pgsql::Error> {
        let query = "SELECT definition FROM pg_catalog.pg_views WHERE viewname=$1";

        let mut create_view = format!("CREATE VIEW \"{}\" AS", self.name);

        for col in transaction.query(query, &[&self.name]).await? {
            create_view.push_str(col.try_get(0)?);
        }

        Ok(create_view)
    }
}

impl Display for TableDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "CREATE TABLE `{}` (", self.name)?;
        write!(
            f,
            "{}",
            itertools::join(
                self.columns
                    .iter()
                    .map(ToString::to_string)
                    .chain(self.constraints.iter().map(ToString::to_string)),
                ",\n"
            )
        )?;
        write!(f, ");")
    }
}

impl TableDescription {
    /// Copy a table's contents from PostgreSQL to Noria
    async fn dump<'a>(
        &self,
        transaction: &'a pgsql::Transaction<'a>,
        mut noria_table: noria::Table,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;

        let nrows = transaction
            .query_one(
                format!(
                    "SELECT count(*) AS nrows FROM \"{}\".\"{}\"",
                    self.schema, self.name
                )
                .as_str(),
                &[],
            )
            .await?
            .try_get::<_, i64>("nrows")?;

        // The most efficient way to copy an entire table is COPY BINARY
        let query = format!(
            "COPY \"{}\".\"{}\" TO stdout BINARY",
            self.schema, self.name
        );
        let rows = transaction.copy_out(query.as_str()).await?;

        let type_map: Vec<_> = self.columns.iter().map(|c| c.type_oid.clone()).collect();
        let binary_rows = pgsql::binary_copy::BinaryCopyOutStream::new(rows, &type_map);

        pin_mut!(binary_rows);

        let mut noria_rows = Vec::with_capacity(BATCH_SIZE);

        info!(rows = %nrows, "Replication started");
        while let Some(Ok(row)) = binary_rows.next().await {
            let noria_row = (0..type_map.len())
                .map(|i| row.try_get::<DataType>(i))
                .collect::<Result<Vec<_>, _>>()?;

            noria_rows.push(noria_row);
            cnt += 1;

            // Accumulate as many inserts as possible before calling into noria, as
            // those calls can be quite expensive
            if noria_rows.len() >= BATCH_SIZE {
                noria_table
                    .insert_many(std::mem::replace(
                        &mut noria_rows,
                        Vec::with_capacity(BATCH_SIZE),
                    ))
                    .await?;
            }

            if cnt % 1_000_000 == 0 {
                let progress = format!("{:.2}%", (cnt as f64 / nrows as f64) * 100.);
                info!(rows_replicated = %cnt, %progress, "Replication progress");
            }
        }

        if !noria_rows.is_empty() {
            noria_table.insert_many(noria_rows).await?;
        }

        info!(rows_replicated = %cnt, "Replication finished");

        Ok(())
    }
}

impl<'a> PostgresReplicator<'a> {
    pub(crate) async fn new(
        client: &'a mut pgsql::Client,
        noria: &'a mut noria::ControllerHandle,
        table_filter: TableFilter,
    ) -> ReadySetResult<PostgresReplicator<'a>> {
        let transaction = client
            .build_transaction()
            .deferrable(true)
            .isolation_level(pgsql::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;

        Ok(PostgresReplicator {
            transaction,
            noria,
            table_filter,
        })
    }

    /// Begin the replication process, starting with the recipe for the database, followed
    /// by each table's contents
    pub async fn snapshot_to_noria<S: AsRef<str>>(
        &mut self,
        snapshot_name: S,
    ) -> ReadySetResult<()> {
        self.set_snapshot(snapshot_name.as_ref()).await?;

        let mut table_list = self.get_table_list(TableKind::RegularTable).await?;
        let view_list = self.get_table_list(TableKind::View).await?;

        table_list.retain(|tbl| self.table_filter.contains(&tbl.schema, &tbl.name));

        trace!(?table_list, "Loaded table list");
        trace!(?view_list, "Loaded view list");

        // For each table, retreive its structure
        let mut tables = Vec::with_capacity(table_list.len());
        for table in table_list {
            trace!(table = %table.name, "Looking up table");
            let create_table = match table.get_table(&self.transaction).await {
                Ok(ct) => ct,
                Err(error) => {
                    error!(%error, "Error looking up table, table will not be used");
                    continue;
                }
            };

            debug!(%create_table, "Extending recipe");
            match future::ready(create_table.to_string().try_into())
                .and_then(|changelist| async {
                    self.noria.extend_recipe_no_leader_ready(changelist).await
                })
                .await
            {
                Ok(_) => tables.push(create_table),
                Err(error) => {
                    error!(%error, "Error extending CREATE TABLE, table will not be used")
                }
            }
        }

        for view in view_list {
            let create_view = view.get_create_view(&self.transaction).await?;
            // Postgres returns a postgres style CREATE statement, but Noria only accepts MySQL
            // style
            let view = match nom_sql::parse_query(Dialect::PostgreSQL, &create_view) {
                Ok(v) => v.to_string(),
                Err(err) => {
                    error!(%err, "Error parsing CREATE VIEW, view will not be used");
                    continue;
                }
            };

            if let Err(err) = self
                .noria
                .extend_recipe_no_leader_ready(view.try_into()?)
                .await
            {
                error!(view = %Sensitive(&create_view), %err, "Error extending CREATE VIEW, view will not be used");
            }
        }

        self.noria
            .set_schema_replication_offset(Some(&PostgresPosition::default().into()))
            .await?;

        // Finally copy each table into noria
        for table in &tables {
            // TODO: parallelize with a connection pool if performance here matters
            // TODO(vlad): should differentiate by schema when implemented
            let span = info_span!("Replicating table", schema = %table.schema, table = %table.name);
            span.in_scope(|| info!("Replicating table"));
            let mut noria_table = self
                .noria
                .table(&table.name)
                .instrument(span.clone())
                .await?;

            noria_table.set_snapshot_mode(true).await?;

            table
                .dump(&self.transaction, noria_table)
                .instrument(span.clone())
                .await?;
        }

        let mut compacting = FuturesUnordered::new();
        for table in tables {
            // TODO(vlad): should differentiate by schema when implemented
            let mut noria_table = self.noria.table(&table.name).await?;
            compacting.push(async move {
                let span = info_span!("Compacting table", table = %table.name);
                span.in_scope(|| info!("Setting replication offset"));
                if let Err(error) = noria_table
                    .set_replication_offset(PostgresPosition::default().into())
                    .instrument(span.clone())
                    .await
                {
                    span.in_scope(|| error!(%error, "Error setting replication offset"));
                    return Err(error);
                };
                span.in_scope(|| info!("Set replication offset"));

                span.in_scope(|| info!("Compacting table"));
                noria_table.set_snapshot_mode(false).await?;
                span.in_scope(|| info!("Compacting finished"));
                ReadySetResult::Ok(())
            })
        }
        while let Some(f) = compacting.next().await {
            f?;
        }

        Ok(())
    }

    /// Retreieve a list of tables of the specified kind
    async fn get_table_list(&mut self, kind: TableKind) -> Result<Vec<TableEntry>, pgsql::Error> {
        let kind_code = match kind {
            TableKind::RegularTable => 'r',
            TableKind::View => 'v',
        } as i8;

        let query = r"
        SELECT n.nspname, c.oid, c.relname, c.relkind
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
        WHERE c.relkind IN ($1) AND n.nspname <> 'pg_catalog'
                                AND n.nspname <> 'information_schema'
                                AND n.nspname !~ '^pg_toast'
        ";

        let tables = self.transaction.query(query, &[&kind_code]).await?;
        tables.into_iter().map(TryInto::try_into).collect()
    }

    /// Assign the specific snapshot to the underlying transaction
    async fn set_snapshot(&mut self, name: &str) -> Result<(), pgsql::Error> {
        let query = format!("SET TRANSACTION SNAPSHOT '{}'", name);
        self.transaction.query(query.as_str(), &[]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Column, Dialect, SqlQuery, TableKey};

    use super::*;

    #[test]
    fn table_description_with_reserved_keywords_to_string_parses() {
        let desc = TableDescription {
            schema: "public".into(),
            name: "ar_internal_metadata".into(),
            columns: vec![
                ColumnEntry {
                    name: "key".into(),
                    definition: "varchar".into(),
                    not_null: true,
                    type_oid: Type::VARCHAR,
                },
                ColumnEntry {
                    name: "value".into(),
                    definition: "varchar".into(),
                    not_null: false,
                    type_oid: Type::VARCHAR,
                },
                ColumnEntry {
                    name: "created_at".into(),
                    definition: "timestamp(6) without time zone".into(),
                    not_null: true,
                    type_oid: Type::TIMESTAMP,
                },
                ColumnEntry {
                    name: "updated_at".into(),
                    definition: "timestamp(6) without time zone".into(),
                    not_null: true,
                    type_oid: Type::TIMESTAMP,
                },
            ],
            constraints: vec![ConstraintEntry {
                name: "ar_internal_metadata_pkey".into(),
                definition: TableKey::PrimaryKey {
                    name: None,
                    columns: vec![Column {
                        name: "key".into(),
                        table: None,
                    }],
                },
                kind: Some(ConstraintKind::PrimaryKey),
            }],
        };
        let res = parse_query(Dialect::MySQL, desc.to_string());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let create_table = match res.unwrap() {
            SqlQuery::CreateTable(ct) => ct,
            _ => panic!(),
        };

        assert_eq!(create_table.table.name, "ar_internal_metadata");
        assert_eq!(create_table.fields.len(), 4);
        assert!(create_table.keys.is_some());
        assert_eq!(create_table.keys.as_ref().unwrap().len(), 1);
        let pkey = create_table.keys.as_ref().unwrap().first().unwrap();
        match pkey {
            TableKey::PrimaryKey { name, columns } => {
                assert!(name.is_none());
                assert_eq!(columns.len(), 1);
                assert_eq!(columns.first().unwrap().name, "key");
            }
            _ => panic!(),
        }
    }
}
