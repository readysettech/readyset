use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display};

use futures::stream::FuturesUnordered;
use futures::{pin_mut, StreamExt};
use noria::{ReadySetError, ReadySetResult};
use postgres_types::Type;
use tokio_postgres as pgsql;
use tracing::{debug, error, info, info_span, trace, Instrument};

use super::PostgresPosition;

const BATCH_SIZE: usize = 1024; // How many queries to buffer before pushing to Noria

pub struct PostgresReplicator<'a> {
    /// This is the underlying (regular) PostgreSQL transaction
    pub(crate) transaction: pgsql::Transaction<'a>,
    pub(crate) noria: &'a mut noria::ControllerHandle,
    /// If Some then only snapshot those tables, otherwise will snapshot all tables
    pub(crate) tables: Option<Vec<String>>,
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
    definition: String,
    #[allow(dead_code)]
    kind: ConstraintKind,
}

impl TryFrom<pgsql::Row> for ColumnEntry {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        Ok(ColumnEntry {
            name: row.try_get(0)?,
            definition: row.try_get(1)?,
            not_null: row.try_get(2)?,
            type_oid: Type::from_oid(row.try_get(3)?).unwrap(),
        })
    }
}

impl Display for ColumnEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}",
            self.name,
            self.definition,
            if self.not_null { " NOT NULL" } else { "" },
        )
    }
}

impl TryFrom<pgsql::Row> for ConstraintEntry {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        Ok(ConstraintEntry {
            name: row.try_get(0)?,
            definition: row.try_get(1)?,
            kind: match row.try_get::<_, i8>(2)? as u8 {
                b'f' => ConstraintKind::ForeignKey,
                b'p' => ConstraintKind::PrimaryKey,
                b'u' => ConstraintKind::UniqueKey,
                b => ConstraintKind::Other(b),
            },
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
    ) -> Result<Vec<ColumnEntry>, pgsql::Error> {
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
            WHERE c.oid = $1 AND c.oid = i.indrelid AND i.indexrelid = c2.oid
            ORDER BY i.indisprimary DESC, c2.relname;
            ";

        let constraints = transaction.query(query, &[&oid]).await?;
        constraints.into_iter().map(TryInto::try_into).collect()
    }

    async fn get_table<'a>(
        self,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<TableDescription, pgsql::Error> {
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
                format!("SELECT count(*) AS nrows FROM \"{}\"", self.name).as_str(),
                &[],
            )
            .await?
            .try_get::<_, i64>("nrows")?;

        // The most efficient way to copy an entire table is COPY BINARY
        let query = format!("COPY \"{}\" TO stdout BINARY", self.name);
        let rows = transaction.copy_out(query.as_str()).await?;

        let type_map: Vec<_> = self.columns.iter().map(|c| c.type_oid.clone()).collect();
        let binary_rows = pgsql::binary_copy::BinaryCopyOutStream::new(rows, &type_map);

        pin_mut!(binary_rows);

        let mut noria_rows = Vec::with_capacity(BATCH_SIZE);

        info!(rows = %nrows, "Replication started");
        while let Some(Ok(row)) = binary_rows.next().await {
            let noria_row = type_map
                .iter()
                .enumerate()
                .map(|(i, t)| match *t {
                    Type::BPCHAR | Type::TEXT | Type::VARCHAR => {
                        Ok(noria_data::DataType::from(row.try_get::<&str>(i)?))
                    }
                    Type::CHAR => Ok(row.try_get::<i8>(i)?.into()),
                    Type::INT2 => Ok((row.try_get::<i16>(i)? as i64).into()),
                    Type::INT4 => Ok((row.try_get::<i32>(i)? as i64).into()),
                    Type::INT8 => Ok((row.try_get::<i64>(i)? as i64).into()),
                    Type::OID => Ok((row.try_get::<u32>(i)? as u64).into()),
                    Type::TIMESTAMP => Ok(row.try_get::<chrono::NaiveDateTime>(i)?.into()),
                    Type::TIMESTAMPTZ => Ok(row
                        .try_get::<chrono::DateTime<chrono::FixedOffset>>(i)?
                        .into()),
                    Type::FLOAT8 => Ok(row.try_get::<f64>(i)?.try_into()?),
                    Type::FLOAT4 => Ok(row.try_get::<f32>(i)?.try_into()?),
                    Type::TIME => Ok(row.try_get::<chrono::NaiveTime>(i)?.into()),
                    Type::DATE => Ok(row.try_get::<chrono::NaiveDate>(i)?.into()),
                    Type::BIT | Type::VARBIT => Ok(row.try_get::<bit_vec::BitVec>(i)?.into()),
                    Type::BYTEA => Ok(noria_data::DataType::ByteArray(
                        row.try_get::<Vec<u8>>(i)?.into(),
                    )),
                    Type::BOOL => Ok(row.try_get::<bool>(i)?.into()),
                    _ => Err(ReadySetError::ReplicationFailed(format!(
                        "Unimplmented type conversion for {}",
                        t
                    ))),
                })
                .collect::<ReadySetResult<Vec<_>>>()?;

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
    pub async fn new(
        client: &'a mut pgsql::Client,
        noria: &'a mut noria::ControllerHandle,
        tables: Option<Vec<String>>,
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
            tables,
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
        if let Some(tables) = self.tables.as_ref() {
            // If a list of specific tables is provided, keep only those specfied
            table_list.retain(|e| !tables.contains(&e.name));
        }

        trace!(?table_list, "Loaded table list");
        trace!(?view_list, "Loaded view list");

        // For each table, retreive its structure
        let mut tables = Vec::with_capacity(table_list.len());
        for table in table_list {
            let create_table = table.get_table(&self.transaction).await?;
            debug!(%create_table, "Extending recipe");
            match self
                .noria
                .extend_recipe_no_leader_ready(&create_table.to_string())
                .await
            {
                Ok(_) => tables.push(create_table),
                Err(err) => error!(%err, "Error extending CREATE TABLE, table will not be used"),
            }
        }

        for view in view_list {
            let create_view = view.get_create_view(&self.transaction).await?;
            // Postgres returns a postgres style CREATE statement, but Noria only accepts MySQL
            // style
            let view = match nom_sql::parse_query(nom_sql::Dialect::PostgreSQL, &create_view) {
                Ok(v) => v.to_string(),
                Err(err) => {
                    error!(%err, "Error parsing CREATE VIEW, view will not be used");
                    continue;
                }
            };

            debug!(%view, "Extending recipe");
            if let Err(err) = self.noria.extend_recipe_no_leader_ready(&view).await {
                error!(%view, %err, "Error extending CREATE VIEW, view will not be used")
            }
        }

        self.noria
            .set_schema_replication_offset(Some(&PostgresPosition::default().into()))
            .await?;

        // Finally copy each table into noria
        for table in &tables {
            // TODO: parallelize with a connection pool if performance here matters
            let span = info_span!("Replicating table", table = %table.name);
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
                                AND pg_catalog.pg_table_is_visible(c.oid)
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
