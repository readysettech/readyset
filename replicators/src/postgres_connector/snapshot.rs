use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use std::future;
use std::time::Instant;

use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{pin_mut, StreamExt, TryFutureExt};
use metrics::register_gauge;
use nom_sql::{
    parse_key_specification_string, parse_sql_type, Column, ColumnConstraint, ColumnSpecification,
    CreateTableStatement, Dialect, Relation, SqlIdentifier, SqlQuery, TableKey,
};
use postgres_types::{accepts, FromSql, Kind, Type};
use readyset::metrics::recorded;
use readyset::recipe::changelist::{Change, ChangeList};
use readyset::{ReadySetError, ReadySetResult};
use readyset_data::{DfType, DfValue, Dialect as DataDialect, PgEnumMetadata};
use readyset_errors::{internal, internal_err, unsupported};
use tokio_postgres as pgsql;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};

use super::connector::CreatedSlot;
use super::PostgresPosition;
use crate::db_util::CreateSchema;
use crate::table_filter::TableFilter;

const BATCH_SIZE: usize = 1024; // How many queries to buffer before pushing to ReadySet

pub struct PostgresReplicator<'a> {
    /// This is the underlying (regular) PostgreSQL transaction used for most queries.
    pub(crate) transaction: pgsql::Transaction<'a>,
    /// A pool to be used for parallelizing snapshot tasks.
    pub(crate) pool: deadpool_postgres::Pool,
    pub(crate) noria: &'a mut readyset::ReadySetHandle,
    /// Filters out tables we are not interested in
    pub(crate) table_filter: TableFilter,
}

#[derive(Debug)]
enum TableKind {
    RegularTable,
    View,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
struct TableDescription {
    name: Relation,
    columns: Vec<ColumnEntry>,
    constraints: Vec<ConstraintEntry>,
}

#[derive(Debug, Clone)]
struct ColumnEntry {
    name: String,
    sql_type: String,
    not_null: bool,
    /// The [`Type`] of this column
    pg_type: Type,
}

#[derive(Debug, Clone)]
struct ConstraintEntry {
    #[allow(dead_code)]
    name: String,
    definition: TableKey,
    #[allow(dead_code)]
    kind: Option<ConstraintKind>,
}

#[derive(Debug)]
struct EnumVariant {
    #[allow(dead_code)]
    oid: u32,
    label: String,
}

#[derive(Clone, Debug)]
struct CustomTypeEntry {
    oid: u32,
    array_oid: u32,
    name: String,
    schema: String,
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
        let type_oid = row.try_get(4 /* pg_type.oid */)?;

        let typtype_to_kind = |typtype: i8| -> ReadySetResult<Kind> {
            match typtype as u8 as char {
                'b' => Ok(Kind::Simple),
                'c' => unsupported!("Composite types are not supported"),
                'd' => unsupported!("Domain types are not supported"),
                'e' => Ok(Kind::Enum(row.try_get(12 /* array_agg(e.enumlabel)... */)?)),
                'p' => Ok(Kind::Pseudo),
                'r' => unsupported!("Range types are not supported"),
                'm' => unsupported!("Multirange types are not supported"),
                c => internal!("Unknown value '{c}' in pg_catalog.pg_type.typtype"),
            }
        };

        let pg_type = if let Some(t) = Type::from_oid(type_oid) {
            t
        } else {
            let kind = if row.try_get(7 /* is_array */)? {
                Kind::Array(Type::new(
                    row.try_get(8)?,
                    row.try_get(9)?,
                    typtype_to_kind(row.try_get(10)?)?,
                    row.try_get(11)?,
                ))
            } else {
                typtype_to_kind(row.try_get(5)?)?
            };

            Type::new(
                row.try_get(3 /* pg_type.typname */)?,
                type_oid,
                kind,
                row.try_get(6 /* pg_namespace.nspname */)?,
            )
        };

        Ok(ColumnEntry {
            name: row.try_get(0 /* pg_attribute.attname */)?,
            not_null: row.try_get(1 /* pg_attribute.attnotnull */)?,
            sql_type: row.try_get(3)?,
            pg_type,
        })
    }
}

impl Display for ColumnEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` {}{}",
            self.name,
            self.sql_type,
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

impl TryFrom<pgsql::Row> for CustomTypeEntry {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        let oid = row.try_get(0)?;
        let array_oid = row.try_get(1)?;
        let name = row.try_get(2)?;
        let schema = row.try_get(3)?;
        Ok(CustomTypeEntry {
            oid,
            array_oid,
            name,
            schema,
        })
    }
}

impl TryFrom<pgsql::Row> for EnumVariant {
    type Error = pgsql::Error;

    fn try_from(row: pgsql::Row) -> Result<Self, Self::Error> {
        let oid = row.try_get(0)?;
        let label = row.try_get(1)?;
        Ok(EnumVariant { oid, label })
    }
}

impl CustomTypeEntry {
    async fn get_variants<'a>(
        &self,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<Vec<EnumVariant>, pgsql::Error> {
        let query = r"
            SELECT oid, enumlabel
            FROM pg_enum
            WHERE enumtypid = $1
            ORDER BY enumsortorder ASC
        ";

        let res = transaction.query(query, &[&self.oid]).await?;
        res.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) fn into_relation(self) -> Relation {
        Relation {
            schema: Some(self.schema.into()),
            name: self.name.into(),
        }
    }
}

impl TableEntry {
    async fn get_columns<'a>(
        oid: u32,
        transaction: &'a pgsql::Transaction<'a>,
    ) -> Result<Vec<ColumnEntry>, ReadySetError> {
        let query = r#"
            SELECT
                a.attname,
                a.attnotnull,
                t.typname,
                CASE
                WHEN t.typtype = 'e'
                THEN format('"%s"."%s"', tn.nspname, t.typname)
                WHEN member_t.oid IS NOT NULL AND member_t.typtype = 'e'
                THEN format('"%s"."%s"[]', member_tn.nspname, member_t.typname)
                ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
                END AS sql_type,
                t.oid,
                t.typtype,
                tn.nspname,
                member_t.oid IS NOT NULL AS is_array,
                member_t.typname,
                member_t.oid,
                member_t.typtype,
                member_tn.nspname,
                (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder ASC)
                 FROM pg_enum e
                 WHERE (member_t.oid IS NULL AND (e.enumtypid = t.oid)) OR e.enumtypid = member_t.oid)
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
            JOIN pg_catalog.pg_namespace tn ON t.typnamespace = tn.oid
            LEFT JOIN pg_catalog.pg_type member_t ON t.typelem = member_t.oid
            LEFT JOIN pg_catalog.pg_namespace member_tn ON member_t.typnamespace = member_tn.oid
            WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            "#;

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
        let columns = Self::get_columns(self.oid, transaction)
            .await
            .map_err(|e| {
                ReadySetError::TableError {
                    table: Relation {
                        schema: Some(self.schema.clone().into()),
                        name: self.name.clone().into(),
                    },
                    source: Box::new(e),
                }
                .context("when loading columns for the table")
            })?;
        let constraints = Self::get_constraints(self.oid, transaction)
            .await
            .map_err(|e| {
                ReadySetError::TableError {
                    table: Relation {
                        schema: Some(self.schema.clone().into()),
                        name: self.name.clone().into(),
                    },
                    source: Box::new(ReadySetError::ReplicationFailed(e.to_string())),
                }
                .context("when loading constraints")
            })?;

        Ok(TableDescription {
            name: Relation {
                schema: Some(self.schema.into()),
                name: self.name.into(),
            },
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
        writeln!(f, "CREATE TABLE {} (", self.name)?;
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
    fn schema(&self) -> ReadySetResult<&SqlIdentifier> {
        self.name
            .schema
            .as_ref()
            .ok_or_else(|| internal_err!("All tables must have a schema in the replicator"))
    }

    fn try_into_change(self) -> ReadySetResult<Change> {
        Ok(Change::CreateTable(CreateTableStatement {
            table: self.name.clone(),
            fields: self
                .columns
                .into_iter()
                .map(|c| {
                    Ok(ColumnSpecification {
                        column: Column {
                            name: c.name.into(),
                            table: Some(self.name.clone()),
                        },
                        sql_type: parse_sql_type(Dialect::PostgreSQL, c.sql_type)
                            .map_err(|e| internal_err!("Could not parse SQL type: {e}"))?,
                        constraints: if c.not_null {
                            vec![ColumnConstraint::NotNull]
                        } else {
                            vec![]
                        },
                        comment: None,
                    })
                })
                .collect::<ReadySetResult<_>>()?,
            keys: if self.constraints.is_empty() {
                None
            } else {
                Some(self.constraints.into_iter().map(|c| c.definition).collect())
            },
            if_not_exists: false,
            options: vec![],
        }))
    }

    /// Copy a table's contents from PostgreSQL to ReadySet
    async fn dump<'a>(
        &self,
        transaction: &'a deadpool_postgres::Transaction<'a>,
        mut noria_table: readyset::Table,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;

        let nrows = transaction
            .query_one(
                format!(
                    "SELECT count(*) AS nrows FROM \"{}\".\"{}\"",
                    self.schema()?,
                    &self.name.name,
                )
                .as_str(),
                &[],
            )
            .await?
            .try_get::<_, i64>("nrows")?;

        // The most efficient way to copy an entire table is COPY BINARY
        let query = format!(
            "COPY \"{}\".\"{}\" TO stdout BINARY",
            self.schema()?,
            self.name.name
        );
        let rows = transaction.copy_out(query.as_str()).await?;

        let type_map: Vec<_> = self.columns.iter().map(|c| c.pg_type.clone()).collect();
        let binary_rows = pgsql::binary_copy::BinaryCopyOutStream::new(rows, &type_map);

        pin_mut!(binary_rows);

        let mut noria_rows = Vec::with_capacity(BATCH_SIZE);

        info!(rows = %nrows, "Snapshotting started");
        let progress_percentage_metric: metrics::Gauge = register_gauge!(
            recorded::REPLICATOR_SNAPSHOT_PERCENT,
            "schema" => self.schema()?.to_string(),
            "name" => self.name.name.to_string()
        );
        let start_time = Instant::now();
        let mut last_report_time = start_time;
        let snapshot_report_interval_secs = snapshot_report_interval_secs as u64;

        while let Some(Ok(row)) = binary_rows.next().await {
            let noria_row = (0..type_map.len())
                .map(|i| row.try_get::<DfValue>(i))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    progress_percentage_metric.set(0.0);
                    err
                })?;

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
                    .await
                    .map_err(|err| {
                        progress_percentage_metric.set(0.0);
                        err
                    })?;
            }

            if snapshot_report_interval_secs != 0
                && last_report_time.elapsed().as_secs() > snapshot_report_interval_secs
            {
                last_report_time = Instant::now();
                let estimate =
                    crate::estimate_remaining_time(start_time.elapsed(), cnt as f64, nrows as f64);
                let progress_percent = (cnt as f64 / nrows as f64) * 100.;
                let progress = format!("{:.2}%", progress_percent);
                info!(rows_replicated = %cnt, %progress, %estimate, "Snapshotting progress");
                progress_percentage_metric.set(progress_percent);
            }
        }

        if !noria_rows.is_empty() {
            noria_table.insert_many(noria_rows).await.map_err(|err| {
                progress_percentage_metric.set(0.0);
                err
            })?;
        }

        info!(rows_replicated = %cnt, "Snapshotting finished");
        progress_percentage_metric.set(100.0);

        Ok(())
    }
}

impl<'a> PostgresReplicator<'a> {
    pub(crate) async fn new(
        client: &'a mut pgsql::Client,
        pool: deadpool_postgres::Pool,
        noria: &'a mut readyset::ReadySetHandle,
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
            pool,
            noria,
            table_filter,
        })
    }

    async fn snapshot_table(
        pool: deadpool_postgres::Pool,
        span: tracing::Span,
        table: &TableDescription,
        noria_table: readyset::Table,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let mut client = pool.get().await?;

        let transaction = client
            .build_transaction()
            .deferrable(true)
            .isolation_level(pgsql::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;

        table
            .dump(&transaction, noria_table, snapshot_report_interval_secs)
            .instrument(span.clone())
            .await
    }

    /// Begin the replication process, starting with the recipe for the database, followed
    /// by each table's contents
    pub(crate) async fn snapshot_to_noria(
        &mut self,
        replication_slot: &CreatedSlot,
        create_schema: &mut CreateSchema,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let wal_position = PostgresPosition::from(replication_slot.consistent_point).into();
        self.set_snapshot(&replication_slot.snapshot_name).await?;

        let mut table_list = self.get_table_list(TableKind::RegularTable).await?;
        let mut view_list = self.get_table_list(TableKind::View).await?;
        let mut custom_types = self.get_custom_types().await?;

        table_list.retain(|tbl| {
            self.table_filter
                .contains(tbl.schema.as_str(), tbl.name.as_str())
        });
        view_list.retain(|view| self.table_filter.contains_schema(&view.schema));
        custom_types.retain(|ty| self.table_filter.contains_schema(&ty.schema));

        trace!(?table_list, "Loaded table list");
        trace!(?view_list, "Loaded view list");
        trace!(?custom_types, "Loaded custom types");

        for ty in custom_types {
            let variants = match ty.get_variants(&self.transaction).await {
                Ok(v) => v,
                Err(error) => {
                    warn!(%error, custom_type=?ty, "Error looking up variants for custom type, type will not be used");
                    continue;
                }
            };
            let changelist = ChangeList::from_change(
                Change::CreateType {
                    ty: DfType::from_enum_variants(
                        variants.into_iter().map(|v| v.label),
                        Some(PgEnumMetadata {
                            name: ty.name.clone().into(),
                            schema: ty.schema.clone().into(),
                            oid: ty.oid,
                            array_oid: ty.array_oid,
                        }),
                    ),
                    name: ty.clone().into_relation(),
                },
                DataDialect::DEFAULT_POSTGRESQL,
            );
            if let Err(error) = self.noria.extend_recipe_no_leader_ready(changelist).await {
                warn!(%error, custom_type=?ty, "Error creating custom type, type will not be used");
            }
        }

        // For each table, retrieve its structure
        let mut tables = Vec::with_capacity(table_list.len());
        for table in table_list {
            let table_name = &table.name.clone().to_string();
            let create_table = match table.get_table(&self.transaction).await {
                Ok(ct) => ct,
                Err(error) => {
                    warn!(%error, "Error looking up table, table will not be used");
                    continue;
                }
            };

            debug!(%create_table, "Extending recipe");
            create_schema.add_table_create(create_table.name.to_string(), create_table.to_string());

            match future::ready(
                create_table
                    .clone()
                    .try_into_change()
                    .map(|change| ChangeList::from_change(change, DataDialect::DEFAULT_POSTGRESQL)),
            )
            .and_then(|changelist| self.noria.extend_recipe_no_leader_ready(changelist))
            .await
            {
                Ok(_) => tables.push(create_table),
                Err(error) => {
                    warn!(%error, table=%table_name, "Error extending CREATE TABLE, table will not be used")
                }
            }
        }

        for view in view_list {
            let view_name = view.name.clone();
            let view_schema = view.schema.clone();
            let create_view = view.get_create_view(&self.transaction).await?;
            create_schema.add_view_create(view_name.clone(), create_view.clone());

            let view = match nom_sql::parse_query(Dialect::PostgreSQL, &create_view) {
                Ok(SqlQuery::CreateView(view)) => view,
                Ok(query) => {
                    error!(
                        kind = %query.query_type(),
                        "Unexpected query type when parsing CREATE VIEW statement"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(%error, view=%view_name, "Error parsing CREATE VIEW, view will not be used");
                    continue;
                }
            };

            debug!(%view, "Extending recipe");

            if let Err(error) = self
                .noria
                .extend_recipe_no_leader_ready(
                    ChangeList::from_change(
                        Change::CreateView(view),
                        DataDialect::DEFAULT_POSTGRESQL,
                    )
                    .with_schema_search_path(vec![view_schema.into()]),
                )
                .await
            {
                warn!(%error, view=%view_name, "Error extending CREATE VIEW, view will not be used");
            }
        }

        // The current schema was replicated, assign the current position
        self.noria
            .set_schema_replication_offset(Some(&wal_position))
            .await?;

        let replication_offsets = self.noria.replication_offsets().await?;

        tables.drain_filter(|t| replication_offsets.has_table(&t.name)).for_each(|t|
            info!(table = %t.name, "Replication offset already exists for table, skipping snapshot")
        );

        // Finally copy each table into noria
        let mut futs = Vec::with_capacity(tables.len());
        for table in &tables {
            let span = info_span!("Replicating table", table = %table.name);
            span.in_scope(|| info!("Replicating table"));
            let mut noria_table = self
                .noria
                .table(table.name.clone())
                .instrument(span.clone())
                .await?;
            noria_table.set_snapshot_mode(true).await?;

            let pool = self.pool.clone();
            futs.push(Self::snapshot_table(
                pool,
                span,
                table,
                noria_table,
                snapshot_report_interval_secs,
            ))
        }

        join_all(futs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ReadySetError>>()?;

        let mut compacting = FuturesUnordered::new();
        for table in tables {
            let mut noria_table = self.noria.table(table.name.clone()).await?;
            let wal_position = wal_position.clone();
            compacting.push(async move {
                let span = info_span!("Compacting table", table = %table.name);
                span.in_scope(|| info!("Setting replication offset"));
                if let Err(error) = noria_table
                    .set_replication_offset(wal_position)
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

    /// Retrieve a list of tables of the specified kind in the specified schema
    async fn get_table_list(&mut self, kind: TableKind) -> Result<Vec<TableEntry>, pgsql::Error> {
        let kind_code = match kind {
            TableKind::RegularTable => 'r',
            TableKind::View => 'v',
        } as i8;

        // We filter out tables that have any generated columns (pgcatalog.pg_attribute.attgenerated
        // <> '') because they are currently unsupported and will cause issues
        // with replication when the column count on an insert doesnt match the column count
        // of the table.
        let query = r"
        SELECT n.nspname, c.oid, c.relname, c.relkind
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
        WHERE c.relkind IN ($1) AND n.nspname <> 'pg_catalog'
                                AND n.nspname <> 'information_schema'
                                AND n.nspname !~ '^pg_toast'
                                AND (c.reltoastrelid = 0 OR pg_relation_size(c.reltoastrelid) = 0)
                                AND c.oid NOT IN(
        SELECT c.oid
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_attribute a
            ON a.attrelid = c.oid
            WHERE attgenerated <> ''
        )
        ";

        let tables = self.transaction.query(query, &[&kind_code]).await?;
        tables.into_iter().map(TryInto::try_into).collect()
    }

    /// Retrieve a list of custom types
    ///
    /// Currently this is limited to enum types since that's all we support, but in the future this
    /// can be extended to support composite types and ranges as well
    async fn get_custom_types(&mut self) -> Result<Vec<CustomTypeEntry>, pgsql::Error> {
        let query = r"
            SELECT t.oid, t.typarray, t.typname, tn.nspname
            FROM pg_type t
            JOIN pg_catalog.pg_namespace tn ON t.typnamespace = tn.oid
            WHERE typtype = 'e'
        ";
        let res = self.transaction.query(query, &[]).await?;
        res.into_iter().map(TryInto::try_into).collect()
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
            name: Relation {
                schema: Some("public".into()),
                name: "ar_internal_metadata".into(),
            },
            columns: vec![
                ColumnEntry {
                    name: "key".into(),
                    sql_type: "varchar".into(),
                    not_null: true,
                    pg_type: Type::VARCHAR,
                },
                ColumnEntry {
                    name: "value".into(),
                    sql_type: "varchar".into(),
                    not_null: false,
                    pg_type: Type::VARCHAR,
                },
                ColumnEntry {
                    name: "created_at".into(),
                    sql_type: "timestamp(6) without time zone".into(),
                    not_null: true,
                    pg_type: Type::TIMESTAMP,
                },
                ColumnEntry {
                    name: "updated_at".into(),
                    sql_type: "timestamp(6) without time zone".into(),
                    not_null: true,
                    pg_type: Type::TIMESTAMP,
                },
            ],
            constraints: vec![ConstraintEntry {
                name: "ar_internal_metadata_pkey".into(),
                definition: TableKey::PrimaryKey {
                    constraint_name: None,
                    index_name: None,
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
            TableKey::PrimaryKey {
                constraint_name,
                index_name,
                columns,
            } => {
                assert!(constraint_name.is_none());
                assert!(index_name.is_none());
                assert_eq!(columns.len(), 1);
                assert_eq!(columns.first().unwrap().name, "key");
            }
            _ => panic!(),
        }
    }
}
