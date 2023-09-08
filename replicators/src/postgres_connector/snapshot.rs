use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use std::future;
use std::time::Instant;

use futures::stream::FuturesUnordered;
use futures::{pin_mut, StreamExt, TryFutureExt};
use itertools::Itertools;
use metrics::register_gauge;
use nom_sql::{
    parse_key_specification_string, parse_sql_type, Column, ColumnConstraint, ColumnSpecification,
    CreateTableBody, CreateTableStatement, Dialect, NonReplicatedRelation, NotReplicatedReason,
    Relation, SqlIdentifier, TableKey,
};
use postgres_types::{accepts, FromSql, Kind, Type};
use readyset_client::metrics::recorded;
use readyset_client::recipe::changelist::{Change, ChangeList, PostgresTableMetadata};
use readyset_client::TableOperation;
use readyset_data::{DfType, DfValue, Dialect as DataDialect, PgEnumMetadata};
use readyset_errors::{internal, internal_err, unsupported, ReadySetError, ReadySetResult};
use replication_offset::postgres::PostgresPosition;
use replication_offset::ReplicationOffset;
use tokio_postgres as pgsql;
use tracing::{debug, info, info_span, trace, warn, Instrument};

use super::connector::CreatedSlot;
use crate::db_util::CreateSchema;
use crate::table_filter::TableFilter;

const BATCH_SIZE: usize = 1024; // How many queries to buffer before pushing to ReadySet

macro_rules! get_transaction {
    ($self:expr) => {
        $self
            .transaction
            .as_ref()
            .expect("Using snapshot transaction after commit")
    };
}

pub struct PostgresReplicator<'a> {
    /// This is the underlying (regular) PostgreSQL transaction used for most queries.
    pub(crate) transaction: Option<pgsql::Transaction<'a>>,
    /// A pool to be used for parallelizing snapshot tasks.
    pub(crate) pool: deadpool_postgres::Pool,
    pub(crate) noria: &'a mut readyset_client::ReadySetHandle,
    /// Filters out tables we are not interested in
    pub(crate) table_filter: TableFilter,
}

#[derive(Debug)]
enum TableKind {
    RegularTable,
    View,
    PartitionedTable,
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
    oid: u32,
    name: Relation,
    columns: Vec<ColumnEntry>,
    constraints: Vec<ConstraintEntry>,
}

#[derive(Debug, Clone)]
struct ColumnEntry {
    attnum: i16,
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
        let type_oid = row.try_get(5 /* pg_type.oid */)?;

        let typtype_to_kind = |typtype: i8| -> ReadySetResult<Kind> {
            match typtype as u8 as char {
                'b' => Ok(Kind::Simple),
                'c' => unsupported!("Composite types are not supported"),
                'd' => unsupported!("Domain types are not supported"),
                'e' => Ok(Kind::Enum(row.try_get(13 /* array_agg(e.enumlabel)... */)?)),
                'p' => Ok(Kind::Pseudo),
                'r' => unsupported!("Range types are not supported"),
                'm' => unsupported!("Multirange types are not supported"),
                c => internal!("Unknown value '{c}' in pg_catalog.pg_type.typtype"),
            }
        };

        let pg_type = if let Some(t) = Type::from_oid(type_oid) {
            t
        } else {
            let kind = if row.try_get(8 /* is_array */)? {
                Kind::Array(Type::new(
                    row.try_get(9)?,
                    row.try_get(10)?,
                    typtype_to_kind(row.try_get(11)?)?,
                    row.try_get(12)?,
                ))
            } else {
                typtype_to_kind(row.try_get(6)?)?
            };

            Type::new(
                row.try_get(3 /* pg_type.typname */)?,
                type_oid,
                kind,
                row.try_get(7 /* pg_namespace.nspname */)?,
            )
        };

        Ok(ColumnEntry {
            attnum: row.try_get(0 /* pg_attribute.attnum */)?,
            name: row.try_get(1 /* pg_attribute.attname */)?,
            not_null: row.try_get(2 /* pg_attribute.attnotnull */)?,
            sql_type: row.try_get(4)?,
            pg_type,
        })
    }
}

impl Display for ColumnEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}",
            Dialect::PostgreSQL.quote_identifier(&self.name),
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
        write!(f, "{}", self.definition.display(Dialect::PostgreSQL))
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
                a.attnum,
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
        &self,
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
            oid: self.oid,
            name: Relation {
                schema: Some(self.schema.clone().into()),
                name: self.name.clone().into(),
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
        writeln!(
            f,
            "CREATE TABLE {} (",
            self.name.display(Dialect::PostgreSQL)
        )?;
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
        Ok(Change::CreateTable {
            pg_meta: Some(PostgresTableMetadata {
                oid: self.oid,
                column_oids: self
                    .columns
                    .iter()
                    .map(|c| (c.name.clone().into(), c.attnum))
                    .collect(),
            }),
            statement: CreateTableStatement {
                if_not_exists: false,
                table: self.name.clone(),
                body: Ok(CreateTableBody {
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
                }),
                options: Ok(vec![]),
            },
        })
    }

    /// Copy a table's contents from PostgreSQL to ReadySet
    async fn dump<'a>(
        &self,
        transaction: &'a deadpool_postgres::Transaction<'a>,
        mut noria_table: readyset_client::Table,
        snapshot_report_interval_secs: u16,
        wal_position: &ReplicationOffset,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;

        let approximate_rows = transaction
            .query_one(
                // Fetch an *approximate estimate* of the number of rows in the table, rather than
                // an exact count (the latter is *significantly* more expensive, especially for
                // large tables). We're only using this for reporting snapshotting progress, so an
                // approximate row count should be fine.
                // Note that sometimes `c.reltuples` can be `-1` if the table is very new and
                // hasn't been analyzed yet, so we `greatest` it with `1` to make
                // sure we always have a positive integer (to avoid panics when subtracting
                // durations)
                "SELECT greatest(c.reltuples::bigint, 1) AS approximate_nrows
                 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE c.relname = $1 AND n.nspname = $2",
                &[&self.name.name.as_str(), &self.schema()?.as_str()],
            )
            .await?
            .try_get::<_, i64>("approximate_nrows")?;

        // The most efficient way to copy an entire table is COPY BINARY
        let query = format!(
            "COPY \"{}\".\"{}\" TO stdout BINARY",
            self.schema()?,
            self.name.name
        );
        let rows = transaction.copy_out(query.as_str()).await?;

        let type_map: Vec<_> = self.columns.iter().map(|c| c.pg_type.clone()).collect();
        let binary_row_batches = pgsql::binary_copy::BinaryCopyOutStream::new(rows, &type_map)
            .chunks(BATCH_SIZE)
            .peekable();

        pin_mut!(binary_row_batches);

        info!(%approximate_rows, "Snapshotting started");
        let progress_percentage_metric: metrics::Gauge = register_gauge!(
            recorded::REPLICATOR_SNAPSHOT_PERCENT,
            "schema" => self.schema()?.to_string(),
            "name" => self.name.name.to_string()
        );
        let start_time = Instant::now();
        let mut last_report_time = start_time;
        let snapshot_report_interval_secs = snapshot_report_interval_secs as u64;
        let mut set_replication_offset_and_snapshot_mode = false;

        while let Some(batch) = binary_row_batches.as_mut().next().await {
            let cnt_copy = cnt;
            let batch_size = batch.len();
            let noria_rows_iter = batch
                .into_iter()
                .enumerate()
                .map(|(index_within_batch, row)| {
                    row.map_err(ReadySetError::from).and_then(|row| {
                        (0..type_map.len())
                            .map(|i| row.try_get::<DfValue>(i))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|err| {
                                progress_percentage_metric.set(0.0);
                                ReadySetError::ReplicationFailed(format!(
                                    "Failed converting to DfValue, table: {}, row: {}, err: {}",
                                    noria_table.table_name().display(Dialect::PostgreSQL),
                                    cnt_copy + index_within_batch,
                                    err
                                ))
                            })
                    })
                });

            cnt += batch_size;

            if binary_row_batches.as_mut().peek().await.is_none() {
                // This is the last batch of rows we're adding to the table, so batch the RPCs to
                // set the replication offset and compact the table along with the insertion
                info!(
                    table = %noria_table.table_name().display(Dialect::PostgreSQL),
                    %wal_position,
                    "Setting replication offset and compacting table"
                );

                let capacity = match noria_rows_iter.size_hint() {
                    (_, Some(high)) => high,
                    (low, None) => low,
                } + 2;
                let mut actions = Vec::with_capacity(capacity);
                for row in noria_rows_iter {
                    actions.push(TableOperation::Insert(row?));
                }

                actions.push(TableOperation::SetReplicationOffset(wal_position.clone()));
                actions.push(TableOperation::SetSnapshotMode(false));

                noria_table.perform_all(actions).await?;
                set_replication_offset_and_snapshot_mode = true;
            } else {
                noria_table
                    .insert_many(noria_rows_iter.collect::<Result<Vec<_>, _>>()?)
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
                let estimate = crate::estimate_remaining_time(
                    start_time.elapsed(),
                    cnt as f64,
                    approximate_rows as f64,
                );
                let progress_percent = (cnt as f64 / approximate_rows as f64) * 100.;
                let progress = format!("{:.2}%", progress_percent);
                info!(rows_replicated = %cnt, %progress, %estimate, "Snapshotting progress");
                progress_percentage_metric.set(progress_percent);
            }
        }

        // If the table was empty, we didn't set the replication offset or disable snapshot mode
        // above, so we need to do it here
        if !set_replication_offset_and_snapshot_mode {
            noria_table
                .perform_all([
                    TableOperation::SetReplicationOffset(wal_position.clone()),
                    TableOperation::SetSnapshotMode(false),
                ])
                .await?;
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
        noria: &'a mut readyset_client::ReadySetHandle,
        table_filter: TableFilter,
    ) -> ReadySetResult<PostgresReplicator<'a>> {
        let transaction = Some(
            client
                .build_transaction()
                .deferrable(true)
                .isolation_level(pgsql::IsolationLevel::RepeatableRead)
                .start()
                .await?,
        );

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
        table: TableDescription,
        noria_table: readyset_client::Table,
        snapshot_report_interval_secs: u16,
        snapshot_name: String,
        wal_position: &ReplicationOffset,
    ) -> ReadySetResult<()> {
        let mut client = pool.get().await?;

        let transaction = client
            .build_transaction()
            .deferrable(true)
            .isolation_level(pgsql::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;

        // Ensure each table has a consistent view by using the same snapshot
        let query = format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
        transaction.query(query.as_str(), &[]).await?;

        table
            .dump(
                &transaction,
                noria_table,
                snapshot_report_interval_secs,
                wal_position,
            )
            .instrument(span.clone())
            .await
            .map_err(|e| ReadySetError::TableError {
                table: table.name.clone(),
                source: Box::new(e),
            })
    }

    /// Snapshot the contents of the upstream database to ReadySet, starting with the DDL, followed
    /// by each table's contents.
    ///
    /// If `full_snapshot` is set to `true`, *all* tables will be snapshotted, even those that
    /// already have replication offsets in ReadySet.
    pub(crate) async fn snapshot_to_noria(
        &mut self,
        replication_slot: &CreatedSlot,
        create_schema: &mut CreateSchema,
        snapshot_report_interval_secs: u16,
        full_snapshot: bool,
    ) -> ReadySetResult<()> {
        let wal_position = PostgresPosition::commit_start(replication_slot.consistent_point).into();
        self.set_snapshot(&replication_slot.snapshot_name).await?;

        let table_list = self.get_table_list(TableKind::RegularTable).await?;
        let view_list = self.get_table_list(TableKind::View).await?;
        let custom_types = self.get_custom_types().await?;

        let (table_list, mut non_replicated) =
            table_list.into_iter().partition::<Vec<_>, _>(|tbl| {
                self.table_filter
                    .should_be_processed(tbl.schema.as_str(), tbl.name.as_str())
            });

        // We don't support partitioned tables (only the partitions themselves) so mark those as
        // non-replicated as well
        let partitioned_tables = self.get_table_list(TableKind::PartitionedTable).await?;
        let partitioned_identifiers: HashSet<_> = partitioned_tables
            .iter()
            .map(|te| format!("{}.{}", te.schema, te.name))
            .collect();

        non_replicated.extend(partitioned_tables);

        // We don't filter the view list by schemas since a view could be in schema 1 (that may not
        // be replicated), but refer to only tables in schema 2 that are all replicated. If we try
        // to process a view that points to unreplicated schemas, we will just fail and ignore that
        // view.
        // Similarly, custom types may have cross-schema references and aren't filtered initially

        trace!(?table_list, "Loaded table list");
        trace!(?view_list, "Loaded view list");
        trace!(?custom_types, "Loaded custom types");

        self.set_replica_identity_for_tables(&table_list).await?;

        self.noria
            .extend_recipe_no_leader_ready(ChangeList::from_changes(
                non_replicated
                    .into_iter()
                    .map(|te| {
                        let te_identifier = format!("{}.{}", te.schema, te.name);
                        let not_replicated_reason: NotReplicatedReason =
                            if partitioned_identifiers.contains(&te_identifier) {
                                NotReplicatedReason::Partitioned
                            } else {
                                NotReplicatedReason::Configuration
                            };
                        Change::AddNonReplicatedRelation(NonReplicatedRelation {
                            name: Relation {
                                schema: Some(te.schema.into()),
                                name: te.name.into(),
                            },
                            reason: not_replicated_reason,
                        })
                    })
                    .collect::<Vec<_>>(),
                DataDialect::DEFAULT_POSTGRESQL,
            ))
            .await?;

        for ty in custom_types {
            let variants = match ty.get_variants(get_transaction!(self)).await {
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

        self.drop_leftover_tables(&table_list, &view_list)
            .await
            .map_err(|e| e.context("Error while cleaning up leftover cache tables"))?;

        // For each table, retrieve its structure
        let mut tables = Vec::with_capacity(table_list.len());
        for table in table_list {
            let table_name = &table.name.clone().to_string();
            match table
                .get_table(get_transaction!(self))
                .and_then(|create_table| {
                    future::ready(
                        create_table
                            .clone()
                            .try_into_change()
                            .map(move |change| (change, create_table)),
                    )
                })
                .and_then(|(change, create_table)| {
                    debug!(%create_table, "Extending recipe");
                    create_schema.add_table_create(
                        create_table.name.display(Dialect::PostgreSQL).to_string(),
                        create_table.to_string(),
                    );
                    let mut changes = if full_snapshot {
                        // If we're doing a full snapshot, drop the table before creating it, to
                        // clear out any old data.
                        vec![Change::Drop {
                            name: create_table.name.clone(),
                            if_exists: true,
                        }]
                    } else {
                        vec![]
                    };
                    changes.push(change);

                    self.noria
                        .extend_recipe_no_leader_ready(ChangeList::from_changes(
                            changes,
                            DataDialect::DEFAULT_POSTGRESQL,
                        ))
                        .map_ok(|_| create_table)
                })
                .await
            {
                Ok(create_table) => {
                    tables.push(create_table);
                }
                Err(error) => {
                    warn!(%error, table=%table_name, "Error extending CREATE TABLE, table will not be used");
                    self.noria
                        .extend_recipe_no_leader_ready(ChangeList::from_change(
                            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                name: Relation {
                                    schema: Some(table.schema.into()),
                                    name: table.name.clone().into(),
                                },
                                reason: NotReplicatedReason::from_string(&error.to_string()),
                            }),
                            DataDialect::DEFAULT_POSTGRESQL,
                        ))
                        .await?;
                }
            }
        }

        for view in view_list {
            let view_name = view.name.clone();
            let view_schema = view.schema.clone();

            match view
                .get_create_view(get_transaction!(self))
                .map_err(|e| e.into())
                .and_then(|create_view| {
                    create_schema.add_view_create(view_name.clone(), create_view.clone());
                    future::ready(
                        nom_sql::parse_create_view(Dialect::PostgreSQL, &create_view)
                            .map_err(|_| ReadySetError::UnparseableQuery { query: create_view }),
                    )
                })
                .and_then(|view| {
                    debug!(view = %view.display(nom_sql::Dialect::PostgreSQL), "Extending recipe");
                    self.noria.extend_recipe_no_leader_ready(
                        ChangeList::from_change(
                            Change::CreateView(view),
                            DataDialect::DEFAULT_POSTGRESQL,
                        )
                        .with_schema_search_path(vec![view_schema.clone().into()]),
                    )
                })
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        %error,
                        view=%view_name,
                        "Error extending CREATE VIEW, view will not be used"
                    );

                    self.noria
                        .extend_recipe_no_leader_ready(ChangeList::from_change(
                            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                name: Relation {
                                    schema: Some(view_schema.into()),
                                    name: view_name.into(),
                                },
                                reason: NotReplicatedReason::from_string(&error.to_string()),
                            }),
                            DataDialect::DEFAULT_POSTGRESQL,
                        ))
                        .await?;
                }
            }
        }

        // The current schema was replicated, assign the current position
        debug!(%wal_position, "Setting schema replication offset");
        self.noria
            .set_schema_replication_offset(Some(&wal_position))
            .await?;

        let replication_offsets = self.noria.replication_offsets().await?;

        if !full_snapshot {
            tables
                .drain_filter(|t| replication_offsets.has_table(&t.name))
                .for_each(|t| {
                    debug!(
                        table = %t.name.display(Dialect::PostgreSQL),
                        "Replication offset already exists for table, skipping snapshot"
                    )
                });
        }

        // Commit the transaction we were using to snapshot the schema. This is important since that
        // transaction holds onto locks for tables which we now need to load data from.
        self.transaction.take().unwrap().commit().await?;

        // Finally copy each table into noria
        let mut snapshotting_tables = FuturesUnordered::new();
        for table in &tables {
            let span =
                info_span!("Snapshotting table", table = %table.name.display(Dialect::PostgreSQL));
            span.in_scope(|| info!("Snapshotting table"));
            let mut noria_table = self
                .noria
                .table(table.name.clone())
                .instrument(span.clone())
                .await?;
            span.in_scope(|| trace!("Setting snapshot mode"));
            noria_table.set_snapshot_mode(true).await?;
            span.in_scope(|| trace!("Set snapshot mode"));

            let pool = self.pool.clone();

            let snapshot_name = replication_slot.snapshot_name.clone();
            let table = table.clone();
            snapshotting_tables.push(Self::snapshot_table(
                pool,
                span,
                table,
                noria_table,
                snapshot_report_interval_secs,
                snapshot_name,
                &wal_position,
            ))
        }

        // Remove from the set of tables any that failed to snapshot,
        // and add them as non-replicated relations.
        // Propagate any non-TableErrors.
        while let Some(res) = snapshotting_tables.next().await {
            if let Err(e) = res {
                match e {
                    ReadySetError::TableError { ref table, .. } => {
                        warn!(%e, table=%table.display(Dialect::PostgreSQL), "Error snapshotting, table will not be used");
                        tables.retain(|t| t.name != *table);
                        self.noria
                            .extend_recipe_no_leader_ready(ChangeList::from_changes(
                                vec![
                                    Change::Drop {
                                        name: table.clone(),
                                        if_exists: false,
                                    },
                                    Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                        name: table.clone(),
                                        reason: NotReplicatedReason::from_string(&format!(
                                            "{:?}",
                                            e
                                        )),
                                    }),
                                ],
                                DataDialect::DEFAULT_POSTGRESQL,
                            ))
                            .await?;
                    }
                    _ => Err(e)?,
                }
            }
        }

        // Wait for all tables to finish compaction
        self.noria.wait_for_all_tables_to_compact().await?;

        Ok(())
    }

    /// Retrieve a list of tables of the specified kind in the specified schema
    async fn get_table_list(&mut self, kind: TableKind) -> Result<Vec<TableEntry>, pgsql::Error> {
        let kind_code = match kind {
            TableKind::RegularTable => 'r',
            TableKind::View => 'v',
            TableKind::PartitionedTable => 'p',
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
                                AND c.oid NOT IN(
        SELECT c.oid
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_attribute a
            ON a.attrelid = c.oid
            WHERE attgenerated <> ''
        )
        ";

        let tables = get_transaction!(self).query(query, &[&kind_code]).await?;
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
        let res = get_transaction!(self).query(query, &[]).await?;
        res.into_iter().map(TryInto::try_into).collect()
    }

    /// Assign the specific snapshot to the underlying transaction
    async fn set_snapshot(&mut self, name: &str) -> Result<(), pgsql::Error> {
        let query = format!("SET TRANSACTION SNAPSHOT '{}'", name);
        get_transaction!(self).query(query.as_str(), &[]).await?;
        Ok(())
    }

    /// Get a list of ReadySet cache tables and compare against the list of upstream tables and
    /// views, dropping any cache tables that are missing upstream.
    ///
    /// This function is called during snapshotting to detect and handle cases where we somehow
    /// have a leftover cache that doesn't correspond to any upstream table or view. Without
    /// removing such tables, we can run potentially run into problems later on where we panic due
    /// to failure to get a replication offset.
    async fn drop_leftover_tables(
        &mut self,
        table_list: &[TableEntry],
        view_list: &[TableEntry],
    ) -> ReadySetResult<()> {
        let mut leftover_tables = self.noria.tables().await?;
        leftover_tables.retain(|rel, _| {
            if let Some(schema) = &rel.schema {
                let schema = schema.as_str();
                let name = rel.name.as_str();

                !table_list
                    .iter()
                    .any(|t| t.schema == schema && t.name == name)
                    && !view_list
                        .iter()
                        .any(|t| t.schema == schema && t.name == name)
            } else {
                // All base tables under Postgres should have a schema, so if this happens then
                // something truly weird is going on. Don't mark this as an leftover table since we
                // don't know what it actually is, but do warn the user via a log message:
                warn!("Existing ReadySet cache table {} has no schema", rel.name);
                false
            }
        });
        if !leftover_tables.is_empty() {
            warn!(
                "Removing existing ReadySet cache tables with no upstream table/view: {}",
                leftover_tables
                    .keys()
                    .map(|r| r.display(Dialect::PostgreSQL))
                    .join(", ")
            );
            for rel in leftover_tables.keys() {
                if let Err(error) = self
                    .noria
                    .extend_recipe_no_leader_ready(ChangeList::from_change(
                        Change::Drop {
                            name: rel.clone(),
                            if_exists: false,
                        },
                        DataDialect::DEFAULT_POSTGRESQL,
                    ))
                    .await
                {
                    warn!(
                        %error,
                        table = %rel.display(Dialect::PostgreSQL),
                        "Error removing leftover cache table"
                    );
                }
            }
        }
        Ok(())
    }

    async fn set_replica_identity_for_tables(
        &self,
        table_list: &[TableEntry],
    ) -> ReadySetResult<()> {
        let tables_needing_replica_identity = get_transaction!(self)
            .query(
                // Find all tables that are in the table list, and don't already have a primary key
                // or a non-default replica identity set
                "select n.nspname as schema, c.relname as name from pg_class c
                 join pg_namespace n
                 on n.oid = c.relnamespace
                 where c.oid not in (select indrelid from pg_index where indisprimary)
                 and c.relreplident = 'd'
                 and c.oid = any ($1::oid[])",
                &[&table_list.iter().map(|t| t.oid).collect::<Vec<_>>()],
            )
            .await?;

        for table_row in tables_needing_replica_identity {
            let schema: String = table_row.get("schema");
            let name: String = table_row.get("name");
            trace!(%schema, %name, "Setting REPLICA IDENTITY FULL for table");
            get_transaction!(self)
                .execute(
                    &format!(r#"ALTER TABLE "{schema}"."{name}" REPLICA IDENTITY FULL"#),
                    &[],
                )
                .await?;
        }

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
            oid: 32541,
            name: Relation {
                schema: Some("public".into()),
                name: "ar_internal_metadata".into(),
            },
            columns: vec![
                ColumnEntry {
                    attnum: 0,
                    name: "key".into(),
                    sql_type: "varchar".into(),
                    not_null: true,
                    pg_type: Type::VARCHAR,
                },
                ColumnEntry {
                    attnum: 1,
                    name: "value".into(),
                    sql_type: "varchar".into(),
                    not_null: false,
                    pg_type: Type::VARCHAR,
                },
                ColumnEntry {
                    attnum: 2,
                    name: "created_at".into(),
                    sql_type: "timestamp(6) without time zone".into(),
                    not_null: true,
                    pg_type: Type::TIMESTAMP,
                },
                ColumnEntry {
                    attnum: 3,
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
        let res = parse_query(Dialect::PostgreSQL, desc.to_string());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let create_table = match res.unwrap() {
            SqlQuery::CreateTable(ct) => ct,
            _ => panic!(),
        };

        assert_eq!(create_table.table.name, "ar_internal_metadata");
        assert_eq!(create_table.body.as_ref().unwrap().fields.len(), 4);
        assert!(create_table.body.as_ref().unwrap().keys.is_some());
        assert_eq!(
            create_table
                .body
                .as_ref()
                .unwrap()
                .keys
                .as_ref()
                .unwrap()
                .len(),
            1
        );
        let pkey = create_table
            .body
            .as_ref()
            .unwrap()
            .keys
            .as_ref()
            .unwrap()
            .first()
            .unwrap();
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
