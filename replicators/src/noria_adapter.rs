use std::collections::{hash_map, HashMap, HashSet};
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use database_utils::{DatabaseURL, UpstreamConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use failpoint_macros::set_failpoint;
use futures::FutureExt;
use metrics::{counter, histogram};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, PoolConstraints, PoolOpts, SslOpts};
use nom_sql::Relation;
use postgres_native_tls::MakeTlsConnector;
use readyset_client::consistency::Timestamp;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::metrics::recorded::{self, SnapshotStatusTag};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::replication::{ReplicationOffset, ReplicationOffsets};
use readyset_client::{ReadySetHandle, Table, TableOperation};
use readyset_data::Dialect;
use readyset_errors::{
    internal_err, invalid_err, set_failpoint_return_err, ReadySetError, ReadySetResult,
};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
use readyset_util::select;
use tokio::sync::Notify;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::db_util::{CreateSchema, DatabaseSchemas};
use crate::mysql_connector::{MySqlBinlogConnector, MySqlReplicator};
use crate::postgres_connector::{
    drop_publication, drop_readyset_schema, drop_replication_slot, PostgresReplicator,
    PostgresWalConnector, PUBLICATION_NAME, REPLICATION_SLOT,
};
use crate::table_filter::TableFilter;

/// Time to wait for requests to coalesce between snapshotting. Useful for preventing a series of
/// DDL changes from thrashing snapshotting
const WAIT_BEFORE_RESNAPSHOT: Duration = Duration::from_secs(3);

const RESNAPSHOT_SLOT: &str = "readyset_resnapshot";

#[derive(Debug)]
pub(crate) enum ReplicationAction {
    TableAction {
        table: Relation,
        actions: Vec<TableOperation>,
        /// The transaction id of a table write operation. Each
        /// table write operation within a transaction should be assigned
        /// the same transaction id. These id's should be monotonically
        /// increasing across transactions.
        txid: Option<u64>,
    },
    DdlChange {
        schema: String,
        changes: Vec<Change>,
    },
    LogPosition,
}

#[async_trait]
pub(crate) trait Connector {
    /// Process logical replication events until an actionable event occurs, returning
    /// the corresponding action.
    ///
    /// # Arguments
    ///
    /// * `last_pos` - the last processed position to. This is used only by Postgres to
    /// advance the replication slot position on the server.
    ///
    /// * `until` - an optional position in the binlog to stop at, even if no actionable
    /// occured. In that case the action [`ReplicationAction::LogPosition`] is returned.
    /// Currently this is only used by MySQL replicator while catching up on replication.
    async fn next_action(
        &mut self,
        last_pos: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)>;
}

/// Cleans up replication related assets on the upstream database as supplied by the
/// UpstreamConfig.
pub async fn cleanup(config: UpstreamConfig) -> ReadySetResult<()> {
    if let DatabaseURL::PostgreSQL(options) = config
        .upstream_db_url
        .as_ref()
        .ok_or_else(|| internal_err!("Replication URL not supplied"))?
        .parse()
        .map_err(|e| invalid_err!("Invalid URL supplied to --upstream-db-url: {e}"))?
    {
        let connector = {
            let mut builder = native_tls::TlsConnector::builder();
            if config.disable_upstream_ssl_verification {
                builder.danger_accept_invalid_certs(true);
            }
            if let Some(root_cert) = config.get_root_cert().await {
                builder.add_root_certificate(root_cert?);
            }
            builder.build().unwrap() // Never returns an error
        };
        let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);

        let repl_slot_name = match &config.replication_server_id {
            Some(server_id) => {
                format!("{}_{}", REPLICATION_SLOT, server_id)
            }
            _ => REPLICATION_SLOT.to_string(),
        };

        let dbname = options.get_dbname().ok_or_else(|| {
            ReadySetError::ReplicationFailed("No database specified for replication".to_string())
        })?;

        let mut cleanup_opts = options.clone();

        cleanup_opts
            .dbname(dbname.as_ref())
            .set_replication_database();
        let (mut client, connection) = cleanup_opts.connect(tls_connector).await?;
        let _connection_handle = tokio::spawn(connection);

        drop_publication(&mut client, &repl_slot_name).await?;

        drop_replication_slot(&mut client, &repl_slot_name).await?;

        drop_readyset_schema(&mut client).await?;
    }

    Ok(())
}

/// An adapter that converts database events into ReadySet API calls
pub struct NoriaAdapter {
    /// The ReadySet API handle
    noria: ReadySetHandle,
    /// The binlog reader
    connector: Box<dyn Connector + Send + Sync>,
    /// The SQL dialect to pass to ReadySet when applying DDL changes
    dialect: Dialect,
    /// A map of cached table mutators
    mutator_map: HashMap<Relation, Option<Table>>,
    /// A HashSet of tables we've already warned about not existing
    warned_missing_tables: HashSet<Relation>,
    /// The set of replication offsets for the schema and the tables, obtained from the controller
    /// at startup and maintained during replication.
    ///
    /// Since some tables may have lagged behind others due to failed snapshotting, we start
    /// replication at the *minimum* replication offset, but ignore any replication events that
    /// come before the offset for that table
    replication_offsets: ReplicationOffsets,
    /// Filters out changes we are not interested in
    table_filter: TableFilter,
    /// If the connector can partially resnapshot a database
    supports_resnapshot: bool,
}

impl NoriaAdapter {
    pub async fn start(
        noria: ReadySetHandle,
        mut config: UpstreamConfig,
        mut notify: Option<Arc<Notify>>,
        telemetry_sender: TelemetrySender,
        server_startup: bool,
        enable_statement_logging: bool,
    ) -> ReadySetResult<!> {
        // Resnapshot when restarting the server to apply changes that may have been made to the
        // replication-tables config parameter.
        let mut resnapshot = server_startup;
        let url: DatabaseURL = config
            .upstream_db_url
            .take()
            .ok_or_else(|| internal_err!("Replication URL not supplied"))?
            .parse()
            .map_err(|e| invalid_err!("Invalid URL supplied to --upstream-db-url: {e}"))?;

        while let Err(err) = match url.clone() {
            DatabaseURL::MySQL(options) => {
                let noria = noria.clone();
                let config = config.clone();
                NoriaAdapter::start_inner_mysql(
                    options,
                    noria,
                    config,
                    &mut notify,
                    resnapshot,
                    &telemetry_sender,
                    enable_statement_logging,
                )
                .await
            }
            DatabaseURL::PostgreSQL(options) => {
                let noria = noria.clone();
                let config = config.clone();
                let connector = {
                    let mut builder = native_tls::TlsConnector::builder();
                    if config.disable_upstream_ssl_verification {
                        builder.danger_accept_invalid_certs(true);
                    }
                    if let Some(root_cert) = config.get_root_cert().await {
                        builder.add_root_certificate(root_cert?);
                    }
                    builder.build().unwrap() // Never returns an error
                };
                let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);
                let pool = pg_pool(
                    options.clone(),
                    config.replication_pool_size,
                    tls_connector.clone(),
                )
                .await?;

                let repl_slot_name = match &config.replication_server_id {
                    Some(server_id) => {
                        format!("{}_{}", REPLICATION_SLOT, server_id)
                    }
                    _ => REPLICATION_SLOT.to_string(),
                };

                NoriaAdapter::start_inner_postgres(
                    options,
                    noria,
                    config,
                    &mut notify,
                    resnapshot,
                    &telemetry_sender,
                    tls_connector,
                    pool,
                    repl_slot_name,
                    enable_statement_logging,
                )
                .await
            }
        } {
            match err {
                ReadySetError::ResnapshotNeeded => {
                    tokio::time::sleep(WAIT_BEFORE_RESNAPSHOT).await;
                    resnapshot = true;
                }
                err => {
                    warn!(error=%err, "Restarting adapter after error encountered");
                    return Err(err);
                }
            }
        }
        unreachable!("inner loop will never stop with an Ok status");
    }

    /// Finish the build and begin monitoring the binlog for changes
    /// If noria has no replication offset information, it will replicate the target database in its
    /// entirety to ReadySet before listening on the binlog
    /// The replication happens in stages:
    /// * READ LOCK is acquired on the database
    /// * Next binlog position is read
    /// * The recipe (schema) DDL is replicated and installed in ReadySet (replacing current recipe)
    /// * Each table is individually replicated into ReadySet
    /// * READ LOCK is released
    /// * Adapter keeps reading binlog from the next position keeping ReadySet up to date
    async fn start_inner_mysql(
        mut mysql_options: mysql::Opts,
        mut noria: ReadySetHandle,
        mut config: UpstreamConfig,
        ready_notify: &mut Option<Arc<Notify>>,
        resnapshot: bool,
        telemetry_sender: &TelemetrySender,
        enable_statement_logging: bool,
    ) -> ReadySetResult<!> {
        use crate::mysql_connector::BinlogPosition;

        if let Some(cert_path) = config.ssl_root_cert.clone() {
            let ssl_opts = SslOpts::default().with_root_cert_path(Some(cert_path));
            mysql_options = OptsBuilder::from_opts(mysql_options)
                .ssl_opts(ssl_opts)
                .into();
        }

        // Load the replication offset for all tables and the schema from ReadySet
        let mut replication_offsets = noria.replication_offsets().await?;

        let table_filter = TableFilter::try_new(
            nom_sql::Dialect::MySQL,
            config.replication_tables.take(),
            mysql_options.db_name(),
        )?;

        let mut db_schemas = DatabaseSchemas::new();

        let pos = match (replication_offsets.max_offset()?, resnapshot) {
            (None, _) | (_, true) => {
                let span = info_span!("taking database snapshot");
                // The default min is already 10, so we keep that the same to reduce complexity
                // overhead of too many flags.
                // The only way PoolConstraints::new() can panic on unwrap is if min is not less
                // than or equal to max, so we naively reset min if max is below 10.
                let constraints = if config.replication_pool_size <= 10 {
                    PoolConstraints::new(config.replication_pool_size, config.replication_pool_size)
                        .unwrap()
                } else {
                    PoolConstraints::new(10, config.replication_pool_size).unwrap()
                };
                let pool_opts = PoolOpts::default().with_constraints(constraints);
                let replicator_opts: mysql_async::Opts =
                    OptsBuilder::from_opts(mysql_options.clone())
                        .pool_opts(pool_opts)
                        .into();
                let pool = mysql::Pool::new(replicator_opts);

                // Query mysql server version
                let db_version = pool
                    .get_conn()
                    .await?
                    .query_first("SELECT @@version")
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| "unknown".to_owned());

                let replicator = MySqlReplicator {
                    pool,
                    table_filter: table_filter.clone(),
                };

                let snapshot_start = Instant::now();
                counter!(
                    recorded::REPLICATOR_SNAPSHOT_STATUS,
                    1u64,
                    "status" => SnapshotStatusTag::Started.value(),
                );

                span.in_scope(|| info!("Starting snapshot"));
                let snapshot_result = replicator
                    .snapshot_to_noria(
                        &mut noria,
                        &mut db_schemas,
                        config.snapshot_report_interval_secs,
                    )
                    .instrument(span.clone())
                    .await;

                let status = if snapshot_result.is_err() {
                    SnapshotStatusTag::Failed.value()
                } else {
                    SnapshotStatusTag::Successful.value()
                };

                counter!(
                    recorded::REPLICATOR_SNAPSHOT_STATUS,
                    1u64,
                    "status" => status
                );

                snapshot_result?;

                // Get updated offests, after potential replication happened
                replication_offsets = noria.replication_offsets().await?;

                // If we have some offsets in `replication_offsets`, that means some tables were
                // already snapshot before we started up. But if we're in this block
                // (`max_offsets()` returned `None`), that means not *all* tables were already
                // snapshot before we started up. So we've got some tables at an old offset that
                // need to catch up to the just-snapshotted tables. We discard
                // replication events for offsets < the replication offset of that table, so we
                // can do this "catching up" by just starting replication at
                // the old offset. Note that at the very least we will
                // always have the schema offset for the minimum.
                let pos: BinlogPosition = replication_offsets
                    .min_present_offset()?
                    .expect("Minimal offset must be present after snapshot")
                    .clone()
                    .into();

                span.in_scope(|| info!("Snapshot finished"));
                histogram!(
                    recorded::REPLICATOR_SNAPSHOT_DURATION,
                    snapshot_start.elapsed().as_micros() as f64
                );

                // Send snapshot complete and redacted schemas telemetry events
                let _ = telemetry_sender.send_event_with_payload(
                    TelemetryEvent::SnapshotComplete,
                    TelemetryBuilder::new()
                        .db_backend("mysql")
                        .db_version(db_version)
                        .build(),
                );
                db_schemas.send_schemas(telemetry_sender).await;

                pos
            }
            (Some(pos), _) => pos.clone().into(),
        };

        // TODO: it is possible that the binlog position from noria is no longer
        // present on the primary, in which case the connection will fail, and we would
        // need to perform a new snapshot
        let connector = Box::new(
            MySqlBinlogConnector::connect(
                mysql_options.clone(),
                pos.clone(),
                config.replication_server_id,
                enable_statement_logging,
            )
            .await?,
        );

        let mut adapter = NoriaAdapter {
            noria: noria.clone(),
            connector,
            replication_offsets,
            mutator_map: HashMap::new(),
            warned_missing_tables: HashSet::new(),
            table_filter,
            supports_resnapshot: true,
            dialect: Dialect::DEFAULT_MYSQL,
        };

        let mut current_pos: ReplicationOffset = pos.try_into()?;

        // At this point it is possible that we just finished replication, but
        // our schema and our tables are taken at different position in the binlog.
        // Until our database has a consistent view of the database at a single point
        // in time, it is not safe to issue any queries. We therefore advance the binlog
        // to the position of the most recent table we have, applying changes as needed.
        // Only once binlog advanced to that point, can we send a ready signal to
        // ReadySet.
        match adapter.replication_offsets.max_offset()? {
            Some(max) if max > &current_pos => {
                info!(start = %current_pos, end = %max, "Catching up");
                let max = max.clone();
                adapter.main_loop(&mut current_pos, Some(max)).await?;
            }
            _ => {}
        }

        info!("MySQL connected");
        info!(binlog_position = %current_pos);

        // Let waiters know that the initial snapshotting is complete.
        if let Some(notify) = ready_notify.take() {
            notify.notify_one();
        }

        adapter.main_loop(&mut current_pos, None).await?;

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_inner_postgres(
        pgsql_opts: pgsql::Config,
        mut noria: ReadySetHandle,
        mut config: UpstreamConfig,
        ready_notify: &mut Option<Arc<Notify>>,
        resnapshot: bool,
        telemetry_sender: &TelemetrySender,
        tls_connector: MakeTlsConnector,
        pool: deadpool_postgres::Pool,
        repl_slot_name: String,
        enable_statement_logging: bool,
    ) -> ReadySetResult<!> {
        macro_rules! handle_joinhandle_result {
            ($res: expr) => {
                match $res {
                    Ok(Ok(_)) => Err(ReadySetError::UpstreamConnectionLost(
                        "connection closed for unknown reason".to_owned(),
                    )),
                    Ok(Err(e)) => Err(ReadySetError::UpstreamConnectionLost(e.to_string())),
                    Err(e) => Err(ReadySetError::UpstreamConnectionLost(e.to_string())),
                }
            };
        }

        let dbname = pgsql_opts.get_dbname().ok_or_else(|| {
            ReadySetError::ReplicationFailed("No database specified for replication".to_string())
        })?;

        // Attempt to retrieve the latest replication offset from ReadySet-server, if none is
        // present begin the snapshot process
        let replication_offsets = noria.replication_offsets().await?;
        let pos = replication_offsets.max_offset()?.map(Into::into);
        let snapshot_report_interval_secs = config.snapshot_report_interval_secs;

        let table_filter = TableFilter::try_new(
            nom_sql::Dialect::PostgreSQL,
            config.replication_tables.take(),
            None,
        )?;

        // For Postgres 13, once we setup ddl replication, the following query can be rejected, so
        // run it ahead of time.
        // TODO: (luke): We can probably consolidate this query with the db_version string query
        // below
        let version_num: u32 = {
            let (client, connection) = pgsql_opts.connect(tls_connector.clone()).await?;
            let connection_handle = tokio::spawn(connection);

            select! {
                result = client.query_one("SHOW server_version_num", &[]) => result
                    .and_then(|row| row.try_get::<_, String>(0))
                    .map_err(|e| {
                        ReadySetError::Internal(format!("Unable to determine postgres version: {}", e))
                    })?
                    .parse()
                    .map_err(|e| {
                        ReadySetError::Internal(format!("Unable to parse postgres version: {}", e))
                    })?,
                c = connection_handle.fuse() => return handle_joinhandle_result!(c),
            }
        };

        let mut connector = Box::new(
            PostgresWalConnector::connect(
                pgsql_opts.clone(),
                dbname,
                config,
                pos,
                tls_connector.clone(),
                &repl_slot_name,
                enable_statement_logging,
            )
            .await?,
        );

        info!("Connected to PostgreSQL");

        let resnapshot_slot_name = format!("{}_{}", RESNAPSHOT_SLOT, repl_slot_name);
        let replication_slot = if let Some(slot) = &connector.replication_slot {
            Some(slot.clone())
        } else if resnapshot || pos.is_none() {
            // This is not an initial connection but we need to resnapshot the latest schema,
            // therefore we create a new replication slot, just so we can get a consistent snapshot
            // with a WAL position attached. This is more robust than locking and allows us to reuse
            // the existing snapshotting code.
            info!(
                slot = resnapshot_slot_name,
                "Recreating replication slot to resnapshot with the latest schema"
            );
            connector
                .drop_replication_slot(&resnapshot_slot_name)
                .await?;
            Some(
                connector
                    .create_replication_slot(&resnapshot_slot_name, true)
                    .await?,
            )
        } else {
            None
        };

        let mut create_schema = CreateSchema::new(dbname.to_string(), nom_sql::Dialect::PostgreSQL);

        if let Some(replication_slot) = replication_slot {
            let snapshot_start = Instant::now();
            // If snapshot name exists, it means we need to make a snapshot to noria

            let (mut client, connection) = pgsql_opts.connect(tls_connector.clone()).await?;

            let connection_handle = tokio::spawn(connection);
            let db_version = client
                .query_one("SELECT version()", &[])
                .await
                .and_then(|row| row.try_get::<_, String>(0))
                .unwrap_or_else(|_| "unknown".to_owned());

            let mut replicator =
                PostgresReplicator::new(&mut client, pool, &mut noria, table_filter.clone())
                    .await?;

            select! {
                snapshot_result = replicator.snapshot_to_noria(
                    &replication_slot,
                    &mut create_schema,
                    snapshot_report_interval_secs,
                    // If we don't have a consistent replication offset from ReadySet, that might be
                    // because only *some* tables are missing a replication offset - in that case we
                    // need to resnapshot *all* tables, because we just dropped the replication slot
                    // above, which prevents us from replicating any writes to tables we do have a
                    // replication offset for that happened while we weren't running.
                    /* full_snapshot = */ pos.is_none()
                ).fuse() =>  {
                    let status = if snapshot_result.is_err() {
                        SnapshotStatusTag::Failed.value()
                    } else {
                        SnapshotStatusTag::Successful.value()
                    };

                    counter!(
                        recorded::REPLICATOR_SNAPSHOT_STATUS,
                        1u64,
                        "status" => status
                    );

                    snapshot_result?;
                },
                c = connection_handle.fuse() => return handle_joinhandle_result!(c),
            }

            info!("Snapshot finished");
            histogram!(
                recorded::REPLICATOR_SNAPSHOT_DURATION,
                snapshot_start.elapsed().as_micros() as f64
            );

            if replication_slot.slot_name == resnapshot_slot_name {
                connector
                    .drop_replication_slot(&resnapshot_slot_name)
                    .await?;
            }

            let _ = telemetry_sender.send_event_with_payload(
                TelemetryEvent::SnapshotComplete,
                TelemetryBuilder::new()
                    .db_backend("psql")
                    .db_version(db_version)
                    .build(),
            );
            create_schema.send_schemas(telemetry_sender).await;
        }

        connector
            .start_replication(&repl_slot_name, PUBLICATION_NAME, version_num)
            .await?;

        let replication_offsets = noria.replication_offsets().await?;
        trace!(?replication_offsets, "Loaded replication offsets");
        let mut min_pos = replication_offsets
            .min_present_offset()?
            .expect("Minimal offset must be present after snapshot")
            .clone();
        let max_pos = replication_offsets
            .max_offset()?
            .expect("Maximum offset must be present after snapshot")
            .clone();

        let mut adapter = NoriaAdapter {
            noria,
            connector,
            replication_offsets,
            mutator_map: HashMap::new(),
            warned_missing_tables: HashSet::new(),
            table_filter,
            supports_resnapshot: true,
            dialect: Dialect::DEFAULT_POSTGRESQL,
        };

        if min_pos != max_pos {
            info!(start = %min_pos, end = %max_pos, "Catching up");
            adapter.main_loop(&mut min_pos, Some(max_pos)).await?;
        }

        // Let waiters know that the initial snapshotting is complete.
        if let Some(notify) = ready_notify.take() {
            notify.notify_one();
        }

        info!("Streaming replication started");

        adapter.main_loop(&mut min_pos, None).await?;

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    /// Apply a DDL string to noria with the current log position
    async fn handle_ddl_change(
        &mut self,
        schema: String,
        changes: Vec<Change>,
        pos: ReplicationOffset,
    ) -> ReadySetResult<()> {
        let mut changelist = ChangeList::from_changes(changes, self.dialect);

        // Remove DDL changes outside the filtered scope
        let mut non_replicated_tables = vec![];
        changelist.changes_mut().retain(|change| match change {
            Change::CreateTable(stmt) => {
                let keep = self
                    .table_filter
                    .should_be_processed(schema.as_str(), stmt.table.name.as_str())
                    && stmt.body.is_ok();
                if !keep {
                    non_replicated_tables.push(Relation {
                        schema: Some(schema.clone().into()),
                        name: stmt.table.name.clone(),
                    })
                }
                keep
            }
            Change::AlterTable(stmt) => self
                .table_filter
                .should_be_processed(schema.as_str(), stmt.table.name.as_str()),
            _ => true,
        });

        // Mark all tables that were filtered as non-replicated, too
        changelist.changes_mut().extend(
            non_replicated_tables
                .into_iter()
                .map(Change::AddNonReplicatedRelation),
        );

        if self.supports_resnapshot && changelist.changes().any(Change::requires_resnapshot) {
            // In case we detect a DDL change that requires a full schema resnapshot exit the loop
            // with the proper status
            if let Some(pos) = self.replication_offsets.max_offset()?.cloned() {
                // Forward all positions to the maximum position (the one prior to this statement)
                // to avoid needless replay later
                self.handle_log_position(pos).await?;
            }
            return Err(ReadySetError::ResnapshotNeeded);
        }

        changelist = changelist.with_schema_search_path(vec![schema.into()]);

        // Collect a list of all tables we're creating for later
        let tables = changelist
            .changes()
            .filter_map(|change| match change {
                Change::CreateTable(t) => Some(t.table.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        match self
            .noria
            .extend_recipe_with_offset(changelist.clone(), &pos, false)
            .await
        {
            // ReadySet likely entered an invalid state, fail the replicator.
            Err(e @ ReadySetError::RecipeInvariantViolated(_)) => return Err(e),
            Err(error) => {
                warn!(%error, "Error extending recipe, DDL statement will not be used");
                counter!(recorded::REPLICATOR_FAILURE, 1u64,);

                let changes = mem::take(changelist.changes_mut());
                // If something went wrong, mark all the tables and views that we just tried to
                // create as non-replicated
                changelist
                    .changes_mut()
                    .extend(changes.into_iter().filter_map(|change| {
                        Some(Change::AddNonReplicatedRelation(match change {
                            Change::CreateTable(stmt) => stmt.table,
                            Change::CreateView(stmt) => stmt.name,
                            Change::AddNonReplicatedRelation(rel) => rel,
                            _ => return None,
                        }))
                    }));
                self.noria.extend_recipe(changelist).await?;
            }
            Ok(_) => {}
        }

        self.replication_offsets.schema = Some(pos.clone());
        self.clear_mutator_cache();

        // Set the replication offset for each table we just created to this replication offset
        // (since otherwise they'll get initialized without an offset)
        for table in &tables {
            self.replication_offsets
                .tables
                .insert(table.clone(), Some(pos.clone()));
            if let Some(mutator) = self.mutator_for_table(table).await? {
                mutator.set_replication_offset(pos.clone()).await?;
            } else {
                warn!(
                    table = %table.display_unquoted(),
                    "Just-created table missing replication offset"
                )
            }
        }

        Ok(())
    }

    /// Update the log position of the schema and the tables
    async fn handle_log_position(&mut self, pos: ReplicationOffset) -> ReadySetResult<()> {
        // Update the log position for the schema
        debug!(%pos, "Setting schema replication offset");
        self.noria.set_schema_replication_offset(Some(&pos)).await?;

        // Update the log position for the tables that are behind this offset
        let tables = self
            .replication_offsets
            .tables
            .iter()
            .filter_map(|(k, v)| match v {
                None => Some(k),
                Some(cur_offset) if *cur_offset < pos => Some(k),
                Some(_) => None,
            })
            .cloned()
            .collect::<Vec<_>>();

        for table in tables {
            if let Some(table) = self.mutator_for_table(&table).await? {
                table.set_replication_offset(pos.clone()).await?;
            }
        }

        self.replication_offsets.advance_offset(pos.clone())?;

        Ok(())
    }

    /// Send table actions to noria tables, and update the binlog position for the table
    async fn handle_table_actions(
        &mut self,
        table: Relation,
        mut actions: Vec<TableOperation>,
        txid: Option<u64>,
        pos: ReplicationOffset,
    ) -> ReadySetResult<()> {
        // Send the rows as are
        let table_mutator = if let Some(table) = self.mutator_for_table(&table).await? {
            table
        } else {
            // The only error we are semi "ok" to ignore for table actions is when a table is not
            // found. Failing to execute an action for an existing table may very well get noria
            // into an inconsistent state. This may happen if eg. a worker fails.
            // This is Ok, since replicator task will reconnect again and retry the action as many
            // times as needed for it to succeed, but it is not safe to continue past this point on
            // a failure.
            if self.warned_missing_tables.insert(table.clone()) {
                warn!(
                    table_name = %table.display(nom_sql::Dialect::PostgreSQL),
                    num_actions = actions.len(),
                    "Could not find table, discarding actions"
                );
            }
            return Ok(());
        };
        actions.push(TableOperation::SetReplicationOffset(pos.clone()));
        table_mutator.perform_all(actions).await?;

        // If there was a transaction id associated, propagate the timestamp with that transaction
        // id.
        // TODO(justin): Make this operation atomic with the table actions being pushed above.
        // TODO(vlad): We have to propagate txid to every table or else we won't be able to ensure
        // proper read after write
        if let Some(tx) = txid {
            let mut timestamp = Timestamp::default();
            timestamp.map.insert(table_mutator.node, tx);
            table_mutator.update_timestamp(timestamp).await?;
        }

        self.replication_offsets.tables.insert(table, Some(pos));

        Ok(())
    }

    /// Handle a single BinlogAction by calling the proper ReadySet RPC. If `catchup` is set,
    /// we will not log warnings for skipping entries, as we may iterate over many entries tables
    /// have already seen when catching each table up to the current binlog offset.
    async fn handle_action(
        &mut self,
        action: ReplicationAction,
        pos: ReplicationOffset,
        catchup: bool,
    ) -> ReadySetResult<()> {
        set_failpoint_return_err!(failpoints::REPLICATION_HANDLE_ACTION);
        // First check if we should skip this action due to insufficient log position or lack of
        // interest
        match &action {
            ReplicationAction::DdlChange { .. } | ReplicationAction::LogPosition => {
                match &self.replication_offsets.schema {
                    Some(cur) if pos <= *cur => {
                        if !catchup {
                            warn!(%pos, %cur, "Skipping schema update for earlier entry");
                        }
                        return Ok(());
                    }
                    _ => {}
                }
            }
            ReplicationAction::TableAction { table, .. } => {
                match self.replication_offsets.tables.get(table) {
                    Some(Some(cur)) if pos <= *cur => {
                        if !catchup {
                            warn!(
                                table = %table.display_unquoted(),
                                %pos,
                                %cur,
                                "Skipping table action for earlier entry"
                            );
                        }
                        return Ok(());
                    }
                    Some(Some(cur)) => {
                        trace!(table = %table.display_unquoted(), %cur);
                    }
                    _ => {
                        trace!(
                            table = %table.display_unquoted(),
                            "no replication offset for table"
                        );
                    }
                }

                if !self.table_filter.should_be_processed(
                    table.schema.as_deref().ok_or_else(|| {
                        internal_err!("All tables should have a schema in the replicator")
                    })?,
                    &table.name,
                ) {
                    return Ok(());
                }
            }
        }

        match action {
            ReplicationAction::DdlChange { schema, changes } => {
                self.handle_ddl_change(schema, changes, pos).await
            }
            ReplicationAction::TableAction {
                table,
                actions,
                txid,
            } => self.handle_table_actions(table, actions, txid, pos).await,
            ReplicationAction::LogPosition => self.handle_log_position(pos).await,
        }
    }

    /// Loop over the actions. `until` may be passed to set a replication offset to stop
    /// replicating at.
    async fn main_loop(
        &mut self,
        position: &mut ReplicationOffset,
        until: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        loop {
            set_failpoint!(failpoints::UPSTREAM, |_| ReadySetResult::Err(
                ReadySetError::ReplicationFailed(
                    "replication-upstream failpoint injected".to_string()
                )
            ));

            if until.as_ref().map(|u| *position >= *u).unwrap_or(false) {
                return Ok(());
            }

            let (action, pos) = match self.connector.next_action(position, until.as_ref()).await {
                Ok(next_action) => next_action,
                // In some cases, we may fail to replicate because of unsupported operations, stop
                // replicating a table if we encounter this type of error.
                Err(ReadySetError::TableError { table, source }) => {
                    self.deny_replication_for_table(table, source).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            *position = pos.clone();
            debug!(%position, "Received replication action");

            trace!(?action);

            if let Err(err) = self.handle_action(action, pos, until.is_some()).await {
                if matches!(err, ReadySetError::ResnapshotNeeded) {
                    info!("Change in DDL requires partial resnapshot");
                } else {
                    error!(error = %err, "Aborting replication task on error");
                    counter!(recorded::REPLICATOR_FAILURE, 1u64,);
                }
                // In some cases, we may fail to replicate because of unsupported operations, stop
                // replicating a table if we encounter this type of error.
                if let ReadySetError::TableError { table, source } = err {
                    self.deny_replication_for_table(table, source).await?;
                    continue;
                }

                error!(error = %err, "Aborting replication task on error");
                counter!(recorded::REPLICATOR_FAILURE, 1u64,);
                return Err(err);
            };
            counter!(recorded::REPLICATOR_SUCCESS, 1u64);
            debug!(%position, "Successfully applied replication action");
        }
    }

    /// When schema changes there is a risk the cached mutators will no longer be in sync
    /// and we need to drop them all
    fn clear_mutator_cache(&mut self) {
        self.mutator_map.clear()
    }

    /// Get a mutator for a noria table from the cache if available, or fetch a new one
    /// from the controller and cache it. Returns None if the table doesn't exist in noria.
    async fn mutator_for_table(&mut self, name: &Relation) -> ReadySetResult<Option<&mut Table>> {
        match self.mutator_map.raw_entry_mut().from_key(name) {
            hash_map::RawEntryMut::Occupied(o) => Ok(o.into_mut().as_mut()),
            hash_map::RawEntryMut::Vacant(v) => match self.noria.table(name.clone()).await {
                Ok(table) => Ok(v.insert(name.clone(), Some(table)).1.as_mut()),
                Err(e) if e.caused_by_table_not_found() => {
                    // Cache the not found result as well as the found result
                    Ok(v.insert(name.clone(), None).1.as_mut())
                }
                Err(e) => Err(e),
            },
        }
    }

    /// Remove the table referenced by the provided schema and table name from our base table and
    /// dataflow state (if any).
    async fn remove_table_from_readyset(&mut self, table: Relation) -> ReadySetResult<()> {
        info!(
            table = %table.display(nom_sql::Dialect::PostgreSQL),
            "Removing table state from readyset"
        );
        self.replication_offsets.tables.remove(&table);
        self.mutator_map.remove(&table);
        // Dropping the table cleans up any dataflow state that may have been made as well as
        // cleaning up the base table on disk.
        let changelist = ChangeList::from_changes(
            vec![
                Change::Drop {
                    name: table.clone(),
                    if_exists: true,
                },
                Change::AddNonReplicatedRelation(table),
            ],
            self.dialect,
        );

        self.noria.extend_recipe(changelist).await?;
        Ok(())
    }

    /// Stops replicating the given table. This is used to abandon replication for a single table
    /// in the event of an error that won't prevent other tables from successfully replicating.
    async fn deny_replication_for_table(
        &mut self,
        table: Relation,
        source: Box<ReadySetError>,
    ) -> ReadySetResult<()> {
        // This is an error log because we don't know of any operations that will cause
        // this currently--if we see this, we should investigate and fix the issue
        // causing a table to not be supported by our replication
        error!(
            table = %table.display(nom_sql::Dialect::PostgreSQL),
            error = %source,
            "Will stop replicating a table due to table error"
        );

        let schema = table.schema.clone().ok_or_else(|| {
            // Tables should have a schema defined at this point, or something has gone wrong
            // somewhere. If we don't have a schema at this point, we don't know which
            // table encountered an error, so we fall back to resnapshotting.
            error!(
                table = %table.display(nom_sql::Dialect::PostgreSQL),
                "Unable to deny replication for table without schema. Will Re-snapshot"
            );
            ReadySetError::ResnapshotNeeded
        })?;
        let name = table.name.clone();

        // Remove the table and any domain state associated with it, so that it is
        // like we never replicated it in the first place
        // If this fails, fall back to resnapshotting to avoid getting in an
        // inconsistent state. Resnapshotting will end up in this block again since
        // there is an action we can't handle--but we cannot stop replicating without
        // successfully removing the table from readyset--that would lead to permanently
        // stale results.
        set_failpoint_return_err!("ignore-table-fail-dropping-table");
        self.remove_table_from_readyset(table.clone()).await.map_err(|error| {
            error!(%error, "failed to remove ignored table from readyset, will need to resnapshot it to continue");
            ReadySetError::ResnapshotNeeded
        })?;

        self.table_filter
            .deny_replication(schema.as_str(), name.as_str());

        Ok(())
    }
}

pub async fn pg_pool(
    config: pgsql::Config,
    pool_size: usize,
    tls: MakeTlsConnector,
) -> Result<deadpool_postgres::Pool, deadpool_postgres::BuildError> {
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    };
    let mgr = Manager::from_config(config, tls, mgr_config);
    Pool::builder(mgr).max_size(pool_size).build()
}
