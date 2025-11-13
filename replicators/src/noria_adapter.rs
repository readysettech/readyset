use std::collections::{hash_map, HashMap, HashSet};
use std::mem;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::FutureExt;
use metrics::{counter, histogram};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, PoolConstraints, PoolOpts};
use native_tls::Certificate;
use postgres_native_tls::MakeTlsConnector;
use postgres_protocol::escape::escape_literal;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use {mysql_async as mysql, tokio_postgres as pgsql};

use database_utils::tls::{get_mysql_tls_config, get_tls_connector, ServerCertVerification};
use database_utils::{DatabaseURL, UpstreamConfig};
use failpoint_macros::set_failpoint;
use readyset_client::metrics::recorded::{self, SnapshotStatusTag};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::{ReadySetHandle, Table, TableOperation, TableStatus};
use readyset_data::Dialect;
use readyset_errors::{internal_err, set_failpoint_return_err, ReadySetError, ReadySetResult};
use readyset_sql::ast::{NonReplicatedRelation, NotReplicatedReason, Relation};
use readyset_sql::DialectDisplay;
use readyset_sql_parsing::ParsingPreset;
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::{retry_with_exponential_backoff, select};
use replication_offset::{ReplicationOffset, ReplicationOffsets};

use crate::db_util::{CreateSchema, DatabaseSchemas};
use crate::mysql_connector::{MySqlBinlogConnector, MySqlReplicator};
use crate::postgres_connector::{
    drop_publication, drop_readyset_schema, drop_replication_slot, PostgresReplicator,
    PostgresWalConnector, PUBLICATION_NAME, REPLICATION_SLOT,
};
use crate::table_filter::TableFilter;
use crate::{ControllerMessage, ReplicatorMessage};

/// Time to wait for requests to coalesce between snapshotting. Useful for preventing a series of
/// DDL changes from thrashing snapshotting
const WAIT_BEFORE_RESNAPSHOT: Duration = Duration::from_secs(3);

const RESNAPSHOT_SLOT: &str = "readyset_resnapshot";

#[derive(Debug)]
pub(crate) enum ReplicationAction {
    TableAction {
        table: Relation,
        actions: Vec<TableOperation>,
    },
    DdlChange {
        schema: String,
        changes: Vec<Change>,
    },
    LogPosition,
    Empty,
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
    /// occurred. In that case the action [`ReplicationAction::LogPosition`] is returned.
    /// Currently this is only used by MySQL replicator while catching up on replication.
    async fn next_action(
        &mut self,
        last_pos: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(Vec<ReplicationAction>, ReplicationOffset)>;
}

/// Cleans up replication related assets on the upstream database as supplied by the
/// UpstreamConfig.
pub async fn cleanup(config: UpstreamConfig) -> ReadySetResult<()> {
    if let DatabaseURL::PostgreSQL(mut options) = config.get_cdc_db_url()? {
        let repl_slot_name = match &config.replication_server_id {
            Some(server_id) => {
                format!("{REPLICATION_SLOT}_{server_id}")
            }
            _ => REPLICATION_SLOT.to_string(),
        };
        let resnapshot_slot_name = resnapshot_slot_name(&repl_slot_name);

        let dbname = options
            .get_dbname()
            .ok_or_else(|| {
                ReadySetError::ReplicationFailed(
                    "No database specified for replication".to_string(),
                )
            })?
            .to_owned();
        options.dbname(dbname).set_replication_database();

        let verification = ServerCertVerification::from(&config).await?;
        let connector = get_tls_connector(&verification)?;
        let tls = MakeTlsConnector::new(connector);

        let (mut client, connection) = options.connect(tls).await?;
        let _connection_handle = tokio::spawn(connection);

        drop_publication(&mut client, &repl_slot_name).await?;
        // Drop primary replication slot
        drop_replication_slot(&mut client, &repl_slot_name).await?;
        // Drop resnapshot replication slot (if exists, if does not we output debug message)
        drop_replication_slot(&mut client, &resnapshot_slot_name).await?;
        // Drop schema
        drop_readyset_schema(&mut client).await?;
    }

    Ok(())
}

pub fn resnapshot_slot_name(repl_slot_name: &String) -> String {
    format!("{RESNAPSHOT_SLOT}_{repl_slot_name}")
}
/// An adapter that converts database events into ReadySet API calls
pub struct NoriaAdapter<'a> {
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
    table_filter: &'a mut TableFilter,
    /// If the connector can partially resnapshot a database
    supports_resnapshot: bool,
    /// Any TableStatus updates sent here will update this controller's state machine.
    table_status_tx: UnboundedSender<(Relation, TableStatus)>,
}

impl<'a> NoriaAdapter<'a> {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        noria: ReadySetHandle,
        config: &UpstreamConfig,
        url: &DatabaseURL,
        table_filter: &mut TableFilter,
        controller_tx: &UnboundedSender<ControllerMessage>,
        replicator_rx: &mut UnboundedReceiver<ReplicatorMessage>,
        telemetry_sender: TelemetrySender,
        server_startup: bool,
        enable_statement_logging: bool,
        parsing_preset: ParsingPreset,
        table_status_tx: UnboundedSender<(Relation, TableStatus)>,
    ) -> ReadySetResult<std::convert::Infallible> {
        // Resnapshot when restarting the server to apply changes that may have been made to the
        // replication-tables config parameter.
        let mut resnapshot = server_startup;
        let mut full_snapshot = false;
        loop {
            let Err(err) = match url.clone() {
                DatabaseURL::MySQL(options) => {
                    let noria = noria.clone();
                    // Notify controller that we are about to start a snapshot if we are
                    // restarting to resnapshot.
                    if resnapshot {
                        let _ = controller_tx.send(ControllerMessage::SnapshotStarting);
                    }
                    NoriaAdapter::start_inner_mysql(
                        options,
                        noria,
                        config,
                        controller_tx,
                        replicator_rx,
                        resnapshot,
                        &telemetry_sender,
                        enable_statement_logging,
                        full_snapshot,
                        table_filter,
                        parsing_preset,
                        table_status_tx.clone(),
                    )
                    .await
                }
                DatabaseURL::PostgreSQL(options) => {
                    let noria = noria.clone();
                    let connector = {
                        let mut builder = native_tls::TlsConnector::builder();
                        if config.disable_upstream_ssl_verification {
                            builder.danger_accept_invalid_certs(true);
                        }
                        if let Some(certs) = config.get_root_certs().await? {
                            for cert in &certs {
                                builder
                                    .add_root_certificate(Certificate::from_der(cert.contents())?);
                            }
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
                            format!("{REPLICATION_SLOT}_{server_id}")
                        }
                        _ => REPLICATION_SLOT.to_string(),
                    };

                    // Notify controller that we are about to start a snapshot if we are
                    // restarting to resnapshot.
                    if resnapshot || full_snapshot {
                        let _ = controller_tx.send(ControllerMessage::SnapshotStarting);
                    }
                    NoriaAdapter::start_inner_postgres(
                        options,
                        noria,
                        config,
                        controller_tx,
                        replicator_rx,
                        resnapshot,
                        full_snapshot,
                        &telemetry_sender,
                        tls_connector,
                        pool,
                        repl_slot_name,
                        enable_statement_logging,
                        table_filter,
                        parsing_preset,
                        table_status_tx.clone(),
                    )
                    .await
                }
            };

            match err {
                ReadySetError::ResnapshotNeeded => {
                    set_failpoint!(failpoints::POSTGRES_PARTIAL_RESNAPSHOT);
                    tokio::time::sleep(WAIT_BEFORE_RESNAPSHOT).await;
                    resnapshot = true;
                    full_snapshot = false;
                }
                ReadySetError::FullResnapshotNeeded => {
                    set_failpoint!(failpoints::POSTGRES_FULL_RESNAPSHOT);
                    tokio::time::sleep(WAIT_BEFORE_RESNAPSHOT).await;
                    resnapshot = true;
                    full_snapshot = true;
                    warn!(error=%err, "Restarting adapter after error encountered. Full resnapshot will be performed");
                }

                err => {
                    warn!(error=%err, "Restarting adapter after error encountered");
                    return Err(err);
                }
            }
        }
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
    #[allow(clippy::too_many_arguments)]
    async fn start_inner_mysql(
        mysql_options: mysql::Opts,
        mut noria: ReadySetHandle,
        config: &UpstreamConfig,
        controller_tx: &UnboundedSender<ControllerMessage>,
        replicator_rx: &mut UnboundedReceiver<ReplicatorMessage>,
        resnapshot: bool,
        telemetry_sender: &TelemetrySender,
        enable_statement_logging: bool,
        full_snapshot: bool,
        table_filter: &'a mut TableFilter,
        parsing_preset: ParsingPreset,
        table_status_tx: UnboundedSender<(Relation, TableStatus)>,
    ) -> ReadySetResult<std::convert::Infallible> {
        use replication_offset::mysql::MySqlPosition;

        let mut mysql_opts_builder = OptsBuilder::from_opts(mysql_options).prefer_socket(false);

        let ssl_opts = get_mysql_tls_config(&ServerCertVerification::from(config).await?);
        if let Some(ssl_opts) = ssl_opts {
            mysql_opts_builder = mysql_opts_builder.ssl_opts(ssl_opts);
        }

        // Load the replication offset for all tables and the schema from ReadySet
        // Retry a few times to give domains a chance to spin up--if we fail at all attempts, we
        // will start the loop over and there will be an error logged
        let mut replication_offsets = retry_with_exponential_backoff!(
            {
                let mut noria = noria.clone();
                noria.replication_offsets().await
            },
            retries: 5,
            delay: 250,
            backoff: 2,
        )?;

        let pos = match (replication_offsets.min_present_offset()?, resnapshot) {
            (None, _) | (_, true) => {
                let span = info_span!("taking database snapshot");

                let mut db_schemas = DatabaseSchemas::new(parsing_preset);

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
                    mysql_opts_builder.clone()
                        .pool_opts(pool_opts)
                        .setup(vec![
                            "SET SESSION sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
                            "SET SESSION character_set_results=utf8mb4",
                        ])
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
                    table_filter,
                    parsing_preset,
                    snapshot_query_comment: config.snapshot_query_comment.clone(),
                    table_status_tx: table_status_tx.clone(),
                };

                let snapshot_start = Instant::now();
                counter!(
                    recorded::REPLICATOR_SNAPSHOT_STATUS,
                    "status" => SnapshotStatusTag::Started.value(),
                )
                .increment(1u64);

                span.in_scope(|| info!("Starting snapshot"));
                let snapshot_result = replicator
                    .snapshot_to_noria(
                        &mut noria,
                        &mut db_schemas,
                        config.snapshot_report_interval_secs,
                        full_snapshot,
                        config.max_parallel_snapshot_tables(),
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
                    "status" => status
                )
                .increment(1u64);

                snapshot_result?;

                // Get updated offsets, after potential replication happened
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
                let pos: MySqlPosition = replication_offsets
                    .min_present_offset()?
                    .expect("Minimal offset must be present after snapshot")
                    .clone()
                    .try_into()?;

                span.in_scope(|| info!("Snapshot finished"));
                histogram!(recorded::REPLICATOR_SNAPSHOT_DURATION)
                    .record(snapshot_start.elapsed().as_micros() as f64);

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
            (Some(pos), _) => pos.clone().try_into()?,
        };

        let connector = Box::new(
            MySqlBinlogConnector::connect(
                noria.clone(),
                mysql_opts_builder,
                pos.clone(),
                enable_statement_logging,
                table_filter.clone(),
                parsing_preset,
                config,
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
            table_status_tx,
        };

        let mut current_pos: ReplicationOffset = pos.into();

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
                if let Err(err) = adapter
                    .main_loop(&mut current_pos, Some(max), controller_tx, replicator_rx)
                    .await
                {
                    warn!(position = %current_pos, "Replicator stopped during catch-up phase");
                    return Err(err);
                }
            }
            _ => {}
        }

        // Let Controller know that the initial snapshotting is complete. Ignores the error, which
        // will not occur unless the Controller dropped the rx half of this channel.
        let _ = controller_tx.send(ControllerMessage::SnapshotDone);

        info!(position = %current_pos, "Streaming replication started");
        if let Err(err) = adapter
            .main_loop(&mut current_pos, None, controller_tx, replicator_rx)
            .await
        {
            warn!(position = %current_pos, "Replicator stopped during streaming replication");
            return Err(err);
        }

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_inner_postgres(
        pgsql_opts: pgsql::Config,
        mut noria: ReadySetHandle,
        config: &UpstreamConfig,
        controller_tx: &UnboundedSender<ControllerMessage>,
        replicator_rx: &mut UnboundedReceiver<ReplicatorMessage>,
        resnapshot: bool,
        mut full_resnapshot: bool,
        telemetry_sender: &TelemetrySender,
        tls_connector: MakeTlsConnector,
        pool: deadpool_postgres::Pool,
        repl_slot_name: String,
        enable_statement_logging: bool,
        table_filter: &'a mut TableFilter,
        parsing_preset: ParsingPreset,
        table_status_tx: UnboundedSender<(Relation, TableStatus)>,
    ) -> ReadySetResult<std::convert::Infallible> {
        set_failpoint_return_err!(failpoints::START_INNER_POSTGRES);

        let dbname = pgsql_opts.get_dbname().ok_or_else(|| {
            ReadySetError::ReplicationFailed("No database specified for replication".to_string())
        })?;

        // Attempt to retrieve the latest replication offset from ReadySet-server, if none is
        // present begin the snapshot process
        // Retry a few times to give domains a chance to spin up--if we fail at all attempts, we
        // will start the loop over and there will be an error logged
        let replication_offsets = retry_with_exponential_backoff!(
            {
                let mut noria = noria.clone();
                noria.replication_offsets().await
            },
            retries: 5,
            delay: 250,
            backoff: 2,
        )?;

        let pos = replication_offsets
            .min_present_offset()?
            .map(TryInto::try_into)
            .transpose()?;
        let snapshot_report_interval_secs = config.snapshot_report_interval_secs;

        let (mut client, connection) = pgsql_opts.connect(tls_connector.clone()).await?;
        let _connection_handle = tokio::spawn(connection);

        // For Postgres 13, once we setup ddl replication, the following query can be rejected, so
        // run it ahead of time.
        let version_num: u32 = client
            .query_one("SHOW server_version_num", &[])
            .await
            .and_then(|row| row.try_get::<_, String>(0))
            .map_err(|e| {
                ReadySetError::Internal(format!("Unable to determine postgres version: {e}"))
            })?
            .parse()
            .map_err(|e| {
                ReadySetError::Internal(format!("Unable to parse postgres version: {e}"))
            })?;

        let max_parallel_snapshot_tables = config.max_parallel_snapshot_tables();
        let mut connector = Box::new(
            PostgresWalConnector::connect(
                pgsql_opts.clone(),
                dbname,
                config,
                pos,
                tls_connector.clone(),
                &repl_slot_name,
                enable_statement_logging,
                full_resnapshot,
                noria.clone(),
                table_filter.clone(),
                parsing_preset,
            )
            .await?,
        );

        info!("Connected to PostgreSQL");

        let resnapshot_slot_name = resnapshot_slot_name(&repl_slot_name);
        let replication_slot = if let Some(slot) = &connector.replication_slot {
            Some(slot.clone())
        } else {
            let escaped_slot_name = escape_literal(&repl_slot_name);
            let readyset_slot_exists = client
                .query_one(
                    &format!("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name={escaped_slot_name})"),
                    &[],
                )
                .await
                .and_then(|row| row.try_get::<_, bool>(0))
                .map_err(|e| {
                    ReadySetError::Internal(format!(
                        "Unable to read replication slot from upstream database: {e}"
                    ))
                })?;

            if readyset_slot_exists {
                info!(%full_resnapshot, %resnapshot, pos=?pos, "readyset_slot_exists");
                if full_resnapshot || resnapshot || pos.is_none() {
                    // This is not an initial connection but we need to resnapshot the latest
                    // schema, therefore we create a new replication slot, just
                    // so we can get a consistent snapshot with a WAL position
                    // attached. This is more robust than locking and allows us to reuse
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
                }
            } else {
                // If our primary replication slot doesn't already exist, we need to create one,
                // which will require us to do a full resnapshot, since we'd have no way of
                // replaying events between our current max offset and the consistent point of the
                // new slot.
                full_resnapshot = true;

                Some(
                    connector
                        .create_replication_slot(&repl_slot_name, true)
                        .await?,
                )
            }
        };

        let mut create_schema = CreateSchema::new(
            dbname.to_string(),
            parsing_preset,
            readyset_sql::Dialect::PostgreSQL,
        );

        if let Some(replication_slot) = replication_slot {
            set_failpoint!(failpoints::POSTGRES_SNAPSHOT_START);

            let snapshot_start = Instant::now();

            let db_version = client
                .query_one("SELECT version()", &[])
                .await
                .and_then(|row| row.try_get::<_, String>(0))
                .unwrap_or_else(|_| "unknown".to_owned());

            let mut replicator = PostgresReplicator::new(
                &mut client,
                pool,
                &mut noria,
                table_filter,
                parsing_preset,
                table_status_tx.clone(),
            )
            .await?;

            let snapshot_result = replicator
                .snapshot_to_noria(
                    &replication_slot,
                    &mut create_schema,
                    snapshot_report_interval_secs,
                    // If we don't have a consistent replication offset from ReadySet, that might
                    // be because only *some* tables are missing a replication offset - in that
                    // in that case we need to resnapshot *all* tables, because we just dropped the
                    // replication slot above, which prevents us from replicating any writes to
                    //  tables we do have a replication offset for that happened while we weren't
                    //  running.
                    pos.is_none() || full_resnapshot,
                    max_parallel_snapshot_tables,
                )
                .await;

            let status = if snapshot_result.is_err() {
                SnapshotStatusTag::Failed.value()
            } else {
                SnapshotStatusTag::Successful.value()
            };

            counter!(
                recorded::REPLICATOR_SNAPSHOT_STATUS,
                "status" => status
            )
            .increment(1u64);

            snapshot_result?;

            info!("Snapshot finished");
            histogram!(recorded::REPLICATOR_SNAPSHOT_DURATION)
                .record(snapshot_start.elapsed().as_micros() as f64);

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
            table_status_tx,
        };

        if min_pos != max_pos {
            info!(start = %min_pos, end = %max_pos, "Catching up");
            if let Err(err) = adapter
                .main_loop(&mut min_pos, Some(max_pos), controller_tx, replicator_rx)
                .await
            {
                warn!(position = %min_pos, "Replicator stopped during catch-up phase");
                return Err(err);
            }
        }

        // Let Controller know that the initial snapshotting is complete. Ignores the error, which
        // will not occur unless the Controller dropped the rx half of this channel.
        let _ = controller_tx.send(ControllerMessage::SnapshotDone);

        info!(position = %min_pos, "Streaming replication started");
        if let Err(err) = adapter
            .main_loop(&mut min_pos, None, controller_tx, replicator_rx)
            .await
        {
            warn!(position = %min_pos, "Replicator stopped during streaming replication");
            return Err(err);
        }

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    /// Apply a DDL string to noria with the current log position
    async fn handle_ddl_change(
        &mut self,
        schema: String,
        changes: Vec<Change>,
        pos: &ReplicationOffset,
    ) -> ReadySetResult<()> {
        let mut changelist = ChangeList::from_changes(changes, self.dialect);

        // Remove DDL changes outside the filtered scope
        let mut non_replicated_tables = vec![];
        changelist.changes_mut().retain(|change| match change {
            Change::CreateTable { statement, .. } => {
                let keep = self
                    .table_filter
                    .should_be_processed(schema.as_str(), statement.table.name.as_str())
                    && statement.body.is_ok()
                    && statement.like.is_none();
                if !keep {
                    non_replicated_tables.push(Relation {
                        schema: Some(schema.clone().into()),
                        name: statement.table.name.clone(),
                    });
                }
                keep
            }
            Change::AlterTable(stmt) => {
                // If this alter table is a rename, we also check whether the new name is filtered
                // out. Only if both are ignored will this return false and this event get skipped.
                self.table_filter
                    .should_be_processed(schema.as_str(), stmt.table.name.as_str())
                    || if let Ok(defs) = &stmt.definitions {
                        defs.iter().any(|def| {
                            if let readyset_sql::ast::AlterTableDefinition::RenameTable {
                                new_name,
                            } = def
                            {
                                self.table_filter
                                    .should_be_processed(schema.as_str(), new_name.name.as_str())
                            } else {
                                false
                            }
                        })
                    } else {
                        false
                    }
            }
            _ => true,
        });

        // Mark all tables that were filtered as non-replicated, too
        for relation in non_replicated_tables {
            if let Some(schema) = &relation.schema {
                self.table_filter
                    .deny_replication(schema, relation.name.as_str());
            }
            changelist
                .changes_mut()
                .push(Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name: relation,
                    reason: NotReplicatedReason::Configuration,
                }));
        }

        // Resolve schemas and drop renamed tables; the new names will get snapshotted when we
        // return `ResnapshotNeeded` in the next `if` block
        for change in changelist.changes_mut() {
            if let Change::AlterTable(stmt) = change {
                if stmt.table.schema.is_none() {
                    stmt.table.schema = Some(schema.clone().into());
                }

                if let Ok(definitions) = &mut stmt.definitions {
                    for def in definitions.iter_mut() {
                        if let readyset_sql::ast::AlterTableDefinition::RenameTable { new_name } =
                            def
                        {
                            if new_name.schema.is_none() {
                                new_name.schema = Some(schema.clone().into());
                            }

                            info!(
                                old_table = %stmt.table.display_unquoted(),
                                new_table = %new_name.display_unquoted(),
                                "Table rename detected, triggering resnapshot for table"
                            );
                            self.drop_table_for_resnapshot(stmt.table.clone()).await?;
                        }
                    }
                }
            }
        }

        if self.supports_resnapshot && changelist.changes().any(Change::requires_resnapshot) {
            // In case we detect a DDL change that requires a full schema resnapshot exit the loop
            // with the proper status
            if let Some(pos) = self.replication_offsets.max_offset()?.cloned() {
                // Forward all positions to the maximum position (the one prior to this statement)
                // to avoid needless replay later
                self.handle_log_position(&pos).await?;
            }
            return Err(ReadySetError::ResnapshotNeeded);
        }

        changelist = changelist.with_schema_search_path(vec![schema.clone().into()]);

        // Collect a list of all tables we're creating for later
        let mut added_tables = HashSet::new();
        for change in changelist.changes() {
            let mut table = match change {
                Change::CreateTable { statement, .. } => statement.table.clone(),
                Change::AddNonReplicatedRelation(table) => table.name.clone(),
                _ => continue,
            };
            table.schema.get_or_insert_with(|| (&schema).into());
            if matches!(change, Change::CreateTable { .. }) {
                added_tables.insert(table);
            } else {
                added_tables.remove(&table);
            }
        }

        match self
            .noria
            .extend_recipe_with_offset(changelist.clone(), pos, false)
            .await
        {
            // ReadySet likely entered an invalid state, fail the replicator.
            Err(e @ ReadySetError::RecipeInvariantViolated(_)) => return Err(e),
            Err(error) => {
                warn!(%error, "Error extending recipe, DDL statement will not be used");
                counter!(recorded::REPLICATOR_FAILURE).increment(1u64);

                let changes = mem::take(changelist.changes_mut());
                // If something went wrong, mark all the tables and views that we just tried to
                // create as non-replicated
                added_tables.clear();
                changelist
                    .changes_mut()
                    .extend(changes.into_iter().filter_map(|change| {
                        Some(Change::AddNonReplicatedRelation(NonReplicatedRelation {
                            name: match change {
                                Change::CreateTable { statement, .. } => statement.table,
                                Change::CreateView(stmt) => stmt.name,
                                Change::AddNonReplicatedRelation(rel) => rel.name,
                                _ => return None,
                            },
                            reason: NotReplicatedReason::from_string(&error.to_string()),
                        }))
                    }));
                for change in changelist.changes_mut() {
                    match change {
                        Change::AddNonReplicatedRelation(NonReplicatedRelation {
                            ref mut name,
                            ..
                        }) if name.schema.is_none() => name.schema = Some((&schema).into()),
                        _ => (),
                    }
                }
                self.noria.extend_recipe(changelist).await?;
            }
            Ok(_) => {}
        }

        self.replication_offsets.schema = Some(pos.clone());
        self.clear_mutator_cache();

        // Set the replication offset for each table we just created to this replication offset
        // (since otherwise they'll get initialized without an offset)
        for table in &added_tables {
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

            // Online freshly-created table.
            if let Err(err) = self
                .table_status_tx
                .send((table.clone(), TableStatus::Online))
            {
                error!(
                    error = %err,
                    table = %table.display_unquoted(),
                    "Failed to notify controller of freshly-created table",
                );
            }
        }

        Ok(())
    }

    /// Update the log position of the schema and the tables
    async fn handle_log_position(&mut self, pos: &ReplicationOffset) -> ReadySetResult<()> {
        // Update the log position for the schema
        debug!(%pos, "Setting schema replication offset");
        self.noria.set_schema_replication_offset(Some(pos)).await?;

        // Update the log position for the tables that are behind this offset
        let tables = self
            .replication_offsets
            .tables
            .iter()
            .filter_map(|(k, v)| match v {
                None => Some(k),
                Some(cur_offset) if *cur_offset < *pos => Some(k),
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
        pos: &ReplicationOffset,
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
                    table_name = %table.display(readyset_sql::Dialect::PostgreSQL),
                    num_actions = actions.len(),
                    position = %pos,
                    "Could not find table, discarding actions"
                );
            }
            return Ok(());
        };
        actions.push(TableOperation::SetReplicationOffset(pos.clone()));
        table_mutator.perform_all(actions).await?;

        self.replication_offsets
            .tables
            .insert(table, Some(pos.clone()));

        Ok(())
    }

    /// Handle a vector of ReplicationAction by calling the proper ReadySet RPC. If `catchup` is
    /// set, we will not log warnings for skipping entries, as we may iterate over many entries
    /// tables have already seen when catching each table up to the current binlog offset.
    async fn handle_action(
        &mut self,
        actions: Vec<ReplicationAction>,
        pos: ReplicationOffset,
        catchup: bool,
    ) -> ReadySetResult<()> {
        set_failpoint_return_err!(failpoints::REPLICATION_HANDLE_ACTION);
        // First check if we should skip this action due to insufficient log position or lack of
        // interest
        let mut actionables: Vec<ReplicationAction> = Vec::new();
        for action in actions {
            match action {
                ReplicationAction::DdlChange { .. } | ReplicationAction::LogPosition => {
                    match &self.replication_offsets.schema {
                        Some(cur) if pos <= *cur => {
                            if !catchup {
                                warn!(%pos, %cur, "Skipping schema update for earlier entry");
                            }
                        }
                        _ => {
                            actionables.push(action);
                        }
                    }
                }
                ReplicationAction::TableAction { table, actions } => {
                    match self.replication_offsets.tables.get(&table) {
                        Some(Some(cur)) if pos <= *cur => {
                            if !catchup {
                                warn!(
                                    table = %table.display_unquoted(),
                                    %pos,
                                    %cur,
                                    "Skipping table action for earlier entry"
                                );
                            }
                            continue;
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
                    if self.table_filter.should_be_processed(
                        table.schema.as_deref().ok_or_else(|| {
                            internal_err!("All tables should have a schema in the replicator")
                        })?,
                        &table.name,
                    ) {
                        actionables.push(ReplicationAction::TableAction { table, actions });
                    }
                }
                ReplicationAction::Empty => {}
            }
        }

        for action in actionables {
            match action {
                ReplicationAction::DdlChange { schema, changes } => {
                    self.handle_ddl_change(schema, changes, &pos).await?
                }
                ReplicationAction::TableAction { table, actions } => {
                    self.handle_table_actions(table, actions, &pos).await?
                }
                ReplicationAction::LogPosition => self.handle_log_position(&pos).await?,
                ReplicationAction::Empty => unreachable!("Should not have an empty action"),
            }
        }
        Ok(())
    }

    /// Loop over the actions. `until` may be passed to set a replication offset to stop
    /// replicating at.
    async fn main_loop(
        &mut self,
        position: &mut ReplicationOffset,
        until: Option<ReplicationOffset>,
        controller_tx: &UnboundedSender<ControllerMessage>,
        replicator_rx: &mut UnboundedReceiver<ReplicatorMessage>,
    ) -> ReadySetResult<()> {
        // Notify the controller that we've started replication if we've entered the main (not
        // catchup) replication loop.
        if until.is_none() {
            // Will not error unless the Controller has dropped the rx half of the channel
            let _ = controller_tx.send(ControllerMessage::ReplicationStarted);
        }

        loop {
            set_failpoint!(failpoints::UPSTREAM, |_| ReadySetResult::Err(
                ReadySetError::ReplicationFailed(
                    "replication-upstream failpoint injected".to_string()
                )
            ));

            if until.as_ref().map(|u| *position >= *u).unwrap_or(false) {
                return Ok(());
            }

            select! {
                biased;
                next_actions = self.connector.next_action(position, until.as_ref()).fuse() => match next_actions {
                    Ok((actions, pos)) => {
                        *position = pos.clone();
                        debug!(%position, "Received replication action");

                        trace!(?actions);
                        if let Err(err) = self.handle_action(actions, pos, until.is_some()).await {
                            if matches!(err, ReadySetError::ResnapshotNeeded) {
                                info!("Change in DDL requires partial resnapshot");
                            } else {
                                error!(error = %err, "Aborting replication task on error");
                                counter!(recorded::REPLICATOR_FAILURE).increment(1u64);
                            }
                            // In some cases, we may fail to replicate because of unsupported operations, stop
                            // replicating a table if we encounter this type of error.
                            if let ReadySetError::TableError { table, source } = err {
                                self.deny_replication_for_table(table, source).await?;
                                continue;
                            }
                            return Err(err);
                        };
                        counter!(recorded::REPLICATOR_SUCCESS).increment(1u64);
                        debug!(%position, "Successfully applied replication action");
                    }
                    Err(ReadySetError::TableError { table, source }) => {
                        if source.is_networking_related() {
                            // Don't deny replication of the error is caused by networking
                            return Err(ReadySetError::TableError { table, source });
                        }
                        self.deny_replication_for_table(table, source).await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
                replicator_message = replicator_rx.recv() => match replicator_message {
                    Some(ReplicatorMessage::ResnapshotTable { table }) => {
                        self.drop_table_for_resnapshot(table).await?;
                        return Err(ReadySetError::ResnapshotNeeded);
                    }
                    Some(ReplicatorMessage::AddTables { tables }) => {
                        for table in tables {
                            if !self.table_filter.should_be_processed(table.schema.as_deref().unwrap(), &table.name) {
                                self.table_filter.allow_replication(table.schema.as_deref().unwrap(), &table.name);
                            }
                        }
                        return Err(ReadySetError::ResnapshotNeeded);
                    }
                    None => {}
                }
            }
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
        match self.mutator_map.entry(name.clone()) {
            hash_map::Entry::Occupied(o) => Ok(o.into_mut().as_mut()),
            hash_map::Entry::Vacant(v) => match self.noria.table(name.clone()).await {
                Ok(table) => Ok(v.insert(Some(table)).as_mut()),
                Err(e) if e.caused_by_table_not_found() => {
                    // Cache the not found result as well as the found result
                    Ok(v.insert(None).as_mut())
                }
                Err(e) => Err(e),
            },
        }
    }

    /// Drop one base table if it exists in ReadySet. This is used to clean up the base table
    /// and restart replication to trigger a new snapshot.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to drop
    ///
    /// # Returns
    ///
    /// A `ReadySetResult` indicating success or failure.
    async fn drop_table_for_resnapshot(&mut self, table: Relation) -> ReadySetResult<()> {
        self.replication_offsets.tables.remove(&table);
        let changelist = ChangeList::from_change(
            Change::Drop {
                name: table.clone(),
                if_exists: true,
            },
            self.dialect,
        );
        self.noria.extend_recipe(changelist).await?;
        Ok(())
    }

    /// Remove the table referenced by the provided schema and table name from our base table and
    /// dataflow state (if any).
    async fn remove_table_from_readyset(&mut self, table: Relation) -> ReadySetResult<()> {
        info!(
            table = %table.display(readyset_sql::Dialect::PostgreSQL),
            "Removing table state from readyset"
        );

        // In case of a domain failure, we might be replacing the failed domain.
        // and at the same time attempting to remove a table from readyset.
        // We need to wait for the new domain to be ready before removing the table.
        retry_with_exponential_backoff!(
            || async {
                let mut noria = self.noria.clone();
                noria.replication_offsets().await
            },
            retries: 5,
            delay: 250,
            backoff: 2,
        )?;

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
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    // assuming NonReplicatedRelation has fields `relation` and `reason`
                    name: table,
                    reason: NotReplicatedReason::TableDropped,
                }),
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
            table = %table.display(readyset_sql::Dialect::PostgreSQL),
            error = %source,
            "Will stop replicating a table due to table error"
        );

        let schema = table.schema.clone().ok_or_else(|| {
            // Tables should have a schema defined at this point, or something has gone wrong
            // somewhere. If we don't have a schema at this point, we don't know which
            // table encountered an error, so we fall back to resnapshotting.
            error!(
                table = %table.display(readyset_sql::Dialect::PostgreSQL),
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
        set_failpoint_return_err!(failpoints::IGNORE_TABLE_FAIL_DROPPING_TABLE);
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
