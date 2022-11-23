use std::collections::{hash_map, HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use database_utils::{DatabaseURL, UpstreamConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use failpoint_macros::set_failpoint;
use futures::FutureExt;
use launchpad::select;
use metrics::{counter, histogram};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, SslOpts};
use nom_sql::Relation;
use readyset::consensus::Authority;
use readyset::consistency::Timestamp;
#[cfg(feature = "failure_injection")]
use readyset::failpoints;
use readyset::metrics::recorded::{self, SnapshotStatusTag};
use readyset::recipe::changelist::{Change, ChangeList};
use readyset::replication::{ReplicationOffset, ReplicationOffsets};
use readyset::{ReadySetError, ReadySetHandle, ReadySetResult, Table, TableOperation};
use readyset_data::Dialect;
use readyset_errors::{internal_err, invalid_err};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
use tokio::sync::Notify;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::db_util::{CreateSchema, DatabaseSchemas};
use crate::mysql_connector::{MySqlBinlogConnector, MySqlReplicator};
use crate::postgres_connector::{
    PostgresReplicator, PostgresWalConnector, PUBLICATION_NAME, REPLICATION_SLOT,
};
use crate::table_filter::TableFilter;

const WAIT_BEFORE_RESNAPSHOT: Duration = Duration::from_secs(3);

const RESNAPSHOT_SLOT: &str = "readyset_resnapshot";

// TODO: Change this to be configurable with a flag.
/// Sets the maximum connection count for the postgres connection pool. This was choosen
/// arbitrarily because the default max connections to postgres for normal user is 100, so this is
/// a sizable pool size while leaving some buffer room for other direct connections that may be
/// required to the underlying database.
const PG_POOL_MAX_SIZE: usize = 50;

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
    pub async fn start_with_authority(
        authority: Authority,
        telemetry_sender: TelemetrySender,
        config: UpstreamConfig,
    ) -> ReadySetResult<!> {
        let noria = readyset::ReadySetHandle::new(authority).await;
        NoriaAdapter::start(noria, config, None, telemetry_sender).await
    }

    pub async fn start(
        noria: ReadySetHandle,
        mut config: UpstreamConfig,
        mut notify: Option<Arc<Notify>>,
        telemetry_sender: TelemetrySender,
    ) -> ReadySetResult<!> {
        let mut resnapshot = false;
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
                )
                .await
            }
            DatabaseURL::PostgreSQL(options) => {
                let noria = noria.clone();
                let config = config.clone();
                let pool = pg_pool(options.clone()).await?;
                NoriaAdapter::start_inner_postgres(
                    options,
                    noria,
                    config,
                    &mut notify,
                    resnapshot,
                    &telemetry_sender,
                    pool,
                )
                .await
            }
        } {
            match err {
                ReadySetError::ResnapshotNeeded => {
                    tokio::time::sleep(WAIT_BEFORE_RESNAPSHOT).await;
                    resnapshot = true;
                }
                err => return Err(err),
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
                let replicator_options = mysql_options.clone();
                let pool = mysql::Pool::new(replicator_options);

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

    async fn start_inner_postgres(
        pgsql_opts: pgsql::Config,
        mut noria: ReadySetHandle,
        mut config: UpstreamConfig,
        ready_notify: &mut Option<Arc<Notify>>,
        resnapshot: bool,
        telemetry_sender: &TelemetrySender,
        pool: deadpool_postgres::Pool,
    ) -> ReadySetResult<!> {
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

        let mut connector = Box::new(
            PostgresWalConnector::connect(
                pgsql_opts.clone(),
                dbname,
                config,
                pos,
                tls_connector.clone(),
            )
            .await?,
        );

        info!("Connected to PostgreSQL");

        let replication_slot = if let Some(slot) = &connector.replication_slot {
            Some(slot.clone())
        } else if resnapshot || pos.is_none() {
            // This is not an initial connection but we need to resnapshot the latest schema,
            // therefore we create a new replication slot, just so we can get a consistent snapshot
            // with a WAL position attached. This is more robust than locking and allows us to reuse
            // the existing snapshotting code.
            info!(
                slot = RESNAPSHOT_SLOT,
                "Recreating replication slot to resnapshot with the latest schema"
            );
            connector.drop_replication_slot(RESNAPSHOT_SLOT).await?;
            Some(
                connector
                    .create_replication_slot(RESNAPSHOT_SLOT, true)
                    .await?,
            )
        } else {
            None
        };

        let mut create_schema = CreateSchema::new(dbname.to_string(), nom_sql::Dialect::PostgreSQL);

        if let Some(replication_slot) = replication_slot {
            let snapshot_start = Instant::now();
            // If snapshot name exists, it means we need to make a snapshot to noria

            let (mut client, connection) = pgsql_opts.connect(tls_connector).await?;

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
                snapshot_result = replicator.snapshot_to_noria(&replication_slot, &mut create_schema, snapshot_report_interval_secs).fuse() =>  {
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
                c = connection_handle.fuse() => c.unwrap()?,
            }

            info!("Snapshot finished");
            histogram!(
                recorded::REPLICATOR_SNAPSHOT_DURATION,
                snapshot_start.elapsed().as_micros() as f64
            );

            if replication_slot.slot_name == RESNAPSHOT_SLOT {
                connector.drop_replication_slot(RESNAPSHOT_SLOT).await?;
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
            .start_replication(REPLICATION_SLOT, PUBLICATION_NAME)
            .await?;

        info!("Streaming replication started");

        let replication_offsets = noria.replication_offsets().await?;
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
        changelist.changes_mut().retain(|change| match change {
            Change::CreateTable(stmt) => self
                .table_filter
                .should_be_processed(schema.as_str(), stmt.table.name.as_str()),
            Change::AlterTable(stmt) => self
                .table_filter
                .should_be_processed(schema.as_str(), stmt.table.name.as_str()),
            _ => true,
        });

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

        match self
            .noria
            .extend_recipe_with_offset(
                changelist.with_schema_search_path(vec![schema.into()]),
                &pos,
                false,
            )
            .await
        {
            // ReadySet likely entered an invalid state, fail the replicator.
            Err(e @ ReadySetError::RecipeInvariantViolated(_)) => return Err(e),
            Err(e) => {
                warn!(error = %e, "Error extending recipe, DDL statement will not be used");
                counter!(recorded::REPLICATOR_FAILURE, 1u64,);
            }
            Ok(_) => {}
        }
        self.replication_offsets.schema = Some(pos);
        self.clear_mutator_cache();

        Ok(())
    }

    /// Update the log position of the schema and the tables
    async fn handle_log_position(&mut self, pos: ReplicationOffset) -> ReadySetResult<()> {
        // Update the log position for the schema
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
                    table_name = %table,
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
                            warn!(%table, %pos, %cur, "Skipping table action for earlier entry");
                        }
                        return Ok(());
                    }
                    _ => {}
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

            let (action, pos) = self.connector.next_action(position, until.as_ref()).await?;
            *position = pos.clone();
            debug!(%position, "Received replication action");

            trace!(?action);

            if let Err(err) = self.handle_action(action, pos, until.is_some()).await {
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
}

pub async fn pg_pool(
    config: pgsql::Config,
) -> Result<deadpool_postgres::Pool, deadpool_postgres::BuildError> {
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    };
    let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
    let tls = postgres_native_tls::MakeTlsConnector::new(connector);
    let mgr = Manager::from_config(config, tls, mgr_config);
    Pool::builder(mgr).max_size(PG_POOL_MAX_SIZE).build()
}
