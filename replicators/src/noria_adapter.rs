use std::collections::{hash_map, HashMap, HashSet};
use std::convert::TryInto;
use std::future;
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser;
use database_utils::DatabaseURL;
use futures::{FutureExt, TryFutureExt};
use launchpad::redacted::RedactedString;
use launchpad::select;
use metrics::{counter, histogram};
use noria::consensus::Authority;
use noria::consistency::Timestamp;
use noria::metrics::recorded::{self, SnapshotStatusTag};
use noria::replication::{ReplicationOffset, ReplicationOffsets};
use noria::{ControllerHandle, ReadySetError, ReadySetResult, Table, TableOperation};
use noria_errors::{internal_err, invalid_err};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::mysql_connector::{MySqlBinlogConnector, MySqlReplicator};
use crate::postgres_connector::{
    PostgresPosition, PostgresReplicator, PostgresWalConnector, PUBLICATION_NAME, REPLICATION_SLOT,
};

/// Shared configuration for replication.
///
/// Usable as command-line options via `#[clap(flatten)]`
#[derive(Debug, Clone, Parser, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// A URL identifying a MySQL or PostgreSQL primary server to replicate from. Should include
    /// username and password if necessary.
    #[clap(long, env = "REPLICATION_URL")]
    #[serde(default)]
    pub replication_url: Option<RedactedString>,

    /// Disable verification of SSL certificates supplied by the replication database (postgres
    /// only, ignored for mysql). Ignored if `--replication-url` is not passed.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this flag. If invalid certificates are
    /// trusted, any certificate for any site will be trusted for use, including expired
    /// certificates. This introduces significant vulnerabilities, and should only be used as a
    /// last resort.
    #[clap(long, env = "DISABLE_REPLICATION_SSL_VERIFICATION")]
    #[serde(default)]
    pub disable_replication_ssl_verification: bool,

    /// Sets the server id when acquiring a binlog replication slot.
    #[clap(long, hide = true)]
    #[serde(default)]
    pub replication_server_id: Option<u32>,

    /// The time to wait before restarting the replicator in seconds.
    #[clap(long, hide = true, default_value = "30", parse(try_from_str = duration_from_seconds))]
    #[serde(default = "default_replicator_restart_timeout")]
    pub replicator_restart_timeout: Duration,
}

fn default_replicator_restart_timeout() -> Duration {
    Config::default().replicator_restart_timeout
}

fn duration_from_seconds(i: &str) -> Result<Duration, ParseIntError> {
    i.parse::<u64>().map(Duration::from_secs)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            replication_url: Default::default(),
            disable_replication_ssl_verification: false,
            replication_server_id: Default::default(),
            replicator_restart_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReplicationAction {
    TableAction {
        table: String,
        actions: Vec<TableOperation>,
        /// The transaction id of a table write operation. Each
        /// table write operation within a transaction should be assigned
        /// the same transaction id. These id's should be monotonically
        /// increasing across transactions.
        txid: Option<u64>,
    },
    SchemaChange {
        ddl: String,
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

/// An adapter that converts database events into Noria API calls
pub struct NoriaAdapter {
    /// The Noria API handle
    noria: ControllerHandle,
    /// The binlog reader
    connector: Box<dyn Connector + Send + Sync>,
    /// A map of cached table mutators
    mutator_map: HashMap<String, Option<Table>>,
    /// A HashSet of tables we've already warned about not existing
    warned_missing_tables: HashSet<String>,
    /// The set of replication offsets for the schema and the tables, obtained from the controller
    /// at startup and maintained during replication.
    ///
    /// Since some tables may have lagged behind others due to failed snapshotting, we start
    /// replication at the *minimum* replication offset, but ignore any replication events that
    /// come before the offset for that table
    replication_offsets: ReplicationOffsets,
}

impl NoriaAdapter {
    pub async fn start_with_authority(authority: Authority, config: Config) -> ReadySetResult<!> {
        let noria = noria::ControllerHandle::new(authority).await;
        NoriaAdapter::start(noria, config, None).await
    }

    pub async fn start(
        noria: ControllerHandle,
        mut config: Config,
        ready_notify: Option<Arc<Notify>>,
    ) -> ReadySetResult<!> {
        let url: DatabaseURL = config
            .replication_url
            .take()
            .ok_or_else(|| internal_err!("Replication URL not supplied"))?
            .parse()
            .map_err(|e| invalid_err!("Invalid URL supplied to --replication-url: {e}"))?;

        match url {
            DatabaseURL::MySQL(options) => {
                NoriaAdapter::start_inner_mysql(options, noria, config, ready_notify).await
            }
            DatabaseURL::PostgreSQL(options) => {
                NoriaAdapter::start_inner_postgres(options, noria, config, ready_notify).await
            }
        }
    }

    /// Finish the build and begin monitoring the binlog for changes
    /// If noria has no replication offset information, it will replicate the target database in its
    /// entirety to Noria before listening on the binlog
    /// The replication happens in stages:
    /// * READ LOCK is acquired on the database
    /// * Next binlog position is read
    /// * The recipe (schema) DDL is replicated and installed in Noria (replacing current recipe)
    /// * Each table is individually replicated into Noria
    /// * READ LOCK is released
    /// * Adapter keeps reading binlog from the next position keeping Noria up to date
    async fn start_inner_mysql(
        mysql_options: mysql::Opts,
        mut noria: ControllerHandle,
        config: Config,
        ready_notify: Option<Arc<Notify>>,
    ) -> ReadySetResult<!> {
        use crate::mysql_connector::BinlogPosition;
        // Load the replication offset for all tables and the schema from Noria
        let mut replication_offsets = noria.replication_offsets().await?;
        let pos = match replication_offsets.max_offset()? {
            None => {
                let span = info_span!("taking database snapshot");
                let replicator_options = mysql_options.clone();
                let pool = mysql::Pool::new(replicator_options);
                let replicator = MySqlReplicator { pool, tables: None };

                let snapshot_start = Instant::now();
                counter!(
                    recorded::REPLICATOR_SNAPSHOT_STATUS,
                    1u64,
                    "status" => SnapshotStatusTag::Started.value(),
                );
                span.in_scope(|| info!("Starting snapshot"));
                let snapshot_result = replicator
                    .snapshot_to_noria(&mut noria, &replication_offsets, true)
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
                // replication events for offsets < the replication offset of that table, so we can
                // do this "catching up" by just starting replication at the old offset.
                // Note that at the very least we will always have the schema offset for the
                // minumum.
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
                pos
            }
            Some(pos) => pos.clone().into(),
        };

        let schemas = mysql_options
            .db_name()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();

        // TODO: it is possible that the binlog position from noria is no longer
        // present on the primary, in which case the connection will fail, and we would
        // need to perform a new snapshot
        let connector = Box::new(
            MySqlBinlogConnector::connect(
                mysql_options,
                schemas,
                pos.clone(),
                config.replication_server_id,
            )
            .await?,
        );

        let mut adapter = NoriaAdapter {
            noria,
            connector,
            replication_offsets,
            mutator_map: HashMap::new(),
            warned_missing_tables: HashSet::new(),
        };

        let mut current_pos: ReplicationOffset = pos.try_into()?;

        // At this point it is possible that we just finished replication, but
        // our schema and our tables are taken at different position in the binlog.
        // Until our database has a consitent view of the database at a single point
        // in time, it is not safe to issue any queries. We therefore advance the binlog
        // to the position of the most recent table we have, applying changes as needed.
        // Only once binlog advanced to that point, can we send a ready signal to noria.
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
        if let Some(notify) = ready_notify {
            notify.notify_one();
        }

        adapter.main_loop(&mut current_pos, None).await?;

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    async fn start_inner_postgres(
        pgsql_opts: pgsql::Config,
        mut noria: ControllerHandle,
        config: Config,
        ready_notify: Option<Arc<Notify>>,
    ) -> ReadySetResult<!> {
        // Attempt to retreive the latest replication offset from noria, if none is present
        // begin the snapshot process
        let replication_offsets = noria.replication_offsets().await?;
        let pos = replication_offsets.max_offset()?.map(Into::into);

        if let Some(pos) = pos {
            info!(wal_position = %pos);
        }

        let dbname = pgsql_opts
            .get_dbname()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();

        let mut connector = Box::new(
            PostgresWalConnector::connect(pgsql_opts.clone(), dbname.first().unwrap(), config, pos)
                .await?,
        );

        info!("Connected to PostgreSQL");

        if let Some(snapshot) = connector.snapshot_name.as_deref() {
            // If snapshot name exists, it means we need to make a snapshot to noria
            let (mut client, connection) = pgsql_opts
                .connect(postgres_native_tls::MakeTlsConnector::new(
                    native_tls::TlsConnector::builder().build().unwrap(),
                ))
                .await?;

            let connection_handle = tokio::spawn(connection);

            let mut replicator = PostgresReplicator::new(&mut client, &mut noria, None).await?;

            select! {
                s = replicator.snapshot_to_noria(snapshot).fuse() => s?,
                c = connection_handle.fuse() => c.unwrap()?,
            }

            info!("Snapshot finished");
        }

        // Let waiters know that the initial snapshotting is complete.
        if let Some(notify) = ready_notify {
            notify.notify_one();
        }

        connector
            .start_replication(REPLICATION_SLOT, PUBLICATION_NAME)
            .await?;

        info!("Streaming replication started");

        let mut adapter = NoriaAdapter {
            noria,
            connector,
            replication_offsets,
            mutator_map: HashMap::new(),
            warned_missing_tables: HashSet::new(),
        };

        adapter
            .main_loop(&mut PostgresPosition::default().into(), None)
            .await?;

        unreachable!("`main_loop` will never stop with an Ok status if `until = None`");
    }

    /// Apply a DDL string to noria with the current log position
    async fn handle_ddl_change(
        &mut self,
        ddl: String,
        pos: ReplicationOffset,
    ) -> ReadySetResult<()> {
        match future::ready(ddl.try_into())
            .and_then(|changelist| async {
                self.noria
                    .extend_recipe_with_offset(changelist, &pos, false)
                    .await
            })
            .await
        {
            // ReadySet likely entered an invalid state, fail the replicator.
            Err(e @ ReadySetError::RecipeInvariantViolated(_)) => return Err(e),
            Err(e) => {
                error!(error = %e, "Error extending recipe, DDL statement will not be used");
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
            if let Some(table) = self.mutator_for_table(table.as_str()).await? {
                table.set_replication_offset(pos.clone()).await?;
            }
        }

        self.replication_offsets.advance_offset(pos.clone())?;

        Ok(())
    }

    /// Send table actions to noria tables, and update the binlog position for the table
    async fn handle_table_actions(
        &mut self,
        table: String,
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

        // If there was a transaction id associated, propagate the
        // timestamp with that transaction id
        // TODO(justin): Make this operation atomic with the table
        // actions being pushed above.
        // TODO(vlad): We have to propagate txid to every table or
        // else we won't be able to ensure proper read after write
        if let Some(tx) = txid {
            let mut timestamp = Timestamp::default();
            timestamp.map.insert(table_mutator.node, tx);
            table_mutator.update_timestamp(timestamp).await?;
        }

        self.replication_offsets
            .tables
            .insert(table.into(), Some(pos));

        Ok(())
    }

    /// Handle a single BinlogAction by calling the proper Noria RPC. If `catchup` is set,
    /// we will not log warnings for skipping entries, as we may iterate over many entries tables
    /// have already seen when catching each table up to the current binlog offset.
    async fn handle_action(
        &mut self,
        action: ReplicationAction,
        pos: ReplicationOffset,
        catchup: bool,
    ) -> ReadySetResult<()> {
        // First check if we should skip this action due to insufficient log position
        match &action {
            ReplicationAction::SchemaChange { .. } | ReplicationAction::LogPosition => {
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
                match self.replication_offsets.tables.get(table.as_str()) {
                    Some(Some(cur)) if pos <= *cur => {
                        if !catchup {
                            warn!(%table, %pos, %cur, "Skipping table action for earlier entry");
                        }
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }

        match action {
            ReplicationAction::SchemaChange { ddl } => self.handle_ddl_change(ddl, pos).await,
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
            if until.as_ref().map(|u| *position >= *u).unwrap_or(false) {
                return Ok(());
            }

            let (action, pos) = self.connector.next_action(position, until.as_ref()).await?;
            *position = pos.clone();
            debug!(?position, "Received replication action");

            trace!(?action);

            if let Err(err) = self.handle_action(action, pos, until.is_some()).await {
                error!(error = %err, "Aborting replication task on error");
                counter!(recorded::REPLICATOR_FAILURE, 1u64,);
                return Err(err);
            };
            counter!(recorded::REPLICATOR_SUCCESS, 1u64);
            debug!(?position, "Successfully applied replication action");
        }
    }

    /// When schema changes there is a risk the cached mutators will no longer be in sync
    /// and we need to drop them all
    fn clear_mutator_cache(&mut self) {
        self.mutator_map.clear()
    }

    /// Get a mutator for a noria table from the cache if available, or fetch a new one
    /// from the controller and cache it. Returns None if the table doesn't exist in noria.
    async fn mutator_for_table(
        &mut self,
        name: impl Into<String> + AsRef<str>,
    ) -> ReadySetResult<Option<&mut Table>> {
        match self.mutator_map.raw_entry_mut().from_key(name.as_ref()) {
            hash_map::RawEntryMut::Occupied(o) => Ok(o.into_mut().as_mut()),
            hash_map::RawEntryMut::Vacant(v) => match self.noria.table(name.as_ref()).await {
                Ok(table) => Ok(v.insert(name.into(), Some(table)).1.as_mut()),
                Err(e) if e.caused_by_table_not_found() => {
                    // Cache the not found result as well as the found result
                    Ok(v.insert(name.into(), None).1.as_mut())
                }
                Err(e) => Err(e),
            },
        }
    }
}
