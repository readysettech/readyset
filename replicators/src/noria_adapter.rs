use crate::mysql_connector::{MySqlBinlogConnector, MySqlReplicator};
use crate::postgres_connector::{
    PostgresPosition, PostgresReplicator, PostgresWalConnector, PUBLICATION_NAME, REPLICATION_SLOT,
};
use async_trait::async_trait;
use futures::FutureExt;
use mysql_async as mysql;
use noria::consistency::Timestamp;
use noria::{consensus::Authority, ReplicationOffset, TableOperation};
use noria::{ControllerHandle, ReadySetError, ReadySetResult, Table, ZookeeperAuthority};
use slog::{debug, error, info, o, Discard, Logger};
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;
use std::fmt::Display;
use std::str::FromStr;
use tokio_postgres as pgsql;

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
    async fn next_action(
        &mut self,
        last_pos: ReplicationOffset,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)>;
}

/// An adapter that converts database events into Noria API calls
pub struct NoriaAdapter<A: Authority + 'static> {
    /// The Noria API handle
    noria: ControllerHandle<A>,
    /// The binlog reader
    connector: Box<dyn Connector + Send + Sync>,
    /// A map of cached table mutators
    mutator_map: HashMap<String, Table>,
    /// Logger
    log: Logger,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AdapterOpts {
    MySql(mysql::Opts),
    Postgres(pgsql::Config),
}

impl FromStr for AdapterOpts {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            let opts: mysql::Opts = s.parse().map_err(|e: mysql::UrlError| e.to_string())?;
            if opts.db_name().is_none() {
                return Err("Database name is required in MySQL URL".to_string());
            }
            Ok(AdapterOpts::MySql(opts))
        } else if s.starts_with("postgres://") || s.starts_with("postgresql://") {
            let opts: pgsql::Config = s.parse().map_err(|e: pgsql::Error| e.to_string())?;
            if opts.get_dbname().is_none() {
                return Err("Database name is required in PostgreSQL URL".to_string());
            }
            Ok(AdapterOpts::Postgres(opts))
        } else {
            Err("A valid URL should begin with mysql:// or postgresql://".to_string())
        }
    }
}

impl NoriaAdapter<ZookeeperAuthority> {
    pub async fn start<S1: Display, S2: Display>(
        zookeeper_addr: S1,
        deployment: S2,
        options: AdapterOpts,
        log: Option<Logger>,
    ) -> ReadySetResult<()> {
        let authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment))?;
        let noria = noria::ControllerHandle::new(authority).await?;
        let log = log.unwrap_or_else(|| Logger::root(Discard, o!()));

        NoriaAdapter::start_inner(noria, options, log, None).await
    }
}

impl<A: Authority> NoriaAdapter<A> {
    /// Same as [`start`](Builder::start), but accepts a MySQL/PostgreSQL url for options
    /// and an externally supplied Noria `ControllerHandle`.
    /// The MySQL url must contain the database name, and user and password if applicable.
    /// i.e. `mysql://user:pass%20word@localhost/database_name` or
    /// `postgresql://user:pass%20word@localhost/database_name`
    #[allow(dead_code)]
    pub async fn start_with_url<U: AsRef<str>>(
        url: U,
        noria: ControllerHandle<A>,
        server_id: Option<u32>,
        log: Logger,
    ) -> ReadySetResult<()> {
        let options = url
            .as_ref()
            .parse()
            .map_err(|e| ReadySetError::ReplicationFailed(format!("Invalid URL format: {}", e)))?;

        NoriaAdapter::start_inner(noria, options, log, server_id).await
    }

    async fn start_inner(
        noria: ControllerHandle<A>,
        options: AdapterOpts,
        log: slog::Logger,
        server_id: Option<u32>,
    ) -> ReadySetResult<()> {
        match options {
            AdapterOpts::MySql(options) => {
                NoriaAdapter::start_inner_mysql(options, noria, server_id, log).await
            }
            AdapterOpts::Postgres(options) => {
                NoriaAdapter::start_inner_postgres(options, noria, log).await
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
        mut noria: ControllerHandle<A>,
        server_id: Option<u32>,
        log: slog::Logger,
    ) -> ReadySetResult<()> {
        // Attempt to retreive the latest replication offset from noria, if none is present
        // begin the snapshot process
        let pos = match noria.replication_offset().await?.map(Into::into) {
            None => {
                info!(log, "Taking database snapshot");

                let replicator_options = mysql_options.clone();
                let pool = mysql::Pool::new(replicator_options);
                let replicator = MySqlReplicator {
                    pool,
                    tables: None,
                    log: log.clone(),
                };

                replicator.replicate_to_noria(&mut noria, true).await?
            }
            Some(pos) => pos,
        };

        info!(log, "Binlog position {:?}", pos);

        let schemas = mysql_options
            .db_name()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();

        // TODO: it is possible that the binlog position from noria is no longer
        // present on the primary, in which case the connection will fail, and we would
        // need to perform a new snapshot
        let connector = Box::new(
            MySqlBinlogConnector::connect(mysql_options, schemas, pos.clone(), server_id).await?,
        );

        info!(log, "MySQL connected");

        let mut adapter = NoriaAdapter {
            noria,
            connector,
            mutator_map: HashMap::new(),
            log,
        };

        adapter.main_loop(pos.try_into()?).await
    }

    async fn start_inner_postgres(
        pgsql_opts: pgsql::Config,
        mut noria: ControllerHandle<A>,
        log: slog::Logger,
    ) -> ReadySetResult<()> {
        // Attempt to retreive the latest replication offset from noria, if none is present
        // begin the snapshot process
        let pos = noria.replication_offset().await?.map(Into::into);

        if let Some(pos) = pos {
            info!(log, "WAL position {}", pos);
        }

        let dbname = pgsql_opts
            .get_dbname()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();

        let mut connector = Box::new(
            PostgresWalConnector::connect(
                pgsql_opts.clone(),
                dbname.first().unwrap(),
                pos,
                log.clone(),
            )
            .await?,
        );

        info!(log, "Connected to PostgreSQL");

        if let Some(snapshot) = connector.snapshot_name.as_deref() {
            // If snapshot name exists, it means we need to make a snapshot to noria
            let (mut client, connection) = pgsql_opts
                .connect(postgres_native_tls::MakeTlsConnector::new(
                    native_tls::TlsConnector::builder().build().unwrap(),
                ))
                .await?;

            let connection_handle = tokio::spawn(connection);

            let mut replicator =
                PostgresReplicator::new(&mut client, &mut noria, None, log.clone()).await?;

            futures::select! {
                s = replicator.snapshot_to_noria(snapshot).fuse() => s?,
                c = connection_handle.fuse() => c.unwrap()?,
            }

            info!(log, "Snapshot finished");
        }

        connector
            .start_replication(REPLICATION_SLOT, PUBLICATION_NAME)
            .await?;

        info!(log, "Streaming replication started");

        let mut adapter = NoriaAdapter {
            noria,
            connector,
            mutator_map: HashMap::new(),
            log,
        };

        adapter.main_loop(PostgresPosition::default().into()).await
    }

    /// Handle a single BinlogAction by calling the proper Noria RPC
    async fn handle_action(
        &mut self,
        action: ReplicationAction,
        pos: ReplicationOffset,
    ) -> Result<(), ReadySetError> {
        match action {
            ReplicationAction::SchemaChange { ddl } => {
                // Send the query to Noria as is
                self.noria
                    .extend_recipe_with_offset(&ddl, Some(pos))
                    .await?;
                self.clear_mutator_cache();
            }

            ReplicationAction::TableAction {
                table,
                mut actions,
                txid,
            } => {
                // Send the rows as are
                let table_mutator = self.mutator_for_table(table).await?;
                actions.push(TableOperation::SetReplicationOffset(pos));
                table_mutator.perform_all(actions).await?;

                // If there was a transaction id associated, propagate the
                // timestamp with that transaction id
                // TODO(justin): Make this operation atomic with the table
                // actions being pushed above.
                if let Some(tx) = txid {
                    let mut timestamp = Timestamp::default();
                    timestamp.map.insert(table_mutator.node, tx);
                    table_mutator.update_timestamp(timestamp).await?;
                }
            }

            ReplicationAction::LogPosition => {
                // Update the log position
                self.noria.extend_recipe_with_offset("", Some(pos)).await?;
            }
        }

        Ok(())
    }

    /// Loop over the actions
    async fn main_loop(&mut self, mut position: ReplicationOffset) -> ReadySetResult<()> {
        loop {
            let (action, pos) = self.connector.next_action(position).await?;
            position = pos.clone();

            debug!(self.log, "{:?}", action);

            if let Err(err) = self.handle_action(action, pos).await {
                error!(self.log, "{}", err);
            }
        }
    }

    /// When schema changes there is a risk the cached mutators will no longer be in sync
    /// and we need to drop them all
    fn clear_mutator_cache(&mut self) {
        self.mutator_map.clear()
    }

    /// Get a mutator for a noria table from the cache if available, or fetch a new one
    /// from the controller and cache it
    async fn mutator_for_table(&mut self, name: String) -> Result<&mut Table, ReadySetError> {
        match self.mutator_map.entry(name) {
            hash_map::Entry::Occupied(o) => Ok(o.into_mut()),
            hash_map::Entry::Vacant(v) => {
                let table = self.noria.table(v.key()).await?;
                Ok(v.insert(table))
            }
        }
    }
}
