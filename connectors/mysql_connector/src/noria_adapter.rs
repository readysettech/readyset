use crate::connector::{BinlogAction, BinlogPosition, MySqlBinlogConnector};
use crate::snapshot::MySqlReplicator;
use noria::{consensus::Authority, ReplicationOffset, TableOperation};
use noria::{ControllerHandle, ReadySetError, ReadySetResult, Table, ZookeeperAuthority};
use slog::{error, info, o, Discard, Logger};
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;

/// An adapter that converts binlog actions into Noria API calls
pub struct MySqlNoriaAdapter<A: Authority + 'static> {
    /// The Noria API handle
    noria: ControllerHandle<A>,
    /// The binlog reader
    connector: MySqlBinlogConnector,
    /// A map of cached table mutators
    mutator_map: HashMap<String, Table>,
    /// Logger
    log: Logger,
}

#[derive(Default)]
pub struct Builder {
    addr: String,
    port: u16,
    snapshot: bool,
    user: Option<String>,
    password: Option<String>,
    db_name: Option<String>,
    zookeeper_address: Option<String>,
    deployment: Option<String>,
    position: Option<BinlogPosition>,
    server_id: Option<u32>,
    log: Option<Logger>,
}

impl Builder {
    /// Create a new builder with the given MySQL primary server address and port
    pub fn new<T: Into<String>>(addr: T, port: u16) -> Self {
        Builder {
            addr: addr.into(),
            port,
            ..Default::default()
        }
    }

    /// A user name for MySQL
    /// The user must have the following permissions:
    /// `SELECT` - to be able to perform a snapshot (WIP)
    /// `RELOAD` - to be able to flush tables and acquire locks for a snapshot (WIP)
    /// `SHOW DATABASES` - to see databases for a snapshot (WIP)
    /// `REPLICATION SLAVE` - to be able to connect and read the binlog
    /// `REPLICATION CLIENT` - to use SHOW MASTER STATUS, SHOW SLAVE STATUS, and SHOW BINARY LOGS;
    pub fn with_user<T: Into<String>>(mut self, user_name: Option<T>) -> Self {
        self.user = user_name.map(Into::into);
        self
    }

    /// The password for the MySQL user
    pub fn with_password<T: Into<String>>(mut self, password: Option<T>) -> Self {
        self.password = password.map(Into::into);
        self
    }

    /// The name of the database to filter, if none is provided all entries will be filtered out
    pub fn with_database<T: Into<String>>(mut self, db_name: Option<T>) -> Self {
        self.db_name = db_name.map(Into::into);
        self
    }

    /// The address of the zookeeper instance for Noria
    pub fn with_zookeeper_addr<T: Into<String>>(mut self, zookeeper_address: Option<T>) -> Self {
        self.zookeeper_address = zookeeper_address.map(Into::into);
        self
    }

    /// The name of the Noria deployment
    pub fn with_deployment<T: Into<String>>(mut self, deployment: Option<T>) -> Self {
        self.deployment = deployment.map(Into::into);
        self
    }

    /// The position where to start reading the binlog
    pub fn with_position(mut self, position: Option<BinlogPosition>) -> Self {
        self.position = position;
        self
    }

    /// The binlog replica must be assigned a unique `server_id` in the replica topology
    pub fn with_server_id(mut self, server_id: Option<u32>) -> Self {
        self.server_id = server_id;
        self
    }

    /// If true, will replicate the target database in its entirety to Noria before listening on the binlog
    /// The replication happens in stages:
    /// * READ LOCK is acquired on the database
    /// * Next binlog position is read
    /// * The recipe (schema) DDL is replicated and installed in Noria (replacing current recipe)
    /// * Each table is individually replicated into Noria
    /// * READ LOCK is released
    /// * Adapter keeps reading binlog from the next position keeping Noria up to date
    pub fn with_snapshot(mut self, do_snapshot: bool) -> Self {
        self.snapshot = do_snapshot;
        self
    }

    pub fn with_logger(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    /// Finish the build and begin monitoring the binlog for changes
    pub async fn start(self) -> ReadySetResult<()> {
        let zookeeper_address = self
            .zookeeper_address
            .unwrap_or_else(|| "127.0.0.1:2181".into());

        let deployment = self
            .deployment
            .ok_or_else(|| ReadySetError::ReplicationFailed("Missing deployment".into()))?;

        let authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_address, deployment))?;
        let mut noria = noria::ControllerHandle::new(authority).await?;

        let mysql_options = mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.addr)
            .tcp_port(self.port)
            .user(self.user)
            .pass(self.password);

        let log = self.log.unwrap_or_else(|| Logger::root(Discard, o!()));

        // In case the snapshot option was specified, take the snapshot and then keep reading from
        // the binlog position specified in the snapshot
        let pos = if self.snapshot {
            info!(log, "Taking database snapshot");
            let replicator_options = mysql_options
                .clone()
                .db_name(self.db_name.clone())
                .compression(Some(mysql_async::Compression::new(7)));

            let pool = mysql_async::Pool::new(replicator_options);
            let replicator = MySqlReplicator {
                pool,
                tables: None,
                log: log.clone(),
            };
            Some(replicator.replicate_to_noria(&mut noria, true).await?)
        } else {
            self.position
        };

        if let Some(pos) = &pos {
            info!(log, "Binlog position {:?}", pos);
        }

        let connector = MySqlBinlogConnector::connect(
            mysql_options,
            self.db_name.map(|s| vec![s]).unwrap_or_default(),
            pos,
            self.server_id,
        )
        .await?;

        info!(log, "MySQL connected");

        let mut adapter = MySqlNoriaAdapter {
            noria,
            connector,
            mutator_map: HashMap::new(),
            log,
        };

        adapter.main_loop().await
    }
}

impl<A: Authority> MySqlNoriaAdapter<A> {
    /// Handle a single BinlogAction by calling the proper Noria RPC
    async fn handle_action(
        &mut self,
        action: BinlogAction,
        pos: Option<ReplicationOffset>,
    ) -> Result<(), ReadySetError> {
        match action {
            BinlogAction::SchemaChange(schema) => {
                // Send the query to Noria as is
                self.noria.extend_recipe_with_offset(&schema, pos).await?;
                self.clear_mutator_cache();
                Ok(())
            }

            BinlogAction::WriteRows { table, mut rows }
            | BinlogAction::DeleteRows { table, mut rows }
            | BinlogAction::UpdateRows { table, mut rows } => {
                // Send the rows as are
                let table_mutator = self.mutator_for_table(table).await?;
                if let Some(offset) = pos {
                    // Update the replication offset
                    rows.push(TableOperation::SetReplicationOffset(offset));
                }
                table_mutator.perform_all(rows).await
            }
        }
    }

    /// Loop over the actions
    async fn main_loop(&mut self) -> ReadySetResult<()> {
        loop {
            let (action, position) = self.connector.next_action().await?;
            info!(self.log, "{:?}", action);
            let offset = position.map(|r| r.try_into()).transpose()?;
            if let Err(err) = self.handle_action(action, offset).await {
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
