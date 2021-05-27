use crate::connector::{PostgresPosition, PostgresWalConnector, WalAction};
use noria::consensus::Authority;
use noria::{ControllerHandle, ReadySetError, ReadySetResult, Table, ZookeeperAuthority};
use slog::{error, info, o, Discard, Logger};
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;

/// An adapter that converts WAL actions into Noria API calls
pub struct PostgreSqlNoriaAdapter<A: Authority + 'static> {
    /// The Noria API handle
    noria: ControllerHandle<A>,
    /// The WAL reader
    connector: PostgresWalConnector,
    /// A map of cached table mutators
    mutator_map: HashMap<String, Table>,
    /// Logger
    log: Logger,
}

#[derive(Default)]
pub struct Builder {
    addr: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    db_name: Option<String>,
    zookeeper_address: Option<String>,
    deployment: Option<String>,
    log: Option<Logger>,
}

impl Builder {
    /// Create a new builder with the given PostgreSQL primary server address and port
    pub fn new<T: Into<String>>(addr: T, port: u16) -> Self {
        Builder {
            addr: addr.into(),
            port,
            ..Default::default()
        }
    }

    /// A user name for PostgreSQL
    pub fn with_user<T: Into<String>>(mut self, user_name: Option<T>) -> Self {
        self.user = user_name.map(Into::into);
        self
    }

    /// The password for the PostgreSQL user
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

    pub fn with_logger(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    async fn start_inner<A: Authority>(
        postgres_options: tokio_postgres::Config,
        mut noria: ControllerHandle<A>,
        log: slog::Logger,
    ) -> ReadySetResult<()> {
        // Attempt to retreive the latest replication offset from noria, if none is present
        // begin the snapshot process
        let pos = match noria.replication_offset().await?.map(Into::into) {
            None => PostgresPosition { lsn: 0 },
            Some(pos) => pos,
        };

        info!(log, "WAL position {}", pos);

        let dbname = postgres_options
            .get_dbname()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();

        let connector = PostgresWalConnector::connect(
            postgres_options,
            dbname.first().unwrap(),
            Some(pos),
            log.clone(),
        )
        .await?;

        info!(log, "PostgreSQL connected");

        let mut adapter = PostgreSqlNoriaAdapter {
            noria,
            connector,
            mutator_map: HashMap::new(),
            log,
        };

        adapter.main_loop().await
    }

    /// Finish the build and begin monitoring the WAL for changes
    /// If noria has no replication offset information, it will replicate the target database in its
    /// entirety to Noria before listening on the WAL
    pub async fn start(self) -> ReadySetResult<()> {
        let zookeeper_address = self
            .zookeeper_address
            .unwrap_or_else(|| "127.0.0.1:2181".into());

        let deployment = self
            .deployment
            .ok_or_else(|| ReadySetError::ReplicationFailed("Missing deployment".into()))?;

        let authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_address, deployment))?;
        let noria = noria::ControllerHandle::new(authority).await?;

        let mut postgres_options = tokio_postgres::Config::new();

        postgres_options.host(&self.addr).port(self.port);
        if let Some(user) = self.user {
            postgres_options.user(&user);
        }

        if let Some(password) = self.password {
            postgres_options.password(&password);
        }

        if let Some(db_name) = self.db_name {
            postgres_options.dbname(&db_name);
        }

        let log = self.log.unwrap_or_else(|| Logger::root(Discard, o!()));

        Self::start_inner(postgres_options, noria, log).await
    }

    /// Same as [`start`](Builder::start), but accepts a PostgreSQL url for options
    /// and externally supplied Noria `ControllerHandle` and `log`.
    /// The PostgreSQL url must contain the database name, and user and password if applicable.
    /// i.e. `postgresql://user:pass%20word@localhost/database_name`
    #[allow(dead_code)]
    pub async fn start_with_url<A: Authority>(
        postgres_url: &str,
        noria: ControllerHandle<A>,
        log: slog::Logger,
    ) -> ReadySetResult<()> {
        let postgres_options = postgres_url.parse().map_err(|e| {
            ReadySetError::ReplicationFailed(format!("Invalid PostgreSQL URL format {}", e))
        })?;

        Self::start_inner(postgres_options, noria, log).await
    }
}

impl<A: Authority> PostgreSqlNoriaAdapter<A> {
    /// Loop over the actions
    async fn main_loop(&mut self) -> ReadySetResult<()> {
        let mut last_ack = PostgresPosition { lsn: 0 };

        loop {
            let action = self.connector.next_action(last_ack).await?;
            info!(self.log, "{:?}", action);

            let WalAction::TableAction {
                table,
                mut actions,
                lsn,
            } = action;

            actions.push(noria::TableOperation::SetReplicationOffset(lsn.try_into()?));

            let table_mutator = self.mutator_for_table(table).await?;

            if let Err(err) = table_mutator.perform_all(actions).await {
                error!(self.log, "{}", err);
            }

            last_ack = lsn;
        }
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
