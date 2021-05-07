use crate::connector::{BinlogAction, BinlogPosition, MySqlBinlogConnector};
use noria::{ControllerHandle, ReadySetError, Table, ZookeeperAuthority};
use std::collections::HashMap;
use tracing::info;

/// An adapter that converts binlog actions into Noria API calls
pub struct MySqlNoriaAdapter {
    /// The Noria API handle
    noria: ControllerHandle<ZookeeperAuthority>,
    /// The binlog reader
    connector: MySqlBinlogConnector,
    /// A map of cached table mutators
    mutator_map: HashMap<String, (Table, Vec<usize>)>,
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
    position: Option<BinlogPosition>,
    server_id: Option<u32>,
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

    /// Finish the build and begin monitoring the binlog for changes
    pub async fn start(self) -> Result<(), mysql_async::Error> {
        let zookeeper_address = self
            .zookeeper_address
            .unwrap_or_else(|| "127.0.0.1:2181".into());
        let deployment = self.deployment.ok_or("Missing depoyement")?;
        let authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_address, deployment))
            .map_err(|err| format!("Zookeeper error {}", err))?;
        let noria = noria::ControllerHandle::new(authority)
            .await
            .map_err(|err| format!("Controller error {}", err))?;

        info!("Noria connected");

        let connector = MySqlBinlogConnector::connect(
            self.addr,
            self.port,
            self.user,
            self.password,
            self.db_name.map(|s| vec![s]).unwrap_or_default(),
            self.position,
            self.server_id,
        )
        .await?;

        info!("MySQL connected");

        let mut adapter = MySqlNoriaAdapter {
            noria,
            connector,
            mutator_map: HashMap::new(),
        };
        adapter.main_loop().await
    }
}

impl MySqlNoriaAdapter {
    /// Handle a single BinlogAction by calling the proper Noria RPC
    async fn handle_action(&mut self, action: BinlogAction) -> Result<(), ReadySetError> {
        match action {
            BinlogAction::SchemaChange(schema) => {
                // Send the query to Noria as is
                self.noria.extend_recipe(&schema).await?;
                self.clear_mutator_cache();
                Ok(())
            }

            BinlogAction::WriteRows { table, rows } => {
                // Send the rows as are
                let (table_mutator, _) = self.mutator_for_table(table).await?;
                table_mutator.insert_many(rows).await
            }

            BinlogAction::DeleteRows { table, rows } => {
                // For delete operations we must first convert the "before_cols" to key cols
                let (table_mutator, key_indices) = self.mutator_for_table(table).await?;

                for row in rows {
                    table_mutator
                        .delete(cols_to_key_cols(row, &key_indices))
                        .await?;
                }
                Ok(())
            }

            BinlogAction::UpdateRows { table, rows } => {
                // For update operations we must first convert the "before_cols" to key cols
                let (table_mutator, key_indices) = self.mutator_for_table(table).await?;

                for row in rows {
                    let (before, after) = row;
                    table_mutator
                        .update(
                            cols_to_key_cols(before, &key_indices),
                            cols_to_modification_vec(after),
                        )
                        .await?;
                }

                Ok(())
            }
        }
    }

    /// Loop over the actions
    async fn main_loop(&mut self) -> Result<(), mysql_async::Error> {
        loop {
            let (action, _position) = self.connector.next_action().await?;
            info!("{:?}", action);
            if let Err(err) = self.handle_action(action).await {
                tracing::error!("{}", err);
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
    async fn mutator_for_table(
        &mut self,
        name: String,
    ) -> Result<(&mut Table, &[usize]), ReadySetError> {
        match self.mutator_map.entry(name) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let (table, key_indices) = o.into_mut();
                Ok((table, key_indices))
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                let table = self.noria.table(v.key()).await?;
                let key_indices = key_indices_for_schema(table.schema().unwrap())?;
                let (table, key_indices) = v.insert((table, key_indices));
                Ok((table, key_indices))
            }
        }
    }
}

/// Attempt to decide which column indices of the schema are the key columns
fn key_indices_for_schema(
    schema: &nom_sql::CreateTableStatement,
) -> Result<Vec<usize>, ReadySetError> {
    let mut key_indices = Vec::with_capacity(2); // Most tables will have 1 key column, few have 2
    let primary_key_columns = schema
        .keys
        .as_ref()
        .and_then(|keys| {
            keys.iter().find_map(|key| match key {
                // Right now noria is unable to perform updates on tables with no primary key defined
                nom_sql::TableKey::PrimaryKey(ref cols) => Some(cols),
                _ => None,
            })
        })
        .ok_or(ReadySetError::EmptyKey)?;

    for (idx, field) in schema.fields.iter().enumerate() {
        if primary_key_columns.contains(&field.column) {
            key_indices.push(idx)
        }
    }

    Ok(key_indices)
}

fn cols_to_modification_vec(sql_cols: Vec<noria::DataType>) -> Vec<(usize, noria::Modification)> {
    sql_cols
        .into_iter()
        .enumerate()
        .map(|(idx, val)| (idx, noria::Modification::Set(val)))
        .collect::<Vec<_>>()
}

fn cols_to_key_cols(sql_cols: Vec<noria::DataType>, key_indices: &[usize]) -> Vec<noria::DataType> {
    sql_cols
        .into_iter()
        .enumerate()
        .filter_map(|(idx, val)| {
            if key_indices.contains(&idx) {
                Some(val)
            } else {
                None
            }
        })
        .collect()
}
