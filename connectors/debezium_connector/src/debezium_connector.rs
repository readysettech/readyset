extern crate serde_json;
use anyhow::{anyhow, Context};
use noria::consensus::ZookeeperAuthority;
use noria::consistency::Timestamp;
use noria::{ControllerHandle, ReadySetError};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::str::FromStr;
use thiserror::Error;
use tokio::stream::StreamExt;
use tracing::{error, info, trace, warn};

mod debezium_message_parser;
mod kafka_message_consumer_wrapper;
use debezium_message_parser::{
    DataChange, DataChangePayload, EventKey, EventValue, SchemaChange, Transaction,
};

use self::{
    debezium_message_parser::DataCollection,
    kafka_message_consumer_wrapper::KafkaMessageConsumerWrapper,
};

/// The database that debezium is connected to. This can impact message
/// structure and topic naming.
#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
}

impl Default for DatabaseType {
    fn default() -> Self {
        DatabaseType::MySQL
    }
}

impl FromStr for DatabaseType {
    type Err = anyhow::Error;
    fn from_str(input: &str) -> anyhow::Result<Self> {
        match input.to_lowercase().as_str() {
            "mysql" => Ok(DatabaseType::MySQL),
            "postgres" => Ok(DatabaseType::PostgreSQL),
            _ => Err(anyhow!(
                "Invalid database type cannot be converted to DatabaseType"
            )),
        }
    }
}

/// Kafka topics the debezium connector reads from.
enum Topic {
    /// Schema change events that include all DDL statements applied to a database.
    SchemaChange,
    /// Data changes associated with each row-level INSERT, UPDATE, or DELETE.
    DataChange,
    /// Transaction metadata for any transaction written to the databases.
    Transaction,
}

/// Errors encountered when handling a messagae from Kafka
#[derive(Debug, Error)]
enum MessageError {
    /// Errors caused by invalid messages.
    ///
    /// Since messages that cause these errors cannot be retried,
    /// if an error of this type occurs while handling a message the message will still be committed
    /// in Kafka
    #[error("Invalid message from kafka: {0}")]
    InvalidMessage(anyhow::Error),

    /// Recoverable errors encountered when handling a message.
    ///
    /// If an error of this type occurs while handling a message the message will *not* be committed
    /// in Kafka
    ///
    /// Since falsely marking an error as internal is safer than falsely marking an error as
    /// invalid, this is the default variant given by the [`From`] impl for this type.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl MessageError {
    fn invalid<E>(e: E) -> Self
    where
        E: std::error::Error + 'static + Send + Sync,
    {
        Self::InvalidMessage(e.into())
    }
}

#[derive(Debug, Default)]
pub struct Builder {
    bootstrap_servers: Option<String>,
    server_name: Option<String>,
    db_name: Option<String>,
    tables: Vec<String>,
    group_id: Option<String>,
    zookeeper_address: Option<String>,
    deployment: Option<String>,
    timeout: Option<String>,
    eof: bool,
    auto_commit: bool,
    database_type: DatabaseType,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_bootstrap_servers<S>(&mut self, bootstrap_servers: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.bootstrap_servers = Some(bootstrap_servers.to_owned());
        self
    }

    pub fn set_server_name<S>(&mut self, server_name: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.server_name = Some(server_name.to_owned());
        self
    }

    pub fn set_db_name<S>(&mut self, db_name: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.db_name = Some(db_name.to_owned());
        self
    }

    pub fn set_tables(&mut self, tables: Vec<String>) -> &mut Self {
        self.tables = tables;
        self
    }

    pub fn set_group_id<S>(&mut self, group_id: Option<&S>) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.group_id = group_id.map(|x| x.to_owned());
        self
    }

    pub fn set_zookeeper_address<S>(&mut self, zookeeper_address: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.zookeeper_address = Some(zookeeper_address.to_owned());
        self
    }

    pub fn set_deployment<S>(&mut self, deployment: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.deployment = Some(deployment.to_owned());
        self
    }

    pub fn set_timeout<S>(&mut self, timeout: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.timeout = Some(timeout.to_owned());
        self
    }

    pub fn set_eof(&mut self, eof: bool) -> &mut Self {
        self.eof = eof;
        self
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) -> &mut Self {
        self.auto_commit = auto_commit;
        self
    }

    pub fn set_database_type(&mut self, db: DatabaseType) -> &mut Self {
        self.database_type = db;
        self
    }

    pub async fn build(&self) -> anyhow::Result<DebeziumConnector> {
        // for each table, we listen to the topic <dbserver>.<dbname>.<tablename>
        let mut topic_names: Vec<String> = Vec::new();
        let mut topics: HashMap<String, Topic> = HashMap::new();
        self.tables
            .iter()
            .map(|table_name| {
                format!(
                    "{}.{}.{}",
                    self.server_name.as_ref().unwrap(),
                    self.db_name.as_ref().unwrap(),
                    table_name
                )
            })
            .for_each(|t| {
                topic_names.push(t.clone());
                topics.insert(t, Topic::DataChange);
            });

        // we also listen to the schema change topic, which is just named <dbserver>
        topic_names.push(self.server_name.clone().unwrap());
        topics.insert(self.server_name.clone().unwrap(), Topic::SchemaChange);

        let transaction_topic = self.server_name.clone().unwrap() + ".transaction";
        topic_names.push(transaction_topic.clone());
        topics.insert(transaction_topic, Topic::Transaction);

        let kafka_consumer = Some(KafkaMessageConsumerWrapper::new(
            self.bootstrap_servers.clone().unwrap(),
            topic_names,
            self.group_id.clone().unwrap_or_else(|| {
                format!(
                    "{}.{}",
                    self.server_name.as_ref().unwrap(),
                    self.db_name.as_ref().unwrap()
                )
            }),
            self.timeout.clone().unwrap(),
            self.eof,
            self.auto_commit,
        )?);

        let zookeeper_address = self.zookeeper_address.as_ref().unwrap().as_str();
        let deployment = self.deployment.as_ref().unwrap().as_str();
        info!(zookeeper_address, deployment, "Connecting to Noria");
        let authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_address, deployment))?;
        let noria = ControllerHandle::new(authority).await?;
        info!("Connection to Noria established");

        Ok(DebeziumConnector {
            kafka_consumer,
            topics,
            noria,
            database_type: self.database_type,
        })
    }
}

pub struct DebeziumConnector {
    kafka_consumer: Option<KafkaMessageConsumerWrapper>,
    topics: HashMap<String, Topic>,
    noria: ControllerHandle<ZookeeperAuthority>,
    database_type: DatabaseType,
}

impl DebeziumConnector {
    pub fn builder() -> Builder {
        Builder::new()
    }

    async fn handle_schema_message(&mut self, message: SchemaChange) -> Result<(), MessageError> {
        let ddl = message.payload.ddl.as_str();
        info!(ddl, "Handling schema change message");

        if let Err(err) = self.noria.extend_recipe(ddl).await {
            if err.caused_by_unparseable_query() {
                warn!(
                    ddl,
                    err = err.to_string().as_str(),
                    "Schema change message failed to parse, ignoring"
                );
                return Err(MessageError::InvalidMessage(err.into()));
            } else {
                return Err(MessageError::Internal(err.into()));
            }
        }

        Ok(())
    }

    async fn handle_change_message(
        &mut self,
        key_message: Option<EventKey>,
        message: DataChange,
    ) -> anyhow::Result<()> {
        trace!("Handling data change message");
        match &message.payload {
            DataChangePayload::Create(p) => {
                // We know that the payload consist of before, after and source fields
                // and that too in that specific order.
                if let Some(table_name) = &p.source.table {
                    let after_field_schema = &message.schema.fields[1];
                    let mut table_mutator =
                        self.noria.table(table_name).await.with_context(|| {
                            format!("Fetching builder for table \"{}\"", table_name)
                        })?;
                    let create_vector =
                        p.get_create_vector(after_field_schema, table_mutator.schema())?;
                    trace!("inserting 1 row into table {}", table_name);
                    table_mutator.insert(create_vector).await?
                }
            }
            DataChangePayload::Update(p) => {
                if let Some(table_name) = &p.source.table {
                    let pk_datatype = key_message
                        .ok_or_else(|| {
                            MessageError::InvalidMessage(anyhow!(
                                "Update data change message missing key"
                            ))
                        })?
                        .get_pk_datatype()?;
                    // We know that the payload consist of before, after and source fields
                    // and that too in that specific order.
                    let after_field_schema = &message.schema.fields[1];
                    let mut table_mutator =
                        self.noria.table(table_name).await.with_context(|| {
                            format!("Fetching builder for table \"{}\"", table_name)
                        })?;
                    let update_vector =
                        p.get_update_vector(after_field_schema, table_mutator.schema())?;
                    table_mutator
                        .update(vec![pk_datatype], update_vector)
                        .await?
                }
            }
            DataChangePayload::Delete { source: src } => {
                if let Some(table_name) = &src.table {
                    let pk_datatype = key_message
                        .ok_or_else(|| {
                            MessageError::InvalidMessage(anyhow!(
                                "Delete data change message missing key"
                            ))
                        })?
                        .get_pk_datatype()?;
                    let mut table_mutator =
                        self.noria.table(table_name).await.with_context(|| {
                            format!("Fetching builder for table \"{}\"", table_name)
                        })?;
                    table_mutator.delete(vec![pk_datatype]).await?
                }
            }
        }
        Ok(())
    }

    /// Parse a transaction id from the transaction marker messages identifier.
    /// The format of the transaction marker identifier changes based on the type
    /// of database debezium is connected to.
    fn parse_transaction_id(&self, id: String) -> anyhow::Result<u64> {
        let gtid_seq = match self.database_type {
            DatabaseType::MySQL => {
                // The GTID is in the form <source_id>:<transaction_id>.
                let mut gtid_tokens = id.split(':');
                gtid_tokens
                    .nth(1)
                    .ok_or_else(|| anyhow!("Failed to parse GTID from string"))?
            }
            DatabaseType::PostgreSQL => id.as_str(),
        };

        gtid_seq
            .parse()
            .map_err(|_| anyhow!("Failed to convert GTID to u64"))
    }

    /// Parse the readyset table names from the debezium data collection names.
    fn parse_table_names<'a>(
        &self,
        collections: &'a [DataCollection],
    ) -> anyhow::Result<Vec<&'a str>> {
        // PostgreSQL data collections are in the format <schema>.<tablename>
        // MySQL data bollections are in the format <dbname>.<tablename>
        // In both cases, we only want the second token.
        collections
            .iter()
            .map(|c| {
                let mut tokens = c.data_collection.split('.');
                tokens
                    .nth(1)
                    .ok_or_else(|| anyhow!("Data collection did not include a valid table name"))
            })
            .collect()
    }

    /// Processes a BEGIN or END transaction message.
    /// When a transaction end message is received, we will increment the
    /// timestamps associated with each changed base table. This new timestamp
    /// will be propagated on the data flow graph.
    async fn handle_transaction_message(&mut self, message: Transaction) -> anyhow::Result<()> {
        trace!("Handling transaction message");
        let payload = &message.payload;
        // We currently do not process payload begin messages or transactions
        // that have not modified anys
        if payload.status == "BEGIN" || payload.data_collections.is_none() {
            return Ok(());
        }

        let collections = payload
            .data_collections
            .as_ref()
            .ok_or_else(|| anyhow!("Transaction metadata had no data collections"))?;
        let tables = self.parse_table_names(collections.as_slice())?;
        let gtid_seq = self.parse_transaction_id(payload.id.clone())?;

        for table in tables {
            // Propagate any collection naming errors
            let mut table_mutator = self.noria.table(table).await?;
            let mut timestamp = Timestamp::default();
            timestamp.map.insert(table_mutator.node, gtid_seq);
            table_mutator.update_timestamp(timestamp).await?;
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: &BorrowedMessage<'_>) -> Result<(), MessageError> {
        trace!("Handling message");
        let owned_message = message.detach();

        let payload = if let Some(payload) = owned_message.payload() {
            payload
        } else {
            trace!("Received message with empty payload");
            return Ok(());
        };

        let topic = self.topics.get(owned_message.topic()).unwrap();

        match topic {
            Topic::SchemaChange => {
                let value_string = std::str::from_utf8(payload).map_err(MessageError::invalid)?;
                let value_message: EventValue =
                    serde_json::from_str(&value_string).map_err(MessageError::invalid)?;

                self.handle_schema_message(value_message.try_into().unwrap())
                    .await?;
            }
            Topic::DataChange => {
                // We have to check existence because on deletes, a tombstone message is
                // sent by the kafka connector.  We really dont use the for anything, so we
                // just ignore them for now.
                let key_message = owned_message
                    .key()
                    .map(|k| -> anyhow::Result<_> {
                        let key_string = std::str::from_utf8(k).map_err(MessageError::invalid)?;
                        let key_message =
                            serde_json::from_str(&key_string).map_err(MessageError::invalid)?;
                        Ok(key_message)
                    })
                    .transpose()?;
                let value_string = std::str::from_utf8(payload).map_err(MessageError::invalid)?;
                let value_message: EventValue =
                    serde_json::from_str(&value_string).map_err(MessageError::invalid)?;

                self.handle_change_message(key_message, value_message.try_into().unwrap())
                    .await?;
            }
            Topic::Transaction => {
                if let Some(payload) = owned_message.payload() {
                    let transaction =
                        std::str::from_utf8(payload).map_err(MessageError::invalid)?;
                    let transaction: Transaction =
                        serde_json::from_str(transaction).map_err(MessageError::invalid)?;

                    self.handle_transaction_message(transaction).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("Running debezium connector");
        let consumer = self
            .kafka_consumer
            .take()
            .expect("Cannot start() a debezium connector twice");
        let mut message_stream = consumer.kafka_consumer.start();

        while let Some(message) = message_stream.next().await {
            match message {
                Ok(message) => {
                    if let Err(e) = self.handle_message(&message).await {
                        error!("Error handling message: {:#}", e);
                        if matches!(e, MessageError::InvalidMessage(_)) {
                            // Continue so we don't commit the message
                            continue;
                        }
                    }

                    consumer
                        .kafka_consumer
                        .commit_message(&message, CommitMode::Async)?;
                }
                Err(e) => error!(
                    error = e.to_string().as_str(),
                    "Received error from kafka message stream"
                ),
            }
        }
        Ok(())
    }
}
