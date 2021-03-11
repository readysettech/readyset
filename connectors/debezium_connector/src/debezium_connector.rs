extern crate serde_json;

use anyhow::{Error, Result};
use noria::consensus::ZookeeperAuthority;
use noria::consistency::Timestamp;
use noria::ControllerHandle;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use std::collections::HashMap;
use std::convert::TryInto;
use tokio::stream::StreamExt;

mod debezium_message_parser;
mod kafka_message_consumer_wrapper;
use debezium_message_parser::{
    DataChange, DataChangePayload, EventKey, EventValue, SchemaChange, Transaction,
};

/// Kafka topics the debezium connector reads from.
enum Topic {
    /// Schema change events that include all DDL statements applied to a database.
    SchemaChange,
    /// Data changes associated with each row-level INSERT, UPDATE, or DELETE.
    DataChange,
    /// Transaction metadata for any transaction written to the databases.
    Transaction,
}

pub struct DebeziumConnector {
    kafka_consumer: kafka_message_consumer_wrapper::KafkaMessageConsumerWrapper,
    zookeeper_conn: String,
    topics: HashMap<String, Topic>,
}

#[derive(Debug, Default)]
pub struct Builder {
    bootstrap_servers: Option<String>,
    server_name: Option<String>,
    db_name: Option<String>,
    tables: Vec<String>,
    group_id: Option<String>,
    zookeeper_conn: Option<String>,
    timeout: Option<String>,
    eof: bool,
    auto_commit: bool,
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

    pub fn set_group_id<S>(&mut self, group_id: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.group_id = Some(group_id.to_owned());
        self
    }

    pub fn set_zookeeper_conn<S>(&mut self, zookeeper_conn: &S) -> &mut Self
    where
        S: ToOwned<Owned = String> + ?Sized,
    {
        self.zookeeper_conn = Some(zookeeper_conn.to_owned());
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

    pub fn build(&self) -> DebeziumConnector {
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

        let consumer = kafka_message_consumer_wrapper::KafkaMessageConsumerWrapper::new(
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
        );

        DebeziumConnector {
            kafka_consumer: consumer,
            zookeeper_conn: self.zookeeper_conn.clone().unwrap(),
            topics,
        }
    }
}

impl DebeziumConnector {
    pub fn builder() -> Builder {
        Builder::new()
    }

    async fn handle_schema_message(
        noria_authority: &mut ControllerHandle<ZookeeperAuthority>,
        message: SchemaChange,
    ) -> Result<()> {
        noria_authority.extend_recipe(&message.payload.ddl).await?;
        Ok(())
    }

    async fn handle_change_message(
        noria_authority: &mut ControllerHandle<ZookeeperAuthority>,
        key_message: EventKey,
        message: DataChange,
    ) -> Result<()> {
        match &message.payload {
            DataChangePayload::Create(p) => {
                // We know that the payload consist of before, after and source fields
                // and that too in that specific order.
                if let Some(table_name) = &p.source.table {
                    let after_field_schema = &message.schema.fields[1];
                    let create_vector = p.get_create_vector(after_field_schema)?;
                    let mut table_mutator = noria_authority.table(table_name).await?;
                    table_mutator.insert(create_vector).await?
                }
            }
            DataChangePayload::Update(p) => {
                if let Some(table_name) = &p.source.table {
                    let pk_datatype = key_message.get_pk_datatype()?;
                    // We know that the payload consist of before, after and source fields
                    // and that too in that specific order.
                    let after_field_schema = &message.schema.fields[1];
                    let update_vector = p.get_update_vector(after_field_schema)?;
                    let mut table_mutator = noria_authority.table(table_name).await?;
                    table_mutator
                        .update(vec![pk_datatype], update_vector)
                        .await?
                }
            }
            DataChangePayload::Delete { source: src } => {
                if let Some(table_name) = &src.table {
                    let pk_datatype = key_message.get_pk_datatype()?;
                    let mut table_mutator = noria_authority.table(table_name).await?;
                    table_mutator.delete(vec![pk_datatype]).await?
                }
            }
        }
        Ok(())
    }

    /// Processes a BEGIN or END transaction message.
    /// When a transaction end message is received, we will increment the
    /// timestamps associated with each changed base table. This new timestamp
    /// will be propagated on the data flow graph.
    async fn handle_transaction_message(
        noria_authority: &mut ControllerHandle<ZookeeperAuthority>,
        message: Transaction,
    ) -> Result<()> {
        let payload = &message.payload;
        // We currently do not process payload begin messages or transactions
        // that have not modified anys
        if payload.status == "BEGIN" || payload.data_collections.is_none() {
            return Ok(());
        }

        // TODO(justin): Create an error type for debezium connector errors and
        // refactor error handling.
        let collections = payload
            .data_collections
            .as_ref()
            .ok_or_else(|| Error::msg("Transaction metadata had no data collections"))?;
        let tables = collections.iter().map(|c| {
            let mut tokens = c.data_collection.split('.');

            // Postgres and MySql have different data collection naming schemes.
            // Postgres names tables as: schema.table, while MySql uses: table.
            // Split the data collection name by the '.' character. If it is postgres,
            // there will be two tokens and we return the second.
            // TODO(justin): Wrap parsing different data collection naming schemes.
            let first = tokens.next();
            let second = tokens.next();

            match second {
                Some(t) => Ok(t),
                None => first.ok_or_else(|| {
                    Error::msg("Data collection did not include a valid table name")
                }),
            }
        });

        for table in tables {
            // Propagate any collection naming errors
            let mut table_mutator = noria_authority.table(table?).await?;
            table_mutator.update_timestamp(Timestamp::default()).await?;
        }
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut message_stream = self.kafka_consumer.kafka_consumer.start();

        let authority = ZookeeperAuthority::new(&self.zookeeper_conn)?;
        let mut ch = ControllerHandle::new(authority).await?;
        while let Some(message) = message_stream.next().await {
            if let Ok(m) = message {
                let owned_message = m.detach();

                if owned_message.payload().is_none() {
                    self.kafka_consumer
                        .kafka_consumer
                        .commit_message(&m, CommitMode::Async)?;
                    return Ok(());
                }

                let value_string = std::str::from_utf8(owned_message.payload().unwrap())?;
                let value_message: EventValue = serde_json::from_str(&value_string)?;
                let topic = self.topics.get(owned_message.topic()).unwrap();

                match topic {
                    Topic::SchemaChange => {
                        DebeziumConnector::handle_schema_message(
                            &mut ch,
                            value_message.try_into().unwrap(),
                        )
                        .await?;
                    }
                    Topic::DataChange => {
                        // We have to check existence because on deletes, a tombstone message is sent by the kafka connector.
                        // We really dont use the for anything, so we just ignore them for now.
                        let key_string = std::str::from_utf8(owned_message.key().unwrap())?;
                        let key_message = serde_json::from_str(&key_string)?;
                        DebeziumConnector::handle_change_message(
                            &mut ch,
                            key_message,
                            value_message.try_into().unwrap(),
                        )
                        .await?;
                    }
                    Topic::Transaction => {
                        if owned_message.payload().is_some() {
                            let transaction =
                                std::str::from_utf8(owned_message.payload().unwrap())?;
                            let transaction: Transaction = serde_json::from_str(transaction)?;

                            DebeziumConnector::handle_transaction_message(&mut ch, transaction)
                                .await?;
                        }
                    }
                }

                self.kafka_consumer
                    .kafka_consumer
                    .commit_message(&m, CommitMode::Async)?;
            }
        }
        Ok(())
    }
}
