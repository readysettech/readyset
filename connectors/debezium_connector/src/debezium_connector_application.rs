extern crate serde_json;

use anyhow::Result;
use noria::consensus::ZookeeperAuthority;
use noria::Builder;
use noria::Handle;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::stream::StreamExt;

mod debezium_message_parser;
mod kafka_message_consumer_wrapper;
use debezium_message_parser::{DataChangePayload, EventKey, EventValue};

pub struct DebeziumConnector {
    kafka_consumer: kafka_message_consumer_wrapper::KafkaMessageConsumerWrapper,
    noria_ip: Option<IpAddr>,
    zookeeper_conn: String,
}

impl DebeziumConnector {
    pub fn new(
        bootstrap_servers: String,
        server_name: String,
        db_name: String,
        tables: Vec<String>,
        group_id: String,
        noria_ip: Option<IpAddr>,
        zookeeper_conn: String,
        timeout: String,
        eof: bool,
        auto_commit: bool,
    ) -> DebeziumConnector {
        // for each table, we listen to the topic <dbserver>.<dbname>.<tablename>
        let mut topic_names: Vec<String> = tables
            .iter()
            .map(|table_name| format!("{}.{}.{}", server_name, db_name, table_name))
            .collect();

        // we also listen to the schema change topic, which is just named <dbserver>
        topic_names.push(server_name);

        let consumer = kafka_message_consumer_wrapper::KafkaMessageConsumerWrapper::new(
            bootstrap_servers,
            topic_names,
            group_id,
            timeout,
            eof,
            auto_commit,
        );

        DebeziumConnector {
            kafka_consumer: consumer,
            noria_ip,
            zookeeper_conn,
        }
    }

    async fn handle_message(
        noria_authority: &mut Handle<ZookeeperAuthority>,
        key_message: &EventKey,
        value_message: &EventValue,
    ) -> Result<()> {
        match value_message {
            EventValue::SchemaChange(e) => {
                noria_authority.extend_recipe(&e.payload.ddl).await?;
            }
            EventValue::DataChange(e) => {
                match &e.payload {
                    DataChangePayload::Create(p) => {
                        // We know that the payload consist of before, after and source fields
                        // and that too in that specific order.
                        if let Some(table_name) = &p.source.table {
                            let after_field_schema = &e.schema.fields[1];
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
                            let after_field_schema = &e.schema.fields[1];
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
            }
        };
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut message_stream = self.kafka_consumer.kafka_consumer.start();

        let authority = ZookeeperAuthority::new(&self.zookeeper_conn)?;
        let mut builder = Builder::default();
        builder.set_listen_addr(self.noria_ip.unwrap());
        let (mut noria_authority, _) = builder.start(Arc::new(authority)).await?;

        while let Some(message) = message_stream.next().await {
            if let Ok(m) = message {
                let owned_message = m.detach();
                // We have to check existence because on deletes, a tombstone message is sent by the kafka connector.
                // We really dont use the for anything, so we just ignore them for now.
                if owned_message.payload().is_some() {
                    let key_string = std::str::from_utf8(m.key().unwrap())?;
                    let value_string = std::str::from_utf8(m.payload().unwrap())?;

                    let key_message: EventKey = serde_json::from_str(&key_string)?;
                    let value_message: EventValue = serde_json::from_str(&value_string)?;

                    DebeziumConnector::handle_message(
                        &mut noria_authority,
                        &key_message,
                        &value_message,
                    )
                    .await?;

                    self.kafka_consumer
                        .kafka_consumer
                        .commit_message(&m, CommitMode::Async)?;
                }
            }
        }
        Ok(())
    }
}
