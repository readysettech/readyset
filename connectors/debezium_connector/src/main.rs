#![warn(clippy::dbg_macro)]
extern crate serde_json;
#[macro_use]
extern crate derive_more;

use clap::{App, Arg};
use std::str::FromStr;

mod debezium_connector;

use debezium_connector::DatabaseType;
pub use debezium_connector::{Builder, DebeziumConnector};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let matches =
        App::new("noria-debezium-connector")
            .version("0.0.1")
            .arg(
                Arg::with_name("table")
                    .short("t")
                    .long("table")
                    .takes_value(true)
                    .multiple(true)
                    .required(true)
                    .env("TABLES")
                    .use_delimiter(true)
                    .help("Tables to track."),
            )
            .arg(
                Arg::with_name("server-name")
                    .short("s")
                    .long("server-name")
                    .takes_value(true)
                    .required(true)
                    .env("SERVER_NAME")
                    .help("The database server name."),
            )
            .arg(
                Arg::with_name("db-name")
                    .short("d")
                    .long("db-name")
                    .takes_value(true)
                    .required(true)
                    .env("DB_NAME")
                    .help("The db name."),
            )
            .arg(
                Arg::with_name("zookeeper-address")
                    .short("z")
                    .long("zookeeper-address")
                    .takes_value(true)
                    .required(true)
                    .default_value("127.0.0.1:2181")
                    .env("ZOOKEEPER_URL")
                    .help("IP:PORT for Zookeeper."),
            )
            .arg(
                Arg::with_name("deployment")
                    .long("deployment")
                    .takes_value(true)
                    .required(true)
                    .env("NORIA_DEPLOYMENT")
                    .help("Noria deployment ID to attach to."),
            )
            .arg(
                Arg::with_name("kafka-bootstrap-servers")
                    .short("k")
                    .long("kafka")
                    .takes_value(true)
                    .required(true)
                    .default_value("localhost:9092")
                    .env("KAFKA_URL")
                    .help("Kafka connection"),
            )
            .arg(
                Arg::with_name("kafka-timeout")
                    .long("timeout")
                    .takes_value(true)
                    .default_value("6000")
                    .help("Session timeout in ms for Kafka Messaging"),
            )
            .arg(Arg::with_name("kafka-partition-eof").long("eof").help(
                "Enable emission of event whenever the consumer reaches the end of a partition",
            ))
            .arg(
                Arg::with_name("kafka-no-auto-commit")
                    .long("no-auto-commit")
                    .help("Disable auto commit for Kafka"),
            )
            .arg(
                Arg::with_name("db_type")
                    .long("database_type")
                    .default_value("mysql")
                    .help("The database we are connected to: mysql or postgres"),
            )
            .arg(
                Arg::with_name("group_id")
                    .long("group-id")
                    .takes_value(true)
                    .help("The name of the kafka consumer group ID"),
            )
            .get_matches();

    let bootstrap_servers: &str = matches.value_of("kafka-bootstrap-servers").unwrap();
    let server_name: &str = matches.value_of("server-name").unwrap();
    let db_name: &str = matches.value_of("db-name").unwrap();
    let tables: Vec<String> = matches
        .values_of("table")
        .unwrap()
        .map(|s| s.to_string())
        .collect();
    let zookeeper_address = matches.value_of("zookeeper-address").unwrap();
    let deployment = matches.value_of("deployment").unwrap();
    let timeout = matches.value_of("kafka-timeout").unwrap();
    let eof = matches.is_present("kafka-partition-eof");
    let auto_commit = !matches.is_present("kafka-no-auto-commit");
    let db_type = DatabaseType::from_str(matches.value_of("db_type").unwrap()).unwrap();
    let group_id = matches.value_of("group_id");

    DebeziumConnector::builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_server_name(server_name)
        .set_db_name(db_name)
        .set_tables(tables)
        .set_zookeeper_address(zookeeper_address)
        .set_deployment(deployment)
        .set_timeout(timeout)
        .set_eof(eof)
        .set_auto_commit(auto_commit)
        .set_database_type(db_type)
        .set_group_id(group_id)
        .build()
        .await?
        .start()
        .await
}
