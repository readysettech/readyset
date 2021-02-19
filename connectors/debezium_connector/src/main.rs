extern crate serde_json;
#[macro_use]
extern crate derive_more;

use clap::{App, Arg};
use std::net::IpAddr;
use std::str::FromStr;

mod debezium_connector_application;

#[tokio::main]
async fn main() {
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
                    .help("Tables to track."),
            )
            .arg(
                Arg::with_name("server-name")
                    .short("s")
                    .long("server-name")
                    .takes_value(true)
                    .required(true)
                    .help("The database server name."),
            )
            .arg(
                Arg::with_name("db-name")
                    .short("d")
                    .long("db-name")
                    .takes_value(true)
                    .required(true)
                    .help("The db name."),
            )
            .arg(
                Arg::with_name("noria-connection")
                    .short("n")
                    .long("noria-connection")
                    .takes_value(true)
                    .required(true)
                    .default_value("127.0.0.1")
                    .help("Noria connection IP."),
            )
            .arg(
                Arg::with_name("zookeeper-connection")
                    .short("z")
                    .long("zookeeper-connection")
                    .takes_value(true)
                    .required(true)
                    .default_value("127.0.0.1:2181/myapp")
                    .help("Zookeeper address in the form IP:PORT/<NORIA_DEPLOYMENT>"),
            )
            .arg(
                Arg::with_name("kafka-bootstrap-servers")
                    .short("k")
                    .long("kafka")
                    .takes_value(true)
                    .required(true)
                    .default_value("localhost:9092")
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
            .get_matches();

    let bootstrap_servers: &str = matches.value_of("kafka-bootstrap-servers").unwrap();
    let server_name: &str = matches.value_of("server-name").unwrap();
    let db_name: &str = matches.value_of("db-name").unwrap();
    let tables: Vec<String> = matches
        .values_of("table")
        .unwrap()
        .map(|s| s.to_string())
        .collect();
    let addr: Option<IpAddr> = matches
        .value_of("noria-connection")
        .map(|ip| IpAddr::from_str(ip).unwrap());
    let zookeeper_conn = matches.value_of("zookeeper-connection").unwrap();
    let timeout = matches.value_of("kafka-timeout").unwrap();
    let eof = matches.is_present("kafka-partition-eof");
    let auto_commit = !matches.is_present("kafka-no-auto-commit");

    let mut application = debezium_connector_application::DebeziumConnector::new(
        bootstrap_servers.to_string(),
        server_name.to_string(),
        db_name.to_string(),
        tables,
        format!("{}.{}", server_name, db_name),
        addr,
        zookeeper_conn.to_string(),
        timeout.to_string(),
        eof,
        auto_commit,
    );

    application.start().await.unwrap();
}
