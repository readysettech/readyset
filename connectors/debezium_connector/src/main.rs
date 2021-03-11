extern crate serde_json;
#[macro_use]
extern crate derive_more;

use clap::{App, Arg};

mod debezium_connector;

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
    let zookeeper_conn = matches.value_of("zookeeper-connection").unwrap();
    let timeout = matches.value_of("kafka-timeout").unwrap();
    let eof = matches.is_present("kafka-partition-eof");
    let auto_commit = !matches.is_present("kafka-no-auto-commit");

    DebeziumConnector::builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_server_name(server_name)
        .set_db_name(db_name)
        .set_tables(tables)
        .set_zookeeper_conn(zookeeper_conn)
        .set_timeout(timeout)
        .set_eof(eof)
        .set_auto_commit(auto_commit)
        .build()?
        .start()
        .await
}
