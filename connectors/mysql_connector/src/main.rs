pub mod connector;
pub mod noria_adapter;

use connector::BinlogPosition;

use clap::{App, Arg};

#[tokio::main]
async fn main() -> mysql_async::Result<()> {
    tracing_subscriber::fmt().init();

    let matches = App::new("noria-mysql-connector")
        .version("0.0.1")
        .arg(
            Arg::with_name("mysql-address")
                .short("a")
                .long("mysql-address")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1")
                .env("MYSQL_SERVER")
                .help("IP or Hostname for MySQL."),
        )
        .arg(
            Arg::with_name("mysql-port")
                .short("p")
                .long("mysql-port")
                .takes_value(true)
                .required(true)
                .default_value("3306")
                .env("MYSQL_PORT")
                .help("Port number for MySQL."),
        )
        .arg(
            Arg::with_name("mysql-user")
                .short("u")
                .long("mysql-user")
                .takes_value(true)
                .required(false)
                .env("MYSQL_USER")
                .help("User name for MySQL."),
        )
        .arg(
            Arg::with_name("mysql-password")
                .long("mysql-password")
                .takes_value(true)
                .required(false)
                .env("MYSQL_PASSWORD")
                .help("User password for MySQL."),
        )
        .arg(
            Arg::with_name("zookeeper-address")
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
            Arg::with_name("db-name")
                .long("db-name")
                .takes_value(true)
                .required(true)
                .env("DB_NAME")
                .help("The db name."),
        )
        .arg(
            Arg::with_name("binlog-file")
                .long("binlog-file")
                .takes_value(true)
                .env("BINLOG_FILE")
                .help("The binlog filename."),
        )
        .arg(
            Arg::with_name("binlog-position")
                .long("binlog-position")
                .takes_value(true)
                .env("BINLOG_POS")
                .default_value("4")
                .help("The offset withing the binlog file."),
        )
        .get_matches();

    let addr = matches.value_of("mysql-address").unwrap();
    let port = clap::value_t!(matches.value_of("mysql-port"), u16).unwrap();
    let user_name = matches.value_of("mysql-user");
    let user_password = matches.value_of("mysql-password");
    let zookeeper_address = matches.value_of("zookeeper-address").unwrap();
    let deployment = matches.value_of("deployment").unwrap();
    let schema = matches.value_of("db-name").unwrap();
    let binlog_position = matches.value_of("binlog-file").map(|f| BinlogPosition {
        binlog_file: f.to_string(),
        position: clap::value_t!(matches.value_of("binlog-position"), u32).unwrap(),
    });

    let builder = noria_adapter::Builder::new(addr, port)
        .with_user(user_name)
        .with_password(user_password)
        .with_zookeeper_addr(Some(zookeeper_address))
        .with_deployment(Some(deployment))
        .with_database(Some(schema))
        .with_position(binlog_position);

    builder.start().await
}
