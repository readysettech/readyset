use clap::{App, Arg};
use noria::consensus::ZookeeperAuthority;
use noria::Builder;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

// This exists for testing purposes. It is an easy script to view the employees table. It assumes the noria deployment
// is named `myapp` and that its listening on address 127.0.0.1
#[tokio::main]
async fn main() {
    // Allow for a query to be passed as parameter, to avoid
    // recompiling each time we want to get a view from a different query.
    // If no value is provided, then it defaults to "testQuery", for
    // backwards compatibility.
    let matches = App::new("view-checker")
        .version("0.0.1")
        .arg(
            Arg::with_name("query")
                .short("q")
                .long("query")
                .takes_value(true)
                .multiple(false)
                .required(false)
                .help("Query to visualize."),
        )
        .get_matches();
    let authority = ZookeeperAuthority::new(&"127.0.0.1:2181/myapp");
    let mut builder = Builder::default();
    builder.set_listen_addr(IpAddr::from_str("127.0.0.1").unwrap());
    let (mut noria_authority, _) = builder.start(Arc::new(authority.unwrap())).await.unwrap();

    println!("Waiting for noria");
    let query = matches.value_of("query").unwrap_or("testQuery");
    let mut getter = noria_authority.view(query).await.unwrap();
    let results = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Results: {:?}", results);
}
