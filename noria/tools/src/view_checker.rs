use clap::{App, Arg};
use noria::consensus::{Authority, ZookeeperAuthority};
use noria::ControllerHandle;

// This exists for testing purposes. It is an easy script to view the employees table. It assumes the noria deployment
// is named `myapp` and that its listening on address 127.0.0.1
#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let authority = Authority::from(ZookeeperAuthority::new("127.0.0.1:2181/myapp")?);
    let mut noria = ControllerHandle::new(authority).await;

    println!("Waiting for noria");
    let query = matches.value_of("query").unwrap_or("testQuery");
    let mut getter = noria.view(query).await?;
    let results = getter.lookup(&[0.into()], true).await?;
    println!("Results: {:?}", results);
    Ok(())
}
