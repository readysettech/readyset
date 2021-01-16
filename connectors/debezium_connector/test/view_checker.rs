use noria::consensus::ZookeeperAuthority;
use noria::Builder;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

// This exists for testing purposes. It is an easy script to view the employees table. It assumes the noria deployment
// is named `myapp` and that its listening on address 127.0.0.1
#[tokio::main]
async fn main() {
    let authority = ZookeeperAuthority::new(&"127.0.0.1:2181/myapp");
    let mut builder = Builder::default();
    builder.set_listen_addr(IpAddr::from_str("127.0.0.1").unwrap());
    let (mut noria_authority, _) = builder.start(Arc::new(authority.unwrap())).await.unwrap();

    println!("Waiting for noria");
    let mut getter = noria_authority.view(&"testQuery").await.unwrap();
    let results = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Results: {:?}", results);
}
