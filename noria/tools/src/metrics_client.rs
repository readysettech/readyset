use noria::consensus::ZookeeperAuthority;
use noria::metrics::client::MetricsClient;
use noria::ControllerHandle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let authority = ZookeeperAuthority::new(&"127.0.0.1:2181/myapp")?;
    let noria = ControllerHandle::new(authority).await?;

    let mut client = MetricsClient::new(noria).unwrap();
    let res = client.get_metrics().await?;
    println!("{:?}", res);

    client.reset_metrics().await?;

    let res = client.get_metrics().await?;
    println!("{:?}", res);

    Ok(())
}
