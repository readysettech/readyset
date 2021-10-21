#![warn(clippy::panic)]
//! Tool to retrieve a metrics dump for the current leader in a
//! deployment.

use clap::Clap;
use noria::consensus::AuthorityType;
use noria::metrics::client::MetricsClient;
use noria::ControllerHandle;

#[derive(Clap)]
#[clap(name = "metrics_dump")]
struct MetricsDump {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(short, long, env("NORIA_DEPLOYMENT"), forbid_empty_values = true)]
    deployment: String,
}

impl MetricsDump {
    pub async fn run(self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        let mut client = MetricsClient::new(handle).unwrap();
        let res = client.get_metrics().await?;
        println!("{:?}", res);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let metrics_dump = MetricsDump::parse();
    metrics_dump.run().await
}
