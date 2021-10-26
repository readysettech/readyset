use clap::Clap;
use noria::consensus::{Authority, ZookeeperAuthority};
use noria::ControllerHandle;
use std::sync::Arc;
use url::Url;

#[derive(Clap)]
#[clap(name = "replicate")]
struct Replicate {
    /// ReadySet's zookeeper connection string.
    #[clap(long)]
    zookeeper_url: String,

    /// The set of queries to replicate on to the readers.
    #[clap(long)]
    queries: Vec<String>,

    /// The reader's to replicate on to.
    #[clap(long)]
    reader_addrs: Vec<String>,
}

impl Replicate {
    pub async fn run(&'static self) -> anyhow::Result<()> {
        let authority = Arc::new(Authority::from(
            ZookeeperAuthority::new(&self.zookeeper_url).await?,
        ));

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        for r in &self.reader_addrs {
            handle
                .replicate_readers(self.queries.clone(), Some(Url::parse(r).unwrap()))
                .await
                .unwrap();
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let replicate: &'static _ = Box::leak(Box::new(Replicate::parse()));
    replicate.run().await
}
