use clap::Clap;
use noria::consensus::ZookeeperAuthority;
use noria::ControllerHandle;
use std::sync::Arc;

#[derive(Clap)]
#[clap(name = "add_query")]
struct AddQuery {
    /// ReadySet's zookeeper connection string.
    #[clap(long)]
    zookeeper_url: String,
}

impl AddQuery {
    pub async fn run(&'static self) -> anyhow::Result<()> {
        let authority = Arc::new(ZookeeperAuthority::new(&self.zookeeper_url)?);

        let mut handle: ControllerHandle<ZookeeperAuthority> =
            ControllerHandle::new(Arc::clone(&authority)).await;
        handle.ready().await.unwrap();
        let q = "QUERY w : SELECT * FROM articles WHERE id = ?;";

        handle.extend_recipe(q).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let r: &'static _ = Box::leak(Box::new(AddQuery::parse()));
    r.run().await
}
