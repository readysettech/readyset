use clap::Clap;
use noria::consensus::{Authority, ZookeeperAuthority};
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
        let authority = Arc::new(Authority::from(ZookeeperAuthority::new(
            &self.zookeeper_url,
        )?));

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        let q = "QUERY w: SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.image_url, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id = ?)) LIMIT 5;";
        handle.extend_recipe(q).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let r: &'static _ = Box::leak(Box::new(AddQuery::parse()));
    r.run().await
}
