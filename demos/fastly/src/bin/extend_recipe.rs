use clap::Clap;
use noria::consensus::AuthorityType;
use noria::ControllerHandle;

#[derive(Clap)]
#[clap(name = "extend_recipe")]
struct ExtendRecipe {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,
    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,
    #[clap(short, long, env("NORIA_DEPLOYMENT"))]
    deployment: String,
}

impl ExtendRecipe {
    pub async fn run(&'static self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        let q = "QUERY w: SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.image_url, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id = ?)) LIMIT 5;";
        handle.extend_recipe(q).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let r: &'static _ = Box::leak(Box::new(ExtendRecipe::parse()));
    r.run().await
}
