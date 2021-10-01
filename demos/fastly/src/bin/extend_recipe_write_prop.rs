use clap::Clap;
use noria::consensus::AuthorityType;
use noria::ControllerHandle;

#[derive(Clap)]
#[clap(name = "extend_recipe_write_propagation")]
struct ExtendRecipeWritePropagation {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,
    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,
    #[clap(short, long, env("NORIA_DEPLOYMENT"))]
    deployment: String,
}

impl ExtendRecipeWritePropagation {
    pub async fn run(&'static self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;
        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();
        let q = "QUERY w : SELECT * FROM articles WHERE id = ?;";

        handle.extend_recipe(q).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let r: &'static _ = Box::leak(Box::new(ExtendRecipeWritePropagation::parse()));
    r.run().await
}
