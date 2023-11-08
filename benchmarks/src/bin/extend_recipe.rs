use clap::Parser;
use readyset_client::consensus::AuthorityType;
use readyset_client::recipe::changelist::ChangeList;
use readyset_client::ReadySetHandle;
use readyset_data::Dialect;

#[derive(Parser)]
#[command(name = "extend_recipe")]
struct ExtendRecipe {
    #[arg(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:8500"))]
    authority_address: String,
    #[arg(
        long,
        env("AUTHORITY"),
        default_value("consul"),
        value_parser = ["consul"]
    )]
    authority: AuthorityType,
    #[arg(short, long, env("DEPLOYMENT"))]
    deployment: String,
}

impl ExtendRecipe {
    pub async fn run(&'static self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment);

        let mut handle: ReadySetHandle = ReadySetHandle::new(authority).await;
        handle.ready().await.unwrap();

        let q = " CREATE CACHE w FROM SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.image_url, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id = ?)) LIMIT 5;";
        handle
            .extend_recipe(ChangeList::from_str(q, Dialect::DEFAULT_MYSQL).unwrap())
            .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let r: &'static _ = Box::leak(Box::new(ExtendRecipe::parse()));
    r.run().await
}
