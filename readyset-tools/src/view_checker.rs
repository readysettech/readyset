#![warn(clippy::panic)]
use clap::Parser;
use readyset_client::consensus::AuthorityType;
use readyset_client::ReadySetHandle;

#[derive(Parser)]
#[clap(name = "view_checker")]
struct ViewChecker {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(short, long, env("DEPLOYMENT"), forbid_empty_values = true)]
    deployment: String,

    #[clap(short, long)]
    query: String,
}

impl ViewChecker {
    pub async fn run(self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;

        let mut handle: ReadySetHandle = ReadySetHandle::new(authority).await;
        handle.ready().await.unwrap();

        println!("Waiting for noria");
        let mut getter = handle.view(self.query).await?.into_reader_handle().unwrap();
        let results = getter.lookup(&[0.into()], true).await?;
        println!("Results: {:?}", results);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let view_checker = ViewChecker::parse();
    view_checker.run().await
}
