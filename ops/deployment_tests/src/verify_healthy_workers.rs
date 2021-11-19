use clap::Parser;
use noria::consensus::AuthorityType;
use noria::ControllerHandle;

#[derive(Parser)]
#[clap(name = "healthy_workers")]
struct VerifyHealthyWorkers {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), default_value("consul"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(short, long, env("NORIA_DEPLOYMENT"))]
    deployment: String,

    #[clap(long)]
    num_workers: u32,
}

impl VerifyHealthyWorkers {
    pub async fn run(self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;
        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();
        let res = handle.healthy_workers().await?.len();
        assert_eq!(res, self.num_workers as usize);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let verify = VerifyHealthyWorkers::parse();
    verify.run().await
}
