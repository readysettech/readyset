use anyhow::anyhow;
use clap::Clap;
use noria::consensus::AuthorityType;
use noria::ControllerHandle;
use std::str::FromStr;

#[derive(Clap)]
#[clap(name = "controller_request")]
struct ControllerRequest {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(short, long, env("NORIA_DEPLOYMENT"))]
    deployment: String,

    /// The name of the endpoint to issue a controller request to.
    /// This currently only supports endpoints without parameters.
    #[clap(short, long)]
    endpoint: Request,
}

enum Request {
    HealthyWorkers,
    ControllerUri,
}

impl FromStr for Request {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "/healthy_workers" => Ok(Request::HealthyWorkers),
            "/controller_uri" => Ok(Request::ControllerUri),
            _ => Err(anyhow!("Unsupported request")),
        }
    }
}

impl Request {
    async fn issue_and_print(&self, mut handle: ControllerHandle) -> anyhow::Result<()> {
        match self {
            Request::HealthyWorkers => {
                let res = handle.healthy_workers().await?;
                println!("{:?}", res);
            }
            Request::ControllerUri => {
                let res = handle.controller_uri().await?;
                println!("{:?}", res);
            }
        }

        Ok(())
    }
}

impl ControllerRequest {
    pub async fn run_command(self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        self.endpoint.issue_and_print(handle).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let controller_requester = ControllerRequest::parse();
    controller_requester.run_command().await
}
