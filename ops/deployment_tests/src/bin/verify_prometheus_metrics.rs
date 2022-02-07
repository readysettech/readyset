use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Result;
use clap::Parser;
use deployment_tools::prometheus;
use prometheus_http_query::{Client, Scheme};

#[derive(Parser)]
#[clap(name = "prometheus_metrics")]
pub struct VerifyPrometheusMetrics {
    /// Address of a prometheus server to execute PromQL against.
    #[clap(long, short = 'p', env = "PROMETHEUS_ADDRESS")]
    prometheus_address: String,

    /// Enables using https to communicate with prometheus.
    #[clap(long)]
    https: bool,

    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Parser)]
pub enum Subcommand {
    NoriaQueried,
    MysqlQueried,
    EndToEndLatency,
}

impl VerifyPrometheusMetrics {
    pub async fn verify_subcommand(&self, prometheus: Client) -> Result<()> {
        match &self.subcommand {
            Subcommand::NoriaQueried => prometheus::verify_db_queried(&prometheus, "noria").await?,
            Subcommand::MysqlQueried => prometheus::verify_db_queried(&prometheus, "mysql").await?,
            Subcommand::EndToEndLatency => {
                prometheus::verify_end_to_end_latency(&prometheus, 0.02).await?
            }
        }

        Ok(())
    }

    pub async fn run(self) -> Result<()> {
        let prometheus_address: Vec<SocketAddr> =
            self.prometheus_address.to_socket_addrs()?.collect();

        #[allow(clippy::indexing_slicing)] // Validated as safe with early bail above.
        let (prom_ip, prom_port) = (prometheus_address[0].ip(), prometheus_address[0].port());
        let scheme = if self.https {
            Scheme::Https
        } else {
            Scheme::Http
        };
        let client = Client::new(scheme, &prom_ip.to_string(), prom_port);
        self.verify_subcommand(client).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let verify = VerifyPrometheusMetrics::parse();
    verify.run().await
}
