use anyhow::{anyhow, bail, ensure, Result};
use clap::Parser;
use prometheus_http_query::aggregations::sum;
//use prometheus_http_query::response::QueryResultType;
use prometheus_http_query::{Client, InstantVector, Scheme, Selector};
use std::convert::TryInto;
use std::net::{SocketAddr, ToSocketAddrs};

/// Prometheus query timeout, specified as a str according to promethes' requirements.
const PROM_QUERY_TIMEOUT: &str = "20s";

#[derive(Parser)]
#[clap(name = "prometheus_metrics")]
struct VerifyPrometheusMetrics {
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

async fn verify_db_queried(prometheus: Client, db: &str) -> Result<()> {
    let v: InstantVector = Selector::new()
        .metric("query_log_execution_time_count")?
        .with("database_type", db)
        .try_into()?;
    let q = sum(v, None).ge_scalar(0.0, true);

    let res = prometheus.query(q, None, Some(PROM_QUERY_TIMEOUT)).await?;

    let instant = res
        .as_instant()
        .ok_or_else(|| anyhow!("Could not be converted to an instant vector"))?;

    match instant.first() {
        Some(v) => {
            ensure!(
                v.sample().value() == "1",
                "No queries have been executed against Noria"
            );
        }
        None => bail!("No noria queries have been executed"),
    }

    Ok(())
}

impl VerifyPrometheusMetrics {
    pub async fn verify_subcommand(&self, prometheus: Client) -> Result<()> {
        match &self.subcommand {
            Subcommand::NoriaQueried => verify_db_queried(prometheus, "noria").await?,
            Subcommand::MysqlQueried => verify_db_queried(prometheus, "mysql").await?,
            Subcommand::EndToEndLatency => {
                let v: InstantVector = Selector::new()
                    .metric("query_log_execution_time")?
                    .with("database_type", "noria")
                    .with("quantile", "0.99")
                    .try_into()?;
                let q = v;

                let res = prometheus.query(q, None, Some(PROM_QUERY_TIMEOUT)).await?;

                let instant = res
                    .as_instant()
                    .ok_or_else(|| anyhow!("Could not be converted to an instant vector"))?;

                // Ensure that every query has a latency under 20ms.
                for q in instant {
                    let val = q.sample().value().parse::<f64>()?;
                    ensure!(
                        val < 0.02,
                        format!(
                            "Queries per second of query is too high, > 20ms. query = {}",
                            q.metric().get("query").unwrap()
                        )
                    );
                }
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
