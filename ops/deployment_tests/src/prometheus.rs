use std::convert::TryInto;
use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::{anyhow, bail, ensure, Result};
use prometheus_http_query::aggregations::sum;
use prometheus_http_query::{Client, InstantVector, Scheme, Selector};

/// Prometheus query timeout, specified as a str according to promethes' requirements.
const PROM_QUERY_TIMEOUT: &str = "20s";

pub async fn client(addr: &str) -> Result<Client> {
    let prometheus_address: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();

    #[allow(clippy::indexing_slicing)] // Validated as safe with early bail above.
    let (prom_ip, prom_port) = (prometheus_address[0].ip(), prometheus_address[0].port());
    Ok(Client::new(Scheme::Http, &prom_ip.to_string(), prom_port))
}

pub async fn verify_db_queried(prometheus: &Client, db: &str) -> Result<()> {
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

pub async fn verify_end_to_end_latency(prometheus: &Client, seconds: f64) -> Result<()> {
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
            val < seconds,
            format!(
                "Queries per second of query is too high, > 20ms. query = {}",
                q.metric().get("query").unwrap()
            )
        );
    }

    Ok(())
}
