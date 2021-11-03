use anyhow::anyhow;
use futures::future::join_all;
use itertools::Itertools;
use noria::{
    consensus::{AuthorityControl, ConsulAuthority},
    ReadySetResult,
};
use prometheus_http_query::{
    response::{InstantVector, QueryResultType},
    Client, InstantVector as InstantVectorReq, Selector,
};
use tokio::{select, sync::Mutex};
use tracing::{error, info};

use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

/// A timeout for the prometheus query to scrape relevant metrics.
/// Constant is in a string format, because that's what the prometheus client library we're using
/// expects for a duration argument. It wants to parse the duration itself.
const PROM_QUERY_TIMEOUT: &str = "20s";

use crate::cache::{
    AllowList, Database, QueryLatenciesBuilder, QueryMetricsCache, QueryWithLatencies,
};

pub struct MetricsReconciler {
    /// Consul client that is used to retrieve the current list of adapter addresses.
    consul: ConsulAuthority,

    /// Prometheus client to scrape metrics from a prom server.
    prom_client: Client,

    /// The current deployment that we are filtering on.
    deployment: String,

    /// The query metrics cache is polled on a regular interval to
    /// determine what the aggregated allow and deny lists are for all adapters we are aggregating
    /// over.
    query_metrics_cache: Arc<Mutex<QueryMetricsCache>>,

    /// The minimum interval between subsequent polls to the query
    /// metrics cache. In practice it may be longer if the queries
    /// that require processing take longer than `min_poll_interval`.
    min_poll_interval: std::time::Duration,

    /// Reciever to return the broadcast signal on.
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,

    /// A flag indicating whether the adapters we're aggregating over are hosting insecure (http,
    /// as opposed to https) endpoints for their respective allow and deny lists.
    insecure_adapters: bool,

    /// A flag indicating whether we should aggregate latencies from the upstream
    /// database.
    show_upstream_latencies: bool,
}

impl MetricsReconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        consul: ConsulAuthority,
        prom_client: Client,
        deployment: String,
        query_metrics_cache: Arc<Mutex<QueryMetricsCache>>,
        min_poll_interval: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
        insecure_adapters: bool,
        show_upstream_latencies: bool,
    ) -> MetricsReconciler {
        MetricsReconciler {
            consul,
            prom_client,
            deployment,
            query_metrics_cache,
            min_poll_interval,
            shutdown_recv,
            insecure_adapters,
            show_upstream_latencies,
        }
    }

    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.min_poll_interval);
        loop {
            select! {
                _ = interval.tick() => {
                    let adapter_addrs = match self.consul.get_adapters().await {
                        Err(e) => {
                            error!(%e, "received an error from consul while trying to retrieve adapter endpoints");
                            continue;
                        }
                        Ok(addrs) => addrs,
                    };

                    let mut deny_list = HashSet::new();
                    let mut allow_list = HashSet::new();
                    let metrics_futs = adapter_addrs.iter().map(|a| {
                        let addr = *a;
                        let insecure_adapters = self.insecure_adapters;
                        reconcile_adapter_metrics(addr, insecure_adapters)
                    }).collect::<Vec<_>>();
                    let adapter_lists = join_all(metrics_futs).await.into_iter().filter_map(|r| {
                        match r {
                            Err(e) => {
                                error!(%e, "received error when reconciling adapter metrics");
                                None
                            }
                            Ok(lists) => Some(lists),
                        }
                    }).collect::<Vec<(Vec<String>, Vec<String>)>>();

                    for (allow, deny) in adapter_lists {
                        deny_list.extend(deny);
                        allow_list.extend(allow);
                    }

                    if let Err(e) = self.populate_query_metrics(allow_list, deny_list).await {
                        error!(%e, "received error when attempting to populate query metrics");
                    }
                }
                _ = self.shutdown_recv.recv() => {
                    info!("Metrics reconciler shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn populate_query_metrics(
        &mut self,
        allow_list: HashSet<String>,
        deny_list: HashSet<String>,
    ) -> anyhow::Result<()> {
        let v: InstantVectorReq = Selector::new()
            .metric("query_log_execution_time")?
            .with("deployment", &self.deployment)
            .without("query", "")
            .try_into()?;

        let res = self
            .prom_client
            .query(v, None, Some(PROM_QUERY_TIMEOUT))
            .await?;
        let filled_allow_list = fill_allow_list(allow_list, res, self.show_upstream_latencies);

        let mut cache = self.query_metrics_cache.lock().await;
        cache.deny_list = deny_list.into_iter().collect();
        cache.allow_list = filled_allow_list?;

        Ok(())
    }
}

/// Pulls the allow and deny list from a given adapter.
async fn reconcile_adapter_metrics(
    addr: SocketAddr,
    insecure_adapters: bool,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let scheme = if insecure_adapters { "http" } else { "https" };
    let (allow_list_addr, deny_list_addr) = (
        format!("{}://{}/allow-list", scheme, addr),
        format!("{}://{}/deny-list", scheme, addr),
    );
    let allow_list_fut = reqwest::get(allow_list_addr);
    let deny_list_fut = reqwest::get(deny_list_addr);
    let resolved = join_all(vec![allow_list_fut, deny_list_fut])
        .await
        .into_iter()
        .map_ok(|resp| resp.json::<Vec<String>>())
        .collect::<reqwest::Result<Vec<_>>>()?;
    let mut json_results = join_all(resolved)
        .await
        .into_iter()
        .collect::<reqwest::Result<Vec<Vec<String>>>>()?;
    let deny_list = json_results.pop().unwrap();
    let allow_list = json_results.pop().unwrap();

    Ok((allow_list, deny_list))
}

/// Adds query latency metric to builder if that's the kind of metric we actually got (from the
/// supplied InstantVector response).
fn add_metric_to_builder(
    builder: &mut QueryLatenciesBuilder,
    instant_vec: &InstantVector,
    show_upstream_latencies: bool,
) {
    if let (Some(q), Some(db)) = (
        instant_vec.metric().get("quantile"),
        instant_vec.metric().get("database_type"),
    ) {
        let database = if db == "noria" {
            Database::Noria
        } else if !show_upstream_latencies {
            return;
        } else {
            Database::Upstream
        };
        let secs: f64 = instant_vec.sample().value().parse().unwrap();
        match &q[..] {
            "0.99" => builder.set_p99(Duration::from_secs_f64(secs), database),
            "0.95" => builder.set_p95(Duration::from_secs_f64(secs), database),
            "0.9" => builder.set_p90(Duration::from_secs_f64(secs), database),
            "0.5" => builder.set_p50(Duration::from_secs_f64(secs), database),
            _ => {}
        }
    }
}

fn fill_allow_list(
    allow_list: HashSet<String>,
    prom_res: QueryResultType,
    show_upstream_latencies: bool,
) -> anyhow::Result<AllowList> {
    let mut builder_map: HashMap<String, QueryLatenciesBuilder> = allow_list
        .into_iter()
        .map(|q| (q, QueryLatenciesBuilder::new()))
        .collect();
    match prom_res {
        QueryResultType::Vector(vector_list) => {
            for instant_vec in vector_list {
                let query = if let Some(q) = instant_vec.metric().get("query") {
                    q
                } else {
                    // This piece of data is somehow missing the query field entirely. Skip.
                    continue;
                };
                match builder_map.get_mut(query) {
                    Some(builder) => {
                        add_metric_to_builder(builder, &instant_vec, show_upstream_latencies)
                    }
                    None => {
                        // Not in our allow list
                        continue;
                    }
                }
            }
        }
        QueryResultType::Matrix(_) => {
            return Err(anyhow!(
                "received a matrix result type from prometheus when we expected a vector"
            ));
        }
    }

    Ok(builder_map
        .into_iter()
        .map(|(k, v)| {
            let (noria, upstream) = v.into_query_latencies();
            QueryWithLatencies::new(k, noria, upstream)
        })
        .collect())
}
