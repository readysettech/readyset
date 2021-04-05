use crate::consensus::Authority;
use crate::metrics::MetricsDump;
use crate::rpc_err;
use crate::{ControllerHandle, ReadySetError, ReadySetResult};
use std::collections::HashMap;
use std::net::SocketAddr;

/// A metrics dump tagged with the address it was received from.
#[derive(Debug)]
pub struct TaggedMetricsDump {
    /// The address of the noria-server the metrics dump was received
    /// from.
    pub addr: SocketAddr,
    /// The set of dumped metrics.
    pub metrics: MetricsDump,
}

/// The MetricsClient handles operations to the metrics collection framework across
/// a Noria deployment.
pub struct MetricsClient<A: Authority + 'static> {
    controller: ControllerHandle<A>,
    client: reqwest::Client,
}

impl<A> MetricsClient<A>
where
    A: Authority,
{
    /// Instantiates a new metrics client connected to the deployment associated with
    /// `controller`.
    pub fn new(controller: ControllerHandle<A>) -> ReadySetResult<Self> {
        Ok(MetricsClient {
            controller,
            client: reqwest::Client::new(),
        })
    }

    /// Retrieves the external address for each noria-server in the deployment.
    /// The result is a HashMap mapping each worker's internal address to their
    /// controller's external address.
    pub async fn get_external_addrs(&mut self) -> ReadySetResult<HashMap<SocketAddr, SocketAddr>> {
        self.controller.external_addrs().await
    }

    /// Retrieves metrics from each noria-server in a deployment and aggregates the results
    /// into a single json string.
    pub async fn get_metrics(&mut self) -> ReadySetResult<Vec<TaggedMetricsDump>> {
        let noria_servers = self.get_external_addrs().await?;

        // TODO(justin): Do these concurrently and join.
        let mut metrics_dumps: Vec<TaggedMetricsDump> = Vec::with_capacity(noria_servers.len());
        for (_, external_addr) in noria_servers {
            let metrics_endpoint =
                format!("http://{}/metrics_dump", external_addr.to_string().as_str());
            let res = self
                .client
                .post(metrics_endpoint.as_str())
                .send()
                .await
                .map_err(|e| e.into())
                .map_err(rpc_err!("MetricsClient::get_metrics"))?;

            let json = res
                .json::<MetricsDump>()
                .await
                .map_err(|e| ReadySetError::SerializationFailed(e.to_string()))?;
            metrics_dumps.push(TaggedMetricsDump {
                addr: external_addr,
                metrics: json,
            });
        }

        Ok(metrics_dumps)
    }

    /// Resets the metrics on each noria-server in the deployment.
    pub async fn reset_metrics(&mut self) -> ReadySetResult<()> {
        let noria_servers = self.get_external_addrs().await?;

        for (_, external_addr) in noria_servers {
            let client = reqwest::Client::new();
            let metrics_endpoint = format!(
                "http://{}/reset_metrics",
                external_addr.to_string().as_str()
            );
            client
                .post(metrics_endpoint.as_str())
                .send()
                .await
                .map_err(|e| e.into())
                .map_err(rpc_err!("MetricsClient::reset_metrics"))?;
        }

        Ok(())
    }
}
