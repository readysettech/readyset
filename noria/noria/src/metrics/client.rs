use noria_errors::{rpc_err, ReadySetError, ReadySetResult};
use url::Url;

use crate::metrics::MetricsDump;
use crate::ControllerHandle;

/// A metrics dump tagged with the address it was received from.
#[derive(Debug)]
pub struct TaggedMetricsDump {
    /// The URI of the noria-server the metrics dump was received
    /// from.
    pub addr: Url,
    /// The set of dumped metrics.
    pub metrics: MetricsDump,
}

/// The MetricsClient handles operations to the metrics collection framework across
/// a Noria deployment.
pub struct MetricsClient {
    controller: ControllerHandle,
    client: reqwest::Client,
}

impl MetricsClient {
    /// Instantiates a new metrics client connected to the deployment associated with
    /// `controller`.
    pub fn new(controller: ControllerHandle) -> ReadySetResult<Self> {
        Ok(MetricsClient {
            controller,
            client: reqwest::Client::new(),
        })
    }

    /// Retrieves the RPC URI for each noria-server in the deployment.
    pub async fn get_workers(&mut self) -> ReadySetResult<Vec<Url>> {
        self.controller.healthy_workers().await
    }

    /// Retrieves metrics for a single noria-server in a deployment.
    pub async fn get_metrics_for_server(&mut self, url: Url) -> ReadySetResult<TaggedMetricsDump> {
        let metrics_endpoint = url.join("metrics_dump")?;
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
        Ok(TaggedMetricsDump {
            addr: url,
            metrics: json,
        })
    }

    /// Retrieves metrics from each noria-server in a deployment and aggregates the results
    /// into a single json string.
    pub async fn get_metrics(&mut self) -> ReadySetResult<Vec<TaggedMetricsDump>> {
        let noria_servers = self.get_workers().await?;

        // TODO(justin): Do these concurrently and join.
        let mut metrics_dumps: Vec<TaggedMetricsDump> = Vec::with_capacity(noria_servers.len());
        for uri in noria_servers {
            metrics_dumps.push(self.get_metrics_for_server(uri).await?);
        }

        Ok(metrics_dumps)
    }

    /// Resets the metrics on each noria-server in the deployment.
    pub async fn reset_metrics(&mut self) -> ReadySetResult<()> {
        let noria_servers = self.get_workers().await?;

        for uri in noria_servers {
            let client = reqwest::Client::new();
            let metrics_endpoint = uri.join("reset_metrics")?;
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
