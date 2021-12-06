use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

use benchmarks::benchmark::{Benchmark, BenchmarkControl};
use benchmarks::benchmark_gauge;
use benchmarks::utils;

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[clap(name = "benchmark_runner")]
struct BenchmarkRunner {
    /// Skips the setup setup when executing the `benchmark`.
    #[clap(long)]
    skip_setup: bool,

    /// Instance label, for metrics.  In CI, it makes sense to set this to the
    /// CL# or commit hash.
    #[clap(long, default_value("local"))]
    instance_label: String,

    /// The duartion, specified as the number of seconds that the experiment
    /// should be running. If `None` is provided, the experiment will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,

    /// Address of a push gateway for a benchmark's prometheus metrics.
    #[clap(long)]
    prometheus_push_gateway: Option<String>,

    #[clap(subcommand)]
    benchmark: Benchmark,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

impl BenchmarkRunner {
    pub async fn init_prometheus(&mut self) -> anyhow::Result<Option<PrometheusHandle>> {
        // Append the full pushgateway config path to the user provided
        // address.
        self.prometheus_push_gateway = self.prometheus_push_gateway.as_ref().map(|s| {
            format!(
                "{}/metrics/job/{}/instance/{}",
                s.replace("//", "/"),
                self.benchmark.name_label(),
                self.instance_label
            )
        });

        let handle = if let Some(prometheus) = &self.prometheus_push_gateway {
            let mut builder = PrometheusBuilder::new()
                .disable_http_listener()
                .idle_timeout(MetricKindMask::ALL, None)
                .push_gateway_config(prometheus, PUSH_GATEWAY_PUSH_INTERVAL);
            for (key, value) in &self.benchmark.labels() {
                builder = builder.add_global_label(key, value);
            }
            let (recorder, exporter) = builder.build_with_exporter()?;
            let handle = recorder.handle();
            metrics::set_boxed_recorder(Box::new(recorder))?;
            tokio::spawn(exporter);
            Some(handle)
        } else {
            None
        };

        Ok(handle)
    }

    pub fn start_metric_readers(&self) -> Option<(JoinHandle<()>, oneshot::Sender<()>)> {
        let push_gateway = self.prometheus_push_gateway.clone()?;
        let forward = self.benchmark.forward_metrics();
        if forward.is_empty() {
            return None;
        }

        let global_labels = Arc::new(self.benchmark.labels().into_iter().collect::<Vec<_>>());

        let (tx, mut rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(PUSH_GATEWAY_PUSH_INTERVAL);
            let client = reqwest::Client::new();
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for item in forward.clone().into_iter() {
                            let req = client.post(&push_gateway);
                            if let Err(e) = item.forward(req, global_labels.clone()).await {
                                warn!("Failed to forward metrics: {}", e);
                            }
                        }
                    },
                    _ = &mut rx => break
                }
            }
        });

        Some((handle, tx))
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let prometheus_handle = self.init_prometheus().await?;

        if !self.skip_setup && !self.benchmark.is_already_setup().await? {
            self.benchmark.setup().await?;
        }

        let importer = self.start_metric_readers();

        let start_time = Instant::now();
        utils::run_for(self.benchmark.benchmark(), self.run_for).await?;
        let duration = start_time.elapsed();

        if let Some((handle, tx)) = importer {
            drop(tx);
            handle.await?;
        }

        benchmark_gauge!(
            "benchmark_duration",
            Microseconds,
            "Time, in microseconds, that it took to run the benchmark",
            duration.as_micros() as f64
        );

        // Push metrics recorded in the push gateway manually before exiting.
        if let (Some(addr), Some(prometheus_handle)) =
            (self.prometheus_push_gateway, prometheus_handle)
        {
            let client = reqwest::Client::default();
            let output = prometheus_handle.render();
            client
                .put(&addr)
                .body(output)
                .send()
                .await?
                .error_for_status()?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let benchmark_runner = BenchmarkRunner::parse();
    benchmark_runner.logging.init()?;

    benchmark_runner.run().await
}
