use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueHint};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

use benchmarks::benchmark::{Benchmark, BenchmarkControl, DeploymentParameters};
use benchmarks::benchmark_gauge;
use benchmarks::utils;

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[clap(name = "benchmark_runner")]
struct BenchmarkRunner {
    /// Skips the setup setup when executing the `benchmark`.
    #[clap(long)]
    skip_setup: bool,

    /// The duartion, specified as the number of seconds that the experiment
    /// should be running. If `None` is provided, the experiment will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,

    #[clap(flatten)]
    pub deployment: DeploymentParameters,

    /// Instead of running the benchmark, write the parameters to a benchmark
    /// specification file, to be run with the from-file subcommand.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    only_to_spec: Option<PathBuf>,

    #[clap(long, value_hint = ValueHint::AnyPath)]
    from_file: Option<PathBuf>,

    #[clap(subcommand)]
    benchmark: Option<Benchmark>,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

impl BenchmarkRunner {
    pub async fn init_prometheus(&mut self) -> anyhow::Result<Option<PrometheusHandle>> {
        // Append the full pushgateway config path to the user provided
        // address.
        self.deployment.prometheus_push_gateway =
            self.deployment.prometheus_push_gateway.as_ref().map(|s| {
                format!(
                    "{}/metrics/job/{}/instance/{}",
                    s.replace("//", "/"),
                    self.benchmark.as_ref().unwrap().name_label(),
                    self.deployment.instance_label
                )
            });

        let handle = if let Some(prometheus) = &self.deployment.prometheus_push_gateway {
            let mut builder = PrometheusBuilder::new()
                .disable_http_listener()
                .idle_timeout(MetricKindMask::ALL, None)
                .push_gateway_config(prometheus, PUSH_GATEWAY_PUSH_INTERVAL);
            for (key, value) in &self.benchmark.as_ref().unwrap().labels() {
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
        let push_gateway = self.deployment.prometheus_push_gateway.clone()?;
        let benchmark = self.benchmark.as_ref().unwrap();
        let forward = benchmark.forward_metrics(&self.deployment);

        if forward.is_empty() {
            return None;
        }

        if self.deployment.prometheus_endpoint.is_none() {
            warn!("No prometheus endpoint passed but this benchmark fowards metrics. The benchmark metrics may be incomplete.");
            return None;
        }

        let global_labels = Arc::new(benchmark.labels().into_iter().collect::<Vec<_>>());

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
        if let Some(f) = &self.from_file {
            self.benchmark = Some(serde_yaml::from_str(&std::fs::read_to_string(f)?)?);
        }

        println!(
            "{}",
            serde_yaml::to_string(&self.benchmark.as_ref().unwrap())?
        );

        if let Some(f) = &self.only_to_spec {
            let f = std::fs::File::create(f)?;
            serde_yaml::to_writer(f, &self.benchmark.as_ref().unwrap())?;
            return Ok(());
        }

        let prometheus_handle = self.init_prometheus().await?;

        let benchmark = self.benchmark.as_ref().unwrap();
        if !self.skip_setup && !benchmark.is_already_setup(&self.deployment).await? {
            benchmark.setup(&self.deployment).await?;
        }

        let importer = self.start_metric_readers();

        let start_time = Instant::now();
        utils::run_for(benchmark.benchmark(&self.deployment), self.run_for).await?;
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
            (self.deployment.prometheus_push_gateway, prometheus_handle)
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
