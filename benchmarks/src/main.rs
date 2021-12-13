use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{AppSettings, Parser, ValueHint};
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
#[clap(name = "benchmark_cmd_runner", global_setting = AppSettings::SubcommandsNegateReqs)]
struct BenchmarkRunner {
    /// Skips the setup setup when executing the `benchmark_cmd`.
    #[clap(long)]
    skip_setup: bool,

    /// The duartion, specified as the number of seconds that the experiment
    /// should be running. If `None` is provided, the experiment will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,

    /// Instead of running the benchmark_cmd, write the parameters to a benchmark_cmd
    /// specification file, to be run with the from-file subcommand.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    only_to_spec: Option<PathBuf>,

    #[clap(flatten)]
    logging: readyset_logging::Options,

    #[clap(flatten)]
    deployment_params: DeploymentParameters,

    /// Pass in the deployment parameters as a YAML formatted file. This overwrites
    /// any deployment_params passed manually.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    deployment: Option<PathBuf>,

    #[clap(subcommand)]
    benchmark_cmd: Option<Benchmark>,

    /// Pass in the benchmark_cmd parameters as a YAML formatted file. This overwrites
    /// any benchmark_cmd subcommand passed in.
    #[clap(long, value_hint = ValueHint::AnyPath, required(true))]
    benchmark: Option<PathBuf>,
}

impl BenchmarkRunner {
    pub async fn init_prometheus(&mut self) -> anyhow::Result<Option<PrometheusHandle>> {
        // Append the full pushgateway config path to the user provided
        // address.
        self.deployment_params.prometheus_push_gateway = self
            .deployment_params
            .prometheus_push_gateway
            .as_ref()
            .map(|s| {
                format!(
                    "{}/metrics/job/{}/instance/{}",
                    s.replace("//", "/"),
                    self.benchmark_cmd.as_ref().unwrap().name_label(),
                    self.deployment_params.instance_label
                )
            });

        let handle = if let Some(prometheus) = &self.deployment_params.prometheus_push_gateway {
            let mut builder = PrometheusBuilder::new()
                .disable_http_listener()
                .idle_timeout(MetricKindMask::ALL, None)
                .push_gateway_config(prometheus, PUSH_GATEWAY_PUSH_INTERVAL);
            for (key, value) in &self.benchmark_cmd.as_ref().unwrap().labels() {
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
        let push_gateway = self.deployment_params.prometheus_push_gateway.clone()?;
        let benchmark = self.benchmark_cmd.as_ref().unwrap();
        let forward = benchmark.forward_metrics(&self.deployment_params);

        if forward.is_empty() {
            return None;
        }

        if self.deployment_params.prometheus_endpoint.is_none() {
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
        if let Some(f) = &self.benchmark {
            self.benchmark_cmd = Some(serde_yaml::from_str(&std::fs::read_to_string(f)?)?);
        }

        if let Some(f) = &self.deployment {
            self.deployment_params = serde_yaml::from_str(&std::fs::read_to_string(f)?)?;
        }

        // After this point deployment_params and benchmark_cmd are guaranteed to be Some.

        println!(
            "{}",
            serde_yaml::to_string(&self.benchmark_cmd.as_ref().unwrap())?
        );

        println!("{}", serde_yaml::to_string(&self.deployment_params)?);

        if let Some(f) = &self.only_to_spec {
            let f = std::fs::File::create(f)?;
            serde_yaml::to_writer(f, &self.benchmark_cmd.as_ref().unwrap())?;
            return Ok(());
        }

        let prometheus_handle = self.init_prometheus().await?;

        let benchmark_cmd = self.benchmark_cmd.as_ref().unwrap();
        if !self.skip_setup
            && !benchmark_cmd
                .is_already_setup(&self.deployment_params)
                .await?
        {
            benchmark_cmd.setup(&self.deployment_params).await?;
        }

        let importer = self.start_metric_readers();

        let start_time = Instant::now();
        utils::run_for(
            benchmark_cmd.benchmark(&self.deployment_params),
            self.run_for,
        )
        .await?;
        let duration = start_time.elapsed();

        if let Some((handle, tx)) = importer {
            drop(tx);
            handle.await?;
        }

        benchmark_gauge!(
            "benchmark_duration",
            Microseconds,
            "Time, in microseconds, that it took to run the benchmark_cmd",
            duration.as_micros() as f64
        );

        // Push metrics recorded in the push gateway manually before exiting.
        if let (Some(addr), Some(prometheus_handle)) = (
            self.deployment_params.prometheus_push_gateway,
            prometheus_handle,
        ) {
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
    let benchmark_cmd_runner = BenchmarkRunner::parse();
    benchmark_cmd_runner.logging.init()?;

    benchmark_cmd_runner.run().await
}
