use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{AppSettings, Parser, ValueHint};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use mysql_async::prelude::Queryable;
use noria::status::{ReadySetStatus, SnapshotStatus};
use noria_server::Handle;
use std::convert::TryFrom;
use std::io::Write;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

use noria_client_test_helpers::mysql_helpers::MySQLAdapter;
use noria_client_test_helpers::setup_like_prod_with_handle;

use benchmarks::benchmark::{Benchmark, BenchmarkControl, BenchmarkResults, DeploymentParameters};
use benchmarks::benchmark_histogram;

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Run ReadySet macrobenchmarks
///
/// The usage of this command is documented at <http://docs/benchmarking.html>
#[derive(Parser)]
#[clap(name = "benchmark_cmd_runner", global_setting = AppSettings::SubcommandsNegateReqs)]
struct BenchmarkRunner {
    /// Skips the setup step when executing the `benchmark_cmd`.
    #[clap(long)]
    skip_setup: bool,

    /// The number of times we should run the benchmark.
    #[clap(long, default_value = "1")]
    iterations: u32,

    /// Instead of running the benchmark_cmd, write the parameters to a benchmark_cmd
    /// specification file, to be run with the from-file subcommand.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    only_to_spec: Option<PathBuf>,

    #[clap(flatten)]
    logging: readyset_logging::Options,

    #[clap(flatten)]
    deployment_params: DeploymentParameters,

    /// Pass in the deployment parameters as a YAML formatted file. This overrides
    /// `--instance-label`, `--prometheus-push-gateway`, `--prometheus-endpoint`,
    /// `--target-conn-str`, and `--setup-conn-str`.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    deployment: Option<PathBuf>,

    #[clap(subcommand)]
    benchmark_cmd: Option<Benchmark>,

    /// Pass in the benchmark_cmd parameters as a YAML formatted file. This overwrites
    /// any benchmark_cmd subcommand passed in.
    #[clap(long, value_hint = ValueHint::AnyPath, required(true))]
    benchmark: Option<PathBuf>,

    /// Treats the target database as a ReadySet database, and polls on snapshot completion
    /// before beginning `benchmark`.
    #[clap(long)]
    wait_for_snapshot: bool,

    /// A file to write the human-readable set of benchmark results to. Results are appended to the
    /// file.
    #[clap(long, value_hint = ValueHint::FilePath)]
    results_file: Option<PathBuf>,

    /// Runs the benchmarks against a noria adapter and server run in the same process. Note that
    /// some of the benchmarks with certain schemas may not work without an upstream database.
    /// When using `--local` benchmark results may vary based on compiler optimizations, using
    /// `--release` will drastically improve results.
    ///
    /// If this argument is passed, the deployment parameter is ignored.
    #[clap(long)]
    local: bool,

    /// Runs the benchmarks against a noria adapter and server run in the same process with the
    /// provided external upstream MySQL database. When using `--local` benchmark results may vary
    /// based on compiler optimizations, using `--release` will drastically improve results.
    ///
    /// If this argument is passed, the deployment parameter is ignored.
    #[clap(long)]
    local_with_mysql: Option<String>,
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
                .idle_timeout(MetricKindMask::ALL, None)
                .with_push_gateway(prometheus, PUSH_GATEWAY_PUSH_INTERVAL)?;
            for (key, value) in &self.benchmark_cmd.as_ref().unwrap().labels() {
                builder = builder.add_global_label(key, value);
            }
            let (recorder, exporter) = builder.build()?;
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

    pub async fn start_local(&self, mysql_addr: Option<String>) -> (DeploymentParameters, Handle) {
        let (mysql_opts, handle) = setup_like_prod_with_handle::<MySQLAdapter>(
            noria_client::BackendBuilder::new().require_authentication(false),
            mysql_addr.clone(),
            true, // wait_for_backend
        )
        .await;

        let target_conn_str = format!(
            "mysql://{}:{}",
            mysql_opts.ip_or_hostname(),
            mysql_opts.tcp_port()
        );

        let setup_conn_str = mysql_addr.unwrap_or_else(|| target_conn_str.clone());

        (
            DeploymentParameters {
                target_conn_str,
                setup_conn_str,
                ..Default::default()
            },
            handle,
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        if let Some(f) = &self.benchmark {
            self.benchmark_cmd = Some(serde_yaml::from_str(&std::fs::read_to_string(f)?)?);
        }

        let mut handle = None;
        if self.local_with_mysql.is_some() {
            let (params, h) = self.start_local(self.local_with_mysql.clone()).await;
            handle = Some(h);
            self.deployment_params = params;
        } else if self.local {
            let (params, h) = self.start_local(None).await;
            handle = Some(h);
            self.deployment_params = params;
        } else if let Some(f) = &self.deployment {
            self.deployment_params = serde_yaml::from_str(&std::fs::read_to_string(f)?)?;
        };

        // After this point deployment_params and benchmark_cmd are guaranteed to be Some.
        let cmd_as_yaml = serde_yaml::to_string(&self.benchmark_cmd.as_ref().unwrap())?;
        let deployment_as_yaml = serde_yaml::to_string(&self.deployment_params)?;
        let identifier = format!("{}\n{}\n", cmd_as_yaml, deployment_as_yaml);
        println!("{}", identifier);

        if let Some(f) = &self.only_to_spec {
            let f = std::fs::File::create(f)?;
            serde_yaml::to_writer(f, &self.benchmark_cmd.as_ref().unwrap())?;
            return Ok(());
        }

        let prometheus_handle = self.init_prometheus().await?;

        let benchmark_cmd = self.benchmark_cmd.as_ref().unwrap();
        if !self.skip_setup {
            benchmark_cmd.setup(&self.deployment_params).await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        let importer = self.start_metric_readers();

        // Check that ReadySet has completed snapshotting via the readyset status.
        // TODO(justin): Abstract this functionality as it is generally useful for tests.
        if self.wait_for_snapshot {
            println!("Waiting for snapshotting to complete...");
            let opts =
                mysql_async::Opts::from_url(&self.deployment_params.target_conn_str).unwrap();
            let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

            loop {
                let res: Vec<mysql_async::Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
                let status = ReadySetStatus::try_from(res)?;
                if status.snapshot_status == SnapshotStatus::Completed {
                    println!("Snapshotting finished!");
                    break;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        let mut results = Vec::new();
        for i in 0..self.iterations {
            if self.iterations > 1 {
                println!("Iteration: {} ---------------------------", i);
                benchmark_cmd.reset(&self.deployment_params).await?;
            }
            let start_time = Instant::now();
            let result = benchmark_cmd.benchmark(&self.deployment_params).await?;
            let duration = start_time.elapsed();
            benchmark_histogram!(
                "benchmark_duration",
                Microseconds,
                "Time, in microseconds, that it took to run the benchmark.",
                duration.as_micros() as f64
            );
            println!("{}", result);
            results.push(result);
        }

        println!("Benchmark Results -----------------------");
        let results = BenchmarkResults::aggregate(&results);
        println!("{}", results);

        // Write human-readable outputs if specified.
        if let Some(f) = self.results_file {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(f)?;
            file.write_all(&serde_yaml::to_vec(&self.benchmark_cmd)?)?;
            file.write_all(&serde_yaml::to_vec(&self.deployment_params)?)?;
            file.write_all(format!("{}", results).as_bytes())?;
        }

        if let Some((handle, tx)) = importer {
            drop(tx);
            handle.await?;
        }

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

        if let Some(h) = handle.as_mut() {
            h.shutdown();
            h.wait_done().await;
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
