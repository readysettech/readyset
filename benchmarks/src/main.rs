use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use benchmarks::benchmark::{Benchmark, BenchmarkControl, BenchmarkResults, DeploymentParameters};
use benchmarks::graph::GraphParams;
use benchmarks::reporting::ReportMode;
use benchmarks::utils::readyset_ready;
use benchmarks::{benchmark_histogram, QUANTILES};
use clap::{Parser, ValueHint};
use database_utils::DatabaseType;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use readyset_adapter::backend::noria_connector::ReadBehavior;
use readyset_adapter::backend::{MigrationMode, UnsupportedSetMode};
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::config::Host;
use tracing::warn;

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Run ReadySet macrobenchmarks
///
/// The usage of this command is documented at <http://docs/benchmarking.html>
#[derive(Parser)]
#[clap(name = "benchmark_cmd_runner", subcommand_negates_reqs = true)]
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
    tracing: readyset_tracing::Options,

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

    /// A file to append the set of benchmark results to, creates the file if it has not yet been
    /// created.
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

    /// When running the benchmark in `--local` or `--local_with_upstream` mode, by default, the
    /// data is not persisted to the underlying RocksDB storage (durability = MEMORY_ONLY). To
    /// enable durability in local mode use the `--persistent` option. When `--persistent` is
    /// specified, data is persisted to RocksDB, but it gets automatically deleted when the
    /// benchmark finishes.
    #[clap(long)]
    persistent: bool,

    /// Runs the benchmarks against a noria adapter and server run in the same process with the
    /// provided external upstream database. When using `--local` benchmark results may vary
    /// based on compiler optimizations, using `--release` will drastically improve results.
    ///
    /// If this argument is passed, the deployment parameter is ignored.
    #[clap(long)]
    local_with_upstream: Option<String>,

    /// Location where benchmark reports are stored, either for validation or storage purposes
    #[clap(long, env = "REPORT_TARGET", requires_all(&["report_mode", "report_profile"]))]
    report_target: Option<String>,

    /// Enables storage / validation of benchmark results, when combined with report_target
    #[clap(long, env = "REPORT_MODE", requires_all(&["report_target", "report_profile"]))]
    report_mode: Option<ReportMode>,

    /// Profile name to save the report under, distinct tests should have unique profiles
    #[clap(long, requires_all(&["report_target", "report_mode"]))]
    report_profile: Option<String>,

    /// Records the commit id to aid potential future analysis
    #[clap(long, hide(true), env = "BUILDKITE_COMMIT")]
    report_commit_id: Option<String>,

    #[clap(flatten)]
    graph_params: GraphParams,
}

fn make_prometheus_url(base: &str, benchmark_name_label: &str, instance_label: &str) -> String {
    format!(
        "{}/metrics/job/{}/instance/{}",
        base, benchmark_name_label, instance_label
    )
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
                make_prometheus_url(
                    s,
                    self.benchmark_cmd.as_ref().unwrap().name_label(),
                    &self.deployment_params.instance_label,
                )
            });

        let handle = if let Some(prometheus) = &self.deployment_params.prometheus_push_gateway {
            let mut builder = PrometheusBuilder::new()
                .idle_timeout(MetricKindMask::ALL, None)
                .with_push_gateway(prometheus, PUSH_GATEWAY_PUSH_INTERVAL, None, None)?;
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
            warn!("No prometheus endpoint passed but this benchmark forwards metrics. The benchmark metrics may be incomplete.");
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

    // Creates a local deployment that may include an external upstream database. This does not
    // mutate the upstream database when --skip-setup is passed.
    pub async fn start_local(
        &self,
        upstream_addr: Option<String>,
    ) -> anyhow::Result<(DeploymentParameters, Handle, ShutdownSender)> {
        let mut test_builder = TestBuilder::new(
            BackendBuilder::default()
                .unsupported_set_mode(UnsupportedSetMode::Allow)
                .require_authentication(false),
        )
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .read_behavior(ReadBehavior::Blocking)
        .persistent(self.persistent);

        if let Some(addr) = &upstream_addr {
            test_builder = test_builder.fallback_url(addr.clone());
        }

        let (target_conn_str, handle, shutdown_tx) = match self.deployment_params.database_type {
            DatabaseType::MySQL => {
                if matches!(&upstream_addr, Some(addr) if !addr.starts_with("mysql")) {
                    bail!("--database-type specifies MySQL but the upstream address does not");
                }

                let (mysql_opts, handle, shutdown_tx) = test_builder.build::<MySQLAdapter>().await;

                // TODO(REA-2787): We should eventually pass a database name here, but due to a bug
                // in the way we handle database names for MySQL connections, the
                // benchmarks don't work if we pass the name here. Since MySQL
                // supports connecting without an explicit name, we just omit it.
                let target_conn_str = format!(
                    "mysql://{}:{}",
                    mysql_opts.ip_or_hostname(),
                    mysql_opts.tcp_port(),
                );

                (target_conn_str, handle, shutdown_tx)
            }
            DatabaseType::PostgreSQL => {
                if matches!(&upstream_addr, Some(addr) if !addr.starts_with("postgres")) {
                    bail!("--database-type specifies PostgreSQL but the upstream address does not");
                }

                let (config, handle, shutdown_tx) = test_builder.build::<PostgreSQLAdapter>().await;
                let host = config
                    .get_hosts()
                    .first()
                    .expect("PostgreSQL URL has no hostname set");
                let host_str = match host {
                    Host::Tcp(tcp) => tcp.as_str(),
                    Host::Unix(p) => p.to_str().expect("Invalid UTF-8 in host"),
                };
                let port = config.get_ports()[0];
                let target_conn_str = format!(
                    "postgres://{}:{}/{}",
                    host_str, port, self.deployment_params.database_name,
                );

                (target_conn_str, handle, shutdown_tx)
            }
            DatabaseType::Vitess => {
                todo!()
            }
        };
        let setup_conn_str = upstream_addr.unwrap_or_else(|| target_conn_str.clone());

        Ok((
            DeploymentParameters {
                target_conn_str,
                setup_conn_str,
                ..self.deployment_params.clone()
            },
            handle,
            shutdown_tx,
        ))
    }

    /// Warn if deployment parameters are overwritten by `--local` or `--deployment`.
    pub fn warn_if_deployment_params(&self, reason: &str) {
        let params = &self.deployment_params;
        if !params.target_conn_str.is_empty()
            || !params.setup_conn_str.is_empty()
            || params.instance_label.as_str() != "local"
        {
            warn!(
                "--target-conn-str, --setup-conn-str, or --instance-label were provided but will \
                be overwritten by --{}",
                reason
            );
        }

        if params.prometheus_push_gateway.is_some() || params.prometheus_endpoint.is_some() {
            warn!(
                "--prometheus-push-gateway or --prometheus-endpoint were provided but will be \
                overwritten by --{}",
                reason
            );
        }
    }

    /// Log if benchmark parameters are overwritten by `--benchmark`.
    pub fn warn_if_benchmark_params(&self) {
        if self.benchmark_cmd.is_some() {
            warn!("A benchmark subcommand was provided by will be overwritten by --benchmark");
        }
    }

    fn load_benchmark_cmd_from_args(&mut self) -> anyhow::Result<()> {
        if let Some(f) = self.benchmark.take() {
            self.warn_if_benchmark_params();

            if !f.exists() {
                bail!(
                    "Benchmark YAML file does not exist, {}",
                    f.to_str().unwrap()
                );
            }
            self.benchmark_cmd = Some(serde_yaml::from_str(&std::fs::read_to_string(f)?)?);
        }

        Ok(())
    }

    pub async fn initialize_from_args(
        &mut self,
    ) -> anyhow::Result<Option<(Handle, ShutdownSender)>> {
        self.load_benchmark_cmd_from_args()?;

        let (params, handle) = if self.local_with_upstream.is_some() {
            self.warn_if_deployment_params("local-with-upstream");
            let (params, h, shutdown_tx) =
                self.start_local(self.local_with_upstream.clone()).await?;
            (params, Some((h, shutdown_tx)))
        } else if self.local {
            self.warn_if_deployment_params("local");

            if self.skip_setup {
                warn!("Ignoring --skip-setup as --local requires setup");
            }

            let (params, h, shutdown_tx) = self.start_local(None).await?;
            (params, Some((h, shutdown_tx)))
        } else if let Some(f) = &self.deployment {
            // Verify that the deployment is passed through some method.
            self.warn_if_deployment_params("deployment");
            if !f.exists() {
                bail!(
                    "Deployment YAML file does not exist, {}",
                    f.to_str().unwrap()
                );
            }

            (serde_yaml::from_str(&std::fs::read_to_string(f)?)?, None)
        } else {
            // --target-conn-str and --setup-conn-str are required unless one of the other methods
            // are passed. Since these are used in benchmarks and must always have a value, these
            // cannot be checked by clap.
            if self.deployment_params.target_conn_str.is_empty()
                || self.deployment_params.setup_conn_str.is_empty()
            {
                bail!(
                    "If --local, --local-with-upstream, --deployment are not supplied, passing \
                  deployment state through --target-conn-str and --setup-conn-str are required"
                );
            }

            (self.deployment_params.clone(), None)
        };
        self.deployment_params = params;

        Ok(handle)
    }

    pub async fn run(&mut self) -> anyhow::Result<Vec<BenchmarkResults>> {
        // Initializes `DeploymentParameters` and `Benchmark` from the set of arguments passed by
        // the user. These arguments need not be passed by the arguments in the flattened structs
        // directly, and instead may be passed via YAML or via arguments like `--local`.
        let handle = self.initialize_from_args().await?;

        let cmd_as_yaml = serde_yaml::to_string(&self.benchmark_cmd.as_ref().unwrap())?;
        let deployment_as_yaml = serde_yaml::to_string(&self.deployment_params)?;
        let identifier = format!("{}\n{}\n", cmd_as_yaml, deployment_as_yaml);
        println!("{}", identifier);

        if let Some(f) = &self.only_to_spec {
            let f = std::fs::File::create(f)?;
            serde_yaml::to_writer(f, &self.benchmark_cmd.as_ref().unwrap())?;
            return Ok(vec![]);
        }

        let prometheus_handle = self.init_prometheus().await?;

        let benchmark_cmd = self.benchmark_cmd.as_ref().unwrap();
        if !self.skip_setup || self.local {
            benchmark_cmd.setup(&self.deployment_params).await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        let importer = self.start_metric_readers();

        // Check that ReadySet has completed snapshotting via the readyset status.
        let readyset_target = format!(
            "{}/{}",
            self.deployment_params.target_conn_str, self.deployment_params.database_name
        );
        readyset_ready(&readyset_target).await?;

        let bench_start_time = std::time::SystemTime::now();

        let mut results = Vec::new();
        for i in 0..self.iterations {
            if self.iterations > 1 {
                println!("Iteration: {} ---------------------------", i);
                benchmark_cmd.reset(&self.deployment_params).await?;
                readyset_ready(&readyset_target).await?;
            }
            let start_time = Instant::now();
            let result = benchmark_cmd.benchmark(&self.deployment_params).await?;
            let duration = start_time.elapsed();
            benchmark_histogram!(
                "benchmark_duration",
                Microseconds,
                "Time, in microseconds, that it took to run the benchmark.".into(),
                duration.as_micros() as f64
            );

            if let Some(report_mode) = self.report_mode {
                let session = benchmarks::reporting::BenchSession {
                    start_time: bench_start_time,
                    commit_id: self.report_commit_id.clone().unwrap_or_default(),
                    template: benchmark_cmd.name().into(),
                    profile_name: self.report_profile.clone().unwrap(),
                };

                let analysis = benchmarks::reporting::report(
                    self.report_target.as_ref().unwrap(),
                    &session,
                    &result,
                    report_mode,
                )
                .await?;
                println!("Regression Analysis: {:?}", analysis);
            }
            results.push(result);
        }

        println!("Benchmark Results -----------------------");
        for (index, iteration) in results.iter().enumerate() {
            let iteration_num = index + 1;
            println!("Iteration {iteration_num} Results:");
            for (metric, data) in &iteration.results {
                let hist = data.to_histogram(0.0, 1.0);
                let samples = hist.len();
                let min = hist.min();
                let max = hist.max();
                let mean = hist.mean();
                print!("\t{metric} ({} - {:?} goal) - Samples: {samples} - Min: {min} - Max: {max} - Mean: {mean}", data.unit, data.desired_action);
                for (label, quantile) in QUANTILES {
                    print!(" - {label}: {}", hist.value_at_quantile(*quantile));
                }
                println!();
            }
        }

        // Write human-readable outputs if specified.
        if let Some(f) = &self.results_file {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(f)?;
            file.write_all(&serde_yaml::to_vec(&self.benchmark_cmd)?)?;
            file.write_all(&serde_yaml::to_vec(&self.deployment_params)?)?;
            file.write_all(format!("{:?}", results).as_bytes())?;
        }

        if let Some((handle, tx)) = importer {
            drop(tx);
            handle.await?;
        }

        // Push metrics recorded in the push gateway manually before exiting.
        if let (Some(addr), Some(prometheus_handle)) = (
            &self.deployment_params.prometheus_push_gateway,
            prometheus_handle,
        ) {
            let client = reqwest::Client::default();
            let output = prometheus_handle.render();
            client
                .put(addr)
                .body(output)
                .send()
                .await?
                .error_for_status()?;
        }

        if let Some((_, shutdown_tx)) = handle {
            shutdown_tx.shutdown().await;
        }

        Ok(results)
    }

    pub async fn run_graph(mut self) -> anyhow::Result<()> {
        // We need iterations to be set to 1 so that we only have to deal with one result per value
        // for the x axis
        if self.iterations != 1 {
            bail!("Can only run with --graph if --iterations=1");
        }

        self.load_benchmark_cmd_from_args()?;
        let mut results = self.graph_params.results_writer()?;
        for run in self.graph_params.clone().runs() {
            self.benchmark_cmd
                .as_mut()
                .unwrap()
                .update_from(run.as_args())?;
            let run_results = self.run().await?;

            assert_eq!(run_results.len(), 1);
            results.write_result(run.x_value(), run_results.into_iter().next().unwrap())?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut benchmark_cmd_runner = BenchmarkRunner::parse();
    benchmark_cmd_runner
        .tracing
        .init("benchmarks", "benchmark-deployment")?;

    if benchmark_cmd_runner.graph_params.graph {
        benchmark_cmd_runner.run_graph().await?;
    } else {
        benchmark_cmd_runner.run().await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;
    #[proptest]
    fn make_prometheus_url(
        #[strategy("[a-z]+://[a-z0-9/]+")] base: String,
        benchmark_name_label: String,
        instance_label: String,
    ) {
        let url = super::make_prometheus_url(&base, &benchmark_name_label, &instance_label);
        url::Url::parse(&url).unwrap();
    }
}
