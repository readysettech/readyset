use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::bail;
use benchmarks::benchmark::{Benchmark, BenchmarkControl, DeploymentParameters};
use benchmarks::reporting::ReportMode;
use benchmarks::utils::readyset_ready;
use benchmarks::QUANTILES;
use clap::builder::ArgPredicate;
use clap::{Parser, ValueHint};
use readyset_server::Handle;
use readyset_server::{PrometheusBuilder, PrometheusHandle};
use readyset_util::shutdown::ShutdownSender;
use tracing::warn;

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Run ReadySet macrobenchmarks
///
/// The usage of this command is documented at <http://docs/benchmarking.html>
#[derive(Debug, Parser)]
#[command(name = "benchmark_cmd_runner", subcommand_negates_reqs = true)]
struct BenchmarkRunner {
    /// Skips the setup step when executing the `benchmark_cmd`.
    #[arg(long)]
    skip_setup: bool,

    /// The number of times we should run the benchmark.
    #[arg(long, default_value = "1")]
    iterations: u32,

    #[command(flatten)]
    tracing: readyset_tracing::Options,

    #[command(flatten)]
    deployment_params: DeploymentParameters,

    /// Pass in the deployment parameters as a YAML formatted file. This overrides
    /// `--instance-label`, `--prometheus-push-gateway`, `--prometheus-endpoint`,
    /// `--target-conn-str`, and `--setup-conn-str`.
    #[arg(long, value_hint = ValueHint::AnyPath)]
    deployment: Option<PathBuf>,

    #[command(subcommand)]
    benchmark_cmd: Option<Benchmark>,

    /// Pass in the benchmark_cmd parameters as a YAML formatted file. This overwrites
    /// any benchmark_cmd subcommand passed in.
    #[arg(long, value_hint = ValueHint::AnyPath, required(true))]
    benchmark: Option<PathBuf>,

    /// Location where benchmark reports are stored, either for validation or storage purposes
    #[arg(long, env = "REPORT_TARGET", requires_ifs([(ArgPredicate::IsPresent, "report_mode"), (ArgPredicate::IsPresent, "report_profile")]))]
    report_target: Option<String>,

    /// Enables storage / validation of benchmark results, when combined with report_target
    #[arg(long, env = "REPORT_MODE", requires_ifs([(ArgPredicate::IsPresent, "report_target"), (ArgPredicate::IsPresent, "report_profile")]))]
    report_mode: Option<ReportMode>,

    /// Profile name to save the report under, distinct tests should have unique profiles
    #[arg(long, requires_ifs([(ArgPredicate::IsPresent, "report_target"), (ArgPredicate::IsPresent, "report_mode")]))]
    report_profile: Option<String>,

    /// Records The commit id to aid potential future analysis
    #[arg(long, hide(true), env = "BUILDKITE_COMMIT")]
    report_commit_id: Option<String>,
}

fn make_prometheus_url(base: &str, benchmark_name_label: &str, instance_label: &str) -> String {
    format!("{base}/metrics/job/{benchmark_name_label}/instance/{instance_label}")
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
            let mut builder = PrometheusBuilder::new().with_push_gateway(
                prometheus,
                PUSH_GATEWAY_PUSH_INTERVAL,
                None,
                None,
            )?;
            for (key, value) in &self.benchmark_cmd.as_ref().unwrap().labels() {
                builder = builder.add_global_label(key, value);
            }
            let (recorder, exporter) = builder.build()?;
            let handle = recorder.handle();
            metrics::set_global_recorder(recorder)?;
            tokio::spawn(exporter);
            Some(handle)
        } else {
            None
        };

        Ok(handle)
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
            self.benchmark_cmd = Some(serde_yaml_ng::from_str(&std::fs::read_to_string(f)?)?);
        }

        Ok(())
    }

    pub async fn initialize_from_args(
        &mut self,
    ) -> anyhow::Result<Option<(Handle, ShutdownSender)>> {
        self.load_benchmark_cmd_from_args()?;

        let (params, handle) = if let Some(f) = &self.deployment {
            if !f.exists() {
                bail!(
                    "Deployment YAML file does not exist, {}",
                    f.to_str().unwrap()
                );
            }

            (serde_yaml_ng::from_str(&std::fs::read_to_string(f)?)?, None)
        } else {
            // --target-conn-str and --setup-conn-str are required unless one of the other methods
            // are passed. Since these are used in benchmarks and must always have a value, these
            // cannot be checked by clap.
            if self.deployment_params.target_conn_str.is_empty()
                || self.deployment_params.setup_conn_str.is_empty()
            {
                bail!(
                    "If --deployment is not supplied, passing \
                  deployment state through --target-conn-str and --setup-conn-str are required"
                );
            }

            (self.deployment_params.clone(), None)
        };
        self.deployment_params = params;

        Ok(handle)
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // Initializes `DeploymentParameters` and `Benchmark` from the set of arguments passed by
        // the user. These arguments need not be passed by the arguments in the flattened structs
        // directly, and instead may be passed via YAML.
        let handle = self.initialize_from_args().await?;

        let cmd_as_yaml = serde_yaml_ng::to_string(&self.benchmark_cmd.as_ref().unwrap())?;
        let deployment_as_yaml = serde_yaml_ng::to_string(&self.deployment_params)?;
        let identifier = format!("{cmd_as_yaml}\n{deployment_as_yaml}\n");
        println!("{identifier}");

        let prometheus_handle = self.init_prometheus().await?;

        let benchmark_cmd = self.benchmark_cmd.as_ref().unwrap();
        if !self.skip_setup {
            benchmark_cmd.setup(&self.deployment_params).await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

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
                println!("Iteration: {i} ---------------------------");
                benchmark_cmd.reset(&self.deployment_params).await?;
                readyset_ready(&readyset_target).await?;
            }
            let start_time = Instant::now();
            let result = benchmark_cmd.benchmark(&self.deployment_params).await?;
            let duration = start_time.elapsed();

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
                println!("Regression Analysis: {analysis:?}");
            }
            results.push((result, duration));
        }

        println!("Benchmark Results -----------------------");
        for (index, (result, duration)) in results.iter().enumerate() {
            let iteration_num = index + 1;
            println!("Iteration {iteration_num} Results:");
            for (metric, data) in &result.results {
                let hist = data.to_histogram();
                let samples = hist.len();
                let qps = samples as f64 / duration.as_secs() as f64;
                let min = hist.min();
                let max = hist.max();
                let mean = hist.mean();
                print!("\t{metric} ({} - {:?} goal) - Average QPS: {qps} - Min: {min} - Max: {max} - Mean: {mean}", data.unit, data.desired_action);
                for (label, quantile) in QUANTILES {
                    print!(" - {label}: {}", hist.value_at_quantile(*quantile));
                }
                println!();
            }
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

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut benchmark_cmd_runner = BenchmarkRunner::parse();
    println!("Benchmark cli options: {:?}", &benchmark_cmd_runner);
    let _ = benchmark_cmd_runner
        .tracing
        .init("benchmarks", "benchmark-deployment")?;

    benchmark_cmd_runner.run().await?;
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
