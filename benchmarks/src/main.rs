use std::time::{Duration, Instant};

use clap::Parser;
use metrics::{GaugeValue, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;

use benchmarks::benchmark::{Benchmark, BenchmarkControl};
use benchmarks::utils;

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

    /// URL to push Prometheus metrics to
    #[clap(long)]
    prometheus: String,

    #[clap(subcommand)]
    benchmark: Benchmark,
}

impl BenchmarkRunner {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let recorder = Box::leak(Box::new({
            let mut builder = PrometheusBuilder::new()
                .disable_http_listener()
                .idle_timeout(MetricKindMask::ALL, None)
                .push_gateway_config(&self.prometheus, Duration::from_secs(10));
            for (key, value) in &self.benchmark.labels() {
                builder = builder.add_global_label(key, value);
            }
            builder.build()
        }));
        let handle = recorder.handle();
        metrics::set_recorder(recorder)?;

        if !self.skip_setup && !self.benchmark.is_already_setup().await? {
            self.benchmark.setup().await?;
        }

        let start_time = Instant::now();
        utils::run_for(self.benchmark.benchmark(), self.run_for).await?;
        let duration = start_time.elapsed();

        // benchmark_gauge!() approximately expands to this
        if let Some(recorder) = metrics::try_recorder() {
            let key = metrics::Key::from_name("benchmark_duration_microseconds");
            recorder.register_gauge(
                &key,
                Some(Unit::Microseconds),
                Some("Time, in microseconds, that it took to run the benchmark"),
            );
            recorder.update_gauge(&key, GaugeValue::Absolute(duration.as_micros() as f64));
        }

        self.prometheus.push_str(&format!(
            "/metrics/job/{}/instance/{}",
            self.benchmark.name_label(),
            self.instance_label
        ));
        self.prometheus = self.prometheus.replace("//", "/");
        let client = reqwest::Client::default();
        let output = handle.render();
        client
            .put(&self.prometheus)
            .body(output)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let benchmark_runner = BenchmarkRunner::parse();
    benchmark_runner.run().await
}
