use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueHint};
use metrics_exporter_prometheus::PrometheusBuilder;
use readyset_client::metrics::{recorded, MetricsDump};
use readyset_client::recipe::changelist::ChangeList;
use readyset_data::Dialect;
use readyset_server::metrics::{get_global_recorder, install_global_recorder, Render};

#[derive(Parser)]
#[clap(name = "snapshot_time")]
struct SnapshotBenchmark {
    /// Sets the upstream database URL for the snapshot
    #[clap(short, long, value_hint = ValueHint::Url)]
    replication_url: String,
    /// How many times to repeat the benchmark
    #[clap(short, long, default_value = "1")]
    iterations: usize,
    /// An optional SQL file with queries to install and measure the time required
    /// for this
    #[clap(short, long)]
    queries: Option<PathBuf>,
}

impl SnapshotBenchmark {
    async fn run(self) -> anyhow::Result<()> {
        readyset_tracing::init_test_logging();
        init_metrics_recorder();

        for _ in 0..self.iterations {
            let mut builder = readyset_server::Builder::for_tests();
            let persistence = readyset_server::PersistenceParameters {
                mode: readyset_server::DurabilityMode::DeleteOnExit,
                ..Default::default()
            };
            builder.set_persistence(persistence);
            builder.set_replication_url(self.replication_url.clone());

            let start = Instant::now();
            let (mut noria, shutdown_tx) = builder.start_local().await?;
            noria.backend_ready().await;

            println!("Snapshot time:     {} s", start.elapsed().as_secs());

            println!("Tables replicated: {}", noria.tables().await?.len());

            println!("Disk space used:   {:.2} MiB", tables_size_metric_mib());

            if let Some(queries) = &self.queries {
                let queries_sql = std::fs::read_to_string(queries)?;
                let start = Instant::now();
                noria
                    .extend_recipe(
                        ChangeList::from_str(queries_sql, Dialect::DEFAULT_MYSQL).unwrap(),
                    )
                    .await?;
                println!("Migration time:    {} s", start.elapsed().as_secs());
                println!("Disk space used:   {:.2} MiB", tables_size_metric_mib());
            }

            shutdown_tx.shutdown().await;
        }

        Ok(())
    }
}

fn init_metrics_recorder() {
    let recorder = PrometheusBuilder::new().build_recorder();
    install_global_recorder(recorder).unwrap();
}

fn tables_size_metric_mib() -> f64 {
    let metrics_handle = get_global_recorder().unwrap();

    let metrics: MetricsDump = serde_json::from_str(&metrics_handle.render()).unwrap();

    let base_sizes = &metrics.metrics[recorded::ESTIMATED_BASE_TABLE_SIZE_BYTES];

    let byte_size: f64 = base_sizes.iter().map(|s| s.value.value().unwrap()).sum();

    byte_size / 1024. / 1024.
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bench = SnapshotBenchmark::parse();
    bench.run().await
}
