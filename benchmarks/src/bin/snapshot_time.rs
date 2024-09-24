use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueHint};
use readyset_client::recipe::changelist::ChangeList;
use readyset_data::Dialect;
use readyset_server::metrics::{
    install_global_recorder, CompositeMetricsRecorder, MetricsRecorder,
};
use readyset_server::NoriaMetricsRecorder;

#[derive(Parser)]
#[command(name = "snapshot_time")]
struct SnapshotBenchmark {
    /// Sets the upstream database URL for the snapshot
    #[arg(short, long, value_hint = ValueHint::Url)]
    replication_url: String,
    /// How many times to repeat the benchmark
    #[arg(short, long, default_value = "1")]
    iterations: usize,
    /// An optional SQL file with queries to install and measure the time required
    /// for this
    #[arg(short, long)]
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

            if let Some(queries) = &self.queries {
                let queries_sql = std::fs::read_to_string(queries)?;
                let start = Instant::now();
                noria
                    .extend_recipe(
                        ChangeList::from_str(queries_sql, Dialect::DEFAULT_MYSQL).unwrap(),
                    )
                    .await?;
                println!("Migration time:    {} s", start.elapsed().as_secs());
            }

            shutdown_tx.shutdown().await;
        }

        Ok(())
    }
}

fn init_metrics_recorder() {
    let rec = CompositeMetricsRecorder::with_recorders(vec![MetricsRecorder::Noria(
        NoriaMetricsRecorder::new(),
    )]);
    install_global_recorder(rec);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bench = SnapshotBenchmark::parse();
    bench.run().await
}
