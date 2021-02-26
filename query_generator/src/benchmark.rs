use clap::Clap;
use humantime::format_duration;
use noria::DataType;
use std::collections::HashMap;
use tokio::time::Instant;

use noria::consensus::LocalAuthority;
use noria_server::{DurabilityMode, PersistenceParameters};
use query_generator::{ColumnName, GeneratorState, Operations, QueryOperation, TableName};

#[derive(Clap)]
pub struct Benchmark {
    /// Comma-separated list of query operations to benchmark.
    operations: Operations,

    #[clap(long)]
    shards: Option<usize>,

    #[clap(long, default_value = "1000")]
    rows_per_table: usize,
}

impl Benchmark {
    #[tokio::main]
    pub async fn run(self) -> anyhow::Result<()> {
        let Operations(ops) = &self.operations;
        eprintln!("Running benchmark of {} queries", ops.len());
        for ops in ops {
            self.benchmark_operations(ops).await?;
        }

        Ok(())
    }

    async fn benchmark_operations(&self, ops: &[QueryOperation]) -> anyhow::Result<()> {
        let mut gen = GeneratorState::default();
        let query = gen.generate_query(ops);
        eprintln!("Benchmarking {}", query.statement);
        let mut noria = self.setup_noria().await?;
        let query_name = "benchmark_query";
        noria.install_recipe(&query.to_recipe(query_name)).await?;
        for (table_name, rows) in query.state.generate_data(self.rows_per_table) {
            self.seed_data(&mut noria, table_name, rows).await?;
        }

        let mut view = noria.view(query_name).await?;
        let lookup_key = query.state.key();
        let start = Instant::now();
        view.lookup(&lookup_key, true).await?;
        eprintln!("Query ran in {}", format_duration(start.elapsed()));

        Ok(())
    }

    async fn seed_data(
        &self,
        noria: &mut noria_server::Handle<LocalAuthority>,
        table_name: &TableName,
        data: Vec<HashMap<&ColumnName, DataType>>,
    ) -> anyhow::Result<()> {
        let mut table = noria.table(table_name.into()).await?;
        let columns = table
            .columns()
            .iter()
            .cloned()
            .map(ColumnName::from)
            .collect::<Vec<_>>();
        table.i_promise_dst_is_same_process();
        table
            .insert_many(data.into_iter().map(|mut row| {
                columns
                    .iter()
                    .map(|col| row.remove(&col).unwrap())
                    .collect::<Vec<_>>()
            }))
            .await?;
        Ok(())
    }

    async fn setup_noria(&self) -> anyhow::Result<noria_server::Handle<LocalAuthority>> {
        let mut builder = noria_server::Builder::default();
        builder.set_sharding(self.shards);
        builder.set_persistence(PersistenceParameters {
            mode: DurabilityMode::DeleteOnExit,
            log_prefix: "benchmarks".to_owned(),
            ..Default::default()
        });
        let (mut noria, _) = builder.start_local().await?;
        futures_util::future::poll_fn(|cx| noria.poll_ready(cx)).await?;
        Ok(noria)
    }
}
