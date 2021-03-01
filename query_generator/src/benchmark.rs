use anyhow::anyhow;
use clap::Clap;
use humantime::format_duration;
use indicatif::ProgressIterator;
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;
use serde_with::{serde_as, DurationNanoSeconds};
use size_format::SizeFormatterSI;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

use noria::consensus::LocalAuthority;
use noria::DataType;
use noria_server::metrics::NoriaMetricsRecorder;
use noria_server::{DurabilityMode, PersistenceParameters};
use query_generator::{ColumnName, GeneratorState, Operations, QueryOperation, TableName};

/// Metrics collected during the run of an individual query
#[serde_as]
#[derive(Serialize)]
pub struct QueryMetrics {
    /// Memory (in bytes) used by materialized nodes prior to doing any reads
    cold_materialization_size: usize,

    /// Time to write when we hit a hole
    #[serde_as(as = "DurationNanoSeconds")]
    cold_write_time: Duration,

    /// Time spent forwarding dataflow during upqueries
    #[serde_as(as = "DurationNanoSeconds")]
    upquery_forward_time: Duration,

    /// Total time to read when we hit a hole
    #[serde_as(as = "DurationNanoSeconds")]
    cold_read_time: Duration,

    /// Time to write when we don't hit a hole
    #[serde_as(as = "DurationNanoSeconds")]
    warm_write_time: Duration,

    /// Time to read when we don't hit a hole
    #[serde_as(as = "DurationNanoSeconds")]
    warm_read_time: Duration,

    /// Memory (in bytes) used by materialized nodes after performing reads
    warm_materialization_size: usize,
}

/// All information about benchmarks run for an individual query
#[derive(Serialize)]
pub struct QueryBenchmarkResult {
    query: String,

    rows_per_table: usize,

    #[serde(flatten)]
    metrics: QueryMetrics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    JSON,
}

#[derive(Error, Debug)]
#[error("Benchmarking of query {query} failed:\n{source}")]
pub struct QueryBenchmarkError {
    query: String,
    source: anyhow::Error,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "table" => Ok(Self::Table),
            "json" => Ok(Self::JSON),
            s => Err(anyhow!(
                "Invalid format {}, expected one of \"table\" or \"json\"",
                s
            )),
        }
    }
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => f.write_str("table"),
            Self::JSON => f.write_str("json"),
        }
    }
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

fn format_query(query: &str) -> String {
    lazy_static! {
        static ref NEWLINE_TOKEN: Regex =
            Regex::new(r"(FROM|WHERE|(?:(?:INNER|LEFT(:? OUTER)?) )?JOIN)").unwrap();
    }
    NEWLINE_TOKEN.replace_all(query, "\n$1").into_owned()
}

fn write_results(results: &[QueryBenchmarkResult], format: OutputFormat) -> std::io::Result<()> {
    match format {
        OutputFormat::Table => {
            use prettytable::{cell, row};
            let mut table = prettytable::Table::new();
            table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
            table.set_titles(row![
                bFb => "Query",
                "cold_materialization_size",
                "cold_write_time",
                "upquery_forward_time",
                "cold_read_time",
                "warm_write_time",
                "warm_read_time",
                "warm_materialization_size",
            ]);
            for result in results {
                table.add_row(row![
                    b => format_query(&result.query),
                    format!("{:.2}B", SizeFormatterSI::new(
                        result.metrics.cold_materialization_size as u64
                    )),
                    format_duration(result.metrics.cold_write_time),
                    format_duration(result.metrics.upquery_forward_time),
                    format_duration(result.metrics.cold_read_time),
                    "TODO", // format_duration(result.metrics.warm_write_time),
                    format_duration(result.metrics.warm_read_time),
                    format!("{:.2}B", SizeFormatterSI::new(
                        result.metrics.warm_materialization_size as u64
                    )),
                ]);
            }
            table.printstd();
        }
        OutputFormat::JSON => {
            println!("{}", serde_json::to_string(results).unwrap());
        }
    }
    Ok(())
}

#[derive(Clap)]
pub struct Benchmark {
    /// Comma-separated list of query operations to benchmark.
    operations: Operations,

    /// Number of shards to run noria with
    #[clap(long)]
    shards: Option<usize>,

    /// Number of rows to seed for each table
    #[clap(long, default_value = "1000")]
    rows_per_table: usize,

    /// Dump the graphviz representation of the query graph before benchmarking each query
    #[clap(long)]
    dump_graph: bool,

    /// Format to use when writing benchmark results to stdout. Accepted values are "table" or
    /// "json".
    #[clap(short = 'o', default_value)]
    output_format: OutputFormat,
}

impl Benchmark {
    #[tokio::main]
    pub async fn run(self) -> anyhow::Result<()> {
        // SAFETY: Called before we spawn any other tasks
        unsafe {
            NoriaMetricsRecorder::install(1024)?;
        }

        let Operations(ops) = &self.operations;
        eprintln!("Running benchmark of {} queries", ops.len());
        let mut results = Vec::with_capacity(ops.len());
        for ops in ops.iter().progress() {
            match self.benchmark_operations(ops).await {
                Ok(result) => results.push(result),
                Err(e) => eprintln!("{}", e),
            }
        }

        write_results(&results, self.output_format)?;

        Ok(())
    }

    async fn benchmark_operations(
        &self,
        ops: &[QueryOperation],
    ) -> Result<QueryBenchmarkResult, QueryBenchmarkError> {
        let mut gen = GeneratorState::default();
        let query = gen.generate_query(ops);
        let query_str = format!("{}", query.statement);
        let res: anyhow::Result<_> = async {
            let mut noria = self.setup_noria().await?;
            let query_name = "benchmark_query";
            noria.install_recipe(&query.to_recipe(query_name)).await?;

            if self.dump_graph {
                println!("{}", noria.graphviz().await?);
            }

            let start = Instant::now();
            for (table_name, rows) in query.state.generate_data(self.rows_per_table) {
                self.seed_data(&mut noria, table_name, rows).await?;
            }
            let cold_write_time = start.elapsed();

            let metrics = noria.metrics_dump().await?;

            let cold_materialization_size = metrics
                .total("domain.total_node_state_size_bytes")
                .unwrap_or(0f64)
                .floor() as usize;

            let baseline_forward_time = metrics.total("domain.forward_time_us").unwrap_or(0f64);

            let mut view = noria.view(query_name).await?;
            let lookup_key = query.state.key();

            let start = Instant::now();
            view.lookup(&lookup_key, true).await?;
            let cold_read_time = start.elapsed();
            let metrics = noria.metrics_dump().await?;

            let warm_materialization_size = metrics
                .total("domain.total_node_state_size_bytes")
                .unwrap_or(0f64)
                .floor() as usize;

            let post_upquery_forward_time = metrics.total("domain.forward_time_us").unwrap_or(0f64);
            let upquery_forward_time = Duration::from_micros(
                (post_upquery_forward_time - baseline_forward_time).round() as u64,
            );

            let start = Instant::now();
            view.lookup(&lookup_key, true).await?;
            let warm_read_time = start.elapsed();

            NoriaMetricsRecorder::get().clear();
            Ok(QueryBenchmarkResult {
                query: format!("{}", query.statement),
                rows_per_table: self.rows_per_table,
                metrics: QueryMetrics {
                    cold_materialization_size,
                    cold_write_time,
                    upquery_forward_time, // TODO
                    cold_read_time,
                    // TODO(grfn): To collect this, we need to write a unique row then time how long
                    // it takes to do a blocking read of that row
                    warm_write_time: Duration::ZERO,
                    warm_read_time,
                    warm_materialization_size,
                },
            })
        }
        .await;

        res.map_err(|source| QueryBenchmarkError {
            query: query_str,
            source,
        })
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
        builder.set_aggressively_update_state_sizes(true);
        let (mut noria, _) = builder.start_local().await?;
        futures_util::future::poll_fn(|cx| noria.poll_ready(cx)).await?;
        Ok(noria)
    }
}
