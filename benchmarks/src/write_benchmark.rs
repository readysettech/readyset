use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use async_trait::async_trait;
use clap::{Parser, ValueHint};
use database_utils::{DatabaseConnection, DatabaseError, DatabaseURL, QueryableConnection};
use itertools::Itertools;
use metrics::Unit;
use nom_sql::{
    BinaryOperator, CacheInner, CreateCacheStatement, Dialect, DialectDisplay, Expr,
    FieldDefinitionExpr, ItemPlaceholder, Literal, SelectStatement, TableExpr, TableExprInner,
};
use parking_lot::Mutex;
use query_generator::{ColumnName, TableSpec};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{debug, info};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram};

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct WriteBenchmark {
    /// Path to the desired database SQL schema. Each table must have a primary key to generate
    /// data.
    #[arg(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[arg(long)]
    target_qps: Option<u64>,

    /// The number of threads to execute the read benchmark across.
    #[arg(long, default_value = "1")]
    threads: u64,

    /// The number of connections to use for the connection pool to the database
    #[arg(long, default_value = "64")]
    pool_size: usize,

    /// The duration, specified as the number of seconds that the benchmark should be running. If
    /// `None` is provided, the benchmark will run until it is interrupted.
    #[arg(long, value_parser = crate::utils::seconds_as_str_to_duration)]
    pub run_for: Option<Duration>,

    /// Optionally create this many indices in each of the tables that're being written to.
    ///
    /// If any of the tables do not have enough columns to create this many distinct indices, the
    /// benchmark run will fail
    #[arg(long)]
    indices_per_table: Option<usize>,
}

#[derive(Clone)]
pub struct WriteBenchmarkThreadData {
    upstream_conn_str: String,
    target_qps: Option<u64>,
    threads: u64,
    pool_size: usize,

    /// Tables we will generate data for from the schema.
    tables: Vec<Arc<Mutex<TableSpec>>>,
}

impl WriteBenchmarkThreadData {
    fn new(w: &WriteBenchmark, deployment: &DeploymentParameters) -> Result<Self> {
        let ddl = std::fs::read_to_string(&w.schema)?;
        let schema = DatabaseSchema::new(&ddl, deployment.database_type.into())?;
        let database_spec = DatabaseGenerationSpec::new(schema);
        let tables = database_spec
            .tables
            .into_values()
            .map(|v| Arc::new(Mutex::new(v.table)))
            .collect();

        Ok(Self {
            upstream_conn_str: deployment.target_conn_str.clone(),
            target_qps: w.target_qps,
            threads: w.threads,
            pool_size: w.pool_size,
            tables,
        })
    }
}

fn columns_for_indices(num_indices: usize, table: &TableSpec) -> Result<Vec<Vec<ColumnName>>> {
    let res: Vec<_> = (1..=table.columns.len())
        .flat_map(|len| table.columns.keys().cloned().permutations(len))
        .take(num_indices)
        .collect();

    if res.len() < num_indices {
        bail!(
            "Not enough columns in table {} to make {num_indices} indices",
            table.name
        );
    }

    Ok(res)
}

fn query_indexed_by_columns(
    table: &TableSpec,
    cols: Vec<ColumnName>,
    dialect: Dialect,
) -> SelectStatement {
    SelectStatement {
        tables: vec![TableExpr {
            inner: TableExprInner::Table(table.name.clone().into()),
            alias: None,
        }],
        fields: vec![FieldDefinitionExpr::All],
        where_clause: cols
            .into_iter()
            .enumerate()
            .map(|(i, c)| Expr::BinaryOp {
                lhs: Box::new(Expr::Column(c.into())),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(Literal::Placeholder(match dialect {
                    Dialect::PostgreSQL => ItemPlaceholder::DollarNumber(i.try_into().unwrap()),
                    Dialect::MySQL => ItemPlaceholder::QuestionMark,
                }))),
            })
            .reduce(|lhs, rhs| Expr::BinaryOp {
                lhs: Box::new(lhs),
                op: BinaryOperator::And,
                rhs: Box::new(rhs),
            }),
        ..Default::default()
    }
}

async fn create_indices(
    conn: &mut DatabaseConnection,
    num_indices: usize,
    schema: &DatabaseSchema,
) -> Result<()> {
    for table_spec in schema.tables().values() {
        info!(num_indices, table = %table_spec.table.name, "Creating indices in table");
        for cols in columns_for_indices(num_indices, &table_spec.table)? {
            let query = query_indexed_by_columns(&table_spec.table, cols, conn.dialect());
            let create_cache = CreateCacheStatement {
                name: None,
                inner: Ok(CacheInner::Statement(Box::new(query))),
                always: false,
                concurrently: false,
                unparsed_create_cache_statement: None,
            };
            conn.query_drop(create_cache.display(conn.dialect()).to_string())
                .await?;
        }
    }

    Ok(())
}

#[async_trait]
impl BenchmarkControl for WriteBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        let mut conn = DatabaseURL::from_str(&deployment.target_conn_str)?
            .connect(None)
            .await?;

        let ddl = std::fs::read_to_string(self.schema.as_path())?;
        let schema = DatabaseSchema::new(&ddl, (&conn).into())?;
        conn.query_drop(&ddl).await?;
        if let Some(num_indices) = self.indices_per_table {
            create_indices(&mut conn, num_indices, &schema).await?;
        }

        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        multi_thread::run_multithread_benchmark::<Self>(
            self.threads,
            WriteBenchmarkThreadData::new(self, deployment)?,
            self.run_for,
        )
        .await
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(
            "schema".to_string(),
            self.schema.to_string_lossy().to_string(),
        );
        labels.insert(
            "target_qps".to_string(),
            self.target_qps.unwrap_or(0).to_string(),
        );
        labels.insert("threads".to_string(), self.threads.to_string());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        Vec::new()
    }

    fn name(&self) -> &'static str {
        "write_benchmark"
    }

    fn data_generator(&mut self) -> Option<&mut DataGenerator> {
        None
    }
}

#[async_trait]
impl MultithreadBenchmark for WriteBenchmark {
    type BenchmarkResult = u128;
    type Parameters = WriteBenchmarkThreadData;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let queries_this_interval = results.len();
        let results_data =
            benchmark_results.entry("query_duration", Unit::Microseconds, MetricGoal::Decreasing);
        for l in results {
            results_data.push(l as f64);
            hist.record(u64::try_from(l).unwrap()).unwrap();
            benchmark_histogram!(
                "write_benchmark.query_duration",
                Microseconds,
                "Duration of queries executed".into(),
                l as f64
            );
        }
        benchmark_counter!(
            "write_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run".into(),
            queries_this_interval as _
        );
        let qps = hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!(
            "write_benchmark.qps",
            Count,
            "Queries per second".into(),
            qps
        );
        debug!(
            "qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999))
        );

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()> {
        let url = DatabaseURL::from_str(&params.upstream_conn_str)?;
        let dialect = url.dialect();
        let pool = url
            .pool_builder(None)?
            .max_connections(params.pool_size)
            .build()?;
        let mut throttle_interval =
            multi_thread::throttle_interval(params.target_qps, params.threads);
        let (err_tx, mut err_rx) = mpsc::channel::<DatabaseError>(1);
        loop {
            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }
            if let Ok(err) = err_rx.try_recv() {
                return Err(err.into());
            }
            let pool = pool.clone();
            let sender = sender.clone();
            let err_tx = err_tx.clone();
            let insert = {
                // TODO(justin): Evaluate if we can improve performance by precomputing these
                // variables outside of the loop.
                let mut rng = rand::thread_rng();
                let index: usize = rng.gen_range(0..(params.tables.len()));
                let mut spec = params.tables.get(index).unwrap().lock();
                let table_name = spec.name.clone();
                let data = spec.generate_data_from_index(1, 0, true);
                let columns = spec.columns.keys().collect::<Vec<_>>();
                nom_sql::InsertStatement {
                    table: table_name.into(),
                    fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
                    data: data
                        .into_iter()
                        .map(|mut row| {
                            columns
                                .iter()
                                .map(|col| {
                                    Expr::Literal(row.remove(col).unwrap().try_into().unwrap())
                                })
                                .collect()
                        })
                        .collect(),
                    ignore: false,
                    on_duplicate: None,
                }
                .display(dialect)
                .to_string()
            };
            tokio::spawn(async move {
                match pool.get_conn().await {
                    Ok(mut conn) => {
                        let start = Instant::now();
                        match conn.query_drop(insert).await {
                            Ok(_) => {
                                let _ = sender.send(start.elapsed().as_micros());
                            }
                            Err(e) => {
                                let _ = err_tx.send(e).await;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = err_tx.send(e).await;
                    }
                }
            });
        }
    }
}
