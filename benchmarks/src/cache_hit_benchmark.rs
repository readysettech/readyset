use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::time::Instant;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use serde::{Deserialize, Serialize};

use crate::benchmark::{BenchmarkControl, DeploymentParameters};
use crate::benchmark_histogram;
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::{ArbitraryQueryParameters, PreparedStatement};
use crate::utils::us_to_ms;

/// The number of times we will try to generate a cache miss using the random
/// generator before giving up. It is possible that we have generated cache hits
/// on all values in the table, and as a result, will no longer be able to
/// generate misses.
const MAX_RANDOM_GENERATIONS: u32 = 20;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct CacheHitBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    query: ArbitraryQueryParameters,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,

    /// The number of cache hits and cache misses to perform.
    #[clap(long, default_value = "1000")]
    num_queries_each: u32,
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Query {
    prep: String,
    params: Vec<String>,
}

// Values cannot be hashed so we turn them into sql text before putting
// them in the Query struct.
impl From<(String, Vec<Value>)> for Query {
    fn from(v: (String, Vec<Value>)) -> Query {
        Query {
            prep: v.0,
            params: v.1.into_iter().map(|s| s.as_sql(false)).collect(),
        }
    }
}

// Assumes that we don't ever perform eviction.
pub struct CachingQueryGenerator {
    prepared_statement: PreparedStatement,
    /// A set of previously generated and executed statement. We can re-execute
    /// this statement to guarentee a cache hit if we are not performing
    /// eviction.
    seen: HashSet<Query>,
}

impl From<PreparedStatement> for CachingQueryGenerator {
    fn from(prepared_statement: PreparedStatement) -> CachingQueryGenerator {
        CachingQueryGenerator {
            prepared_statement,
            seen: HashSet::new(),
        }
    }
}

impl CachingQueryGenerator {
    pub fn generate_cache_miss(&mut self) -> Result<Query> {
        let mut attempts = 0;
        while attempts < MAX_RANDOM_GENERATIONS {
            let q = Query::from(self.prepared_statement.generate_query());
            if !self.seen.contains(&q) {
                self.seen.insert(q.clone());
                return Ok(q);
            }

            attempts += 1;
        }

        return Err(anyhow!(
            "Unable to generate cache miss in {} attempts",
            MAX_RANDOM_GENERATIONS
        ));
    }

    pub fn generate_cache_hit(&self) -> Result<Query> {
        match self.seen.iter().next() {
            Some(q) => Ok(q.clone()),
            None => Err(anyhow!(
                "Unable to generate cache hit without first generating a cache miss"
            )),
        }
    }
}

#[async_trait]
impl BenchmarkControl for CacheHitBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;
        Ok(())
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<()> {
        // Prepare the query to retrieve the query schema.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // Explicitely migrate the query before benchmarking.
        self.query.migrate(&mut conn).await?;

        let mut gen = CachingQueryGenerator::from(self.query.prepared_statement(&mut conn).await?);

        // Generate the cache misses.
        self.run_queries(&mut conn, &mut gen, true).await?;
        // Generate the cache hits.
        self.run_queries(&mut conn, &mut gen, false).await?;

        Ok(())
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}

impl CacheHitBenchmark {
    async fn run_queries(
        &self,
        conn: &mut mysql_async::Conn,
        gen: &mut CachingQueryGenerator,
        cache_miss: bool,
    ) -> Result<()> {
        // Generates 1000 cache misses.
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for _ in 0..self.num_queries_each {
            let Query { prep, params } = if cache_miss {
                gen.generate_cache_miss()?
            } else {
                gen.generate_cache_hit()?
            };
            let start = Instant::now();
            let _: Vec<Row> = conn.exec(prep, params).await?;
            let elapsed = start.elapsed();
            hist.record(u64::try_from(elapsed.as_micros()).unwrap())
                .unwrap();

            let histogram_name = format!(
                "cache_hit_benchmark.{}_duration",
                if cache_miss { "miss" } else { "hit" }
            );
            benchmark_histogram!(
                &histogram_name,
                Microseconds,
                "Duration of queries executed",
                elapsed.as_micros() as f64
            );
        }
        println!(
            "Over 1000 cache {}",
            if cache_miss { "misses" } else { "hits" }
        );
        println!(
            "p50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999))
        );

        Ok(())
    }
}
