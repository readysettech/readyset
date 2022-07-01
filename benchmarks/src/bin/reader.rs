use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{env, fs, mem};

use anyhow::Result;
use clap::{ArgEnum, Parser};
use database_utils::{DatabaseConnection, DatabaseURL};
use noria::consensus::AuthorityType;
use noria::{ControllerHandle, KeyComparison, View, ViewQuery};
use noria_data::DataType;
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use vec1::Vec1;

static THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, ArgEnum)]
enum Dist {
    Uniform,
    Zipf,
}

#[derive(Parser, Clone)]
struct NoriaClientOpts {
    #[clap(
        short,
        long,
        required_if_eq("database-type", "noria"),
        env("AUTHORITY_ADDRESS"),
        default_value("127.0.0.1:2181")
    )]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), required_if_eq("database-type", "noria"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(
        short,
        required_if_eq("database-type", "noria"),
        long,
        env("NORIA_DEPLOYMENT")
    )]
    deployment: Option<String>,

    /// The amount of time to batch a set of requests for in ms. If
    /// `batch_duration_ms` is 0, no batching is performed. Only
    /// valid for some database types.
    #[clap(long, default_value = "1")]
    batch_duration_ms: u64,

    /// The maximum batch size for lookup requests. Only valid
    /// for some database types.
    #[clap(long, default_value = "10")]
    batch_size: u64,
}

#[derive(Parser, Clone)]
struct MySqlOpts {
    /// MySQL database connection string.
    /// Only one of MySQL database url and zooekeper url can be specified.
    #[clap(long, required_if_eq("database-type", "mysql"))]
    database_url: Option<DatabaseURL>,

    /// The path to the parameterized query.
    #[clap(long, required_if_eq("database-type", "mysql"))]
    query: Option<PathBuf>,

    /// Number of parameters to generate for a parameterized query.
    #[clap(long, required_if_eq("database-type", "mysql"))]
    nparams: Option<usize>,
}

#[derive(Clone, ArgEnum)]
enum DatabaseType {
    Noria,
    MySql,
}

#[derive(Parser)]
#[clap(name = "reader")]
struct Reader {
    /// The number of users in the system.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// The index of the first user in the system, keys are generated in the range
    /// [user_offset..user_offset+user_table_rows).
    #[clap(long, default_value = "0")]
    user_offset: u64,

    /// The target rate at the reader issues queries at.
    #[clap(long)]
    target_qps: Option<u64>,

    /// The number of threads to spawn to issue reader queries.
    #[clap(long, default_value = "1")]
    threads: u64,

    /// The distribution to use to generate the random user ids
    ///
    /// 'uniform' - samples uniformly in the range [0..user_table_rows)
    ///
    /// 'zipf' - sample pattern is skewed such that 90% of accesses are for 10% of the ids (Zipf;
    /// α=1.15)
    #[clap(arg_enum, default_value = "uniform")]
    distribution: Dist,

    /// Override the default alpha parameter for the zipf distribution
    #[clap(long, required_if_eq("distribution", "zipf"), default_value = "1.15")]
    alpha: f64,

    /// The number of seconds that the experiment should be running.
    /// If `None` is provided, the experiment will run until it is interrupted.
    #[clap(long)]
    run_for: Option<u32>,

    /// The type of database the client is connecting to. This determines
    /// the set of opts that should be populated. See `noria_opts` and
    /// `mysql_opts`.
    #[clap(arg_enum, default_value = "noria")]
    database_type: DatabaseType,

    /// The set of options to be specified by the user to connect via a
    /// noria client.
    #[clap(flatten)]
    noria_opts: NoriaClientOpts,

    /// The set of options to be specified by a user to connect via a
    /// mysql client.
    #[clap(flatten)]
    mysql_opts: MySqlOpts,
}

#[derive(Debug, Clone)]
struct ReaderThreadUpdate {
    /// Query end-to-end latency in 1/10th of a ms.
    queries: Vec<u128>,
}

#[derive(Debug, Clone)]
struct BatchedQuery {
    key: Vec<u32>,
    issued: Instant,
}

/// Generates random queries using a provided `Distribution`
struct QueryFactory<T, D> {
    distribution: D,
    offset: u64,
    rng: rand::rngs::SmallRng,
    nparams: usize,
    _t: std::marker::PhantomData<T>,
}

trait QueryGenerator {
    fn query(&mut self) -> BatchedQuery;
}

impl<T, D> QueryFactory<T, D>
where
    T: TryInto<u64>,
    <T as TryInto<u64>>::Error: std::fmt::Debug,
    D: Distribution<T>,
{
    fn new(distribution: D, offset: u64, nparams: usize) -> Self {
        QueryFactory {
            distribution,
            offset,
            rng: rand::rngs::SmallRng::from_entropy(),
            nparams,
            _t: Default::default(),
        }
    }

    fn new_query(&mut self) -> BatchedQuery {
        let QueryFactory {
            distribution,
            offset,
            rng,
            ..
        } = self;

        BatchedQuery {
            key: (0..self.nparams)
                .map(|_| (distribution.sample(rng).try_into().unwrap() + *offset) as u32)
                .collect(),
            issued: Instant::now(),
        }
    }
}

impl<T, D> QueryGenerator for QueryFactory<T, D>
where
    T: TryInto<u64>,
    <T as TryInto<u64>>::Error: std::fmt::Debug,
    D: Distribution<T>,
{
    fn query(&mut self) -> BatchedQuery {
        self.new_query()
    }
}

/// Manages execution of queries to a specific backend for a single
/// thread of execution. Re-uses the same back-end connection for
/// subsequent queries.
enum QueryExecutor {
    Noria(NoriaExecutor),
    MySql(MySqlExecutor),
}

impl QueryExecutor {
    /// Add a query for execution. Execution of the query may be delayed
    /// by the query executor. The function returns any queries that it
    /// executed.
    async fn on_query(&mut self, q: BatchedQuery) -> Result<Vec<BatchedQuery>> {
        match &mut *self {
            QueryExecutor::Noria(e) => e.on_query(q).await,
            QueryExecutor::MySql(e) => e.on_query(q).await,
        }
    }
}

struct QueryBatcher {
    batch_duration: Duration,
    batch_size: u64,
    batch_start: Instant,
    current_batch: Vec<BatchedQuery>,
}

impl QueryBatcher {
    fn new(batch_duration: Duration, batch_size: u64) -> Self {
        Self {
            batch_duration,
            batch_size,
            batch_start: Instant::now(),
            current_batch: Vec::new(),
        }
    }

    fn add_query(&mut self, query: BatchedQuery) {
        if self.current_batch.is_empty() {
            self.batch_start = Instant::now();
        }

        self.current_batch.push(query)
    }

    /// Consume the current batch if it is ready and return it.
    fn get_batch_if_ready(&mut self) -> Option<Vec<BatchedQuery>> {
        let now = Instant::now();
        if self.current_batch.len() < self.batch_size as usize
            && now < self.batch_start + self.batch_duration
        {
            return None;
        }

        let mut ret = Vec::new();
        mem::swap(&mut ret, &mut self.current_batch);
        Some(ret)
    }
}

/// Executes queries directly to Noria through the `View` API.
struct NoriaExecutor {
    view: View,
    query_batcher: QueryBatcher,
}

impl NoriaExecutor {
    async fn init(opts: NoriaClientOpts) -> Self {
        let authority = opts
            .authority
            .to_authority(&opts.authority_address, &opts.deployment.unwrap())
            .await;
        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        let view = handle.view("w").await.unwrap();
        Self {
            view,
            query_batcher: QueryBatcher::new(
                Duration::from_millis(opts.batch_duration_ms),
                opts.batch_size,
            ),
        }
    }

    async fn on_query(&mut self, q: BatchedQuery) -> Result<Vec<BatchedQuery>> {
        // Queries may be batched when executed in Noria. This may delay
        // sending queries to the database in order to package sets of queries
        // together. This is done based on time: package all queries every
        // `batch_interval`, or it is done based on number of queries: only
        // batch queries into batches up to size `self.batch_size`.
        self.query_batcher.add_query(q);
        if let Some(batch) = self.query_batcher.get_batch_if_ready() {
            // It is batch time, execute the batched query and calculate the time
            // for each query from the query start times.
            let keys: Vec<_> = batch
                .iter()
                .map(|k| KeyComparison::Equal(Vec1::new(DataType::Int(k.key[0] as _))))
                .collect();

            let vq = ViewQuery {
                key_comparisons: keys,
                block: true,
                filter: None,
                timestamp: None,
            };

            let r = self.view.raw_lookup(vq).await?.into_results().unwrap();
            assert_eq!(r.len(), batch.len());
            assert!(r.iter().all(|rset| !rset.is_empty()));

            return Ok(batch);
        }

        // The executor did not execute any queries when this query was added.
        Ok(Vec::new())
    }
}

/// Executes queries directly to Noria through the `View` API.
struct MySqlExecutor {
    conn: DatabaseConnection,
    query: &'static str,
}

impl MySqlExecutor {
    async fn init(opts: MySqlOpts) -> Self {
        let fastly_query_file = opts.query.clone().unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_read_query.sql")
        });
        let query = Box::leak(
            fs::read_to_string(fastly_query_file)
                .unwrap()
                .into_boxed_str(),
        );
        Self {
            conn: opts.database_url.unwrap().connect().await.unwrap(),
            query,
        }
    }

    async fn on_query(&mut self, q: BatchedQuery) -> Result<Vec<BatchedQuery>> {
        let _: Vec<Vec<DataType>> = self.conn.execute(self.query, &q.key).await.unwrap();
        Ok(vec![q])
    }
}

impl Reader {
    async fn generate_queries(
        &'static self,
        mut query_factory: Box<dyn QueryGenerator + Send>,
        query_interval: Option<Duration>,
        sender: UnboundedSender<ReaderThreadUpdate>,
        mut executor: QueryExecutor,
        run_for: &Option<u32>,
    ) -> anyhow::Result<()> {
        let mut last_thread_update = Instant::now();
        let mut reader_update = ReaderThreadUpdate {
            queries: Vec::new(),
        };

        // We may throttle the rate at which queries are sent if `query_interval`
        // is specified. `query_interval` determines the amount of time we should
        // wait between subsequent queries.
        let mut qps_sent: u32 = 0;
        let mut qps_start = Instant::now();

        // Enable support for exiting query generation after a pre-determined amount
        // of time.
        let run_until = run_for.map(|seconds| Instant::now() + Duration::from_secs(seconds as u64));

        let should_continue = || {
            if let Some(until) = run_until {
                Instant::now() < until
            } else {
                true
            }
        };

        loop {
            // Every `THREAD_UPDATE_INTERVAL` we send query metrics to a single
            // source for aggregation and statistics calculations.
            if last_thread_update.elapsed() >= THREAD_UPDATE_INTERVAL {
                sender.send(reader_update.clone())?;
                reader_update.queries.clear();
                last_thread_update = Instant::now();
                if !should_continue() {
                    // We drop the senders so that the receiver
                    // eventually stops.
                    drop(sender);
                    break;
                }
            }

            let now = Instant::now();
            let next_query = query_interval.map(|i| qps_start + i * qps_sent);

            // Throttle the thread if we are trying to hit a target qps.
            if let Some(t) = next_query {
                let remaining_interval = t.checked_duration_since(now).unwrap_or_default();
                // TODO(justin): Handle throttling when there is batching.
                if remaining_interval > *query_interval.as_ref().unwrap() / 10 {
                    // Sleep for 1/2 of the remaining interval so we don't miss the desired send
                    // time
                    tokio::time::sleep(remaining_interval / 2).await;
                    continue;
                }
            }

            let finished = executor.on_query(query_factory.query()).await?;
            qps_sent += 1;

            if qps_sent == 10_000_000 {
                // Restart the counter to prevent error aggregation
                qps_sent = 0;
                qps_start = now;
            }

            let finish = Instant::now();
            for query in finished {
                reader_update
                    .queries
                    .push((finish.duration_since(query.issued)).as_micros() / 100);
            }
        }
        Ok(())
    }

    async fn process_updates(
        &'static self,
        mut receiver: UnboundedReceiver<ReaderThreadUpdate>,
    ) -> anyhow::Result<()> {
        // Process updates from readers, calculate statistics to report. We
        // store each channel's message in a hashmap for each interval. If
        // we receive a message from the same thread twice before we've
        // received a message from all threads, it's likely a thread has
        // failed.
        let mut updates = Vec::new();
        let mut last_check = Instant::now();
        loop {
            let elapsed = last_check.elapsed();
            if elapsed >= THREAD_UPDATE_INTERVAL {
                last_check = Instant::now();
                self.process_thread_updates(&mut updates, elapsed)
                    .await
                    .unwrap();
                updates.clear();
            }

            match receiver.recv().await {
                Some(r) => updates.push(r.queries),
                None => break,
            }
        }
        Ok(())
    }

    async fn process_thread_updates(
        &self,
        updates: &mut [Vec<u128>],
        period: Duration,
    ) -> anyhow::Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for u in updates {
            for l in u {
                hist.record(u64::try_from(*l).unwrap()).unwrap();
            }
        }
        let qps = hist.len() as f64 / period.as_secs() as f64;
        println!(
            "qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            hist.value_at_quantile(0.5) as f64 / 10.0,
            hist.value_at_quantile(0.9) as f64 / 10.0,
            hist.value_at_quantile(0.99) as f64 / 10.0,
            hist.value_at_quantile(0.9999) as f64 / 10.0
        );
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        let query_issue_interval = self
            .target_qps
            .map(|t| Duration::from_nanos(1000000000 / t * self.threads));
        let (tx, rx) = unbounded_channel();

        let mut threads: Vec<_> = Vec::with_capacity(self.threads as usize);
        for _ in 0..self.threads {
            let thread_tx = tx.clone();
            // Create the thread query exector.
            let executor: QueryExecutor = match self.database_type {
                DatabaseType::Noria => {
                    QueryExecutor::Noria(NoriaExecutor::init(self.noria_opts.clone()).await)
                }
                DatabaseType::MySql => {
                    QueryExecutor::MySql(MySqlExecutor::init(self.mysql_opts.clone()).await)
                }
            };

            threads.push(tokio::spawn(async move {
                // Create the thread query generator.
                let factory: Box<dyn QueryGenerator + Send> = match self.distribution {
                    Dist::Uniform => Box::new(QueryFactory::new(
                        Uniform::new(0, self.user_table_rows as u64),
                        self.user_offset,
                        self.mysql_opts.nparams.unwrap_or(1),
                    )),
                    Dist::Zipf => Box::new(QueryFactory::new(
                        zipf::ZipfDistribution::new(self.user_table_rows - 1, self.alpha).unwrap(),
                        self.user_offset,
                        self.mysql_opts.nparams.unwrap_or(1),
                    )),
                };

                self.generate_queries(
                    factory,
                    query_issue_interval,
                    thread_tx,
                    executor,
                    &self.run_for,
                )
                .await
            }));
        }

        // The original tx channel is never used.
        drop(tx);

        threads.push(tokio::spawn(async move { self.process_updates(rx).await }));

        let res = futures::future::join_all(threads).await;
        for err_res in res.iter().filter(|e| e.is_err()) {
            if let Err(e) = err_res {
                eprintln!("Error executing query {}", e);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let reader: &'static mut _ = Box::leak(Box::new(Reader::parse()));
    reader.run().await
}
