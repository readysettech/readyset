use anyhow::Result;
use mysql::chrono::Utc;
use noria::consensus::{Authority, ZookeeperAuthority};
use noria::ControllerHandle;
use noria::DataType;
use noria::KeyComparison;
use noria::View;
use noria::ViewQuery;
use noria_logictest::upstream::{DatabaseConnection, DatabaseURL};
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;
use rinfluxdb::line_protocol::LineBuilder;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, fs, mem};
use structopt::clap::{arg_enum, ArgGroup};
use structopt::StructOpt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use url::Url;
use vec1::Vec1;

static THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

arg_enum! {
    #[derive(StructOpt)]
    enum Dist {
        Uniform,
        Zipf,
    }
}

#[derive(StructOpt, Clone)]
struct NoriaClientOpts {
    /// ReadySet's zookeeper connection string.
    #[structopt(long, required_if("database_type", "noria"))]
    zookeeper_url: Option<String>,

    /// The region used when requesting a view.
    #[structopt(long)]
    target_region: Option<String>,

    /// The amount of time to batch a set of requests for in ms. If
    /// `batch_duration_ms` is 0, no batching is performed. Only
    /// valid for some database types.
    #[structopt(long, default_value = "1")]
    batch_duration_ms: u64,

    /// The maximum batch size for lookup requests. Only valid
    /// for some database types.
    #[structopt(long, default_value = "10")]
    batch_size: u64,
}

#[derive(StructOpt, Clone)]
struct MySqlOpts {
    /// MySQL database connection string.
    /// Only one of MySQL database url and zooekeper url can be specified.
    #[structopt(long, required_if("database_type", "mysql"))]
    database_url: Option<DatabaseURL>,

    /// The path to the parameterized query.
    #[structopt(long, required_if("database_type", "mysql"))]
    query: Option<PathBuf>,
}

arg_enum! {
    #[derive(StructOpt)]
    enum DatabaseType {
        Noria,
        MySql,
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "reader",
    group = ArgGroup::with_name("influx")
        .requires_all(&["influx-host",
                        "influx-database",
                        "influx-user",
                        "influx-password"])
        .multiple(true))
]
struct Reader {
    /// The number of users in the system.
    #[structopt(long, default_value = "10")]
    user_table_rows: usize,

    /// The index of the first user in the system, keys are generated in the range [user_offset..user_offset+user_table_rows).
    #[structopt(long, default_value = "0")]
    user_offset: u64,

    /// The target rate at the reader issues queries at.
    #[structopt(long)]
    target_qps: Option<u64>,

    /// The number of threads to spawn to issue reader queries.
    #[structopt(long, default_value = "1")]
    threads: u64,

    /// The distribution to use to generate the random user ids
    ///
    /// 'uniform' - samples uniformly in the range [0..user_table_rows)
    ///
    /// 'zipf' - sample pattern is skewed such that 90% of accesses are for 10% of the ids (Zipf; α=1.15)
    #[structopt(
        long,
        default_value = "uniform",
        case_insensitive = true,
        rename_all = "lower"
    )]
    distribution: Dist,

    /// Override the default alpha parameter for the zipf distribution
    #[structopt(long, required_if("distribution", "zipf"), default_value = "1.15")]
    alpha: f64,

    /// The number of seconds that the experiment should be running.
    /// If `None` is provided, the experiment will run until it is interrupted.
    #[structopt(long)]
    run_for: Option<u32>,

    /// The InfluxDB host address.
    #[structopt(long, group = "influx")]
    influx_host: Option<String>,

    /// The InfluxDB database to write to.
    #[structopt(long, group = "influx")]
    influx_database: Option<String>,

    /// The username to authenticate to InfluxDB.
    #[structopt(long, group = "influx")]
    influx_user: Option<String>,

    /// The password to authenticate to InfluxDB.
    #[structopt(long, group = "influx")]
    influx_password: Option<String>,

    /// The type of database the client is connecting to. This determines
    /// the set of opts that should be populated. See `noria_opts` and
    /// `mysql_opts`.
    #[structopt(long, default_value = "noria")]
    database_type: DatabaseType,

    /// The set of options to be specified by the user to connect via a
    /// noria client.
    #[structopt(flatten)]
    noria_opts: NoriaClientOpts,

    /// The set of options to be specified by a user to connect via a
    /// mysql client.
    #[structopt(flatten)]
    mysql_opts: MySqlOpts,
}

#[derive(Debug, Clone)]
struct ReaderThreadUpdate {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
}

#[derive(Debug, Clone, Copy)]
struct BatchedQuery {
    key: u64,
    issued: Instant,
}

/// Generates random queries using a provided `Distribution`
struct QueryFactory<T, D> {
    distribution: D,
    offset: u64,
    rng: rand::rngs::SmallRng,
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
    fn new(distribution: D, offset: u64) -> Self {
        QueryFactory {
            distribution,
            offset,
            rng: rand::rngs::SmallRng::from_entropy(),
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
            key: distribution.sample(rng).try_into().unwrap() + *offset,
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
        let authority = Arc::new(Authority::from(
            ZookeeperAuthority::new(&opts.zookeeper_url.unwrap()).unwrap(),
        ));
        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        // Retrieve or create a view for a reader node in a random region, or
        // `self.target_region` if it is specified.
        let view = match &opts.target_region {
            None => handle.view("w").await.unwrap(),
            Some(r) => handle.view_from_region("w", r.clone()).await.unwrap(),
        };

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
                .map(|k| KeyComparison::Equal(Vec1::new(DataType::Int(k.key as i32))))
                .collect();

            let vq = ViewQuery {
                key_comparisons: keys,
                block: true,
                filter: None,
                timestamp: None,
            };

            let r = self.view.raw_lookup(vq).await?;
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
    query: String,
}

impl MySqlExecutor {
    async fn init(opts: MySqlOpts) -> Self {
        let fastly_query_file = opts.query.clone().unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_read_query.sql")
        });
        let query = fs::read_to_string(fastly_query_file).unwrap();
        Self {
            conn: opts.database_url.unwrap().connect().await.unwrap(),
            query,
        }
    }

    async fn on_query(&mut self, q: BatchedQuery) -> Result<Vec<BatchedQuery>> {
        let _ = self
            .conn
            .execute(self.query.clone(), vec![q.key as u32])
            .await
            .unwrap();
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
                    // Sleep for 1/2 of the remaining interval so we don't miss the desired send time
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
                    .push((finish.duration_since(query.issued)).as_millis());
            }
        }
        Ok(())
    }

    async fn process_updates(
        &'static self,
        mut receiver: UnboundedReceiver<ReaderThreadUpdate>,
        http_client: reqwest::Client,
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
                self.process_thread_updates(&mut updates, elapsed, &http_client)
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
        http_client: &reqwest::Client,
    ) -> anyhow::Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for u in updates {
            for l in u {
                hist.record(u64::try_from(*l).unwrap()).unwrap();
            }
        }
        let qps = hist.len() as f64 / period.as_secs() as f64;
        let percentiles = vec![
            ("p50", hist.value_at_quantile(0.5)),
            ("p90", hist.value_at_quantile(0.9)),
            ("p99", hist.value_at_quantile(0.99)),
            ("p9999", hist.value_at_percentile(0.9999)),
        ];

        if let Some(influx_host) = &self.influx_host {
            let timestamp = Utc::now();
            let mut measurements = vec![LineBuilder::new("read")
                .insert_field("qps", qps)
                .set_timestamp(timestamp)
                .build()
                .to_string()];
            for (name, percentile) in percentiles {
                measurements.push(
                    LineBuilder::new("read")
                        .insert_field(name, percentile)
                        .set_timestamp(timestamp)
                        .build()
                        .to_string(),
                )
            }
            let response = http_client
                .post(Url::parse(format!("{}/write", influx_host.clone()).as_str()).unwrap())
                .body(measurements.join("\n"))
                .header("Content-Type", "text/plain")
                .query(&[
                    ("db", &self.influx_database.as_ref().unwrap().clone()),
                    ("u", &self.influx_user.as_ref().unwrap().clone()),
                    ("p", &self.influx_password.as_ref().unwrap().clone()),
                ])
                .send()
                .await
                .unwrap();
            if !response.status().is_success() {
                panic!(
                    "Request to InfluxDB failed. Status: {} | Message: {}",
                    response.status().as_u16(),
                    response.text().await.unwrap()
                )
            }
        } else {
            println!(
                "qps: {:.0}\tp50: {} ms\tp90: {} ms\tp99: {} ms\tp99.99: {} ms",
                qps,
                hist.value_at_quantile(0.5),
                hist.value_at_quantile(0.9),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.9999)
            );
        }
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        let query_issue_interval = self
            .target_qps
            .map(|t| Duration::from_nanos(1000000000 / t * self.threads));
        let (tx, rx) = unbounded_channel();
        let http_client = reqwest::Client::new();

        let mut threads: Vec<_> = (0..self.threads)
            .map(|_| {
                let thread_tx = tx.clone();
                tokio::spawn(async move {
                    // Create the thread query generator.
                    let factory: Box<dyn QueryGenerator + Send> = match self.distribution {
                        Dist::Uniform => Box::new(QueryFactory::new(
                            Uniform::new(0, self.user_table_rows as u64),
                            self.user_offset,
                        )),
                        Dist::Zipf => Box::new(QueryFactory::new(
                            zipf::ZipfDistribution::new(self.user_table_rows - 1, self.alpha)
                                .unwrap(),
                            self.user_offset,
                        )),
                    };

                    // Create the thread query exector.
                    let executor: QueryExecutor = match self.database_type {
                        DatabaseType::Noria => {
                            QueryExecutor::Noria(NoriaExecutor::init(self.noria_opts.clone()).await)
                        }
                        DatabaseType::MySql => {
                            QueryExecutor::MySql(MySqlExecutor::init(self.mysql_opts.clone()).await)
                        }
                    };

                    self.generate_queries(
                        factory,
                        query_issue_interval,
                        thread_tx,
                        executor,
                        &self.run_for,
                    )
                    .await
                })
            })
            .collect();

        // The original tx channel is never used.
        drop(tx);

        threads.push(tokio::spawn(async move {
            self.process_updates(rx, http_client).await
        }));

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
    let reader: &'static mut _ = Box::leak(Box::new(Reader::from_args()));
    reader.run().await
}
