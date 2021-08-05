#![feature(drain_filter)]
use noria::consensus::ZookeeperAuthority;
use noria::ControllerHandle;
use noria::DataType;
use noria::KeyComparison;
use noria::ViewQuery;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::clap::arg_enum;
use structopt::StructOpt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use vec1::Vec1;

static THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

arg_enum! {
    #[derive(StructOpt)]
    enum Dist {
        Uniform,
        Zipf,
    }
}

#[derive(StructOpt)]
#[structopt(name = "reader")]
struct Reader {
    /// The number of users in the system.
    #[structopt(long, default_value = "10")]
    user_table_rows: usize,

    /// ReadySet's zookeeper connection string.
    #[structopt(long)]
    zookeeper_url: String,

    /// The target rate at the reader issues queries at.
    #[structopt(long)]
    target_qps: Option<u64>,

    /// The region used when requesting a view.
    #[structopt(long)]
    target_region: Option<String>,

    /// The amount of time to batch a set of requests for in ms. If
    /// `batch_duration_ms` is 0, no batching is performed.
    #[structopt(long, default_value = "1")]
    batch_duration_ms: u64,

    /// The maximum batch size for lookup requests.
    #[structopt(long, default_value = "10")]
    batch_size: u64,

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

    #[structopt(long, required_if("distribution", "zipf"), default_value = "1.15")]
    /// Override the default alpha parameter for the zipf distribution
    alpha: f64,
}

#[derive(Debug, Clone)]
struct ReaderThreadUpdate {
    // Query end-to-end latency in ms.
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
    fn new(distribution: D) -> Self {
        QueryFactory {
            distribution,
            rng: rand::rngs::SmallRng::from_entropy(),
            _t: Default::default(),
        }
    }

    fn new_query(&mut self) -> BatchedQuery {
        let QueryFactory {
            distribution, rng, ..
        } = self;

        BatchedQuery {
            key: distribution.sample(rng).try_into().unwrap(),
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

impl Reader {
    async fn generate_queries(
        &self,
        mut query_factory: Box<dyn QueryGenerator + Send>,
        query_interval: Option<Duration>,
        sender: UnboundedSender<ReaderThreadUpdate>,
        authority: Arc<ZookeeperAuthority>,
    ) -> anyhow::Result<()> {
        let mut handle: ControllerHandle<ZookeeperAuthority> =
            ControllerHandle::new(authority).await?;
        handle.ready().await.unwrap();

        let mut last_thread_update = Instant::now();
        let mut reader_update = ReaderThreadUpdate {
            queries: Vec::new(),
        };

        let mut view = match &self.target_region {
            None => handle.view("w").await.unwrap(),
            Some(r) => handle.view_from_region("w", r.clone()).await.unwrap(),
        };

        let batch_interval = Duration::from_millis(self.batch_duration_ms);
        let mut batch_start = Instant::now();
        let mut queries = Vec::new();

        let mut qps_sent: u32 = 0;
        let mut qps_start = Instant::now();

        loop {
            // Update the aggregator process on this reader's state.
            if last_thread_update.elapsed() >= THREAD_UPDATE_INTERVAL {
                sender.send(reader_update.clone())?;
                reader_update.queries.clear();
                last_thread_update = Instant::now();
            }

            let now = Instant::now();
            let next_query = query_interval.map(|i| qps_start + i * qps_sent);

            // Throttle the thread if we are trying to hit a target qps.
            if let Some(t) = next_query {
                let remaining_interval = t.checked_duration_since(now).unwrap_or_default();
                if remaining_interval > *query_interval.as_ref().unwrap() / 10
                    && now > batch_start + batch_interval
                {
                    // Sleep for 1/2 of the remaining interval so we don't miss the desired send time
                    tokio::time::sleep(remaining_interval / 2).await;
                    continue;
                }
            }

            // A client is executing a query.
            if queries.is_empty() {
                batch_start = now;
            }

            queries.push(query_factory.query());
            qps_sent += 1;

            if qps_sent == 10_000_000 {
                // Restart the counter to prevent error aggregation
                qps_sent = 0;
                qps_start = now;
            }

            // Client executes a query but we have not reached the batch interval
            // so we keep adding queries.
            if queries.len() < self.batch_size as usize && now < batch_start + batch_interval {
                continue;
            }

            // It is batch time, execute the batched query and calculate the time
            // for each query from the query start times.
            let keys: Vec<_> = queries
                .iter()
                .map(|k| KeyComparison::Equal(Vec1::new(DataType::Int(k.key as i32))))
                .collect();

            let vq = ViewQuery {
                key_comparisons: keys,
                block: true,
                filter: None,
                timestamp: None,
            };

            let r = view.raw_lookup(vq).await?;
            assert_eq!(r.len(), queries.len());

            let finish = Instant::now();

            let mut i: isize = -1;

            // Filter and record comleted queries only, queries that didn't complete
            // will remain and we need to send them to our blocking resolver
            for query in queries.drain_filter(|_| {
                i += 1;
                !r[i as usize].is_empty()
            }) {
                reader_update
                    .queries
                    .push((finish.duration_since(query.issued)).as_millis());
            }

            if !queries.is_empty() {
                panic!("Expected actual results");
            }

            queries.clear();
        }
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
                self.process_thread_updates(&mut updates, elapsed).unwrap();
                updates.clear();
            }

            let r = receiver.recv().await.unwrap();
            updates.push(r.queries);
        }
    }

    fn process_thread_updates(
        &'static self,
        updates: &mut [Vec<u128>],
        period: Duration,
    ) -> anyhow::Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for u in updates {
            for l in u {
                hist.record(u64::try_from(*l).unwrap()).unwrap();
            }
        }

        println!(
            "qps: {:.0}\tp50: {} ms\tp90: {} ms\tp99: {} ms\tp99.99: {} ms",
            hist.len() as f64 / period.as_secs() as f64,
            hist.value_at_quantile(0.5),
            hist.value_at_quantile(0.9),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.9999)
        );
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        let query_issue_interval = self
            .target_qps
            .map(|t| Duration::from_nanos(1000000000 / t * self.threads));
        let (tx, rx) = unbounded_channel();
        let authority = Arc::new(ZookeeperAuthority::new(&self.zookeeper_url)?);

        let mut handle: ControllerHandle<ZookeeperAuthority> =
            ControllerHandle::new(Arc::clone(&authority)).await?;
        handle.ready().await.unwrap();

        let mut threads: Vec<_> = (0..self.threads)
            .map(|_| {
                let thread_tx = tx.clone();
                let authority = Arc::clone(&authority);

                tokio::spawn(async move {
                    let factory: Box<dyn QueryGenerator + Send> = match self.distribution {
                        Dist::Uniform => Box::new(QueryFactory::new(Uniform::new(
                            0,
                            self.user_table_rows as u64,
                        ))),
                        Dist::Zipf => Box::new(QueryFactory::new(
                            zipf::ZipfDistribution::new(self.user_table_rows, self.alpha).unwrap(),
                        )),
                    };

                    self.generate_queries(factory, query_issue_interval, thread_tx, authority)
                        .await
                })
            })
            .collect();

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
    // Reader is set to static as it is referenced by multiple threads.
    let reader: &'static _ = Box::leak(Box::new(Reader::from_args()));
    reader.run().await
}
