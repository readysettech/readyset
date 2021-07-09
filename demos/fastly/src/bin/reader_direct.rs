use clap::Clap;
use noria::consensus::ZookeeperAuthority;
use noria::ControllerHandle;
use noria::DataType;
use noria::KeyComparison;
use noria::ViewQuery;
use rand::distributions::{Distribution, Uniform};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use vec1::Vec1;

static THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clap)]
#[clap(name = "reader")]
struct Reader {
    /// The number of users in the system.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// ReadySet's zookeeper connection string.
    #[clap(long)]
    zookeeper_url: String,

    /// The target rate at the reader issues queries at.
    #[clap(long)]
    target_qps: Option<u64>,

    /// The amount of time to batch a set of requests for in ms. If
    /// `batch_duration_ms` is not specified, no batching is performed.
    #[clap(long, default_value = "0")]
    batch_duration_ms: u64,

    /// The maximum batch size for lookup requests.
    #[clap(long, default_value = "10")]
    batch_size: u64,

    /// The number of threads to spawn to issue reader queries.
    #[clap(long, default_value = "1")]
    threads: u64,
}

#[derive(Clone)]
struct ReaderThreadUpdate {
    // The id of the thread assinged by the parent.
    id: u32,
    // Query end-to-end latency in ms.
    queries: Vec<u128>,
}

impl Reader {
    fn generate_user_id(&self) -> usize {
        let mut rng = rand::thread_rng();
        let uniform = Uniform::from(0..self.user_table_rows);
        uniform.sample(&mut rng)
    }

    async fn generate_queries(
        &self,
        id: u32,
        query_interval: Option<Duration>,
        sender: Sender<ReaderThreadUpdate>,
        authority: Arc<ZookeeperAuthority>,
    ) -> anyhow::Result<()> {
        let mut handle: ControllerHandle<ZookeeperAuthority> =
            ControllerHandle::new(authority).await?;
        handle.ready().await.unwrap();

        let mut next_query = query_interval.map(|i| Instant::now() + i);
        let mut last_thread_update = Instant::now();
        let mut reader_update = ReaderThreadUpdate {
            id,
            queries: Vec::new(),
        };

        let mut view = handle.view("w").await.unwrap();

        let batch_interval = Duration::from_millis(self.batch_duration_ms);
        let mut batch_start = Instant::now();
        let mut batch_query_start = Vec::new();
        let mut batch_keys = Vec::new();

        loop {
            let now = Instant::now();

            // Update the aggregator process on this reader's state.
            if now - last_thread_update >= THREAD_UPDATE_INTERVAL {
                sender.send(reader_update.clone())?;
                reader_update.queries.clear();
                last_thread_update = Instant::now();
            }

            // Throttle the thread if we are trying to hit a target qps.
            if let Some(t) = next_query {
                if now < t {
                    // Sleep for 1/10th the query interval so we don't miss any
                    // intervals.
                    sleep(Duration::from_nanos(
                        (query_interval.as_ref().unwrap().as_nanos() / 10) as u64,
                    ));
                    continue;
                }
            }

            // A client is executing a query.
            next_query = query_interval.map(|i| now + i);
            let id = self.generate_user_id();

            if batch_query_start.is_empty() {
                batch_start = Instant::now();
            }

            batch_query_start.push(Instant::now());
            batch_keys.push(id);

            // Client executes a query but we have not reached the batch interval
            // so we keep adding queries.
            if batch_keys.len() < self.batch_size as usize && now < batch_start + batch_interval {
                continue;
            }

            // It is batch time, execute the batched query and calculate the time
            // for each query from the query start times.
            let keys: Vec<_> = batch_keys
                .iter()
                .map(|k| KeyComparison::Equal(Vec1::new(DataType::Int(*k as i32))))
                .collect();
            let vq = ViewQuery {
                key_comparisons: keys,
                block: true,
                order_by: None,
                limit: Some(5),
                filter: None,
                timestamp: None,
            };
            let _ = view.raw_lookup(vq).await?;
            let finish = Instant::now();

            for q_start in &batch_query_start {
                reader_update.queries.push((finish - *q_start).as_millis());
            }
            batch_query_start.clear();
            batch_keys.clear();
        }
    }

    async fn process_updates(
        &'static self,
        receiver: Receiver<ReaderThreadUpdate>,
    ) -> anyhow::Result<()> {
        // Process updates from readers, calculate statistics to report. We
        // store each channel's message in a hashmap for each interval. If
        // we receive a message from the same thread twice before we've
        // received a message from all threads, it's likely a thread has
        // failed.
        let mut updates = Vec::new();
        let mut next_check = Instant::now() + THREAD_UPDATE_INTERVAL;
        loop {
            let now = Instant::now();
            if now > next_check {
                self.process_thread_updates(&updates).unwrap();
                updates.clear();
                next_check = now + THREAD_UPDATE_INTERVAL;
            }

            let r = receiver.recv_timeout(THREAD_UPDATE_INTERVAL * 2).unwrap();
            updates.push(r);
        }
    }

    fn process_thread_updates(&'static self, updates: &[ReaderThreadUpdate]) -> anyhow::Result<()> {
        let mut query_latencies: Vec<u128> = Vec::new();
        for u in updates {
            query_latencies.append(&mut u.queries.clone());
        }

        let qps = query_latencies.len() as f64 / THREAD_UPDATE_INTERVAL.as_secs() as f64;
        let avg_latency =
            query_latencies.iter().sum::<u128>() as f64 / query_latencies.len() as f64;

        println!("qps: {}\tavg_latency: {}", qps, avg_latency);
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        let query_issue_interval = self
            .target_qps
            .map(|t| Duration::from_nanos(1000000000 / t * self.threads));
        let (tx, rx): (Sender<ReaderThreadUpdate>, Receiver<ReaderThreadUpdate>) = mpsc::channel();

        let authority = Arc::new(ZookeeperAuthority::new(&self.zookeeper_url)?);

        let mut handle: ControllerHandle<ZookeeperAuthority> =
            ControllerHandle::new(Arc::clone(&authority)).await?;
        handle.ready().await.unwrap();

        let q = "QUERY w: SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.image_url, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id = ?)) LIMIT 5;";
        handle.extend_recipe(&q).await?;

        let mut threads: Vec<_> = (0..self.threads)
            .map(|id| {
                let thread_tx = tx.clone();
                let authority = Arc::clone(&authority);
                tokio::spawn(async move {
                    self.generate_queries(id as u32, query_issue_interval, thread_tx, authority)
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
    let reader: &'static _ = Box::leak(Box::new(Reader::parse()));
    reader.run().await
}
