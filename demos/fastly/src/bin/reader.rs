use anyhow::bail;
use clap::{Clap, ValueHint};
use noria_logictest::generate::DatabaseURL;
use rand::distributions::{Distribution, Uniform};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::sleep;
use std::time::{Duration, Instant};

static THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clap)]
#[clap(name = "reader")]
struct Reader {
    /// The number of users in the system.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// The path to the parameterized query.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    query: Option<PathBuf>,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: DatabaseURL,

    /// The target rate at the reader issues queries at.
    #[clap(long, default_value = "10")]
    target_qps: u64,

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
        query: String,
        query_interval: Duration,
        sender: Sender<ReaderThreadUpdate>,
    ) -> anyhow::Result<()> {
        let mut next_query = Instant::now() + query_interval;
        let mut last_thread_update = Instant::now();
        let mut reader_update = ReaderThreadUpdate {
            id,
            queries: Vec::new(),
        };
        loop {
            let now = Instant::now();

            // Update the aggregator process on this reader's state.
            if now - last_thread_update >= THREAD_UPDATE_INTERVAL {
                sender.send(reader_update.clone())?;
                reader_update.queries.clear();
                last_thread_update = Instant::now();
            }

            if now < next_query {
                // Sleep for 1/10th the query interval so we don't miss any
                // intervals.
                sleep(Duration::from_nanos(
                    (query_interval.as_nanos() / 10) as u64,
                ));
                continue;
            }

            next_query = now + query_interval;
            let mut conn = self.database_url.connect().await.unwrap();

            // Execute and time the query.
            let id = self.generate_user_id();
            let query_start = Instant::now();
            let _ = conn.execute(query.clone(), vec![id as u64]).await.unwrap();
            let query_elapsed = Instant::now() - query_start;
            reader_update.queries.push(query_elapsed.as_millis());
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
        loop {
            let r = receiver.recv_timeout(THREAD_UPDATE_INTERVAL * 2).unwrap();
            updates.push(r);

            if updates.len() == self.threads as usize {
                self.process_thread_updates(&updates).unwrap();
                updates.clear();
            }
        }
    }

    fn process_thread_updates(&'static self, updates: &[ReaderThreadUpdate]) -> anyhow::Result<()> {
        let mut updates_seen = HashSet::new();
        let mut query_latencies: Vec<u128> = Vec::new();
        for u in updates {
            if !updates_seen.insert(u.id) {
                bail!("Multiple updates from the same thread");
            }

            query_latencies.append(&mut u.queries.clone());
        }

        let qps = query_latencies.len() as f64 / THREAD_UPDATE_INTERVAL.as_secs() as f64;
        let avg_latency =
            query_latencies.iter().sum::<u128>() as f64 / query_latencies.len() as f64;

        println!("qps: {}\tavg_latency: {}", qps, avg_latency);
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        println!(
            "Starting reader with threads {}, target_qps {}",
            self.threads, self.target_qps
        );

        let fastly_query_file = self.query.clone().unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_read_query.sql")
        });
        let param_query = fs::read_to_string(fastly_query_file)?;
        let query_issue_interval =
            Duration::from_nanos(1000000000 / self.target_qps * self.threads);
        let (tx, rx): (Sender<ReaderThreadUpdate>, Receiver<ReaderThreadUpdate>) = mpsc::channel();

        let mut threads: Vec<_> = (0..self.threads + 1)
            .map(|id| {
                let query = param_query.clone();
                let thread_tx = tx.clone();
                tokio::spawn(async move {
                    self.generate_queries(id as u32, query, query_issue_interval, thread_tx)
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
