use clap::{Clap, ValueHint};
use noria_logictest::generate::DatabaseURL;
use rand::distributions::{Distribution, Uniform};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

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

impl Reader {
    fn generate_user_id(&self) -> usize {
        let mut rng = rand::thread_rng();
        let uniform = Uniform::from(0..self.user_table_rows);
        uniform.sample(&mut rng)
    }

    async fn generate_queries(
        &self,
        query: String,
        query_interval: Duration,
    ) -> anyhow::Result<()> {
        let generate_queries_start = Instant::now();

        let mut next_query = Instant::now() + query_interval;
        loop {
            // Currently each query is issued on the same thread. If a query execute exceeds
            // `1/target_qps` seconds queries will be issued slower than the target qps.
            let now = Instant::now();
            if now < next_query {
                sleep(Duration::from_millis(10));
                continue;
            }

            next_query = now + query_interval;
            let id = self.generate_user_id();
            let mut conn = self.database_url.connect().await?;
            let query_start = Instant::now();
            let _ = conn.execute(query.clone(), vec![id as u64]).await?;
            let query_elapsed = Instant::now() - query_start;

            println!(
                "{}: {}",
                (query_start - generate_queries_start).as_secs_f32(),
                query_elapsed.as_millis()
            );
        }
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
        let query_issue_interval = Duration::from_millis(1000 / self.target_qps / self.threads);
        let threads = (0..self.threads).map(|_| {
            let query = param_query.clone();
            tokio::spawn(async move { self.generate_queries(query, query_issue_interval).await })
        });
        futures::future::join_all(threads).await;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Reader is set to static as it is referenced by multiple threads.
    let reader: &'static _ = Box::leak(Box::new(Reader::parse()));
    reader.run().await
}
