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
}

impl Reader {
    fn generate_user_id(&self) -> usize {
        let mut rng = rand::thread_rng();
        let uniform = Uniform::from(0..self.user_table_rows);
        uniform.sample(&mut rng)
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let fastly_query_file = self.query.clone().unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_read_query.sql")
        });
        let param_query = fs::read_to_string(fastly_query_file)?;
        let query_issue_interval = Duration::from_millis(1000 / self.target_qps);

        // Currently each query is issued on the same thread. If a query execute exceeds
        // `1/target_qps` seconds queries will be issued slower than the target qps.
        let mut next_query = Instant::now() + query_issue_interval;
        loop {
            let now = Instant::now();
            if now < next_query {
                sleep(Duration::from_millis(10));
                continue;
            }

            next_query = now + query_issue_interval;
            let id = self.generate_user_id();
            let mut conn = self.database_url.connect().await?;
            let _ = conn.execute(param_query.clone(), vec![id as u64]).await?;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let reader = Reader::parse();
    reader.run().await
}
