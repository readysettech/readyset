use std::time::Duration;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use chrono::Utc;
use clap::Parser;
use database_utils::DatabaseURL;
use futures::future::select_all;
use readyset_data::DfValue;

#[derive(Parser, Debug)]
struct Opts {
    /// SQL endpoint to connect to
    #[clap(env = "DATABASE_URL", parse(try_from_str))]
    database_url: DatabaseURL,

    /// Number of greenthreads to run hitting the database
    #[clap(long, env = "PARALLELISM", short = 'j', default_value = "16")]
    parallelism: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let counter: &'static RelaxedCounter = Box::leak(Box::new(RelaxedCounter::new(0)));

    let mut tasks = Vec::with_capacity(opts.parallelism + 2);
    for _i in 0..opts.parallelism {
        let mut conn = opts.database_url.connect(None).await?;
        let counter = counter;
        let handle = tokio::task::spawn(async move {
            let mut i = 600000;
            loop {
                let _ = conn
                    .execute_str::<Vec<u32>, DfValue>("SELECT * FROM t", vec![])
                    .await
                    .unwrap();
                counter.inc();
                i += 1;
                if i > 7000000 {
                    i = 600000;
                }
            }
        });
        tasks.push(handle);
    }

    tasks.push(tokio::task::spawn(async move {
        let mut old_count = 0;
        loop {
            let new_count = counter.get() as u64;
            println!("qps: {}", (new_count - old_count));
            old_count = new_count;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }));

    tasks.push(tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
    }));

    let start_time = Utc::now();
    if let Err(e) = select_all(tasks).await.0 {
        eprintln!("Failed to join tasks: {}", e);
    }
    let duration = Utc::now() - start_time;

    println!("--- total queries: {}", counter.get());
    println!("--- total time: {}", duration);
    println!(
        "--- QPS:  {}",
        counter.get() / duration.num_seconds() as usize
    );

    Ok(())
}
