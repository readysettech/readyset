use std::time::Duration;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use chrono::Utc;
use clap::Clap;
use futures::future::select_all;
use mysql_async::prelude::*;

#[derive(Clap, Debug)]
struct Opts {
    /// MySQL endpoint to connect to; you probably want a noria-mysql adapter
    #[clap(long, env = "DATABASE_URL")]
    database_url: String,

    /// Number of greenthreads to run hitting the database
    #[clap(long, env = "PARALLELISM", short = 'j', default_value = "16")]
    parallelism: usize,
}

#[tokio::main]
async fn main() {
    let opts = Opts::parse();
    let pool = mysql_async::Pool::new(opts.database_url.as_ref());

    let counter: &'static RelaxedCounter = Box::leak(Box::new(RelaxedCounter::new(0)));

    let mut tasks = Vec::with_capacity(opts.parallelism + 2);
    for _i in 0..opts.parallelism {
        let mut conn = pool.get_conn().await.unwrap();
        let counter = counter;
        let handle = tokio::task::spawn(async move {
            let mut i = 600000;
            loop {
                let _invites: Result<Vec<mysql_async::Row>, _> = "SELECT * FROM invites_users WHERE invites_users.invite_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AND invites_users.user_id = ?"
                    .with((i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8, i + 9, i + 10))
                    .fetch(&mut conn)
                    .await;
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
}
