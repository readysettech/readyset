//! Write client that writes (1) new articles, (2) new
//! recommendations for users.
//!
//! Assumptions: There is only a single write client
//! in the system.

use benchmarks::utils::generate::parallel_load;
use benchmarks::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use clap::{Parser, ValueHint};
use noria_data::DataType;
use noria_logictest::upstream::DatabaseURL;
use query_generator::ColumnGenerationSpec;
use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

static REPORTING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[clap(name = "writer")]
struct Writer {
    /// The number of users in the system.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// The number of rows in the articles table.
    #[clap(long, default_value = "10")]
    article_table_rows: usize,

    #[clap(long, default_value = "10")]
    author_table_rows: usize,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: DatabaseURL,

    /// Path to the fastly data model SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: Option<PathBuf>,

    /// The number of threads to spawn to issue write queries.
    #[clap(long, default_value = "1")]
    threads: u64,
}

#[derive(Clone)]
struct WriterThreadUpdate {
    /// A batch of writes' end-to-end latency in ms.
    queries: Vec<u128>,
}

impl Writer {
    async fn generate_writes(
        &self,
        articles: Arc<AtomicUsize>,
        sender: Sender<WriterThreadUpdate>,
        schema: DatabaseSchema,
    ) -> anyhow::Result<()> {
        let mut next_report = Instant::now() + REPORTING_INTERVAL;
        let mut writer_update = WriterThreadUpdate {
            queries: Vec::new(),
        };

        loop {
            writer_update.queries.push(
                self.generate_article_inserts(articles.clone(), schema.clone())
                    .await?,
            );

            writer_update.queries.push(
                self.generate_recommendation_inserts(articles.clone(), schema.clone())
                    .await?,
            );

            let now = Instant::now();
            if now > next_report {
                sender.send(writer_update.clone())?;
                writer_update.queries.clear();
                next_report = now + REPORTING_INTERVAL;
            }
        }
    }
    async fn generate_article_inserts(
        &self,
        articles: Arc<AtomicUsize>,
        schema: DatabaseSchema,
    ) -> anyhow::Result<u128> {
        let article_idx = articles.fetch_add(4, Ordering::Relaxed);
        let mut database_spec = DatabaseGenerationSpec::new(schema).table_rows("articles", 4);
        // Article table overrides.
        let table = database_spec.table_spec("articles");
        table.set_column_generator_specs(&[
            (
                "id".into(),
                ColumnGenerationSpec::UniqueFrom(article_idx as u32),
            ),
            (
                "priority".into(),
                ColumnGenerationSpec::Uniform(DataType::Int(0), DataType::Int(128)),
            ),
            (
                "author_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.author_table_rows as u32),
                ),
            ),
        ]);

        // TODO(justin): This includes generating the data for the insert.
        let before = Instant::now();
        parallel_load(self.database_url.clone(), database_spec).await?;
        Ok((Instant::now() - before).as_millis())
    }

    pub async fn generate_recommendation_inserts(
        &self,
        articles: Arc<AtomicUsize>,
        schema: DatabaseSchema,
    ) -> anyhow::Result<u128> {
        let num_articles = articles.load(Ordering::Relaxed);

        // Generate a new row for each user. This may duplicate existing recommendations
        // as we don't check the database.
        let mut database_spec =
            DatabaseGenerationSpec::new(schema).table_rows("recommendations", self.user_table_rows);
        let table = database_spec.table_spec("recommendations");
        table.set_column_generator_specs(&[
            ("user_id".into(), ColumnGenerationSpec::Unique),
            (
                "article_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(num_articles as u32),
                ),
            ),
        ]);

        let before = Instant::now();
        parallel_load(self.database_url.clone(), database_spec).await?;
        Ok((Instant::now() - before).as_millis())
    }

    async fn process_updates(
        &'static self,
        receiver: Receiver<WriterThreadUpdate>,
    ) -> anyhow::Result<()> {
        // Process updates from readers, calculate statistics to report. We
        // store each channel's message in a hashmap for each interval. If
        // we receive a message from the same thread twice before we've
        // received a message from all threads, it's likely a thread has
        // failed.
        let mut updates = Vec::new();
        let mut next_check = Instant::now() + REPORTING_INTERVAL;
        loop {
            let now = Instant::now();
            if now > next_check {
                self.process_thread_updates(&updates).unwrap();
                updates.clear();
                next_check = now + REPORTING_INTERVAL;
            }

            let r = receiver.recv_timeout(REPORTING_INTERVAL * 2).unwrap();
            updates.push(r);
        }
    }

    fn process_thread_updates(&'static self, updates: &[WriterThreadUpdate]) -> anyhow::Result<()> {
        let mut query_latencies: Vec<u128> = Vec::new();
        for u in updates {
            query_latencies.append(&mut u.queries.clone());
        }

        let qps = query_latencies.len() as f64 / REPORTING_INTERVAL.as_secs() as f64;
        let avg_latency =
            query_latencies.iter().sum::<u128>() as f64 / query_latencies.len() as f64;

        println!("qps: {}\tavg_latency: {}", qps, avg_latency);
        Ok(())
    }

    pub async fn run(&'static self) -> anyhow::Result<()> {
        let fastly_schema = DatabaseSchema::try_from(self.schema.clone().unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_db.sql")
        }))?;

        let current_articles = Arc::new(AtomicUsize::new(self.article_table_rows));
        let (tx, rx): (Sender<WriterThreadUpdate>, Receiver<WriterThreadUpdate>) = mpsc::channel();

        // Spawn a thread for articles
        let mut threads: Vec<_> = (0..self.threads)
            .map(|_| {
                let schema = fastly_schema.clone();
                let articles = current_articles.clone();
                let thread_tx = tx.clone();
                tokio::spawn(async move { self.generate_writes(articles, thread_tx, schema).await })
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
    let writer: &'static mut _ = Box::leak(Box::new(Writer::parse()));
    writer.run().await
}
