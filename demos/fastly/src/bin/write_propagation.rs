//! Write propagation experiment client.
//!
//! This client writes new articles to the system and
//! measures the propagation time for the article
//! to reach the viewer. This is done by:
//!   1. Issuing a write for a article with a specific
//!      article_id.
//!   2. Attempting to read from a view with that article_id
//!      as the key.
//!
//!   We measure the duration from the time the write is
//!   issued, to the time that the view returns the result as
//!   the write propagation time.

use clap::{Clap, ValueHint};
use demo_utils::generate::load_to_backend;
use demo_utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use nom_sql::SelectStatement;
use noria::consistency::Timestamp;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria::{DataType, KeyComparison, View, ViewQuery};
use noria_client::backend::{self, Backend};
use noria_client::backend::{
    mysql_connector::MySqlConnector, noria_connector::NoriaConnector, BackendBuilder,
};
use query_generator::ColumnGenerationSpec;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, Instant};
use vec1::Vec1;

static REPORTING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clap)]
#[clap(name = "writer")]
struct Writer {
    /// The number of rows already in the articles table. This
    /// is used as the starting article_id.
    #[clap(long, default_value = "10")]
    article_table_rows: usize,

    /// The number of rows in the author table.
    #[clap(long, default_value = "10")]
    author_table_rows: usize,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: String,

    /// ReadySet's zookeeper connection string.
    #[clap(long)]
    zookeeper_url: String,

    /// Path to the fastly data model SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: Option<PathBuf>,

    /// The target rate that writes are issued with.
    #[clap(long)]
    target_qps: Option<u64>,

    /// Number of threads to issue write queries.
    #[clap(long, default_value = "1")]
    threads: u64,
}

#[derive(Clone)]
struct WriterThreadUpdate {
    /// A batch of writes' end-to-end latency in ms.
    queries: Vec<u128>,
    /// A batch of writes' write latency to the customer db.
    db_queries: Vec<u128>,
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
            db_queries: Vec::new(),
        };

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
        let zk_auth = ZookeeperAuthority::new(&self.zookeeper_url).unwrap();
        let mut ch = ControllerHandle::new(zk_auth).await.unwrap();

        let writer = MySqlConnector::new(self.database_url.clone()).await;
        let writer: backend::Writer<ZookeeperAuthority> = backend::Writer::MySqlConnector(writer);
        let mysql_connector = Some(MySqlConnector::new(self.database_url.clone()).await);
        let noria_connector = NoriaConnector::new(
            ch.clone(),
            auto_increments.clone(),
            query_cache.clone(),
            None,
        )
        .await;

        let mut b = BackendBuilder::new()
            .reader(backend::Reader {
                mysql_connector,
                noria_connector,
            })
            .writer(writer)
            .require_authentication(false)
            .enable_ryw(true)
            .build();

        ch.extend_recipe("QUERY w : SELECT * FROM articles WHERE id = ?;")
            .await?;

        let mut view = ch.view("w").await.unwrap();

        let mut interval = self
            .target_qps
            .map(|t| tokio::time::interval(Duration::from_nanos(1000000000 / t * self.threads)));

        loop {
            if let Some(t) = &mut interval {
                t.tick().await;
            }

            let next_article = articles.fetch_add(1, Ordering::Relaxed);
            let start = Instant::now();
            self.generate_and_insert_article(next_article, schema.clone(), &mut b)
                .await
                .unwrap();
            let db_write = Instant::now();

            let ticket = b.ticket().clone().unwrap();
            self.read_article(next_article, ticket, &mut view)
                .await
                .unwrap();

            writer_update
                .queries
                .push((Instant::now() - start).as_millis());
            writer_update
                .db_queries
                .push((db_write - start).as_millis());

            if start > next_report {
                sender.send(writer_update.clone())?;
                writer_update.queries.clear();
                next_report = start + REPORTING_INTERVAL;
            }
        }
    }
    async fn generate_and_insert_article(
        &self,
        article: usize,
        schema: DatabaseSchema,
        mut backend: &mut Backend<ZookeeperAuthority>,
    ) -> anyhow::Result<()> {
        let mut database_spec = DatabaseGenerationSpec::new(schema).table_rows("articles", 1);
        // Article table overrides.
        let table = database_spec.table_spec("articles");
        table.set_column_generator_specs(&[
            (
                "id".into(),
                ColumnGenerationSpec::UniqueFrom(article as u32),
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

        load_to_backend(&mut backend, database_spec).await?;
        Ok(())
    }

    async fn read_article(
        &self,
        article: usize,
        ticket: Timestamp,
        view: &mut View,
    ) -> anyhow::Result<()> {
        let vq = ViewQuery {
            key_comparisons: vec![KeyComparison::Equal(Vec1::new(DataType::Int(
                article as i32,
            )))],
            block: true,
            filter: None,
            timestamp: Some(ticket),
        };

        let _ = view.raw_lookup(vq).await?;
        Ok(())
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
        let mut db_latencies: Vec<u128> = Vec::new();
        for u in updates {
            query_latencies.append(&mut u.queries.clone());
            db_latencies.append(&mut u.db_queries.clone());
        }

        let qps = query_latencies.len() as f64 / REPORTING_INTERVAL.as_secs() as f64;
        let avg_latency =
            query_latencies.iter().sum::<u128>() as f64 / query_latencies.len() as f64;
        let avg_db_latency = db_latencies.iter().sum::<u128>() as f64 / db_latencies.len() as f64;

        println!(
            "qps: {}\te2e_latency: {}\tdb_latency: {}",
            qps, avg_latency, avg_db_latency
        );
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
