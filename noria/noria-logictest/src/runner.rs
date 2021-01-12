use anyhow::{anyhow, bail, Context};
use colored::*;
use mysql::prelude::Queryable;
use mysql::{Row, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_mysql::backend::noria_connector::NoriaConnector;
use noria_mysql::backend::Backend;
use noria_server::Builder;

use crate::ast::{Query, Record, Statement, StatementResult, Type};
use crate::parser;

#[derive(Debug, Clone)]
pub struct TestScript {
    path: PathBuf,
    records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub zookeeper_host: String,
    pub zookeeper_port: u16,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            zookeeper_host: "127.0.0.1".to_string(),
            zookeeper_port: 2181,
        }
    }
}

impl RunOptions {
    pub fn zookeeper_addr(&self) -> String {
        format!("{}:{}", self.zookeeper_host, self.zookeeper_port)
    }
}

impl TestScript {
    pub fn read<R: io::Read>(path: PathBuf, input: R) -> anyhow::Result<Self> {
        let records = parser::read_records(input)?;
        Ok(Self { path, records })
    }

    pub fn open_file(path: PathBuf) -> anyhow::Result<Self> {
        let file = File::open(&path)?;
        Self::read(path, file)
    }

    pub fn run_file(path: PathBuf, opts: RunOptions) -> anyhow::Result<()> {
        let script = Self::open_file(path)?;
        script.run(opts)
    }

    pub fn name(&self) -> Cow<'_, str> {
        match self.path.file_name() {
            Some(n) => n.to_string_lossy(),
            None => Cow::Borrowed("unknown"),
        }
    }

    pub fn run(&self, opts: RunOptions) -> anyhow::Result<()> {
        println!(
            "==> {} {}",
            "Running test script".bold(),
            self.path.canonicalize()?.to_string_lossy().blue()
        );

        let deployment = self.make_deployment();
        let conn_opts = self.setup_mysql_adapter(&deployment, opts);
        let mut conn = mysql::Conn::new(conn_opts).with_context(|| "connecting to noria-mysql")?;

        for record in &self.records {
            match record {
                Record::Statement(stmt) => self
                    .run_statement(stmt, &mut conn)
                    .with_context(|| format!("Running statement {}", stmt.command))?,
                Record::Query(query) => self
                    .run_query(query, &mut conn)
                    .with_context(|| format!("Running query {}", query.query))?,
                Record::HashThreshold(_) => {}
                Record::Halt => break,
            }
        }

        println!(
            "{}",
            format!("==> Successfully ran {} operations", self.records.len()).bold()
        );

        Ok(())
    }

    fn run_statement(&self, stmt: &Statement, conn: &mut mysql::Conn) -> anyhow::Result<()> {
        let res = conn.query_drop(&stmt.command);
        match stmt.result {
            StatementResult::Ok => {
                if let Err(e) = res {
                    bail!("Statement failed: {}", e);
                }
            }
            StatementResult::Error => {
                if res.is_ok() {
                    bail!("Statement should have failed, but succeeded");
                }
            }
        }
        Ok(())
    }

    fn run_query(&self, query: &Query, conn: &mut mysql::Conn) -> anyhow::Result<()> {
        let results = conn.query_fold(&query.query, Ok(vec![]), |acc, mut row: Row| {
            let mut acc = acc?;
            acc.reserve(query.column_types.len());
            for (col_idx, col_type) in query.column_types.iter().enumerate() {
                let val: Value = row.take(col_idx).ok_or(anyhow!(
                    "Row had the wrong number of columns: expected {}, but got {}",
                    query.column_types.len(),
                    row.len()
                ))?;
                let typ = Type::of_mysql_value(&val);
                if val != Value::NULL && Type::of_mysql_value(&val) != Some(*col_type) {
                    bail!(
                        "Invalid column type at index {}: expected {}, but got {} (value: {:?})",
                        col_idx,
                        col_type,
                        match typ {
                            Some(typ) => format!("{}", typ),
                            None => "NULL".to_string(),
                        },
                        val,
                    )
                }
                acc.push(val);
            }
            Ok(acc)
        })??;
        match query.results {
            crate::ast::QueryResults::Hash { count, digest } => {
                if count != results.len() {
                    bail!(
                        "Wrong number of results returned: expected {}, but got {}",
                        count,
                        results.len(),
                    );
                }
            }
            crate::ast::QueryResults::Results(_) => {}
        }
        Ok(())
    }

    fn make_deployment(&self) -> Deployment {
        Deployment::new(self.name())
    }

    fn setup_mysql_adapter(&self, deployment: &Deployment, run_opts: RunOptions) -> mysql::Opts {
        let barrier = Arc::new(Barrier::new(2));
        let n = deployment.name.clone();
        let b = barrier.clone();
        let zk_addr = run_opts.zookeeper_addr();
        thread::spawn(move || {
            let authority = ZookeeperAuthority::new(&format!("{}/{}", &zk_addr, n)).unwrap();
            let builder = Builder::default();
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            let _handle = rt.block_on(builder.start(Arc::new(authority))).unwrap();
            b.wait();
            loop {
                thread::sleep(Duration::from_millis(1000));
            }
        });

        barrier.wait();

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let zk_auth = ZookeeperAuthority::new(&format!(
            "{}/{}",
            run_opts.zookeeper_addr(),
            deployment.name
        ))
        .unwrap();

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let ch = rt.block_on(ControllerHandle::new(zk_auth)).unwrap();

        thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();

            let reader = NoriaConnector::new(
                rt.handle().clone(),
                ch.clone(),
                auto_increments.clone(),
                query_cache.clone(),
            );

            let writer = NoriaConnector::new(rt.handle().clone(), ch, auto_increments, query_cache);

            let reader = rt.block_on(reader);
            let writer = rt.block_on(writer);
            let b = Backend::new(true, true, Box::new(reader), Box::new(writer), false, false);
            MysqlIntermediary::run_on_tcp(b, s).unwrap();
            drop(rt);
        });

        mysql::OptsBuilder::default().tcp_port(addr.port()).into()
    }
}

struct Deployment {
    name: String,
}

impl Deployment {
    fn new<S: AsRef<str>>(prefix: S) -> Self {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!(
            "{}.{}.{}",
            prefix.as_ref(),
            current_time.as_secs(),
            current_time.subsec_nanos()
        );

        Self { name }
    }
}

impl Drop for Deployment {
    fn drop(&mut self) {
        // Remove the ZK data if we created any:
        let zk = ZooKeeper::connect(
            "127.0.0.1:2181",
            Duration::from_secs(3),
            |_: WatchedEvent| {},
        );

        if let Ok(z) = zk {
            let _ = z.delete_recursive(&format!("/{}", self.name));
        }
    }
}
