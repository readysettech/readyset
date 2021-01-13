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
    pub deployment_name: String,
    pub zookeeper_host: String,
    pub zookeeper_port: u16,
    pub use_mysql: bool,
    pub mysql_host: String,
    pub mysql_port: u16,
    pub mysql_user: String,
    pub mysql_db: String,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            deployment_name: "sqllogictest".to_string(),
            zookeeper_host: "127.0.0.1".to_string(),
            zookeeper_port: 2181,
            use_mysql: false,
            mysql_host: "localhost".to_string(),
            mysql_port: 3306,
            mysql_user: "root".to_string(),
            mysql_db: "sqllogictest".to_string(),
        }
    }
}

impl RunOptions {
    pub fn zookeeper_addr(&self) -> String {
        format!("{}:{}", self.zookeeper_host, self.zookeeper_port)
    }

    pub fn mysql_opts_no_db(&self) -> mysql::Opts {
        mysql::OptsBuilder::new()
            .ip_or_hostname(Some(self.mysql_host.clone()))
            .tcp_port(self.mysql_port)
            .user(Some(self.mysql_user.clone()))
            .into()
    }

    pub fn mysql_opts(&self) -> mysql::Opts {
        mysql::OptsBuilder::from_opts(self.mysql_opts_no_db())
            .db_name(Some(self.mysql_db.clone()))
            .into()
    }
}

impl Drop for RunOptions {
    fn drop(&mut self) {
        if self.use_mysql {
            if let Ok(mut conn) = mysql::Conn::new(self.mysql_opts_no_db()) {
                conn.query_drop(format!("DROP DATABASE {}", self.mysql_db))
                    .unwrap_or(());
            }
        } else if let Ok(z) = ZooKeeper::connect(
            &self.zookeeper_addr(),
            Duration::from_secs(3),
            |_: WatchedEvent| {},
        ) {
            z.delete_recursive(&format!("/{}", self.deployment_name))
                .unwrap_or(());
        }
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

        let use_mysql = opts.use_mysql;
        let mut conn = if opts.use_mysql {
            let mut create_db_conn =
                mysql::Conn::new(opts.mysql_opts_no_db()).with_context(|| "connecting to mysql")?;
            create_db_conn
                .query_drop(format!("CREATE DATABASE {}", opts.mysql_db))
                .with_context(|| "creating database")?;
            mysql::Conn::new(opts.mysql_opts()).with_context(|| "connecting to mysql")?
        } else {
            let conn_opts = self.setup_mysql_adapter(&opts);
            mysql::Conn::new(conn_opts).with_context(|| "connecting to noria-mysql")?
        };

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
            format!(
                "==> Successfully ran {} operations against {}",
                self.records.len(),
                if use_mysql { "MySQL" } else { "Noria" }
            )
            .bold()
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

    fn setup_mysql_adapter(&self, run_opts: &RunOptions) -> mysql::Opts {
        let barrier = Arc::new(Barrier::new(2));
        let n = run_opts.deployment_name.clone();
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
            run_opts.deployment_name
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
