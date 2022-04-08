//! This module contains a [`criterion`] benchmark which compares mutliple different methods of
//! proxying queries to postgresql:
//!
//! * No proxy, as a control
//! * Proxying via a [direct tcp proxy](`direct_proxy`)
//! * Proxying via [`psql_srv`] handling queries, then issuing those same queries to an upstream
//!   connection
//!
//! To run these benchmarks:
//!
//! ```notrust
//! $ cargo criterion -p noria-psql --bench proxy
//! ```

use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::{try_select, Either};
use noria_psql::Value;
use psql_srv::QueryResponse;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

const BUFFER_SIZE: usize = 16 * 1024 * 1024;

const NUM_ROWS: usize = 10_000;

async fn proxy_pipe(mut upstream: TcpStream, mut downstream: TcpStream) -> io::Result<()> {
    let mut upstream_buf = [0; BUFFER_SIZE];
    let mut downstream_buf = [0; BUFFER_SIZE];
    loop {
        match try_select(
            Box::pin(upstream.read(&mut upstream_buf)),
            Box::pin(downstream.read(&mut downstream_buf)),
        )
        .await
        .map_err(|e| e.factor_first().0)?
        {
            Either::Left((n, _)) if n > 0 => {
                downstream.write_all(&upstream_buf[..n]).await?;
            }
            Either::Right((n, _)) if n > 0 => {
                upstream.write_all(&downstream_buf[..n]).await?;
            }
            _ => {}
        }
    }
}

async fn direct_proxy<A>(listen: A, upstream: SocketAddr) -> io::Result<JoinHandle<()>>
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(listen).await?;
    Ok(tokio::spawn(async move {
        loop {
            let (sock, _addr) = listener.accept().await.unwrap();
            sock.set_nodelay(true).unwrap();

            let upstream = TcpStream::connect(upstream.clone()).await.unwrap();
            upstream.set_nodelay(true).unwrap();

            tokio::spawn(proxy_pipe(sock, upstream));
        }
    }))
}

struct Backend {
    upstream: Client,
}

#[async_trait]
impl psql_srv::Backend for Backend {
    const SERVER_VERSION: &'static str = "14";

    type Value = Value;
    type Row = Vec<Self::Value>;
    type Resultset = Vec<Self::Row>;

    async fn on_init(&mut self, _database: &str) -> Result<(), psql_srv::Error> {
        Ok(())
    }

    async fn on_query(
        &mut self,
        query: &str,
    ) -> Result<psql_srv::QueryResponse<Self::Resultset>, psql_srv::Error> {
        let res = self
            .upstream
            .query(query, &[])
            .await
            .map_err(|e| psql_srv::Error::Unknown(e.to_string()))?;

        Ok(QueryResponse::Select {
            schema: res
                .first()
                .map(|row| {
                    row.columns()
                        .iter()
                        .map(|col| psql_srv::Column {
                            name: col.name().into(),
                            col_type: col.type_().clone(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            resultset: res
                .into_iter()
                .map(|r| {
                    (0..r.len())
                        .map(|i| Value {
                            col_type: r.columns()[i].type_().clone(),
                            value: r.get(i),
                        })
                        .collect()
                })
                .collect(),
        })
    }

    async fn on_prepare(
        &mut self,
        _query: &str,
    ) -> Result<psql_srv::PrepareResponse, psql_srv::Error> {
        todo!()
    }

    async fn on_execute(
        &mut self,
        _statement_id: u32,
        _params: &[psql_srv::Value],
    ) -> Result<psql_srv::QueryResponse<Self::Resultset>, psql_srv::Error> {
        todo!()
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), psql_srv::Error> {
        todo!()
    }
}

async fn psql_srv_proxy<A>(listen: A) -> io::Result<JoinHandle<()>>
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(listen).await?;
    Ok(tokio::spawn(async move {
        loop {
            let (sock, _addr) = listener.accept().await.unwrap();
            let (client, conn) =
                tokio_postgres::connect("postgresql://postgres:noria@127.0.0.1/noria", NoTls)
                    .await
                    .unwrap();
            tokio::spawn(conn);
            let backend = Backend { upstream: client };
            psql_srv::run_backend(backend, sock).await
        }
    }))
}

async fn setup_fixture_data() {
    let (client, conn) =
        tokio_postgres::connect("postgresql://postgres:noria@127.0.0.1/noria", NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
        .simple_query("DROP TABLE IF EXISTS benchmark_fixture_data;")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE benchmark_fixture_data (x TEXT);")
        .await
        .unwrap();

    for _ in 0..NUM_ROWS {
        client
            .query(
                "INSERT INTO benchmark_fixture_data (x) VALUES ($1);",
                &[&"a"],
            )
            .await
            .unwrap();
    }
}

fn benchmark_direct_proxy(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    rt.block_on(setup_fixture_data());

    for (query, label) in [
        ("SELECT 1", "one value"),
        ("SELECT x FROM benchmark_fixture_data", "many values"),
    ] {
        let mut group = c.benchmark_group(format!("proxy to postgres/{}", label));

        group.bench_function("no proxy (control)", |b| {
            let rt = Runtime::new().unwrap();
            let (client, conn) = rt
                .block_on(tokio_postgres::connect(
                    "postgresql://postgres:noria@127.0.0.1/noria",
                    NoTls,
                ))
                .unwrap();
            rt.spawn(conn);
            let c = &client;

            b.to_async(&rt).iter(move || async move {
                c.simple_query(query).await.unwrap();
            });
        });

        group.bench_function("direct proxy", |b| {
            let rt = Runtime::new().unwrap();
            let _proxy = rt
                .block_on(direct_proxy(
                    "0.0.0.0:8888",
                    "127.0.0.1:5432".parse().unwrap(),
                ))
                .unwrap();
            let (client, conn) = rt
                .block_on(tokio_postgres::connect(
                    "postgresql://postgres:noria@127.0.0.1:8888/noria",
                    NoTls,
                ))
                .unwrap();
            rt.spawn(conn);
            let c = &client;

            b.to_async(&rt).iter(move || async move {
                c.simple_query(query).await.unwrap();
            });
        });

        group.bench_function("psql-srv proxy", |b| {
            let rt = Runtime::new().unwrap();
            let _proxy = rt.block_on(psql_srv_proxy("0.0.0.0:8888")).unwrap();
            let (client, conn) = rt
                .block_on(tokio_postgres::connect(
                    "postgresql://postgres:noria@127.0.0.1:8888/noria",
                    NoTls,
                ))
                .unwrap();
            rt.spawn(conn);
            let c = &client;

            b.to_async(&rt).iter(move || async move {
                c.simple_query(query).await.unwrap();
            });
        });

        group.finish();
    }
}

criterion_group!(benches, benchmark_direct_proxy);
criterion_main!(benches);
