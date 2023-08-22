//! This module contains a [`criterion`] benchmark which compares multiple different methods of
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
//! $ cargo criterion -p readyset-psql --bench proxy
//! ```

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, vec};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::{try_select, Either};
use futures::stream::Peekable;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use postgres_types::Type;
use psql_srv::{
    Credentials, CredentialsNeeded, PrepareResponse, PsqlBackend, PsqlValue, QueryResponse,
};
use readyset_data::DfValue;
use readyset_psql::ParamRef;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, Row, RowStream, Statement};

const BUFFER_SIZE: usize = 16 * 1024 * 1024;

const NUM_ROWS: usize = 10_000;

async fn proxy_pipe(mut upstream: TcpStream, mut downstream: TcpStream) -> io::Result<()> {
    let mut upstream_buf = Box::new([0; BUFFER_SIZE]);
    let mut downstream_buf = Box::new([0; BUFFER_SIZE]);
    loop {
        match try_select(
            Box::pin(upstream.read(upstream_buf.as_mut())),
            Box::pin(downstream.read(downstream_buf.as_mut())),
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

            let upstream = TcpStream::connect(upstream).await.unwrap();
            upstream.set_nodelay(true).unwrap();

            tokio::spawn(proxy_pipe(sock, upstream));
        }
    }))
}

enum ResultStream {
    Owned(vec::IntoIter<Row>),
    Streaming(Pin<Box<Peekable<RowStream>>>),
}

impl Stream for ResultStream {
    type Item = Result<Vec<PsqlValue>, psql_srv::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            ResultStream::Owned(iter) => Poll::Ready(
                iter.next()
                    .map(|r| (0..r.len()).map(|i| Ok(r.get(i))).collect()),
            ),
            ResultStream::Streaming(stream) => {
                Poll::Ready(ready!(stream.as_mut().poll_next(cx)).map(|res| match res {
                    Ok(r) => (0..r.len()).map(|i| Ok(r.get(i))).collect(),
                    Err(e) => Err(e.into()),
                }))
            }
        }
    }
}

struct Backend {
    upstream: Client,
    streaming: bool,
    statements: Vec<Statement>,
}

impl Backend {
    fn new(upstream: Client, streaming: bool) -> Self {
        Self {
            upstream,
            streaming,
            statements: Vec::with_capacity(2048),
        }
    }
}

#[async_trait]
impl PsqlBackend for Backend {
    type Resultset = ResultStream;

    fn version(&self) -> String {
        "14".into()
    }

    fn credentials_for_user(&self, _user: &str) -> Option<Credentials> {
        Some(Credentials::Any)
    }

    async fn on_init(&mut self, _database: &str) -> Result<CredentialsNeeded, psql_srv::Error> {
        Ok(CredentialsNeeded::None)
    }

    async fn on_query(
        &mut self,
        query: &str,
    ) -> Result<QueryResponse<Self::Resultset>, psql_srv::Error> {
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
                            table_oid: None,
                            attnum: None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            resultset: ResultStream::Owned(res.into_iter()),
        })
    }

    async fn on_prepare(
        &mut self,
        query: &str,
        _parameter_data_types: &[Type],
    ) -> Result<PrepareResponse, psql_srv::Error> {
        let stmt = self
            .upstream
            .prepare(query)
            .await
            .map_err(|e| psql_srv::Error::Unknown(e.to_string()))?;

        let resp = PrepareResponse {
            prepared_statement_id: self.statements.len() as _,
            param_schema: stmt.params().to_vec(),
            row_schema: stmt
                .columns()
                .iter()
                .map(|c| psql_srv::Column {
                    name: c.name().into(),
                    col_type: c.type_().clone(),
                    table_oid: None,
                    attnum: None,
                })
                .collect(),
        };

        self.statements.push(stmt);

        Ok(resp)
    }

    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[psql_srv::PsqlValue],
    ) -> Result<QueryResponse<Self::Resultset>, psql_srv::Error> {
        let stmt = self
            .statements
            .get(statement_id as usize)
            .ok_or_else(|| psql_srv::Error::MissingPreparedStatement(statement_id.to_string()))?;

        let mut res = Box::pin(
            self.upstream
                .query_raw(
                    stmt,
                    params
                        .iter()
                        .map(|p| DfValue::try_from(ParamRef(p)).unwrap()),
                )
                .await?
                .peekable(),
        );

        let schema = res
            .as_mut()
            .peek()
            .await
            .and_then(|r| r.as_ref().ok())
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|col| psql_srv::Column {
                        name: col.name().into(),
                        col_type: col.type_().clone(),
                        table_oid: None,
                        attnum: None,
                    })
                    .collect()
            })
            .unwrap_or_default();

        let resultset = if self.streaming {
            ResultStream::Streaming(res)
        } else {
            ResultStream::Owned(res.try_collect::<Vec<_>>().await?.into_iter())
        };

        Ok(QueryResponse::Select { schema, resultset })
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), psql_srv::Error> {
        Ok(())
    }
}

async fn psql_srv_proxy<A>(listen: A, streaming: bool) -> io::Result<JoinHandle<()>>
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
            let backend = Backend::new(client, streaming);
            tokio::spawn(psql_srv::run_backend(backend, sock, false, None));
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

        group.bench_function("accumulating psql-srv proxy", |b| {
            let rt = Runtime::new().unwrap();
            let _proxy = rt.block_on(psql_srv_proxy("0.0.0.0:8888", false)).unwrap();
            let (client, conn) = rt
                .block_on(tokio_postgres::connect(
                    "postgresql://postgres:noria@127.0.0.1:8888/noria",
                    NoTls,
                ))
                .unwrap();
            rt.spawn(conn);
            let c = &client;

            b.to_async(&rt).iter(move || async move {
                c.query(query, &[]).await.unwrap();
            });
        });

        group.bench_function("streaming psql-srv proxy", |b| {
            let rt = Runtime::new().unwrap();
            let _proxy = rt.block_on(psql_srv_proxy("0.0.0.0:8888", true)).unwrap();
            let (client, conn) = rt
                .block_on(tokio_postgres::connect(
                    "postgresql://postgres:noria@127.0.0.1:8888/noria",
                    NoTls,
                ))
                .unwrap();
            rt.spawn(conn);
            let c = &client;

            b.to_async(&rt).iter(move || async move {
                c.query(query, &[]).await.unwrap();
            });
        });

        group.finish();
    }
}

criterion_group!(benches, benchmark_direct_proxy);
criterion_main!(benches);
