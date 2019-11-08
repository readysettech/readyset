use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use mysql_async::prelude::*;
use std::future::Future;
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct Conn {
    // TODO: maybe just use a pool here?
    c: async_lease::Lease<mysql_async::Conn>,
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, failure::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches<'_>) -> <Self as VoteClient>::Future {
        let addr = args.value_of("address").unwrap();
        let addr = format!("mysql://{}", addr);
        let db = args.value_of("database").unwrap().to_string();

        async move {
            let opts = mysql_async::Opts::from_url(&addr).unwrap();

            if params.prime {
                let mut opts = mysql_async::OptsBuilder::from_opts(opts.clone());
                opts.db_name(None::<&str>);
                opts.init(vec![
                    "SET max_heap_table_size = 4294967296;",
                    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
                ]);
                let mut conn = mysql_async::Conn::new(opts).await.unwrap();
                let workaround = format!("DROP DATABASE IF EXISTS {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();
                let workaround = format!("CREATE DATABASE {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();

                // create tables with indices
                let workaround = format!("USE {}", db);
                conn = conn.drop_query(&workaround).await.unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE art \
                         (id bigint not null, title varchar(16) not null, votes bigint not null, \
                         PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                        (),
                    )
                    .await
                    .unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE vt \
                         (u bigint not null, id bigint not null, KEY id (id)) ENGINE = MEMORY;",
                        (),
                    )
                    .await
                    .unwrap();

                // prepop
                let mut aid = 1;
                let bs = 1000;
                assert_eq!(params.articles % bs, 0);
                for _ in 0..params.articles / bs {
                    let mut sql = String::new();
                    sql.push_str("INSERT INTO art (id, title, votes) VALUES ");
                    for i in 0..bs {
                        if i != 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str("(");
                        sql.push_str(&format!("{}, 'Article #{}'", aid, aid));
                        sql.push_str(", 0)");
                        aid += 1;
                    }
                    conn = conn.drop_query(sql).await.unwrap();
                }
            }

            // now we connect for real
            let mut opts = mysql_async::OptsBuilder::from_opts(opts);
            opts.db_name(Some(db));
            opts.init(vec![
                "SET max_heap_table_size = 4294967296;",
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            ]);
            opts.stmt_cache_size(10000);

            Ok(Conn {
                c: async_lease::Lease::from(mysql_async::Conn::new(opts).await.unwrap()),
            })
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.c.poll_acquire(cx).map(Ok)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let mut lease = self.c.transfer();
        async move {
            let conn = lease.take();

            let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..len).map(|_| "?").collect::<Vec<_>>().join(",");
            let qstring = format!("SELECT id, title, votes FROM art WHERE id IN ({})", vals);

            let qresult = conn.prep_exec(qstring, &ids).await.unwrap();
            let (conn, rows) = qresult
                .reduce_and_drop(0, |rows, _| rows + 1)
                .await
                .unwrap();

            // Give back the connection for other callers.
            // After this, `poll_ready` may return `Ok(Ready)` again.
            lease.restore(conn);

            // <= because IN() collapses duplicates
            assert!(rows <= ids.len());

            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.c.poll_acquire(cx).map(Ok)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let mut lease = self.c.transfer();
        async move {
            let conn = lease.take();

            let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..len).map(|_| "(0, ?)").collect::<Vec<_>>().join(", ");
            let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
            let conn = conn.drop_exec(vote_qstring, &ids).await.unwrap();

            let vals = (0..len).map(|_| "?").collect::<Vec<_>>().join(",");
            // NOTE: this is *not* correct for duplicate ids
            let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN ({})", vals);
            let conn = conn.drop_exec(vote_qstring, &ids).await.unwrap();

            // Give back the connection for other callers.
            // After this, `poll_ready` may return `Ok(Ready)` again.
            lease.restore(conn);

            Ok(())
        }
    }
}
