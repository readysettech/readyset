use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use redis::AsyncCommands;
use std::future::Future;
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct Conn {
    // concurrency limit is per load generator!
    svc: tower_limit::ConcurrencyLimit<ActualConn>,
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, failure::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches) -> <Self as VoteClient>::Future {
        let addr = args.value_of("address").unwrap();

        eprintln!("OBS OBS OBS: redis is single-threaded");

        let connstr = format!("redis://{}/", addr);
        let client = redis::Client::open(connstr).unwrap();

        async move {
            let mut conn = client.get_multiplexed_tokio_connection().await?;

            if params.prime {
                // for prepop, we need a mutator
                let mut mset = redis::pipe();
                for i in 0..params.articles {
                    mset.cmd("SET").arg((i + 1) as i32).arg(0).ignore();
                }
                mset.query_async(&mut conn)
                    .await
                    .context("failed to do article prepopulation")?;
            }

            // we need to provide _some_ backpressure, otherwise the load generator won't start to
            // batch (which we want to improve throughput). the choice of 2048 is semi-random. it's
            // the same as noria is using at time of writing.
            let svc = tower_limit::ConcurrencyLimit::new(ActualConn { c: conn }, 2048);
            Ok(Conn { svc })
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = <tower_limit::ConcurrencyLimit<ActualConn> as Service<ReadRequest>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<ReadRequest>::poll_ready(&mut self.svc, cx)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        Service::<ReadRequest>::call(&mut self.svc, req)
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = <tower_limit::ConcurrencyLimit<ActualConn> as Service<WriteRequest>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<WriteRequest>::poll_ready(&mut self.svc, cx)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        Service::<WriteRequest>::call(&mut self.svc, req)
    }
}

#[derive(Clone)]
pub(crate) struct ActualConn {
    c: redis::aio::MultiplexedConnection,
}

impl Service<ReadRequest> for ActualConn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let len = req.0.len();
        let mut conn = self.c.clone();
        async move {
            if req.0.len() == 1 {
                // https://github.com/mitsuhiko/redis-rs/issues/336
                let _: i32 = conn.get(req.0[0]).await?;
            } else {
                let rows: Vec<i32> = conn.get(&req.0[..]).await?;
                assert_eq!(rows.len(), len);
            }
            Ok(())
        }
    }
}

impl Service<WriteRequest> for ActualConn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let mut conn = self.c.clone();
        let mut batch = redis::pipe();
        for aid in req.0 {
            batch.cmd("INCRBY").arg(aid).arg(1).ignore();
        }

        async move {
            batch.query_async(&mut conn).await?;
            Ok(())
        }
    }
}
