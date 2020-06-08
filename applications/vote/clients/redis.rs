use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use redis::AsyncCommands;
use std::future::Future;
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct Conn {
    c: redis::aio::MultiplexedConnection,
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

            Ok(Conn { c: conn })
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // no backpressure, so queueing will happen internally in the library probably.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let len = req.0.len();
        let mut conn = self.c.clone();
        async move {
            let rows: Vec<i32> = conn.get(req.0).await?;
            assert_eq!(rows.len(), len);
            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
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
