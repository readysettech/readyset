use crate::clients::localsoup::graph::RECIPE;
use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use noria::{self, ControllerHandle, TableOperation, ZookeeperAuthority};
use std::future::Future;
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct Conn {
    ch: ControllerHandle<ZookeeperAuthority>,
    r: Option<noria::View>,
    w: Option<noria::Table>,
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, failure::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches) -> <Self as VoteClient>::Future {
        let zk = format!(
            "{}/{}",
            args.value_of("zookeeper").unwrap(),
            args.value_of("deployment").unwrap()
        );

        async move {
            let zk = ZookeeperAuthority::new(&zk)?;
            let mut c = ControllerHandle::new(zk).await?;
            if params.prime {
                // for prepop, we need a mutator
                c.install_recipe(RECIPE).await?;
                let mut a = c.table("Article").await?;
                a.perform_all(
                    (0..params.articles)
                        .map(|i| vec![((i + 1) as i32).into(), format!("Article #{}", i).into()]),
                )
                .await
                .context("failed to do article prepopulation")?;
            }

            let v = c.table("Vote").await?;
            let awvc = c.view("ArticleWithVoteCount").await?;
            Ok(Conn {
                ch: c,
                r: Some(awvc),
                w: Some(v),
            })
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.r
            .as_mut()
            .unwrap()
            .poll_ready(cx)
            .map_err(failure::Error::from)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let len = req.0.len();
        let arg = req
            .0
            .into_iter()
            .map(|article_id| vec![(article_id as i32).into()])
            .collect();

        let fut = self.r.as_mut().unwrap().call((arg, true));
        async move {
            let rows = fut.await?;
            assert_eq!(rows.len(), len);
            for row in rows {
                assert_eq!(row.len(), 1);
            }
            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<Vec<TableOperation>>::poll_ready(self.w.as_mut().unwrap(), cx)
            .map_err(failure::Error::from)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let data: Vec<TableOperation> = req
            .0
            .into_iter()
            .map(|article_id| vec![(article_id as i32).into(), 0.into()].into())
            .collect();

        let fut = self.w.as_mut().unwrap().call(data);
        async move {
            fut.await?;
            Ok(())
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        drop(self.r.take());
        drop(self.w.take());
    }
}
