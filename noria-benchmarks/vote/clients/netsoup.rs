use crate::clients::localsoup::graph::RECIPE;
use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use noria::{self, ControllerHandle, TableOperation, ZookeeperAuthority};
use tokio::prelude::*;
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct Conn {
    ch: ControllerHandle<ZookeeperAuthority>,
    r: Option<noria::View>,
    w: Option<noria::Table>,
}

impl VoteClient for Conn {
    type Future = Box<Future<Item = Self, Error = failure::Error> + Send>;
    fn new(
        _: tokio::runtime::TaskExecutor,
        params: Parameters,
        args: clap::ArgMatches,
    ) -> <Self as VoteClient>::Future {
        let zk = format!(
            "{}/{}",
            args.value_of("zookeeper").unwrap(),
            args.value_of("deployment").unwrap()
        );

        Box::new(
            ZookeeperAuthority::new(&zk)
                .into_future()
                .and_then(ControllerHandle::new)
                .and_then(move |mut c| {
                    if params.prime {
                        // for prepop, we need a mutator
                        future::Either::A(
                            c.install_recipe(RECIPE)
                                .and_then(move |_| c.table("Article").map(move |a| (c, a)))
                                .and_then(move |(c, a)| {
                                    a.perform_all((0..params.articles).map(|i| {
                                        vec![
                                            ((i + 1) as i32).into(),
                                            format!("Article #{}", i).into(),
                                        ]
                                    }))
                                    .map(move |_| c)
                                    .map_err(|e| e.error)
                                    .then(|r| {
                                        r.context("failed to do article prepopulation")
                                            .map_err(Into::into)
                                    })
                                }),
                        )
                    } else {
                        future::Either::B(future::ok(c))
                    }
                })
                .and_then(|mut c| c.table("Vote").map(move |v| (c, v)))
                .and_then(|(mut c, v)| c.view("ArticleWithVoteCount").map(move |awvc| (c, v, awvc)))
                .map(|(c, v, awvc)| Conn {
                    ch: c,
                    r: Some(awvc),
                    w: Some(v),
                }),
        )
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = Box<Future<Item = (), Error = failure::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.r
            .as_mut()
            .unwrap()
            .poll_ready()
            .map_err(failure::Error::from)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let len = req.0.len();
        let arg = req
            .0
            .into_iter()
            .map(|article_id| vec![(article_id as usize).into()])
            .collect();

        Box::new(
            self.r
                .as_mut()
                .unwrap()
                .call((arg, true))
                .map(move |rows| {
                    // TODO: assert_eq!(rows.map(|rows| rows.len()), Ok(1));
                    assert_eq!(rows.len(), len);
                })
                .map_err(failure::Error::from),
        )
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = Box<Future<Item = (), Error = failure::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Service::<Vec<TableOperation>>::poll_ready(self.w.as_mut().unwrap())
            .map_err(failure::Error::from)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let data: Vec<TableOperation> = req
            .0
            .into_iter()
            .map(|article_id| vec![(article_id as usize).into(), 0.into()].into())
            .collect();

        Box::new(
            self.w
                .as_mut()
                .unwrap()
                .call(data)
                .map(|_| ())
                .map_err(failure::Error::from),
        )
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        drop(self.r.take());
        drop(self.w.take());
    }
}
