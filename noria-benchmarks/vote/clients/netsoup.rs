use crate::clients::localsoup::graph::RECIPE;
use crate::clients::{Parameters, VoteClient};
use clap;
use failure::ResultExt;
use noria::{self, ControllerHandle, DataType, ZookeeperAuthority};
use tokio::prelude::*;

#[derive(Clone)]
pub(crate) struct Conn {
    ch: ControllerHandle<ZookeeperAuthority>,
    r: Option<noria::View>,
    w: Option<noria::Table>,
}

impl VoteClient for Conn {
    // TODO: existential once https://github.com/rust-lang/rust/issues/53546 is fixed
    type NewFuture = Box<Future<Item = Self, Error = failure::Error> + Send>;
    type ReadFuture = Box<Future<Item = (), Error = failure::Error> + Send>;
    type WriteFuture = Box<Future<Item = (), Error = failure::Error> + Send>;

    fn spawn(
        _: tokio::runtime::TaskExecutor,
        params: Parameters,
        args: clap::ArgMatches,
    ) -> Self::NewFuture {
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
                                .and_then(move |(c, mut a)| {
                                    a.perform_all((0..params.articles).map(|i| {
                                        vec![
                                            ((i + 1) as i32).into(),
                                            format!("Article #{}", i).into(),
                                        ]
                                    }))
                                    .map(move |_| c)
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

    fn handle_writes(&mut self, ids: &[i32]) -> Self::WriteFuture {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        Box::new(
            self.w
                .as_mut()
                .unwrap()
                .perform_all(data)
                .map_err(failure::Error::from),
        )
    }

    fn handle_reads(&mut self, ids: &[i32]) -> Self::ReadFuture {
        let arg: Vec<_> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        let len = ids.len();
        Box::new(
            self.r
                .as_mut()
                .unwrap()
                .multi_lookup(arg, true)
                .map(|rows| {
                    // TODO
                    //assert_eq!(rows.map(|rows| rows.len()), Ok(1));
                    rows.len()
                })
                .map(move |rows| {
                    assert_eq!(rows, len);
                })
                .map_err(failure::Error::from),
        )
    }
}
