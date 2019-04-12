use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use noria::{self, TableOperation};
use std::path::PathBuf;
use std::sync::Arc;
use std::time;
use tokio::prelude::*;
use tower_service::Service;

pub(crate) mod graph;

#[derive(Clone)]
pub(crate) struct LocalNoria {
    _g: Arc<graph::Graph>,
    // in Option because we need to drop them first
    // see https://aochagavia.github.io/blog/enforcing-drop-order-in-rust/
    r: Option<noria::View>,
    w: Option<noria::Table>,
}

// View and Table are both Send, but graph::Graph isn't Sync, so Arc<Graph> isn't Send.
// We only stick Graph inside an Arc so that it won't be dropped, we never actually access it!
unsafe impl Send for LocalNoria {}

impl VoteClient for LocalNoria {
    type Future = Box<Future<Item = Self, Error = failure::Error> + Send>;
    fn new(
        ex: tokio::runtime::TaskExecutor,
        params: Parameters,
        args: clap::ArgMatches,
    ) -> <Self as VoteClient>::Future {
        use noria::{DurabilityMode, PersistenceParameters};

        assert!(params.prime);

        let verbose = args.is_present("verbose");
        let fudge = args.is_present("fudge-rpcs");

        let mut persistence = PersistenceParameters::default();
        persistence.mode = if args.is_present("durability") {
            if args.is_present("retain-logs-on-exit") {
                DurabilityMode::Permanent
            } else {
                DurabilityMode::DeleteOnExit
            }
        } else {
            DurabilityMode::MemoryOnly
        };
        let flush_ns = value_t_or_exit!(args, "flush-timeout", u32);
        persistence.flush_timeout = time::Duration::new(0, flush_ns);
        persistence.persistence_threads = value_t_or_exit!(args, "persistence-threads", i32);
        persistence.log_prefix = "vote".to_string();
        persistence.log_dir = args
            .value_of("log-dir")
            .and_then(|p| Some(PathBuf::from(p)));

        // setup db
        let mut s = graph::Builder::default();
        s.logging = verbose;
        s.sharding = match value_t_or_exit!(args, "shards", usize) {
            0 => None,
            x => Some(x),
        };
        s.stupid = args.is_present("stupid");
        let purge = args.value_of("purge").unwrap().to_string();
        s.purge = purge.clone();
        let g = s.start(ex, persistence);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }

        Box::new(
            g.and_then(|mut g| g.graph.handle().table("Article").map(move |a| (g, a)))
                .and_then(move |(g, mut a)| {
                    if fudge {
                        a.i_promise_dst_is_same_process();
                    }

                    a.perform_all((0..params.articles).map(|i| {
                        vec![
                            ((i + 1) as i32).into(),
                            format!("Article #{}", i + 1).into(),
                        ]
                    }))
                    .map(move |_| g)
                    .map_err(|e| e.error)
                    .then(|r| {
                        r.context("failed to do article prepopulation")
                            .map_err(failure::Error::from)
                    })
                })
                .and_then(move |mut g| {
                    if verbose {
                        println!("Done with prepopulation");
                    }

                    // TODO: allow writes to propagate
                    let view = if purge == "none" {
                        "ArticleWithVoteCount"
                    } else {
                        "SHALLOW_ArticleWithVoteCount"
                    };

                    g.graph
                        .handle()
                        .view(view)
                        .and_then(move |r| {
                            g.graph.handle().table("Vote").map(move |mut w| {
                                if fudge {
                                    // fudge write rpcs by sending just the pointer over tcp
                                    w.i_promise_dst_is_same_process();
                                }
                                LocalNoria {
                                    _g: Arc::new(g),
                                    r: Some(r),
                                    w: Some(w),
                                }
                            })
                        })
                }),
        )
    }
}

impl Service<ReadRequest> for LocalNoria {
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

impl Service<WriteRequest> for LocalNoria {
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

impl Drop for LocalNoria {
    fn drop(&mut self) {
        drop(self.r.take());
        drop(self.w.take());
    }
}
