use crate::clients::{Parameters, VoteClient};
use clap;
use failure::ResultExt;
use futures::Future;
use noria::{self, DataType};
use std::path::PathBuf;
use std::sync::Arc;
use std::time;

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
    existential type NewFuture: Future<Item = Self, Error = failure::Error>;
    existential type ReadFuture: Future<Item = (), Error = failure::Error>;
    existential type WriteFuture: Future<Item = (), Error = failure::Error>;

    fn spawn(
        ex: tokio::runtime::TaskExecutor,
        params: Parameters,
        args: clap::ArgMatches,
    ) -> Self::NewFuture {
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
        let g = s.start(ex, persistence);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }

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
                .then(|r| {
                    r.context("failed to do article prepopulation")
                        .map_err(Into::into)
                })
            })
            .and_then(move |mut g| {
                if verbose {
                    println!("Done with prepopulation");
                }

                // TODO: allow writes to propagate

                g.graph
                    .handle()
                    .view("ArticleWithVoteCount")
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
            })
    }

    fn handle_writes(&mut self, ids: &[i32]) -> Self::WriteFuture {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        self.w
            .as_mut()
            .unwrap()
            .perform_all(data)
            .map_err(failure::Error::from)
    }

    fn handle_reads(&mut self, ids: &[i32]) -> Self::ReadFuture {
        let arg = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        let len = ids.len();
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
            .map_err(failure::Error::from)
    }
}

impl Drop for LocalNoria {
    fn drop(&mut self) {
        drop(self.r.take());
        drop(self.w.take());
    }
}
