use crate::clients::{Parameters, VoteClient};
use clap;
use futures::Future;
use noria::{self, DataType};
use std::path::PathBuf;
use std::sync::Arc;
use std::time;

pub(crate) mod graph;

#[derive(Clone)]
pub(crate) struct LocalNoria {
    g: Arc<graph::Graph>,
    r: noria::View,
    #[allow(dead_code)]
    w: noria::Table,
}

impl VoteClient for LocalNoria {
    type NewFuture = Box<Future<Item = Self, Error = ()>>;
    type ReadFuture = Box<Future<Item = (), Error = noria::ViewError>>;
    type WriteFuture = Box<Future<Item = (), Error = noria::TableError>>;

    fn spawn(params: &Parameters, args: &clap::ArgMatches) -> Self::NewFuture {
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

        // TODO: reuse pool
        let mut g = s.make(persistence);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }
        let mut a = g.graph.table("Article").unwrap();
        if fudge {
            a.i_promise_dst_is_same_process();
        }

        Box::new(
            a.perform_all((0..params.articles).map(|i| {
                vec![
                    ((i + 1) as i32).into(),
                    format!("Article #{}", i + 1).into(),
                ]
            }))
            .and_then(move |_| {
                if verbose {
                    println!("Done with prepopulation");
                }

                // TODO: allow writes to propagate

                let r = g.graph.view("ArticleWithVoteCount").unwrap();
                let mut w = g.graph.table("Vote").unwrap();
                if fudge {
                    // fudge write rpcs by sending just the pointer over tcp
                    w.i_promise_dst_is_same_process();
                }

                LocalNoria {
                    g: Arc::new(g),
                    r,
                    w,
                }
            }),
        )
    }

    fn handle_writes(&mut self, ids: &[i32]) -> Self::WriteFuture {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        Box::new(self.w.perform_all(data))
    }

    fn handle_reads(&mut self, ids: &[i32]) -> Self::ReadFuture {
        let arg = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        let len = ids.len();
        Box::new(
            self.r
                .multi_lookup(arg, true)
                .map(|rows| {
                    // TODO
                    //assert_eq!(rows.map(|rows| rows.len()), Ok(1));
                    rows.len()
                })
                .map(move |rows| {
                    assert_eq!(rows, len);
                }),
        )
    }
}
