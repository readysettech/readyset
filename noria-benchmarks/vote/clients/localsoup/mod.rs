use clap;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use noria::{self, DataType};
use std::path::PathBuf;
use std::thread;
use std::time;

pub(crate) mod graph;

pub(crate) struct Client {
    _ch: noria::ControllerHandle<noria::LocalAuthority>,
    r: noria::View,
    #[allow(dead_code)]
    w: noria::Table,
}

pub(crate) struct Constructor(graph::Graph, bool);

// this is *only* safe because the only method we call is `duplicate` which is safe to call from
// other threads.
unsafe impl Send for Constructor {}

impl VoteClientConstructor for Constructor {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        use noria::{DurabilityMode, PersistenceParameters};

        assert!(params.prime);

        let verbose = args.is_present("verbose");

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
        let mut s = graph::Setup::default();
        s.logging = verbose;
        s.sharding = match value_t_or_exit!(args, "shards", usize) {
            0 => None,
            x => Some(x),
        };
        s.stupid = args.is_present("stupid");
        let mut g = s.make(persistence);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }
        let mut a = g.graph.table("Article").unwrap();
        a.batch_insert((0..params.articles).map(|i| {
            vec![
                ((i + 1) as i32).into(),
                format!("Article #{}", i + 1).into(),
            ]
        }))
        .unwrap();
        if verbose {
            println!("Done with prepopulation");
        }

        let fudge = args.is_present("fudge-rpcs");

        // allow writes to propagate
        thread::sleep(time::Duration::from_secs(1));

        Constructor(g, fudge)
    }

    fn make(&mut self) -> Self::Instance {
        let mut ch = self.0.graph.pointer().connect().unwrap();
        let r = ch.view("ArticleWithVoteCount").unwrap();
        let mut w = ch.table("Vote").unwrap();
        if self.1 {
            // fudge write rpcs by sending just the pointer over tcp
            w.i_promise_dst_is_same_process();
        }
        Client { _ch: ch, r, w }
    }

    fn spawns_threads() -> bool {
        true
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        self.w.insert_all(data).unwrap();
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let arg = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        let rows = self
            .r
            .multi_lookup(arg, true)
            .unwrap()
            .into_iter()
            .map(|_rows| {
                // TODO
                //assert_eq!(rows.map(|rows| rows.len()), Ok(1));
            })
            .count();
        assert_eq!(rows, ids.len());
    }
}
