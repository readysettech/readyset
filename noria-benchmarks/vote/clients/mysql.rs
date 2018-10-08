use clap;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use mysql::{self, Opts, OptsBuilder};

pub(crate) struct Client {
    conn: mysql::Conn,
}

pub(crate) struct Conf {
    opts: Opts,
}

impl VoteClientConstructor for Conf {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        let addr = args.value_of("address").unwrap();
        let addr = format!("mysql://{}", addr);
        let db = args.value_of("database").unwrap();
        let opts = Opts::from_url(&addr).unwrap();

        if params.prime {
            let mut opts = OptsBuilder::from_opts(opts.clone());
            opts.db_name(None::<&str>);
            opts.init(vec![
                "SET max_heap_table_size = 4294967296;",
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            ]);
            let mut conn = mysql::Conn::new(opts).unwrap();
            if conn.query(format!("USE {}", db)).is_ok() {
                conn.query(format!("DROP DATABASE {}", &db).as_str())
                    .unwrap();
            }
            conn.query(format!("CREATE DATABASE {}", &db).as_str())
                .unwrap();

            // create tables with indices
            conn.query(format!("USE {}", db)).unwrap();
            conn.prep_exec(
                "CREATE TABLE art (id bigint not null, title varchar(16) not null, votes bigint not null, \
                 PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();
            conn.prep_exec(
                "CREATE TABLE vt (u bigint not null, id bigint not null, KEY id (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();

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
                conn.query(sql).unwrap();
            }
        }

        // now we connect for real
        let mut opts = OptsBuilder::from_opts(opts);
        opts.db_name(Some(db));
        opts.init(vec![
            "SET max_heap_table_size = 4294967296;",
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
        ]);
        opts.stmt_cache_size(10000);

        Conf { opts: opts.into() }
    }

    fn make(&mut self) -> Self::Instance {
        Client {
            conn: mysql::Conn::new(self.opts.clone()).unwrap(),
        }
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let ids = ids.into_iter().map(|a| a as &_).collect::<Vec<_>>();

        let vals = (0..ids.len())
            .map(|_| "(0, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
        self.conn.prep_exec(vote_qstring, &ids).unwrap();

        let vals = (0..ids.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        // NOTE: this is *not* correct for duplicate ids
        let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN ({})", vals);
        self.conn.prep_exec(vote_qstring, &ids).unwrap();
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let ids = ids.into_iter().map(|a| a as &_).collect::<Vec<_>>();
        let vals = (0..ids.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        let qstring = format!("SELECT id, title, votes FROM art WHERE id IN ({})", vals);

        let mut rows = 0;
        let mut qresult = self.conn.prep_exec(qstring, &ids).unwrap();
        while qresult.more_results_exists() {
            for row in qresult.by_ref() {
                row.unwrap();
                rows += 1;
            }
        }

        // <= because IN() collapses duplicates
        assert!(rows <= ids.len());
    }
}
