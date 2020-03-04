use clap;
use crate::clients::{Parameters, VoteClient, VoteClientConstructor};
use memcached::{
    self,
    proto::{MultiOperation, ProtoType},
};
use mysql::{self, Opts, OptsBuilder};
use std::collections::BTreeMap;

pub(crate) struct Client {
    my: mysql::Conn,
    mem: memcached::Client,
}

pub(crate) struct Conf {
    my: Opts,
    mem: String,
}

impl VoteClientConstructor for Conf {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        let my_addr = args.value_of("mysql-address").unwrap();
        let my_addr = format!("mysql://{}", my_addr);
        let db = args.value_of("database").unwrap();
        let opts = Opts::from_url(&my_addr).unwrap();
        let mem_addr = args.value_of("memcached-address").unwrap();

        // first, prime mysql
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
                    sql.push_str(&format!("({}, 'Article #{}', 0)", aid, aid));
                    aid += 1;
                }
                conn.query(sql).unwrap();
            }
        }

        // then prime memcached
        if params.prime {
            // prepop
            let mut c = memcached::Client::connect(
                &[(&format!("tcp://{}", mem_addr), 1)],
                ProtoType::Binary,
            )
            .unwrap();

            let mut aid = 1;
            let bs = 1000;
            assert_eq!(params.articles % bs, 0);
            for _ in 0..params.articles / bs {
                use std::collections::BTreeMap;
                let articles: Vec<_> = (0..bs)
                    .map(|i| {
                        let article_id = aid + i;
                        (
                            format!("article_{}", article_id),
                            format!("Article #{}, 0", article_id),
                        )
                    })
                    .collect();
                let mut m = BTreeMap::new();
                for &(ref k, ref v) in &articles {
                    m.insert(k.as_bytes(), (v.as_bytes(), 0, 0));
                }
                c.set_multi(m).unwrap();

                aid += bs;
            }
        }

        let mut opts = OptsBuilder::from_opts(opts);
        opts.db_name(Some(db));
        opts.init(vec![
            "SET max_heap_table_size = 4294967296;",
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
        ]);
        opts.stmt_cache_size(10000);

        Conf {
            my: opts.into(),
            mem: mem_addr.to_string(),
        }
    }

    fn make(&mut self) -> Self::Instance {
        Client {
            my: mysql::Conn::new(self.my.clone()).unwrap(),
            mem: memcached::Client::connect(
                &[(&format!("tcp://{}", self.mem), 1)],
                ProtoType::Binary,
            )
            .unwrap(),
        }
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let ids = ids.into_iter().map(|a| a as &_).collect::<Vec<_>>();

        // record the votes
        let vals = (0..ids.len())
            .map(|_| "(0, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
        self.my.prep_exec(vote_qstring, &ids).unwrap();

        let vals = (0..ids.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        // NOTE: this is *not* correct for duplicate ids
        let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN ({})", vals);
        self.my.prep_exec(vote_qstring, &ids).unwrap();

        // and invalidate the cache
        let keys: Vec<_> = ids
            .into_iter()
            .map(|article_id| format!("article_{}", article_id))
            .collect();
        let ids: Vec<_> = keys.iter().map(|key| key.as_bytes()).collect();
        drop(self.mem.delete_multi(&ids[..]));
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        // first read as much as we can from cache
        let keys: Vec<_> = ids
            .into_iter()
            .map(|article_id| (article_id, format!("article_{}", article_id)))
            .collect();
        let key_bytes: Vec<_> = keys.iter().map(|k| k.1.as_bytes()).collect();

        let mut rows = 0;
        let mut misses = Vec::with_capacity(ids.len());
        let vals = self.mem.get_multi(&key_bytes[..]).unwrap();
        for &(key, ref kstr) in &keys {
            if let Some(_) = vals.get(kstr.as_bytes()) {
                rows += 1;
            } else {
                misses.push(key)
            }
        }

        if !misses.is_empty() {
            let ids = misses.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..misses.len()).map(|_| "?").collect::<Vec<_>>().join(",");
            let qstring = format!("SELECT art.* FROM art WHERE art.id IN ({})", vals);

            let mut m = Vec::new();
            let mut qresult = self.my.prep_exec(qstring, &ids).unwrap();
            while qresult.more_results_exists() {
                for row in qresult.by_ref() {
                    let row = row.unwrap();
                    m.push((
                        format!("article_{}", row.get::<i64, _>("id").unwrap()),
                        format!(
                            "{}, {}",
                            row.get::<String, _>("title").unwrap(),
                            row.get::<i64, _>("votes").unwrap()
                        ),
                    ));
                    rows += 1;
                }
            }

            let m: BTreeMap<_, _> = m
                .iter()
                .map(|&(ref k, ref v)| (k.as_bytes(), (v.as_bytes(), 0, 0)))
                .collect();

            // write results back to memcached
            self.mem.set_multi(m).unwrap();
        }

        // <= because IN() collapses duplicates
        assert!(rows <= ids.len());
    }
}
