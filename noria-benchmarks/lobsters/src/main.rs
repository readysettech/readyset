#![feature(nll)]

#[macro_use]
extern crate clap;
extern crate mysql_async as my;

use clap::{App, Arg};
use futures::future::Either;
use futures::{Future, IntoFuture, Stream};
use my::prelude::*;
use std::collections::HashMap;
use std::time;
use trawler::{LobstersRequest, UserId};

const ORIGINAL_SCHEMA: &'static str = include_str!("../db-schema/original.sql");
const NORIA_SCHEMA: &'static str = include_str!("../db-schema/noria.sql");
const NATURAL_SCHEMA: &'static str = include_str!("../db-schema/natural.sql");

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Variant {
    Original,
    Noria,
    Natural,
}

#[derive(Clone, Debug)]
enum MaybeConn {
    Connected(my::Pool),
    None(my::OptsBuilder),
}

impl MaybeConn {
    fn pool(&mut self) -> &mut my::Pool {
        match *self {
            MaybeConn::Connected(ref mut pool) => pool,
            MaybeConn::None(ref opts) => {
                let c = my::Pool::new(opts.clone());
                let _ = std::mem::replace(self, MaybeConn::Connected(c));
                self.pool()
            }
        }
    }
}

struct MysqlTrawler {
    c: MaybeConn,
    variant: Variant,
    tokens: HashMap<u32, String>,
    simulate_shards: Option<u32>,
}
impl MysqlTrawler {
    fn new(variant: Variant, opts: my::OptsBuilder, simulate_shards: Option<u32>) -> Self {
        MysqlTrawler {
            c: MaybeConn::None(opts),
            tokens: HashMap::new(),
            simulate_shards,
            variant,
        }
    }
}
/*
impl Drop for MysqlTrawler {
    fn drop(&mut self) {
        self.c.disconnect();
    }
}
*/

mod endpoints;

impl trawler::LobstersClient for MysqlTrawler {
    type Error = my::error::Error;
    type RequestFuture = Box<futures::Future<Item = (), Error = Self::Error> + Send>;
    type SetupFuture = Box<futures::Future<Item = (), Error = Self::Error> + Send>;

    fn setup(&mut self) -> Self::SetupFuture {
        let mut opts = if let MaybeConn::None(ref opts) = self.c {
            opts.clone()
        } else {
            unreachable!("connection established before setup");
        };
        opts.pool_constraints(my::PoolConstraints::new(1, 1));
        let variant = self.variant;
        let db: String = my::Opts::from(opts.clone())
            .get_db_name()
            .unwrap()
            .to_string();
        let c = my::Pool::new(opts);
        let db_drop = format!("DROP DATABASE {}", db);
        let db_create = format!("CREATE DATABASE {}", db);
        let db_use = format!("USE {}", db);
        Box::new(
            c.get_conn()
                .and_then(move |c| c.drop_query(&db_drop))
                .and_then(move |c| c.drop_query(&db_create))
                .and_then(move |c| c.drop_query(&db_use))
                .then(move |r| {
                    let c = r.unwrap();
                    let schema = match variant {
                        Variant::Original => ORIGINAL_SCHEMA,
                        Variant::Noria => NORIA_SCHEMA,
                        Variant::Natural => NATURAL_SCHEMA,
                    };
                    futures::stream::iter_ok(schema.lines())
                        .fold((c, String::new()), move |(c, mut current_q), line| {
                            if line.starts_with("--") || line.is_empty() {
                                return Either::A(Ok((c, current_q)).into_future());
                            }
                            if !current_q.is_empty() {
                                current_q.push_str(" ");
                            }
                            current_q.push_str(line);
                            if current_q.ends_with(';') {
                                Either::B(c.drop_query(&current_q).then(
                                    move |r| -> Result<_, my::error::Error> {
                                        let c = r.unwrap();
                                        current_q.clear();
                                        Ok((c, current_q))
                                    },
                                ))
                            } else {
                                Either::A(Ok((c, current_q)).into_future())
                            }
                        })
                        .map(|_| ())
                }),
        )
    }

    fn handle(
        &mut self,
        acting_as: Option<UserId>,
        req: trawler::LobstersRequest,
    ) -> Self::RequestFuture {
        let c = self.c.pool().get_conn();

        let c = if let Some(u) = acting_as {
            let tokens = self.tokens.get(&u).cloned();
            Either::A(c.and_then(move |c| {
                if let Some(u) = tokens {
                    Either::A(c.drop_exec(
                        "SELECT users.* \
                         FROM users WHERE users.session_token = ?",
                        (u,),
                    ))
                } else {
                    Either::B(futures::future::ok(c))
                }
            }))
        } else {
            Either::B(c)
        };

        // TODO: traffic management
        // https://github.com/lobsters/lobsters/blob/master/app/controllers/application_controller.rb#L37
        /*
        let c = c.and_then(|c| {
            c.start_transaction(my::TransactionOptions::new())
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:date' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:hits' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 100 \
                         WHERE keystores.key = 'traffic:hits'",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 1521590012 \
                         WHERE keystores.key = 'traffic:date'",
                    )
                })
                .and_then(|t| t.commit())
        });
        */

        macro_rules! handle_req {
            ($module:tt, $req:expr) => {
                match req {
                    LobstersRequest::User(uid) => endpoints::$module::user::handle(c, acting_as, uid),
                    LobstersRequest::Frontpage => endpoints::$module::frontpage::handle(c, acting_as),
                    LobstersRequest::Comments => endpoints::$module::comments::handle(c, acting_as),
                    LobstersRequest::Recent => endpoints::$module::recent::handle(c, acting_as),
                    LobstersRequest::Login => {
                        Box::new(
                            c.and_then(move |c| {
                                c.first_exec::<_, _, my::Row>(
                                    "\
                                     SELECT  1 as one \
                                     FROM `users` \
                                     WHERE `users`.`username` = ?",
                                    (format!("user{}", acting_as.unwrap()),),
                                )
                            })
                            .and_then(move |(c, user)| {
                                if user.is_none() {
                                    let uid = acting_as.unwrap();
                                    futures::future::Either::A(c.drop_exec(
                                        "\
                                         INSERT INTO `users` \
                                         (`username`, `email`, `password_digest`, `created_at`, \
                                         `session_token`, `rss_token`, `mailing_list_token`) \
                                         VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (
                                            format!("user{}", uid),
                                            format!("user{}@example.com", uid),
                                            "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka", // test
                                            chrono::Local::now().naive_local(),
                                            format!("token{}", uid),
                                            format!("rsstoken{}", uid),
                                            format!("mtok{}", uid),
                                        ),
                                    ))
                                } else {
                                    futures::future::Either::B(futures::future::ok(c))
                                }
                            })
                            .map(|c| (c, false)),
                        )
                    }
                    LobstersRequest::Logout => Box::new(c.map(|c| (c, false))),
                    LobstersRequest::Story(id) => {
                        endpoints::$module::story::handle(c, acting_as, self.simulate_shards, id)
                    }
                    LobstersRequest::StoryVote(story, v) => {
                        endpoints::$module::story_vote::handle(c, acting_as, story, v)
                    }
                    LobstersRequest::CommentVote(comment, v) => {
                        endpoints::$module::comment_vote::handle(c, acting_as, comment, v)
                    }
                    LobstersRequest::Submit { id, title } => {
                        endpoints::$module::submit::handle(c, acting_as, id, title)
                    }
                    LobstersRequest::Comment { id, story, parent } => {
                        endpoints::$module::comment::handle(c, acting_as, id, story, parent)
                    }
                }
            }
        };

        let c = match self.variant {
            Variant::Original => handle_req!(original, req),
            Variant::Noria => handle_req!(noria, req),
            Variant::Natural => handle_req!(natural, req),
        };

        // notifications
        let c = if let Some(uid) = acting_as {
            let variant = self.variant;
            Either::A(c.and_then(move |(c, with_notifications)| {
                if !with_notifications {
                    return Either::A(futures::future::ok(c));
                }

                Either::B(match variant {
                    Variant::Original => Box::new(endpoints::original::notifications(c, uid))
                        as Box<Future<Item = my::Conn, Error = my::error::Error> + Send>,
                    Variant::Noria => Box::new(endpoints::noria::notifications(c, uid)),
                    Variant::Natural => Box::new(endpoints::natural::notifications(c, uid)),
                })
            }))
        } else {
            Either::B(c.map(|(c, _)| c))
        };

        Box::new(c.map(move |_| ()))
    }
}

fn main() {
    let args = App::new("trawler-mysql")
        .version("0.1")
        .about("Benchmark a lobste.rs Rails installation using MySQL directly")
        .arg(
            Arg::with_name("memscale")
                .long("memscale")
                .takes_value(true)
                .default_value("1.0")
                .help("Memory scale factor for workload"),
        )
        .arg(
            Arg::with_name("reqscale")
                .long("reqscale")
                .takes_value(true)
                .default_value("1.0")
                .help("Reuest load scale factor for workload"),
        )
        .arg(
            Arg::with_name("issuers")
                .short("i")
                .long("issuers")
                .takes_value(true)
                .default_value("1")
                .help("Number of issuers to run"),
        )
        .arg(
            Arg::with_name("prime")
                .long("prime")
                .help("Set if the backend must be primed with initial stories and comments."),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .long("queries")
                .possible_values(&["original", "noria", "natural"])
                .takes_value(true)
                .required(true)
                .help("Which set of queries to run"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("fakeshards")
                .long("simulate-shards")
                .takes_value(true)
                .conflicts_with("memscale")
                .help("Simulate if read_ribbons base had N shards"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(true)
                .default_value("10")
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Use file-based serialized HdrHistograms")
                .takes_value(true)
                .long_help(
                    "If the file already exists, the existing histogram is extended.\
                     There are two histograms, written out in order: \
                     sojourn and remote.",
                ),
        )
        .arg(
            Arg::with_name("dbn")
                .value_name("DBN")
                .takes_value(true)
                .default_value("mysql://lobsters@localhost/soup")
                .index(1),
        )
        .get_matches();

    let variant = match args.value_of("queries").unwrap() {
        "original" => Variant::Original,
        "noria" => Variant::Noria,
        "natural" => Variant::Natural,
        _ => unreachable!(),
    };
    let simulate_shards = args
        .value_of("fakeshards")
        .map(|_| value_t_or_exit!(args, "fakeshards", u32));
    assert!(
        simulate_shards.is_none() || value_t_or_exit!(args, "memscale", f64) == 1.0,
        "cannot simulate sharding with memscale != 1 (b/c of NUM_STORIES)"
    );

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(
        value_t_or_exit!(args, "memscale", f64),
        value_t_or_exit!(args, "reqscale", f64),
    )
    .issuers(value_t_or_exit!(args, "issuers", usize))
    .time(
        time::Duration::from_secs(value_t_or_exit!(args, "warmup", u64)),
        time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64)),
    )
    .in_flight(50);

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    // check that we can indeed connect
    let mut opts = my::OptsBuilder::from_opts(args.value_of("dbn").unwrap());
    opts.tcp_nodelay(true);
    opts.pool_constraints(my::PoolConstraints::new(50, 50));
    let s = MysqlTrawler::new(variant, opts.into(), simulate_shards);

    wl.run(s, args.is_present("prime"));
}
