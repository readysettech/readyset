extern crate mysql_async as my;

// https://github.com/rust-lang/rust/pull/64856
macro_rules! format {
    ($($arg:tt)*) => {{
        let res = std::format!($($arg)*);
        res
    }}
}

use clap::value_t_or_exit;
use clap::{App, Arg};
use futures_util::future::{ready, Either, TryFutureExt};
use my::prelude::*;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
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
    reset: bool,
}
impl MysqlTrawler {
    fn new(variant: Variant, opts: my::OptsBuilder, simulate_shards: Option<u32>) -> Self {
        MysqlTrawler {
            c: MaybeConn::None(opts),
            tokens: HashMap::new(),
            simulate_shards,
            variant,
            reset: false,
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
    type RequestFuture = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;
    type SetupFuture = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;
    type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn setup(&mut self) -> Self::SetupFuture {
        let mut opts = if let MaybeConn::None(ref opts) = self.c {
            opts.clone()
        } else {
            unreachable!("connection established before setup");
        };
        opts.pool_options(my::PoolOptions::with_constraints(
            my::PoolConstraints::new(1, 1).unwrap(),
        ));
        let variant = self.variant;
        let db: String = my::Opts::from(opts.clone())
            .get_db_name()
            .unwrap()
            .to_string();
        let c = my::Pool::new(opts);
        let db_drop = format!("DROP DATABASE {}", db);
        let db_create = format!("CREATE DATABASE {}", db);
        let db_use = format!("USE {}", db);
        Box::pin(async move {
            let mut c = c.get_conn().await?;
            c = c.drop_query(&db_drop).await?;
            c = c.drop_query(&db_create).await?;
            c = c.drop_query(&db_use).await?;
            let schema = match variant {
                Variant::Original => ORIGINAL_SCHEMA,
                Variant::Noria => NORIA_SCHEMA,
                Variant::Natural => NATURAL_SCHEMA,
            };
            let mut current_q = String::new();
            for line in schema.lines() {
                if line.starts_with("--") || line.is_empty() {
                    continue;
                }
                if !current_q.is_empty() {
                    current_q.push_str(" ");
                }
                current_q.push_str(line);
                if current_q.ends_with(';') {
                    c = c.drop_query(&current_q).await?;
                    current_q.clear();
                }
            }
            Ok(())
        })
    }

    fn handle(
        &mut self,
        acting_as: Option<UserId>,
        req: trawler::LobstersRequest,
        priming: bool,
    ) -> Self::RequestFuture {
        let c = self.c.pool().get_conn();

        let c = if priming {
            Either::Right(c)
        } else if let Some(u) = acting_as {
            let tokens = self.tokens.get(&u).cloned();
            Either::Left(c.and_then(move |c| {
                if let Some(u) = tokens {
                    Either::Left(c.drop_exec(
                        "SELECT users.* \
                         FROM users WHERE users.session_token = ?",
                        (u,),
                    ))
                } else {
                    Either::Right(ready(Ok(c)))
                }
            }))
        } else {
            Either::Right(c)
        };

        // Give shim a heads up that we have finished priming.
        let c = if !priming {
            if !self.reset {
                self.reset = true;
                Either::Right(c.and_then(|c| c.drop_query("SET @primed = 1")))
            } else {
                Either::Left(c)
            }
        } else {
            Either::Left(c)
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
            ($module:tt, $req:expr, $sim_shards:ident) => {{
                match req {
                    LobstersRequest::User(uid) => endpoints::$module::user::handle(c, acting_as, uid).await,
                    LobstersRequest::Frontpage => endpoints::$module::frontpage::handle(c, acting_as).await,
                    LobstersRequest::Comments => endpoints::$module::comments::handle(c, acting_as).await,
                    LobstersRequest::Recent => endpoints::$module::recent::handle(c, acting_as).await,
                    LobstersRequest::Login => {
                        let c = c.await?;
                            let (mut c, user) = c.first_exec::<_, _, my::Row>(
                                "\
                                 SELECT  1 as one \
                                 FROM `users` \
                                 WHERE `users`.`username` = ?",
                                (format!("user{}", acting_as.unwrap()),),
                            ).await?;

                            if user.is_none() {
                                let uid = acting_as.unwrap();
                                c = c.drop_exec(
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
                                ).await?;
                            }

                            Ok((c, false))
                    }
                    LobstersRequest::Logout => Ok((c.await?, false)),
                    LobstersRequest::Story(id) => {
                        endpoints::$module::story::handle(c, acting_as, $sim_shards, id).await
                    }
                    LobstersRequest::StoryVote(story, v) => {
                        endpoints::$module::story_vote::handle(c, acting_as, story, v).await
                    }
                    LobstersRequest::CommentVote(comment, v) => {
                        endpoints::$module::comment_vote::handle(c, acting_as, comment, v).await
                    }
                    LobstersRequest::Submit { id, title } => {
                        endpoints::$module::submit::handle(c, acting_as, id, title, priming).await
                    }
                    LobstersRequest::Comment { id, story, parent } => {
                        endpoints::$module::comment::handle(c, acting_as, id, story, parent, priming).await
                    }
                }
            }}
        };

        let variant = self.variant;
        let simulate_shards = self.simulate_shards;
        Box::pin(async move {
            let (c, with_notifications) = match variant {
                Variant::Original => handle_req!(original, req, simulate_shards),
                Variant::Noria => handle_req!(noria, req, simulate_shards),
                Variant::Natural => handle_req!(natural, req, simulate_shards),
            }?;

            // notifications
            if let Some(uid) = acting_as {
                if with_notifications && !priming {
                    match variant {
                        Variant::Original => endpoints::original::notifications(c, uid).await,
                        Variant::Noria => endpoints::noria::notifications(c, uid).await,
                        Variant::Natural => endpoints::natural::notifications(c, uid).await,
                    }?;
                }
            }

            Ok(())
        })
    }

    fn shutdown(mut self) -> Self::ShutdownFuture {
        let _ = self.c.pool();
        if let MaybeConn::Connected(pool) = self.c {
            Box::pin(pool.disconnect())
        } else {
            unreachable!();
        }
    }
}

fn main() {
    let args = App::new("trawler-mysql")
        .version("0.1")
        .about("Benchmark a lobste.rs Rails installation using MySQL directly")
        .arg(
            Arg::with_name("scale")
                .long("scale")
                .takes_value(true)
                .default_value("1.0")
                .help("Reuest load scale factor for workload"),
        )
        .arg(
            Arg::with_name("in-flight")
                .long("in-flight")
                .takes_value(true)
                .default_value("50")
                .help("Number of allowed concurrent requests"),
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
    let in_flight = value_t_or_exit!(args, "in-flight", usize);

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(value_t_or_exit!(args, "scale", f64))
        .time(
            time::Duration::from_secs(value_t_or_exit!(args, "warmup", u64)),
            time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64)),
        )
        .in_flight(in_flight);

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    // check that we can indeed connect
    let mut opts = my::OptsBuilder::from_opts(args.value_of("dbn").unwrap());
    opts.tcp_nodelay(true);
    opts.pool_options(my::PoolOptions::with_constraints(
        my::PoolConstraints::new(in_flight, in_flight).unwrap(),
    ));
    let s = MysqlTrawler::new(variant, opts.into(), simulate_shards);

    wl.run(s, args.is_present("prime"));
}
