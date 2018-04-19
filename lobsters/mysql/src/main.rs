#![feature(nll)]

extern crate chrono;
#[macro_use]
extern crate clap;
extern crate futures;
extern crate mysql_async as my;
extern crate tokio_core;
extern crate trawler;

use clap::{App, Arg};
use futures::Future;
use futures::future::Either;
use my::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time;
use trawler::{LobstersRequest, UserId};

struct MysqlSpawner {
    opts: my::OptsBuilder,
}
impl MysqlSpawner {
    fn new(opts: my::OptsBuilder) -> Self {
        MysqlSpawner { opts }
    }
}

struct MysqlTrawler {
    c: my::Pool,
    tokens: RefCell<HashMap<u32, String>>,
}
impl MysqlTrawler {
    fn new(handle: &tokio_core::reactor::Handle, opts: my::Opts) -> Self {
        MysqlTrawler {
            c: my::Pool::new(opts, handle),
            tokens: HashMap::new().into(),
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
    type Factory = MysqlSpawner;

    fn spawn(spawner: &mut Self::Factory, handle: &tokio_core::reactor::Handle) -> Self {
        MysqlTrawler::new(handle, spawner.opts.clone().into())
    }

    fn setup(spawner: &mut Self::Factory) {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let mut opts = spawner.opts.clone();
        opts.pool_min(None::<usize>);
        opts.pool_max(None::<usize>);
        let db: String = my::Opts::from(opts.clone()).get_db_name().unwrap().clone();
        let c = my::Pool::new(opts, &core.handle());
        core.run(
            c.get_conn()
                .and_then(|c| c.drop_query(&format!("DROP DATABASE {}", db)))
                .and_then(|c| c.drop_query(&format!("CREATE DATABASE {}", db)))
                .and_then(|c| c.drop_query(&format!("USE {}", db))),
        ).unwrap();
        for q in include_str!("../db-schema.sql").lines() {
            if q.starts_with("--") {
                continue;
            }
            core.run(c.get_conn().and_then(|c| c.drop_query(q)))
                .unwrap();
        }
    }

    fn handle(
        this: Rc<Self>,
        acting_as: Option<UserId>,
        req: trawler::LobstersRequest,
    ) -> Box<futures::Future<Item = time::Duration, Error = ()>> {
        let sent = time::Instant::now();

        let c = this.c.get_conn();

        let c = if let Some(u) = acting_as {
            let this = this.clone();
            Either::A(c.and_then(move |c| {
                let tokens = this.tokens.borrow();
                if let Some(u) = tokens.get(&u) {
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

        let c = match req {
            LobstersRequest::User(uid) => endpoints::user::handle(c, acting_as, uid),
            LobstersRequest::Frontpage => endpoints::frontpage::handle(c, acting_as),
            LobstersRequest::Comments => endpoints::comments::handle(c, acting_as),
            LobstersRequest::Recent => endpoints::recent::handle(c, acting_as),
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
                    }).and_then(move |(c, user)| {
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
            LobstersRequest::Story(id) => endpoints::story::handle(c, acting_as, id),
            LobstersRequest::StoryVote(story, v) => {
                endpoints::story_vote::handle(c, acting_as, story, v)
            }
            LobstersRequest::CommentVote(comment, v) => {
                endpoints::comment_vote::handle(c, acting_as, comment, v)
            }
            LobstersRequest::Submit { id, title } => {
                endpoints::submit::handle(c, acting_as, id, title)
            }
            LobstersRequest::Comment { id, story, parent } => {
                endpoints::comment::handle(c, acting_as, id, story, parent)
            }
        };

        // notifications
        let c = if let Some(uid) = acting_as {
            Either::A(c.and_then(move |(c, with_notifications)| {
                if !with_notifications {
                    return Either::A(futures::future::ok(c));
                }

                Either::B(c.drop_exec(
                    "SELECT COUNT(*) \
                     FROM `replying_comments_for_count`
                     WHERE `replying_comments_for_count`.`user_id` = ? \
                     GROUP BY `replying_comments_for_count`.`user_id` \
                     ",
                    (uid,),
                ).and_then(move |c| {
                    c.drop_exec(
                        "SELECT `keystores`.* \
                         FROM `keystores` \
                         WHERE `keystores`.`key` = ?",
                        (format!("user:{}:unread_messages", uid),),
                    )
                }))
            }))
        } else {
            Either::B(c.map(|(c, _)| c))
        };

        Box::new(c.map_err(|e| {
            eprintln!("{:?}", e);
        }).map(move |_| sent.elapsed()))
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
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("30")
                .help("Benchmark runtime in seconds"),
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

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(
        value_t_or_exit!(args, "memscale", f64),
        value_t_or_exit!(args, "reqscale", f64),
    ).issuers(value_t_or_exit!(args, "issuers", usize))
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
    opts.pool_min(Some(50usize));
    opts.pool_max(Some(50usize));
    let mut s = MysqlSpawner::new(opts);

    if !args.is_present("prime") {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        use trawler::LobstersClient;
        let c = Rc::new(MysqlTrawler::spawn(&mut s, &core.handle()));
        core.run(MysqlTrawler::handle(c, None, LobstersRequest::Frontpage))
            .unwrap();
    }

    wl.run::<MysqlTrawler, _>(s, args.is_present("prime"));
}
