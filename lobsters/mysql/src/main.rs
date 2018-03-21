extern crate mysql_async as my;
extern crate tokio_core;
extern crate trawler;
#[macro_use]
extern crate clap;
extern crate futures;

use clap::{App, Arg};
use trawler::{LobstersRequest, Vote};
use futures::Future;
use my::prelude::*;

use std::time;
use std::collections::HashSet;
use std::rc::Rc;

struct MysqlSpawner {
    opts: my::Opts,
}
impl MysqlSpawner {
    fn new(mut opts: my::OptsBuilder) -> Self {
        opts.tcp_nodelay(true);
        MysqlSpawner { opts: opts.into() }
    }
}

struct MysqlTrawler {
    c: my::Pool,
}
impl MysqlTrawler {
    fn new(handle: &tokio_core::reactor::Handle, opts: my::Opts) -> Self {
        let mut c = my::Pool::new(opts, handle);
        MysqlTrawler { c }
    }
}
/*
impl Drop for MysqlTrawler {
    fn drop(&mut self) {
        self.c.disconnect();
    }
}
*/
impl trawler::LobstersClient for MysqlTrawler {
    type Factory = MysqlSpawner;

    fn spawn(spawner: &mut Self::Factory, handle: &tokio_core::reactor::Handle) -> Self {
        MysqlTrawler::new(handle, spawner.opts.clone())
    }

    fn handle(
        this: Rc<Self>,
        req: trawler::LobstersRequest,
    ) -> Box<futures::Future<Item = time::Duration, Error = ()>> {
        let sent = time::Instant::now();
        match req {
            LobstersRequest::Frontpage => Box::new(
                this.c
                    .get_conn()
                    .and_then(|c| {
                        c.drop_exec(
                            "\
                             SELECT users.* \
                             FROM users WHERE users.session_token = ? \
                             ORDER BY users.id ASC LIMIT 1",
                            ("KMQEEJjXymcyFj3j7Qn3c3kZ5AFcghUxscm6J9c0a3XBTMjD2OA9PEoecxyt",),
                        )
                    })
                    .and_then(|c| {
                        c.start_transaction(my::TransactionOptions::new())
                            .and_then(|t| {
                                t.drop_query(
                                 "SELECT keystores.* FROM keystores WHERE keystores.key = 'traffic:date' ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE; \
                                 SELECT keystores.* FROM keystores WHERE keystores.key = 'traffic:hits' ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE; \
                                 UPDATE keystores SET value = 1521590012 WHERE keystores.key = 'traffic:date';",
                            )
                            })
                            .and_then(|t| t.commit())
                    })
                    .and_then(|c| c.drop_query("SELECT `tags`.* FROM `tags` WHERE 1=0"))
                    .and_then(|c| {
                        c.query(
                            "SELECT  `stories`.* FROM `stories` \
                             WHERE `stories`.`merged_story_id` IS NULL \
                             AND `stories`.`is_expired` = 0 \
                             AND ((CAST(upvotes AS signed) - CAST(downvotes AS signed)) >= 0) \
                             ORDER BY hotness LIMIT 26 OFFSET 0",
                        )
                    })
                    .and_then(|stories| {
                        stories.reduce_and_drop(
                            (HashSet::new(), HashSet::new()),
                            |(mut users, mut stories), story| {
                                users.insert(story.get::<String, _>("user_id").unwrap());
                                stories.insert(story.get::<String, _>("id").unwrap());
                                (users, stories)
                            },
                        )
                    })
                    .and_then(|(c, (users, stories))| {
                        let users: Vec<_> = users.into_iter().collect();
                        c.drop_query(&format!(
                            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                            users.join(",")
                        )).map(move |c| (c, stories))
                    })
                    .and_then(|(c, stories)| {
                        let stories = stories.into_iter().collect::<Vec<_>>().join(", ");
                        c
                            .drop_query(&format!(
                                "SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` IN ({})", stories
                            ))
                            .map(move |c| (c, stories))
                    })
                    .and_then(|(c, stories)| {
                        c
                            .drop_query(&format!(
                                "SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` IN ({})", stories
                            ))
                            .map(move |c| (c, stories))
                    })
                    .and_then(|(c, stories)| {
                        c.query(&format!(
                        "SELECT `taggings`.* FROM `taggings` WHERE `taggings`.`story_id` IN ({})",
                        stories
                    ))
                    })
                    .and_then(|taggings| {
                        taggings.reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                            tags.insert(tagging.get::<String, _>("tag_id").unwrap());
                            tags
                        })
                    })
                    .and_then(|(c, tags)| {
                        let tags = tags.into_iter()
                            .map(String::from)
                            .collect::<Vec<_>>()
                            .join(", ");
                        c.drop_query(&format!(
                            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                            tags
                        ))
                    })
                    .map_err(|e| {
                        eprintln!("{:?}", e);
                    })
                    .map(move |_| sent.elapsed()),
            ),
            LobstersRequest::Recent => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(|c| {
                            c.drop_exec(
                                "\
                                 SELECT users.* \
                                 FROM users WHERE users.session_token = ? \
                                 ORDER BY users.id ASC LIMIT 1",
                                ("KMQEEJjXymcyFj3j7Qn3c3kZ5AFcghUxscm6J9c0a3XBTMjD2OA9PEoecxyt",),
                            )
                        })
                        .and_then(|c| {
                            c.start_transaction(my::TransactionOptions::new())
                                .and_then(|t| {
                                    t.drop_query(
                                 "SELECT keystores.* FROM keystores WHERE keystores.key = 'traffic:date' ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE; \
                                 SELECT keystores.* FROM keystores WHERE keystores.key = 'traffic:hits' ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE; \
                                 UPDATE keystores SET value = 1521590012 WHERE keystores.key = 'traffic:date';",
                            )
                                })
                                .and_then(|t| t.commit())
                        })
                        .and_then(|c| c.drop_query("SELECT `tags`.* FROM `tags` WHERE 1=0"))
                        .and_then(|c| {
                            // /recent is a little weird:
                            // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
                            // but it *basically* just looks for stories in the past few days
                            // because all our stories are for the same day, we add a LIMIT
                            c.query(
                                "SELECT `stories`.`id`, \
                                 `stories`.`upvotes`, \
                                 `stories`.`downvotes`, \
                                 `stories`.`user_id` \
                                 FROM `stories` \
                                 WHERE `stories`.`merged_story_id` IS NULL \
                                 AND `stories`.`is_expired` = 0 \
                                 AND `stories`.`created_at` > NOW() - INTERVAL 3 DAY \
                                 WHERE upvotes - downvotes <= 5 \
                                 ORDER BY stories.id DESC, stories.created_at DESC \
                                 LIMIT 25",
                            )
                        })
                        .and_then(|stories| {
                            stories.reduce_and_drop(Vec::new(), |mut stories, story| {
                                stories.push(story.get::<String, _>("id").unwrap());
                                stories
                            })
                        })
                        .and_then(|(c, stories)| {
                            c.query(&format!(
                                "SELECT  `stories`.* FROM `stories` WHERE `stories`.`id` IN ({})",
                                stories.join(", ")
                            ))
                        })
                        .and_then(|stories| {
                            stories.reduce_and_drop(
                                (HashSet::new(), HashSet::new()),
                                |(mut users, mut stories), story| {
                                    users.insert(story.get::<String, _>("user_id").unwrap());
                                    stories.insert(story.get::<String, _>("id").unwrap());
                                    (users, stories)
                                },
                            )
                        })
                        .and_then(|(c, (users, stories))| {
                            let users: Vec<_> = users.into_iter().collect();
                            c.drop_query(&format!(
                                "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                                users.join(",")
                            )).map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            let stories = stories.into_iter().collect::<Vec<_>>().join(", ");
                            c
                            .drop_query(&format!(
                                "SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` IN ({})", stories
                            ))
                            .map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            c
                            .drop_query(&format!(
                                "SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` IN ({})", stories
                            ))
                            .map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            c.query(&format!(
                        "SELECT `taggings`.* FROM `taggings` WHERE `taggings`.`story_id` IN ({})",
                        stories
                        ))
                        })
                        .and_then(|taggings| {
                            taggings.reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                                tags.insert(tagging.get::<String, _>("tag_id").unwrap());
                                tags
                            })
                        })
                        .and_then(|(c, tags)| {
                            let tags = tags.into_iter()
                                .map(String::from)
                                .collect::<Vec<_>>()
                                .join(", ");
                            c.drop_query(&format!(
                                "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                                tags
                            ))
                        })
                        .map_err(|e| {
                            eprintln!("{:?}", e);
                        })
                        .map(move |_| sent.elapsed()),
                )
            }
            LobstersRequest::Login(uid) => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::Logout(..) => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::Story(id) => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::StoryVote(user, story, v) => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::CommentVote(user, comment, v) => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::Submit { id, user, title } => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
            LobstersRequest::Comment {
                id,
                user,
                story,
                parent,
            } => {
                // TODO
                Box::new(futures::future::ok(time::Duration::new(0, 0)))
            }
        }
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
    let mut s = MysqlSpawner::new(my::OptsBuilder::from_opts(args.value_of("dbn").unwrap()));
    let mut core = tokio_core::reactor::Core::new().unwrap();
    use trawler::LobstersClient;
    let c = Rc::new(MysqlTrawler::spawn(&mut s, &core.handle()));
    core.run(MysqlTrawler::handle(c, LobstersRequest::Frontpage))
        .unwrap();

    wl.run::<MysqlTrawler, _>(s, args.is_present("prime"));
}
