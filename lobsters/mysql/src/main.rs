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
        MysqlTrawler {
            c: my::Pool::new(opts, handle),
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

        let c = this.c.get_conn();

        // TODO: session management. now predictable token! pass in `as` parameter?
        // TODO: notifications
        /*
        let c = c.and_then(|c| {
            c.drop_exec(
                "\
                 SELECT users.* \
                 FROM users WHERE users.session_token = ? \
                 ORDER BY users.id ASC LIMIT 1",
                ("KMQEEJjXymcyFj3j7Qn3c3kZ5AFcghUxscm6J9c0a3XBTMjD2OA9PEoecxyt",),
            )
        });
        */

        // TODO: traffic management
        // https://github.com/lobsters/lobsters/blob/master/app/controllers/application_controller.rb#L37
        let c = c.and_then(|c| {
            c.start_transaction(my::TransactionOptions::new())
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:date' \
                         ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:hits' \
                         ORDER BY keystores.key ASC LIMIT 1 FOR UPDATE",
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

        // TODO: some queries are different when logged in.
        // e.g., / also loads tag filters, votes, hidden_stories, and saved_stories
        let c: Box<Future<Item = my::Conn, Error = my::errors::Error>> = match req {
            LobstersRequest::User(uid) => Box::new(c.and_then(move |c| {
                c.first_exec::<_, _, my::Row>(
                    "SELECT  `users`.* FROM `users` \
                     WHERE `users`.`username` = ? \
                     ORDER BY `users`.`id` ASC LIMIT 1",
                    (format!("user{}", uid),),
                )
            }).and_then(move |(c, user)| {
                let uid = user.unwrap().get::<u32, _>("id").unwrap();

                // most popular tag
                c.drop_exec(
                    "SELECT  `tags`.* FROM `tags` \
                     INNER JOIN `taggings` ON `taggings`.`tag_id` = `tags`.`id` \
                     INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
                     WHERE `tags`.`inactive` = 0 \
                     AND `stories`.`user_id` = ? \
                     GROUP BY `tags`.`id` \
                     ORDER BY COUNT(*) desc LIMIT 1",
                    (uid,),
                ).and_then(move |c| {
                        c.drop_query(&format!(
                            "SELECT  `keystores`.* \
                             FROM `keystores` \
                             WHERE `keystores`.`key` = 'user:{}:stories_submitted' \
                             ORDER BY `keystores`.`key` ASC LIMIT 1",
                            uid
                        ))
                    })
                    .and_then(move |c| {
                        c.drop_query(&format!(
                            "SELECT  `keystores`.* \
                             FROM `keystores` \
                             WHERE `keystores`.`key` = 'user:{}:comments_posted' \
                             ORDER BY `keystores`.`key` ASC LIMIT 1",
                            uid
                        ))
                    })
                    .and_then(move |c| {
                        c.drop_exec(
                            "SELECT  1 AS one FROM `hats` \
                             WHERE `hats`.`user_id` = ? LIMIT 1",
                            (uid,),
                        )
                    })
            })),
            LobstersRequest::Frontpage => Box::new(
                c.and_then(|c| c.drop_query("SELECT `tags`.* FROM `tags` WHERE 1=0"))
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
                                users.insert(story.get::<u32, _>("user_id").unwrap());
                                stories.insert(story.get::<u32, _>("id").unwrap());
                                (users, stories)
                            },
                        )
                    })
                    .and_then(|(c, (users, stories))| {
                        let users = users
                            .into_iter()
                            .map(|id| format!("{}", id))
                            .collect::<Vec<_>>()
                            .join(",");
                        c.drop_query(&format!(
                            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                            users,
                        )).map(move |c| (c, stories))
                    })
                    .and_then(|(c, stories)| {
                        let stories = stories
                            .into_iter()
                            .map(|id| format!("{}", id))
                            .collect::<Vec<_>>()
                            .join(",");
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
                            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
                            tags
                        })
                    })
                    .and_then(|(c, tags)| {
                        let tags = tags.into_iter()
                            .map(|id| format!("{}", id))
                            .collect::<Vec<_>>()
                            .join(",");
                        c.drop_query(&format!(
                            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                            tags
                        ))
                    }),
            ),
            LobstersRequest::Recent => {
                Box::new(
                    c.and_then(|c| c.drop_query("SELECT `tags`.* FROM `tags` WHERE 1=0"))
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
                                 AND upvotes - downvotes <= 5 \
                                 ORDER BY stories.id DESC, stories.created_at DESC \
                                 LIMIT 25",
                            )
                        })
                        .and_then(|stories| {
                            stories.reduce_and_drop(Vec::new(), |mut stories, story| {
                                stories.push(story.get::<u32, _>("id").unwrap());
                                stories
                            })
                        })
                        .and_then(|(c, stories)| {
                            let stories = stories
                                .into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(",");
                            c.query(&format!(
                                "SELECT  `stories`.* FROM `stories` WHERE `stories`.`id` IN ({})",
                                stories
                            ))
                        })
                        .and_then(|stories| {
                            stories.reduce_and_drop(
                                (HashSet::new(), HashSet::new()),
                                |(mut users, mut stories), story| {
                                    users.insert(story.get::<u32, _>("user_id").unwrap());
                                    stories.insert(story.get::<u32, _>("id").unwrap());
                                    (users, stories)
                                },
                            )
                        })
                        .and_then(|(c, (users, stories))| {
                            let users = users
                                .into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(",");
                            c.drop_query(&format!(
                                "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                                users
                            )).map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            let stories = stories
                                .into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(",");
                            c.drop_query(&format!(
                                "SELECT `suggested_titles`.* \
                                 FROM `suggested_titles` \
                                 WHERE `suggested_titles`.`story_id` IN ({})",
                                stories
                            )).map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            c.drop_query(&format!(
                                "SELECT `suggested_taggings`.* \
                                 FROM `suggested_taggings` \
                                 WHERE `suggested_taggings`.`story_id` IN ({})",
                                stories
                            )).map(move |c| (c, stories))
                        })
                        .and_then(|(c, stories)| {
                            c.query(&format!(
                                "SELECT `taggings`.* \
                                 FROM `taggings` \
                                 WHERE `taggings`.`story_id` IN ({})",
                                stories
                            ))
                        })
                        .and_then(|taggings| {
                            taggings.reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                                tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
                                tags
                            })
                        })
                        .and_then(|(c, tags)| {
                            let tags = tags.into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(", ");
                            c.drop_query(&format!(
                                "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                                tags
                            ))
                        }),
                )
            }
            LobstersRequest::Login(uid) => Box::new(c.and_then(move |c| {
                c.first_exec::<_, _, my::Row>(
                    "\
                     SELECT  1 as one \
                     FROM `users` \
                     WHERE `users`.`username` = ? \
                     ORDER BY `users`.`id` ASC LIMIT 1",
                    (format!("user{}", uid),),
                )
            }).and_then(move |(c, user)| {
                if user.is_none() {
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
                            "2018-03-25 16:00:24",
                            format!("token{}", uid),
                            "ryEKWLIxQe7yKGrUdg0vRpV25PzVUhJP4uwMAW8qw8Ye6W1DueXu1crKE4Ba",
                            "COoHFt9BXj",
                        ),
                    ))
                } else {
                    futures::future::Either::B(futures::future::ok(c))
                }
            })),
            LobstersRequest::Logout(..) => Box::new(c),
            LobstersRequest::Story(id) => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(move |c| {
                            c.prep_exec(
                                "SELECT `stories`.* \
                                 FROM `stories` \
                                 WHERE `stories`.`short_id` = ? \
                                 ORDER BY `stories`.`id` ASC LIMIT 1",
                                (::std::str::from_utf8(&id[..]).unwrap(),),
                            ).and_then(|result| result.collect_and_drop::<my::Row>())
                                .map(|(c, mut story)| (c, story.swap_remove(0)))
                        })
                        .and_then(|(c, story)| {
                            let author = story.get::<u32, _>("user_id").unwrap();
                            let id = story.get::<u32, _>("id").unwrap();
                            c.drop_exec(
                                "SELECT `users`.* FROM `users` WHERE `users`.`id` = ? LIMIT 1",
                                (author,),
                            ).map(move |c| (c, id))
                        })
                        .and_then(|(c, story)| {
                            // XXX: probably not drop here, but we know we have no merged stories
                            c.drop_exec(
                                "SELECT `stories`.`id` \
                                 FROM `stories` \
                                 WHERE `stories`.`merged_story_id` = ?",
                                (story,),
                            ).map(move |c| (c, story))
                        })
                        .and_then(|(c, story)| {
                            c.prep_exec(
                                "SELECT `comments`.* \
                                 FROM `comments` \
                                 WHERE `comments`.`story_id` = ? \
                                 ORDER BY \
                                 (upvotes - downvotes) < 0 ASC, \
                                 confidence DESC",
                                (story,),
                            ).map(move |comments| (comments, story))
                        })
                        .and_then(|(comments, story)| {
                            comments
                                .reduce_and_drop(
                                    (HashSet::new(), HashSet::new()),
                                    |(mut users, mut comments), comment| {
                                        users.insert(comment.get::<u32, _>("user_id").unwrap());
                                        comments.insert(comment.get::<u32, _>("id").unwrap());
                                        (users, comments)
                                    },
                                )
                                .map(move |(c, folded)| (c, folded, story))
                        })
                        .and_then(|(c, (users, comments), story)| {
                            // get user info for all commenters
                            let users = users
                                .into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(", ");
                            c.drop_query(&format!(
                                "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                                users
                            )).map(move |c| (c, comments, story))
                        })
                        .and_then(|(c, comments, story)| {
                            // get comment votes
                            // XXX: why?!
                            let comments = comments
                                .into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(", ");
                            c.drop_query(&format!(
                                "SELECT `votes`.* FROM `votes` WHERE `votes`.`comment_id` IN ({})",
                                comments
                            )).map(move |c| (c, story))
                            // NOTE: lobste.rs here fetches the user list again. unclear why?
                        })
                        .and_then(|(c, story)| {
                            c.prep_exec(
                                "SELECT `taggings`.* \
                                 FROM `taggings` \
                                 WHERE `taggings`.`story_id` = ?",
                                (story,),
                            )
                        })
                        .and_then(|taggings| {
                            taggings.reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                                tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
                                tags
                            })
                        })
                        .and_then(|(c, tags)| {
                            let tags = tags.into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(", ");
                            c.drop_query(&format!(
                                "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                                tags
                            ))
                        }),
                )
            }
            LobstersRequest::StoryVote(user, story, v) => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(move |c| {
                            c.prep_exec(
                                "SELECT `stories`.* \
                                 FROM `stories` \
                                 WHERE `stories`.`short_id` = ? \
                                 ORDER BY `stories`.`id` ASC LIMIT 1",
                                (::std::str::from_utf8(&story[..]).unwrap(),),
                            ).and_then(|result| result.collect_and_drop::<my::Row>())
                                .map(|(c, mut story)| (c, story.swap_remove(0)))
                        })
                        .and_then(move |(c, story)| {
                            let author = story.get::<u32, _>("user_id").unwrap();
                            let id = story.get::<u32, _>("id").unwrap();
                            let score = story.get::<f64, _>("hotness").unwrap();
                            c.drop_exec(
                                "SELECT  `votes`.* \
                                 FROM `votes` \
                                 WHERE `votes`.`user_id` = ? \
                                 AND `votes`.`story_id` = ? \
                                 AND `votes`.`comment_id` IS NULL \
                                 ORDER BY `votes`.`id` ASC LIMIT 1",
                                (user, id),
                            ).map(move |c| (c, author, id, score))
                        })
                        .and_then(move |(c, author, story, score)| {
                            // TODO: do something else if user has already voted
                            // TODO: technically need to re-load story under transaction
                            c.start_transaction(my::TransactionOptions::new())
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "INSERT INTO `votes` \
                                         (`user_id`, `story_id`, `vote`) \
                                         VALUES \
                                         (?, ?, ?)",
                                        (
                                            user,
                                            story,
                                            match v {
                                                Vote::Up => 1,
                                                Vote::Down => 0,
                                            },
                                        ),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `users` \
                                             SET `karma` = `karma` {} \
                                             WHERE `users`.`id` = ?",
                                            match v {
                                                Vote::Up => "+ 1",
                                                Vote::Down => "- 1",
                                            }
                                        ),
                                        (author,),
                                    )
                                })
                                .and_then(move |t| {
                                    // get all the stuff needed to compute updated hotness
                                    t.drop_exec(
                                        "SELECT `tags`.* \
                                        FROM `tags` \
                                        INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                                        WHERE `taggings`.`story_id` = ?",
                                        (story,))
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "SELECT \
                                         `comments`.`upvotes`, \
                                         `comments`.`downvotes` \
                                         FROM `comments` \
                                         WHERE `comments`.`story_id` = ? \
                                         AND user_id <> ?",
                                        (story, author),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "SELECT `stories`.`id` \
                                         FROM `stories` \
                                         WHERE `stories`.`merged_story_id` = ?",
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    // the *actual* algorithm for computing hotness isn't all
                                    // that interesting to us. it does affect what's on the
                                    // frontpage, but we're okay with using a more basic
                                    // upvote/downvote ratio thingy. See Story::calculated_hotness
                                    // in the lobsters source for details.
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE stories SET \
                                             upvotes = COALESCE(upvotes, 0) {}, \
                                             downvotes = COALESCE(downvotes, 0) {}, \
                                             hotness = '{}' \
                                             WHERE id = ?",
                                            match v {
                                                Vote::Up => "+ 1",
                                                Vote::Down => "+ 0",
                                            },
                                            match v {
                                                Vote::Up => "+ 0",
                                                Vote::Down => "+ 1",
                                            },
                                            score + match v {
                                                Vote::Up => 1.0,
                                                Vote::Down => -1.0,
                                            }
                                        ),
                                        (story,),
                                    )
                                })
                                .and_then(|t| t.commit())
                        }),
                )
            }
            LobstersRequest::CommentVote(user, comment, v) => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(move |c| {
                            c.first_exec::<_, _, my::Row>(
                                "SELECT `comments`.* \
                                 FROM `comments` \
                                 WHERE `comments`.`short_id` = ? \
                                 ORDER BY `comments`.`id` ASC LIMIT 1",
                                (::std::str::from_utf8(&comment[..]).unwrap(),),
                            )
                        })
                        .and_then(move |(c, comment)| {
                            let comment = comment.unwrap();
                            let author = comment.get::<u32, _>("user_id").unwrap();
                            let id = comment.get::<u32, _>("id").unwrap();
                            let story = comment.get::<u32, _>("story_id").unwrap();
                            let upvotes = comment.get::<u32, _>("upvotes").unwrap();
                            let downvotes = comment.get::<u32, _>("downvotes").unwrap();
                            c.drop_exec(
                                "SELECT  `votes`.* \
                                 FROM `votes` \
                                 WHERE `votes`.`user_id` = ? \
                                 AND `votes`.`story_id` = ? \
                                 AND `votes`.`comment_id` = ? \
                                 ORDER BY `votes`.`id` ASC LIMIT 1",
                                (user, story, id),
                            ).map(move |c| (c, author, id, story, upvotes, downvotes))
                        })
                        .and_then(move |(c, author, comment, story, upvotes, downvotes)| {
                            // TODO: do something else if user has already voted
                            // TODO: technically need to re-load comment under transaction
                            c.start_transaction(my::TransactionOptions::new())
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "INSERT INTO `votes` \
                                         (`user_id`, `story_id`, `comment_id`, `vote`) \
                                         VALUES \
                                         (?, ?, ?, ?)",
                                        (
                                            user,
                                            story,
                                            comment,
                                            match v {
                                                Vote::Up => 1,
                                                Vote::Down => 0,
                                            },
                                        ),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `users` \
                                             SET `karma` = `karma` {} \
                                             WHERE `id` = ?",
                                            match v {
                                                Vote::Up => "+ 1",
                                                Vote::Down => "- 1",
                                            }
                                        ),
                                        (author,),
                                    )
                                })
                                .and_then(move |t| {
                                    // approximate Comment::calculate_hotness
                                    let confidence =
                                        upvotes as f64 / (upvotes as f64 + downvotes as f64);
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `comments` \
                                             SET \
                                             `upvotes` = `upvotes` {}, \
                                             `downvotes` = `downvotes` {}, \
                                             `confidence` = {}
                                             WHERE `id` = ?",
                                            match v {
                                                Vote::Up => "+ 1",
                                                Vote::Down => "+ 0",
                                            },
                                            match v {
                                                Vote::Up => "+ 0",
                                                Vote::Down => "+ 1",
                                            },
                                            confidence,
                                        ),
                                        (comment,),
                                    )
                                })
                                .and_then(move |c| {
                                    // get all the stuff needed to compute updated hotness
                                    c.first_exec::<_, _, my::Row>(
                                        "SELECT `stories`.* \
                                         FROM `stories` \
                                         WHERE `stories`.`id` = ? \
                                         ORDER BY `stories`.`id` ASC LIMIT 1",
                                        (story,),
                                    ).map(|(c, story)| {
                                        let story = story.unwrap();
                                        (
                                            c,
                                            story.get::<u32, _>("user_id").unwrap(),
                                            story.get::<f64, _>("hotness").unwrap(),
                                        )
                                    })
                                })
                                .and_then(move |(t, story_author, score)| {
                                    t.drop_exec(
                                        "SELECT `tags`.* \
                                        FROM `tags` \
                                        INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                                        WHERE `taggings`.`story_id` = ?",
                                        (story,))
                                        .map(move |t| (t, story_author, score))
                                })
                                .and_then(move |(t, story_author, score)| {
                                    t.drop_exec(
                                        "SELECT \
                                         `comments`.`upvotes`, \
                                         `comments`.`downvotes` \
                                         FROM `comments` \
                                         WHERE `comments`.`story_id` = ? \
                                         AND user_id <> ?",
                                        (story, story_author),
                                    ).map(move |t| (t, score))
                                })
                                .and_then(move |(t, score)| {
                                    t.drop_exec(
                                        "SELECT `stories`.`id` \
                                         FROM `stories` \
                                         WHERE `stories`.`merged_story_id` = ?",
                                        (story,),
                                    ).map(move |t| (t, score))
                                })
                                .and_then(move |(t, score)| {
                                    // the *actual* algorithm for computing hotness isn't all
                                    // that interesting to us. it does affect what's on the
                                    // frontpage, but we're okay with using a more basic
                                    // upvote/downvote ratio thingy. See Story::calculated_hotness
                                    // in the lobsters source for details.
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE stories SET \
                                             upvotes = COALESCE(upvotes, 0) {}, \
                                             downvotes = COALESCE(downvotes, 0) {}, \
                                             hotness = '{}' \
                                             WHERE id = ?",
                                            match v {
                                                Vote::Up => "+ 1",
                                                Vote::Down => "+ 0",
                                            },
                                            match v {
                                                Vote::Up => "+ 0",
                                                Vote::Down => "+ 1",
                                            },
                                            score + match v {
                                                Vote::Up => 1.0,
                                                Vote::Down => -1.0,
                                            }
                                        ),
                                        (story,),
                                    )
                                })
                                .and_then(|t| t.commit())
                        }),
                )
            }
            LobstersRequest::Submit { id, user, title } => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(move |c| {
                            // check that tags exist
                            c.drop_query(
                                "SELECT  1 AS one FROM `tags` \
                                 INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                                 WHERE `taggings`.`story_id` IS NULL \
                                 AND (1=0) \
                                 AND `tags`.`tag` IN ('test') LIMIT 1",
                            )
                        })
                        .and_then(|c| {
                            // check that tags are active
                            c.first::<_, my::Row>(
                                "SELECT  `tags`.* FROM `tags` \
                                 WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test') \
                                 ORDER BY `tags`.`id` ASC LIMIT 1",
                            )
                        })
                        .map(|(c, tag)| (c, tag.unwrap().get::<u32, _>("id")))
                        .and_then(move |(c, tag)| {
                            // check that story id isn't already assigned
                            c.drop_exec(
                                "SELECT  1 AS one FROM `stories` \
                                 WHERE `stories`.`short_id` = ? LIMIT 1",
                                (::std::str::from_utf8(&id[..]).unwrap(),),
                            ).map(move |c| (c, tag))
                        })
                        .map(|c| {
                            // TODO: check for similar stories if there's a url
                            // SELECT  `stories`.*
                            // FROM `stories`
                            // WHERE `stories`.`url` IN (
                            //  'https://google.com/test',
                            //  'http://google.com/test',
                            //  'https://google.com/test/',
                            //  'http://google.com/test/',
                            //  ... etc
                            // )
                            // AND (is_expired = 0 OR is_moderated = 1)
                            // ORDER BY id DESC LIMIT 1
                            c
                        })
                        .map(|c| {
                            // TODO
                            // real impl queries `tags` and `users` again here..?
                            c
                        })
                        .and_then(move |(c, tag)| {
                            // TODO: real impl checks *new* short_id and duplicate urls *again*
                            // TODO: sometimes submit url
                            c.start_transaction(my::TransactionOptions::new())
                                .and_then(move |t| {
                                    t.prep_exec(
                                        "INSERT INTO `stories` \
                                         (`created_at`, `user_id`, `title`, \
                                         `description`, `short_id`, `upvotes`, `hotness`, \
                                         `markeddown_description`) \
                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (
                                            "2018-03-24 15:43:27",
                                            user,
                                            title,
                                            "to infinity", // lorem ipsum?
                                            ::std::str::from_utf8(&id[..]).unwrap(),
                                            1,
                                            -19216.2884921,
                                            "<p>to infinity</p>\n",
                                        ),
                                    )
                                })
                                .and_then(|q| {
                                    let story = q.last_insert_id().unwrap();
                                    q.drop_result().map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_exec(
                                        "INSERT INTO `taggings` (`story_id`, `tag_id`) \
                                         VALUES (?, ?)",
                                        (story, tag),
                                    ).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_query(&format!(
                                        "INSERT INTO keystores (`key`, `value`) \
                                         VALUES \
                                         ('user:{}:stories_submitted', 1) \
                                         ON DUPLICATE KEY UPDATE `value` = `value` + 1",
                                        user
                                    )).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_query(&format!(
                                        "SELECT  `keystores`.* \
                                         FROM `keystores` \
                                         WHERE `keystores`.`key` = 'user:{}:stories_submitted' \
                                         ORDER BY `keystores`.`key` ASC LIMIT 1",
                                        user
                                    )).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_exec(
                                        "SELECT  `votes`.* FROM `votes` \
                                         WHERE `votes`.`user_id` = ? \
                                         AND `votes`.`story_id` = ? \
                                         AND `votes`.`comment_id` IS NULL \
                                         ORDER BY `votes`.`id` ASC LIMIT 1",
                                        (user, story),
                                    ).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_exec(
                                        "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
                                         VALUES (?, ?, 1)",
                                        (user, story),
                                    ).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    t.drop_exec(
                                        "SELECT `comments`.`upvotes`, `comments`.`downvotes` \
                                         FROM `comments` \
                                         WHERE `comments`.`story_id` = ? \
                                         AND (user_id <> ?)",
                                        (story, user),
                                    ).map(move |t| (t, story))
                                })
                                .and_then(move |(t, story)| {
                                    // why oh why is story hotness *updated* here?!
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `stories` \
                                             SET `hotness` = {}
                                             WHERE `stories`.`id` = ?",
                                            -19216.5479744,
                                        ),
                                        (story,),
                                    )
                                })
                                .and_then(|t| t.commit())
                        }), // TODO: read_ribbons
                )
            }
            LobstersRequest::Comment {
                id,
                user,
                story,
                parent,
            } => {
                Box::new(
                    this.c
                        .get_conn()
                        .and_then(move |c| {
                            c.first_exec::<_, _, my::Row>(
                                "SELECT `stories`.* \
                                 FROM `stories` \
                                 WHERE `stories`.`short_id` = ? \
                                 ORDER BY `stories`.`id` ASC LIMIT 1",
                                (::std::str::from_utf8(&story[..]).unwrap(),),
                            ).map(|(c, story)| (c, story.unwrap()))
                        })
                        .and_then(|(c, story)| {
                            let author = story.get::<u32, _>("user_id").unwrap();
                            let hotness = story.get::<f64, _>("hotness").unwrap();
                            let id = story.get::<u32, _>("id").unwrap();
                            c.drop_exec(
                                "SELECT `users`.* FROM `users` WHERE `users`.`id` = ? LIMIT 1",
                                (author,),
                            ).map(move |c| (c, author, id, hotness))
                        })
                        .and_then(move |(c, author, story, hotness)| {
                            let fut = if let Some(parent) = parent {
                                // check that parent exists
                                futures::future::Either::A(c.first_exec::<_, _, my::Row>(
                                    "SELECT  `comments`.* FROM `comments` \
                                     WHERE `comments`.`story_id` = ? \
                                     AND `comments`.`short_id` = ? \
                                     ORDER BY `comments`.`id` ASC LIMIT 1",
                                    (story, ::std::str::from_utf8(&parent[..]).unwrap()),
                                ).map(|(c, parent)| {
                                    let parent = parent.unwrap();
                                    (
                                        c,
                                        Some((
                                            parent.get::<u32, _>("id"),
                                            parent.get::<u32, _>("thread_id"),
                                        )),
                                    )
                                }))
                            } else {
                                futures::future::Either::B(futures::future::ok((c, None)))
                            };
                            fut.map(move |(c, parent)| (c, author, story, parent, hotness))
                        })
                        .map(|c| {
                            // TODO: real site checks for recent comments by same author with same
                            // parent to ensure we don't double-post accidentally
                            c
                        })
                        .and_then(move |(c, author, story, parent, hotness)| {
                            // check that short id is available
                            c.drop_exec(
                                "SELECT  1 AS one FROM `comments` \
                                 WHERE `comments`.`short_id` = ? LIMIT 1",
                                (::std::str::from_utf8(&id[..]).unwrap(),),
                            ).map(move |c| (c, author, story, parent, hotness))
                        })
                        .and_then(move |(c, author, story, parent, hotness)| {
                            // TODO: real impl checks *new* short_id *again*
                            c.start_transaction(my::TransactionOptions::new())
                                .and_then(move |t| {
                                    if let Some((parent, thread)) = parent {
                                        futures::future::Either::A(t.prep_exec(
                                            "INSERT INTO `comments` \
                                             (`created_at`, `updated_at`, `short_id`, `story_id`, \
                                             `user_id`, `parent_comment_id`, `thread_id`, \
                                             `comment`, `upvotes`, `confidence`, \
                                             `markeddown_comment`) \
                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                            (
                                                "2018-03-24 15:43:27",
                                                "2018-03-24 15:43:27",
                                                ::std::str::from_utf8(&id[..]).unwrap(),
                                                story,
                                                user,
                                                parent,
                                                thread,
                                                "moar benchmarking", // lorem ipsum?
                                                1,
                                                0.1828847834138887,
                                                "<p>moar benchmarking</p>\n",
                                            ),
                                        ))
                                    } else {
                                        futures::future::Either::B(t.prep_exec(
                                            "INSERT INTO `comments` \
                                             (`created_at`, `updated_at`, `short_id`, `story_id`, \
                                             `user_id`, `comment`, `upvotes`, `confidence`, \
                                             `markeddown_comment`) \
                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                            (
                                                "2018-03-24 15:43:27",
                                                "2018-03-24 15:43:27",
                                                ::std::str::from_utf8(&id[..]).unwrap(),
                                                story,
                                                user,
                                                "moar benchmarking", // lorem ipsum?
                                                1,
                                                0.1828847834138887,
                                                "<p>moar benchmarking</p>\n",
                                            ),
                                        ))
                                    }
                                })
                                .and_then(|q| {
                                    let comment = q.last_insert_id().unwrap();
                                    q.drop_result().map(move |t| (t, comment))
                                })
                                .and_then(move |(t, comment)| {
                                    // but why?!
                                    t.drop_exec(
                                        "SELECT  `votes`.* FROM `votes` \
                                         WHERE `votes`.`user_id` = ? \
                                         AND `votes`.`story_id` = ? \
                                         AND `votes`.`comment_id` = ? \
                                         ORDER BY `votes`.`id` ASC LIMIT 1",
                                        (user, story, comment),
                                    ).map(move |t| (t, comment))
                                })
                                .and_then(move |(t, comment)| {
                                    t.drop_exec(
                                        "INSERT INTO `votes` \
                                         (`user_id`, `story_id`, `comment_id`, `vote`) \
                                         VALUES (?, ?, ?, 1)",
                                        (user, story, comment),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "SELECT `stories`.`id` \
                                         FROM `stories` \
                                         WHERE `stories`.`merged_story_id` = ?",
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    // why are these ordered?
                                    t.prep_exec(
                                        "SELECT `comments`.* \
                                         FROM `comments` \
                                         WHERE `comments`.`story_id` = ? \
                                         ORDER BY \
                                         (upvotes - downvotes) < 0 ASC, \
                                         confidence DESC",
                                        (story,),
                                    ).and_then(|q| q.reduce_and_drop(0, |rows, _| rows + 1))
                                })
                                .and_then(move |(t, count)| {
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `stories` \
                                             SET `comments_count` = {}
                                             WHERE `stories`.`id` = ?",
                                            count,
                                        ),
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    // get all the stuff needed to compute updated hotness
                                    t.drop_exec(
                                        "SELECT `tags`.* \
                                         FROM `tags` \
                                         INNER JOIN `taggings` \
                                         ON `tags`.`id` = `taggings`.`tag_id` \
                                         WHERE `taggings`.`story_id` = ?",
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "SELECT \
                                         `comments`.`upvotes`, \
                                         `comments`.`downvotes` \
                                         FROM `comments` \
                                         WHERE `comments`.`story_id` = ? \
                                         AND user_id <> ?",
                                        (story, author),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_exec(
                                        "SELECT `stories`.`id` \
                                         FROM `stories` \
                                         WHERE `stories`.`merged_story_id` = ?",
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    // why oh why is story hotness *updated* here?!
                                    t.drop_exec(
                                        &format!(
                                            "UPDATE `stories` \
                                             SET `hotness` = {}
                                             WHERE `stories`.`id` = ?",
                                            hotness,
                                        ),
                                        (story,),
                                    )
                                })
                                .and_then(move |t| {
                                    t.drop_query(&format!(
                                        "INSERT INTO keystores (`key`, `value`) \
                                         VALUES \
                                         ('user:{}:comments_posted', 1) \
                                         ON DUPLICATE KEY UPDATE `value` = `value` + 1",
                                        user
                                    ))
                                })
                                .and_then(move |t| {
                                    t.drop_query(&format!(
                                        "SELECT  `keystores`.* \
                                         FROM `keystores` \
                                         WHERE `keystores`.`key` = 'user:{}:comments_posted' \
                                         ORDER BY `keystores`.`key` ASC LIMIT 1",
                                        user
                                    ))
                                    // TODO: technically it also selects from users for the
                                    // author of the parent comment here..
                                })
                                .and_then(|t| t.commit())
                        }),
                )
            }
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
    let mut s = MysqlSpawner::new(my::OptsBuilder::from_opts(args.value_of("dbn").unwrap()));
    let mut core = tokio_core::reactor::Core::new().unwrap();
    use trawler::LobstersClient;
    let c = Rc::new(MysqlTrawler::spawn(&mut s, &core.handle()));
    core.run(MysqlTrawler::handle(c, LobstersRequest::Frontpage))
        .unwrap();

    wl.run::<MysqlTrawler, _>(s, args.is_present("prime"));
}
