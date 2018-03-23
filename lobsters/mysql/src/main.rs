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
                        })
                        .map_err(|e| {
                            eprintln!("{:?}", e);
                        })
                        .map(move |_| sent.elapsed()),
                )
            }
            LobstersRequest::StoryVote(user, story, v) => {
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
                                 UPDATE keystores SET value = 100 WHERE keystores.key = 'traffic:hits'; \
                                 UPDATE keystores SET value = 1521590012 WHERE keystores.key = 'traffic:date';",
                            )
                                })
                                .and_then(|t| t.commit())
                        })
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
                        })
                        .map_err(|e| {
                            eprintln!("{:?}", e);
                        })
                        .map(move |_| sent.elapsed()),
                )
            }
            LobstersRequest::CommentVote(user, comment, v) => {
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
                        .and_then(move |c| {
                            c.prep_exec(
                                "SELECT `comments`.* \
                                 FROM `comments` \
                                 WHERE `comments`.`short_id` = ? \
                                 ORDER BY `comments`.`id` ASC LIMIT 1",
                                (::std::str::from_utf8(&comment[..]).unwrap(),),
                            ).and_then(|result| result.collect_and_drop::<my::Row>())
                                .map(|(c, mut comment)| (c, comment.swap_remove(0)))
                        })
                        .and_then(move |(c, comment)| {
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
                        .and_then(move |(c, author, story, comment, upvotes, downvotes)| {
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
                                             WHERE `users`.`id` = ?",
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
                                        (author,),
                                    )
                                })
                                .and_then(move |c| {
                                    // get all the stuff needed to compute updated hotness
                                    c.prep_exec(
                                        "SELECT `stories`.* \
                                         FROM `stories` \
                                         WHERE `stories`.`id` = ? \
                                         ORDER BY `stories`.`id` ASC LIMIT 1",
                                        (story,),
                                    ).and_then(|result| result.collect_and_drop::<my::Row>())
                                        .map(|(c, mut story)| {
                                            let story = story.swap_remove(0);
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
                        })
                        .map_err(|e| {
                            eprintln!("{:?}", e);
                        })
                        .map(move |_| sent.elapsed()),
                )
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
