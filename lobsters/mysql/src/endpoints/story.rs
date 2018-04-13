use chrono;
use futures;
use futures::Future;
use futures::future::Either;
use my;
use my::prelude::*;
use std::collections::HashSet;
use trawler::{StoryId, UserId};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    Box::new(
        c.and_then(move |c| {
            c.prep_exec(
                "SELECT `stories`.* \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = ?",
                (::std::str::from_utf8(&id[..]).unwrap(),),
            ).and_then(|result| result.collect_and_drop::<my::Row>())
                .map(|(c, mut story)| (c, story.swap_remove(0)))
        }).and_then(|(c, story)| {
                let author = story.get::<u32, _>("user_id").unwrap();
                let id = story.get::<u32, _>("id").unwrap();
                c.drop_exec(
                    "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
                    (author,),
                ).map(move |c| (c, id))
            })
            .and_then(move |(c, story)| {
                // NOTE: technically this happens before the select from user...
                match acting_as {
                    None => Either::A(futures::future::ok(c)),
                    Some(uid) => {
                        // keep track of when the user last saw this story
                        // NOTE: *technically* the update only happens at the end...
                        Either::B(c.first_exec::<_, _, my::Row>(
                            "SELECT  `read_ribbons`.* \
                             FROM `read_ribbons` \
                             WHERE `read_ribbons`.`user_id` = ? \
                             AND `read_ribbons`.`story_id` = ?",
                            (&uid, &story),
                        ).and_then(move |(c, rr)| {
                            let now = chrono::Local::now().naive_local();
                            match rr {
                                None => Either::A(c.drop_exec(
                                    "INSERT INTO \
                                     `read_ribbons` \
                                     (`created_at`, \
                                     `updated_at`, \
                                     `user_id`, \
                                     `story_id`) \
                                     VALUES (?, \
                                     ?, \
                                     ?, \
                                     ?)",
                                    (now, now, uid, story),
                                )),
                                Some(rr) => Either::B(c.drop_exec(
                                    format!(
                                        "UPDATE `read_ribbons` \
                                         SET \
                                         `read_ribbons`.`updated_at` \
                                         = {} \
                                         WHERE \
                                         `read_ribbons`.`id` = ?",
                                        now
                                    ),
                                    (rr.get::<u32, _>("id").unwrap(),),
                                )),
                            }
                        }))
                    }
                }.map(move |c| (c, story))
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
                    "SELECT `comments`.*, \
                     `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
                     FROM `comments` \
                     WHERE `comments`.`story_id` = ? \
                     ORDER BY \
                     saldo ASC, \
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
            .and_then(move |(c, story)| match acting_as {
                None => Either::A(futures::future::ok((c, story))),
                Some(uid) => Either::B(
                    c.drop_exec(
                        "SELECT `votes`.* \
                         FROM `votes` \
                         WHERE `votes`.`user_id` = ? \
                         AND `votes`.`story_id` = ? \
                         AND `votes`.`comment_id` IS NULL",
                        (uid, story),
                    ).and_then(move |c| {
                            c.drop_exec(
                                "SELECT `hidden_stories`.* \
                                 FROM `hidden_stories` \
                                 WHERE `hidden_stories`.`user_id` = ? \
                                 AND `hidden_stories`.`story_id` = ?",
                                (uid, story),
                            )
                        })
                        .and_then(move |c| {
                            c.drop_exec(
                                "SELECT `saved_stories`.* \
                                 FROM `saved_stories` \
                                 WHERE `saved_stories`.`user_id` = ? \
                                 AND `saved_stories`.`story_id` = ?",
                                (uid, story),
                            )
                        })
                        .map(move |c| (c, story)),
                ),
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
            .map(|c| (c, true)),
    )
}
