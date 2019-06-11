use chrono;
use futures;
use futures::Future;
use my;
use my::prelude::*;
use trawler::{CommentId, StoryId, UserId};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: CommentId,
    story: StoryId,
    parent: Option<CommentId>,
) -> Box<Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(move |c| {
            c.first_exec::<_, _, my::Row>(
                "SELECT `stories`.* \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = ?",
                (::std::str::from_utf8(&story[..]).unwrap(),),
            )
            .map(|(c, story)| (c, story.unwrap()))
        })
        .and_then(|(c, story)| {
            let author = story.get::<u32, _>("user_id").unwrap();
            let id = story.get::<u32, _>("id").unwrap();
            c.drop_exec(
                "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
                (author,),
            )
            .map(move |c| (c, id))
        })
        .and_then(move |(c, story)| {
            let fut = if let Some(parent) = parent {
                // check that parent exists
                futures::future::Either::A(
                    c.first_exec::<_, _, my::Row>(
                        "SELECT  `comments`.* FROM `comments` \
                         WHERE `comments`.`story_id` = ? \
                         AND `comments`.`short_id` = ?",
                        (story, ::std::str::from_utf8(&parent[..]).unwrap()),
                    )
                    .map(move |(c, p)| {
                        if let Some(p) = p {
                            (
                                c,
                                Some((
                                    p.get::<u32, _>("id").unwrap(),
                                    p.get::<Option<u32>, _>("thread_id").unwrap(),
                                )),
                            )
                        } else {
                            eprintln!(
                                "failed to find parent comment {} in story {}",
                                ::std::str::from_utf8(&parent[..]).unwrap(),
                                story
                            );
                            (c, None)
                        }
                    }),
                )
            } else {
                futures::future::Either::B(futures::future::ok((c, None)))
            };
            fut.map(move |(c, parent)| (c, story, parent))
        })
        .map(|c| {
            // TODO: real site checks for recent comments by same author with same
            // parent to ensure we don't double-post accidentally
            c
        })
        .and_then(move |(c, story, parent)| {
            // check that short id is available
            c.drop_exec(
                "SELECT  1 AS one FROM `comments` \
                 WHERE `comments`.`short_id` = ?",
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .map(move |c| (c, story, parent))
        })
        .and_then(move |(c, story, parent)| {
            // TODO: real impl checks *new* short_id *again*

            // NOTE: MySQL technically does everything inside this and_then in a transaction,
            // but let's be nice to it
            let now = chrono::Local::now().naive_local();
            if let Some((parent, thread)) = parent {
                futures::future::Either::A(c.prep_exec(
                    "INSERT INTO `comments` \
                     (`created_at`, `updated_at`, `short_id`, `story_id`, \
                     `user_id`, `parent_comment_id`, `thread_id`, \
                     `comment`, `markeddown_comment`) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        now,
                        now,
                        ::std::str::from_utf8(&id[..]).unwrap(),
                        story,
                        user,
                        parent,
                        thread,
                        "moar benchmarking", // lorem ipsum?
                        "<p>moar benchmarking</p>\n",
                    ),
                ))
            } else {
                futures::future::Either::B(c.prep_exec(
                    "INSERT INTO `comments` \
                     (`created_at`, `updated_at`, `short_id`, `story_id`, \
                     `user_id`, `comment`, `markeddown_comment`) \
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        now,
                        now,
                        ::std::str::from_utf8(&id[..]).unwrap(),
                        story,
                        user,
                        "moar benchmarking", // lorem ipsum?
                        "<p>moar benchmarking</p>\n",
                    ),
                ))
            }
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
                     AND `votes`.`comment_id` = ?",
                    (user, story, comment),
                )
                .map(move |t| (t, comment))
            })
            .and_then(move |(t, comment)| {
                t.drop_exec(
                    "INSERT INTO `votes` \
                     (`user_id`, `story_id`, `comment_id`, `vote`) \
                     VALUES (?, ?, ?, ?)",
                    (user, story, comment, 1),
                )
            })
        })
        .map(|c| (c, false)),
    )
}
