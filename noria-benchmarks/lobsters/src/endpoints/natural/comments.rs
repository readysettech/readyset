use futures;
use futures::future::{self, Either};
use futures::Future;
use my;
use my::prelude::*;
use std::collections::HashSet;
use std::iter;
use trawler::UserId;

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Box<Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    Box::new(
        c.and_then(move |c| {
            c.query(
                "SELECT  `comments`.id \
                 FROM `comments` \
                 WHERE `comments`.`is_deleted` = 0 \
                 AND `comments`.`is_moderated` = 0 \
                 ORDER BY id DESC \
                 LIMIT 40 OFFSET 0",
            )
        })
        .and_then(|res| {
            res.reduce_and_drop(Vec::new(), |mut xs, x| {
                xs.push(x.get::<u32, _>("id").unwrap());
                xs
            })
        })
        .and_then(move |(c, ids)| {
            let cids = ids
                .iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");

            c.query(&format!(
                "SELECT `comment_with_votes`.* \
                 FROM `comment_with_votes` \
                 WHERE comment_with_votes.id IN ({})",
                cids
            ))
        })
        .and_then(|comments| {
            comments.reduce_and_drop(
                (Vec::new(), HashSet::new(), HashSet::new()),
                |(mut comments, mut users, mut stories), comment| {
                    comments.push(comment.get::<u32, _>("id").unwrap());
                    users.insert(comment.get::<u32, _>("user_id").unwrap());
                    stories.insert(comment.get::<u32, _>("story_id").unwrap());
                    (comments, users, stories)
                },
            )
        })
        .and_then(move |(c, (comments, users, stories))| match acting_as {
            None => Either::A(future::ok((c, (comments, users, stories)))),
            Some(uid) => {
                let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let fut = {
                    let args: Vec<_> = iter::once(&uid as &_)
                        .chain(stories.iter().map(|c| c as &_))
                        .collect();
                    c.drop_exec(
                        &format!(
                            "SELECT 1 FROM hidden_stories \
                             WHERE user_id = ? \
                             AND hidden_stories.story_id IN ({})",
                            params
                        ),
                        args,
                    )
                };
                Either::B(fut.map(move |c| (c, (comments, users, stories))))
            }
        })
        .and_then(|(c, (comments, users, stories))| {
            let users = users
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
            c.drop_query(&format!(
                "SELECT `users`.* FROM `users` \
                 WHERE `users`.`id` IN ({})",
                users
            ))
            .map(move |c| (c, comments, stories))
        })
        .and_then(|(c, comments, stories)| {
            let stories = stories
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
            c.query(&format!(
                "SELECT  `story_with_votes`.* FROM `story_with_votes` \
                 WHERE `story_with_votes`.`id` IN ({})",
                stories
            ))
            .map(move |stories| (stories, comments))
        })
        .and_then(|(stories, comments)| {
            stories
                .reduce_and_drop(HashSet::new(), |mut authors, story| {
                    authors.insert(story.get::<u32, _>("user_id").unwrap());
                    authors
                })
                .map(move |(c, authors)| (c, authors, comments))
        })
        .and_then(move |(c, authors, comments)| match acting_as {
            None => Either::A(futures::future::ok((c, authors))),
            Some(uid) => {
                let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let comments: Vec<_> = iter::once(&uid as &_)
                    .chain(comments.iter().map(|c| c as &_))
                    .collect();
                Either::B(
                    c.drop_exec(
                        &format!(
                            "SELECT `votes`.* FROM `votes` \
                             WHERE `votes`.`user_id` = ? \
                             AND `votes`.`comment_id` IN ({})",
                            params
                        ),
                        comments,
                    )
                    .map(move |c| (c, authors)),
                )
            }
        })
        .and_then(|(c, authors)| {
            // NOTE: the real website issues all of these one by one...
            let authors = authors
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
            c.drop_query(&format!(
                "SELECT  `users`.* FROM `users` \
                 WHERE `users`.`id` IN ({})",
                authors
            ))
        })
        .map(|c| (c, true)),
    )
}
