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
    // rustfmt
    //
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the `NOW()` hack to support dbs primed a while ago
    let main = c.and_then(|c| {
        c.query(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` IS NULL \
             AND `stories`.`is_expired` = 0 \
             AND CAST(upvotes AS signed) - CAST(downvotes AS signed) <= 5 \
             ORDER BY stories.id DESC LIMIT 51",
        )
    }).and_then(|stories| {
            stories.reduce_and_drop(
                (HashSet::new(), HashSet::new()),
                |(mut users, mut stories), story| {
                    users.insert(story.get::<u32, _>("user_id").unwrap());
                    stories.insert(story.get::<u32, _>("id").unwrap());
                    (users, stories)
                },
            )
        })
        .and_then(move |(c, (users, stories))| {
            if stories.is_empty() {
                panic!("got no stories from /recent");
            }

            let stories_in = stories
                .iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");

            match acting_as {
                Some(uid) => Either::A(
                    c.drop_exec(
                        "SELECT `hidden_stories`.`story_id` \
                         FROM `hidden_stories` \
                         WHERE `hidden_stories`.`user_id` = ?",
                        (uid,),
                    ).and_then(move |c| {
                            c.prep_exec(
                                "SELECT `tag_filters`.* FROM `tag_filters` \
                                 WHERE `tag_filters`.`user_id` = ?",
                                (uid,),
                            )
                        })
                        .and_then(|tags| {
                            tags.reduce_and_drop(Vec::new(), |mut tags, tag| {
                                tags.push(tag.get::<u32, _>("tag_id").unwrap());
                                tags
                            })
                        })
                        .and_then(move |(c, tags)| {
                            if tags.is_empty() {
                                return Either::A(future::ok((c, (users, stories_in, stories))));
                            }

                            let s = stories
                                .iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(",");
                            let tags = tags.into_iter()
                                .map(|id| format!("{}", id))
                                .collect::<Vec<_>>()
                                .join(",");

                            Either::B(c.drop_query(format!(
                                "SELECT `taggings`.`story_id` \
                                 FROM `taggings` \
                                 WHERE `taggings`.`story_id` IN ({}) \
                                 AND `taggings`.`tag_id` IN ({})",
                                s, tags
                            )).map(move |c| (c, (users, stories_in, stories))))
                        }),
                ),
                None => Either::B(future::ok((c, (users, stories_in, stories)))),
            }
        })
        .and_then(|(c, (users, stories_in, stories))| {
            let users = users
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
            c.drop_query(&format!(
                "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
                users
            )).map(move |c| (c, stories_in, stories))
        })
        .and_then(|(c, stories_in, stories)| {
            c.drop_query(&format!(
                "SELECT `suggested_titles`.* \
                 FROM `suggested_titles` \
                 WHERE `suggested_titles`.`story_id` IN ({})",
                stories_in
            )).map(move |c| (c, stories_in, stories))
        })
        .and_then(|(c, stories_in, stories)| {
            c.drop_query(&format!(
                "SELECT `suggested_taggings`.* \
                 FROM `suggested_taggings` \
                 WHERE `suggested_taggings`.`story_id` IN ({})",
                stories_in
            )).map(move |c| (c, stories_in, stories))
        })
        .and_then(|(c, stories_in, stories)| {
            c.query(&format!(
                "SELECT `taggings`.* \
                 FROM `taggings` \
                 WHERE `taggings`.`story_id` IN ({})",
                stories_in
            )).map(move |t| (t, stories))
        })
        .and_then(|(taggings, stories)| {
            taggings
                .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                    tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
                    tags
                })
                .map(move |x| (x, stories))
        })
        .and_then(|((c, tags), stories)| {
            let tags = tags.into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(", ");
            c.drop_query(&format!(
                "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                tags
            )).map(move |c| (c, stories))
        });

    // also load things that we need to highlight
    Box::new(
        match acting_as {
            None => Either::A(main.map(|(c, _)| c)),
            Some(uid) => Either::B(
                main.and_then(move |(c, stories)| {
                    let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    let values: Vec<_> = iter::once(&uid as &_)
                        .chain(stories.iter().map(|s| s as &_))
                        .collect();
                    c.drop_exec(
                        &format!(
                            "SELECT `votes`.* FROM `votes` \
                             WHERE `votes`.`user_id` = ? \
                             AND `votes`.`story_id` IN ({}) \
                             AND `votes`.`comment_id` IS NULL",
                            story_params
                        ),
                        values,
                    ).map(move |c| (c, story_params, stories))
                }).and_then(move |(c, story_params, stories)| {
                        let values: Vec<_> = iter::once(&uid as &_)
                            .chain(stories.iter().map(|s| s as &_))
                            .collect();
                        c.drop_exec(
                            &format!(
                                "SELECT `hidden_stories`.* \
                                 FROM `hidden_stories` \
                                 WHERE `hidden_stories`.`user_id` = ? \
                                 AND `hidden_stories`.`story_id` IN ({})",
                                story_params
                            ),
                            values,
                        ).map(move |c| (c, story_params, stories))
                    })
                    .and_then(move |(c, story_params, stories)| {
                        let values: Vec<_> = iter::once(&uid as &_)
                            .chain(stories.iter().map(|s| s as &_))
                            .collect();
                        c.drop_exec(
                            &format!(
                                "SELECT `saved_stories`.* \
                                 FROM `saved_stories` \
                                 WHERE `saved_stories`.`user_id` = ? \
                                 AND `saved_stories`.`story_id` IN ({})",
                                story_params
                            ),
                            values,
                        )
                    }),
            ),
        }.map(|c| (c, true)),
    )
}
