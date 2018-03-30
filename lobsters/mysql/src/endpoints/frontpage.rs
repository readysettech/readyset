use futures::Future;
use futures::future::Either;
use my;
use my::prelude::*;
use std::collections::HashSet;
use trawler::UserId;

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    let initial = match acting_as {
        Some(uid) => Either::A(
            c.and_then(move |c| {
                c.query(&format!(
                    "SELECT `tag_filters`.* FROM `tag_filters` \
                     WHERE `tag_filters`.`user_id` = {}",
                    uid
                ))
            }).and_then(|tags| {
                    tags.reduce_and_drop(Vec::new(), |mut tags, tag| {
                        tags.push(tag.get::<u32, _>("tag_id").unwrap());
                        tags
                    })
                })
                .and_then(move |(c, tags)| {
                    let tags = tags.into_iter()
                        .map(|id| format!("{}", id))
                        .collect::<Vec<_>>()
                        .join(",");
                    c.query(&format!(
                        "SELECT  `stories`.* FROM `stories` \
                         WHERE `stories`.`merged_story_id` IS NULL \
                         AND `stories`.`is_expired` = 0 \
                         AND ((\
                         CAST(upvotes AS signed) \
                         - \
                         CAST(downvotes AS signed))\
                         >= 0) \
                         AND (`stories`.`id` NOT IN \
                         (SELECT `hidden_stories`.`story_id` \
                         FROM `hidden_stories` \
                         WHERE `hidden_stories`.`user_id` = {}\
                         )\
                         ) {} \
                         ORDER BY hotness LIMIT 26 OFFSET 0",
                        uid,
                        if tags.is_empty() {
                            String::from("")
                        } else {
                            // should probably just inline tag_filters here instead
                            format!(
                                " AND (`stories`.`id` NOT IN (\
                                 SELECT `taggings`.`story_id` \
                                 FROM `taggings` \
                                 WHERE `taggings`.`tag_id` IN ({}) \
                                 )) ",
                                tags
                            )
                        },
                    ))
                }),
        ),
        None => Either::B(c.and_then(|c| {
            c.query(
                "SELECT  `stories`.* FROM `stories` \
                 WHERE `stories`.`merged_story_id` IS NULL \
                 AND `stories`.`is_expired` = 0 \
                 AND ((CAST(upvotes AS signed) - CAST(downvotes AS signed)) >= 0) \
                 ORDER BY hotness LIMIT 26 OFFSET 0",
            )
        })),
    };

    let main = initial
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
                "SELECT `taggings`.* FROM `taggings` \
                 WHERE `taggings`.`story_id` IN ({})",
                stories
            )).map(move |t| (t, stories))
        })
        .and_then(|(taggings, stories)| {
            taggings
                .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
                    tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
                    tags
                })
                .map(|x| (x, stories))
        })
        .and_then(|((c, tags), stories)| {
            let tags = tags.into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
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
                    c.drop_query(&format!(
                        "SELECT `votes`.* FROM `votes` \
                         WHERE `votes`.`user_id` = {} \
                         AND `votes`.`story_id` IN ({}) \
                         AND `votes`.`comment_id` IS NULL",
                        uid, stories,
                    )).map(move |c| (c, stories))
                }).and_then(move |(c, stories)| {
                        c.drop_query(&format!(
                            "SELECT `hidden_stories`.* \
                             FROM `hidden_stories` \
                             WHERE `hidden_stories`.`user_id` = {} \
                             AND `hidden_stories`.`story_id` IN ({})",
                            uid, stories,
                        )).map(move |c| (c, stories))
                    })
                    .and_then(move |(c, stories)| {
                        c.drop_query(&format!(
                            "SELECT `saved_stories`.* \
                             FROM `saved_stories` \
                             WHERE `saved_stories`.`user_id` = {} \
                             AND `saved_stories`.`story_id` IN ({})",
                            uid, stories,
                        ))
                    }),
            ),
        }.map(|c| (c, true)),
    )
}
