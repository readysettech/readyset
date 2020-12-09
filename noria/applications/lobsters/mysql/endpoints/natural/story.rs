use chrono;
use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;
    let (mut c, mut story) = c
        .prep_exec(
            "SELECT `story_with_votes`.* \
             FROM `story_with_votes` \
             WHERE `story_with_votes`.`short_id` = ?",
            (::std::str::from_utf8(&id[..]).unwrap(),),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c = c
        .drop_exec(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
            (author,),
        )
        .await?;
    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let (x, rr) = c
            .first_exec::<_, _, my::Row>(
                "SELECT  `read_ribbons`.* \
                     FROM `read_ribbons` \
                     WHERE `read_ribbons`.`user_id` = ? \
                     AND `read_ribbons`.`story_id` = ?",
                (&uid, &story),
            )
            .await?;
        let now = chrono::Local::now().naive_local();
        c = match rr {
            None => {
                x.drop_exec(
                    "INSERT INTO `read_ribbons` \
                         (`created_at`, `updated_at`, `user_id`, `story_id`) \
                         VALUES (?, ?, ?, ?)",
                    (now, now, uid, story),
                )
                .await?
            }
            Some(rr) => {
                x.drop_exec(
                    "UPDATE `read_ribbons` \
                         SET `read_ribbons`.`updated_at` = ? \
                         WHERE `read_ribbons`.`id` = ?",
                    (now, rr.get::<u32, _>("id").unwrap()),
                )
                .await?
            }
        };
    }

    // XXX: probably not drop here, but we know we have no merged stories
    c = c
        .drop_exec(
            "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
            (story,),
        )
        .await?;

    let comments = c
        .prep_exec(
            "SELECT `comment_with_votes`.* \
             FROM `comment_with_votes` \
             WHERE `comment_with_votes`.`story_id` = ? \
             ORDER BY \
             comment_with_votes.score DESC",
            (story,),
        )
        .await?;

    let (mut c, (users, comments)) = comments
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut comments), comment| {
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                comments.insert(comment.get::<u32, _>("id").unwrap());
                (users, comments)
            },
        )
        .await?;

    // get user info for all commenters
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c = c
        .drop_query(&format!(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
            users
        ))
        .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<_> = iter::once(&uid as &_)
            .chain(comments.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`comment_id` IN ({})",
                    params
                ),
                values,
            )
            .await?;
    }

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        c = c
            .drop_exec(
                "SELECT `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` IS NULL",
                (uid, story),
            )
            .await?;
        c = c
            .drop_exec(
                "SELECT `hidden_stories`.* \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = ? \
                 AND `hidden_stories`.`story_id` = ?",
                (uid, story),
            )
            .await?;
        c = c
            .drop_exec(
                "SELECT `saved_stories`.* \
                 FROM `saved_stories` \
                 WHERE `saved_stories`.`user_id` = ? \
                 AND `saved_stories`.`story_id` = ?",
                (uid, story),
            )
            .await?;
    }

    let taggings = c
        .prep_exec(
            "SELECT `taggings`.* \
             FROM `taggings` \
             WHERE `taggings`.`story_id` = ?",
            (story,),
        )
        .await?;

    let (c, tags) = taggings
        .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    let c = c
        .drop_query(&format!(
            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
            tags
        ))
        .await?;

    Ok((c, true))
}
