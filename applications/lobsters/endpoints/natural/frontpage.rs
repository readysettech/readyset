use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let (c, stories) = c
        .query("SELECT `frontpage_ids`.id FROM `frontpage_ids`")
        .await?
        .reduce_and_drop(Vec::new(), |mut xs, x| {
            xs.push(x.get::<u32, _>("id").unwrap());
            xs
        })
        .await?;

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    // NOTE: the filters should be *before* the topk
    let res = c
        .query(&format!(
            "SELECT `story_with_votes`.* \
             FROM `story_with_votes` \
             WHERE `story_with_votes`.`id` IN ({}) \
             AND `story_with_votes`.`merged_story_id` IS NULL \
             AND `story_with_votes`.`is_expired` = 0 \
             AND `story_with_votes`.`score` >= 0",
            stories_in
        ))
        .await?;
    let (mut c, users) = res
        .reduce_and_drop(HashSet::new(), |mut users, story| {
            users.insert(story.get::<u32, _>("user_id").unwrap());
            users
        })
        .await?;

    if let Some(uid) = acting_as {
        c = c
            .drop_exec(
                "SELECT `tag_filters`.* FROM `tag_filters` \
                 WHERE `tag_filters`.`user_id` = ?",
                (uid,),
            )
            .await?;

        c = c
            .drop_query(format!(
                "SELECT `taggings`.`story_id` \
                 FROM `taggings` \
                 WHERE `taggings`.`story_id` IN ({})",
                // AND `taggings`.`tag_id` IN ({})",
                stories_in,
                //tags
            ))
            .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c = c
        .drop_query(&format!(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
            users,
        ))
        .await?;

    c = c
        .drop_query(&format!(
            "SELECT `suggested_titles`.* \
             FROM `suggested_titles` \
             WHERE `suggested_titles`.`story_id` IN ({})",
            stories_in
        ))
        .await?;

    c = c
        .drop_query(&format!(
            "SELECT `suggested_taggings`.* \
             FROM `suggested_taggings` \
             WHERE `suggested_taggings`.`story_id` IN ({})",
            stories_in
        ))
        .await?;

    let taggings = c
        .query(&format!(
            "SELECT `taggings`.* FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
            stories_in
        ))
        .await?;

    let (mut c, tags) = taggings
        .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c = c
        .drop_query(&format!(
            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
            tags
        ))
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`story_id` IN ({}) \
                     AND `votes`.`comment_id` IS NULL",
                    story_params
                ),
                values,
            )
            .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT `hidden_stories`.* \
                     FROM `hidden_stories` \
                     WHERE `hidden_stories`.`user_id` = ? \
                     AND `hidden_stories`.`story_id` IN ({})",
                    story_params
                ),
                values,
            )
            .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT `saved_stories`.* \
                     FROM `saved_stories` \
                     WHERE `saved_stories`.`user_id` = ? \
                     AND `saved_stories`.`story_id` IN ({})",
                    story_params
                ),
                values,
            )
            .await?;
    }

    Ok((c, true))
}
