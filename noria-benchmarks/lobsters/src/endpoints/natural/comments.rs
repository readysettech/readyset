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
    let res = c
        .query(
            "SELECT  `comments`.id \
             FROM `comments` \
             WHERE `comments`.`is_deleted` = 0 \
             AND `comments`.`is_moderated` = 0 \
             ORDER BY id DESC \
             LIMIT 40 OFFSET 0",
        )
        .await?;

    let (c, ids) = res
        .reduce_and_drop(Vec::new(), |mut xs, x| {
            xs.push(x.get::<u32, _>("id").unwrap());
            xs
        })
        .await?;

    let cids = ids
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let comments = c
        .query(&format!(
            "SELECT `comment_with_votes`.* \
             FROM `comment_with_votes` \
             WHERE comment_with_votes.id IN ({})",
            cids
        ))
        .await?;

    let (mut c, (comments, users, stories)) = comments
        .reduce_and_drop(
            (Vec::new(), HashSet::new(), HashSet::new()),
            |(mut comments, mut users, mut stories), comment| {
                comments.push(comment.get::<u32, _>("id").unwrap());
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                stories.insert(comment.get::<u32, _>("story_id").unwrap());
                (comments, users, stories)
            },
        )
        .await?;

    if let Some(uid) = acting_as {
        let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let args: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|c| c as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT 1 FROM hidden_stories \
                     WHERE user_id = ? \
                     AND hidden_stories.story_id IN ({})",
                    params
                ),
                args,
            )
            .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c = c
        .drop_query(&format!(
            "SELECT `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
            users
        ))
        .await?;

    let stories = stories
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let stories = c
        .query(&format!(
            "SELECT  `story_with_votes`.* FROM `story_with_votes` \
             WHERE `story_with_votes`.`id` IN ({})",
            stories
        ))
        .await?;

    let (mut c, authors) = stories
        .reduce_and_drop(HashSet::new(), |mut authors, story| {
            authors.insert(story.get::<u32, _>("user_id").unwrap());
            authors
        })
        .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let comments: Vec<_> = iter::once(&uid as &_)
            .chain(comments.iter().map(|c| c as &_))
            .collect();

        c = c
            .drop_exec(
                &format!(
                    "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`comment_id` IN ({})",
                    params
                ),
                comments,
            )
            .await?;
    }

    // NOTE: the real website issues all of these one by one...
    let authors = authors
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    c = c
        .drop_query(&format!(
            "SELECT  `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
            authors
        ))
        .await?;

    Ok((c, true))
}
