use chrono;
use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    // check that tags are active
    let (mut c, tag) = c
        .first::<_, my::Row>(
            "SELECT  `tags`.* FROM `tags` \
             WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test')",
        )
        .await?;
    let tag = tag.unwrap().get::<u32, _>("id");

    if !priming {
        // check that story id isn't already assigned
        c = c
            .drop_exec(
                "SELECT  1 AS one FROM `stories` \
                 WHERE `stories`.`short_id` = ?",
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .await?;
    }

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

    // TODO
    // real impl queries `tags` and `users` again here..?

    // TODO: real impl checks *new* short_id and duplicate urls *again*
    // TODO: sometimes submit url

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let q = c
        .prep_exec(
            "INSERT INTO `stories` \
             (`created_at`, `user_id`, `title`, \
             `description`, `short_id`, `markeddown_description`) \
             VALUES (?, ?, ?, ?, ?, ?)",
            (
                chrono::Local::now().naive_local(),
                user,
                title,
                "to infinity", // lorem ipsum?
                ::std::str::from_utf8(&id[..]).unwrap(),
                "<p>to infinity</p>\n",
            ),
        )
        .await?;
    let story = q.last_insert_id().unwrap();
    let mut c = q.drop_result().await?;

    c = c
        .drop_exec(
            "INSERT INTO `taggings` (`story_id`, `tag_id`) \
             VALUES (?, ?)",
            (story, tag),
        )
        .await?;

    if !priming {
        c = c
            .drop_exec(
                "SELECT  `votes`.* FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` IS NULL",
                (user, story),
            )
            .await?;
    }

    c = c
        .drop_exec(
            "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
             VALUES (?, ?, ?)",
            (user, story, 1),
        )
        .await?;

    Ok((c, false))
}
