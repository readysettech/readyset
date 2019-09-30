use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let (mut c, comment) = c
        .first_exec::<_, _, my::Row>(
            "SELECT `comments`.* \
             FROM `comments` \
             WHERE `comments`.`short_id` = ?",
            (::std::str::from_utf8(&comment[..]).unwrap(),),
        )
        .await?;

    let comment = comment.unwrap();
    let sid = comment.get::<u32, _>("story_id").unwrap();
    let comment = comment.get::<u32, _>("id").unwrap();
    c = c
        .drop_exec(
            "SELECT  `votes`.* \
             FROM `votes` \
             WHERE `votes`.`user_id` = ? \
             AND `votes`.`story_id` = ? \
             AND `votes`.`comment_id` = ?",
            (user, sid, comment),
        )
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c = c
        .drop_exec(
            "INSERT INTO `votes` \
             (`user_id`, `story_id`, `comment_id`, `vote`) \
             VALUES \
             (?, ?, ?, ?)",
            (
                user,
                sid,
                comment,
                match v {
                    Vote::Up => 1,
                    Vote::Down => 0,
                },
            ),
        )
        .await?;

    Ok((c, false))
}
