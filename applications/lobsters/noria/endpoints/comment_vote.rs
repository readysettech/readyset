use noria::DataType;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let comment = c
        .view("comment_vote_1")
        .await?
        .lookup(&[::std::str::from_utf8(&comment[..]).unwrap()])
        .await?;

    let comment = comment.unwrap();
    let sid = comment.get::<u32, _>("story_id").unwrap();
    let comment = comment.get::<u32, _>("id").unwrap();
    let _ = c
        .view("comment_vote_2")
        .await?
        .lookup(&[user, sid, comment])
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let tbl = c.table("vote").await?;
    tbl.insert(vec![
        user,
        sid,
        comment,
        match v {
            Vote::Up => 1,
            Vote::Down => 0,
        },
    ])
    .await?;

    Ok((c, false))
}
