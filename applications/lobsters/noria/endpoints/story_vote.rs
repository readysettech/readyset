use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let story = c
        .view("story_vote_1")
        .await?
        .lookup_first(&[::std::str::from_utf8(&story[..]).unwrap().into()], true)
        .await?
        .unwrap();

    let story = story["id"];
    let _ = c
        .view("story_vote_2")
        .await?
        .lookup(&[user.into(), story], true)
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let tbl = c.table("votes").await?;
    tbl.insert(vec![
        user.into(),
        story,
        match v {
            Vote::Up => 1,
            Vote::Down => 0,
        }
        .into(),
    ])
    .await?;

    Ok((c, false))
}
