use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let story = c
        .view("story_vote_1")
        .await?
        .lookup(&[::std::str::from_utf8(&story[..]).unwrap()], true)
        .await?;
    let story = story.swap_remove(0);

    let story = story.get::<u32, _>("id").unwrap();
    let _ = c
        .view("story_vote_2")
        .await?
        .lookup(&[user, story], true)
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let tbl = c.table("votes").await?;
    tbl.insert(vec![
        user,
        story,
        match v {
            Vote::Up => 1,
            Vote::Down => 0,
        },
    ])
    .await?;

    Ok((c, false))
}
