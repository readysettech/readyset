use noria::DataType;
use std::future::Future;
use tower_util::ServiceExt;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(crate::Conn, bool), anyhow::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, anyhow::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let mut story = c
        .view("story_vote_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup_first(&[::std::str::from_utf8(&story[..]).unwrap().into()], true)
        .await?
        .unwrap();

    let story = story.take("id").unwrap();
    let _ = c
        .view("story_vote_2")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[user.into(), story.clone()], true)
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let mut votes = c.table("votes").await?.ready_oneshot().await?;
    let vote: Result<Vec<DataType>, anyhow::Error> = noria::row!(votes,
        "id" => rand::random::<i64>(),
        "user_id" => user,
        "story_id" => story,
        "vote" => match v {
            Vote::Up => 1,
            Vote::Down => 0,
        },
    );
    let vote: Vec<DataType> = vote?;
    votes.insert(vote).await?;

    Ok((c, false))
}
