use noria::DataType;
use std::future::Future;
use tower_util::ServiceExt;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
) -> Result<(crate::Conn, bool), anyhow::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, anyhow::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    // check that tags are active
    let tag = c
        .view("submit_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup_first(&[DataType::from(0i32)], true)
        .await?;
    let tag = tag.unwrap().take("id").unwrap();

    if !priming {
        // check that story id isn't already assigned
        let _ = c
            .view("submit_2")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[::std::str::from_utf8(&id[..]).unwrap().into()], true)
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

    // XXX: last_insert_id
    let story_id = super::slug_to_id(&id);

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let mut stories = c.table("stories").await?.ready_oneshot().await?;
    let story: Result<Vec<DataType>, anyhow::Error> = noria::row!(stories,
        "id" => story_id,
        "created_at" => chrono::Local::now().naive_local(),
        "user_id" => user,
        "title" => title,
        "description" => "body",
        "short_id" => ::std::str::from_utf8(&id[..]).unwrap(),
        "markeddown_description" => "body",
    );
    let story = story?;
    stories.insert(story).await?;

    let mut taggings = c.table("taggings").await?.ready_oneshot().await?;
    let tagging: Result<Vec<DataType>, anyhow::Error> = noria::row!(taggings,
        "id" => rand::random::<i64>(),
        "story_id" => story_id,
        "tag_id" => tag,
    );
    let tagging = tagging?;
    taggings.insert(tagging).await?;

    if !priming {
        let _ = c
            .view("submit_3")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[user.into(), story_id.into()], true)
            .await?;
    }

    let mut votes = c.table("votes").await?.ready_oneshot().await?;
    let vote: Result<Vec<DataType>, anyhow::Error> = noria::row!(votes,
        "id" => rand::random::<i64>(),
        "user_id" => user,
        "story_id" => story_id,
        "vote" => 1,
    );
    let vote = vote?;
    votes.insert(vote).await?;

    Ok((c, false))
}
