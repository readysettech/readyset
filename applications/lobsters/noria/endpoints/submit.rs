use chrono;
use noria::DataType;
use std::future::Future;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    // check that tags are active
    let tag = c
        .view("submit_1")
        .await?
        .lookup_first(&[DataType::from(0i32)], true)
        .await?;
    let tag = tag.unwrap().take("id").unwrap();

    if !priming {
        // check that story id isn't already assigned
        let _ = c
            .view("submit_2")
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

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let mut stories = c.table("stories").await?;
    let story = noria::row!(
        stories,
        "created_at" => chrono::Local::now().naive_local(),
        "user_id" => user,
        "title" => title,
        "description" => "body",
        "short_id" => ::std::str::from_utf8(&id[..]).unwrap(),
        "markeddown_description" => "body",
    );
    stories.insert(story).await?;
    // XXX: last_insert_id
    let story = super::slug_to_id(&id);

    c.table("taggings")
        .await?
        .insert(vec![story.into(), tag])
        .await?;

    if !priming {
        let _ = c
            .view("submit_3")
            .await?
            .lookup(&[user.into(), story.into()], true)
            .await?;
    }

    c.table("votes")
        .await?
        .insert(vec![user.into(), story.into(), 1.into()])
        .await?;

    Ok((c, false))
}
