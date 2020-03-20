use chrono;
use std::collections::HashSet;
use std::future::Future;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;

    let mut story = c
        .view("story_1")
        .await?
        .lookup_first(&[::std::str::from_utf8(&id[..]).unwrap().into()], true)
        .await?
        .unwrap();
    let author = story.take("user_id").unwrap();
    let story = story.take("id").unwrap();

    let _ = c.view("story_2").await?.lookup(&[author], true).await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let _ = c
            .view("story_3")
            .await?
            .lookup(&[uid.into(), story.clone()], true)
            .await?;
        let now = chrono::Local::now().naive_local();
        let mut tbl = c.table("read_ribbons").await?;
        let row = noria::row!(tbl,
            "id" => (i64::from(uid) << 32) + Into::<i64>::into(&story),
            "created_at" => now,
            "updated_at" => now,
            "user_id" => uid,
            "story_id" => &story,
        );
        let set = noria::update!(tbl,
            "updated_at" => noria::Modification::Set(now.into())
        );
        tbl.insert_or_update(row, set).await?;
    }

    // XXX: probably not drop here, but we know we have no merged stories
    let _ = c
        .view("story_4")
        .await?
        .lookup(&[story.clone()], true)
        .await?;

    let mut users = HashSet::new();
    let mut comments = HashSet::new();
    for mut comment in c
        .view("story_5")
        .await?
        .lookup(&[story.clone()], true)
        .await?
    {
        users.insert(comment.take("user_id").unwrap());
        comments.insert(comment.take("id").unwrap());
    }

    // get user info for all commenters
    let _ = c
        .view("story_6")
        .await?
        .multi_lookup(users.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    if let Some(uid) = acting_as {
        let mut view = c.view("story_7").await?;
        // TODO: multi-lookup
        for comment in comments {
            let _ = view.lookup(&[uid.into(), comment.into()], true).await?;
        }
    }

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        let _ = c
            .view("story_8")
            .await?
            .lookup(&[uid.into(), story.clone()], true)
            .await?;
        let _ = c
            .view("story_9")
            .await?
            .lookup(&[uid.into(), story.clone()], true)
            .await?;
        let _ = c
            .view("story_10")
            .await?
            .lookup(&[uid.into(), story.clone()], true)
            .await?;
    }

    let tags: HashSet<_> = c
        .view("story_11")
        .await?
        .lookup(&[story], true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap())
        .collect();

    let _ = c
        .view("story_12")
        .await?
        .multi_lookup(tags.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    Ok((c, true))
}
