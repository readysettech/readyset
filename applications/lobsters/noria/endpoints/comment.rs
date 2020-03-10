use chrono;
use noria::DataType;
use std::future::Future;
use trawler::{CommentId, StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: CommentId,
    story: StoryId,
    parent: Option<CommentId>,
    priming: bool,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    let story = c
        .view("comment_1")
        .await?
        .lookup(&[::std::str::from_utf8(&story[..]).unwrap()])
        .await?;
    let story = story.unwrap();
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();

    if !priming {
        let _ = c.view("comment_2").await?.lookup(&[author]).await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let p = c
            .view("comment_3")
            .await?
            .lookup_first(&[story, ::std::str::from_utf8(&parent[..]).unwrap()])
            .await?;

        if let Some(p) = p {
            Some((
                p.get::<u32, _>("id").unwrap(),
                p.get::<Option<u32>, _>("thread_id").unwrap(),
            ))
        } else {
            eprintln!(
                "failed to find parent comment {} in story {}",
                ::std::str::from_utf8(&parent[..]).unwrap(),
                story
            );
            None
        }
    } else {
        None
    };

    // TODO: real site checks for recent comments by same author with same
    // parent to ensure we don't double-post accidentally

    if !priming {
        // check that short id is available
        let _ = c
            .view("comment_4")
            .await?
            .lookup(&[::std::str::from_utf8(&id[..]).unwrap().into()], true)
            .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let tbl = c.table("comments").await?;
    let q = if let Some((parent, thread)) = parent {
        tbl.insert(vec![
            now.into(),                                     // created_at
            now.into(),                                     // updated_at
            ::std::str::from_utf8(&id[..]).unwrap().into(), // short_id
            story.into(),                                   // story_id
            user.into(),                                    // user_id
            parent.into(),                                  // parent_comment_id
            thread.into(),                                  // thread_id
            "moar".into(),                                  // comment
            "moar".into(),                                  // markdown comment
        ])
        .await?;
    } else {
        tbl.insert(vec![
            now.into(),                                     // created_at
            now.into(),                                     // updated_at
            ::std::str::from_utf8(&id[..]).unwrap().into(), // short_id
            story.into(),                                   // story_id
            user.into(),                                    // user_id
            DataType::None,                                 // parent_comment_id
            DataType::None,                                 // thread_id
            "moar".into(),                                  // comment
            "moar".into(),                                  // markdown comment
        ])
        .await?;
    };
    // TODO: last_insert_id
    let comment = 1;

    if !priming {
        // but why?!
        let _ = c
            .view("comment_5")
            .await?
            .lookup(&[user.into(), story, comment.into()], true)
            .await?;
    }

    let votes = c.table("votes").await?;
    votes
        .insert(vec![user.into(), story, comment.into(), 1.into()])
        .await?;

    Ok((c, false))
}
