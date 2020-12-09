use chrono;
use std::future::Future;
use tower_util::ServiceExt;
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

    let mut story = c
        .view("comment_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup_first(&[::std::str::from_utf8(&story[..]).unwrap().into()], true)
        .await?
        .unwrap();
    let author = story.take("user_id").unwrap();
    let story = story.take("id").unwrap();

    if !priming {
        let _ = c
            .view("comment_2")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[author], true)
            .await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let p = c
            .view("comment_3")
            .await?
            .ready_oneshot()
            .await?
            .lookup_first(
                &[
                    story.clone(),
                    ::std::str::from_utf8(&parent[..]).unwrap().into(),
                ],
                true,
            )
            .await?;

        if let Some(mut p) = p {
            Some((p.take("id").unwrap(), p.take("thread_id").unwrap()))
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
            .ready_oneshot()
            .await?
            .lookup(&[::std::str::from_utf8(&id[..]).unwrap().into()], true)
            .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // XXX: last_insert_id
    let comment_id = super::slug_to_id(&id);

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let mut tbl = c.table("comments").await?.ready_oneshot().await?;

    let comment = if let Some((parent, thread)) = parent {
        noria::row!(tbl,
            "id" => comment_id,
            "created_at" => now,
            "updated_at" => now,
            "short_id" => ::std::str::from_utf8(&id[..]).unwrap(),
            "story_id" => &story,
            "user_id" => user,
            "parent_comment_id" => parent,
            "thread_id" => thread,
            "comment" => "moar",
            "markeddown_comment" => "moar",
        )
    } else {
        noria::row!(tbl,
            "id" => comment_id,
            "created_at" => now,
            "updated_at" => now,
            "short_id" => ::std::str::from_utf8(&id[..]).unwrap(),
            "story_id" => &story,
            "user_id" => user,
            "comment" => "moar",
            "markeddown_comment" => "moar",
        )
    };
    tbl.insert(comment).await?;

    if !priming {
        // but why?!
        let _ = c
            .view("comment_5")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[user.into(), story.clone(), comment_id.into()], true)
            .await?;
    }

    let mut votes = c.table("votes").await?.ready_oneshot().await?;
    let vote = noria::row!(votes,
        "id" => rand::random::<i64>(),
        "user_id" => user,
        "story_id" => story,
        "comment_id" => comment_id,
        "vote" => 1,
    );
    votes.insert(vote).await?;

    Ok((c, false))
}
