use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(crate::Conn, bool), failur::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failur::Error>> + Send,
{
    let c = c.await?;

    let view = c.view("frontpage_1").await?;
    let stories = view.lookup(&[], true).await?;

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    // NOTE: the filters should be *before* the topk
    let view = c.view("frontpage_2").await?;
    let users: HashSet<_> = view
        .multi_lookup(stories.clone(), true)
        .await?
        .into_iter()
        .map(|story| story.into_iter().last().unwrap())
        .collect();

    if let Some(uid) = acting_as {
        let view = c.view("frontpage_3").await?;
        let _ = view.lookup(&[uid], true).await?;

        // TODO
        // AND `taggings`.`tag_id` IN ({})",
        //tags
        let view = c.view("frontpage_4").await?;
        let _ = view.multi_lookup(stories.clone(), true).await?;
    }

    let _ = c.view("frontpage_5").await?.multi_lookup(users).await?;
    let _ = c
        .view("frontpage_6")
        .await?
        .multi_lookup(stories.clone())
        .await?;
    let _ = c
        .view("frontpage_7")
        .await?
        .multi_lookup(stories.clone())
        .await?;

    let tags: HashSet<_> = c
        .view("frontpage_8")
        .await?
        .multi_lookup(stories)
        .await?
        .into_iter()
        .map(|story| story.into_iter().last().unwrap())
        .collect();

    let _ = c.view("frontpage_9").await?.multi_lookup(tags).await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let view = c.view("frontpage_10").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid, story]).await?;
        }

        let view = c.view("frontpage_11").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid, story]).await?;
        }

        let view = c.view("frontpage_12").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid, story]).await?;
        }
    }

    Ok((c, true))
}
