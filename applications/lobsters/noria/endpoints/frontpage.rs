use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;

    let view = c.view("frontpage_1").await?;
    let stories = view.lookup(&[], true).await?;

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories: Vec<_> = stories.into_iter().map(|row| row["id"]).collect();
    let stories_multi: Vec<_> = stories.iter().map(|dt| vec![dt.clone()]).collect();

    // NOTE: the filters should be *before* the topk
    let view = c.view("frontpage_2").await?;
    let users: HashSet<_> = view
        .multi_lookup(stories_multi.clone(), true)
        .await?
        .into_iter()
        .map(|story| story.into_iter().last().unwrap()["user_id"])
        .collect();

    if let Some(uid) = acting_as {
        let view = c.view("frontpage_3").await?;
        let _ = view.lookup(&[uid.into()], true).await?;

        // TODO
        // AND `taggings`.`tag_id` IN ({})",
        //tags
        let view = c.view("frontpage_4").await?;
        let _ = view.multi_lookup(stories_multi.clone(), true).await?;
    }

    let _ = c
        .view("frontpage_5")
        .await?
        .multi_lookup(users.into_iter().map(|v| vec![v]).collect(), true)
        .await?;
    let _ = c
        .view("frontpage_6")
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;
    let _ = c
        .view("frontpage_7")
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;

    let tags: HashSet<_> = c
        .view("frontpage_8")
        .await?
        .multi_lookup(stories_multi, true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap()["tag_id"])
        .collect();

    let _ = c
        .view("frontpage_9")
        .await?
        .multi_lookup(tags.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let view = c.view("frontpage_10").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }

        let view = c.view("frontpage_11").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }

        let view = c.view("frontpage_12").await?;
        // TODO: multi-lookup
        for story in stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }
    }

    Ok((c, true))
}
