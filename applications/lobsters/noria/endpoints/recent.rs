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
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the `NOW()` hack to support dbs primed a while ago
    let c = c.await?;

    let stories = c.view("recent_1").await?.lookup(&[], true).await?;

    assert!(!stories.is_empty(), "got no stories from /recent");

    let stories: Vec<_> = stories.into_iter().map(|row| row["id"]).collect();
    let stories_multi: Vec<_> = stories.iter().map(|dt| vec![dt.clone()]).collect();

    let users: HashSet<_> = c
        .view("recent_2")
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?
        .into_iter()
        .map(|story| story.into_iter().last().unwrap()["user_id"])
        .collect();

    if let Some(uid) = acting_as {
        let _ = c
            .view("recent_3")
            .await?
            .lookup(&[uid.into()], true)
            .await?;

        // TODO
        //AND `taggings`.`tag_id` IN ({})",
        //tags
        let _ = c
            .view("recent_4")
            .await?
            .multi_lookup(stories_multi.clone(), true)
            .await?;
    }

    let _ = c
        .view("recent_5")
        .await?
        .multi_lookup(users.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    let _ = c
        .view("recent_6")
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;
    let _ = c
        .view("recent_7")
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;

    let tags: HashSet<_> = c
        .view("recent_8")
        .await?
        .multi_lookup(stories_multi, true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap()["tag_id"])
        .collect();

    let _ = c
        .view("recent_9")
        .await?
        .multi_lookup(tags.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let view = c.view("recent_10").await?;
        // TODO: multi-lookup
        for story in &stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }

        let view = c.view("recent_11").await?;
        // TODO: multi-lookup
        for story in &stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }

        let view = c.view("recent_12").await?;
        // TODO: multi-lookup
        for story in &stories {
            view.lookup(&[uid.into(), story.clone()], true).await?;
        }
    }

    Ok((c, true))
}
