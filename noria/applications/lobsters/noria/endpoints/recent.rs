use noria::DataType;
use std::collections::HashSet;
use std::future::Future;
use tower_util::ServiceExt;
use trawler::UserId;
use vec1::vec1;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(crate::Conn, bool), anyhow::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, anyhow::Error>> + Send,
{
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the `NOW()` hack to support dbs primed a while ago
    let c = c.await?;

    let stories = c
        .view("recent_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[DataType::from(0i32)], true)
        .await?;

    assert!(!stories.is_empty(), "got no stories from /recent");

    let stories: Vec<_> = stories
        .into_iter()
        .map(|mut row| row.take("id").unwrap())
        .collect();
    let stories_multi: Vec<_> = stories.iter().map(|dt| vec1![dt.clone()].into()).collect();

    let users: HashSet<_> = c
        .view("recent_2")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?
        .into_iter()
        .filter_map(|story| {
            // recent_2 filters out some stories with particularly low scores
            story
                .into_iter()
                .last()
                .map(|mut s| s.take("user_id").unwrap())
        })
        .collect();

    if let Some(uid) = acting_as {
        let _ = c
            .view("recent_3")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[uid.into()], true)
            .await?;

        // TODO
        //AND `taggings`.`tag_id` IN ({})",
        //tags
        let _ = c
            .view("recent_4")
            .await?
            .ready_oneshot()
            .await?
            .multi_lookup(stories_multi.clone(), true)
            .await?;
    }

    let _ = c
        .view("recent_5")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(users.into_iter().map(|v| vec1![v].into()).collect(), true)
        .await?;

    let _ = c
        .view("recent_6")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;
    let _ = c
        .view("recent_7")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;

    let tags: HashSet<_> = c
        .view("recent_8")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi, true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap().take("tag_id").unwrap())
        .collect();

    let _ = c
        .view("recent_9")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(tags.into_iter().map(|v| vec1![v].into()).collect(), true)
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let keys: Vec<_> = stories
            .iter()
            .map(|sid| vec1![uid.into(), sid.clone()].into())
            .collect();

        c.view("recent_10")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys.clone(), true)
            .await?;

        c.view("recent_11")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys.clone(), true)
            .await?;

        c.view("recent_12")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys, true)
            .await?;
    }

    Ok((c, true))
}
