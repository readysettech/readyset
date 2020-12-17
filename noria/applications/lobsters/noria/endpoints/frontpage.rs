use noria::DataType;
use std::collections::HashSet;
use std::future::Future;
use tower_util::ServiceExt;
use trawler::UserId;
use vec1::vec1;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;

    let stories = c
        .view("frontpage_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[DataType::from(0i32)], true)
        .await?;

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories: Vec<_> = stories
        .into_iter()
        .map(|mut row| row.take("id").unwrap())
        .collect();
    let stories_multi: Vec<_> = stories.iter().map(|dt| vec1![dt.clone()].into()).collect();

    // NOTE: the filters should be *before* the topk
    let users: HashSet<_> = c
        .view("frontpage_2")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?
        .into_iter()
        .filter_map(|story| {
            // frontpage_2 filters out some stories with particularly low scores
            story
                .into_iter()
                .last()
                .map(|mut s| s.take("user_id").unwrap())
        })
        .collect();

    if let Some(uid) = acting_as {
        let _ = c
            .view("frontpage_3")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[uid.into()], true)
            .await?;

        // TODO
        // AND `taggings`.`tag_id` IN ({})",
        //tags
        let _ = c
            .view("frontpage_4")
            .await?
            .ready_oneshot()
            .await?
            .multi_lookup(stories_multi.clone(), true)
            .await?;
    }

    let _ = c
        .view("frontpage_5")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(users.into_iter().map(|v| vec1![v].into()).collect(), true)
        .await?;
    let _ = c
        .view("frontpage_6")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;
    let _ = c
        .view("frontpage_7")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi.clone(), true)
        .await?;

    let tags: HashSet<_> = c
        .view("frontpage_8")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories_multi, true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap().take("tag_id").unwrap())
        .collect();

    let _ = c
        .view("frontpage_9")
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

        c.view("frontpage_10")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys.clone(), true)
            .await?;

        c.view("frontpage_11")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys.clone(), true)
            .await?;

        c.view("frontpage_12")
            .await?
            .ready_and()
            .await?
            .multi_lookup(keys, true)
            .await?;
    }

    Ok((c, true))
}
