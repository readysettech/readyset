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
    let ids: Vec<_> = c.view("comments_1").await?.lookup(&[], true).await?.into();

    let cview = c.view("comments_2").await?;
    let mut comments = Vec::new();
    let mut users = HashSet::new();
    let mut stories = HashSet::new();
    for comment in cview.multi_lookup(ids, true).await? {
        let comment = comment.into_iter().last().unwrap();
        comments.push(comment["id"]);
        users.insert(comment["user_id"]);
        stories.insert(comment["story_id"]);
    }

    if let Some(uid) = acting_as {
        let cview = c.view("comments_3").await?;
        // TODO: multi-lookup
        for story in &stories {
            cview.lookup(&[uid.into(), story.clone()], true).await?;
        }
    }

    let cview = c.view("comments_4").await?;
    let _ = cview
        .multi_lookup(users.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    let cview = c.view("comments_5").await?;
    let authors: HashSet<_> = cview
        .multi_lookup(stories.into_iter().map(|v| vec![v]).collect(), true)
        .await?
        .into_iter()
        .map(|row| row.into_iter().next().unwrap()["user_id"])
        .collect();

    if let Some(uid) = acting_as {
        let cview = c.view("comments_6").await?;
        // TODO: multi-lookup
        for comment in comments {
            cview.lookup(&[uid.into(), comment], true).await?;
        }
    }

    let cview = c.view("comments_7").await?;
    let _ = cview
        .multi_lookup(authors.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    Ok((c, true))
}
