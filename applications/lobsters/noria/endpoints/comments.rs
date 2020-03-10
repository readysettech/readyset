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
    let ids = c.view("comments_1").await?.lookup(&[]).await?;

    let cview = c.view("comments_2").await?;
    let mut comments = Vec::new();
    let mut users = HashSet::new();
    let mut stories = HashSet::new();
    for comment in cview.multi_lookup(ids).await? {
        let mut comment = comment.into_iter();
        comments.push(comment.next().unwrap());
        users.insert(comment.next().unwrap());
        stories.insert(comment.next().unwrap());
    }

    if let Some(uid) = acting_as {
        let cview = c.view("comments_3").await?;
        // TODO: multi-lookup
        for story in &stories {
            cview.lookup(&[uid, story]).await?;
        }
    }

    let cview = c.view("comments_4").await?;
    let _ = cview.multi_lookup(users, true).await?;

    let cview = c.view("comments_5").await?;
    let authors = cview
        .multi_lookup(stories, true)
        .await?
        .into_iter()
        .map(|row| row.into_iter().next().unwrap())
        .collect();

    if let Some(uid) = acting_as {
        let cview = c.view("comments_6").await?;
        // TODO: multi-lookup
        for comment in comments {
            cview.lookup(&[uid.into(), comment], true).await?;
        }
    }

    let cview = c.view("comments_7").await?;
    let _ = cview.multi_lookup(authors, true).await?;

    Ok((c, true))
}
