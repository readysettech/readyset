use std::future::Future;
use tower_util::ServiceExt;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(crate::Conn, bool), anyhow::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, anyhow::Error>> + Send,
{
    let c = c.await?;

    let user = c
        .view("user_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup_first(&[format!("user{}", uid).into()], true)
        .await?;
    let uid = user.unwrap().take("id").unwrap();

    let _ = c
        .view("user_2")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[uid.clone()], true)
        .await?;

    // most popular tag
    let tag = c
        .view("user_3")
        .await?
        .ready_oneshot()
        .await?
        .lookup_first(&[uid.clone()], true)
        .await?;

    if let Some(mut tag) = tag {
        let tag = tag.take("id").unwrap();
        let _ = c
            .view("user_4")
            .await?
            .ready_oneshot()
            .await?
            .lookup(&[tag], true)
            .await?;
    }

    let _ = c
        .view("user_5")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[uid], true)
        .await?;

    Ok((c, true))
}
