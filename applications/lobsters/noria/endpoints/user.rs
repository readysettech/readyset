use std::future::Future;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;

    let user = c
        .view("user_1")
        .await?
        .lookup(&[format!("user{}", uid)], true)
        .await?;
    let uid = user.unwrap().get::<u32, _>("id").unwrap();

    let _ = c.view("user_2").await?.lookup(&[uid], true).await?;

    // most popular tag
    let rows = c.view("user_3").await?.lookup(&[uid], true).await?;

    if !rows.is_empty() {
        let tag = rows.swap_remove(0);
        let _ = c
            .view("user_4")
            .await?
            .lookup(&[tag.get::<u32, _>("id").unwrap()], true)
            .await?;
    }

    let _ = c.view("user_5").await?.lookup(&[uid], true).await?;

    Ok((c, true))
}
