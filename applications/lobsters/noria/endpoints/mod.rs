pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

pub(crate) async fn notifications(
    mut c: crate::Conn,
    uid: u32,
) -> Result<crate::Conn, failure::Error> {
    let _ = c.view("notif_1").await?.lookup(&[uid.into()], true).await?;
    let _ = c
        .view("notif_2")
        .await?
        .lookup(&[format!("user:{}:unread_messages", uid).into()], true)
        .await?;

    Ok(c)
}
