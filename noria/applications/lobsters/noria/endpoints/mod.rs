use tower_util::ServiceExt;

pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

pub(crate) async fn notifications(c: crate::Conn, uid: u32) -> Result<crate::Conn, anyhow::Error> {
    let _ = c
        .view("notif_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[uid.into()], true)
        .await?;
    let _ = c
        .view("notif_2")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[format!("user:{}:unread_messages", uid).into()], true)
        .await?;

    Ok(c)
}

#[inline]
fn slug_to_id(slug: &[u8; 6]) -> u32 {
    // convert id to unique string
    // 36 possible characters (a-z0-9)
    let mut id = 0;
    let mut mult = 1;
    for i in 0..slug.len() {
        let i = slug.len() - i - 1;
        let digit = if slug[i] >= b'0' && slug[i] <= b'9' {
            slug[i] - b'0'
        } else {
            slug[i] - b'a' + 10
        } as u32;
        id += digit * mult;
        mult *= 36;
    }
    id
}
