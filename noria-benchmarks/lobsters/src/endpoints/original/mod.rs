pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

use my;
use my::prelude::*;

pub(crate) async fn notifications(mut c: my::Conn, uid: u32) -> Result<my::Conn, my::error::Error> {
    c = c
        .drop_exec(
            "SELECT COUNT(*) \
                     FROM `replying_comments_for_count`
                     WHERE `replying_comments_for_count`.`user_id` = ? \
                     GROUP BY `replying_comments_for_count`.`user_id` \
                     ",
            (uid,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
            (format!("user:{}:unread_messages", uid),),
        )
        .await?;

    Ok(c)
}
