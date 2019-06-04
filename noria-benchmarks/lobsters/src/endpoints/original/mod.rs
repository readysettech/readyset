pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

use futures::Future;
use my;
use my::prelude::*;

pub(crate) fn notifications(
    c: my::Conn,
    uid: u32,
) -> impl Future<Item = my::Conn, Error = my::error::Error> {
    c.drop_exec(
        "SELECT COUNT(*) \
                     FROM `replying_comments_for_count`
                     WHERE `replying_comments_for_count`.`user_id` = ? \
                     GROUP BY `replying_comments_for_count`.`user_id` \
                     ",
        (uid,),
    )
    .and_then(move |c| {
        c.drop_exec(
            "SELECT `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
            (format!("user:{}:unread_messages", uid),),
        )
    })
}
