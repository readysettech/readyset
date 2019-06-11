use futures::future::{self, Either};
use futures::Future;
use my;
use my::prelude::*;
use trawler::UserId;

pub(crate) fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Box<Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    Box::new(
        c.and_then(move |c| {
            c.first_exec::<_, _, my::Row>(
                "SELECT  `users`.* FROM `users` \
                 WHERE `users`.`username` = ?",
                (format!("user{}", uid),),
            )
        })
        .and_then(move |(c, user)| {
            let uid = user.unwrap().get::<u32, _>("id").unwrap();

            c.drop_exec(
                "SELECT user_stats.* FROM user_stats WHERE user_stats.id = ?",
                (uid,),
            )
            .and_then(move |c| {
                // most popular tag
                c.prep_exec(
                    "SELECT  `tags`.`id`, COUNT(*) AS `count` FROM `taggings` \
                     INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` \
                     INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
                     WHERE `tags`.`inactive` = 0 \
                     AND `stories`.`user_id` = ? \
                     GROUP BY `tags`.`id` \
                     ORDER BY `count` desc LIMIT 1",
                    (uid,),
                )
            })
        })
        .and_then(|result| result.collect_and_drop::<my::Row>())
        .map(|(c, mut rows)| {
            if rows.is_empty() {
                (c, None)
            } else {
                (c, Some(rows.swap_remove(0)))
            }
        })
        .and_then(move |(c, tag)| match tag {
            Some(tag) => Either::A(c.drop_exec(
                "SELECT  `tags`.* \
                 FROM `tags` \
                 WHERE `tags`.`id` = ?",
                (tag.get::<u32, _>("id").unwrap(),),
            )),
            None => Either::B(future::ok(c)),
        })
        .and_then(move |c| {
            c.drop_exec(
                "SELECT  1 AS one FROM `hats` \
                 WHERE `hats`.`user_id` = ? LIMIT 1",
                (uid,),
            )
        })
        .map(|c| (c, true)),
    )
}
