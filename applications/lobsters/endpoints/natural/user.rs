use my;
use my::prelude::*;
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
    let (mut c, user) = c
        .first_exec::<_, _, my::Row>(
            "SELECT  `users`.* FROM `users` \
             WHERE `users`.`username` = ?",
            (format!("user{}", uid),),
        )
        .await?;
    let uid = user.unwrap().get::<u32, _>("id").unwrap();

    c = c
        .drop_exec(
            "SELECT user_stats.* FROM user_stats WHERE user_stats.id = ?",
            (uid,),
        )
        .await?;

    // most popular tag
    let (mut c, mut rows) = c
        .prep_exec(
            "SELECT  `tags`.`id`, COUNT(*) AS `count` FROM `taggings` \
             INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` \
             INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
             WHERE `tags`.`inactive` = 0 \
             AND `stories`.`user_id` = ? \
             GROUP BY `tags`.`id` \
             ORDER BY `count` desc LIMIT 1",
            (uid,),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;

    if !rows.is_empty() {
        let tag = rows.swap_remove(0);
        c = c
            .drop_exec(
                "SELECT  `tags`.* \
                 FROM `tags` \
                 WHERE `tags`.`id` = ?",
                (tag.get::<u32, _>("id").unwrap(),),
            )
            .await?;
    }

    c = c
        .drop_exec(
            "SELECT  1 AS one FROM `hats` \
             WHERE `hats`.`user_id` = ? LIMIT 1",
            (uid,),
        )
        .await?;

    Ok((c, true))
}
