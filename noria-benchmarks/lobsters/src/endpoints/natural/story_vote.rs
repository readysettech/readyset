use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId, Vote};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Box<Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(move |c| {
            c.prep_exec(
                "SELECT `stories`.* \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = ?",
                (::std::str::from_utf8(&story[..]).unwrap(),),
            ).and_then(|result| result.collect_and_drop::<my::Row>())
                .map(|(c, mut story)| (c, story.swap_remove(0)))
        }).and_then(move |(c, story)| {
                let id = story.get::<u32, _>("id").unwrap();
                c.drop_exec(
                    "SELECT  `votes`.* \
                     FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`story_id` = ? \
                     AND `votes`.`comment_id` IS NULL",
                    (user, id),
                ).map(move |c| (c, id))
            })
            .and_then(move |(c, story)| {
                // TODO: do something else if user has already voted
                // TODO: technically need to re-load story under transaction

                // NOTE: MySQL technically does everything inside this and_then in a transaction,
                // but let's be nice to it
                c.drop_exec(
                    "INSERT INTO `votes` \
                     (`user_id`, `story_id`, `vote`) \
                     VALUES \
                     (?, ?, ?)",
                    (
                        user,
                        story,
                        match v {
                            Vote::Up => 1,
                            Vote::Down => 0,
                        },
                    ),
                )
            })
            .map(|c| (c, false)),
    )
}
