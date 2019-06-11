use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId, Vote};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Box<Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(move |c| {
            c.first_exec::<_, _, my::Row>(
                "SELECT `comments`.* \
                 FROM `comments` \
                 WHERE `comments`.`short_id` = ?",
                (::std::str::from_utf8(&comment[..]).unwrap(),),
            )
        })
        .and_then(move |(c, comment)| {
            let comment = comment.unwrap();
            let id = comment.get::<u32, _>("id").unwrap();
            let story = comment.get::<u32, _>("story_id").unwrap();
            c.drop_exec(
                "SELECT  `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` = ?",
                (user, story, id),
            )
            .map(move |c| (c, id, story))
        })
        .and_then(move |(c, comment, story)| {
            // TODO: do something else if user has already voted
            // TODO: technically need to re-load comment under transaction

            // NOTE: MySQL technically does everything inside this and_then in a transaction,
            // but let's be nice to it
            c.drop_exec(
                "INSERT INTO `votes` \
                 (`user_id`, `story_id`, `comment_id`, `vote`) \
                 VALUES \
                 (?, ?, ?, ?)",
                (
                    user,
                    story,
                    comment,
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
