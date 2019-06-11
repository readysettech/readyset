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
            )
            .and_then(|result| result.collect_and_drop::<my::Row>())
            .map(|(c, mut story)| (c, story.swap_remove(0)))
        })
        .and_then(move |(c, story)| {
            let author = story.get::<u32, _>("user_id").unwrap();
            let id = story.get::<u32, _>("id").unwrap();
            let score = story.get::<f64, _>("hotness").unwrap();
            c.drop_exec(
                "SELECT  `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` IS NULL",
                (user, id),
            )
            .map(move |c| (c, author, id, score))
        })
        .and_then(move |(c, author, story, score)| {
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
            .and_then(move |t| {
                t.drop_exec(
                    &format!(
                        "UPDATE `users` \
                         SET `users`.`karma` = `users`.`karma` {} \
                         WHERE `users`.`id` = ?",
                        match v {
                            Vote::Up => "+ 1",
                            Vote::Down => "- 1",
                        }
                    ),
                    (author,),
                )
            })
            .and_then(move |t| {
                // get all the stuff needed to compute updated hotness
                t.drop_exec(
                    "SELECT `tags`.* \
                     FROM `tags` \
                     INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                     WHERE `taggings`.`story_id` = ?",
                    (story,),
                )
            })
            .and_then(move |t| {
                t.drop_exec(
                    "SELECT \
                     `comments`.`upvotes`, \
                     `comments`.`downvotes` \
                     FROM `comments` \
                     JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
                     WHERE `comments`.`story_id` = ? \
                     AND `comments`.`user_id` <> `stories`.`user_id`",
                    (story,),
                )
            })
            .and_then(move |t| {
                t.drop_exec(
                    "SELECT `stories`.`id` \
                     FROM `stories` \
                     WHERE `stories`.`merged_story_id` = ?",
                    (story,),
                )
            })
            .and_then(move |t| {
                // the *actual* algorithm for computing hotness isn't all
                // that interesting to us. it does affect what's on the
                // frontpage, but we're okay with using a more basic
                // upvote/downvote ratio thingy. See Story::calculated_hotness
                // in the lobsters source for details.
                t.drop_exec(
                    &format!(
                        "UPDATE stories SET \
                         stories.upvotes = stories.upvotes {}, \
                         stories.downvotes = stories.downvotes {}, \
                         stories.hotness = ? \
                         WHERE stories.id = ?",
                        match v {
                            Vote::Up => "+ 1",
                            Vote::Down => "+ 0",
                        },
                        match v {
                            Vote::Up => "+ 0",
                            Vote::Down => "+ 1",
                        },
                    ),
                    (
                        score
                            - match v {
                                Vote::Up => 1.0,
                                Vote::Down => -1.0,
                            },
                        story,
                    ),
                )
            })
        })
        .map(|c| (c, false)),
    )
}
