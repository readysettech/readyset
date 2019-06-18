use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId, Vote};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
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
            let author = comment.get::<u32, _>("user_id").unwrap();
            let id = comment.get::<u32, _>("id").unwrap();
            let story = comment.get::<u32, _>("story_id").unwrap();
            let upvotes = comment.get::<u32, _>("upvotes").unwrap();
            let downvotes = comment.get::<u32, _>("downvotes").unwrap();
            c.drop_exec(
                "SELECT  `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` = ?",
                (user, story, id),
            )
            .map(move |c| (c, author, id, story, upvotes, downvotes))
        })
        .and_then(move |(c, author, comment, story, upvotes, downvotes)| {
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
                // approximate Comment::calculate_hotness
                let confidence = upvotes as f64 / (upvotes as f64 + downvotes as f64);
                t.drop_exec(
                    &format!(
                        "UPDATE `comments` \
                         SET \
                         `comments`.`upvotes` = `comments`.`upvotes` {}, \
                         `comments`.`downvotes` = `comments`.`downvotes` {}, \
                         `comments`.`confidence` = ? \
                         WHERE `id` = ?",
                        match v {
                            Vote::Up => "+ 1",
                            Vote::Down => "+ 0",
                        },
                        match v {
                            Vote::Up => "+ 0",
                            Vote::Down => "+ 1",
                        },
                    ),
                    (confidence, comment),
                )
            })
            .and_then(move |c| {
                // get all the stuff needed to compute updated hotness
                c.first_exec::<_, _, my::Row>(
                    "SELECT `stories`.* \
                     FROM `stories` \
                     WHERE `stories`.`id` = ?",
                    (story,),
                )
                .map(|(c, story)| {
                    let story = story.unwrap();
                    (c, story.get::<f64, _>("hotness").unwrap())
                })
            })
            .and_then(move |(t, score)| {
                t.drop_exec(
                    "SELECT `tags`.* \
                     FROM `tags` \
                     INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                     WHERE `taggings`.`story_id` = ?",
                    (story,),
                )
                .map(move |t| (t, score))
            })
            .and_then(move |(t, score)| {
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
                .map(move |t| (t, score))
            })
            .and_then(move |(t, score)| {
                t.drop_exec(
                    "SELECT `stories`.`id` \
                     FROM `stories` \
                     WHERE `stories`.`merged_story_id` = ?",
                    (story,),
                )
                .map(move |t| (t, score))
            })
            .and_then(move |(t, score)| {
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
                         WHERE id = ?",
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
