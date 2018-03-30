use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId, Vote};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(move |c| {
            c.first_exec::<_, _, my::Row>(
                "SELECT `comments`.* \
                 FROM `comments` \
                 WHERE `comments`.`short_id` = ? \
                 ORDER BY `comments`.`id` ASC LIMIT 1",
                (::std::str::from_utf8(&comment[..]).unwrap(),),
            )
        }).and_then(move |(c, comment)| {
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
                     AND `votes`.`comment_id` = ? \
                     ORDER BY `votes`.`id` ASC LIMIT 1",
                    (user, story, id),
                ).map(move |c| (c, author, id, story, upvotes, downvotes))
            })
            .and_then(move |(c, author, comment, story, upvotes, downvotes)| {
                // TODO: do something else if user has already voted
                // TODO: technically need to re-load comment under transaction
                c.start_transaction(my::TransactionOptions::new())
                    .and_then(move |t| {
                        t.drop_exec(
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
                    .and_then(move |t| {
                        t.drop_exec(
                            &format!(
                                "UPDATE `users` \
                                 SET `karma` = `karma` {} \
                                 WHERE `id` = ?",
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
                                             `upvotes` = `upvotes` {}, \
                                             `downvotes` = `downvotes` {}, \
                                             `confidence` = {}
                                             WHERE `id` = ?",
                                match v {
                                    Vote::Up => "+ 1",
                                    Vote::Down => "+ 0",
                                },
                                match v {
                                    Vote::Up => "+ 0",
                                    Vote::Down => "+ 1",
                                },
                                confidence,
                            ),
                            (comment,),
                        )
                    })
                    .and_then(move |c| {
                        // get all the stuff needed to compute updated hotness
                        c.first_exec::<_, _, my::Row>(
                            "SELECT `stories`.* \
                             FROM `stories` \
                             WHERE `stories`.`id` = ? \
                             ORDER BY `stories`.`id` ASC LIMIT 1",
                            (story,),
                        ).map(|(c, story)| {
                            let story = story.unwrap();
                            (
                                c,
                                story.get::<u32, _>("user_id").unwrap(),
                                story.get::<f64, _>("hotness").unwrap(),
                            )
                        })
                    })
                    .and_then(move |(t, story_author, score)| {
                        t.drop_exec(
                            "SELECT `tags`.* \
                             FROM `tags` \
                             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                             WHERE `taggings`.`story_id` = ?",
                            (story,),
                        ).map(move |t| (t, story_author, score))
                    })
                    .and_then(move |(t, story_author, score)| {
                        t.drop_exec(
                            "SELECT \
                             `comments`.`upvotes`, \
                             `comments`.`downvotes` \
                             FROM `comments` \
                             WHERE `comments`.`story_id` = ? \
                             AND user_id <> ?",
                            (story, story_author),
                        ).map(move |t| (t, score))
                    })
                    .and_then(move |(t, score)| {
                        t.drop_exec(
                            "SELECT `stories`.`id` \
                             FROM `stories` \
                             WHERE `stories`.`merged_story_id` = ?",
                            (story,),
                        ).map(move |t| (t, score))
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
                                 upvotes = COALESCE(upvotes, 0) {}, \
                                 downvotes = COALESCE(downvotes, 0) {}, \
                                 hotness = '{}' \
                                 WHERE id = ?",
                                match v {
                                    Vote::Up => "+ 1",
                                    Vote::Down => "+ 0",
                                },
                                match v {
                                    Vote::Up => "+ 0",
                                    Vote::Down => "+ 1",
                                },
                                score - match v {
                                    Vote::Up => 1.0,
                                    Vote::Down => -1.0,
                                }
                            ),
                            (story,),
                        )
                    })
                    .and_then(|t| t.commit())
            })
            .map(|c| (c, false)),
    )
}
