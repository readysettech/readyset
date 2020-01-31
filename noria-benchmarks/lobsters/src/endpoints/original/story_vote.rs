use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();
    let (mut c, mut story) = c
        .prep_exec(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
            (::std::str::from_utf8(&story[..]).unwrap(),),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);

    let author = story.get::<u32, _>("user_id").unwrap();
    let score = story.get::<f64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c = c
        .drop_exec(
            "SELECT  `votes`.* \
             FROM `votes` \
             WHERE `votes`.`user_id` = ? \
             AND `votes`.`story_id` = ? \
             AND `votes`.`comment_id` IS NULL",
            (user, story),
        )
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c = c
        .drop_exec(
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
        .await?;

    c = c
        .drop_exec(
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
        .await?;

    // get all the stuff needed to compute updated hotness
    c = c
        .drop_exec(
            "SELECT `tags`.* \
             FROM `tags` \
             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
             WHERE `taggings`.`story_id` = ?",
            (story,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT \
             `comments`.`upvotes`, \
             `comments`.`downvotes` \
             FROM `comments` \
             JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
             WHERE `comments`.`story_id` = ? \
             AND `comments`.`user_id` <> `stories`.`user_id`",
            (story,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
            (story,),
        )
        .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    c = c
        .drop_exec(
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
        .await?;

    Ok((c, false))
}
