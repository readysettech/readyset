use chrono;
use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
where
    F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(|c| {
            // check that tags are active
            c.first::<_, my::Row>(
                "SELECT  `tags`.* FROM `tags` \
                 WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test')",
            )
        })
        .map(|(c, tag)| (c, tag.unwrap().get::<u32, _>("id")))
        .and_then(move |(c, tag)| {
            // check that story id isn't already assigned
            c.drop_exec(
                "SELECT  1 AS one FROM `stories` \
                 WHERE `stories`.`short_id` = ?",
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .map(move |c| (c, tag))
        })
        .map(|c| {
            // TODO: check for similar stories if there's a url
            // SELECT  `stories`.*
            // FROM `stories`
            // WHERE `stories`.`url` IN (
            //  'https://google.com/test',
            //  'http://google.com/test',
            //  'https://google.com/test/',
            //  'http://google.com/test/',
            //  ... etc
            // )
            // AND (is_expired = 0 OR is_moderated = 1)
            c
        })
        .map(|c| {
            // TODO
            // real impl queries `tags` and `users` again here..?
            c
        })
        .and_then(move |(c, tag)| {
            // TODO: real impl checks *new* short_id and duplicate urls *again*
            // TODO: sometimes submit url

            // NOTE: MySQL technically does everything inside this and_then in a transaction,
            // but let's be nice to it
            c.prep_exec(
                "INSERT INTO `stories` \
                 (`created_at`, `user_id`, `title`, \
                 `description`, `short_id`, `upvotes`, `hotness`, \
                 `markeddown_description`) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    chrono::Local::now().naive_local(),
                    user,
                    title,
                    "to infinity", // lorem ipsum?
                    ::std::str::from_utf8(&id[..]).unwrap(),
                    1,
                    -19216.2884921,
                    "<p>to infinity</p>\n",
                ),
            )
            .and_then(|q| {
                let story = q.last_insert_id().unwrap();
                q.drop_result().map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                t.drop_exec(
                    "INSERT INTO `taggings` (`story_id`, `tag_id`) \
                     VALUES (?, ?)",
                    (story, tag),
                )
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                let key = format!("user:{}:stories_submitted", user);
                t.drop_exec(
                    "INSERT INTO keystores (`key`, `value`) \
                     VALUES (?, ?) \
                     ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
                    (key, 1),
                )
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                let key = format!("user:{}:stories_submitted", user);
                t.drop_exec(
                    "SELECT  `keystores`.* \
                     FROM `keystores` \
                     WHERE `keystores`.`key` = ?",
                    (key,),
                )
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                t.drop_exec(
                    "SELECT  `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`story_id` = ? \
                     AND `votes`.`comment_id` IS NULL",
                    (user, story),
                )
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                t.drop_exec(
                    "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
                     VALUES (?, ?, ?)",
                    (user, story, 1),
                )
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
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
                .map(move |t| (t, story))
            })
            .and_then(move |(t, story)| {
                // why oh why is story hotness *updated* here?!
                t.drop_exec(
                    "UPDATE `stories` \
                     SET `hotness` = ? \
                     WHERE `stories`.`id` = ?",
                    (-19216.5479744, story),
                )
            })
        })
        .map(|c| (c, false)),
    )
}
