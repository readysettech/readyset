use chrono;
use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use trawler::{StoryId, UserId};

const NUM_STORIES: u32 = 40650;

#[inline]
fn slug_to_id(slug: &[u8; 6]) -> u32 {
    // convert id to unique string
    // 36 possible characters (a-z0-9)
    let mut id = 0;
    let mut mult = 1;
    for i in 0..slug.len() {
        let i = slug.len() - i - 1;
        let digit = if slug[i] >= b'0' && slug[i] <= b'9' {
            slug[i] - b'0'
        } else {
            slug[i] - b'a' + 10
        } as u32;
        id += digit * mult;
        mult *= 36;
    }
    id
}

#[test]
fn slug_conversion() {
    fn id_to_slug(mut id: u32) -> [u8; 6] {
        // convert id to unique string
        // 36 possible characters (a-z0-9)
        let mut slug = [0; 6];
        let mut digit: u8;
        digit = (id % 36) as u8;
        slug[5] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        digit = (id % 36) as u8;
        slug[4] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        digit = (id % 36) as u8;
        slug[3] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        digit = (id % 36) as u8;
        slug[2] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        digit = (id % 36) as u8;
        slug[1] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        digit = (id % 36) as u8;
        slug[0] = digit + if digit < 10 { b'0' } else { b'a' - 10 };
        id /= 36;
        debug_assert_eq!(id, 0);
        slug
    }

    assert_eq!(slug_to_id(&id_to_slug(0)), 0);
    assert_eq!(slug_to_id(&id_to_slug(1 << 2)), 1 << 2);
    assert_eq!(slug_to_id(&id_to_slug(9)), 9);
    assert_eq!(slug_to_id(&id_to_slug(10)), 10);
    assert_eq!(slug_to_id(&id_to_slug(35)), 35);
    assert_eq!(slug_to_id(&id_to_slug(36)), 36);
    assert_eq!(slug_to_id(&id_to_slug(37)), 37);
    assert_eq!(slug_to_id(&id_to_slug(1 << 8)), 1 << 8);
    assert_eq!(slug_to_id(&id_to_slug(1 << 12)), 1 << 12);
    assert_eq!(slug_to_id(&id_to_slug(1 << 16)), 1 << 16);
    assert_eq!(slug_to_id(&id_to_slug(12492)), 12492);
    assert_eq!(slug_to_id(&id_to_slug(1943)), 1943);
    assert_eq!(slug_to_id(&id_to_slug(NUM_STORIES - 1)), NUM_STORIES - 1);
}

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    simulate_shards: Option<u32>,
    id: StoryId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;
    let (c, mut story) = c
        .prep_exec(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
            (::std::str::from_utf8(&id[..]).unwrap(),),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c = c
        .drop_exec(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
            (author,),
        )
        .await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        let mut pick = simulate_shards.is_none();
        if let Some(shards) = simulate_shards {
            let id = slug_to_id(&id);

            // worst shard is the one that gets the most popular story
            let worst_sharding_mod = (NUM_STORIES - 1) % shards;
            if id % shards == worst_sharding_mod {
                pick = true;
            }
        }

        if pick {
            // keep track of when the user last saw this story
            // NOTE: *technically* the update only happens at the end...
            let (x, rr) = c
                .first_exec::<_, _, my::Row>(
                    "SELECT  `read_ribbons`.* \
                     FROM `read_ribbons` \
                     WHERE `read_ribbons`.`user_id` = ? \
                     AND `read_ribbons`.`story_id` = ?",
                    (&uid, &story),
                )
                .await?;
            let now = chrono::Local::now().naive_local();
            c = match rr {
                None => {
                    x.drop_exec(
                        "INSERT INTO `read_ribbons` \
                         (`created_at`, `updated_at`, `user_id`, `story_id`) \
                         VALUES (?, ?, ?, ?)",
                        (now, now, uid, story),
                    )
                    .await?
                }
                Some(rr) => {
                    x.drop_exec(
                        "UPDATE `read_ribbons` \
                         SET `read_ribbons`.`updated_at` = ? \
                         WHERE `read_ribbons`.`id` = ?",
                        (now, rr.get::<u32, _>("id").unwrap()),
                    )
                    .await?
                }
            };
        }
    }

    // XXX: probably not drop here, but we know we have no merged stories
    c = c
        .drop_exec(
            "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
            (story,),
        )
        .await?;

    let comments = c
        .prep_exec(
            "SELECT `comments`.*, \
             `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
             FROM `comments` \
             WHERE `comments`.`story_id` = ? \
             ORDER BY \
             saldo ASC, \
             confidence DESC",
            (story,),
        )
        .await?;

    let (c, (users, comments)) = comments
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut comments), comment| {
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                comments.insert(comment.get::<u32, _>("id").unwrap());
                (users, comments)
            },
        )
        .await?;

    // get user info for all commenters
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c = c
        .drop_query(&format!(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
            users
        ))
        .await?;

    // get comment votes
    // XXX: why?!
    let comments = comments
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c = c
        .drop_query(&format!(
            "SELECT `votes`.* FROM `votes` WHERE `votes`.`comment_id` IN ({})",
            comments
        ))
        .await?;

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        c = c
            .drop_exec(
                "SELECT `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` IS NULL",
                (uid, story),
            )
            .await?;
        c = c
            .drop_exec(
                "SELECT `hidden_stories`.* \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = ? \
                 AND `hidden_stories`.`story_id` = ?",
                (uid, story),
            )
            .await?;
        c = c
            .drop_exec(
                "SELECT `saved_stories`.* \
                 FROM `saved_stories` \
                 WHERE `saved_stories`.`user_id` = ? \
                 AND `saved_stories`.`story_id` = ?",
                (uid, story),
            )
            .await?;
    }

    let taggings = c
        .prep_exec(
            "SELECT `taggings`.* \
             FROM `taggings` \
             WHERE `taggings`.`story_id` = ?",
            (story,),
        )
        .await?;

    let (c, tags) = taggings
        .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    let c = c
        .drop_query(&format!(
            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
            tags
        ))
        .await?;

    Ok((c, true))
}
