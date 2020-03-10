use chrono;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
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
    id: StoryId,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;

    let story = c
        .view("story_1")
        .await?
        .lookup(&[::std::str::from_utf8(&id[..]).unwrap().into()], true)
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32>("user_id").unwrap();
    let story = story.get::<u32>("id").unwrap();

    let _ = c.view("story_2").await?.lookup(&[author], true).await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let _ = c
            .view("story_3")
            .await?
            .lookup(&[uid.into(), story], true)
            .await?;
        let now = chrono::Local::now().naive_local();
        let tbl = c.table("read_ribbons").await?;
        tbl.insert_or_update(
            vec![
                now.into(),   // created_at
                now.into(),   // updated_at,
                uid.into(),   // user_id,
                story.into(), // story_id
            ],
            vec![(1, noria::Modification::Set(now.into()))],
        )
        .await?;
    }

    // XXX: probably not drop here, but we know we have no merged stories
    let _ = c.view("story_4").await?.lookup(&[story], true).await?;

    let mut users = HashSet::new();
    let mut comments = HashSet::new();
    for comment in c.view("story_5").await?.lookup(&[story], true).await? {
        users.insert(comment.get::<u32>("user_id").unwrap());
        comments.insert(comment.get::<u32>("id").unwrap());
    }

    // get user info for all commenters
    let _ = c.view("story_6").await?.multi_lookup(users, true).await?;

    if let Some(uid) = acting_as {
        let view = c.view("story_7").await?;
        // TODO: multi-lookup
        for comment in comments {
            let _ = view.lookup(&[uid.into(), comment.into()], true).await?;
        }
    }

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        let _ = c
            .view("story_8")
            .await?
            .lookup(&[uid.into(), story.into()], true)
            .await?;
        let _ = c
            .view("story_9")
            .await?
            .lookup(&[uid.into(), story.into()], true)
            .await?;
        let _ = c
            .view("story_10")
            .await?
            .lookup(&[uid.into(), story.into()], true)
            .await?;
    }

    let tags: HashSet<_> = c
        .view("story_11")
        .await?
        .lookup(&[story], true)
        .await?
        .into_iter()
        .map(|tagging| tagging.into_iter().last().unwrap())
        .collect();

    let _ = c.view("story_12").await?.multi_lookup(tags, true).await?;

    Ok((c, true))
}
