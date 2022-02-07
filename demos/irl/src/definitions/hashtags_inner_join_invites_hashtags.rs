use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct HashtagsInnerJoinInvitesHashtags {
    // All columns from `hashtags`
    id: i64,            // int unsigned NOT NULL AUTO_INCREMENT,
    name: String,       // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String, // timestamp NULL DEFAULT NULL,
    updated_at: String, // timestamp NULL DEFAULT NULL,
    image: String,      // varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
    // Columns from `invites_hashtags`
    ih_invite_id: i64,     // int DEFAULT NULL,
    ih_hashtag_id: i64,    // int DEFAULT NULL,
    ih_created_at: String, // timestamp NULL DEFAULT NULL,
    ih_updated_at: String, // timestamp NULL DEFAULT NULL,
}

impl FromRow for HashtagsInnerJoinInvitesHashtags {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(HashtagsInnerJoinInvitesHashtags {
            id: get!(row, 0),
            name: get!(row, 1),
            created_at: get!(row, 2),
            updated_at: get!(row, 3),
            image: get!(row, 4),
            ih_invite_id: get!(row, 5),
            ih_hashtag_id: get!(row, 6),
            ih_created_at: get!(row, 7),
            ih_updated_at: get!(row, 8),
        })
    }
}
