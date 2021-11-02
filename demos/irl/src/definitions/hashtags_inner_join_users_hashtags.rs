use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct HashtagsInnerJoinUsersHashtags {
    // All columns from `hashtags`
    id: i64,            // int unsigned NOT NULL AUTO_INCREMENT,
    name: String,       // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String, // timestamp NULL DEFAULT NULL,
    updated_at: String, // timestamp NULL DEFAULT NULL,
    image: String,      // varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
    // Columns from `users_hashtags`
    uh_user_id: i64,       // int NOT NULL,
    uh_hashtag_id: i64,    // int NOT NULL,
    uh_created_at: String, // timestamp NULL DEFAULT NULL,
    uh_updated_at: String, // timestamp NULL DEFAULT NULL,
}

impl FromRow for HashtagsInnerJoinUsersHashtags {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(HashtagsInnerJoinUsersHashtags {
            id: get!(row, 0),
            name: get!(row, 1),
            created_at: get!(row, 2),
            updated_at: get!(row, 3),
            image: get!(row, 4),
            uh_user_id: get!(row, 5),
            uh_hashtag_id: get!(row, 6),
            uh_created_at: get!(row, 7),
            uh_updated_at: get!(row, 8),
        })
    }
}
