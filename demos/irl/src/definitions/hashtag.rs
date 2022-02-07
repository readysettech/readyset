use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct Hashtag {
    // All columns from `hashtags`
    id: i64,            // int unsigned NOT NULL AUTO_INCREMENT,
    name: String,       // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String, // timestamp NULL DEFAULT NULL,
    updated_at: String, // timestamp NULL DEFAULT NULL,
    image: String,      // varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
}

impl FromRow for Hashtag {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(Hashtag {
            id: get!(row, 0),
            name: get!(row, 1),
            created_at: get!(row, 2),
            updated_at: get!(row, 3),
            image: get!(row, 4),
        })
    }
}
