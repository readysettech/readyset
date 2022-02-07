use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct InviteFilter {
    // All columns from `invite_filters`
    id: i64,                   // int unsigned NOT NULL AUTO_INCREMENT,
    name: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'https://media.irl.co/xnqplfwolikhtrpwysjcajvwzljnjmifqyvgkxmiatgiqwohmv.png',
    featured_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    priority: i64,          // int DEFAULT NULL,
    created_at: String,     // timestamp NULL DEFAULT NULL,
    updated_at: String,     // timestamp NULL DEFAULT NULL,
    is_default: i64,        // tinyint NOT NULL DEFAULT '0',
    card_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    full_screen_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    circle_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    icon_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    unsplash_image: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
}

impl FromRow for InviteFilter {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(InviteFilter {
            id: get!(row, 0),
            name: get!(row, 1),
            image: get!(row, 2),
            featured_image: get!(row, 3),
            priority: get!(row, 4),
            created_at: get!(row, 5),
            updated_at: get!(row, 6),
            is_default: get!(row, 7),
            card_image: get!(row, 8),
            full_screen_image: get!(row, 9),
            circle_image: get!(row, 10),
            icon_image: get!(row, 11),
            unsplash_image: get!(row, 12),
        })
    }
}
