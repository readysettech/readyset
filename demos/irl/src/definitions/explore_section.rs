use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct ExploreSection {
    // All columns from `explore_sections`
    id: i64,                // bigint unsigned NOT NULL AUTO_INCREMENT,
    name: String,           // varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
    section_key: String,    // varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
    gradient_start: String, // varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    gradient_end: String,   // varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    image: String,          // varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String,     // timestamp NULL DEFAULT NULL,
    updated_at: String,     // timestamp NULL DEFAULT NULL,
    users_can_post: i64,    // tinyint NOT NULL DEFAULT '0',
    is_active: i64,         // tinyint(1) NOT NULL DEFAULT '0',
    organization_id: i64,   // int DEFAULT NULL,
    web_image: String,      // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    es_type: String,        // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'default',
    priority: i64,          // tinyint NOT NULL DEFAULT '0',
}

impl FromRow for ExploreSection {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(ExploreSection {
            id: get!(row, 0),
            name: get!(row, 1),
            section_key: get!(row, 2),
            gradient_start: get!(row, 3),
            gradient_end: get!(row, 4),
            image: get!(row, 5),
            created_at: get!(row, 6),
            updated_at: get!(row, 7),
            users_can_post: get!(row, 8),
            is_active: get!(row, 9),
            organization_id: get!(row, 10),
            web_image: get!(row, 11),
            es_type: get!(row, 12),
            priority: get!(row, 13),
        })
    }
}
