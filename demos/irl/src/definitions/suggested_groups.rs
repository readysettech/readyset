use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct SuggestedGroup {
    // All columns from `suggested_groups`
    id: i64,                               // bigint unsigned NOT NULL AUTO_INCREMENT,
    title: String,                         // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    image: String,                         // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String,                    // timestamp NULL DEFAULT NULL,
    updated_at: String,                    // timestamp NULL DEFAULT NULL,
    recurring_frequency: String,           // varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    recurring_event_title_name: String,    // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    hex_color: String,                     // varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
    is_active: i64,                        // tinyint NOT NULL DEFAULT '1',
    default_recurring_invite_data_id: i64, // int DEFAULT NULL,
}

impl FromRow for SuggestedGroup {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(SuggestedGroup {
            id: get!(row, 0),
            title: get!(row, 1),
            image: get!(row, 2),
            created_at: get!(row, 3),
            updated_at: get!(row, 4),
            recurring_frequency: get!(row, 5),
            recurring_event_title_name: get!(row, 6),
            hex_color: get!(row, 7),
            is_active: get!(row, 8),
            default_recurring_invite_data_id: get!(row, 9),
        })
    }
}
