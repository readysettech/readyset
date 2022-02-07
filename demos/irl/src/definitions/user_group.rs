use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct UserGroup {
    // All columns from `users_groups`
    id: i64,                     // bigint unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,                // int unsigned NOT NULL,
    group_id: i64,               // int unsigned NOT NULL,
    inviter_id: i64,             // int unsigned NOT NULL,
    deleted_at: String,          // datetime DEFAULT NULL,
    created_at: String,          // timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at: String,          // timestamp NULL DEFAULT NULL,
    is_seen: i64,                // tinyint(1) NOT NULL DEFAULT '0',
    chat_tab_updated_at: String, // datetime DEFAULT NULL,
    pending: i64,                // tinyint NOT NULL DEFAULT '0',
    joined_at: String,           // datetime DEFAULT NULL,
    last_opened_at: String,      // datetime DEFAULT NULL,
    mute_until: String,          // datetime DEFAULT NULL,
    denied_at: String,           // timestamp NULL DEFAULT NULL,
    is_requested: i64,           // tinyint NOT NULL DEFAULT '0',
}

impl FromRow for UserGroup {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(UserGroup {
            id: get!(row, 0),
            user_id: get!(row, 1),
            group_id: get!(row, 2),
            inviter_id: get!(row, 3),
            deleted_at: get!(row, 4),
            created_at: get!(row, 5),
            updated_at: get!(row, 6),
            is_seen: get!(row, 7),
            chat_tab_updated_at: get!(row, 8),
            pending: get!(row, 9),
            joined_at: get!(row, 10),
            last_opened_at: get!(row, 11),
            mute_until: get!(row, 12),
            denied_at: get!(row, 13),
            is_requested: get!(row, 14),
        })
    }
}
