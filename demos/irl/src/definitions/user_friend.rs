use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct UserFriend {
    // All columns from `users_friends`
    id: i64,                             // int unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,                        // int DEFAULT NULL,
    friend_id: i64,                      // int DEFAULT NULL,
    muted: i64,                          // tinyint(1) DEFAULT '0',
    is_deleted: i64,                     // tinyint(1) DEFAULT '0',
    ignored: i64,                        // tinyint(1) DEFAULT '0',
    blocked: i64,                        // tinyint(1) DEFAULT '0',
    is_close_friend: i64,                // tinyint(1) DEFAULT '0',
    reminder_notif_sent_at: String,      // datetime DEFAULT NULL,
    friend_request_sent_at: String,      // datetime DEFAULT NULL,
    created_at: String,                  // timestamp NULL DEFAULT NULL,
    updated_at: String,                  // timestamp NULL DEFAULT NULL,
    friend_dismissed_friend_prompt: i64, // tinyint NOT NULL DEFAULT '0',
    requested: i64,                      // tinyint(1) NOT NULL DEFAULT '0',
    request_denied: i64,                 // tinyint NOT NULL DEFAULT '0',
    is_subscribed: i64,                  // tinyint NOT NULL DEFAULT '0',
    seen_close_friend: i64,              // tinyint(1) DEFAULT '0',
    is_followed_from_contacts: i64,      // tinyint NOT NULL DEFAULT '0',
}

impl FromRow for UserFriend {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(UserFriend {
            id: get!(row, 0),
            user_id: get!(row, 1),
            friend_id: get!(row, 2),
            muted: get!(row, 3),
            is_deleted: get!(row, 4),
            ignored: get!(row, 5),
            blocked: get!(row, 6),
            is_close_friend: get!(row, 7),
            reminder_notif_sent_at: get!(row, 8),
            friend_request_sent_at: get!(row, 9),
            created_at: get!(row, 10),
            updated_at: get!(row, 11),
            friend_dismissed_friend_prompt: get!(row, 12),
            requested: get!(row, 13),
            request_denied: get!(row, 14),
            is_subscribed: get!(row, 15),
            seen_close_friend: get!(row, 16),
            is_followed_from_contacts: get!(row, 17),
        })
    }
}
