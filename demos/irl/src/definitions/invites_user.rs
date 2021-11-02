use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct InvitesUser {
    // All columns from `invites_users`
    id: i64,                       // bigint unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,                  // int DEFAULT NULL,
    invite_id: i64,                // int NOT NULL,
    attending: i64,                // tinyint(1) DEFAULT '0',
    pending: i64,                  // tinyint(1) DEFAULT '0',
    created_at: String,            // timestamp NULL DEFAULT NULL,
    updated_at: String,            // timestamp NULL DEFAULT NULL,
    inviter_id: i64,               // int DEFAULT NULL,
    did_tap_invite: i64,           // tinyint(1) DEFAULT '0',
    did_tap_next_time: i64,        // tinyint(1) DEFAULT '0',
    did_send_seen_notif: i64,      // tinyint(1) DEFAULT '0',
    seen_at: String,               // datetime DEFAULT NULL,
    last_opened_at: String,        // datetime DEFAULT NULL,
    did_opt_out_of_recurring: i64, // tinyint(1) DEFAULT '0',
    wants_to_repeat: i64,          // tinyint NOT NULL DEFAULT '0',
    removed: i64,                  // tinyint DEFAULT '0',
    interested: i64,               // tinyint NOT NULL DEFAULT '0',
    did_report: i64,               // tinyint NOT NULL DEFAULT '0',
    updated_at_for_feed: String,   // datetime NOT NULL,
    invited_at: String,            // datetime DEFAULT NULL,
    chat_tab_updated_at: String,   // datetime DEFAULT NULL,
    banned: i64,                   // tinyint NOT NULL DEFAULT '0',
    is_host: i64,                  // tinyint DEFAULT '0',
    uninvited: i64,                // tinyint NOT NULL DEFAULT '0',
    added_from_subscribe: i64,     // tinyint NOT NULL DEFAULT '0',
    mute_until: String,            // datetime DEFAULT NULL,
}

impl FromRow for InvitesUser {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(InvitesUser {
            id: get!(row, 0),
            user_id: get!(row, 1),
            invite_id: get!(row, 2),
            attending: get!(row, 3),
            pending: get!(row, 4),
            created_at: get!(row, 5),
            updated_at: get!(row, 6),
            inviter_id: get!(row, 7),
            did_tap_invite: get!(row, 8),
            did_tap_next_time: get!(row, 9),
            did_send_seen_notif: get!(row, 10),
            seen_at: get!(row, 11),
            last_opened_at: get!(row, 12),
            did_opt_out_of_recurring: get!(row, 13),
            wants_to_repeat: get!(row, 14),
            removed: get!(row, 15),
            interested: get!(row, 16),
            did_report: get!(row, 17),
            updated_at_for_feed: get!(row, 18),
            invited_at: get!(row, 19),
            chat_tab_updated_at: get!(row, 20),
            banned: get!(row, 21),
            is_host: get!(row, 22),
            uninvited: get!(row, 23),
            added_from_subscribe: get!(row, 24),
            mute_until: get!(row, 25),
        })
    }
}
