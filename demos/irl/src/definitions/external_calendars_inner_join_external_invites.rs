use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct ExternalCalendarsInnerJoinExternalInvites {
    // All columns from `external_calendars`
    id: i64,                      // bigint unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,                 // int unsigned NOT NULL,
    external_account_id: i64,     // bigint unsigned NOT NULL,
    external_calendar_id: String, // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    name: String,                 // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    last_sync: String,            // timestamp NOT NULL,
    is_enabled: i64,              // tinyint NOT NULL DEFAULT '1',
    primary: i64,                 // tinyint NOT NULL DEFAULT '0',
    next_sync_token: String,      // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    callback_expiration: String,  // varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    hex_color: String,            // varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    callback_id: String,          // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    callback_token: String,       // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String,           // timestamp NULL DEFAULT NULL,
    updated_at: String,           // timestamp NULL DEFAULT NULL,
    should_skip_callback: i64,    // tinyint NOT NULL DEFAULT '0',
    user_id_subscribing_to: i64,  // bigint DEFAULT NULL,
    is_irl_cal: i64,              // tinyint DEFAULT NULL,
    access_type: String, // varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'reader',
    // Columns from `external_invites`
    ei_invite_id: i64, // int unsigned NOT NULL,
}

impl FromRow for ExternalCalendarsInnerJoinExternalInvites {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(ExternalCalendarsInnerJoinExternalInvites {
            id: get!(row, 0),
            user_id: get!(row, 1),
            external_account_id: get!(row, 2),
            external_calendar_id: get!(row, 3),
            name: get!(row, 4),
            last_sync: get!(row, 5),
            is_enabled: get!(row, 6),
            primary: get!(row, 7),
            next_sync_token: get!(row, 8),
            callback_expiration: get!(row, 9),
            hex_color: get!(row, 10),
            callback_id: get!(row, 11),
            callback_token: get!(row, 12),
            created_at: get!(row, 13),
            updated_at: get!(row, 14),
            should_skip_callback: get!(row, 15),
            user_id_subscribing_to: get!(row, 16),
            is_irl_cal: get!(row, 17),
            access_type: get!(row, 18),
            ei_invite_id: get!(row, 19),
        })
    }
}
