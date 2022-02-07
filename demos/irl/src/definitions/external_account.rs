use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct ExternalAccount {
    // All columns from `external_accounts`
    id: i64,            // bigint unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,       // int unsigned NOT NULL,
    image: String,      // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    is_default: String, // tinyint(1) NOT NULL DEFAULT '0',
    external_user_id: String, /* varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT
                         * NULL, */
    email: String,               // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    ea_type: String,             // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    access_token: String,        // text COLLATE utf8mb4_unicode_ci NOT NULL,
    expire_token: i64,           // bigint NOT NULL,
    refresh_token: String,       // text COLLATE utf8mb4_unicode_ci NOT NULL,
    needs_re_authorization: i64, // tinyint NOT NULL DEFAULT '0',
    should_email_people_in_events: i64, // tinyint NOT NULL DEFAULT '0',
    callback_token: String,      // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    callback_id: String,         // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    callback_token_expiration: i64, // bigint NOT NULL,
    created_at: String,          // timestamp NULL DEFAULT NULL,
    updated_at: String,          // timestamp NULL DEFAULT NULL,
    add_irl_message_in_all_events: i64, // tinyint NOT NULL DEFAULT '0',
    first: String,               // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    last: String,                // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    phone: String,               // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    last_full_sync_start_date: String, // datetime DEFAULT NULL,
    last_full_sync_completion_date: String, // datetime DEFAULT NULL,
    organization_id: i64,        // int DEFAULT NULL,
    first_access_token: String,  // text COLLATE utf8mb4_unicode_ci NOT NULL,
    did_move_callback: i64,      // tinyint NOT NULL DEFAULT '0',
    key_file_name: String,       // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    two_way_sync_enabled: i64,   // tinyint NOT NULL DEFAULT '0',
}

impl FromRow for ExternalAccount {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(ExternalAccount {
            id: get!(row, 0),
            user_id: get!(row, 1),
            image: get!(row, 2),
            is_default: get!(row, 3),
            external_user_id: get!(row, 4),
            email: get!(row, 5),
            ea_type: get!(row, 6),
            access_token: get!(row, 7),
            expire_token: get!(row, 8),
            refresh_token: get!(row, 9),
            needs_re_authorization: get!(row, 10),
            should_email_people_in_events: get!(row, 11),
            callback_token: get!(row, 12),
            callback_id: get!(row, 13),
            callback_token_expiration: get!(row, 14),
            created_at: get!(row, 15),
            updated_at: get!(row, 16),
            add_irl_message_in_all_events: get!(row, 17),
            first: get!(row, 18),
            last: get!(row, 19),
            phone: get!(row, 20),
            last_full_sync_start_date: get!(row, 21),
            last_full_sync_completion_date: get!(row, 22),
            organization_id: get!(row, 23),
            first_access_token: get!(row, 24),
            did_move_callback: get!(row, 25),
            key_file_name: get!(row, 26),
            two_way_sync_enabled: get!(row, 27),
        })
    }
}
