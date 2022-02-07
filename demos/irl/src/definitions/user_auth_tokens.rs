use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct UserAuthToken {
    // All columns from `user_auth_tokens`
    id: i64,                  // int unsigned NOT NULL AUTO_INCREMENT,
    user_id: i64,             // int NOT NULL,
    device_token: String,     // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    auth_token: String,       // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    sns_arn: String,          // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    device_type: String,      // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String,       // timestamp NULL DEFAULT NULL,
    updated_at: String,       // timestamp NULL DEFAULT NULL,
    version: String,          // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    granted_notifs: i64,      // tinyint NOT NULL DEFAULT '0',
    device_model: String,     // varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    uuid: String,             // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    total_failed_notifs: i64, // int NOT NULL DEFAULT '0',
    last_used_at: String,     // datetime DEFAULT CURRENT_TIMESTAMP,
    os_version: String,       // varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
}

impl FromRow for UserAuthToken {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(UserAuthToken {
            id: get!(row, 0),
            user_id: get!(row, 1),
            device_token: get!(row, 2),
            auth_token: get!(row, 3),
            sns_arn: get!(row, 4),
            device_type: get!(row, 5),
            created_at: get!(row, 6),
            updated_at: get!(row, 7),
            version: get!(row, 8),
            granted_notifs: get!(row, 9),
            device_model: get!(row, 10),
            uuid: get!(row, 11),
            total_failed_notifs: get!(row, 12),
            last_used_at: get!(row, 13),
            os_version: get!(row, 14),
        })
    }
}
