use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

use crate::get;

#[allow(dead_code)]
pub struct RecurringInvite {
    // All columns from `recurring_invites`
    id: i64,                         // int unsigned NOT NULL AUTO_INCREMENT,
    frequency: String,               // varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
    week_start: String,              // varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    by_day: String,                  // varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    end_date: String,                // datetime DEFAULT NULL,
    count: i64,                      // int DEFAULT NULL,
    google_recurring_string: String, // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    google_calendar_id: String,      // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    created_at: String,              // timestamp NULL DEFAULT NULL,
    updated_at: String,              // timestamp NULL DEFAULT NULL,
    is_deleted: i64,                 // tinyint NOT NULL DEFAULT '0',
    timezone: String, // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'America/Los_Angeles',
    interval: i64,    // tinyint NOT NULL DEFAULT '1',
    by_month: String, // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
}

impl FromRow for RecurringInvite {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(RecurringInvite {
            id: get!(row, 0),
            frequency: get!(row, 1),
            week_start: get!(row, 2),
            by_day: get!(row, 3),
            end_date: get!(row, 4),
            count: get!(row, 5),
            google_recurring_string: get!(row, 6),
            google_calendar_id: get!(row, 7),
            created_at: get!(row, 8),
            updated_at: get!(row, 9),
            is_deleted: get!(row, 10),
            timezone: get!(row, 11),
            interval: get!(row, 12),
            by_month: get!(row, 13),
        })
    }
}
