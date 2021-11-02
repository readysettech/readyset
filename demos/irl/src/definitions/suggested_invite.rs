use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct SuggestedInvite {
    // All columns from `suggested_invites`
    id: i64,                                // int unsigned NOT NULL AUTO_INCREMENT,
    name: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    priority: i64, // int DEFAULT '0',
    date: String, // date DEFAULT NULL,
    filter_id: i64, // int DEFAULT NULL,
    gradient_id: i64, // int DEFAULT '1',
    is_active: i64, // tinyint(1) DEFAULT '0',
    hex_color: String, // varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    kind: i64,    // smallint DEFAULT NULL,
    group_id: i64, // int DEFAULT NULL,
    global: i64,  // tinyint(1) DEFAULT '1',
    first_comment: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    latitude: f64,         // double(18,12) DEFAULT NULL,
    longitude: f64,        // double(18,12) DEFAULT NULL,
    created_at: String,    // timestamp NULL DEFAULT NULL,
    updated_at: String,    // timestamp NULL DEFAULT NULL,
    gender: String, // varchar(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    giveaway_num_wins: i64, // tinyint NOT NULL DEFAULT '0',
    giveaway_joining_limit: i64, // tinyint NOT NULL DEFAULT '0',
    giveaway_dialogue_title: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    giveaway_dialogue_description: String, // text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    giveaway_win_comment: String,    // text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    giveaway_lose_comment_appended: String, // varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    is_live_giveaway: i64,                  // tinyint NOT NULL DEFAULT '0',
    suggested_invite_id: i64,               // int DEFAULT NULL,
    image: String, // varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    initialization_date: String, // date DEFAULT NULL,
}

impl FromRow for SuggestedInvite {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(SuggestedInvite {
            id: get!(row, 0),
            name: get!(row, 1),
            priority: get!(row, 2),
            date: get!(row, 3),
            filter_id: get!(row, 4),
            gradient_id: get!(row, 5),
            is_active: get!(row, 6),
            hex_color: get!(row, 7),
            kind: get!(row, 8),
            group_id: get!(row, 9),
            global: get!(row, 10),
            first_comment: get!(row, 11),
            latitude: get!(row, 12),
            longitude: get!(row, 13),
            created_at: get!(row, 14),
            updated_at: get!(row, 15),
            gender: get!(row, 16),
            giveaway_num_wins: get!(row, 17),
            giveaway_joining_limit: get!(row, 18),
            giveaway_dialogue_title: get!(row, 19),
            giveaway_dialogue_description: get!(row, 20),
            giveaway_win_comment: get!(row, 21),
            giveaway_lose_comment_appended: get!(row, 22),
            is_live_giveaway: get!(row, 23),
            suggested_invite_id: get!(row, 24),
            image: get!(row, 25),
            initialization_date: get!(row, 26),
        })
    }
}
