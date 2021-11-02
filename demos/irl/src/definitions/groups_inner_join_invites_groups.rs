use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct GroupsInnerJoinInvitesGroups {
    // All columns from `groups`
    id: i64,                                 // bigint unsigned NOT NULL AUTO_INCREMENT,
    name: String,                            // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    user_id: i64,                            // int unsigned NOT NULL,
    deleted_at: String,                      // datetime DEFAULT NULL,
    created_at: String,                      // timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at: String,                      // timestamp NULL DEFAULT NULL,
    suggested_group_id: i64,                 // int DEFAULT NULL,
    image: String,                           // varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    key: String,                             // varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    chat_tab_updated_at: String,             // datetime DEFAULT NULL,
    organization_id: i64,                    // int DEFAULT NULL,
    is_official_for_org: i64,                // int NOT NULL DEFAULT '0',
    can_send_invites: i64,                   // tinyint NOT NULL DEFAULT '1',
    description: String,                     // text COLLATE utf8mb4_unicode_ci,
    add_all_org_members: i64,                // tinyint NOT NULL DEFAULT '0',
    is_paid: i64,                            // tinyint NOT NULL DEFAULT '0',
    is_restricted: i64,                      // tinyint(1) NOT NULL DEFAULT '0',
    other_user_id: i64,                      // int DEFAULT NULL,
    is_public: i64,                          // tinyint(1) NOT NULL DEFAULT '0',
    latitude: f64,                           // double(18,12) unsigned DEFAULT NULL,
    longitude: f64,                          // double(18,12) DEFAULT NULL,
    address: String, // varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    chat_messages_disabled_for_members: i64, // tinyint NOT NULL DEFAULT '0',
    chat_images_disabled_for_members: i64, // tinyint NOT NULL DEFAULT '0',
    chat_polls_disabled_for_members: i64, // tinyint NOT NULL DEFAULT '0',
    is_request_to_join: i64, // tinyint NOT NULL DEFAULT '0',
    is_approved_for_explore: i64, // tinyint NOT NULL DEFAULT '0',
    is_name_auto_generated: i64, // tinyint NOT NULL DEFAULT '0',
    // Columns from `invites_groups`
    ih_invite_id: i64,     // int unsigned NOT NULL,
    ih_group_id: i64,      // int unsigned NOT NULL,
    ih_created_at: String, // timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    ih_updated_at: String, // timestamp NULL DEFAULT NULL,
}

impl FromRow for GroupsInnerJoinInvitesGroups {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(GroupsInnerJoinInvitesGroups {
            id: get!(row, 0),
            name: get!(row, 1),
            user_id: get!(row, 2),
            deleted_at: get!(row, 3),
            created_at: get!(row, 4),
            updated_at: get!(row, 5),
            suggested_group_id: get!(row, 6),
            image: get!(row, 7),
            key: get!(row, 8),
            chat_tab_updated_at: get!(row, 9),
            organization_id: get!(row, 10),
            is_official_for_org: get!(row, 11),
            can_send_invites: get!(row, 12),
            description: get!(row, 13),
            add_all_org_members: get!(row, 14),
            is_paid: get!(row, 15),
            is_restricted: get!(row, 16),
            other_user_id: get!(row, 17),
            is_public: get!(row, 18),
            latitude: get!(row, 19),
            longitude: get!(row, 20),
            address: get!(row, 21),
            chat_messages_disabled_for_members: get!(row, 22),
            chat_images_disabled_for_members: get!(row, 23),
            chat_polls_disabled_for_members: get!(row, 24),
            is_request_to_join: get!(row, 25),
            is_approved_for_explore: get!(row, 26),
            is_name_auto_generated: get!(row, 27),
            ih_invite_id: get!(row, 28),
            ih_group_id: get!(row, 29),
            ih_created_at: get!(row, 30),
            ih_updated_at: get!(row, 31),
        })
    }
}
