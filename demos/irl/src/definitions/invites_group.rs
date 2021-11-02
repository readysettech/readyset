use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct InvitesGroup {
    // All columns from `invites_groups`
    id: i64,            // bigint unsigned NOT NULL AUTO_INCREMENT,
    invite_id: i64,     // int unsigned NOT NULL,
    group_id: i64,      // int unsigned NOT NULL,
    inviter_id: i64,    // int unsigned NOT NULL,
    deleted_at: String, // datetime DEFAULT NULL,
    created_at: String, // timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at: String, // timestamp NULL DEFAULT NULL,
}

impl FromRow for InvitesGroup {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(InvitesGroup {
            id: get!(row, 0),
            invite_id: get!(row, 1),
            group_id: get!(row, 2),
            inviter_id: get!(row, 3),
            deleted_at: get!(row, 4),
            created_at: get!(row, 5),
            updated_at: get!(row, 6),
        })
    }
}
