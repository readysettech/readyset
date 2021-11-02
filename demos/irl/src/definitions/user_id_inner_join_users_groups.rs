use crate::get;
use mysql::prelude::FromRow;
use mysql::{FromRowError, Row};

#[allow(dead_code)]
pub struct UserIdInnerJoinUsersGroups {
    // ID column from `users`
    id: u64, //  int unsigned NOT NULL AUTO_INCREMENT,
    // `users_groups` columns
    ug_group_id: i64,
    ug_user_id: i64,
    ug_joined_at: String,
    ug_pending: i64,
    ug_inviter_id: i64,
    ug_is_seen: i64,
    ug_deleted_at: String,
    ug_is_requested: i64,
    ug_denied_at: String,
    ug_created_at: String,
    ug_updated_at: String,
}

impl FromRow for UserIdInnerJoinUsersGroups {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(UserIdInnerJoinUsersGroups {
            id: get!(row, 0),
            ug_group_id: get!(row, 1),
            ug_user_id: get!(row, 2),
            ug_joined_at: get!(row, 3),
            ug_pending: get!(row, 4),
            ug_inviter_id: get!(row, 5),
            ug_is_seen: get!(row, 6),
            ug_deleted_at: get!(row, 7),
            ug_is_requested: get!(row, 8),
            ug_denied_at: get!(row, 9),
            ug_created_at: get!(row, 10),
            ug_updated_at: get!(row, 11),
        })
    }
}
