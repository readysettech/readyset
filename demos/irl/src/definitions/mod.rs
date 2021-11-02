pub mod explore_section;
pub mod external_account;
pub mod external_calendars_inner_join_external_invites;
pub mod external_products;
pub mod group;
pub mod groups_inner_join_invites_groups;
pub mod groups_inner_join_users_groups;
pub mod hashtag;
pub mod hashtags_inner_join_invites_hashtags;
pub mod hashtags_inner_join_users_hashtags;
pub mod invite_filter;
pub mod invites;
pub mod invites_group;
pub mod invites_inner_join_invites_group;
pub mod invites_inner_join_invites_user;
pub mod invites_user;
pub mod organizations_inner_join_external_accounts;
pub mod paid_group_seeded_data;
pub mod recurring_invite;
pub mod suggested_groups;
pub mod suggested_invite;
pub mod user;
pub mod user_auth_tokens;
pub mod user_friend;
pub mod user_group;
pub mod user_id_inner_join_users_groups;
pub mod users_inner_join_users_friends;
pub mod users_inner_join_users_groups;

#[macro_export]
macro_rules! get {
    ($row:expr, $position:literal) => {
        match $row.get($position) {
            Some(val) => val,
            None => return Err(FromRowError($row)),
        }
    };
}
