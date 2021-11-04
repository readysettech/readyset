mod definitions;

use crate::definitions::explore_section::ExploreSection;
use crate::definitions::external_account::ExternalAccount;
use crate::definitions::external_calendars_inner_join_external_invites::ExternalCalendarsInnerJoinExternalInvites;
use crate::definitions::external_products::ExternalProduct;
use crate::definitions::group::Group;
use crate::definitions::groups_inner_join_invites_groups::GroupsInnerJoinInvitesGroups;
use crate::definitions::groups_inner_join_users_groups::GroupsInnerJoinUsersGroups;
use crate::definitions::hashtag::Hashtag;
use crate::definitions::hashtags_inner_join_invites_hashtags::HashtagsInnerJoinInvitesHashtags;
use crate::definitions::hashtags_inner_join_users_hashtags::HashtagsInnerJoinUsersHashtags;
use crate::definitions::invite_filter::InviteFilter;
use crate::definitions::invites::Invites;
use crate::definitions::invites_group::InvitesGroup;
use crate::definitions::invites_inner_join_invites_group::InvitesInnerJoinInvitesGroup;
use crate::definitions::invites_inner_join_invites_user::InvitesInnerJoinInvitesUser;
use crate::definitions::invites_user::InvitesUser;
use crate::definitions::organizations_inner_join_external_accounts::OrganizationInnerJoinExternalAccounts;
use crate::definitions::paid_group_seeded_data::PaidGroupSeededData;
use crate::definitions::recurring_invite::RecurringInvite;
use crate::definitions::suggested_groups::SuggestedGroup;
use crate::definitions::suggested_invite::SuggestedInvite;
use crate::definitions::user::User;
use crate::definitions::user_auth_tokens::UserAuthToken;
use crate::definitions::user_friend::UserFriend;
use crate::definitions::user_group::UserGroup;
use crate::definitions::user_id_inner_join_users_groups::UserIdInnerJoinUsersGroups;
use crate::definitions::users_inner_join_users_friends::UserInnerJoinUsersFriends;
use crate::definitions::users_inner_join_users_groups::UserInnerJoinUsersGroups;
use clap::Parser;
use mysql::prelude::{FromRow, Queryable};
use mysql::{Conn, Params};
use std::env;
use std::time::Duration;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

/// Runs all the IRL queries against the provided
/// MySQL database (which must have the schemas already installed).
/// This script will not panic, and it will report
/// how many queries succeeded and how many failed in the logs.
/// The failed queries will be showed along the error message.
#[derive(Parser, Debug)]
struct IrlQueryTest {
    /// The URL to the MySQL database.
    #[clap(default_value = "mysql://127.0.0.1:3307")]
    mysql_url: String,
}

impl IrlQueryTest {
    async fn run(&self) {
        let opts = mysql::Opts::from_url(self.mysql_url.as_str()).unwrap();
        let mut adapter_conn = mysql::Conn::new(opts).unwrap();
        let mut total = 0;
        let mut failed = 0;
        let mut success = 0;

        /// Evaluates a given query with its parameters.
        /// $result = The expected query result TYPE. Must implement `FromRow`.
        /// $query = The query to be executed.
        /// $params = The parameters to use. Must implement `Into<Params>`.
        macro_rules! evaluate {
            ($(($result:ty, $query:expr, $params:expr);)+) => {
                $(evaluate!(@impl, $result, $query, $params);)+
            };
            (@impl, $result:ty, $query:expr, $params:expr) => {
                match evaluate_query::<$result, _>(&mut adapter_conn, $query, $params) {
                    Ok(_) => {
                        success += 1;
                    },
                    Err(_) => {
                        failed += 1;
                    },
                }
                total += 1;
            };
        }

        let params_invites_join_invites_users = Params::Positional(vec![
            1.into(),
            2.into(),
            4.into(),
            5.into(),
            6.into(),
            7.into(),
            8.into(),
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
            6.into(),
            1.into(),
            2.into(),
            3.into(),
            "1,2,3".into(),
            "4,5,6".into(),
            2.into(),
            Duration::new(0, 0).into(),
        ]);

        evaluate!(
             (UserInnerJoinUsersGroups,
                 r"SELECT users.*, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                 FROM users INNER JOIN users_groups ON users.id = users_groups.user_id 
                 WHERE users_groups.group_id = ? AND users_groups.pending = ? AND users_groups.is_requested = ? AND 
                 users_groups.denied_at IS ? AND users_groups.deleted_at IS ? ORDER BY field(users_groups.user_id, ?) DESC LIMIT ?",
                (1, 2, 3, Duration::new(0, 0), Duration::new(0, 0), "", 1));
             (User, r"SELECT * FROM users WHERE users.id = ? LIMIT ?", (2, 3));
             (UserAuthToken, r"SELECT * FROM user_auth_tokens WHERE auth_token = ? LIMIT ?", (2, 3));
             (UserIdInnerJoinUsersGroups, r"SELECT users.id, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id
                WHERE users_groups.group_id = ? AND is_requested = ? AND pending = ? AND users.id > ? ORDER BY users.id ASC LIMIT ?",
                 (2,3,1,5,6));
             ((i64,), r"SELECT count(*) FROM users WHERE username = ? AND id <> ?", ("", 1));
             (User, r"SELECT * FROM users WHERE id = ? LIMIT ?", (1, 2)); // Duplicated
             (User, r"SELECT * FROM users WHERE users.id in (?)", ("2,3,4",));
             // Where did this table come from?
             ((String,), r"SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?",
                 ("", ""));
             (HashtagsInnerJoinInvitesHashtags, r"SELECT hashtags.*, invites_hashtags.invite_id, invites_hashtags.hashtag_id, invites_hashtags.created_at, invites_hashtags.updated_at
                FROM hashtags INNER JOIN invites_hashtags ON hashtags.id = invites_hashtags.hashtag_id
                WHERE invites_hashtags.invite_id in (?)", ("1,2,3",));
             (InvitesUser, r"SELECT * FROM invites_users WHERE invites_users.invite_id in (?) AND invites_users.user_id = ?", ("1,2,3", 2));
             (GroupsInnerJoinInvitesGroups, r"SELECT DISTINCT groups.*, invites_groups.invite_id, invites_groups.group_id, invites_groups.created_at, invites_groups.updated_at
                FROM groups INNER JOIN invites_groups ON groups.id = invites_groups.group_id INNER JOIN users_groups ON users_groups.group_id = groups.id
                WHERE groups.deleted_at IS ? AND invites_groups.deleted_at IS ? AND invites_groups.invite_id in (?) AND
                (users_groups.pending = ? AND users_groups.is_requested = ? AND users_groups.denied_at IS ? AND
                users_groups.deleted_at IS ? AND (users_groups.user_id = ?) OR groups.is_public = ?)",
                 (Duration::new(0, 0), Duration::new(0, 0), "1,2,3", 2, 3, Duration::new(0, 0), Duration::new(0, 0), 2, 3));
             (ExternalCalendarsInnerJoinExternalInvites, r"SELECT external_calendars.*, external_invites.invite_id
                FROM external_calendars INNER JOIN external_invites ON external_invites.external_calendar_id = external_calendars.id
                WHERE external_invites.invite_id in (?)",
                 ("3,4,5",));
             (OrganizationInnerJoinExternalAccounts, r"SELECT organizations.*, external_accounts.user_id, external_accounts.organization_id
                FROM organizations INNER JOIN external_accounts ON organizations.id = external_accounts.organization_id
                WHERE external_accounts.user_id in (?)",
                 ("5,6,7",));
             ((i64,), r"UPDATE users SET last_session = ? users.updated_at = ? WHERE id = ?", ("", "", 2));
             (OrganizationInnerJoinExternalAccounts, r"SELECT organizations.*, external_accounts.user_id, external_accounts.organization_id
                FROM organizations INNER JOIN external_accounts ON organizations.id = external_accounts.organization_id
                WHERE external_accounts.user_id = ? AND organizations.content_level > ? AND organizations.user_id IS NOT ?",
                 (1, 2, 3));
             (HashtagsInnerJoinUsersHashtags, r"SELECT hashtags.*, users_hashtags.user_id, users_hashtags.hashtag_id, users_hashtags.created_at, users_hashtags.updated_at
                FROM hashtags INNER JOIN users_hashtags ON hashtags.id = users_hashtags.hashtag_id
                WHERE users_hashtags.user_id = ?", (2,));
             (Invites, r"SELECT * FROM invites WHERE ? = ?", ("id", ""));
             ((i64,), r"SELECT count(*) FROM groups INNER JOIN users_groups ON groups.id = users_groups.group_id LEFT JOIN mutual_follow_groups ON groups.id = mutual_follow_groups.group_id
                WHERE users_groups.user_id = ? AND mutual_follow_groups.group_id IS ? AND groups.deleted_at IS ? AND
                users_groups.deleted_at IS ? AND (groups.id in (?) OR groups.is_paid = ? OR groups.is_public = ?)",
                 (1, 2, Duration::new(0, 0), Duration::new(0, 0), "1,2,3", 1, 2));
             (GroupsInnerJoinUsersGroups, r"SELECT groups.*, users_groups.user_id, users_groups.group_id, users_groups.chat_tab_updated_at, users_groups.is_seen, users_groups.inviter_id, users_groups.id, users_groups.pending, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM groups INNER JOIN users_groups ON groups.id = users_groups.group_id LEFT JOIN mutual_follow_groups ON groups.id = mutual_follow_groups.group_id
                WHERE users_groups.user_id = ? AND mutual_follow_groups.group_id IS ? AND groups.deleted_at IS ? AND
                users_groups.deleted_at IS ? AND (is_paid = ? OR is_public = ?) ORDER BY updated_at DESC",
                 (1, 2, Duration::new(0, 0), Duration::new(0, 0), 3, 4));
             (SuggestedGroup, r"SELECT * FROM suggested_groups WHERE is_active = ? AND suggested_groups.id in (?)", (1, "3,4,5"));
             (ExternalProduct, r"SELECT * FROM external_products
                WHERE table_name = ? AND is_for_acceptance = ? AND external_products.row_id in (?)",
                 ("", 1, "2,3,4"));
             (Invites, r"SELECT * FROM invites WHERE key = ?", ("",));
             (UserGroup, r"SELECT * FROM users_groups WHERE users_groups.group_id in (?) AND users_groups.user_id = ?", ("2,3,4", 5));
             (RecurringInvite, r"SELECT * FROM recurring_invites WHERE ? = ?", ("id", 2));
             ((i64,), r"SELECT id FROM users WHERE id in (?)", ("2,3,4",));
             (InvitesUser, r"SELECT * FROM invites_users WHERE invite_id = ? AND id > ? ORDER BY id ASC LIMIT ?", (1, 2, 3));
             (InvitesInnerJoinInvitesUser, r"SELECT invites.*, invites_users.user_id, invites_users.invite_id, invites_users.chat_tab_updated_at, invites_users.interested, invites_users.did_report, invites_users.banned, invites_users.attending, invites_users.removed, invites_users.inviter_id, invites_users.invited_at, invites_users.did_send_seen_notif, invites_users.seen_at, invites_users.did_tap_invite, invites_users.did_tap_next_time, invites_users.pending, invites_users.last_opened_at, invites_users.did_opt_out_of_recurring, invites_users.created_at, invites_users.updated_at
                FROM invites INNER JOIN invites_users ON invites.id = invites_users.invite_id 
                WHERE invites_users.user_id = ? AND did_report = ? AND (recurring_invite_index > ? OR recurring_invite_index IS ?)",
                 (1, 2, 3, 5));
             (Invites, r"SELECT * FROM invites WHERE invites.id = ? LIMIT ?", (2,3));
             (Invites, r"SELECT * FROM invites WHERE id = ? LIMIT ?", (2,3)); // Duplicated
             (Invites, r"SELECT invites.* FROM invites WHERE id in (?) AND start_date >= ? AND id NOT in (?) ORDER BY start_date ASC",
                 ("1,2,3", Duration::new(0, 0), "5,6,7"));
             ((i64,), r"SELECT count(*) FROM users WHERE email = ? AND id <> ?", ("", 2));
             ((i64,), r"SELECT invites.id FROM invites INNER JOIN invites_users ON invites.id = invites_users.invite_id
                WHERE invites_users.user_id = ? AND (recurring_invite_index > ? OR recurring_invite_index IS ?) AND
                invites.is_deleted = ? AND did_report = ? AND interested != ? AND did_tap_next_time = ? AND removed = ? AND
                pending = ? AND banned = ? AND is_reported = ? AND is_public = ? AND
                ((inviter_id IS NOT ? AND pending = ?) OR attending = ? OR interested = ?) AND
                invites.user_id NOT in (?) AND (inviter_id NOT in (?) OR inviter_id IS ?) AND date >= ? ORDER BY start_date ASC",
                 params_invites_join_invites_users.clone());
             ((i64,), r"SELECT invites.id FROM invites INNER JOIN invites_users ON invites.id = invites_users.invite_id
                WHERE invites_users.user_id = ? AND (recurring_invite_index > ? OR recurring_invite_index IS ?) AND
                invites.is_deleted = ? AND did_report = ? AND interested != ? AND did_tap_next_time = ? AND removed = ? AND
                pending = ? AND banned = ? AND is_reported = ? AND is_public = ? AND
                ((inviter_id IS NOT ? AND pending = ?) OR attending = ? OR interested = ?) AND
                invites.user_id NOT in (?) AND (inviter_id NOT in (?) OR inviter_id IS ?) AND date < ? ORDER BY start_date DESC",
                 params_invites_join_invites_users);
             (Group, r"SELECT * FROM groups WHERE id = ? LIMIT ?", (3, 3));
             ((i64,), r"SELECT id FROM users WHERE ? = ?", ("id", 3));
             (UserInnerJoinUsersGroups, r"SELECT users.*, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id 
                WHERE users_groups.group_id = ? AND users_groups.pending = ? AND users_groups.is_requested = ? AND 
                users_groups.denied_at IS ? AND users_groups.deleted_at IS ? AND user_id = ? LIMIT ?",
                 (1, 2, 3, Duration::new(0, 0), Duration::new(0, 0), 5, 6));
             (Invites, r"SELECT invites.* FROM invites WHERE id in (?) AND start_date < ? ORDER BY start_date DESC LIMIT ?", ("1,2,3", Duration::new(0, 0)));
             (UserIdInnerJoinUsersGroups, r"SELECT users.id, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id
                WHERE users_groups.group_id = ? AND users_groups.pending = ? AND users_groups.is_requested = ? AND
                users_groups.denied_at IS ? AND users_groups.deleted_at IS ? AND users.id > ? ORDER BY users.id ASC LIMIT ?",
                 (1, 2, 3, Duration::new(0, 0), Duration::new(0, 0), 2, 3));
             (InviteFilter, r"SELECT * FROM invite_filters WHERE priority IS NOT ? ORDER BY priority DESC", (3,));
             (Invites, r"SELECT invites.* FROM invites WHERE ? = ? AND start_date >= ? AND ? = ? ORDER BY start_date ASC",
                 ("id", 1, Duration::new(0, 0), "user_id", 2));
             (ExternalAccount, r"SELECT * FROM external_accounts WHERE user_id = ?", (1,));
             (Group, r"SELECT * FROM groups WHERE key = ?", ("",));
             (InviteFilter, r"SELECT * FROM invite_filters WHERE invite_filters.id in (?)", ("1,2,3",));
             (ExploreSection, r"SELECT * FROM explore_sections WHERE organization_id IS ? AND users_can_post = ?", (1, 0));
             (UserInnerJoinUsersGroups, r"SELECT users.*, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id
                WHERE users_groups.group_id = ? AND users_groups.pending = ? AND users_groups.is_requested = ? AND
                users_groups.denied_at IS ? AND users_groups.deleted_at IS ? AND users.id in (?)",
                 (1, 2,3, Duration::new(0, 0), Duration::new(0, 0), "1,2,3"));
             (UserInnerJoinUsersGroups, r"SELECT users.*, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id
                WHERE users_groups.pending = ? AND users_groups.is_requested = ? AND users_groups.denied_at IS ? AND
                users_groups.deleted_at IS ? AND users_groups.group_id in (?) LIMIT ?",
                 (1, 2, Duration::new(0, 0), Duration::new(0, 0), "1,2,3", 5));
             (UserGroup, r"SELECT * FROM users_groups WHERE user_id = ? LIMIT ?", (1, 2));
             (RecurringInvite, r"SELECT * FROM recurring_invites WHERE recurring_invites.id in (?)", ("1,2,3", ));
             (GroupsInnerJoinInvitesGroups, r"SELECT groups.*, invites_groups.invite_id, invites_groups.group_id, invites_groups.created_at, invites_groups.updated_at
                FROM groups INNER JOIN invites_groups ON groups.id = invites_groups.group_id
                WHERE invites_groups.invite_id = ? AND groups.deleted_at IS ? AND invites_groups.deleted_at IS ? AND
                groups.id > ? ORDER BY groups.id ASC LIMIT ?",
                 (1, Duration::new(0, 0), Duration::new(0, 0), 4, 5));
             (InvitesGroup, r"SELECT * FROM invites_groups WHERE invite_id = ? AND id > ? ORDER BY id ASC LIMIT ?", (1, 3, 5));
             (Invites, r"SELECT invites.* FROM invites WHERE ? = ? AND start_date < ? ORDER BY start_date DESC LIMIT ?",
                 ("id", 3, Duration::new(0, 0), 5));
             (ExternalAccount, r"SELECT * FROM external_accounts WHERE external_accounts.user_id = ? AND external_accounts.user_id IS NOT ? AND type != ?",
                 (1, 2, ""));
             (User, r"SELECT * FROM users WHERE ? = ?", ("id", 3));
             (User, r"SELECT * FROM users WHERE email = ? LIMIT ?", ("", 3));
             (User, r"SELECT * FROM users WHERE id in (?)", ("1,2,3",));
             ((i64,), r"SELECT count(*) FROM users WHERE username = ?", ("",));
             (User, r"SELECT * FROM users WHERE app_download_token = ? LIMIT ?", ("", 5));
             (SuggestedGroup, r"SELECT * FROM suggested_groups", ());
             (GroupsInnerJoinUsersGroups, r"SELECT groups.*, users_groups.user_id, users_groups.group_id, users_groups.chat_tab_updated_at, users_groups.is_seen, users_groups.inviter_id, users_groups.id, users_groups.pending, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM groups INNER JOIN users_groups ON groups.id = users_groups.group_id LEFT JOIN mutual_follow_groups ON groups.id = mutual_follow_groups.group_id
                WHERE users_groups.user_id = ? AND mutual_follow_groups.group_id IS ? AND groups.deleted_at IS ? AND
                users_groups.deleted_at IS ? AND
                (groups.id in (?) OR groups.is_paid = ? OR groups.is_public = ?) group by groups.id ORDER BY updated_at DESC",
                 (1, 2, Duration::new(0, 0), Duration::new(0, 0), "1,2,3", 1, 0));
             (Hashtag, r"SELECT * FROM hashtags WHERE ? = ?", ("id", 3));
             (User, r"SELECT * FROM users WHERE id in (?) AND id != ? AND is_deleted = ?", ("1,2,3", 4, 0));
             (User, r"SELECT * FROM users WHERE phone = ? LIMIT ?", ("", 5));
             (Group, r"SELECT * FROM groups WHERE (id = ? AND deleted_at IS ?) LIMIT ?", (1, Duration::new(0, 0), 5));
             (PaidGroupSeededData, r"SELECT * FROM paid_group_seeded_data WHERE group_id = ? LIMIT ?", (1, 2));
             (SuggestedGroup, r"SELECT * FROM suggested_groups WHERE is_active = ? AND ? = ?", (0, "id", 2));
             ((i64, i64), r"SELECT user_id, friend_id FROM users_friends outside INNER JOIN users ON outside.user_id = users.id
                WHERE outside.friend_id IN (?) AND (outside.is_deleted = ? AND outside.blocked = ? AND
                outside.request_denied = ? AND outside.requested = ? AND outside.ignored = ? AND
                outside.user_id NOT IN (?)) AND
                NOT EXISTS
                    (SELECT * FROM users_friends inside
                    WHERE inside.user_id = outside.friend_id AND inside.friend_id = outside.user_id) AND users.signed_up = ?",
                 ("1,2,3", 0, 0, "3,4,5", 1));
             (UserAuthToken, r"SELECT * FROM user_auth_tokens
                WHERE user_auth_tokens.user_id = ? AND user_auth_tokens.user_id IS NOT ? AND
                granted_notifs = ? LIMIT ?",
                 (1, 2, 1, 5));
             ((i64, String, i64), r"SELECT promoted_users_to_follow.user_id, promoted_users_to_follow.reason, COUNT (invites.id)
                FROM promoted_users_to_follow INNER JOIN users ON users.id = promoted_users_to_follow.user_id INNER JOIN invites_users ON invites_users.user_id = promoted_users_to_follow.user_id INNER JOIN invites ON invites_users.invite_id = invites.id
                WHERE promoted_users_to_follow.user_id NOT IN (?) AND promoted_users_to_follow.is_active = ? AND
                invites.date >= ? GROUP BY promoted_users_to_follow.user_id
                HAVING invite_count > ? ORDER BY promoted_users_to_follow.priority ASC LIMIT ?",
                 ("2,3,4", 1, Duration::new(0, 0), 5));
             (UserFriend, r"SELECT * FROM users_friends WHERE friend_id in (?) AND requested = ? AND ignored = ?",
                 ("1,2,3", 1, 0));
             ((i64, i64), r"SELECT user_id, follower_count FROM top_followed_users ORDER BY follower_count DESC LIMIT ?",
                 (3,));
             (SuggestedInvite, r"SELECT * FROM suggested_invites
                WHERE (date > ? OR date IS ?) AND (initialization_date <= ? OR initialization_date IS ?) AND
                is_active = ? ORDER BY priority DESC",
                 (Duration::new(0, 0), Duration::new(0, 0), Duration::new(0, 0), Duration::new(0, 0), 1));
             (User, r"SELECT * FROM users WHERE username = ? AND is_deleted = ? LIMIT ?",
                 ("", 0, 5));
             (User, r"SELECT * FROM users WHERE is_deleted = ? AND id NOT in (?) AND signed_up = ? AND id in (?)",
                 (0, "1,2,3", 1, "3,4,5"));
             // Is this query correct? It has variables like ":limit" which make me think they were
             // supposed to be positional parameters
             ((i64, String, String, i64),
                r"SELECT follows_of_follows.friend_id, through_followed_user.first, through_followed_user.last, COUNT(*)
                FROM users_friends INNER JOIN users_friends ON follows.friend_id = follows_of_follows.user_id INNER JOIN users ON follows_of_follows.user_id = through_followed_user.id
                WHERE (follows.user_id = :user_id_1 AND through_followed_user.is_verified = ? AND
                follows_of_follows.friend_id NOT IN
                    (SELECT DISTINCT subquery_follows.friend_id FROM users_friends
                    WHERE (subquery_follows.user_id = :user_id_2)) AND follows_of_follows.friend_id != :user_id_3)
                    GROUP BY follows_of_follows.friend_id ORDER BY number_of_followeds_following DESC LIMIT :limit",
                 (1,));
             ((i64,), r"SELECT friend_id FROM users_friends WHERE user_id = ? AND ignored = ?", (1, 0));
             (UserInnerJoinUsersGroups, r"SELECT users.*, users_groups.group_id, users_groups.user_id, users_groups.joined_at, users_groups.pending, users_groups.inviter_id, users_groups.is_seen, users_groups.deleted_at, users_groups.is_requested, users_groups.denied_at, users_groups.created_at, users_groups.updated_at
                FROM users INNER JOIN users_groups ON users.id = users_groups.user_id
                WHERE users_groups.group_id = ? AND is_requested = ? AND pending = ? AND user_id = ? LIMIT ?",
                 (1, 0, 1, 2));
             (UserInnerJoinUsersFriends, r"SELECT users.*, users_friends.user_id, users_friends.friend_id, users_friends.is_deleted, users_friends.blocked, users_friends.ignored, users_friends.requested, users_friends.request_denied, users_friends.created_at, users_friends.updated_at
                FROM users INNER JOIN users_friends ON users.id = users_friends.friend_id
                WHERE users_friends.user_id = ? AND users_friends.is_deleted = ? AND
                blocked = ? AND ignored = ? AND request_denied = ? AND requested = ?",
                 (1, 0, 1, 1, 0, 0));
             (UserGroup, r"SELECT * FROM users_groups WHERE user_id = ? AND group_id = ? LIMIT ?", (1, 2, 5));
             (InvitesInnerJoinInvitesGroup, r"SELECT invites.*, invites_groups.group_id, invites_groups.invite_id, invites_groups.created_at, invites_groups.updated_at
                FROM invites INNER JOIN invites_groups ON invites.id = invites_groups.invite_id
                WHERE invites_groups.group_id = ? AND invites.is_recurring_invite_template = ? AND
                invites_groups.deleted_at IS ? AND invites.is_deleted = ? AND
                invites.is_recurring_invite_template = ? AND invites.is_expired = ? AND
                (date = ? OR date > ?) ORDER BY start_date DESC LIMIT ?",
                 (1, 1, Duration::new(0, 0), 0, 0, 1, Duration::new(0, 0), Duration::new(0, 0), 5));
             ((i64,), r"SELECT id FROM users WHERE (? = ? OR ? = ?) AND id NOT in (?)", ("id", 1, "username", "", "1,2,3"));
             (ExternalAccount, r"SELECT * FROM external_accounts WHERE user_id = ? AND organization_id IS NOT ? LIMIT ?", (1, "2,3,4", 5));
             ((i64,), r"SELECT count(*) FROM external_accounts
                WHERE external_accounts.user_id = ? AND
                external_accounts.user_id IS NOT ? AND type != ? AND type = ?",
                 (1, 2, ""));
             (UserInnerJoinUsersFriends, r"SELECT users.*, users_friends.friend_id, users_friends.user_id, users_friends.is_deleted, users_friends.blocked, users_friends.ignored, users_friends.requested, users_friends.request_denied, users_friends.created_at, users_friends.updated_at
                FROM users INNER JOIN users_friends ON users.id = users_friends.user_id
                WHERE users_friends.friend_id = ? AND users_friends.is_deleted = ? AND
                blocked = ? AND ignored = ? AND request_denied = ? AND
                ignored = ? AND requested = ? ORDER BY users_friends.user_id DESC LIMIT ?",
                 (1, 0, 0, 1, 0, 0, 1, 5)); // Using `ignored = ?` twice...
             (InvitesInnerJoinInvitesGroup, r"SELECT invites.*, invites_groups.group_id, invites_groups.invite_id, invites_groups.created_at, invites_groups.updated_at
                FROM invites INNER JOIN invites_groups ON invites.id = invites_groups.invite_id
                WHERE invites_groups.group_id = ? AND invites.is_recurring_invite_template = ? AND
                (date = ? OR date > ?) AND invites.is_deleted = ? AND invites.is_expired = ?",
                 (1, 0, Duration::new(0, 0), Duration::new(0, 0), 0, 1));
        );

        info!(
            "run a total of {} queries, {} succeeded and {} failed",
            total, success, failed
        );
    }
}

fn evaluate_query<R, T>(adapter_conn: &mut Conn, query: &str, params: T) -> Result<(), ()>
where
    R: FromRow,
    T: Into<Params>,
{
    debug!("running query");
    match adapter_conn.exec::<R, _, _>(query, params) {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = e.to_string();
            info!(%query, %err_msg, "query failed");
            Err(())
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::new(
            env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned()),
        ))
        .init();

    let opts = IrlQueryTest::parse();
    opts.run().await;
}
