//! Storage for the adapter's allowed-users set.
//!
//! When set, this Authority key is the sole source of truth for which users may authenticate,
//! so it survives restarts and overrides the `--allowed-users` CLI/config bootstrap. The set is
//! persisted as a map from username to credentials, matching the in-memory representation
//! consumed on the authentication hot path.

use std::collections::HashMap;
use std::fmt;
use std::mem;

use readyset_errors::{unsupported, ReadySetError, ReadySetResult};
use readyset_sql::Dialect;
use readyset_util::redacted::RedactedString;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::AuthorityControl;

/// Authority storage path for the allowed-users set.
pub(crate) const ALLOWED_USERS_PATH: &str = "allowed_users";

/// The passwords a single allowed user may authenticate with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserCredentials {
    /// The user's primary password.
    pub current: RedactedString,
    /// A secondary password retained during a rotation.
    pub old: Option<RedactedString>,
}

impl UserCredentials {
    /// Credentials with only a primary password.
    pub fn new(current: String) -> Self {
        Self {
            current: current.into(),
            old: None,
        }
    }
}

impl From<String> for UserCredentials {
    fn from(current: String) -> Self {
        Self::new(current)
    }
}

impl From<&str> for UserCredentials {
    fn from(current: &str) -> Self {
        Self::new(current.to_string())
    }
}

/// The persisted representation is a bare string while only a primary password exists, which is
/// byte-compatible with the plain string this key held before secondary passwords existed, in
/// both the serde_json and rmp_serde Authority formats. Only a mid-rotation entry serializes as
/// a map holding both passwords.
impl Serialize for UserCredentials {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.old {
            None => serializer.serialize_str(&self.current),
            Some(old) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("current", &self.current.0)?;
                map.serialize_entry("old", &old.0)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for UserCredentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CredentialsVisitor;

        impl<'de> Visitor<'de> for CredentialsVisitor {
            type Value = UserCredentials;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a password string or a map with current and old passwords")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UserCredentials::new(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UserCredentials::new(v))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut current = None;
                let mut old = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "current" => current = Some(map.next_value::<String>()?),
                        "old" => old = map.next_value::<Option<String>>()?,
                        _ => {
                            return Err(serde::de::Error::unknown_field(&key, &["current", "old"]));
                        }
                    }
                }
                Ok(UserCredentials {
                    current: current
                        .ok_or_else(|| serde::de::Error::missing_field("current"))?
                        .into(),
                    old: old.map(Into::into),
                })
            }
        }

        deserializer.deserialize_any(CredentialsVisitor)
    }
}

/// The full allowed-users set, keyed by username.
pub type AllowedUsersMap = HashMap<String, UserCredentials>;

/// A password mutation requested for an existing allowed user.
#[derive(Debug)]
pub enum PasswordChange {
    /// Set a new primary password. With `retain_current`, the previous primary is kept as the
    /// secondary.
    Set {
        password: RedactedString,
        retain_current: bool,
    },
    /// Retire the retained secondary password.
    DiscardOld,
}

/// Error unless `dialect` supports password rotation. `feature` names what requires it.
fn ensure_dual_password_support(
    dialect: Dialect,
    feature: fmt::Arguments<'_>,
) -> ReadySetResult<()> {
    if !dialect.supports_dual_passwords() {
        unsupported!("{feature} not supported by upstream");
    }
    Ok(())
}

/// Extension methods on [`AuthorityControl`] for managing the allowed-users set.
///
/// Mutations take a `seed` map used only when the key is absent: the first mutation captures the
/// current in-memory set (the CLI/config bootstrap, including the upstream-URL user) before
/// applying its delta, so restarting after the first `ALTER READYSET ADD/MODIFY/DROP USER` does
/// not silently drop the originally-configured users.
#[async_trait::async_trait]
pub trait UserStore: AuthorityControl {
    /// Return the persisted allowed-users set, or `None` if the key has never been written.
    async fn load_allowed_users(&self) -> ReadySetResult<Option<AllowedUsersMap>> {
        self.try_read::<AllowedUsersMap>(ALLOWED_USERS_PATH).await
    }

    /// Return the persisted allowed-users set, initializing it from `bootstrap` if the key does
    /// not exist yet. Atomic, so concurrently-starting adapters agree on a single initial set:
    /// the first writer's `bootstrap` wins and later callers observe it. After this runs the key
    /// always exists, so the Authority is the source of truth from then on.
    async fn load_or_init_allowed_users(
        &self,
        dialect: Dialect,
        bootstrap: AllowedUsersMap,
    ) -> ReadySetResult<AllowedUsersMap> {
        self.read_modify_write::<_, AllowedUsersMap, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let users = stored.unwrap_or_else(|| bootstrap.clone());
                for (user, credentials) in &users {
                    if credentials.old.is_some() {
                        ensure_dual_password_support(
                            dialect,
                            format_args!("retained password for user '{user}'"),
                        )?;
                    }
                }
                Ok(users)
            },
        )
        .await?
    }

    /// Insert `user` with `password`, returning the resulting full set. Errors if `user` already
    /// exists.
    async fn add_allowed_user(
        &self,
        seed: AllowedUsersMap,
        user: String,
        password: String,
    ) -> ReadySetResult<AllowedUsersMap> {
        self.read_modify_write::<_, AllowedUsersMap, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let mut users = stored.unwrap_or_else(|| seed.clone());
                if users.contains_key(&user) {
                    return Err(ReadySetError::BadRequest(format!(
                        "user '{user}' already exists"
                    )));
                }
                users.insert(user.clone(), UserCredentials::new(password.clone()));
                Ok(users)
            },
        )
        .await?
    }

    /// Apply `change` to `user`'s credentials, returning the resulting full set. Error if
    /// `user` does not exist.
    ///
    /// Match MySQL dual-password semantics. On a plain set, replace the primary password and
    /// leave any secondary unchanged, except that emptying the primary password also drops the
    /// secondary. When retaining, save the previous primary as the secondary, replacing any
    /// existing one. Retaining errors when either the current or the new password is empty.
    /// When discarding, drop any secondary, succeeding when there is none.
    async fn modify_allowed_user(
        &self,
        dialect: Dialect,
        seed: AllowedUsersMap,
        user: String,
        change: PasswordChange,
    ) -> ReadySetResult<AllowedUsersMap> {
        match change {
            PasswordChange::Set {
                retain_current: true,
                ..
            } => ensure_dual_password_support(dialect, format_args!("RETAIN CURRENT PASSWORD"))?,
            PasswordChange::DiscardOld => {
                ensure_dual_password_support(dialect, format_args!("DISCARD OLD PASSWORD"))?
            }
            PasswordChange::Set {
                retain_current: false,
                ..
            } => {}
        }
        self.read_modify_write::<_, AllowedUsersMap, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let mut users = stored.unwrap_or_else(|| seed.clone());
                let Some(credentials) = users.get_mut(&user) else {
                    return Err(ReadySetError::BadRequest(format!(
                        "user '{user}' not found"
                    )));
                };
                match &change {
                    PasswordChange::Set {
                        password,
                        retain_current: true,
                    } => {
                        if credentials.current.is_empty() {
                            return Err(ReadySetError::BadRequest(format!(
                                "cannot RETAIN CURRENT PASSWORD for user '{user}': current \
                                 password is empty"
                            )));
                        }
                        if password.is_empty() {
                            return Err(ReadySetError::BadRequest(format!(
                                "cannot RETAIN CURRENT PASSWORD for user '{user}': new password \
                                 is empty"
                            )));
                        }
                        credentials.old =
                            Some(mem::replace(&mut credentials.current, password.clone()));
                    }
                    PasswordChange::Set {
                        password,
                        retain_current: false,
                    } => {
                        credentials.current = password.clone();
                        // A user with an empty primary password cannot have a secondary.
                        if credentials.current.is_empty() {
                            credentials.old = None;
                        }
                    }
                    PasswordChange::DiscardOld => credentials.old = None,
                }
                Ok(users)
            },
        )
        .await?
    }

    /// Remove `user`, returning the resulting full set. Errors if `user` does not exist.
    async fn drop_allowed_user(
        &self,
        seed: AllowedUsersMap,
        user: String,
    ) -> ReadySetResult<AllowedUsersMap> {
        self.read_modify_write::<_, AllowedUsersMap, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let mut users = stored.unwrap_or_else(|| seed.clone());
                if users.remove(&user).is_none() {
                    return Err(ReadySetError::BadRequest(format!(
                        "user '{user}' not found"
                    )));
                }
                Ok(users)
            },
        )
        .await?
    }
}

impl<A: AuthorityControl + ?Sized> UserStore for A {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::consensus::{Authority, LocalAuthority, LocalAuthorityStore};

    fn make_authority() -> Authority {
        Authority::from(LocalAuthority::new_with_store(Arc::new(
            LocalAuthorityStore::new(),
        )))
    }

    fn seed() -> AllowedUsersMap {
        AllowedUsersMap::from([(
            "root".to_string(),
            UserCredentials::new("rootpw".to_string()),
        )])
    }

    fn set(password: &str) -> PasswordChange {
        PasswordChange::Set {
            password: password.to_string().into(),
            retain_current: false,
        }
    }

    fn set_retain(password: &str) -> PasswordChange {
        PasswordChange::Set {
            password: password.to_string().into(),
            retain_current: true,
        }
    }

    fn credentials(current: &str, old: Option<&str>) -> UserCredentials {
        UserCredentials {
            current: current.to_string().into(),
            old: old.map(|old| old.to_string().into()),
        }
    }

    /// An authority holding the seed users plus `alice` with `password` and no secondary.
    async fn authority_with_alice(password: &str) -> Authority {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), password.to_string())
            .await
            .unwrap();
        authority
    }

    /// Apply `change` to `alice`, returning the resulting full set.
    async fn modify_alice(
        authority: &Authority,
        change: PasswordChange,
    ) -> ReadySetResult<AllowedUsersMap> {
        authority
            .modify_allowed_user(
                Dialect::MySQL,
                AllowedUsersMap::new(),
                "alice".to_string(),
                change,
            )
            .await
    }

    /// A set with no secondary passwords serializes to the same bytes as the plain
    /// username-to-password map this key held before secondary passwords existed, and those
    /// bytes still decode, in both Authority serialization formats.
    #[test]
    fn serde_compat_with_single_password_format() {
        let legacy = HashMap::from([
            ("alice".to_string(), "pw1".to_string()),
            ("bob".to_string(), "pw2".to_string()),
        ]);
        let current = AllowedUsersMap::from([
            ("alice".to_string(), credentials("pw1", None)),
            ("bob".to_string(), credentials("pw2", None)),
        ]);

        assert_eq!(
            serde_json::to_value(&current).unwrap(),
            serde_json::to_value(&legacy).unwrap()
        );
        assert_eq!(
            serde_json::from_value::<AllowedUsersMap>(serde_json::to_value(&legacy).unwrap())
                .unwrap(),
            current
        );

        assert_eq!(
            rmp_serde::from_slice::<AllowedUsersMap>(&rmp_serde::to_vec(&legacy).unwrap()).unwrap(),
            current
        );
        assert_eq!(
            rmp_serde::from_slice::<HashMap<String, String>>(&rmp_serde::to_vec(&current).unwrap())
                .unwrap(),
            legacy
        );
    }

    /// A set mid-rotation round-trips through both Authority serialization formats.
    #[test]
    fn serde_round_trips_retained_password() {
        let users = AllowedUsersMap::from([
            ("alice".to_string(), credentials("new", Some("old"))),
            ("bob".to_string(), credentials("pw", None)),
        ]);

        assert_eq!(
            serde_json::from_slice::<AllowedUsersMap>(&serde_json::to_vec(&users).unwrap())
                .unwrap(),
            users
        );
        assert_eq!(
            rmp_serde::from_slice::<AllowedUsersMap>(&rmp_serde::to_vec(&users).unwrap()).unwrap(),
            users
        );
    }

    #[tokio::test]
    async fn load_returns_none_before_any_write() {
        let authority = make_authority();
        assert!(authority.load_allowed_users().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_or_init_seeds_then_is_authoritative() {
        let authority = make_authority();
        // First start writes the bootstrap set and returns it.
        let first = authority
            .load_or_init_allowed_users(Dialect::MySQL, seed())
            .await
            .unwrap();
        assert_eq!(first, seed());
        assert_eq!(authority.load_allowed_users().await.unwrap(), Some(seed()));

        // A later start with a different bootstrap is ignored; the persisted set wins.
        let other = AllowedUsersMap::from([("different".to_string(), credentials("pw", None))]);
        let second = authority
            .load_or_init_allowed_users(Dialect::MySQL, other)
            .await
            .unwrap();
        assert_eq!(second, seed());
    }

    #[tokio::test]
    async fn load_or_init_rejects_retained_password_without_rotation_support() {
        let bootstrap =
            AllowedUsersMap::from([("alice".to_string(), credentials("new", Some("old")))]);

        let authority = make_authority();
        let err = authority
            .load_or_init_allowed_users(Dialect::PostgreSQL, bootstrap.clone())
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("retained password for user 'alice'"));
        // The rejected bootstrap is not persisted.
        assert!(authority.load_allowed_users().await.unwrap().is_none());

        let resolved = authority
            .load_or_init_allowed_users(Dialect::MySQL, bootstrap.clone())
            .await
            .unwrap();
        assert_eq!(resolved, bootstrap);
    }

    #[tokio::test]
    async fn first_add_seeds_then_applies_delta() {
        let authority = make_authority();
        let result = authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        // The seed (bootstrap users) is captured alongside the newly-added user.
        assert_eq!(result.get("root"), Some(&credentials("rootpw", None)));
        assert_eq!(result.get("alice"), Some(&credentials("secret", None)));

        let loaded = authority.load_allowed_users().await.unwrap().unwrap();
        assert_eq!(loaded, result);
    }

    #[tokio::test]
    async fn add_existing_user_errors() {
        let authority = authority_with_alice("secret").await;
        // The seed is ignored once the key exists, so re-adding a seeded user also errors.
        assert!(authority
            .add_allowed_user(
                AllowedUsersMap::new(),
                "alice".to_string(),
                "other".to_string()
            )
            .await
            .is_err());
        assert!(authority
            .add_allowed_user(seed(), "root".to_string(), "other".to_string())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn modify_rotates_password() {
        let authority = authority_with_alice("secret").await;
        let result = modify_alice(&authority, set("newsecret")).await.unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("newsecret", None)));
    }

    #[tokio::test]
    async fn modify_unknown_user_errors() {
        let authority = authority_with_alice("secret").await;
        assert!(authority
            .modify_allowed_user(
                Dialect::MySQL,
                AllowedUsersMap::new(),
                "bob".to_string(),
                set("pw")
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn retain_saves_previous_password_as_secondary() {
        let authority = authority_with_alice("pw1").await;
        let result = modify_alice(&authority, set_retain("pw2")).await.unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("pw2", Some("pw1"))));
    }

    #[tokio::test]
    async fn retain_replaces_existing_secondary() {
        let authority = authority_with_alice("pw1").await;
        modify_alice(&authority, set_retain("pw2")).await.unwrap();
        let result = modify_alice(&authority, set_retain("pw3")).await.unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("pw3", Some("pw2"))));
    }

    #[tokio::test]
    async fn plain_set_leaves_secondary_unchanged() {
        let authority = authority_with_alice("pw1").await;
        modify_alice(&authority, set_retain("pw2")).await.unwrap();
        let result = modify_alice(&authority, set("pw3")).await.unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("pw3", Some("pw1"))));
    }

    #[tokio::test]
    async fn discard_old_retires_secondary_and_is_idempotent() {
        let authority = authority_with_alice("pw1").await;
        modify_alice(&authority, set_retain("pw2")).await.unwrap();
        let result = modify_alice(&authority, PasswordChange::DiscardOld)
            .await
            .unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("pw2", None)));

        // Discarding again, with no secondary present, still succeeds.
        let result = modify_alice(&authority, PasswordChange::DiscardOld)
            .await
            .unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("pw2", None)));
    }

    #[tokio::test]
    async fn retain_with_empty_current_password_errors() {
        let authority = authority_with_alice("").await;
        let err = modify_alice(&authority, set_retain("pw"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("current password is empty"));
    }

    #[tokio::test]
    async fn retain_with_empty_new_password_errors() {
        let authority = authority_with_alice("pw1").await;
        let err = modify_alice(&authority, set_retain("")).await.unwrap_err();
        assert!(err.to_string().contains("new password is empty"));
    }

    #[tokio::test]
    async fn plain_set_to_empty_drops_secondary() {
        let authority = authority_with_alice("pw1").await;
        modify_alice(&authority, set_retain("pw2")).await.unwrap();
        let result = modify_alice(&authority, set("")).await.unwrap();
        assert_eq!(result.get("alice"), Some(&credentials("", None)));
    }

    #[tokio::test]
    async fn drop_removes_user() {
        let authority = authority_with_alice("secret").await;
        let result = authority
            .drop_allowed_user(AllowedUsersMap::new(), "alice".to_string())
            .await
            .unwrap();
        assert!(!result.contains_key("alice"));
        assert!(result.contains_key("root"));
    }

    #[tokio::test]
    async fn drop_unknown_user_errors() {
        let authority = authority_with_alice("secret").await;
        assert!(authority
            .drop_allowed_user(AllowedUsersMap::new(), "bob".to_string())
            .await
            .is_err());
    }
}
