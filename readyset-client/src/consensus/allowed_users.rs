//! Storage for the adapter's allowed-users set.
//!
//! When set, this Authority key is the sole source of truth for which users may authenticate,
//! so it survives restarts and overrides the `--allowed-users` CLI/config bootstrap. The set is
//! persisted as a map from username to plaintext password, matching the in-memory representation
//! consumed on the authentication hot path.

use std::collections::HashMap;

use readyset_errors::{ReadySetError, ReadySetResult};

use super::AuthorityControl;

/// Authority storage path for the allowed-users set.
pub(crate) const ALLOWED_USERS_PATH: &str = "allowed_users";

/// Extension methods on [`AuthorityControl`] for managing the allowed-users set.
///
/// Mutations take a `seed` map used only when the key is absent: the first mutation captures the
/// current in-memory set (the CLI/config bootstrap, including the upstream-URL user) before
/// applying its delta, so restarting after the first `ALTER READYSET ADD/MODIFY/DROP USER` does
/// not silently drop the originally-configured users.
#[async_trait::async_trait]
pub trait UserStore: AuthorityControl {
    /// Return the persisted allowed-users set, or `None` if the key has never been written.
    async fn load_allowed_users(&self) -> ReadySetResult<Option<HashMap<String, String>>> {
        self.try_read::<HashMap<String, String>>(ALLOWED_USERS_PATH)
            .await
    }

    /// Return the persisted allowed-users set, initializing it from `bootstrap` if the key does
    /// not exist yet. Atomic, so concurrently-starting adapters agree on a single initial set:
    /// the first writer's `bootstrap` wins and later callers observe it. After this runs the key
    /// always exists, so the Authority is the source of truth from then on.
    async fn load_or_init_allowed_users(
        &self,
        bootstrap: HashMap<String, String>,
    ) -> ReadySetResult<HashMap<String, String>> {
        self.read_modify_write::<_, HashMap<String, String>, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| Ok(stored.unwrap_or_else(|| bootstrap.clone())),
        )
        .await?
    }

    /// Insert `user` with `password`, returning the resulting full set. Errors if `user` already
    /// exists.
    async fn add_allowed_user(
        &self,
        seed: HashMap<String, String>,
        user: String,
        password: String,
    ) -> ReadySetResult<HashMap<String, String>> {
        self.read_modify_write::<_, HashMap<String, String>, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let mut users = stored.unwrap_or_else(|| seed.clone());
                if users.contains_key(&user) {
                    return Err(ReadySetError::BadRequest(format!(
                        "user '{user}' already exists"
                    )));
                }
                users.insert(user.clone(), password.clone());
                Ok(users)
            },
        )
        .await?
    }

    /// Replace `user`'s password, returning the resulting full set. Errors if `user` does not
    /// exist.
    async fn modify_allowed_user(
        &self,
        seed: HashMap<String, String>,
        user: String,
        password: String,
    ) -> ReadySetResult<HashMap<String, String>> {
        self.read_modify_write::<_, HashMap<String, String>, ReadySetError>(
            ALLOWED_USERS_PATH,
            move |stored| {
                let mut users = stored.unwrap_or_else(|| seed.clone());
                if !users.contains_key(&user) {
                    return Err(ReadySetError::BadRequest(format!(
                        "user '{user}' not found"
                    )));
                }
                users.insert(user.clone(), password.clone());
                Ok(users)
            },
        )
        .await?
    }

    /// Remove `user`, returning the resulting full set. Errors if `user` does not exist.
    async fn drop_allowed_user(
        &self,
        seed: HashMap<String, String>,
        user: String,
    ) -> ReadySetResult<HashMap<String, String>> {
        self.read_modify_write::<_, HashMap<String, String>, ReadySetError>(
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

    fn seed() -> HashMap<String, String> {
        HashMap::from([("root".to_string(), "rootpw".to_string())])
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
        let first = authority.load_or_init_allowed_users(seed()).await.unwrap();
        assert_eq!(first, seed());
        assert_eq!(authority.load_allowed_users().await.unwrap(), Some(seed()));

        // A later start with a different bootstrap is ignored; the persisted set wins.
        let other = HashMap::from([("different".to_string(), "pw".to_string())]);
        let second = authority.load_or_init_allowed_users(other).await.unwrap();
        assert_eq!(second, seed());
    }

    #[tokio::test]
    async fn first_add_seeds_then_applies_delta() {
        let authority = make_authority();
        let result = authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        // The seed (bootstrap users) is captured alongside the newly-added user.
        assert_eq!(result.get("root").map(String::as_str), Some("rootpw"));
        assert_eq!(result.get("alice").map(String::as_str), Some("secret"));

        let loaded = authority.load_allowed_users().await.unwrap().unwrap();
        assert_eq!(loaded, result);
    }

    #[tokio::test]
    async fn add_existing_user_errors() {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        // The seed is ignored once the key exists, so re-adding a seeded user also errors.
        assert!(authority
            .add_allowed_user(HashMap::new(), "alice".to_string(), "other".to_string())
            .await
            .is_err());
        assert!(authority
            .add_allowed_user(seed(), "root".to_string(), "other".to_string())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn modify_rotates_password() {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        let result = authority
            .modify_allowed_user(HashMap::new(), "alice".to_string(), "newsecret".to_string())
            .await
            .unwrap();
        assert_eq!(result.get("alice").map(String::as_str), Some("newsecret"));
    }

    #[tokio::test]
    async fn modify_unknown_user_errors() {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        assert!(authority
            .modify_allowed_user(HashMap::new(), "bob".to_string(), "pw".to_string())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn drop_removes_user() {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        let result = authority
            .drop_allowed_user(HashMap::new(), "alice".to_string())
            .await
            .unwrap();
        assert!(!result.contains_key("alice"));
        assert!(result.contains_key("root"));
    }

    #[tokio::test]
    async fn drop_unknown_user_errors() {
        let authority = make_authority();
        authority
            .add_allowed_user(seed(), "alice".to_string(), "secret".to_string())
            .await
            .unwrap();
        assert!(authority
            .drop_allowed_user(HashMap::new(), "bob".to_string())
            .await
            .is_err());
    }
}
