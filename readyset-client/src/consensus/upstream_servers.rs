//! Storage for the upstream server lists.
//!
//! Once written, this key overrides `--upstream-db-url` and `--cdc-db-url`. The first process to
//! start seeds it from its command line; after that the lists change only through `ALTER READYSET
//! CHANGE UPSTREAM` and `ALTER READYSET CHANGE CDC`.
//!
//! The lists are stored in command-line form, credentials included.

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_util::redacted::RedactedString;
use serde::{Deserialize, Serialize};

use super::AuthorityControl;

/// Authority storage path for the upstream server lists.
pub(crate) const UPSTREAM_SERVERS_PATH: &str = "upstream_servers";

/// The persisted lists. `cdc_db_url` is `None` when CDC defaults to the upstream list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedUpstreamServers {
    pub upstream_db_url: Option<RedactedString>,
    pub cdc_db_url: Option<RedactedString>,
}

/// Extension methods on [`AuthorityControl`] for managing the upstream server lists.
#[async_trait::async_trait]
pub trait UpstreamServerStore: AuthorityControl {
    /// Return the persisted lists, or `None` if the key has never been written.
    async fn load_upstream_servers(&self) -> ReadySetResult<Option<PersistedUpstreamServers>> {
        self.try_read::<PersistedUpstreamServers>(UPSTREAM_SERVERS_PATH)
            .await
    }

    /// Return the persisted lists, initializing them from `bootstrap` if the key does not exist
    /// yet. Atomic: the first writer wins.
    async fn load_or_init_upstream_servers(
        &self,
        bootstrap: PersistedUpstreamServers,
    ) -> ReadySetResult<PersistedUpstreamServers> {
        self.read_modify_write::<_, PersistedUpstreamServers, ReadySetError>(
            UPSTREAM_SERVERS_PATH,
            move |stored| Ok(stored.unwrap_or_else(|| bootstrap.clone())),
        )
        .await?
    }

    /// Replace the `--upstream-db-url` list, returning the new value. The stored list must still
    /// match `expected`, which also seeds the key when it is absent.
    async fn set_upstream_db_url(
        &self,
        expected: PersistedUpstreamServers,
        list: RedactedString,
    ) -> ReadySetResult<PersistedUpstreamServers> {
        self.read_modify_write::<_, PersistedUpstreamServers, ReadySetError>(
            UPSTREAM_SERVERS_PATH,
            move |stored| {
                let mut lists = stored.unwrap_or_else(|| expected.clone());
                if lists.upstream_db_url != expected.upstream_db_url {
                    return Err(concurrent_edit("upstream"));
                }
                lists.upstream_db_url = Some(list.clone());
                Ok(lists)
            },
        )
        .await?
    }

    /// Like [`set_upstream_db_url`](Self::set_upstream_db_url), for the `--cdc-db-url` list.
    async fn set_cdc_db_url(
        &self,
        expected: PersistedUpstreamServers,
        list: RedactedString,
    ) -> ReadySetResult<PersistedUpstreamServers> {
        self.read_modify_write::<_, PersistedUpstreamServers, ReadySetError>(
            UPSTREAM_SERVERS_PATH,
            move |stored| {
                let mut lists = stored.unwrap_or_else(|| expected.clone());
                if lists.cdc_db_url != expected.cdc_db_url {
                    return Err(concurrent_edit("CDC"));
                }
                lists.cdc_db_url = Some(list.clone());
                Ok(lists)
            },
        )
        .await?
    }
}

/// The retryable error for an edit whose `expected` list is no longer stored.
fn concurrent_edit(which: &str) -> ReadySetError {
    ReadySetError::BadRequest(format!(
        "the {which} server list changed while this statement was validating; retry the edit \
         against the current list"
    ))
}

impl<A: AuthorityControl + ?Sized> UpstreamServerStore for A {}

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

    fn lists(upstream: &str, cdc: Option<&str>) -> PersistedUpstreamServers {
        PersistedUpstreamServers {
            upstream_db_url: Some(upstream.to_string().into()),
            cdc_db_url: cdc.map(|c| c.to_string().into()),
        }
    }

    #[tokio::test]
    async fn load_returns_none_before_any_write() {
        let authority = make_authority();
        assert!(authority.load_upstream_servers().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn first_start_seeds_then_the_stored_value_wins() {
        let authority = make_authority();
        let seed = lists("mysql://u:p@a/db|60,mysql://u:p@b/db", None);
        let first = authority
            .load_or_init_upstream_servers(seed.clone())
            .await
            .unwrap();
        assert_eq!(first, seed);

        let other = lists("mysql://u:p@z/db", Some("mysql://u:p@y/db"));
        let second = authority
            .load_or_init_upstream_servers(other)
            .await
            .unwrap();
        assert_eq!(second, seed);
        assert_eq!(authority.load_upstream_servers().await.unwrap(), Some(seed));
    }

    #[tokio::test]
    async fn edits_replace_one_list_and_keep_the_other() {
        let authority = make_authority();
        let seed = lists("mysql://u:p@a/db", Some("mysql://u:p@c/db"));
        let after_upstream = authority
            .set_upstream_db_url(
                seed.clone(),
                "mysql://u:p@a/db,mysql://u:p@b/db".to_string().into(),
            )
            .await
            .unwrap();
        assert_eq!(
            after_upstream
                .upstream_db_url
                .as_deref()
                .map(String::as_str),
            Some("mysql://u:p@a/db,mysql://u:p@b/db")
        );
        assert_eq!(after_upstream.cdc_db_url, seed.cdc_db_url);

        let after_cdc = authority
            .set_cdc_db_url(
                after_upstream.clone(),
                "mysql://u:p@d/db".to_string().into(),
            )
            .await
            .unwrap();
        assert_eq!(after_cdc.upstream_db_url, after_upstream.upstream_db_url);
        assert_eq!(
            after_cdc.cdc_db_url.as_deref().map(String::as_str),
            Some("mysql://u:p@d/db")
        );
        assert_eq!(
            authority.load_upstream_servers().await.unwrap(),
            Some(after_cdc)
        );
    }

    #[tokio::test]
    async fn an_edit_based_on_a_replaced_list_is_rejected() {
        let authority = make_authority();
        let seed = lists("mysql://u:p@a/db", Some("mysql://u:p@c/db"));
        authority
            .load_or_init_upstream_servers(seed.clone())
            .await
            .unwrap();

        authority
            .set_upstream_db_url(seed.clone(), "mysql://u:p@z/db".to_string().into())
            .await
            .unwrap();

        authority
            .set_upstream_db_url(seed.clone(), "mysql://u:p@y/db".to_string().into())
            .await
            .expect_err("a concurrent upstream edit must be rejected");

        let after_cdc = authority
            .set_cdc_db_url(seed, "mysql://u:p@d/db".to_string().into())
            .await
            .unwrap();
        assert_eq!(
            after_cdc.cdc_db_url.as_deref().map(String::as_str),
            Some("mysql://u:p@d/db")
        );
        assert_eq!(
            after_cdc.upstream_db_url.as_deref().map(String::as_str),
            Some("mysql://u:p@z/db")
        );
    }
}
