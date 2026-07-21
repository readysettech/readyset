//! Storage for MCP (Model Context Protocol) authentication tokens.
//!
//! Tokens are stored in the Authority so that all adapter instances in the cluster
//! can validate them against the same store. Token values are hashed before storage.

use chrono::{DateTime, Utc};
use readyset_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::AuthorityControl;

/// Authority storage path for MCP tokens.
pub(crate) const MCP_TOKENS_PATH: &str = "mcp_tokens";

/// The scope of permissions granted by an MCP token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum McpTokenScope {
    /// SHOW commands, EXPLAIN, status queries only.
    ReadOnly,
    /// ReadOnly + CREATE/DROP CACHE.
    CacheAdmin,
    /// All operations including ALTER READYSET.
    Full,
}

impl std::fmt::Display for McpTokenScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "read_only"),
            Self::CacheAdmin => write!(f, "cache_admin"),
            Self::Full => write!(f, "full"),
        }
    }
}

/// A stored MCP token record. The raw token value is never persisted —
/// only its SHA-256 hash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct McpToken {
    /// Human-readable identifier chosen by the user.
    pub name: String,
    /// SHA-256 hash of the token value (lowercase hex).
    pub hash: String,
    /// Permission scope this token grants.
    pub scope: McpTokenScope,
    /// When the token was created.
    pub created_at: DateTime<Utc>,
    /// When the token expires. `None` means the token never expires.
    pub expires_at: Option<DateTime<Utc>>,
}

impl McpToken {
    /// Hash a raw token value using SHA-256.
    pub fn hash_value(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Returns true if the token has an expiration date that has passed.
    pub fn is_expired(&self) -> bool {
        matches!(self.expires_at, Some(exp) if exp <= Utc::now())
    }
}

/// Extension methods on [`AuthorityControl`] for managing MCP tokens.
#[async_trait::async_trait]
pub trait McpTokenStore: AuthorityControl {
    /// Return all stored tokens.
    async fn mcp_tokens(&self) -> ReadySetResult<Vec<McpToken>> {
        Ok(self
            .try_read::<Vec<McpToken>>(MCP_TOKENS_PATH)
            .await?
            .unwrap_or_default())
    }

    /// Insert a new token. Returns an error if a token with the same name already exists.
    async fn add_mcp_token(&self, token: McpToken) -> ReadySetResult<()> {
        self.read_modify_write::<_, Vec<McpToken>, ReadySetError>(MCP_TOKENS_PATH, move |tokens| {
            let mut tokens = tokens.unwrap_or_default();
            if tokens.iter().any(|t| t.name == token.name) {
                return Err(ReadySetError::Internal(format!(
                    "MCP token '{}' already exists",
                    token.name
                )));
            }
            tokens.push(token.clone());
            Ok(tokens)
        })
        .await??;
        Ok(())
    }

    /// Remove a token by name. Returns an error if the token does not exist.
    async fn remove_mcp_token(&self, name: &str) -> ReadySetResult<()> {
        let name = name.to_string();
        self.read_modify_write::<_, Vec<McpToken>, ReadySetError>(MCP_TOKENS_PATH, move |tokens| {
            let mut tokens = tokens.unwrap_or_default();
            let before = tokens.len();
            tokens.retain(|t| t.name != name);
            if tokens.len() == before {
                return Err(ReadySetError::Internal(format!(
                    "MCP token '{name}' not found"
                )));
            }
            Ok(tokens)
        })
        .await??;
        Ok(())
    }

    /// Look up a token by its raw value. Returns `Some(token)` only if the hash matches
    /// a stored token and the token has not expired.
    async fn find_mcp_token(&self, value: &str) -> ReadySetResult<Option<McpToken>> {
        let hash = McpToken::hash_value(value);
        Ok(self
            .mcp_tokens()
            .await?
            .into_iter()
            .find(|t| t.hash == hash && !t.is_expired()))
    }

    /// Update the expiration timestamp on an existing token. `expires_at = None`
    /// removes any previous expiration (the token becomes non-expiring).
    /// Returns an error if the token does not exist.
    async fn set_mcp_token_expires_at(
        &self,
        name: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> ReadySetResult<()> {
        let name = name.to_string();
        self.read_modify_write::<_, Vec<McpToken>, ReadySetError>(MCP_TOKENS_PATH, move |tokens| {
            let mut tokens = tokens.unwrap_or_default();
            match tokens.iter_mut().find(|t| t.name == name) {
                Some(token) => {
                    token.expires_at = expires_at;
                    Ok(tokens)
                }
                None => Err(ReadySetError::Internal(format!(
                    "MCP token '{name}' not found"
                ))),
            }
        })
        .await??;
        Ok(())
    }
}

impl<A: AuthorityControl + ?Sized> McpTokenStore for A {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Duration;

    use super::*;
    use crate::consensus::{Authority, LocalAuthority, LocalAuthorityStore};

    fn make_token(name: &str, value: &str, scope: McpTokenScope) -> McpToken {
        McpToken {
            name: name.to_string(),
            hash: McpToken::hash_value(value),
            scope,
            created_at: Utc::now(),
            expires_at: None,
        }
    }

    fn make_authority() -> Authority {
        Authority::from(LocalAuthority::new_with_store(Arc::new(
            LocalAuthorityStore::new(),
        )))
    }

    #[tokio::test]
    async fn add_and_find_token() {
        let authority = make_authority();
        let token = make_token("claude", "rs_mcp_secret", McpTokenScope::ReadOnly);
        authority.add_mcp_token(token.clone()).await.unwrap();

        let found = authority.find_mcp_token("rs_mcp_secret").await.unwrap();
        assert_eq!(found, Some(token));
    }

    #[tokio::test]
    async fn find_returns_none_for_unknown_value() {
        let authority = make_authority();
        assert!(authority
            .find_mcp_token("nonexistent")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn cannot_add_duplicate_name() {
        let authority = make_authority();
        let token = make_token("dup", "v1", McpTokenScope::ReadOnly);
        authority.add_mcp_token(token.clone()).await.unwrap();

        let dup = make_token("dup", "v2", McpTokenScope::Full);
        assert!(authority.add_mcp_token(dup).await.is_err());
    }

    #[tokio::test]
    async fn remove_token_by_name() {
        let authority = make_authority();
        let token = make_token("x", "val", McpTokenScope::ReadOnly);
        authority.add_mcp_token(token).await.unwrap();

        authority.remove_mcp_token("x").await.unwrap();
        assert!(authority.find_mcp_token("val").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn remove_unknown_name_errors() {
        let authority = make_authority();
        assert!(authority.remove_mcp_token("nope").await.is_err());
    }

    #[tokio::test]
    async fn expired_tokens_are_not_found() {
        let authority = make_authority();
        let mut token = make_token("expired", "val", McpTokenScope::ReadOnly);
        token.expires_at = Some(Utc::now() - Duration::seconds(1));
        authority.add_mcp_token(token).await.unwrap();

        assert!(authority.find_mcp_token("val").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_expires_at_extends_lifetime() {
        let authority = make_authority();
        let mut token = make_token("t", "val", McpTokenScope::ReadOnly);
        token.expires_at = Some(Utc::now() - Duration::seconds(1));
        authority.add_mcp_token(token).await.unwrap();
        // Token is expired — not findable.
        assert!(authority.find_mcp_token("val").await.unwrap().is_none());

        // Push the expiration into the future.
        let future = Utc::now() + Duration::hours(1);
        authority
            .set_mcp_token_expires_at("t", Some(future))
            .await
            .unwrap();

        let found = authority.find_mcp_token("val").await.unwrap();
        assert_eq!(found.as_ref().map(|t| t.expires_at), Some(Some(future)));
    }

    #[tokio::test]
    async fn set_expires_at_removes_expiration() {
        let authority = make_authority();
        let mut token = make_token("t", "val", McpTokenScope::ReadOnly);
        token.expires_at = Some(Utc::now() + Duration::hours(1));
        authority.add_mcp_token(token).await.unwrap();

        authority.set_mcp_token_expires_at("t", None).await.unwrap();

        let found = authority.find_mcp_token("val").await.unwrap().unwrap();
        assert_eq!(found.expires_at, None);
    }

    #[tokio::test]
    async fn set_expires_at_unknown_name_errors() {
        let authority = make_authority();
        assert!(authority
            .set_mcp_token_expires_at("missing", None)
            .await
            .is_err());
    }

    #[test]
    fn hash_is_deterministic() {
        let a = McpToken::hash_value("secret");
        let b = McpToken::hash_value("secret");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn different_values_hash_differently() {
        assert_ne!(
            McpToken::hash_value("secret-1"),
            McpToken::hash_value("secret-2")
        );
    }

    #[test]
    fn never_expires_when_expires_at_is_none() {
        let token = McpToken {
            name: "t".to_string(),
            hash: "h".to_string(),
            scope: McpTokenScope::ReadOnly,
            created_at: Utc::now(),
            expires_at: None,
        };
        assert!(!token.is_expired());
    }

    #[test]
    fn expires_when_expires_at_in_the_past() {
        let token = McpToken {
            name: "t".to_string(),
            hash: "h".to_string(),
            scope: McpTokenScope::ReadOnly,
            created_at: Utc::now(),
            expires_at: Some(Utc::now() - Duration::seconds(1)),
        };
        assert!(token.is_expired());
    }

    #[test]
    fn not_expired_when_expires_at_in_the_future() {
        let token = McpToken {
            name: "t".to_string(),
            hash: "h".to_string(),
            scope: McpTokenScope::ReadOnly,
            created_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::hours(1)),
        };
        assert!(!token.is_expired());
    }
}
