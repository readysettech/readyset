//! Embedded MCP HTTP server.
//!
//! When enabled, the Readyset adapter exposes the MCP (Model Context Protocol)
//! over Streamable HTTP on a dedicated port. Clients connect with
//! `Authorization: Bearer <token>` headers; tokens are validated against the
//! Authority-backed token store.
//!
//! All tool calls are dispatched via a loopback SQL connection to the adapter's
//! own SQL listener — the MCP HTTP endpoint is a thin protocol translator, not
//! a parallel query engine. This gives MCP tools the full adapter pipeline
//! (parsing, rewriting, migration, upstream connection) for free, with no
//! duplicated Backend.
//!
//! The loopback connection authenticates as the adapter's upstream-db-url user
//! because `UpstreamDatabase::set_user` is a no-op for the current upstream
//! implementations — per-bearer upstream credentials aren't plumbed through.
//! All bearers share the single upstream identity; bearer tokens gate HTTP
//! access and tool scope.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use readyset_client::consensus::Authority;
use readyset_client::consensus::mcp_tokens::{McpToken, McpTokenScope, McpTokenStore};
use readyset_mcp::server::ReadysetMcpServer;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Configuration for the embedded MCP HTTP server.
#[derive(Debug)]
pub struct McpHttpConfig {
    /// Address the MCP HTTP server listens on.
    pub listen_addr: SocketAddr,
}

/// How many HTTP requests the middleware will buffer/inspect concurrently.
/// Caps MCP load so heavy traffic (or a misbehaving client) cannot starve the
/// adapter's shared Tokio runtime.
const MCP_CONCURRENCY_LIMIT: usize = 256;

/// Maximum JSON-RPC request body size the middleware will buffer to inspect
/// the JSON-RPC method and tool name. The current tool surface fits comfortably
/// under this; larger bodies are rejected with 413 before they consume memory.
const MAX_MCP_BODY_BYTES: usize = 64 * 1024;

/// TTL on cached Authority token lookups. Positive and negative results are
/// cached for this long. Short enough that rotating a token or changing scope
/// takes effect promptly; long enough that we avoid hammering the Authority
/// on the request hot path.
const TOKEN_CACHE_TTL: Duration = Duration::from_secs(30);

/// Start the MCP HTTP server. Runs until the cancellation token is triggered.
///
/// `mcp_server` is the rmcp tool router (from `readyset-mcp`). Each MCP session
/// gets its own clone of the handler. All handlers share the same underlying
/// SQL connection pool.
pub async fn serve(
    authority: Arc<Authority>,
    mcp_server: ReadysetMcpServer,
    config: McpHttpConfig,
    cancel: CancellationToken,
) -> Result<()> {
    tracing::info!(
        addr = %config.listen_addr,
        "starting embedded MCP HTTP server"
    );

    // Silence rmcp's per-session INFO chatter (`Service initialized`, `received
    // notification`, `client initialized`) when MCP runs. We amend the effective
    // LOG_LEVEL with `rmcp=warn` unless the operator explicitly set an `rmcp`
    // directive, in which case their choice wins. Doing this here (not via a
    // global default in readyset-tracing) keeps the adapter's default log
    // filter clean and only applies when MCP is actually enabled.
    apply_rmcp_log_filter();

    let http_config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(true)
        .with_cancellation_token(cancel.child_token());

    // Disable the per-session keep-alive timeout. rmcp reaps idle sessions
    // after 5 minutes by default and logs the teardown at ERROR, which is
    // noisy for clients like Claude Code that leave the HTTP connection
    // open between interactions. Sessions are still closed on client
    // disconnect and on adapter shutdown via the cancellation token.
    let mut session_manager = LocalSessionManager::default();
    session_manager.session_config.keep_alive = None;

    let service: StreamableHttpService<ReadysetMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(mcp_server.clone()),
            Arc::new(session_manager),
            http_config,
        );

    let auth_state = AuthState {
        authority,
        cache: Arc::new(TokenCache::new(TOKEN_CACHE_TTL)),
        concurrency: Arc::new(Semaphore::new(MCP_CONCURRENCY_LIMIT)),
    };
    let router = axum::Router::new().nest_service("/mcp", service).layer(
        axum::middleware::from_fn_with_state(auth_state, bearer_auth),
    );

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .with_context(|| format!("failed to bind MCP HTTP server to {}", config.listen_addr))?;

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel.cancelled_owned().await })
        .await
        .context("MCP HTTP server error")?;

    Ok(())
}

/// Shared state for the Bearer auth middleware.
#[derive(Clone)]
struct AuthState {
    authority: Arc<Authority>,
    cache: Arc<TokenCache>,
    concurrency: Arc<Semaphore>,
}

/// Short-TTL cache over Authority-backed token lookups. Positive lookups store
/// the resolved [`McpToken`]; negative lookups (unknown token) cache `None` so
/// a bearer-spraying attacker can't force one Authority round-trip per request.
struct TokenCache {
    entries: RwLock<HashMap<String, CachedTokenEntry>>,
    ttl: Duration,
}

struct CachedTokenEntry {
    token: Option<McpToken>,
    expires_at: Instant,
}

impl TokenCache {
    fn new(ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Return the cached lookup result if it has not expired.
    fn get(&self, key: &str) -> Option<Option<McpToken>> {
        let now = Instant::now();
        let entries = self.entries.read();
        let entry = entries.get(key)?;
        if entry.expires_at <= now {
            return None;
        }
        Some(entry.token.clone())
    }

    fn insert(&self, key: String, token: Option<McpToken>) {
        let expires_at = Instant::now() + self.ttl;
        let mut entries = self.entries.write();
        // Opportunistic GC of stale entries so the map doesn't grow unbounded
        // under bearer-spraying.
        let now = Instant::now();
        entries.retain(|_, v| v.expires_at > now);
        entries.insert(key, CachedTokenEntry { token, expires_at });
    }
}

/// Axum middleware that validates `Authorization: Bearer <token>` headers
/// against the Authority-backed MCP token store, and enforces the token's
/// scope against the tool being called.
async fn bearer_auth(State(state): State<AuthState>, req: Request<Body>, next: Next) -> Response {
    // Cap concurrent in-flight MCP requests. The Semaphore permit is held for
    // the full duration of the downstream dispatch; releasing only happens on
    // drop, so this caps both body-buffering and downstream SQL concurrency.
    let _permit = match state.concurrency.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "MCP endpoint at concurrency limit",
            )
                .into_response();
        }
    };

    let header_value = match req.headers().get(header::AUTHORIZATION) {
        Some(v) => v,
        None => return unauthorized("missing Authorization header"),
    };

    let header_str = match header_value.to_str() {
        Ok(s) => s,
        Err(_) => return unauthorized("malformed Authorization header"),
    };

    let token = match header_str.strip_prefix("Bearer ") {
        Some(t) => t.trim(),
        None => return unauthorized("expected 'Bearer <token>'"),
    };

    if !is_safe_token_format(token) {
        return unauthorized("invalid token format");
    }

    // Cache lookup first. A `Some(None)` result means "we already asked the
    // Authority and this token is not valid" — short-circuit as 401 without
    // hitting the Authority again.
    let resolved: Option<McpToken> = if let Some(cached) = state.cache.get(token) {
        cached
    } else {
        match state.authority.find_mcp_token(token).await {
            Ok(t) => {
                state.cache.insert(token.to_string(), t.clone());
                t
            }
            Err(e) => {
                tracing::warn!(error = %e, "token lookup failed");
                // Don't leak Authority availability to unauthenticated callers.
                return unauthorized("invalid or expired token");
            }
        }
    };

    let info = match resolved {
        Some(info) => info,
        None => return unauthorized("invalid or expired token"),
    };

    // Only application/json bodies are accepted. Any other Content-Type —
    // form-urlencoded, multipart, octet-stream — would bypass scope_check's
    // JSON-based inspection.
    if !has_json_content_type(req.headers()) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MCP endpoint requires application/json",
        )
            .into_response();
    }

    // Content-Encoding (gzip/br/deflate) is not supported: a decompressed body
    // would bypass scope_check on the raw bytes. If upstream middleware ever
    // adds a decompression layer, this check must move after it.
    if req.headers().contains_key(header::CONTENT_ENCODING) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MCP endpoint does not accept compressed bodies",
        )
            .into_response();
    }

    // Buffer the body so we can inspect the JSON-RPC method for scope enforcement.
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, MAX_MCP_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("body read error: {e}"),
            )
                .into_response();
        }
    };

    if let Err(resp) = scope_check(&body_bytes, &info.scope) {
        return *resp;
    }

    let req = Request::from_parts(parts, Body::from(body_bytes));
    next.run(req).await
}

fn unauthorized(msg: &'static str) -> Response {
    (StatusCode::UNAUTHORIZED, msg).into_response()
}

/// True if the request's Content-Type is `application/json` (ignoring parameters
/// like `; charset=utf-8`).
fn has_json_content_type(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| {
            let media_type = ct.split(';').next().unwrap_or("").trim();
            media_type.eq_ignore_ascii_case("application/json")
        })
        .unwrap_or(false)
}

/// Enforce `scope` against the JSON-RPC request body.
///
/// The endpoint only accepts the methods on an allowlist; unknown methods are
/// rejected before they can reach the downstream service, so a future MCP
/// surface (e.g. `prompts/call`) can't bypass this by adding a method the
/// scope map doesn't know about.
///
/// JSON-RPC 2.0 batches (top-level array) are rejected with HTTP 400: an
/// attacker could otherwise smuggle a `tools/call` for a tool their token
/// doesn't permit inside a batch past a single-request scope check. The
/// 2025-06-18 MCP spec drops batching support, so this matches where the
/// protocol is headed anyway.
fn scope_check(body: &[u8], scope: &McpTokenScope) -> Result<(), Box<Response>> {
    // Fail closed on malformed JSON — rmcp wouldn't accept it anyway, and
    // returning OK here on parse failure is an easy way to mis-route bytes
    // past the gate.
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "request body is not valid JSON").into_response(),
            ));
        }
    };
    if v.is_array() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "batched JSON-RPC requests are not supported",
            )
                .into_response(),
        ));
    }

    let method = v.get("method").and_then(|m| m.as_str());
    let Some(method) = method else {
        // Responses/server-pushed messages on the client→server POST channel
        // don't have a method. Let them through — they're not tool dispatch.
        return Ok(());
    };

    if !is_allowed_method(method) {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                format!("unsupported JSON-RPC method '{method}'"),
            )
                .into_response(),
        ));
    }

    if method != "tools/call" {
        return Ok(());
    }

    let Some(tool) = v
        .get("params")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
    else {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "tools/call missing params.name").into_response(),
        ));
    };
    if !scope_allows(scope, tool) {
        return Err(Box::new(
            (
                StatusCode::FORBIDDEN,
                format!("token scope '{scope}' does not permit tool '{tool}'"),
            )
                .into_response(),
        ));
    }
    Ok(())
}

/// Allowlist of JSON-RPC methods the endpoint accepts. Anything outside this
/// set is rejected with 400 rather than forwarded downstream, so future MCP
/// surfaces can't bypass scope enforcement by using an unhandled method.
fn is_allowed_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "notifications/initialized"
            | "notifications/cancelled"
            | "ping"
            | "tools/list"
            | "tools/call"
    )
}

/// True if a token with `scope` is allowed to invoke `tool`.
fn scope_allows(scope: &McpTokenScope, tool: &str) -> bool {
    match required_scope(tool) {
        RequiredScope::ReadOnly => true,
        RequiredScope::CacheAdmin => {
            matches!(scope, McpTokenScope::CacheAdmin | McpTokenScope::Full)
        }
        RequiredScope::Full => matches!(scope, McpTokenScope::Full),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredScope {
    ReadOnly,
    CacheAdmin,
    Full,
}

fn required_scope(tool: &str) -> RequiredScope {
    match tool {
        "readyset_status"
        | "readyset_version"
        | "show_proxied_queries"
        | "show_proxied_supported"
        | "show_caches"
        | "explain_cache_support" => RequiredScope::ReadOnly,
        "create_cache" | "drop_cache" => RequiredScope::CacheAdmin,
        // Unknown tools default to the strictest scope so adding a tool
        // without updating this map fails closed.
        _ => RequiredScope::Full,
    }
}

/// Token format check: `rs_mcp_` prefix plus up to 128 ASCII alphanumeric or
/// underscore characters. Matches the format emitted by `CREATE MCP TOKEN`
/// and rejects malformed input before hitting the Authority.
fn is_safe_token_format(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Amend the tracing filter so rmcp's per-session lifecycle events (which fire
/// at INFO) don't flood the log. If the user already specified an `rmcp`
/// directive in `LOG_LEVEL`, their choice wins.
fn apply_rmcp_log_filter() {
    let current = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into());
    if current.split(',').any(|d| d.trim().starts_with("rmcp")) {
        return;
    }
    let amended = format!("{current},rmcp=warn");
    if let Err(e) = readyset_tracing::set_log_level(&amended) {
        tracing::debug!(error = %e, "could not install rmcp log filter");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_token_accepts_generated_format() {
        assert!(is_safe_token_format("rs_mcp_abc123XYZ"));
        assert!(is_safe_token_format("a_b_c_123"));
    }

    #[test]
    fn safe_token_rejects_unsafe_chars() {
        assert!(!is_safe_token_format(""));
        assert!(!is_safe_token_format("has space"));
        assert!(!is_safe_token_format("has'quote"));
        assert!(!is_safe_token_format("has;semi"));
        // Period is no longer allowed — generated tokens never contain it.
        assert!(!is_safe_token_format("has.dot"));
        assert!(!is_safe_token_format(&"a".repeat(129)));
    }

    fn status_of(res: Result<(), Box<Response>>) -> Option<StatusCode> {
        res.err().map(|r| r.status())
    }

    #[test]
    fn scope_check_permits_allowlisted_non_tool_call_methods() {
        let init = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        assert!(scope_check(init, &McpTokenScope::ReadOnly).is_ok());
        let list = br#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
        assert!(scope_check(list, &McpTokenScope::ReadOnly).is_ok());
    }

    #[test]
    fn scope_check_rejects_unknown_method() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"prompts/call","params":{}}"#;
        assert_eq!(
            status_of(scope_check(body, &McpTokenScope::Full)),
            Some(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn scope_check_rejects_non_json_body() {
        assert_eq!(
            status_of(scope_check(b"not json", &McpTokenScope::ReadOnly)),
            Some(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn scope_check_denies_tool_outside_scope() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"drop_cache","arguments":{"name":"x"}}}"#;
        assert_eq!(
            status_of(scope_check(body, &McpTokenScope::ReadOnly)),
            Some(StatusCode::FORBIDDEN)
        );
    }

    #[test]
    fn scope_check_permits_tool_in_scope() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"readyset_status"}}"#;
        assert!(scope_check(body, &McpTokenScope::ReadOnly).is_ok());
    }

    #[test]
    fn scope_check_rejects_tool_call_without_name() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}"#;
        assert_eq!(
            status_of(scope_check(body, &McpTokenScope::Full)),
            Some(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn scope_check_rejects_batch() {
        let batch = br#"[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"drop_cache","arguments":{"name":"x"}}}]"#;
        assert_eq!(
            status_of(scope_check(batch, &McpTokenScope::ReadOnly)),
            Some(StatusCode::BAD_REQUEST)
        );
        let read_only_batch =
            br#"[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"readyset_status"}}]"#;
        assert_eq!(
            status_of(scope_check(read_only_batch, &McpTokenScope::Full)),
            Some(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn read_only_scope_permits_read_tools() {
        assert!(scope_allows(&McpTokenScope::ReadOnly, "readyset_status"));
        assert!(scope_allows(&McpTokenScope::ReadOnly, "show_caches"));
        assert!(scope_allows(
            &McpTokenScope::ReadOnly,
            "explain_cache_support"
        ));
    }

    #[test]
    fn read_only_scope_rejects_mutations() {
        assert!(!scope_allows(&McpTokenScope::ReadOnly, "drop_cache"));
        assert!(!scope_allows(&McpTokenScope::ReadOnly, "create_cache"));
    }

    #[test]
    fn cache_admin_scope_permits_cache_mutations() {
        assert!(scope_allows(&McpTokenScope::CacheAdmin, "readyset_status"));
        assert!(scope_allows(&McpTokenScope::CacheAdmin, "drop_cache"));
        assert!(scope_allows(&McpTokenScope::CacheAdmin, "create_cache"));
    }

    #[test]
    fn full_scope_permits_everything() {
        assert!(scope_allows(&McpTokenScope::Full, "readyset_status"));
        assert!(scope_allows(&McpTokenScope::Full, "drop_cache"));
        assert!(scope_allows(&McpTokenScope::Full, "some_future_tool"));
    }

    #[test]
    fn unknown_tool_requires_full_scope() {
        assert!(!scope_allows(&McpTokenScope::ReadOnly, "unknown_tool"));
        assert!(!scope_allows(&McpTokenScope::CacheAdmin, "unknown_tool"));
        assert!(scope_allows(&McpTokenScope::Full, "unknown_tool"));
    }

    #[test]
    fn json_content_type_accepts_bare_and_with_parameters() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        assert!(has_json_content_type(&headers));
        headers.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8".parse().unwrap(),
        );
        assert!(has_json_content_type(&headers));
    }

    #[test]
    fn json_content_type_rejects_other_types() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        assert!(!has_json_content_type(&headers));
        let empty = axum::http::HeaderMap::new();
        assert!(!has_json_content_type(&empty));
    }

    #[tokio::test]
    async fn token_cache_hits_and_expires() {
        let cache = TokenCache::new(Duration::from_millis(30));
        assert!(cache.get("k").is_none());

        let token = McpToken {
            name: "t".into(),
            hash: "h".into(),
            scope: McpTokenScope::ReadOnly,
            created_at: chrono::Utc::now(),
            expires_at: None,
        };
        cache.insert("k".into(), Some(token.clone()));
        assert_eq!(cache.get("k"), Some(Some(token)));
        // Negative cache works too.
        cache.insert("miss".into(), None);
        assert_eq!(cache.get("miss"), Some(None));

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(cache.get("k").is_none());
        assert!(cache.get("miss").is_none());
    }
}
