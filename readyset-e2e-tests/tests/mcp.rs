//! End-to-end smoke test for the embedded MCP HTTP endpoint.
//!
//! Spins up a full Readyset adapter + server via [`TestBuilder`], then stands
//! up an MCP HTTP listener sharing the same [`Authority`]. The MCP endpoint
//! dispatches tool calls via a loopback SQL connection to the adapter's own
//! SQL listener — the same path every other client uses.

use std::net::SocketAddr;
use std::sync::Arc;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use readyset_adapter::mcp_http::{self, McpHttpConfig};
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_mcp::connection::{ConnectionConfig, DbType, ReadysetConnection, TlsMode};
use readyset_mcp::server::ReadysetMcpServer;
use readyset_tracing::init_test_logging;
use readyset_client_test_helpers::TestShutdownSender;
use serde_json::{Value, json};
use test_utils::{tags, upstream};
use tokio::net::TcpListener;
use tokio::test;

/// Stand up an MCP HTTP endpoint sharing `authority` with a running Readyset
/// adapter, connecting back via loopback SQL.
async fn spawn_mcp_endpoint(
    authority: Arc<Authority>,
    sql_opts: &mysql_async::Opts,
    shutdown_tx: &TestShutdownSender<MySQLAdapter>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = ConnectionConfig {
        host: sql_opts.ip_or_hostname().to_string(),
        port: sql_opts.tcp_port(),
        user: sql_opts.user().unwrap_or("").to_string(),
        password: sql_opts.pass().unwrap_or("").to_string(),
        database: sql_opts.db_name().map(|s| s.to_string()),
        db_type: DbType::Mysql,
        tls_mode: TlsMode::Disable,
        tls_root_cert: None,
        tls_disable_verification: false,
    };
    let conn = ReadysetConnection::new(&config).await.unwrap();
    let mcp_server = ReadysetMcpServer::new(conn);

    let mut mcp_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_shutdown = cancel.clone();
        tokio::spawn(async move {
            mcp_shutdown.recv().await;
            cancel_for_shutdown.cancel();
        });
        let config = McpHttpConfig { listen_addr: addr };
        drop(listener);
        let _ = mcp_http::serve(authority, mcp_server, config, cancel).await;
    });

    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("MCP HTTP server did not become ready");
}

/// Post a JSON-RPC body to the MCP endpoint with a Bearer token.
async fn post_mcp(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

/// Parse an rmcp response, which may come back as JSON or as an SSE stream
/// with a single `data:` event. Returns the first JSON-RPC object found.
async fn parse_mcp_response(resp: reqwest::Response) -> Value {
    let text = resp.text().await.unwrap();
    if let Ok(v) = serde_json::from_str::<Value>(&text) {
        return v;
    }
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ")
            && let Ok(v) = serde_json::from_str::<Value>(data)
        {
            return v;
        }
    }
    panic!("could not parse MCP response body: {text}");
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn mcp_happy_path() {
    init_test_logging();

    // Explicitly create the Authority so the MCP endpoint can share it.
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))));

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .authority(authority.clone())
        .build::<MySQLAdapter>()
        .await;

    let mcp_addr = spawn_mcp_endpoint(authority, &rs_opts, &shutdown_tx).await;
    let mcp_url = format!("http://{mcp_addr}/mcp");

    // Create a token via the SQL listener.
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    let row: Row = rs_conn
        .query_first("CREATE MCP TOKEN 'e2e-test' WITH SCOPE cache_admin")
        .await
        .unwrap()
        .expect("CREATE MCP TOKEN returned no row");
    let token: String = row.get("token").expect("missing token column");
    assert!(
        token.starts_with("rs_mcp_"),
        "unexpected token format: {token}"
    );

    let client = reqwest::Client::new();

    // 1. initialize
    let init_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "e2e-test", "version": "0.0.1"}
        }
    });
    let resp = post_mcp(&client, &mcp_url, &token, init_body).await;
    assert_eq!(resp.status(), 200, "initialize should succeed");
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .expect("server must return mcp-session-id on initialize")
        .to_str()
        .unwrap()
        .to_string();
    let _ = parse_mcp_response(resp).await;

    // 2. initialized notification
    let notified = client
        .post(&mcp_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(
        notified.status().is_success(),
        "notifications/initialized should be accepted"
    );

    // 3. tools/list
    let list_resp = client
        .post(&mcp_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.status(), 200);
    let list_json = parse_mcp_response(list_resp).await;
    let tools = list_json["result"]["tools"]
        .as_array()
        .expect("tools/list must return an array");
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    for expected in [
        "readyset_status",
        "readyset_version",
        "show_caches",
        "show_proxied_queries",
        "show_proxied_supported",
        "explain_cache_support",
        "create_cache",
        "drop_cache",
    ] {
        assert!(
            tool_names.contains(&expected),
            "expected tool '{expected}' in tools/list; got {tool_names:?}"
        );
    }

    // 4. tools/call readyset_status — should succeed and return some content
    let call_resp = client
        .post(&mcp_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "readyset_status",
                    "arguments": {}
                }
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(call_resp.status(), 200);
    let call_json = parse_mcp_response(call_resp).await;
    let content = &call_json["result"]["content"];
    let text = content[0]["text"]
        .as_str()
        .expect("tool result must include text content");
    assert!(!text.is_empty(), "readyset_status returned empty text");

    // 5. Scope enforcement: read_only token cannot call drop_cache.
    let ro_row: Row = rs_conn
        .query_first("CREATE MCP TOKEN 'e2e-readonly' WITH SCOPE read_only")
        .await
        .unwrap()
        .unwrap();
    let ro_token: String = ro_row.get("token").unwrap();

    let denied = client
        .post(&mcp_url)
        .header("Authorization", format!("Bearer {ro_token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "drop_cache", "arguments": {"name": "nonexistent"}}
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        denied.status(),
        403,
        "read_only token should not be permitted to call drop_cache"
    );

    // 5a. Batched JSON-RPC requests must be rejected outright.
    let batch_bypass = client
        .post(&mcp_url)
        .header("Authorization", format!("Bearer {ro_token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            json!([
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "drop_cache", "arguments": {"name": "nonexistent"}}
                }
            ])
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        batch_bypass.status(),
        400,
        "batched JSON-RPC requests must be rejected before scope dispatch"
    );

    // 6. Missing Authorization header -> 401
    let unauth = client
        .post(&mcp_url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        unauth.status(),
        401,
        "request without Authorization header should be rejected"
    );

    drop(shutdown_tx);
}
