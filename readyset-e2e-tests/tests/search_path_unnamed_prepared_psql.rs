//! Regression tests for unnamed prepared statement memoization in the Postgres
//! extended protocol. Readyset memoizes the unnamed slot as an optimization,
//! which Postgres never does (it re-plans on every Parse), so the memo must not
//! reuse a plan whose routing was decided under context that has since changed:
//!
//! - REA-6740: re-Parsing the same text after `SET search_path` must resolve
//!   against the new path, not return the previous schema's memoized rows.
//! - REA-6746: an unnamed slot first Parsed inside a transaction memoizes an
//!   upstream-only plan; after COMMIT/ROLLBACK, re-Parsing must recover caching
//!   rather than stay pinned to the transaction-bypassed plan.
//!
//! `tokio_postgres` always names its prepared statements, which take a different
//! code path, so these reproduce the extended-protocol unnamed slot with a raw
//! pgwire client that sends Parse/Bind/Execute with an empty statement name.

use bytes::BytesMut;
use postgres_protocol::IsNull;
use postgres_protocol::message::{backend, frontend};
use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_client_test_helpers::{
    Adapter, TestBuilder, derive_test_name, psql_helpers::PostgreSQLAdapter,
};
use readyset_server::Handle;
use readyset_tracing::init_test_logging;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::test;
use tokio_postgres::config::Host;

/// Minimal pgwire client that speaks the extended protocol with unnamed
/// statements, which `tokio_postgres` does not expose.
struct RawPgConn {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl RawPgConn {
    async fn connect(host: &str, port: u16, user: &str, dbname: &str) -> Self {
        let stream = TcpStream::connect((host, port)).await.expect("connect");
        let mut conn = Self {
            stream,
            read_buf: BytesMut::new(),
        };

        let mut out = BytesMut::new();
        frontend::startup_message([("user", user), ("database", dbname)], &mut out)
            .expect("startup message");
        conn.stream.write_all(&out).await.expect("send startup");

        // Authentication is disabled for this backend, so drain
        // AuthenticationOk / ParameterStatus / BackendKeyData up to the first
        // ReadyForQuery.
        conn.read_until_ready().await;
        conn
    }

    async fn next_message(&mut self) -> backend::Message {
        loop {
            if let Some(msg) = backend::Message::parse(&mut self.read_buf).expect("parse message") {
                return msg;
            }
            let n = self.stream.read_buf(&mut self.read_buf).await.expect("read");
            assert!(n != 0, "server closed connection unexpectedly");
        }
    }

    async fn read_until_ready(&mut self) {
        loop {
            match self.next_message().await {
                backend::Message::ReadyForQuery(_) => return,
                backend::Message::ErrorResponse(err) => {
                    panic!("unexpected error response: {}", error_text(&err));
                }
                _ => {}
            }
        }
    }

    /// Run a simple-protocol statement (used for SET / DDL), asserting success.
    async fn simple_query(&mut self, query: &str) {
        self.simple_query_rows(query).await;
    }

    /// Run a simple-protocol statement, returning the columns of each row.
    async fn simple_query_rows(&mut self, query: &str) -> Vec<Vec<String>> {
        let mut out = BytesMut::new();
        frontend::query(query, &mut out).expect("query message");
        self.stream.write_all(&out).await.expect("send query");
        let mut rows = Vec::new();
        loop {
            match self.next_message().await {
                backend::Message::DataRow(row) => rows.push(columns(&row)),
                backend::Message::ReadyForQuery(_) => return rows,
                backend::Message::ErrorResponse(err) => {
                    panic!("unexpected error response: {}", error_text(&err));
                }
                _ => {}
            }
        }
    }

    /// Route of the most recent statement, per `EXPLAIN LAST STATEMENT`.
    async fn last_query_destination(&mut self) -> String {
        let rows = self.simple_query_rows("EXPLAIN LAST STATEMENT").await;
        rows.into_iter()
            .next()
            .and_then(|cols| cols.into_iter().next())
            .expect("EXPLAIN LAST STATEMENT row")
    }

    /// Parse/Bind/Execute `query` as an unnamed prepared statement with a single
    /// text-format parameter, returning the first column of every row.
    async fn unnamed_query(&mut self, query: &str, param: &str) -> Vec<String> {
        let mut out = BytesMut::new();
        frontend::parse("", query, std::iter::empty::<u32>(), &mut out).expect("parse");
        frontend::bind(
            "",
            "",
            std::iter::empty::<i16>(),
            [param],
            |value: &str, buf: &mut BytesMut| {
                buf.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            },
            std::iter::empty::<i16>(),
            &mut out,
        )
        .unwrap_or_else(|_| panic!("bind"));
        frontend::execute("", 0, &mut out).expect("execute");
        frontend::sync(&mut out);
        self.stream.write_all(&out).await.expect("send extended");

        let mut rows = Vec::new();
        loop {
            match self.next_message().await {
                backend::Message::DataRow(row) => {
                    rows.push(first_column(&row));
                }
                backend::Message::ReadyForQuery(_) => return rows,
                backend::Message::ErrorResponse(err) => {
                    panic!("unexpected error response: {}", error_text(&err));
                }
                _ => {}
            }
        }
    }
}

/// Decode the columns of a `DataRow` as UTF-8. The body storage holds the field
/// sequence after the column count: `[i32 len][bytes]...`, with `len == -1` for
/// a null column.
fn columns(row: &backend::DataRowBody) -> Vec<String> {
    let buf = row.buffer();
    let mut cols = Vec::new();
    let mut i = 0;
    while i + 4 <= buf.len() {
        let len = i32::from_be_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
        i += 4;
        if len < 0 {
            cols.push(String::new());
            continue;
        }
        let end = i + len as usize;
        cols.push(String::from_utf8_lossy(&buf[i..end]).into_owned());
        i = end;
    }
    cols
}

/// Decode the first column of a `DataRow` as UTF-8.
fn first_column(row: &backend::DataRowBody) -> String {
    columns(row).into_iter().next().expect("data row has a column")
}

fn error_text(err: &backend::ErrorResponseBody) -> String {
    use fallible_iterator::FallibleIterator;
    let mut fields = err.fields();
    while let Some(field) = fields.next().expect("error field") {
        if field.type_() == b'M' {
            return String::from_utf8_lossy(field.value_bytes()).into_owned();
        }
    }
    "<no message>".to_string()
}

/// Recreate `test_name` upstream and apply the seed statements against it.
async fn seed_upstream(test_name: &str, stmts: &[&str]) {
    PostgreSQLAdapter::recreate_database(test_name).await;
    let mut cfg = readyset_client_test_helpers::psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = readyset_client_test_helpers::psql_helpers::connect(cfg).await;
    for stmt in stmts {
        upstream.simple_query(stmt).await.expect("seed upstream");
    }
}

/// Build a Readyset PostgreSQL backend replicating `test_name` and connect a raw
/// pgwire client to it. The returned handles keep the server alive for the test.
async fn connect_backend(test_name: &str) -> (RawPgConn, Handle, ShutdownSender) {
    let backend_builder = BackendBuilder::default()
        .require_authentication(false)
        .replication_enabled(false);

    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(backend_builder)
        .recreate_database(false)
        .replicate_db(test_name)
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;

    let host = match &rs_opts.get_hosts()[0] {
        Host::Tcp(h) => h.clone(),
        other => panic!("expected TCP host, got {other:?}"),
    };
    let port = rs_opts.get_ports()[0];
    let user = rs_opts.get_user().unwrap_or("postgres").to_string();

    let conn = RawPgConn::connect(&host, port, &user, test_name).await;
    (conn, handle, shutdown_tx)
}

#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn unnamed_prepared_reresolves_after_set_search_path() {
    init_test_logging();

    let test_name = derive_test_name();
    seed_upstream(
        &test_name,
        &[
            "CREATE SCHEMA schema_a",
            "CREATE SCHEMA schema_b",
            "CREATE TABLE schema_a.items (id bigserial PRIMARY KEY, label text NOT NULL)",
            "CREATE TABLE schema_b.items (id bigserial PRIMARY KEY, label text NOT NULL)",
            "INSERT INTO schema_a.items (label) VALUES ('a-one'), ('a-two')",
            "INSERT INTO schema_b.items (label) VALUES ('b-one'), ('b-two'), ('b-three')",
        ],
    )
    .await;

    let (mut conn, _handle, shutdown_tx) = connect_backend(&test_name).await;

    let query = "SELECT label FROM items WHERE 1 = $1 ORDER BY id";

    // Register the shallow cache while search_path resolves `items` to schema_a,
    // so the cache's query id carries the schema_a context.
    conn.simple_query("SET search_path = schema_a, public").await;
    conn.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS \
         FROM SELECT label FROM items WHERE 1 = $1 ORDER BY id",
    )
    .await;

    // First execution misses and populates the shallow cache under schema_a's
    // query id; the second serves from the shallow cache.
    let rows_a = conn.unnamed_query(query, "1").await;
    assert_eq!(rows_a, vec!["a-one", "a-two"], "schema_a rows");
    assert_eq!(conn.last_query_destination().await, "upstream");
    let rows_a = conn.unnamed_query(query, "1").await;
    assert_eq!(rows_a, vec!["a-one", "a-two"], "schema_a rows (cached)");
    assert_eq!(conn.last_query_destination().await, "readyset_shallow");

    conn.simple_query("SET search_path = schema_b, public").await;
    let rows_b = conn.unnamed_query(query, "1").await;
    assert_eq!(
        rows_b,
        vec!["b-one", "b-two", "b-three"],
        "re-Parsing the same unnamed text after SET search_path must resolve \
         against schema_b, not return schema_a's memoized rows",
    );

    shutdown_tx.shutdown().await;
}

/// REA-6746: an unnamed statement first Parsed inside a transaction memoizes an
/// upstream-only plan, because the shallow cache is bypassed while a transaction
/// is open (the default `Never` policy). After COMMIT the connection is no longer
/// in a transaction, so re-Parsing the identical text must re-plan and recover
/// shallow caching rather than reuse the transaction-bypassed plan indefinitely.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn unnamed_prepared_recovers_cache_after_transaction() {
    init_test_logging();

    let test_name = derive_test_name();
    seed_upstream(
        &test_name,
        &[
            "CREATE TABLE ticker (id integer PRIMARY KEY, version bigint NOT NULL)",
            "INSERT INTO ticker (id, version) VALUES (1, 0)",
        ],
    )
    .await;

    let (mut conn, _handle, shutdown_tx) = connect_backend(&test_name).await;

    let query = "SELECT version FROM ticker WHERE id = $1";
    conn.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS \
         FROM SELECT version FROM ticker WHERE id = $1",
    )
    .await;

    // The first Parse happens inside a transaction, so the cache is bypassed and
    // the unnamed slot memoizes an upstream-only plan.
    conn.simple_query("BEGIN").await;
    let rows = conn.unnamed_query(query, "1").await;
    assert_eq!(rows, vec!["0"], "in-transaction rows");
    assert_eq!(conn.last_query_destination().await, "upstream");
    conn.simple_query("COMMIT").await;

    // After COMMIT the connection is transaction-free. The first read re-plans and
    // fills the shallow cache (upstream); every read after that must serve from the
    // shallow cache instead of staying pinned to the transaction-bypassed plan.
    let rows = conn.unnamed_query(query, "1").await;
    assert_eq!(rows, vec!["0"], "post-commit fill rows");
    assert_eq!(conn.last_query_destination().await, "upstream");
    for read in 0..3 {
        let rows = conn.unnamed_query(query, "1").await;
        assert_eq!(rows, vec!["0"], "post-commit cached rows (read {read})");
        assert_eq!(
            conn.last_query_destination().await,
            "readyset_shallow",
            "re-Parsing after COMMIT must recover shallow caching (read {read})",
        );
    }

    shutdown_tx.shutdown().await;
}
