//! End-to-end coverage of `verify --rewrite`: re-recording a query's expected results in place
//! against an upstream database.
//!
//! Runs against a MySQL container the nextest tcv wrapper reserves for the `:mysql84:` test name,
//! so `cargo nt run` provisions it automatically (no `--ignored`, no manual DSN). The reserved
//! database is dropped and recreated.

use std::env;
use std::path::PathBuf;

use database_utils::{DatabaseType, DatabaseURL};
use readyset_logictest::runner::{RunOptions, TestScript};
use test_utils::upstream;

/// Build the upstream MySQL URL from the standard `MYSQL_*` env vars the tcv wrapper exports,
/// falling back to a local default.
fn mysql_url() -> DatabaseURL {
    let host = env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into());
    let user = env::var("MYSQL_USER").unwrap_or_else(|_| "root".into());
    let pass = env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "noria".into());
    let db = env::var("MYSQL_DATABASE").unwrap_or_else(|_| "noria".into());
    format!("mysql://{user}:{pass}@{host}:{port}/{db}")
        .parse()
        .expect("valid MySQL URL")
}

#[upstream(mysql, 84)]
#[tokio::test(flavor = "multi_thread")]
async fn rewrite_records_actual_results_and_preserves_comments() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/rewrite/wrong_expected.test");

    // Work on a throwaway copy so the checked-in fixture is never mutated.
    let dst = std::env::temp_dir().join("readyset-logictest-rewrite-test.test");
    std::fs::copy(&fixture, &dst).expect("copy fixture");

    let opts = |rewrite: bool| RunOptions {
        upstream_database_url: Some(mysql_url()),
        rewrite,
        ..RunOptions::default_for_database(DatabaseType::MySQL)
    };

    // Re-record against upstream.
    TestScript::open_file(dst.clone())
        .expect("read copy")
        .run(opts(true))
        .await
        .expect("rewrite run");

    let rewritten = std::fs::read_to_string(&dst).expect("read rewritten");

    // The wrong recorded value is gone; the actual rows (2, 3) are recorded.
    assert!(
        !rewritten.contains("999"),
        "stale recorded value should be replaced:\n{rewritten}"
    );
    assert!(
        rewritten.contains("\n2\n3\n"),
        "actual rows recorded:\n{rewritten}"
    );

    // Comments and SQL are preserved verbatim.
    assert!(rewritten.contains("# REA-0000 sentinel comment that must survive a rewrite"));
    assert!(rewritten.contains("SELECT x FROM t WHERE x >= 2"));

    // The rewritten script now passes a normal verify.
    TestScript::open_file(dst.clone())
        .expect("reopen rewritten")
        .run(opts(false))
        .await
        .expect("rewritten script must verify clean against upstream");

    let _ = std::fs::remove_file(&dst);
}

#[upstream(mysql, 84)]
#[tokio::test(flavor = "multi_thread")]
async fn rewrite_preserves_error_tagged_records() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/rewrite/error_tag_preserved.test");

    let dst = std::env::temp_dir().join("readyset-logictest-rewrite-error-tag.test");
    std::fs::copy(&fixture, &dst).expect("copy fixture");

    let opts = RunOptions {
        upstream_database_url: Some(mysql_url()),
        rewrite: true,
        ..RunOptions::default_for_database(DatabaseType::MySQL)
    };

    // The `statement error` succeeds against upstream; the rewrite must run it for its side effect
    // and not abort.
    TestScript::open_file(dst.clone())
        .expect("read copy")
        .run(opts)
        .await
        .expect("rewrite must not abort on a statement-error that succeeds upstream");

    let rewritten = std::fs::read_to_string(&dst).expect("read rewritten");

    // The error-tagged query's recorded block is preserved verbatim, not re-recorded to upstream's
    // rows (2, 3).
    assert!(
        rewritten.contains("error: readyset-specific query failure"),
        "error tag preserved:\n{rewritten}"
    );
    assert!(
        rewritten.contains("\n42\n"),
        "error-tagged query's recorded block must be preserved, not re-recorded:\n{rewritten}"
    );

    // The plain query is re-recorded, and the statement-error's side effect (inserting 4) is
    // visible, proving the rewrite continued past it.
    assert!(
        !rewritten.contains("999"),
        "plain query should be re-recorded:\n{rewritten}"
    );
    assert!(
        rewritten.contains("\n3\n4\n"),
        "re-recorded rows reflect the statement-error side effect:\n{rewritten}"
    );

    let _ = std::fs::remove_file(&dst);
}
