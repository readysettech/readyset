//! Asserts that NEWDECIMAL columns from upstream MySQL decode to `Value::Numeric`
//! end-to-end through `TestScript::run`.
//!
//! Runs against a MySQL container the nextest tcv wrapper reserves for the `:mysql84:` test
//! name, so `cargo nt run` provisions it automatically (no `--ignored`, no manual DSN).

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
async fn avg_sum_distinct_range_param_passes() {
    let path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/numeric_routing/avg_sum_distinct_range_param.test");

    let mut script = TestScript::open_file(path).expect("read fixture");
    let opts = RunOptions {
        upstream_database_url: Some(mysql_url()),
        // `error:` tags mark Readyset-specific failures; the queries succeed on vanilla upstream,
        // so verifying directly against MySQL must not enforce them (matches the nightly upstream
        // CI steps, which pass `--ignore-error-tags`).
        ignore_error_tags: true,
        ..RunOptions::default_for_database(DatabaseType::MySQL)
    };

    script
        .run(opts)
        .await
        .expect("fixture must pass against upstream MySQL");
}
