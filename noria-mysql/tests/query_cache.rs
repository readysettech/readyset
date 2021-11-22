use mysql::prelude::*;
use noria_client::query_status_cache::QueryStatusCache;
use noria_client_test_helpers::sleep;
use serial_test::serial;
use std::sync::Arc;
mod common;
use common::query_cache_setup;

//TODO(DAN): use tokio::test for query_cache integration tests
#[test]
#[serial]
fn query_cache_blocking_migration_fallback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), true)).unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    conn.query_drop("SELECT * FROM t").unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
    let _ = conn.query_drop("SELECT * FROM t WHERE a = NOW()");
    assert_eq!(rt.block_on(query_status_cache.deny_list()).len(), 1);
}

#[test]
#[serial]
fn query_cache_blocking_migration_no_fallback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), false)).unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    conn.query_drop("SELECT * FROM t").unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
    let _ = conn.query_drop("SELECT * FROM t WHERE a = NOW()");
    assert_eq!(rt.block_on(query_status_cache.deny_list()).len(), 1);
}

#[test]
#[serial]
fn query_cache_blocking_migration_fallback_prepare() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), true)).unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let stmt = conn.prep("SELECT * FROM t").unwrap();
    conn.exec_drop(stmt, ()).unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
    let _ = conn.prep("SELECT t.b FROM t WHERE a > ?");
    assert_eq!(rt.block_on(query_status_cache.deny_list()).len(), 1);
}

#[test]
#[serial]
fn query_cache_blocking_migration_no_fallback_prepare() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), false)).unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let stmt = conn.prep("SELECT * FROM t").unwrap();
    conn.exec_drop(stmt, ()).unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
    let _ = conn.prep("SELECT t.b FROM t WHERE a > ?");
    assert_eq!(rt.block_on(query_status_cache.deny_list()).len(), 1);
}

// Test that a query can go from pending -> successful with no fallback
#[test]
#[serial]
fn query_cache_blocking_migration_pending_to_success() {
    use nom_sql::parser::parse_query;
    use nom_sql::Dialect;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), false)).unwrap();

    let query = "SELECT a FROM t";
    let parsed_query = match parse_query(Dialect::MySQL, query).unwrap() {
        nom_sql::SqlQuery::Select(q) => Some(q),
        _ => None,
    }
    .unwrap();
    rt.block_on(query_status_cache.set_pending_migration(&parsed_query));

    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();

    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 0);
    let stmt = conn.prep("SELECT a FROM t").unwrap();
    conn.exec_drop(stmt, ()).unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
}

// Test that a query can go from pending -> successful with fallback
#[test]
#[serial]
fn query_cache_blocking_migration_pending_to_success_fallback() {
    use nom_sql::parser::parse_query;
    use nom_sql::Dialect;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let query_status_cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(1)));
    let mut conn = mysql::Conn::new(query_cache_setup(query_status_cache.clone(), true)).unwrap();

    let query = "SELECT a FROM t";
    let parsed_query = match parse_query(Dialect::MySQL, query).unwrap() {
        nom_sql::SqlQuery::Select(q) => Some(q),
        _ => None,
    }
    .unwrap();
    rt.block_on(query_status_cache.set_pending_migration(&parsed_query));

    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();

    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 0);
    let stmt = conn.prep("SELECT a FROM t").unwrap();
    conn.exec_drop(stmt, ()).unwrap();
    assert_eq!(rt.block_on(query_status_cache.allow_list()).len(), 1);
}
