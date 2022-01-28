use mysql::{prelude::*, Statement};
use mysql::{Conn, Result, Row};
use noria_client::backend::MigrationMode;
use noria_client::backend::QueryInfo;
use noria_client::query_status_cache::QueryStatusCache;
use noria_client_metrics::QueryDestination;
use noria_client_test_helpers::mysql_helpers::query_cache_setup;
use noria_client_test_helpers::sleep;
use serial_test::serial;
use std::convert::TryFrom;
use std::sync::Arc;

/// Retrieves where the query executed by parsing the row returned by
/// EXPLAIN LAST STATEMENT.
fn last_query_destination(conn: &mut Conn) -> QueryDestination {
    let res: Row = conn.query_first("EXPLAIN LAST STATEMENT").unwrap().unwrap();
    QueryInfo::try_from(&res).unwrap().destination
}

// With in_request_path migration and fallback, an supported query should execute on Noria
// and be marked allowed on completion, an unsupported query should execute on Noria
// and then fallback, and be marked denied.
#[test]
#[serial]
fn in_request_path_query_with_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        true, // fallback enabled
        MigrationMode::InRequestPath,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Noria);

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::NoriaThenFallback
    );

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );
}

// With in_request_path query mode without fallback, a supported query should execute on Noria
// and be marked allowed on completion, an unsupported query should execute on Noria
// and then fallback, and be marked denied.
#[test]
#[serial]
fn in_request_path_query_without_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        false, // fallback disabled
        MigrationMode::InRequestPath,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_err()); // Unable to handle this unsupported query.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
}

// With the out_of_band query mode and fallback, both supported and unsupported
// queries should be executed against fallback, they should not be added to the
// allow list. Performing an explicit migration allows the query to be added to
// the allow list on next exeution.
#[test]
#[serial]
fn out_of_band_query_with_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        true, // fallback enabled
        MigrationMode::OutOfBand,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<Vec<Row>> = conn.query("CREATE CACHED QUERY test AS SELECT * FROM t");
    assert!(res.is_ok());

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t");
    assert!(res.is_ok()); // Executed successfully against noria.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Noria);
}

// With in_request_path migration and fallback, a supported query should execute on Noria
// and be marked allowed on completion, an unsupported query should execute on Noria
// and then fallback, and be marked denied.
#[test]
#[serial]
fn in_request_path_prep_exec_with_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let opts = query_cache_setup(
        query_status_cache.clone(),
        true, // fallback enabled
        MigrationMode::InRequestPath,
    );

    let mut conn = mysql::Conn::new(opts).unwrap();

    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<_> = conn.prep("SELECT * FROM t");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Both);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Noria);

    let res: Result<_> = conn.prep("SELECT * FROM t WHERE a = NOW() AND b = 1");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Both);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<Statement> = conn.prep("SELECT * FROM t WHERE a = NOW() AND  b = 2");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );
}

// With in_request_path query mode without fallback, a supported query should execute on Noria
// and be marked allowed on completion, an unsupported query should execute on Noria
// and then fallback, and be marked denied.
#[test]
#[serial]
fn in_request_path_prep_without_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        false, // fallback disabled
        MigrationMode::InRequestPath,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<_> = conn.prep("SELECT * FROM t");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<_> = conn.prep("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_err()); // Unable to handle this unsupported query.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
}

// With the out_of_band query mode and fallback, both supported and unsupported
// queries should be executed against fallback, they should not be added to the
// allow list. Performing an explicit migration allows the query to be added to
// the allow list on next exeution.
#[test]
#[serial]
fn out_of_band_prep_exec_with_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        true, // fallback enabled
        MigrationMode::OutOfBand,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Statement> = conn.prep("SELECT * FROM t");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Both);
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<Statement> = conn.prep("SELECT * FROM t WHERE a = NOW()");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Both);
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );

    let res: Result<_> = conn.query_drop("CREATE CACHED QUERY test AS SELECT * FROM t");
    assert!(res.is_ok());

    let stmt: Statement = conn
        .prep("SELECT * FROM t")
        .expect("Executed successfully against noria");
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Noria);

    let res: Result<Vec<Row>> = conn.exec(&stmt, ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(last_query_destination(&mut conn), QueryDestination::Noria);

    let res: Result<_> = conn.query_drop("DROP CACHED QUERY test");
    assert!(res.is_ok());

    // Should go back to fallback after we dropped the query
    let res: Result<Vec<Row>> = conn.exec(&stmt, ());
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_destination(&mut conn),
        QueryDestination::Fallback
    );
}

// Allow migrations within the request path. Both migrations, the CREATE QUERY
// CACHE statement and the SELECT * FROM t should be rewritten to be the same
// entry in the query status cache. Otherwise we would have more than one entry
// in the allow list.
#[test]
#[serial]
fn in_request_path_rewritten_query_without_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        false, // fallback disabled
        MigrationMode::InRequestPath,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Vec<Row>> =
        conn.query("CREATE CACHED QUERY test AS SELECT * FROM t WHERE a = ? AND b = ?");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = 4 AND b = 5");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
}

// With the out_of_band query mode without fallback, queries that are not
// explicitely migrated are denied. We verify that two queries end up being
// cached as the same query in the query status cache. Otherwise, the second
// query will fail as being disallowed.
#[test]
#[serial]
fn out_of_band_rewritten_query_without_fallback() {
    let query_status_cache = Arc::new(QueryStatusCache::new());
    let mut conn = mysql::Conn::new(query_cache_setup(
        query_status_cache.clone(),
        false, // fallback disabled
        MigrationMode::OutOfBand,
    ))
    .unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)").unwrap();
    sleep();
    let res: Result<Vec<Row>> =
        conn.query("CREATE CACHED QUERY test AS SELECT * FROM t WHERE a = ? AND b = ?");
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = 4 AND b = 5");
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
}
