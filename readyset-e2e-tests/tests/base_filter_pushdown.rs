use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_adapter::query_status_cache::MigrationStyle;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage};

const TABLE: &str = "CREATE TABLE t (id int PRIMARY KEY, owner int, kind int, v int, \
                     status varchar(10)); \
                     INSERT INTO t VALUES (1, 1, 2, 10, 'a'), (2, 1, 3, 20, 'b'), \
                     (3, 1, 4, 30, 'a'), (4, 2, 2, 40, 'c'), (5, 2, 4, 50, 'b')";

const WRITES: &str = "INSERT INTO t VALUES (6, 2, 3, 60, 'a'), (7, 2, 5, 70, 'b')";

struct Cache {
    name: &'static str,
    /// The cached query. Reads bind the owner to the first parameter and 5 to any others.
    query: &'static str,
    /// The index the cache's replays read the table from, and the constant each reads it by
    reads: [&'static str; 2],
}

const CACHES: [Cache; 3] = [
    Cache {
        name: "point",
        query: "SELECT v FROM t WHERE owner = ? AND (kind = 2 OR kind = 3)",
        reads: ["HashMap[2, 1] by (2)", "HashMap[2, 1] by (3)"],
    },
    Cache {
        name: "ranged",
        query: "SELECT v FROM t WHERE owner = ? AND (kind = 2 OR kind = 3) AND v > ? ORDER BY v LIMIT 2",
        reads: ["BTreeMap[2, 1, 3] by (2)", "BTreeMap[2, 1, 3] by (3)"],
    },
    Cache {
        name: "text",
        query: "SELECT v FROM t WHERE owner = ? AND (status = 'a' OR status = 'b')",
        reads: ["HashMap[4, 1] by (a)", "HashMap[4, 1] by (b)"],
    },
];

/// `query` with each `?` replaced by `value(i)`, where `i` is the parameter's position
fn bind(query: &str, value: impl Fn(usize) -> String) -> String {
    let mut parts = query.split('?');
    let mut bound = parts.next().unwrap_or_default().to_owned();
    for (i, part) in parts.enumerate() {
        bound.push_str(&value(i));
        bound.push_str(part);
    }
    bound
}

/// The cached query, read for owners 1 and 2
fn reads(cache: &Cache) -> [String; 2] {
    [1, 2].map(|owner| bind(cache.query, |i| if i == 0 { owner } else { 5 }.to_string()))
}

/// Checks that the graph shows the constants each cache reads the table by, and that one
/// materialization has every index the caches read and none on their keys alone
fn assert_reads_constants(dots: &[String], indexes: impl IntoIterator<Item = String>) {
    for (cache, dot) in CACHES.iter().zip(dots) {
        for read in cache.reads {
            assert!(
                dot.contains(&format!("constants: {read}")),
                "{}",
                cache.name
            );
        }
    }
    let read = CACHES.map(|cache| cache.reads[0].split(" by ").next().unwrap_or_default());
    let unused = ["HashMap[1]", "BTreeMap[1, 3]"];
    assert!(indexes.into_iter().any(|indexes| {
        read.iter().all(|index| indexes.contains(index))
            && !unused.iter().any(|index| indexes.contains(index))
    }));
}

/// Checks that each cache reads the table by its constants and serves the upstream's rows
async fn mysql_check(rs: &mut Conn, upstream: &mut Conn) {
    for cache in &CACHES {
        for query in reads(cache) {
            let mut rows: Vec<i32> = upstream.query(&query).await.unwrap();
            rows.sort();
            eventually!(run_test: {
                let mut read: Vec<i32> = rs.query(&query).await.unwrap();
                read.sort();
                let info: QueryInfo =
                    rs.query_first("EXPLAIN LAST STATEMENT").await.unwrap().unwrap();
                (read, info.destination)
            }, then_assert: |(read, destination)| {
                assert_eq!(destination, QueryDestination::Readyset(Some(cache.name.into())));
                assert_eq!(read, rows, "{query}");
            })
        }
    }
    let mut dots: Vec<String> = Vec::new();
    for cache in &CACHES {
        let graphviz = format!("EXPLAIN GRAPHVIZ FOR CACHE {}", cache.name);
        dots.push(rs.query_first(graphviz).await.unwrap().unwrap());
    }
    let rows: Vec<Row> = rs.query("EXPLAIN MATERIALIZATIONS").await.unwrap();
    let indexes = rows.iter().map(|r| r.get::<String, _>("indexes").unwrap());
    assert_reads_constants(&dots, indexes);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern)]
async fn filter_index_mysql() {
    readyset_tracing::init_test_logging();
    let db_name = derive_test_name();
    mysql_helpers::recreate_database(&db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&db_name));
    let mut upstream = Conn::new(upstream_opts).await.unwrap();
    upstream.query_drop(TABLE).await.unwrap();

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .replicate_db(&db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs = Conn::new(rs_opts).await.unwrap();
    eventually! {
        let rows: Vec<Row> = rs.query("SHOW READYSET STATUS").await.unwrap();
        rows.iter().any(|r| r.get::<String, _>(1).as_deref() == Some("Online"))
    }
    for Cache { name, query, .. } in &CACHES {
        let create = format!("CREATE CACHE {name} FROM {query}");
        rs.query_drop(create).await.unwrap();
    }
    mysql_check(&mut rs, &mut upstream).await;
    drop(rs);

    let (rs_opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;
    let mut rs = Conn::new(rs_opts).await.unwrap();
    mysql_check(&mut rs, &mut upstream).await;
    upstream.query_drop(WRITES).await.unwrap();
    mysql_check(&mut rs, &mut upstream).await;

    shutdown_tx.shutdown().await;
}

fn column(messages: Vec<SimpleQueryMessage>, i: usize) -> Vec<String> {
    let value = |row: &tokio_postgres::SimpleQueryRow| row.get(i).unwrap_or_default().to_owned();
    let rows = messages.iter().filter_map(|message| match message {
        SimpleQueryMessage::Row(row) => Some(value(row)),
        _ => None,
    });
    rows.collect()
}

/// The `v` values `query` returns, sorted
async fn psql_values(client: &Client, query: &str) -> Vec<i32> {
    let values = column(client.simple_query(query).await.unwrap(), 0);
    let mut values: Vec<i32> = values.iter().map(|v| v.parse().unwrap()).collect();
    values.sort();
    values
}

/// Checks that each cache reads the table by its constants and serves the upstream's rows
async fn psql_check(rs: &Client, upstream: &Client) {
    for cache in &CACHES {
        for query in reads(cache) {
            let rows = psql_values(upstream, &query).await;
            eventually!(run_test: {
                let read = psql_values(rs, &query).await;
                (read, last_query_info(rs).await.destination)
            }, then_assert: |(read, destination)| {
                assert_eq!(destination, QueryDestination::Readyset(Some(cache.name.into())));
                assert_eq!(read, rows, "{query}");
            })
        }
    }
    let mut dots: Vec<String> = Vec::new();
    for cache in &CACHES {
        let graphviz = format!("EXPLAIN GRAPHVIZ FOR CACHE {}", cache.name);
        dots.push(column(rs.simple_query(&graphviz).await.unwrap(), 0).concat());
    }
    let materializations = rs.simple_query("EXPLAIN MATERIALIZATIONS").await.unwrap();
    let indexes = materializations.iter().filter_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get("indexes").map(str::to_owned),
        _ => None,
    });
    assert_reads_constants(&dots, indexes);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn filter_index_postgres() {
    readyset_tracing::init_test_logging();
    let db_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db_name).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(&db_name);
    let upstream = psql_helpers::connect(upstream_config).await;
    upstream.simple_query(TABLE).await.unwrap();

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .replicate_db(&db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let rs = psql_helpers::connect(rs_opts).await;
    eventually! {
        let status = column(rs.simple_query("SHOW READYSET STATUS").await.unwrap(), 1);
        status.iter().any(|value| value == "Online")
    }
    for cache in &CACHES {
        let query = bind(cache.query, |i| format!("${}", i + 1));
        let create = format!("CREATE CACHE {} FROM {query}", cache.name);
        rs.simple_query(&create).await.unwrap();
    }
    psql_check(&rs, &upstream).await;
    drop(rs);

    let (rs_opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;
    let rs = psql_helpers::connect(rs_opts).await;
    psql_check(&rs, &upstream).await;
    upstream.simple_query(WRITES).await.unwrap();
    psql_check(&rs, &upstream).await;

    shutdown_tx.shutdown().await;
}
