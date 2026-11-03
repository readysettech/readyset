//! Compares Readyset with MySQL for expressions over a DECIMAL and another number or a string. Each
//! test caches a statement of the expressions it checks for each pair of column types, or each
//! column, then reads it through Readyset and from MySQL and compares each expression's type and
//! values.

use futures::{StreamExt, stream};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, Opts, Pool, Row, Value};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, derive_test_name, sleep};
use readyset_sql_parsing::ParsingPreset;
use test_utils::{tags, upstream};

/// How many statements run at once.
const PARALLEL: usize = 16;

/// Each integer type, signed and unsigned, DECIMALs of several sizes, the floats and a string.
const COLUMNS: &[(&str, &str)] = &[
    ("ti", "tinyint"),
    ("uti", "tinyint unsigned"),
    ("si", "smallint"),
    ("usi", "smallint unsigned"),
    ("mi", "mediumint"),
    ("umi", "mediumint unsigned"),
    ("i", "int"),
    ("ui", "int unsigned"),
    ("bi", "bigint"),
    ("ubi", "bigint unsigned"),
    ("d2_1", "decimal(2,1)"),
    ("d10_2", "decimal(10,2)"),
    ("d8_4", "decimal(8,4)"),
    ("d30_0", "decimal(30,0)"),
    ("d65_30", "decimal(65,30)"),
    ("dbl", "double"),
    ("flt", "float"),
    ("v", "varchar(32)"),
];

/// The largest value of each type, the smallest, small values, zeros and NULLs, in `COLUMNS`
/// order. MySQL works out a product in at most 81 digits and drops the last digits of one side
/// when it needs more, so the largest DECIMAL(65,30) has only 27 digits after the point, which
/// keeps its square exact.
const ROWS: &[&str] = &[
    "127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, \
     18446744073709551615, 9.9, 99999999.99, 9999.9999, 999999999999999999999999999999, \
     99999.999999999999999999999999999, 1.5, 0.5, '2.5'",
    "-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0, -9.9, \
     -99999999.99, -9999.9999, -999999999999999999999999999999, \
     -99999.999999999999999999999999999, -1.5, -0.5, '-2.5'",
    "1, 2, -3, 4, 5, 6, -7, 8, 9, 10, 1.5, -2.25, 0.0625, 3, 0.000000000000000000000000000001, \
     2.5, 0.25, '1.5'",
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '0'",
    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
     NULL, NULL, NULL",
];

fn is_decimal(column: &str) -> bool {
    column.starts_with('d') && column != "dbl"
}

/// Returns every pair of columns where at least one is a DECIMAL.
fn decimal_pairs() -> impl Iterator<Item = (&'static str, &'static str)> {
    COLUMNS
        .iter()
        .flat_map(|(a, _)| COLUMNS.iter().map(move |(b, _)| (*a, *b)))
        .filter(|(a, b)| is_decimal(a) || is_decimal(b))
}

/// An expression's result: its type, its digits after the point, and its value in each row.
#[derive(PartialEq)]
struct Column {
    ty: ColumnType,
    decimals: u8,
    values: Vec<Value>,
}

impl Column {
    /// Drops the trailing zeros after the point of each decimal value.
    fn trim_zeros(&mut self) {
        for value in &mut self.values {
            if let Value::Bytes(bytes) = value
                && let Ok(text) = std::str::from_utf8(bytes)
                && text.contains('.')
            {
                *value = Value::from(text.trim_end_matches('0').trim_end_matches('.'));
            }
        }
    }
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let values: Vec<String> = self
            .values
            .iter()
            .map(|value| value.as_sql(false))
            .collect();
        write!(
            f,
            "{:?}({}) [{}]",
            self.ty,
            self.decimals,
            values.join(", ")
        )
    }
}

/// Returns the statement selecting the id and `exprs` from the table.
fn select(exprs: &[String]) -> String {
    format!("SELECT id, {} FROM m", exprs.join(", "))
}

/// Reads `sql` and returns each column after the id, with the rows in id order, or the error.
async fn read(conn: &mut Conn, sql: &str) -> Result<Vec<Column>, String> {
    let mut rows: Vec<Row> = match conn.exec(sql, ()).await {
        Ok(rows) => rows,
        Err(Error::Server(err)) => return Err(format!("error {}", err.code)),
        Err(err) => return Err(err.to_string()),
    };
    rows.sort_by_key(|row| row.get::<i32, _>(0));
    let columns = rows[0].columns_ref();
    Ok((1..columns.len())
        .map(|i| Column {
            ty: columns[i].column_type(),
            decimals: columns[i].decimals(),
            values: rows.iter().map(|row| row[i].clone()).collect(),
        })
        .collect())
}

/// Reads the statement selecting `exprs` through Readyset and from MySQL, and returns each
/// expression with a different type or values, and whether Readyset sent the read to MySQL.
async fn check(readyset: &mut Conn, upstream: &mut Conn, exprs: Vec<String>) -> Vec<String> {
    let sql = select(&exprs);
    let want = read(upstream, &sql).await;
    let got = read(readyset, &sql).await;
    let (got, want) = match (got, want) {
        (Ok(got), Ok(want)) => (got, want),
        (got, want) if got == want => return Vec::new(),
        (got, want) => return vec![format!("{sql}: Readyset {got:?}, MySQL {want:?}")],
    };
    let mut mismatches = Vec::new();
    let destination = last_query_info(readyset).await.destination;
    if !matches!(destination, QueryDestination::Readyset(_)) {
        mismatches.push(format!("{sql}: Readyset sent it to MySQL"));
    }
    for ((expr, mut got), mut want) in exprs.iter().zip(got).zip(want) {
        // TODO(REA-7193): MySQL writes each value of a CASE with its own branch's digits after
        // the point, and Readyset with the CASE's, so the test compares a CASE's values as
        // numbers.
        if expr.starts_with("CASE") {
            got.trim_zeros();
            want.trim_zeros();
        }
        if got != want {
            mismatches.push(format!("{expr}: Readyset {got:?}, MySQL {want:?}"));
        }
    }
    mismatches
}

/// Caches a statement selecting each list of expressions, reads each through Readyset and from
/// MySQL, and fails with every difference.
async fn compare(statements: Vec<Vec<String>>) {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    mysql_helpers::recreate_database(&db).await;
    let upstream_opts: Opts = mysql_helpers::upstream_config().db_name(Some(&db)).into();
    let mut upstream = Conn::new(upstream_opts.clone()).await.unwrap();
    let columns = COLUMNS
        .iter()
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows = ROWS
        .iter()
        .enumerate()
        .map(|(id, row)| format!("({id}, {row})"))
        .collect::<Vec<_>>()
        .join(", ");
    upstream
        .query_drop(format!(
            "CREATE TABLE m (id int PRIMARY KEY, {columns}); INSERT INTO m VALUES {rows}"
        ))
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .parsing_preset(ParsingPreset::for_prod())
        .replicate_db(&db)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = Conn::new(opts.clone()).await.unwrap();

    let mut mismatches = Vec::new();
    let mut cached = Vec::new();
    for exprs in statements {
        let sql = select(&exprs);
        match readyset
            .query_drop(format!("CREATE CACHE FROM {sql}"))
            .await
        {
            Ok(()) => cached.push(exprs),
            Err(err) => mismatches.push(format!("{sql}: Readyset doesn't cache it: {err}")),
        }
    }
    sleep().await;

    let (readyset, upstream) = (Pool::new(opts), Pool::new(upstream_opts));
    let found: Vec<String> = stream::iter(cached)
        .map(|exprs| {
            let (readyset, upstream) = (readyset.clone(), upstream.clone());
            async move {
                let mut readyset = readyset.get_conn().await.unwrap();
                let mut upstream = upstream.get_conn().await.unwrap();
                check(&mut readyset, &mut upstream, exprs).await
            }
        })
        .buffer_unordered(PARALLEL)
        .concat()
        .await;
    mismatches.extend(found);
    mismatches.sort();
    assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn arithmetic_on_a_decimal_matches_mysql() {
    compare(
        decimal_pairs()
            .map(|(a, b)| {
                ["+", "-", "*", "/", "%"]
                    .map(|op| format!("{a} {op} {b}"))
                    .into()
            })
            .collect(),
    )
    .await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn choosing_among_decimals_matches_mysql() {
    // MySQL chooses among a DECIMAL and a string as strings, so the test covers numbers only.
    compare(
        decimal_pairs()
            .filter(|(a, b)| *a != "v" && *b != "v")
            .map(|(a, b)| {
                vec![
                    format!("COALESCE({a}, {b})"),
                    format!("IFNULL({a}, {b})"),
                    format!("GREATEST({a}, {b})"),
                    format!("LEAST({a}, {b})"),
                    format!("CASE WHEN id < 2 THEN {a} ELSE {b} END"),
                ]
            })
            .collect(),
    )
    .await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn round_of_a_decimal_matches_mysql() {
    // MySQL 5.7 keeps as many digits after the point as ROUND asks for, up to 30, where later
    // versions keep at most the argument's. Readyset follows the later versions, so on 5.7 the
    // test leaves out asking for more digits than the column has.
    let upstream = Conn::new(mysql_helpers::upstream_config()).await.unwrap();
    let mysql57 = upstream.server_version() < (8, 0, 0);
    compare(
        COLUMNS
            .iter()
            .filter(|(column, _)| is_decimal(column))
            .map(|(column, ty)| {
                let (_, scale) = ty.trim_end_matches(')').rsplit_once(',').unwrap();
                let scale: i64 = scale.parse().unwrap();
                let kept = |digits: &&str| match digits.trim_start_matches(", ").parse::<i64>() {
                    Ok(asked) => !mysql57 || asked <= scale,
                    // No precision, or one read from a column.
                    Err(_) => true,
                };
                ["", ", -1", ", 0", ", 1", ", 2", ", 5", ", 31", ", id"]
                    .into_iter()
                    .filter(kept)
                    .map(|digits| format!("ROUND({column}{digits})"))
                    .collect()
            })
            .collect(),
    )
    .await;
}
