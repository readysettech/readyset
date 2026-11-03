//! Compares Readyset with Postgres for expressions over a numeric and another number. Each test
//! caches a statement of the expressions it checks for each pair of column types, or each column,
//! then reads it through Readyset and from Postgres and compares each expression's type and values.

use futures::{StreamExt, stream};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name, sleep};
use readyset_data::DfValue;
use readyset_sql_parsing::ParsingPreset;
use test_utils::{tags, upstream};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

/// How many statements run at once.
const PARALLEL: usize = 16;

/// Each integer type, numerics with and without a precision, and the floats.
const COLUMNS: &[(&str, &str)] = &[
    ("i2", "smallint"),
    ("i4", "int"),
    ("i8", "bigint"),
    ("n", "numeric"),
    ("n10_2", "numeric(10,2)"),
    ("n8_4", "numeric(8,4)"),
    ("f4", "real"),
    ("f8", "double precision"),
];

/// The largest value of each type, the smallest, small values and NULLs, in `COLUMNS` order. No
/// row has a zero, since Postgres fails a statement that divides by zero.
const ROWS: &[&str] = &[
    "32767, 2147483647, 9223372036854775807, 12345678901234567890.123456789, 99999999.99, \
     9999.9999, 1.5, 2.5",
    "-32768, -2147483648, -9223372036854775808, -12345678901234567890.123456789, -99999999.99, \
     -9999.9999, -1.5, -2.5",
    "1, -2, 3, 1.5, -2.25, 0.0625, 0.25, 0.5",
    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL",
];

fn is_numeric(column: &str) -> bool {
    column.starts_with('n')
}

fn is_float(column: &str) -> bool {
    column.starts_with('f')
}

/// Returns every pair of columns.
fn pairs() -> impl Iterator<Item = (&'static str, &'static str)> {
    COLUMNS
        .iter()
        .flat_map(|(a, _)| COLUMNS.iter().map(move |(b, _)| (*a, *b)))
}

/// An expression's result: its type and its value in each row. A numeric shows all its digits
/// after the point, as Postgres writes it.
#[derive(Debug, PartialEq)]
struct Column {
    ty: Type,
    values: Vec<Option<String>>,
}

/// Returns the statement selecting the id and `exprs` from the table.
fn select(exprs: &[String]) -> String {
    format!("SELECT id, {} FROM m", exprs.join(", "))
}

/// Reads `sql` and returns each column after the id, with the rows in id order, or the error.
async fn read(client: &Client, sql: &str) -> Result<Vec<Column>, String> {
    let error = |err: tokio_postgres::Error| match err.code() {
        Some(code) => format!("error {}", code.code()),
        None => err.to_string(),
    };
    let statement = client.prepare(sql).await.map_err(error)?;
    let mut rows = client.query(&statement, &[]).await.map_err(error)?;
    rows.sort_by_key(|row| row.get::<_, i32>(0));
    // A zero numeric shows its digits after the point only when asked for them.
    let show = |value: DfValue| match value {
        DfValue::Numeric(d) if d.is_zero() => {
            let scale = d.scale().unwrap_or(0).max(0) as usize;
            format!("{d:.scale$}")
        }
        value => value.to_string(),
    };
    Ok((1..statement.columns().len())
        .map(|i| Column {
            ty: statement.columns()[i].type_().clone(),
            values: rows
                .iter()
                .map(|row| match row.try_get::<_, Option<DfValue>>(i) {
                    Ok(value) => value.map(show),
                    Err(err) => Some(format!("{err:?}")),
                })
                .collect(),
        })
        .collect())
}

/// Reads the statement selecting `exprs` through Readyset and from Postgres, and returns each
/// expression with a different type or values, and whether Readyset sent the read to Postgres.
async fn check(readyset: &Client, upstream: &Client, exprs: Vec<String>) -> Vec<String> {
    let sql = select(&exprs);
    let want = read(upstream, &sql).await;
    let got = read(readyset, &sql).await;
    let (got, want) = match (got, want) {
        (Ok(got), Ok(want)) => (got, want),
        (got, want) if got == want => return Vec::new(),
        (got, want) => return vec![format!("{sql}: Readyset {got:?}, Postgres {want:?}")],
    };
    let mut mismatches = Vec::new();
    let destination = last_query_info(readyset).await.destination;
    if !matches!(destination, QueryDestination::Readyset(_)) {
        mismatches.push(format!("{sql}: Readyset sent it to Postgres"));
    }
    for ((expr, got), want) in exprs.iter().zip(got).zip(want) {
        if got != want {
            mismatches.push(format!("{expr}: Readyset {got:?}, Postgres {want:?}"));
        }
    }
    mismatches
}

/// Caches a statement selecting each list of expressions, reads each through Readyset and from
/// Postgres, and fails with every difference.
async fn compare(statements: Vec<Vec<String>>) {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db).await;
    let mut config = psql_helpers::upstream_config();
    config.dbname(&db);
    let upstream = psql_helpers::connect(config.clone()).await;
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
        .batch_execute(&format!(
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
        .build::<PostgreSQLAdapter>()
        .await;
    let readyset = psql_helpers::connect(opts.clone()).await;

    let mut mismatches = Vec::new();
    let mut cached = Vec::new();
    for exprs in statements {
        let sql = select(&exprs);
        match readyset
            .simple_query(&format!("CREATE CACHE FROM {sql}"))
            .await
        {
            Ok(_) => cached.push(exprs),
            Err(err) => mismatches.push(format!("{sql}: Readyset doesn't cache it: {err}")),
        }
    }
    sleep().await;

    let found: Vec<String> = stream::iter(cached)
        .map(|exprs| {
            let (opts, config) = (opts.clone(), config.clone());
            async move {
                let readyset = psql_helpers::connect(opts).await;
                let upstream = psql_helpers::connect(config).await;
                check(&readyset, &upstream, exprs).await
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
#[upstream(postgres)]
async fn arithmetic_on_a_numeric_matches_postgres() {
    compare(
        pairs()
            .filter(|(a, b)| is_numeric(a) || is_numeric(b))
            .map(|(a, b)| {
                let float = is_float(a) || is_float(b);
                // TODO(REA-6642): Readyset divides numerics to 100 digits where Postgres keeps at
                // least 16 significant digits, so the test leaves out dividing two numbers into a
                // numeric. Postgres has no remainder of a float.
                let ops: &[&str] = if float {
                    &["+", "-", "*", "/"]
                } else {
                    &["+", "-", "*", "%"]
                };
                ops.iter().map(|op| format!("{a} {op} {b}")).collect()
            })
            .collect(),
    )
    .await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn greatest_and_least_of_numbers_match_postgres() {
    compare(
        pairs()
            .map(|(a, b)| vec![format!("GREATEST({a}, {b})"), format!("LEAST({a}, {b})")])
            .collect(),
    )
    .await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn round_of_a_numeric_matches_postgres() {
    compare(
        COLUMNS
            .iter()
            .filter(|(column, _)| is_numeric(column))
            .map(|(column, _)| {
                ["", ", -1", ", 0", ", 1", ", 2", ", 5", ", id"]
                    .map(|digits| format!("round({column}{digits})"))
                    .into()
            })
            .collect(),
    )
    .await;
}
