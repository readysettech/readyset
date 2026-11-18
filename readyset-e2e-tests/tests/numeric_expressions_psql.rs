//! Compares Readyset with Postgres for expressions over a numeric and another number. Each test
//! caches a statement of the expressions it checks for each pair of column types, or each column,
//! then reads it through Readyset and from Postgres and compares each expression's type and values.

use test_utils::{tags, upstream};

use crate::oracle;

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

/// Fills the table `m` of [`COLUMNS`] with [`ROWS`], and compares each list of expressions selected
/// from it with Postgres.
async fn compare(statements: Vec<Vec<String>>) {
    oracle::postgres::compare(COLUMNS, ROWS, "m", statements).await
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
