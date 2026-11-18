//! Compares Readyset with MySQL for expressions over numbers and strings. Each test caches a
//! statement of the expressions it checks for each pair of column types, or each column, then reads
//! it through Readyset and from MySQL and compares each expression's type and values.

use mysql_async::Conn;
use readyset_client_test_helpers::mysql_helpers;
use test_utils::{tags, upstream};

use crate::oracle;

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

/// Rows of small integers, where a signed column holds -7 and 2 and an unsigned one 8 and 3, so
/// that mixing them gives a result of either sign.
const INTEGER_ROWS: &[&str] = &[
    "-7, 8, -7, 8, -7, 8, -7, 8, -7, 8, 0, 0, 0, 0, 0, 0, 0, '0'",
    "2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 0, 0, 0, 0, 0, 0, 0, '0'",
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '0'",
    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
     NULL, NULL, NULL",
];

fn is_integer(column: &str) -> bool {
    COLUMNS[..10].iter().any(|(name, _)| *name == column)
}

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

/// Fills the table `m` of [`COLUMNS`] with `rows`, and compares each list of expressions selected
/// from it with MySQL.
async fn compare(rows: &[&str], statements: Vec<Vec<String>>) {
    oracle::mysql::compare(COLUMNS, rows, "m", statements).await
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn arithmetic_on_a_decimal_matches_mysql() {
    compare(
        ROWS,
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
        ROWS,
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
        ROWS,
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

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn arithmetic_on_integers_matches_mysql() {
    let integers = || {
        COLUMNS
            .iter()
            .map(|(name, _)| *name)
            .filter(|c| is_integer(c))
    };
    let unsigned = |column: &str| column.starts_with('u');
    compare(
        INTEGER_ROWS,
        integers()
            .flat_map(|a| integers().map(move |b| (a, b)))
            .map(|(a, b)| {
                ["+", "-", "*", "%", "<", "=", "AND", "OR"]
                    .into_iter()
                    // MySQL fails when a result with an unsigned side is negative, so the test
                    // leaves out a signed minus an unsigned value, and a product of mixed signs.
                    .filter(|op| match *op {
                        "-" => unsigned(a) || !unsigned(b),
                        "*" => unsigned(a) == unsigned(b),
                        _ => true,
                    })
                    .map(|op| format!("{a} {op} {b}"))
                    .collect()
            })
            .chain(integers().map(|a| {
                vec![
                    format!("NOT {a}"),
                    format!("{a} IN (2, 3)"),
                    format!("{a} BETWEEN 1 AND 5"),
                    format!("{a} > -1"),
                    format!("{a} BETWEEN -1 AND 1"),
                ]
            }))
            .collect(),
    )
    .await;
}
