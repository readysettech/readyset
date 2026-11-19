//! PostgreSQL literals for values of a [`SqlType`], for tests that check Readyset against
//! PostgreSQL. A literal is a complete SQL expression of its type, such as `'1.5'::NUMERIC(5, 2)`.

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::string::string_regex;

use crate::ast::{Relation, SqlType};
use crate::{Dialect, DialectDisplay};

/// The enum type that [`postgres_setup_sql`] creates.
pub const POSTGRES_TEST_ENUM: &str = "test_enum";

/// The labels of [`POSTGRES_TEST_ENUM`] in declaration order, which is the order that PostgreSQL
/// compares them in. A comparison of their text gives a different order.
pub const POSTGRES_TEST_ENUM_LABELS: &[&str] = &["b", "a", "c"];

/// SQL that creates the citext extension and [`POSTGRES_TEST_ENUM`], which the literals need. It
/// can run more than once in a database.
pub fn postgres_setup_sql() -> String {
    let labels = POSTGRES_TEST_ENUM_LABELS
        .iter()
        .map(|l| format!("'{l}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE EXTENSION IF NOT EXISTS citext; DO $$ BEGIN CREATE TYPE {POSTGRES_TEST_ENUM} AS \
         ENUM ({labels}); EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
    )
}

fn is_test_enum(relation: &Relation) -> bool {
    relation.schema.is_none() && relation.name.as_str() == POSTGRES_TEST_ENUM
}

/// One type of each kind that Readyset supports for PostgreSQL, with and without type parameters,
/// and an array of most kinds that PostgreSQL can compare.
pub fn postgres_representative_types() -> Vec<SqlType> {
    use SqlType::*;
    let scalars = vec![
        Bool,
        SmallInt(None),
        Int(None),
        BigInt(None),
        Numeric(None),
        Numeric(Some((5, Some(2)))),
        Numeric(Some((3, Some(0)))),
        Real,
        Double,
        Text,
        VarChar(Some(5)),
        Char(Some(3)),
        Char(None),
        Citext,
        QuotedChar,
        ByteArray,
        Uuid,
        Bit(Some(3)),
        VarBit(None),
        VarBit(Some(3)),
        Date,
        Time,
        Timestamp,
        TimestampTz,
        Json,
        Jsonb,
        Tsvector,
        Other(POSTGRES_TEST_ENUM.into()),
    ];
    let arrays = [
        Int(None),
        BigInt(None),
        Numeric(None),
        Double,
        Text,
        Char(Some(3)),
        Bool,
        Date,
        TimestampTz,
        Uuid,
        ByteArray,
        Jsonb,
        Other(POSTGRES_TEST_ENUM.into()),
    ]
    .into_iter()
    .map(|t| Array(Box::new(t)));
    scalars.into_iter().chain(arrays).collect()
}

/// The name of the type that a literal of `ty` casts to. SERIAL and BIGSERIAL are valid only as
/// column types, so their literals cast to INT and BIGINT.
fn type_name(ty: &SqlType) -> String {
    match ty {
        SqlType::Serial => "INT".to_owned(),
        SqlType::BigSerial => "BIGINT".to_owned(),
        SqlType::Array(element) => format!("{}[]", type_name(element)),
        ty => ty.display(Dialect::PostgreSQL).to_string(),
    }
}

/// The longest bit string that a literal holds. A cast to a longer BIT type pads it with zeros.
const MAX_BITS: u16 = 16;

fn cast(value: &str, ty: &SqlType) -> String {
    format!("'{}'::{}", value.replace('\'', "''"), type_name(ty))
}

/// A literal of array type `ty` that holds `elements`, each a literal or `NULL`.
fn array(elements: &[String], ty: &SqlType) -> String {
    if elements.is_empty() {
        format!("'{{}}'::{}", type_name(ty))
    } else {
        format!("ARRAY[{}]::{}", elements.join(", "), type_name(ty))
    }
}

const TEXT_EDGES: &[&str] = &[
    "",
    " ",
    "a",
    "a ",
    " a",
    "A",
    "B",
    "b",
    "ab",
    "aa",
    "\u{e4}",
    "z",
    "\u{1F600}",
];

fn numeric_edges(precision: u16, scale: u8) -> Vec<String> {
    let scale = usize::from(scale);
    let int_digits = usize::from(precision).saturating_sub(scale);
    let fraction = |digit: &str| {
        if scale == 0 {
            String::new()
        } else {
            format!(".{}", digit.repeat(scale))
        }
    };
    let max = format!(
        "{}{}",
        if int_digits == 0 {
            "0".to_owned()
        } else {
            "9".repeat(int_digits)
        },
        fraction("9")
    );
    let mut edges = vec![
        "0".to_owned(),
        "-0".to_owned(),
        "0.5".to_owned(),
        "NaN".to_owned(),
    ];
    edges.push(format!("-{max}"));
    edges.push(max);
    if scale > 0 {
        edges.push(format!("0.{}1", "0".repeat(scale - 1)));
    }
    if int_digits > 0 {
        edges.extend(["1.5".to_owned(), "2".to_owned()]);
    }
    edges
}

/// The edge values of `ty` as literals: bounds, signed zeros, NaN, infinities, blanks, letter case
/// and precision limits. Empty if `ty` is not a PostgreSQL type that Readyset supports.
pub fn postgres_edge_literals(ty: &SqlType) -> Vec<String> {
    use SqlType::*;
    let casts = |values: &[&str]| values.iter().map(|v| cast(v, ty)).collect::<Vec<_>>();
    match ty {
        Bool => casts(&["false", "true"]),
        SmallInt(_) | Int2 => casts(&["-32768", "-1", "0", "1", "2", "32767"]),
        Int(_) | Int4 | Serial => casts(&["-2147483648", "-1", "0", "2", "16777217", "2147483647"]),
        BigInt(_) | Int8 | BigSerial => casts(&[
            "-9223372036854775808",
            "-1",
            "0",
            "2",
            "16777217",
            "9007199254740993",
            "9223372036854775807",
        ]),
        Real => casts(&[
            "-Infinity",
            "-3.4028235e38",
            "-1.5",
            "-0",
            "0",
            "1e-45",
            "1.5",
            "2",
            "1004.3",
            "16777216",
            "Infinity",
            "NaN",
        ]),
        Double | Float => casts(&[
            "-Infinity",
            "-1.7976931348623157e308",
            "-1.5",
            "-0",
            "0",
            "5e-324",
            "1.5",
            "2",
            "1004.3",
            "16777216",
            "9007199254740992",
            "1e300",
            "Infinity",
            "NaN",
        ]),
        Numeric(None) => casts(&[
            "-Infinity",
            "-1.5",
            "-0",
            "0",
            "1.5",
            "2",
            "2.00",
            "1004.3",
            "16777217",
            "9007199254740993",
            "1e300",
            "Infinity",
            "NaN",
        ]),
        Numeric(Some((precision, scale))) => numeric_edges(*precision, scale.unwrap_or(0))
            .iter()
            .map(|v| cast(v, ty))
            .collect(),
        Decimal(precision, scale) => numeric_edges(u16::from(*precision), *scale)
            .iter()
            .map(|v| cast(v, ty))
            .collect(),
        Text | VarChar(_) | Char(_) | Citext => casts(TEXT_EDGES),
        QuotedChar => casts(&["", "a", "A", "b"]),
        ByteArray => casts(&[r"\x", r"\x00", r"\x0001", r"\x61", r"\x6100", r"\xff"]),
        Uuid => casts(&[
            "00000000-0000-0000-0000-000000000000",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A12",
            "a0eebc999c0b4ef8bb6d6bb9bd380a13",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]),
        Bit(len) => {
            let len = usize::from(len.unwrap_or(1).clamp(1, MAX_BITS));
            let mut values = vec![
                "0".repeat(len),
                "1".repeat(len),
                format!("1{}", "0".repeat(len - 1)),
                format!("{}1", "0".repeat(len - 1)),
            ];
            values.dedup();
            values.iter().map(|v| cast(v, ty)).collect()
        }
        VarBit(_) => casts(&["", "0", "1", "10", "100", "101", "1010"]),
        Date => casts(&[
            "4713-01-01 BC",
            "0001-01-01",
            "1970-01-01",
            "1999-12-31",
            "2000-01-01",
            "2000-01-02",
            "2000-02-29",
            "5874897-12-31",
            "infinity",
            "-infinity",
        ]),
        Time => casts(&[
            "00:00:00",
            "00:00:00.000001",
            "12:34:56.789",
            "23:59:59.999999",
            "24:00:00",
        ]),
        Timestamp => casts(&[
            "4713-01-01 00:00:00 BC",
            "1999-12-31 23:59:59.999999",
            "2000-01-01 00:00:00",
            "2000-01-01 00:00:00.000001",
            "2000-01-01 12:00:00",
            "2000-01-02 00:00:00",
            "294276-12-31 23:59:59.999999",
            "infinity",
            "-infinity",
        ]),
        TimestampTz => casts(&[
            "1999-12-31 23:59:59.999999+00",
            "2000-01-01 00:00:00+00",
            "2000-01-01 01:00:00+01",
            "1999-12-31 23:00:00-01",
            "2000-01-01 00:00:00.000001+00",
            "2000-01-01 12:00:00+00",
            "2000-01-02 00:00:00+00",
            "infinity",
            "-infinity",
        ]),
        Json => casts(&["{}", "1", "[]", "null"]),
        Jsonb => casts(&[
            "null",
            "false",
            "true",
            "0",
            "1",
            "1.0",
            "2",
            r#""""#,
            r#""a""#,
            r#""b""#,
            "[]",
            "[1]",
            "[1, 2]",
            "[2]",
            "{}",
            r#"{"a": 1}"#,
            r#"{"a": 1.0}"#,
            r#"{"a": 2}"#,
            r#"{"b": 1}"#,
            r#"{"a": 1, "b": 2}"#,
        ]),
        Tsvector => casts(&["", "a", "a b", "b", "a:1"]),
        Other(relation) if is_test_enum(relation) => casts(POSTGRES_TEST_ENUM_LABELS),
        Array(element) => {
            let elements = postgres_edge_literals(element);
            if elements.is_empty() {
                return vec![];
            }
            let pick = |indexes: &[usize]| {
                indexes
                    .iter()
                    .map(|&i| elements[i.min(elements.len() - 1)].clone())
                    .collect::<Vec<_>>()
            };
            vec![
                array(&[], ty),
                array(&pick(&[0]), ty),
                array(&pick(&[1]), ty),
                array(&pick(&[0, 1]), ty),
                array(&pick(&[1, 0]), ty),
                array(&["NULL".to_owned()], ty),
                array(&[pick(&[0])[0].clone(), "NULL".to_owned()], ty),
            ]
        }
        // The match names every variant, so that a new `SqlType` variant must state its literals.
        Signed
        | Unsigned
        | UnsignedInteger
        | SignedInteger
        | IntUnsigned(_)
        | BigIntUnsigned(_)
        | TinyInt(_)
        | TinyIntUnsigned(_)
        | SmallIntUnsigned(_)
        | MediumInt(_)
        | MediumIntUnsigned(_)
        | Blob
        | LongBlob
        | MediumBlob
        | TinyBlob
        | TinyText
        | MediumText
        | LongText
        | DateTime(_)
        | Interval { .. }
        | Binary(_)
        | VarBinary(_)
        | Enum(_)
        | MacAddr
        | Inet
        | Cidr
        | Point
        | PostgisPoint
        | PostgisPolygon
        | Other(_) => vec![],
    }
}

fn float_text(f: f64) -> String {
    match f {
        f if f.is_nan() => "NaN".to_owned(),
        f64::INFINITY => "Infinity".to_owned(),
        f64::NEG_INFINITY => "-Infinity".to_owned(),
        f => format!("{f:e}"),
    }
}

fn date_text() -> impl Strategy<Value = String> {
    (1..=9999u32, 1..=12u32, 1..=28u32).prop_map(|(y, m, d)| format!("{y:04}-{m:02}-{d:02}"))
}

fn time_text() -> impl Strategy<Value = String> {
    (0..24u32, 0..60u32, 0..60u32, 0..1_000_000u32)
        .prop_map(|(h, m, s, us)| format!("{h:02}:{m:02}:{s:02}.{us:06}"))
}

fn json_text() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("null".to_owned()),
        any::<bool>().prop_map(|b| b.to_string()),
        (-3i32..3).prop_map(|i| i.to_string()),
        (-3i32..3).prop_map(|i| format!("{i}.0")),
        select(vec!["\"\"", "\"a\"", "\"b\""]).prop_map(str::to_owned),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..3).prop_map(|items| format!("[{}]", items.join(", "))),
            vec((select(vec!["a", "b"]), inner), 0..3).prop_map(|fields| {
                let fields = fields
                    .iter()
                    .map(|(k, v)| format!("\"{k}\": {v}"))
                    .collect::<Vec<_>>();
                format!("{{{}}}", fields.join(", "))
            }),
        ]
    })
}

/// A strategy for the text of a random value of `ty`. `None` for an array, and for a type whose
/// only literals are its edge values.
fn random_text(ty: &SqlType) -> Option<BoxedStrategy<String>> {
    use SqlType::*;
    Some(match ty {
        Bool => any::<bool>().prop_map(|b| b.to_string()).boxed(),
        SmallInt(_) | Int2 => any::<i16>().prop_map(|i| i.to_string()).boxed(),
        Int(_) | Int4 | Serial => any::<i32>().prop_map(|i| i.to_string()).boxed(),
        BigInt(_) | Int8 | BigSerial => any::<i64>().prop_map(|i| i.to_string()).boxed(),
        Real => any::<f32>().prop_map(|f| float_text(f.into())).boxed(),
        Double | Float => any::<f64>().prop_map(float_text).boxed(),
        Numeric(None) => (any::<i64>(), -30i32..30)
            .prop_map(|(m, e)| format!("{m}e{e}"))
            .boxed(),
        Numeric(Some((precision, scale))) => numeric_text(*precision, scale.unwrap_or(0)).boxed(),
        Decimal(precision, scale) => numeric_text(u16::from(*precision), *scale).boxed(),
        Text | VarChar(_) | Char(_) | Citext => {
            string_regex("[ aAbBz\u{e4}\u{1F600}]{0,4}").ok()?.boxed()
        }
        ByteArray => vec(any::<u8>(), 0..4)
            .prop_map(|bytes| {
                let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
                format!(r"\x{hex}")
            })
            .boxed(),
        Uuid => (any::<[u8; 16]>(), any::<bool>())
            .prop_map(|(bytes, upper)| {
                let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
                let text = format!(
                    "{}-{}-{}-{}-{}",
                    &hex[..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..]
                );
                if upper { text.to_uppercase() } else { text }
            })
            .boxed(),
        Bit(len) => {
            let len = usize::from(len.unwrap_or(1).clamp(1, MAX_BITS));
            bit_text(len..=len).boxed()
        }
        VarBit(len) => bit_text(0..=usize::from(len.unwrap_or(MAX_BITS).min(MAX_BITS))).boxed(),
        Date => date_text().boxed(),
        Time => time_text().boxed(),
        Timestamp => (date_text(), time_text())
            .prop_map(|(d, t)| format!("{d} {t}"))
            .boxed(),
        TimestampTz => (date_text(), time_text(), -12i32..=14, select(vec![0, 30]))
            .prop_map(|(d, t, h, m)| format!("{d} {t}{h:+03}:{m:02}"))
            .boxed(),
        Json | Jsonb => json_text().boxed(),
        QuotedChar | Tsvector | Array(_) | Other(_) => return None,
        Signed
        | Unsigned
        | UnsignedInteger
        | SignedInteger
        | IntUnsigned(_)
        | BigIntUnsigned(_)
        | TinyInt(_)
        | TinyIntUnsigned(_)
        | SmallIntUnsigned(_)
        | MediumInt(_)
        | MediumIntUnsigned(_)
        | Blob
        | LongBlob
        | MediumBlob
        | TinyBlob
        | TinyText
        | MediumText
        | LongText
        | DateTime(_)
        | Interval { .. }
        | Binary(_)
        | VarBinary(_)
        | Enum(_)
        | MacAddr
        | Inet
        | Cidr
        | Point
        | PostgisPoint
        | PostgisPolygon => return None,
    })
}

fn numeric_text(precision: u16, scale: u8) -> impl Strategy<Value = String> {
    let scale = usize::from(scale);
    let int_digits = usize::from(precision).saturating_sub(scale);
    let digits = |n: usize| {
        vec(0..10u8, n).prop_map(|ds| ds.iter().map(|d| char::from(b'0' + d)).collect::<String>())
    };
    (any::<bool>(), 0..=int_digits.min(30), 0..=scale.min(30))
        .prop_flat_map(move |(negative, i, f)| (Just(negative), digits(i), digits(f)))
        .prop_map(|(negative, i, f)| {
            let int = if i.is_empty() { "0".to_owned() } else { i };
            let sign = if negative { "-" } else { "" };
            if f.is_empty() {
                format!("{sign}{int}")
            } else {
                format!("{sign}{int}.{f}")
            }
        })
}

fn bit_text(len: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = String> {
    vec(any::<bool>(), len)
        .prop_map(|bits| bits.iter().map(|&b| if b { '1' } else { '0' }).collect())
}

/// A strategy for a literal of `ty`: an edge value or a random value. `None` if `ty` is not a
/// PostgreSQL type that Readyset supports.
pub fn arbitrary_postgres_literal(ty: &SqlType) -> Option<BoxedStrategy<String>> {
    let edges = postgres_edge_literals(ty);
    if edges.is_empty() {
        return None;
    }
    let edge = select(edges).boxed();
    let random = match ty {
        SqlType::Array(element) => {
            let element = arbitrary_postgres_literal(element)?;
            let ty = ty.clone();
            vec(
                prop_oneof![4 => element, 1 => Just("NULL".to_owned())],
                0..4,
            )
            .prop_map(move |elements| array(&elements, &ty))
            .boxed()
        }
        _ => match random_text(ty) {
            Some(text) => {
                let ty = ty.clone();
                text.prop_map(move |v| cast(&v, &ty)).boxed()
            }
            None => return Some(edge),
        },
    };
    Some(prop_oneof![1 => edge, 3 => random].boxed())
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;
    use crate::ast::SqlTypeArbitraryOptions;

    #[test]
    fn every_representative_type_has_literals() {
        for ty in postgres_representative_types() {
            assert!(!postgres_edge_literals(&ty).is_empty(), "{ty:?}");
            assert!(arbitrary_postgres_literal(&ty).is_some(), "{ty:?}");
        }
    }

    #[proptest]
    fn every_arbitrary_postgres_type_has_literals(
        #[strategy(any_with::<SqlType>(SqlTypeArbitraryOptions {
            dialect: Dialect::PostgreSQL,
            ..Default::default()
        }))]
        ty: SqlType,
    ) {
        prop_assert!(arbitrary_postgres_literal(&ty).is_some(), "{ty:?}");
    }
}
