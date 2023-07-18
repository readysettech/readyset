//! This holds all necessary logic to produce [`Strategy`]s that generate base [`Expr`].
//! Base [`Expr`] are either [`Expr::Literal`] or [`Expr::Column`].
// TODO(fran): Add a way to configure the columns available to the `Expr` generation, and their
//  types, so we can use said columns as a base `Expr` of the column type.
use nom_sql::{Double, Expr, Float, Literal};
use proptest::prelude::BoxedStrategy;
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};

// TODO(fran): Add configuration to generate `Expr::Column`.
//  That is tricky because we need to know which columns are available, and which types they have,
//  and ideally, we should be able to simply choose to use a column of that given type when we
//  need that type.

/// The specification to use when generating a base [`Expr`].
///
/// Essentially, this defines the type of the [`Expr`] and which values it will use when generating
/// literals.
/// The different enum kinds here don't just define types in the traditional sense, but rather a
/// particular shape of [`Expr`] that we might want to generate.
/// For example, the [`ExprSpec::Timezone`] generates string values, but constrained to valid
/// timezone strings, such as "UTC", "Europe/Amsterdam", "MET", "US/Eastern",
/// "Americas/Argentina/Buenos_Aires", etc.
#[allow(dead_code)]
pub(super) enum ExprSpec {
    Bool,
    TinyInt(BoxedStrategy<i8>),
    SmallInt(BoxedStrategy<i16>),
    Int(BoxedStrategy<i32>),
    BigInt(BoxedStrategy<i64>),
    UnsignedTinyInt(BoxedStrategy<u8>),
    UnsignedSmallInt(BoxedStrategy<u16>),
    UnsignedInt(BoxedStrategy<u32>),
    UnsignedBigInt(BoxedStrategy<u64>),
    Float(BoxedStrategy<f32>),
    Double(BoxedStrategy<f64>),
    // TODO(fran): Make collation configurable.
    String(BoxedStrategy<String>),
    Timestamp,
    DateTime,
    Timezone(bool),
    Null,
}

// Helper macro to generate the different types of numeric `Expr`s.
macro_rules! numeric_expr {
    ($range:expr, i64) => {{
        $range
            .prop_map(|i| Expr::Literal(Literal::Integer(i as i64)))
            .boxed()
    }};
    ($range:expr, u64) => {{
        $range
            .prop_map(|i| Expr::Literal(Literal::UnsignedInteger(i as u64)))
            .boxed()
    }};
}

/// Produces a [`Strategy`] that generates a base [`Expr`], based on the given [`ExprSpec`].
#[allow(dead_code)]
pub(super) fn generate_base_expr(params: ExprSpec) -> BoxedStrategy<Expr> {
    match params {
        ExprSpec::Bool => proptest::bool::ANY
            .prop_map(|b| Expr::Literal(Literal::Boolean(b)))
            .boxed(),
        ExprSpec::TinyInt(range) => range
            .prop_map(|int| Expr::Literal(Literal::Integer(int as i64)))
            .boxed(),
        ExprSpec::SmallInt(range) => numeric_expr!(range, i64),
        ExprSpec::Int(range) => numeric_expr!(range, i64),
        ExprSpec::BigInt(range) => numeric_expr!(range, i64),
        ExprSpec::UnsignedTinyInt(range) => {
            numeric_expr!(range, u64)
        }
        ExprSpec::UnsignedSmallInt(range) => {
            numeric_expr!(range, u64)
        }
        ExprSpec::UnsignedInt(range) => {
            numeric_expr!(range, u64)
        }
        ExprSpec::UnsignedBigInt(range) => {
            numeric_expr!(range, u64)
        }
        ExprSpec::Timestamp => readyset_util::arbitrary::arbitrary_date_time()
            .prop_map(|val| Expr::Literal(Literal::String(val.to_string())))
            .boxed(),
        ExprSpec::DateTime => readyset_util::arbitrary::arbitrary_date_time()
            .prop_map(|val| Expr::Literal(Literal::String(val.to_string())))
            .boxed(),
        ExprSpec::Timezone(named_timezones) => {
            // Offset from -13:59 to +14:00
            r"([+-]?(0?[0-9]|1[0-3]):[0-5][0-9])|\+14:00".prop_flat_map(move |offset| {
                if named_timezones {
                    prop_oneof![
                            Just(offset),
                            // Just some of the many IANA timezones, for testing
                            "UTC|MET|US/Eastern|US/Central|Europe/Amsterdam|America/Argentina/Buenos_Aires"
                        ].boxed()
                } else {
                    Just(offset).boxed()
                }
            }).prop_map(|timezone| Expr::Literal(Literal::String(timezone))).boxed()
        }
        ExprSpec::Float(f) => f
            .prop_map(|f| {
                Expr::Literal(Literal::Float(Float {
                    value: f,
                    precision: u8::MAX,
                }))
            })
            .boxed(),
        ExprSpec::Double(d) => d
            .prop_map(|d| {
                Expr::Literal(Literal::Double(Double {
                    value: d,
                    precision: u8::MAX,
                }))
            })
            .boxed(),
        ExprSpec::String(string) => string
            .prop_map(|s| Expr::Literal(Literal::String(s)))
            .boxed(),
        ExprSpec::Null => Just(Expr::Literal(Literal::Null)).boxed(),
    }
}
