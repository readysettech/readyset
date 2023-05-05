use nom_sql::{Dialect, SqlType};
use proptest::strategy::{Just, Strategy};
use proptest::{option, prop_oneof};
use SqlType::*;

/// Returns a proptest strategy to generate *numeric* [`SqlType`]s, optionally filtering to only
/// those which are valid for the given SQL dialect
pub fn arbitrary_numeric_type(dialect: Option<Dialect>) -> impl Strategy<Value = SqlType> {
    let mut variants = vec![SmallInt(None), Int(None), BigInt(None), Double, Float, Real];

    if dialect.is_none() || dialect == Some(Dialect::MySQL) {
        variants.extend([
            TinyInt(None),
            UnsignedInt(None),
            UnsignedBigInt(None),
            UnsignedTinyInt(None),
            UnsignedSmallInt(None),
        ])
    }

    proptest::sample::select(variants)
}

/// Returns a proptest strategy to generate types which are valid as the argument to the `min` and
/// `max` aggregates in PostgreSQL.
///
/// From <https://www.postgresql.org/docs/current/functions-aggregate.html>:
///
/// > Available for any numeric, string, date/time, or enum type, as well as inet, interval, money,
/// > oid, pg_lsn, tid, xid8, and arrays of any of these types.
pub fn arbitrary_postgres_min_max_arg_type() -> impl Strategy<Value = SqlType> {
    prop_oneof![
        // numeric...
        Just(SmallInt(None)),
        Just(Int(None)),
        Just(BigInt(None)),
        Just(Double),
        Just(Float),
        Just(Real),
        Just(Int2).boxed(),
        Just(Int4).boxed(),
        Just(Int8).boxed(),
        option::of((1..=65u16).prop_flat_map(|n| {
            (
                Just(n),
                if n > 28 {
                    Just(None).boxed()
                } else {
                    option::of(0..=(n as u8)).boxed()
                },
            )
        }))
        .prop_map(Numeric),
        (1..=28u8).prop_flat_map(|prec| (1..=prec).prop_map(move |scale| Decimal(prec, scale))),
        // string...
        option::of(1..255u16).prop_map(Char).boxed(),
        option::of(1..255u16).prop_map(VarChar),
        Just(Text).boxed(),
        Just(Citext).boxed(),
        // date/time...
        Just(Date).boxed(),
        Just(Time).boxed(),
        Just(Timestamp).boxed(),
        Just(TimestampTz).boxed(),
        // TODO: enum...
        Just(Inet)
    ]
    // TODO: arrays
    // .prop_recursive(1, 1, 1, |elem| elem.prop_map(|elem| Array(Box::new(elem))))
}
