use nom_sql::{Dialect, SqlType};
use proptest::strategy::Strategy;
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
