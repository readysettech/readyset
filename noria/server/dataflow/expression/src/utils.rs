use nom_sql::SqlType;
use noria_data::noria_type::Type;
use noria_data::DataType;

use crate::{BuiltinFunction, Expression};

/** These helpers initialize `Expression` variants with a type field. These are
 * not inteded for use outside of tests. A planned implementation of the type
 * inference system will make the type paramter of `Expression` generic, which
 * will allow variants to be constructed without any type information - leaving
 * that to the type inference system. These functions will then be removed */

/// Helper to create `Expression::Column`. Type is unknown by default. The correct type may need to
/// be populated when type is checked at runtime
///
/// Not intended for use outside of tests
pub fn make_column(index: usize) -> Expression {
    column_with_type(index, Type::Unknown)
}

/// Create `Expression::Column` with type set to Int
///
/// Not intended for use outside of tests
pub fn make_int_column(index: usize) -> Expression {
    column_with_type(index, Type::Sql(SqlType::Int(None)))
}

/// Create `Expression::Column` with `Type` ty.
pub fn column_with_type(index: usize, ty: Type) -> Expression {
    Expression::Column { index, ty }
}

/// Create `Expression::Literal` from `DataType`. Type is inferred from `DataType`.
///
/// Not intended for use outside of tests
pub fn make_literal(val: DataType) -> Expression {
    Expression::Literal {
        val: val.clone(),
        ty: val.sql_type().into(),
    }
}

/// Create `Expression::Call` from `BuiltinFunction`. Type is `Unknown`.
///
/// Not intended for use outside of tests
pub fn make_call(func: BuiltinFunction) -> Expression {
    Expression::Call {
        func,
        ty: Type::Unknown,
    }
}
