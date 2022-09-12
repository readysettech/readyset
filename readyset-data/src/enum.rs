use nom_sql::{EnumType, Literal, SqlType};
use readyset_errors::ReadySetResult;

use crate::{integer, DfType, DfValue};

/// Coerce an enum value to a different type.
///
/// In the case of converting to text types, we use the enum elements to map the enum integer index
/// to the corresponding enum label. Otherwise, we fall back to integer::coerce_integer.
pub(crate) fn coerce_enum(
    enum_value: u64,
    enum_elements: &[Literal],
    to_ty: &SqlType,
    from_ty: &DfType,
) -> ReadySetResult<DfValue> {
    if to_ty.is_any_text() {
        let idx = usize::try_from(enum_value).unwrap_or(0);

        // TODO Enforce length limits for Char/VarChar if applicable

        if idx == 0 {
            Ok(DfValue::from(""))
        } else if let Some(Literal::String(s)) = enum_elements.get(idx - 1) {
            Ok(DfValue::from(s.as_str()))
        } else {
            Ok(DfValue::from("")) // must be out of bounds of enum_elements
        }
    } else {
        integer::coerce_integer(enum_value, to_ty, from_ty)
    }
}

/// Apply enum index boundaries to a number, converting anything outside the boundaries to 0.
///
/// Generally speaking, coercing a different number type to an enum is more or less the same as
/// coercing it to an integer, except that any values less than zero or greater than the maximum
/// enum index will be converted to 0 instead. (Note, however, that MySQL coerces non-integer
/// numbers to enum by rounding them down instead of rounding them off, but these semantics will be
/// applied outside this function.) This helper function aids us in applying these semantics.
pub(crate) fn apply_enum_limits(idx: usize, elements: &EnumType) -> usize {
    if idx > elements.len() {
        0
    } else {
        idx
    }
}
