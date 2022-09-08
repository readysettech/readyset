use nom_sql::{Literal, SqlType};
use readyset_errors::{ReadySetError, ReadySetResult};

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
        let err = || ReadySetError::DfValueConversionError {
            src_type: "Enum".to_string(),
            target_type: to_ty.to_string(),
            details: "out of bounds".to_string(),
        };

        let idx = usize::try_from(enum_value).map_err(|_| err())?;

        // TODO Enforce length limits for Char/VarChar if applicable

        if idx == 0 {
            Ok(DfValue::from(""))
        } else if let Some(Literal::String(s)) = enum_elements.get(idx - 1) {
            Ok(DfValue::from(s.as_str()))
        } else {
            Err(err())
        }
    } else {
        integer::coerce_integer(enum_value, "Enum", to_ty, from_ty)
    }
}
