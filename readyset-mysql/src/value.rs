use std::convert::{TryFrom, TryInto};

use mysql_common::chrono::{NaiveDate, NaiveDateTime};
use mysql_srv::{Value, ValueInner};
use readyset_data::DfValue;
use readyset_errors::{ReadySetError, ReadySetResult};

pub(crate) fn mysql_value_to_dataflow_value(value: Value) -> ReadySetResult<DfValue> {
    Ok(match value.into_inner() {
        ValueInner::Null => DfValue::None,
        ValueInner::Bytes(b) => DfValue::from(b),
        ValueInner::Int(i) => i.into(),
        ValueInner::UInt(i) => (i as i32).into(),
        ValueInner::Double(f) => DfValue::try_from(f)?,
        ValueInner::Datetime(_) => DfValue::TimestampTz(
            NaiveDateTime::try_from(value)
                .map_err(|e| ReadySetError::DfValueConversionError {
                    src_type: "ValueInner::Datetime".to_string(),
                    target_type: "DfValue::TimestampTz".to_string(),
                    details: format!("{:?}", e),
                })?
                .into(),
        ),
        ValueInner::Time(_) => {
            DfValue::Time(
                value
                    .try_into()
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "ValueInner::Time".to_string(),
                        target_type: "DfValue::Time".to_string(),
                        details: format!("{:?}", e),
                    })?,
            )
        }
        ValueInner::Date(_) => DfValue::TimestampTz(
            NaiveDate::try_from(value)
                .map_err(|e| ReadySetError::DfValueConversionError {
                    src_type: "ValueInner::Date".to_string(),
                    target_type: "DfValue::TimestampTz".to_string(),
                    details: format!("{:?}", e),
                })?
                .into(),
        ),
    })
}
