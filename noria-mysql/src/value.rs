use std::convert::{TryFrom, TryInto};

use mysql_common::chrono::{NaiveDate, NaiveDateTime};
use mysql_srv::{Value, ValueInner};
use noria_data::DataType;
use noria_errors::{ReadySetError, ReadySetResult};

pub(crate) fn mysql_value_to_datatype(value: Value) -> ReadySetResult<DataType> {
    Ok(match value.into_inner() {
        ValueInner::Null => DataType::None,
        ValueInner::Bytes(b) => DataType::from(b),
        ValueInner::Int(i) => i.into(),
        ValueInner::UInt(i) => (i as i32).into(),
        ValueInner::Double(f) => DataType::try_from(f)?,
        ValueInner::Datetime(_) => DataType::TimestampTz(
            NaiveDateTime::try_from(value)
                .map_err(|e| ReadySetError::DataTypeConversionError {
                    src_type: "ValueInner::Datetime".to_string(),
                    target_type: "DataType::TimestampTz".to_string(),
                    details: format!("{:?}", e),
                })?
                .into(),
        ),
        ValueInner::Time(_) => DataType::Time(value.try_into().map_err(|e| {
            ReadySetError::DataTypeConversionError {
                src_type: "ValueInner::Time".to_string(),
                target_type: "Datatype::Time".to_string(),
                details: format!("{:?}", e),
            }
        })?),
        ValueInner::Date(_) => DataType::TimestampTz(
            NaiveDate::try_from(value)
                .map_err(|e| ReadySetError::DataTypeConversionError {
                    src_type: "ValueInner::Date".to_string(),
                    target_type: "Datatype::TimestampTz".to_string(),
                    details: format!("{:?}", e),
                })?
                .into(),
        ),
    })
}
