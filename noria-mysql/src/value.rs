use msql_srv::{Value, ValueInner};
use mysql::chrono::NaiveDate;
use noria::{DataType, ReadySetError, ReadySetResult};
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

pub(crate) fn mysql_value_to_datatype(value: Value) -> ReadySetResult<DataType> {
    Ok(match value.into_inner() {
        ValueInner::Null => DataType::None,
        ValueInner::Bytes(b) => DataType::try_from(b).unwrap(),
        ValueInner::Int(i) => i.into(),
        ValueInner::UInt(i) => (i as i32).into(),
        ValueInner::Double(f) => DataType::try_from(f)?,
        ValueInner::Datetime(_) => DataType::Timestamp(value.try_into().map_err(|e| {
            ReadySetError::DataTypeConversionError {
                val: format!("{:?}", value.into_inner()),
                src_type: "ValueInner::Datetime".to_string(),
                target_type: "DataType::Timestamp".to_string(),
                details: format!("{:?}", e),
            }
        })?),
        ValueInner::Time(_) => DataType::Time(Arc::new(value.try_into().map_err(|e| {
            ReadySetError::DataTypeConversionError {
                val: format!("{:?}", value.into_inner()),
                src_type: "ValueInner::Time".to_string(),
                target_type: "Datatype::Time".to_string(),
                details: format!("{:?}", e),
            }
        })?)),
        ValueInner::Date(_) => DataType::Timestamp(
            NaiveDate::try_from(value)
                .map_err(|e| ReadySetError::DataTypeConversionError {
                    val: format!("{:?}", value.into_inner()),
                    src_type: "ValueInner::Time".to_string(),
                    target_type: "Datatype::Time".to_string(),
                    details: format!("{:?}", e),
                })?
                .and_hms(0, 0, 0),
        ),
    })
}
