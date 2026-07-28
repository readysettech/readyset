use std::convert::{TryFrom, TryInto};

use mysql_common::chrono::{NaiveDate, NaiveDateTime};
use mysql_srv::{ColumnType, ParamValue, Value, ValueInner};
use readyset_data::encoding::Encoding;
use readyset_data::{DfValue, TimestampTz};
use readyset_errors::{ReadySetError, ReadySetResult};

/// Convert a binary-protocol execute parameter to a [`DfValue`]. String parameters from a
/// latin1 or cp850 client are transcoded to UTF-8, keeping the byte-array representation used
/// for string parameters; parameters of other types keep their raw bytes.
///
/// The type split mirrors MySQL's parameter conversion rules: STRING, VAR_STRING, and VARCHAR
/// parameters are interpreted in character_set_client, while the BLOB family is binary and
/// exempt from charset conversion (it's how clients bind raw byte payloads).
pub(crate) fn mysql_param_to_dataflow_value(
    param: ParamValue<'_>,
    client_encoding: Encoding,
) -> ReadySetResult<DfValue> {
    if matches!(client_encoding, Encoding::Latin1 | Encoding::Cp850)
        && matches!(
            param.coltype,
            ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_VARCHAR
        )
    {
        if let ValueInner::Bytes(b) = param.value.into_inner() {
            return Ok(DfValue::from(client_encoding.decode(b)?.into_bytes()));
        }
    }
    mysql_value_to_dataflow_value(param.value)
}

pub(crate) fn mysql_value_to_dataflow_value(value: Value) -> ReadySetResult<DfValue> {
    Ok(match value.into_inner() {
        ValueInner::Null => DfValue::None,
        ValueInner::Bytes(b) => DfValue::from(b.to_vec()),
        ValueInner::Int(i) => i.into(),
        ValueInner::UInt(i) => i.into(),
        ValueInner::Double(f) => DfValue::try_from(f)?,
        ValueInner::Datetime(_) => {
            if let Ok(ndt) = NaiveDateTime::try_from(value) {
                DfValue::TimestampTz(ndt.into())
            } else {
                DfValue::TimestampTz(TimestampTz::zero())
            }
        }
        ValueInner::Time(_) => {
            DfValue::Time(
                value
                    .try_into()
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "ValueInner::Time".to_string(),
                        target_type: "DfValue::Time".to_string(),
                        details: format!("{e:?}"),
                    })?,
            )
        }
        ValueInner::Date(_) => DfValue::TimestampTz(
            NaiveDate::try_from(value)
                .map_err(|e| ReadySetError::DfValueConversionError {
                    src_type: "ValueInner::Date".to_string(),
                    target_type: "DfValue::TimestampTz".to_string(),
                    details: format!("{e:?}"),
                })?
                .into(),
        ),
    })
}
