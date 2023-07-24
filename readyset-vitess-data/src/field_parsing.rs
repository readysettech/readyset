use std::sync::Arc;

use anyhow::Result;
use mysql_time::MySqlTime;
use readyset_data::{DfValue, Text, TimestampTz};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use vitess_grpc::query::Type;

pub fn vstream_value_to_noria_value(raw_value: &[u8], field_type: Type) -> Result<DfValue> {
    let str_value = std::str::from_utf8(raw_value)?;

    match field_type {
        Type::NullType => Ok(DfValue::None),
        Type::Uint8 | Type::Uint16 | Type::Uint24 | Type::Uint32 | Type::Uint64 | Type::Year => {
            Ok(DfValue::UnsignedInt(str_value.parse::<u64>()?))
        }
        Type::Int8 | Type::Int16 | Type::Int24 | Type::Int32 | Type::Int64 => {
            Ok(DfValue::Int(str_value.parse::<i64>()?))
        }
        Type::Text | Type::Varchar | Type::Json | Type::Char => {
            Ok(DfValue::Text(Text::from(str_value)))
        }
        Type::Blob | Type::Varbinary | Type::Binary => {
            Ok(DfValue::ByteArray(Arc::new(raw_value.to_vec())))
        }
        Type::Float32 | Type::Float64 => Ok(DfValue::Float(str_value.parse::<f32>()?)),
        Type::Decimal => {
            let decimal = Decimal::from_str(str_value)?;
            Ok(DfValue::Numeric(Arc::new(decimal)))
        }
        Type::Date | Type::Datetime | Type::Timestamp => {
            let timestamp = TimestampTz::from_str(str_value)?;
            Ok(DfValue::TimestampTz(timestamp))
        }
        Type::Time => {
            let mysql_time = MySqlTime::from_bytes(raw_value)?;
            Ok(DfValue::Time(mysql_time))
        }
        Type::Bit => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Enum => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Set => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Tuple => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Geometry => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Expression => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Hexnum => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Hexval => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Bitnum => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
    }
}
