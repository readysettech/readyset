use std::sync::Arc;

use anyhow::Result;
use readyset_data::{DfValue, Text};
use vitess_grpc::query::Type;

pub fn vstream_value_to_noria_value(raw_value: &[u8], field_type: Type) -> Result<DfValue> {
    let str_value = std::str::from_utf8(raw_value)?;

    match field_type {
        Type::NullType => Ok(DfValue::None),

        Type::Uint8 | Type::Uint16 | Type::Uint24 | Type::Uint32 | Type::Uint64 => {
            Ok(DfValue::UnsignedInt(str_value.parse::<u64>()?))
        }

        Type::Int8 | Type::Int16 | Type::Int24 | Type::Int32 | Type::Int64 => {
            Ok(DfValue::Int(str_value.parse::<i64>()?))
        }

        Type::Text | Type::Varchar => Ok(DfValue::Text(Text::from(str_value))),

        Type::Blob | Type::Varbinary | Type::Binary => {
            Ok(DfValue::ByteArray(Arc::new(raw_value.to_vec())))
        }

        Type::Float32 | Type::Float64 => Ok(DfValue::Float(str_value.parse::<f32>()?)),

        Type::Timestamp => todo!(),
        Type::Date => todo!(),
        Type::Time => todo!(),
        Type::Datetime => todo!(),
        Type::Year => todo!(),
        Type::Decimal => todo!(),
        Type::Char => todo!(),
        Type::Bit => todo!(),
        Type::Enum => todo!(),
        Type::Set => todo!(),
        Type::Tuple => todo!(),
        Type::Geometry => todo!(),
        Type::Json => todo!(),
        Type::Expression => todo!(),
        Type::Hexnum => todo!(),
        Type::Hexval => todo!(),
        Type::Bitnum => todo!(),
    }
}
