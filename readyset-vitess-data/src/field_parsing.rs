use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use mysql_time::MySqlTime;
use readyset_data::{DfValue, Text, TimestampTz};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use vitess_grpc::query::Type;

pub fn vstream_value_to_noria_value(
    raw_value: &[u8],
    field_type: Type,
    type_name: Option<&String>, // TODO: Pass a reference to the list of enum values
) -> Result<DfValue> {
    let str_value = std::str::from_utf8(raw_value)?;

    match field_type {
        Type::NullType => Ok(DfValue::None),
        Type::Uint8 | Type::Uint16 | Type::Uint24 | Type::Uint32 | Type::Uint64 | Type::Year => {
            Ok(DfValue::UnsignedInt(str_value.parse()?))
        }
        Type::Int8 | Type::Int16 | Type::Int24 | Type::Int32 | Type::Int64 => {
            Ok(DfValue::Int(str_value.parse()?))
        }
        Type::Text | Type::Varchar | Type::Json | Type::Char => {
            Ok(DfValue::Text(Text::from(str_value)))
        }
        Type::Blob | Type::Varbinary | Type::Binary => {
            Ok(DfValue::ByteArray(Arc::new(raw_value.to_vec())))
        }
        Type::Float32 => Ok(DfValue::Float(str_value.parse()?)),
        Type::Float64 => Ok(DfValue::Double(str_value.parse()?)),

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
        Type::Enum => {
            let type_name = type_name.ok_or_else(|| anyhow::anyhow!("Missing type name"))?;
            let enum_values = parse_enum_or_set(type_name);
            let enum_index = str_value.parse::<usize>()?;
            let enum_value = enum_values.get(enum_index).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid enum index: {}, enum values: {:?}",
                    enum_index,
                    enum_values
                )
            })?;

            Ok(DfValue::Text(Text::from(enum_value.deref())))
        }
        Type::Set => {
            let type_name = type_name.ok_or_else(|| anyhow::anyhow!("Missing type name"))?;
            let set_values = parse_enum_or_set(type_name);
            let set_raw_value = str_value.parse::<usize>()?;

            let set_values = set_values
                .iter()
                .enumerate()
                .filter(|(i, _)| set_raw_value & (1 << i) != 0)
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>();

            let df_values = set_values
                .iter()
                .map(|v| DfValue::Text(Text::from(v.deref())))
                .collect::<Vec<_>>();

            Ok(DfValue::Array(Arc::new(df_values.try_into()?)))
        }
        Type::Geometry => panic!(
            "Not implemented yet: type={:?}, val={}",
            field_type, str_value
        ),
        Type::Expression | Type::Hexnum | Type::Hexval | Type::Bitnum | Type::Tuple => {
            Err(anyhow::anyhow!(
                "Not implemented yet: type={:?}, val={}",
                field_type,
                str_value
            ))
        }
    }
}

fn parse_enum_or_set(def: &str) -> Vec<String> {
    // Attempt to find the opening and closing parentheses.
    let start = def.find('(');
    let end = def.rfind(')');

    if let (Some(start), Some(end)) = (start, end) {
        // Extract the portion of the string inside the parentheses.
        let contents = &def[start + 1..end];

        // Split the contents by commas, then remove single quotes from each value.
        contents
            .split(',')
            .map(|s| s.trim_matches('\'').to_string())
            .collect()
    } else {
        Vec::new()
    }
}
