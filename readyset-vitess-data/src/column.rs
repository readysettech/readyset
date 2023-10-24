use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use bit_vec::BitVec;
use mysql_time::MySqlTime;
use readyset_data::{DfValue, Text, TimestampTz};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use vitess_grpc::query::{Field, MySqlFlag, Type};

fn parse_enum_or_set(def: &str, flags: u32) -> Vec<String> {
    if flags & MySqlFlag::EnumFlag as u32 == 0 && flags & MySqlFlag::SetFlag as u32 == 0 {
        return Vec::new();
    }

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

pub struct Column {
    pub name: String,
    pub grpc_type: vitess_grpc::query::Type,
    pub column_type: String,
    pub flags: u32,
    enum_or_set_values: Vec<String>,
}

impl Column {
    pub fn new(
        name: &str,
        grpc_type: vitess_grpc::query::Type,
        column_type: &str,
        flags: u32,
    ) -> Self {
        let enum_or_set_values = parse_enum_or_set(column_type, flags);

        Self {
            name: name.to_string(),
            grpc_type,
            column_type: column_type.to_string(),
            flags,
            enum_or_set_values,
        }
    }

    pub fn vstream_value_to_noria_value(&self, raw_value: &[u8]) -> Result<DfValue> {
        if raw_value.is_empty() {
            return Ok(DfValue::None);
        }

        match self.grpc_type {
            Type::NullType => Ok(DfValue::None),
            Type::Uint8
            | Type::Uint16
            | Type::Uint24
            | Type::Uint32
            | Type::Uint64
            | Type::Year => {
                let str_value = std::str::from_utf8(raw_value)?;
                Ok(DfValue::UnsignedInt(str_value.parse()?))
            }
            Type::Int8 | Type::Int16 | Type::Int24 | Type::Int32 | Type::Int64 => {
                let str_value = std::str::from_utf8(raw_value)?;
                Ok(DfValue::Int(str_value.parse()?))
            }
            Type::Text | Type::Varchar | Type::Json | Type::Char => {
                let str_value = std::str::from_utf8(raw_value)?;
                Ok(DfValue::Text(Text::from(str_value)))
            }
            Type::Blob | Type::Varbinary | Type::Binary => {
                Ok(DfValue::ByteArray(Arc::new(raw_value.to_vec())))
            }
            Type::Float32 => {
                let str_value = std::str::from_utf8(raw_value)?;
                Ok(DfValue::Float(str_value.parse()?))
            }
            Type::Float64 => {
                let str_value = std::str::from_utf8(raw_value)?;
                Ok(DfValue::Double(str_value.parse()?))
            }
            Type::Decimal => {
                let str_value = std::str::from_utf8(raw_value)?;
                let decimal = Decimal::from_str(str_value)?;
                Ok(DfValue::Numeric(Arc::new(decimal)))
            }
            Type::Date | Type::Datetime | Type::Timestamp => {
                let str_value = std::str::from_utf8(raw_value)?;
                let timestamp = TimestampTz::from_str(str_value)?;
                Ok(DfValue::TimestampTz(timestamp))
            }
            Type::Time => {
                let mysql_time = MySqlTime::from_bytes(raw_value)?;
                Ok(DfValue::Time(mysql_time))
            }
            Type::Enum => {
                let str_value = std::str::from_utf8(raw_value)?;
                let enum_index = str_value.parse::<usize>()?;
                let enum_value = self.enum_or_set_values.get(enum_index).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Invalid enum index: {}, enum values: {:?}",
                        enum_index,
                        self.enum_or_set_values
                    )
                })?;

                Ok(DfValue::Text(Text::from(enum_value.deref())))
            }
            Type::Set => {
                let str_value = std::str::from_utf8(raw_value)?;
                let set_raw_value = str_value.parse::<usize>()?;

                let set_values = self
                    .enum_or_set_values
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
            Type::Bit => {
                let bit_vec = BitVec::from_bytes(raw_value);
                Ok(DfValue::BitVector(Arc::new(bit_vec)))
            }
            Type::Geometry
            | Type::Expression
            | Type::Hexnum
            | Type::Hexval
            | Type::Bitnum
            | Type::Tuple => Err(anyhow::anyhow!(
                "MySQL Type parsing is not implemented for type={:?}. Raw value={:?}",
                self.column_type,
                raw_value
            )),
        }
    }

    pub fn is_not_null(&self) -> bool {
        self.flags & MySqlFlag::NotNullFlag as u32 != 0
    }

    pub fn is_primary_key(&self) -> bool {
        self.flags & MySqlFlag::PriKeyFlag as u32 != 0
    }

    pub fn is_unique_key(&self) -> bool {
        self.flags & MySqlFlag::UniqueKeyFlag as u32 != 0
    }

    pub fn is_multiple_key(&self) -> bool {
        self.flags & MySqlFlag::MultipleKeyFlag as u32 != 0
    }

    pub fn is_blob(&self) -> bool {
        self.flags & MySqlFlag::BlobFlag as u32 != 0
    }

    pub fn is_unsigned(&self) -> bool {
        self.flags & MySqlFlag::UnsignedFlag as u32 != 0
    }

    pub fn is_zerofill(&self) -> bool {
        self.flags & MySqlFlag::ZerofillFlag as u32 != 0
    }

    pub fn is_binary(&self) -> bool {
        self.flags & MySqlFlag::BinaryFlag as u32 != 0
    }

    pub fn is_enum(&self) -> bool {
        self.flags & MySqlFlag::EnumFlag as u32 != 0
    }

    pub fn is_auto_increment(&self) -> bool {
        self.flags & MySqlFlag::AutoIncrementFlag as u32 != 0
    }

    pub fn is_timestamp(&self) -> bool {
        self.flags & MySqlFlag::TimestampFlag as u32 != 0
    }

    pub fn is_set(&self) -> bool {
        self.flags & MySqlFlag::SetFlag as u32 != 0
    }

    pub fn has_no_default_value(&self) -> bool {
        self.flags & MySqlFlag::NoDefaultValueFlag as u32 != 0
    }

    pub fn is_on_update_now(&self) -> bool {
        self.flags & MySqlFlag::OnUpdateNowFlag as u32 != 0
    }

    pub fn is_num(&self) -> bool {
        self.flags & MySqlFlag::NumFlag as u32 != 0
    }

    pub fn is_part_key(&self) -> bool {
        self.flags & MySqlFlag::PartKeyFlag as u32 != 0
    }

    pub fn is_unique(&self) -> bool {
        self.flags & MySqlFlag::UniqueFlag as u32 != 0
    }

    pub fn is_bincmp(&self) -> bool {
        self.flags & MySqlFlag::BincmpFlag as u32 != 0
    }
}

impl From<&Field> for Column {
    fn from(field: &Field) -> Self {
        Column::new(
            &field.name,
            vitess_grpc::query::Type::from_i32(field.r#type).unwrap(),
            &field.column_type,
            field.flags,
        )
    }
}
