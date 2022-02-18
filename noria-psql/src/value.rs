use std::convert::TryFrom;

use eui48::MacAddress;
use noria_data::DataType;
use psql_srv as ps;
use rust_decimal::Decimal;
use tokio_postgres::types::Type;
use tracing::error;
use uuid::Uuid;

/// An encapsulation of a Noria `DataType` value that facilitates conversion of this `DataType`
/// into a `psql_srv::Value`.
pub struct Value {
    /// A type attribute used to determine which variant of `psql_srv::Value` the `value` attribute
    /// should be converted to.
    pub col_type: Type,

    /// The data value itself.
    pub value: DataType,
}

impl TryFrom<Value> for ps::Value {
    type Error = ps::Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        // TODO: Implement this for the rest of the types, including at least:
        // - Type::Time
        // - Unsigned{Int,Smallint,Bigint}
        match (v.col_type, v.value) {
            (_, DataType::None) => Ok(ps::Value::Null),
            (Type::CHAR, DataType::Text(v)) => Ok(ps::Value::Char(v)),
            (Type::CHAR, DataType::TinyText(t)) => Ok(ps::Value::Char(t.as_str().into())),
            (Type::VARCHAR, DataType::Text(v)) => Ok(ps::Value::Varchar(v)),
            (Type::VARCHAR, DataType::TinyText(t)) => Ok(ps::Value::Varchar(t.as_str().into())),

            (Type::INT2, DataType::Int(v)) => Ok(ps::Value::Smallint(v as _)),
            (Type::INT4, DataType::Int(v)) => Ok(ps::Value::Int(v as _)),
            (Type::INT8, DataType::Int(v)) => Ok(ps::Value::Bigint(v as _)),

            (Type::INT2, DataType::UnsignedInt(v)) => Ok(ps::Value::Smallint(v as _)),
            (Type::INT4, DataType::UnsignedInt(v)) => Ok(ps::Value::Int(v as _)),
            (Type::INT8, DataType::UnsignedInt(v)) => Ok(ps::Value::Bigint(v as _)),

            (Type::FLOAT4, DataType::Float(f)) => Ok(ps::Value::Float(f)),
            (Type::FLOAT8, DataType::Double(f)) => Ok(ps::Value::Double(f)),
            (Type::NUMERIC, DataType::Double(f)) => Ok(ps::Value::Numeric(
                <Decimal>::try_from(f).map_err(|e| ps::Error::InternalError(e.to_string()))?,
            )),
            (Type::NUMERIC, DataType::Numeric(ref d)) => Ok(ps::Value::Numeric(*d.as_ref())),
            (Type::TEXT, DataType::Text(v)) => Ok(ps::Value::Text(v)),
            (Type::TEXT, DataType::TinyText(t)) => Ok(ps::Value::Text(t.as_str().into())),
            (Type::TIMESTAMP, DataType::TimestampTz(v)) => {
                Ok(ps::Value::Timestamp(v.to_chrono().naive_local()))
            }
            (Type::TIMESTAMPTZ, DataType::TimestampTz(v)) => {
                Ok(ps::Value::TimestampTz(v.to_chrono()))
            }
            (Type::DATE, DataType::TimestampTz(v)) => {
                Ok(ps::Value::Date(v.to_chrono().naive_local().date()))
            }
            (Type::TIME, DataType::Time(t)) => Ok(ps::Value::Time((t).into())),
            (Type::BOOL, DataType::UnsignedInt(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BOOL, DataType::Int(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BYTEA, DataType::ByteArray(b)) => Ok(ps::Value::ByteArray(
                std::sync::Arc::try_unwrap(b).unwrap_or_else(|v| v.as_ref().to_vec()),
            )),
            (Type::MACADDR, DataType::Text(m)) => Ok(ps::Value::MacAddress(
                MacAddress::parse_str(m.as_str())
                    .map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::UUID, DataType::Text(u)) => Ok(ps::Value::Uuid(
                Uuid::parse_str(u.as_str()).map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::JSON, ref d @ (DataType::Text(_) | DataType::TinyText(_))) => {
                Ok(ps::Value::Json(
                    <&str>::try_from(d)
                        .map_err(|e| ps::Error::InternalError(e.to_string()))
                        .and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .map_err(|e| ps::Error::ParseError(e.to_string()))
                        })?,
                ))
            }
            (Type::JSONB, ref d @ (DataType::Text(_) | DataType::TinyText(_))) => {
                Ok(ps::Value::Jsonb(
                    <&str>::try_from(d)
                        .map_err(|e| ps::Error::InternalError(e.to_string()))
                        .and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .map_err(|e| ps::Error::ParseError(e.to_string()))
                        })?,
                ))
            }
            (Type::BIT, DataType::BitVector(ref b)) => Ok(ps::Value::Bit(b.as_ref().clone())),
            (Type::VARBIT, DataType::BitVector(ref b)) => Ok(ps::Value::VarBit(b.as_ref().clone())),
            (t, dt) => {
                error!(
                    psql_type = %t,
                    data_type = ?dt.sql_type(),
                    "Tried to serialize value to postgres with unsupported type"
                );
                Err(ps::Error::UnsupportedType(t))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use noria_data::TinyText;

    use super::*;

    #[test]
    fn tiny_text_char() {
        let val = Value {
            col_type: Type::CHAR,
            value: DataType::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Char("aaaaaaaaaaaaaa".into())
        );
    }

    #[test]
    fn tiny_text_varchar() {
        let val = Value {
            col_type: Type::VARCHAR,
            value: DataType::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Varchar("aaaaaaaaaaaaaa".into())
        );
    }

    #[test]
    fn tiny_text_text() {
        let val = Value {
            col_type: Type::TEXT,
            value: DataType::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Text("aaaaaaaaaaaaaa".into())
        );
    }
}
