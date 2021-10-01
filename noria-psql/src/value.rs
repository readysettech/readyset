use arccstr::ArcCStr;
use eui48::MacAddress;
use noria::{DataType, ReadySetError};
use psql_srv as ps;
use std::convert::TryFrom;
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
        let from_tiny_text = |v| {
            // TODO Avoid this allocation by adding a TinyText storage option in psql-srv.
            ArcCStr::try_from(
                <&str>::try_from(v)
                    .map_err(|e: ReadySetError| ps::Error::InternalError(e.to_string()))?,
            )
            .map_err(|_| ps::Error::InternalError("unexpected nul within TinyText".to_string()))
        };

        // TODO: Implement this for the rest of the types, including at least:
        // - Type::Time
        // - Unsigned{Int,Smallint,Bigint}
        match (v.col_type, v.value) {
            (_, DataType::None) => Ok(ps::Value::Null),
            (Type::CHAR, DataType::Text(v)) => Ok(ps::Value::Char(v)),
            (Type::CHAR, ref v @ DataType::TinyText(_)) => Ok(ps::Value::Char(from_tiny_text(v)?)),
            (Type::VARCHAR, DataType::Text(v)) => Ok(ps::Value::Varchar(v)),
            (Type::VARCHAR, ref v @ DataType::TinyText(_)) => {
                Ok(ps::Value::Varchar(from_tiny_text(v)?))
            }
            (Type::INT2, DataType::Int(v)) => Ok(ps::Value::Smallint(v as _)),
            (Type::INT4, DataType::Int(v)) => Ok(ps::Value::Int(v)),
            (Type::INT8, DataType::BigInt(v)) => Ok(ps::Value::Bigint(v)),
            (Type::INT8, DataType::UnsignedBigInt(v)) => Ok(ps::Value::Bigint(v as _)),
            (Type::INT8, DataType::Int(v)) => Ok(ps::Value::Bigint(v as _)),
            (Type::FLOAT4, DataType::Float(f, _)) => Ok(ps::Value::Float(f)),
            (Type::FLOAT8, DataType::Double(f, _)) => Ok(ps::Value::Double(f)),
            (Type::NUMERIC, DataType::Numeric(ref d)) => Ok(ps::Value::Numeric(*d.as_ref())),
            (Type::TEXT, DataType::Text(v)) => Ok(ps::Value::Text(v)),
            (Type::TEXT, ref v @ DataType::TinyText(_)) => Ok(ps::Value::Text(from_tiny_text(v)?)),
            (Type::TIMESTAMP, DataType::Timestamp(v)) => Ok(ps::Value::Timestamp(v)),
            (Type::DATE, DataType::Timestamp(v)) => Ok(ps::Value::Date(v.date())),
            (Type::TIME, DataType::Time(t)) => Ok(ps::Value::Time((*t.as_ref()).into())),
            (Type::BOOL, DataType::UnsignedInt(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BOOL, DataType::Int(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BYTEA, DataType::ByteArray(b)) => Ok(ps::Value::ByteArray(b.as_ref().clone())),
            (Type::MACADDR, DataType::Text(m)) => Ok(ps::Value::MacAddress(
                m.to_str()
                    .map_err(|e| ps::Error::EncodeError(e.into()))
                    .and_then(|s| {
                        MacAddress::parse_str(s).map_err(|e| ps::Error::ParseError(e.to_string()))
                    })?,
            )),
            (Type::UUID, DataType::Text(u)) => Ok(ps::Value::Uuid(
                u.to_str()
                    .map_err(|e| ps::Error::EncodeError(e.into()))
                    .and_then(|s| {
                        Uuid::parse_str(s).map_err(|e| ps::Error::ParseError(e.to_string()))
                    })?,
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

    use super::*;

    #[test]
    fn tiny_text_char() {
        let val = Value {
            col_type: Type::CHAR,
            value: DataType::TinyText([b'a'; 15]),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Char(ArcCStr::try_from("aaaaaaaaaaaaaaa").unwrap())
        );
    }

    #[test]
    fn tiny_text_varchar() {
        let val = Value {
            col_type: Type::VARCHAR,
            value: DataType::TinyText([b'a'; 15]),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Varchar(ArcCStr::try_from("aaaaaaaaaaaaaaa").unwrap())
        );
    }

    #[test]
    fn tiny_text_text() {
        let val = Value {
            col_type: Type::TEXT,
            value: DataType::TinyText([b'a'; 15]),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Text(ArcCStr::try_from("aaaaaaaaaaaaaaa").unwrap())
        );
    }
}
