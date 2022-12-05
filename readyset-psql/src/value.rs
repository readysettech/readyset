use std::convert::{TryFrom, TryInto};
use std::net::IpAddr;

use eui48::MacAddress;
use postgres_types::Kind;
use ps::util::type_is_oid;
use psql_srv as ps;
use readyset_data::DfValue;
use readyset_tracing::{error, trace};
use rust_decimal::Decimal;
use tokio_postgres::types::Type;
use uuid::Uuid;

/// An encapsulation of a ReadySet `DfValue` value that facilitates conversion of this `DfValue`
/// into a `psql_srv::Value`.
pub struct Value {
    /// A type attribute used to determine which variant of `psql_srv::Value` the `value` attribute
    /// should be converted to.
    pub col_type: Type,

    /// The data value itself.
    pub value: DfValue,
}

impl TryFrom<Value> for ps::Value {
    type Error = ps::Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let convert_enum_value = |vs: &[String], val| {
            let idx = u64::try_from(val).map_err(|e| {
                ps::Error::InternalError(format!("Invalid representation for enum value: {e}"))
            })?;
            if idx == 0 {
                return Err(ps::Error::InternalError("Invalid enum value".into()));
            }
            vs.get((idx - 1) as usize)
                .ok_or_else(|| ps::Error::InternalError("Enum variant index out-of-bounds".into()))
                .cloned()
        };

        match (v.col_type, v.value) {
            (_, DfValue::None) => Ok(ps::Value::Null),
            (Type::CHAR, DfValue::Int(v)) => Ok(ps::Value::Char(v.try_into()?)),
            (Type::CHAR, DfValue::UnsignedInt(v)) => Ok(ps::Value::Char(v.try_into()?)),

            (Type::VARCHAR, DfValue::Text(v)) => Ok(ps::Value::VarChar(v)),
            (Type::VARCHAR, DfValue::TinyText(t)) => Ok(ps::Value::VarChar(t.as_str().into())),
            (Type::NAME, DfValue::Text(t)) => Ok(ps::Value::Name(t)),
            (Type::NAME, DfValue::TinyText(t)) => Ok(ps::Value::Name(t.as_str().into())),
            (Type::BPCHAR, DfValue::Text(v)) => Ok(ps::Value::BpChar(v)),
            (Type::BPCHAR, DfValue::TinyText(t)) => Ok(ps::Value::BpChar(t.as_str().into())),

            (Type::INT2, DfValue::Int(v)) => Ok(ps::Value::SmallInt(v as _)),
            (Type::INT4, DfValue::Int(v)) => Ok(ps::Value::Int(v as _)),
            (Type::INT8, DfValue::Int(v)) => Ok(ps::Value::BigInt(v as _)),

            (Type::INT2, DfValue::UnsignedInt(v)) => Ok(ps::Value::SmallInt(v as _)),
            (Type::INT4, DfValue::UnsignedInt(v)) => Ok(ps::Value::Int(v as _)),
            (Type::INT8, DfValue::UnsignedInt(v)) => Ok(ps::Value::BigInt(v as _)),

            (ref ty, DfValue::UnsignedInt(v)) if type_is_oid(ty) => {
                Ok(ps::Value::Oid(v.try_into()?))
            }
            (ref ty, DfValue::Int(v)) if type_is_oid(ty) => Ok(ps::Value::Oid(v.try_into()?)),

            (Type::FLOAT4 | Type::FLOAT8, DfValue::Float(f)) => Ok(ps::Value::Float(f)),
            (Type::FLOAT8, DfValue::Double(f)) => Ok(ps::Value::Double(f)),
            (Type::NUMERIC, DfValue::Double(f)) => Ok(ps::Value::Numeric(
                <Decimal>::try_from(f).map_err(|e| ps::Error::InternalError(e.to_string()))?,
            )),
            (Type::NUMERIC, DfValue::Numeric(ref d)) => Ok(ps::Value::Numeric(*d.as_ref())),
            (Type::TEXT, DfValue::Text(v)) => Ok(ps::Value::Text(v)),
            (Type::TEXT, DfValue::TinyText(t)) => Ok(ps::Value::Text(t.as_str().into())),
            (Type::TIMESTAMP, DfValue::TimestampTz(v)) => {
                Ok(ps::Value::Timestamp(v.to_chrono().naive_local()))
            }
            (Type::TIMESTAMPTZ, DfValue::TimestampTz(v)) => {
                Ok(ps::Value::TimestampTz(v.to_chrono()))
            }
            (Type::DATE, DfValue::TimestampTz(v)) => {
                Ok(ps::Value::Date(v.to_chrono().naive_local().date()))
            }
            (Type::TIME, DfValue::Time(t)) => Ok(ps::Value::Time((t).into())),
            (Type::BOOL, DfValue::UnsignedInt(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BOOL, DfValue::Int(v)) => Ok(ps::Value::Bool(v != 0)),
            (Type::BYTEA, DfValue::ByteArray(b)) => Ok(ps::Value::ByteArray(
                std::sync::Arc::try_unwrap(b).unwrap_or_else(|v| v.as_ref().to_vec()),
            )),
            (Type::MACADDR, DfValue::Text(m)) => Ok(ps::Value::MacAddress(
                MacAddress::parse_str(m.as_str())
                    .map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::INET, dt @ (DfValue::Text(_) | DfValue::TinyText(_))) => Ok(ps::Value::Inet(
                <&str>::try_from(&dt)
                    .unwrap()
                    .parse::<IpAddr>()
                    .map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::UUID, DfValue::Text(u)) => Ok(ps::Value::Uuid(
                Uuid::parse_str(u.as_str()).map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::JSON, ref d @ (DfValue::Text(_) | DfValue::TinyText(_))) => Ok(ps::Value::Json(
                <&str>::try_from(d)
                    .map_err(|e| ps::Error::InternalError(e.to_string()))
                    .and_then(|s| {
                        serde_json::from_str::<serde_json::Value>(s)
                            .map_err(|e| ps::Error::ParseError(e.to_string()))
                    })?,
            )),
            (Type::JSONB, ref d @ (DfValue::Text(_) | DfValue::TinyText(_))) => {
                Ok(ps::Value::Jsonb(
                    <&str>::try_from(d)
                        .map_err(|e| ps::Error::InternalError(e.to_string()))
                        .and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .map_err(|e| ps::Error::ParseError(e.to_string()))
                        })?,
                ))
            }
            (Type::BIT, DfValue::BitVector(ref b)) => Ok(ps::Value::Bit(b.as_ref().clone())),
            (Type::VARBIT, DfValue::BitVector(ref b)) => Ok(ps::Value::VarBit(b.as_ref().clone())),
            (t, DfValue::Array(ref arr)) => {
                if let Kind::Array(member) = t.kind() {
                    let mut arr = (**arr).clone();
                    if let Kind::Enum(vs) = member.kind() {
                        for val in arr.values_mut() {
                            *val = convert_enum_value(vs, val.clone())?.clone().into();
                        }
                    }
                    Ok(ps::Value::Array(arr, t))
                } else {
                    Err(ps::Error::InternalError(format!(
                        "Mismatched type for value: expected array type, but got {t}"
                    )))
                }
            }
            (_, DfValue::PassThrough(ref p)) => Ok(ps::Value::PassThrough((**p).clone())),
            (t, val) => {
                if let Kind::Enum(vs) = t.kind() {
                    Ok(ps::Value::Text(
                        convert_enum_value(vs, val)?.as_str().into(),
                    ))
                } else {
                    trace!(?t, ?val);
                    error!(
                        psql_type = %t,
                        data_type = ?val.sql_type(),
                        "Tried to serialize value to postgres with unsupported type"
                    );
                    Err(ps::Error::UnsupportedType(t))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use readyset_data::TinyText;

    use super::*;

    #[test]
    fn i8_char() {
        let val = Value {
            col_type: Type::CHAR,
            value: DfValue::Int(8i8 as _),
        };
        assert_eq!(ps::Value::try_from(val).unwrap(), ps::Value::Char(8));
    }

    #[test]
    fn tiny_text_varchar() {
        let val = Value {
            col_type: Type::VARCHAR,
            value: DfValue::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::VarChar("aaaaaaaaaaaaaa".into())
        );
    }

    #[test]
    fn tiny_text_text() {
        let val = Value {
            col_type: Type::TEXT,
            value: DfValue::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Text("aaaaaaaaaaaaaa".into())
        );
    }
}
