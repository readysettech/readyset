use std::convert::{TryFrom, TryInto};

use cidr::IpInet;
use eui48::MacAddress;
use postgres_types::Kind;
use ps::util::type_is_oid;
use ps::PsqlValue;
use psql_srv as ps;
use readyset_data::DfValue;
use rust_decimal::Decimal;
use tokio_postgres::types::Type;
use tracing::{error, trace};
use uuid::Uuid;

/// An encapsulation of a ReadySet `DfValue` value that facilitates conversion of this `DfValue`
/// into a `psql_srv::PsqlValue`.
pub struct TypedDfValue {
    /// A type attribute used to determine which variant of `psql_srv::PsqlValue` the `value`
    /// attribute should be converted to.
    pub col_type: Type,

    /// The data value itself.
    pub value: DfValue,
}

impl TryFrom<TypedDfValue> for PsqlValue {
    type Error = ps::Error;

    fn try_from(v: TypedDfValue) -> Result<Self, Self::Error> {
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
            (_, DfValue::None) => Ok(PsqlValue::Null),
            (Type::CHAR, DfValue::Int(v)) => Ok(PsqlValue::Char(v.try_into()?)),
            (Type::VARCHAR, DfValue::Text(v)) => Ok(PsqlValue::VarChar(v)),
            (Type::VARCHAR, DfValue::TinyText(t)) => Ok(PsqlValue::VarChar(t.as_str().into())),
            (Type::NAME, DfValue::Text(t)) => Ok(PsqlValue::Name(t)),
            (Type::NAME, DfValue::TinyText(t)) => Ok(PsqlValue::Name(t.as_str().into())),
            (Type::BPCHAR, DfValue::Text(v)) => Ok(PsqlValue::BpChar(v)),
            (Type::BPCHAR, DfValue::TinyText(t)) => Ok(PsqlValue::BpChar(t.as_str().into())),

            (Type::INT2, DfValue::Int(v)) => Ok(PsqlValue::SmallInt(v as _)),
            (Type::INT4, DfValue::Int(v)) => Ok(PsqlValue::Int(v as _)),
            (Type::INT8, DfValue::Int(v)) => Ok(PsqlValue::BigInt(v as _)),

            (Type::INT2, DfValue::UnsignedInt(v)) => Ok(PsqlValue::SmallInt(v as _)),
            (Type::INT4, DfValue::UnsignedInt(v)) => Ok(PsqlValue::Int(v as _)),
            (Type::INT8, DfValue::UnsignedInt(v)) => Ok(PsqlValue::BigInt(v as _)),

            (ref ty, DfValue::UnsignedInt(v)) if type_is_oid(ty) => {
                Ok(PsqlValue::Oid(v.try_into()?))
            }
            (ref ty, DfValue::Int(v)) if type_is_oid(ty) => Ok(PsqlValue::Oid(v.try_into()?)),

            (Type::FLOAT4 | Type::FLOAT8, DfValue::Float(f)) => Ok(PsqlValue::Float(f)),
            (Type::FLOAT8, DfValue::Double(f)) => Ok(PsqlValue::Double(f)),
            (Type::NUMERIC, DfValue::Double(f)) => Ok(PsqlValue::Numeric(
                <Decimal>::try_from(f).map_err(|e| ps::Error::InternalError(e.to_string()))?,
            )),
            (Type::NUMERIC, DfValue::Numeric(ref d)) => Ok(PsqlValue::Numeric(*d.as_ref())),
            (Type::TEXT, DfValue::Text(v)) => Ok(PsqlValue::Text(v)),
            (Type::TEXT, DfValue::TinyText(t)) => Ok(PsqlValue::Text(t.as_str().into())),
            (ref ty, DfValue::Text(v)) if ty.name() == "citext" => Ok(PsqlValue::Text(v)),
            (ref ty, DfValue::TinyText(t)) if ty.name() == "citext" => {
                Ok(PsqlValue::Text(t.as_str().into()))
            }
            (Type::TIMESTAMP, DfValue::TimestampTz(v)) => {
                Ok(PsqlValue::Timestamp(v.to_chrono().naive_local()))
            }
            (Type::TIMESTAMPTZ, DfValue::TimestampTz(v)) => {
                Ok(PsqlValue::TimestampTz(v.to_chrono()))
            }
            (Type::DATE, DfValue::TimestampTz(v)) => {
                Ok(PsqlValue::Date(v.to_chrono().naive_local().date()))
            }
            (Type::TIME, DfValue::Time(t)) => Ok(PsqlValue::Time((t).into())),
            (Type::BOOL, DfValue::UnsignedInt(v)) => Ok(PsqlValue::Bool(v != 0)),
            (Type::BOOL, DfValue::Int(v)) => Ok(PsqlValue::Bool(v != 0)),
            (Type::BYTEA, DfValue::ByteArray(b)) => Ok(PsqlValue::ByteArray(
                std::sync::Arc::try_unwrap(b).unwrap_or_else(|v| v.as_ref().to_vec()),
            )),
            (Type::MACADDR, DfValue::Text(m)) => Ok(PsqlValue::MacAddress(
                MacAddress::parse_str(m.as_str())
                    .map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::INET, dt @ (DfValue::Text(_) | DfValue::TinyText(_))) => Ok(PsqlValue::Inet(
                <&str>::try_from(&dt)
                    .unwrap()
                    .parse::<IpInet>()
                    .map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::UUID, DfValue::Text(u)) => Ok(PsqlValue::Uuid(
                Uuid::parse_str(u.as_str()).map_err(|e| ps::Error::ParseError(e.to_string()))?,
            )),
            (Type::JSON, ref d @ (DfValue::Text(_) | DfValue::TinyText(_))) => Ok(PsqlValue::Json(
                <&str>::try_from(d)
                    .map_err(|e| ps::Error::InternalError(e.to_string()))
                    .and_then(|s| {
                        serde_json::from_str::<serde_json::Value>(s)
                            .map_err(|e| ps::Error::ParseError(e.to_string()))
                    })?,
            )),
            (Type::JSONB, ref d @ (DfValue::Text(_) | DfValue::TinyText(_))) => {
                Ok(PsqlValue::Jsonb(
                    <&str>::try_from(d)
                        .map_err(|e| ps::Error::InternalError(e.to_string()))
                        .and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .map_err(|e| ps::Error::ParseError(e.to_string()))
                        })?,
                ))
            }
            (Type::BIT, DfValue::BitVector(ref b)) => Ok(PsqlValue::Bit(b.as_ref().clone())),
            (Type::VARBIT, DfValue::BitVector(ref b)) => Ok(PsqlValue::VarBit(b.as_ref().clone())),
            (t, DfValue::Array(ref arr)) => {
                if let Kind::Array(member) = t.kind() {
                    let mut arr = (**arr).clone();
                    if let Kind::Enum(vs) = member.kind() {
                        for val in arr.values_mut() {
                            *val = convert_enum_value(vs, val.clone())?.clone().into();
                        }
                    }
                    Ok(PsqlValue::Array(arr, t))
                } else {
                    Err(ps::Error::InternalError(format!(
                        "Mismatched type for value: expected array type, but got {t}"
                    )))
                }
            }
            (_, DfValue::PassThrough(ref p)) => Ok(PsqlValue::PassThrough((**p).clone())),
            (t, val) => {
                if let Kind::Enum(vs) = t.kind() {
                    Ok(PsqlValue::Text(
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
        let val = TypedDfValue {
            col_type: Type::CHAR,
            value: DfValue::Int(8i8 as _),
        };
        assert_eq!(PsqlValue::try_from(val).unwrap(), PsqlValue::Char(8));
    }

    #[test]
    fn tiny_text_varchar() {
        let val = TypedDfValue {
            col_type: Type::VARCHAR,
            value: DfValue::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            PsqlValue::try_from(val).unwrap(),
            PsqlValue::VarChar("aaaaaaaaaaaaaa".into())
        );
    }

    #[test]
    fn tiny_text_text() {
        let val = TypedDfValue {
            col_type: Type::TEXT,
            value: DfValue::TinyText(TinyText::from_arr(b"aaaaaaaaaaaaaa")),
        };
        assert_eq!(
            PsqlValue::try_from(val).unwrap(),
            PsqlValue::Text("aaaaaaaaaaaaaa".into())
        );
    }
}
