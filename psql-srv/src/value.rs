use std::error::Error;

use bit_vec::BitVec;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use cidr::IpInet;
use eui48::MacAddress;
use postgres_types::{FromSql, Kind, Type};
use readyset_data::{Array, Text};
use rust_decimal::Decimal;
use uuid::Uuid;

/// A PostgreSQL data value that can be received from, or sent to, a PostgreSQL frontend.
#[derive(Clone, Debug, PartialEq)]
pub enum PsqlValue {
    Null,
    Bool(bool),
    VarChar(Text),
    Name(Text),
    BpChar(Text),
    Char(i8),
    Int(i32),
    BigInt(i64),
    SmallInt(i16),
    Oid(u32),
    Double(f64),
    Float(f32),
    Numeric(Decimal),
    Text(Text),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<FixedOffset>),
    Date(NaiveDate),
    Time(NaiveTime),
    ByteArray(Vec<u8>),
    MacAddress(MacAddress),
    Inet(IpInet),
    Uuid(Uuid),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
    Bit(BitVec),
    VarBit(BitVec),
    Array(Array, Type),
    PassThrough(readyset_data::PassThrough),
}

impl<'a> FromSql<'a> for PsqlValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match ty.kind() {
            Kind::Array(_) => Array::from_sql(ty, raw).map(|a| PsqlValue::Array(a, ty.clone())),
            Kind::Enum(_) => Text::try_from(raw)
                .map(PsqlValue::Text)
                .map_err(|e| e.into()),
            _ => match *ty {
                // Note we have arms here for every PsqlValue variant except OID. We have an OID
                // variant of PsqlValue defined, but we don't support that type, so we'll handle it
                // in the catchall passthrough arm at the end of this match block.
                Type::BOOL => bool::from_sql(ty, raw).map(PsqlValue::Bool),
                Type::VARCHAR => <&str>::from_sql(ty, raw).map(|s| PsqlValue::VarChar(s.into())),
                Type::NAME => <&str>::from_sql(ty, raw).map(|s| PsqlValue::Name(s.into())),
                Type::BPCHAR => <&str>::from_sql(ty, raw).map(|s| PsqlValue::BpChar(s.into())),
                Type::CHAR => i8::from_sql(ty, raw).map(PsqlValue::Char),
                Type::INT4 => i32::from_sql(ty, raw).map(PsqlValue::Int),
                Type::INT8 => i64::from_sql(ty, raw).map(PsqlValue::BigInt),
                Type::INT2 => i16::from_sql(ty, raw).map(PsqlValue::SmallInt),
                Type::OID
                | Type::REGCLASS
                | Type::REGCOLLATION
                | Type::REGCONFIG
                | Type::REGDICTIONARY
                | Type::REGNAMESPACE
                | Type::REGOPER
                | Type::REGOPERATOR
                | Type::REGPROC
                | Type::REGPROCEDURE
                | Type::REGROLE
                | Type::REGTYPE => u32::from_sql(ty, raw).map(PsqlValue::Oid),
                Type::FLOAT8 => f64::from_sql(ty, raw).map(PsqlValue::Double),
                Type::FLOAT4 => f32::from_sql(ty, raw).map(PsqlValue::Float),
                Type::NUMERIC => {
                    // rust-decimal has a bug whereby it will successfully deserialize from the
                    // Postgres binary format NUMERIC values with scales in [0, 255], but it will
                    // panic when serializing them to bincode if they are outside [0, 28].
                    let d = Decimal::from_sql(ty, raw)?;
                    if d.scale() > 28 {
                        Err(format!(
                            "Unsupported scale {} on NUMERIC value - \
                             max supported by ReadySet is 28",
                            d.scale(),
                        )
                        .into())
                    } else {
                        Ok(PsqlValue::Numeric(d))
                    }
                }
                Type::TEXT => <&str>::from_sql(ty, raw).map(|s| PsqlValue::Text(s.into())),
                Type::TIMESTAMP => NaiveDateTime::from_sql(ty, raw).map(PsqlValue::Timestamp),
                Type::TIMESTAMPTZ => <chrono::DateTime<chrono::FixedOffset>>::from_sql(ty, raw)
                    .map(PsqlValue::TimestampTz),
                Type::DATE => NaiveDate::from_sql(ty, raw).map(PsqlValue::Date),
                Type::TIME => NaiveTime::from_sql(ty, raw).map(PsqlValue::Time),
                Type::BYTEA => <Vec<u8>>::from_sql(ty, raw).map(PsqlValue::ByteArray),
                Type::MACADDR => MacAddress::from_sql(ty, raw).map(PsqlValue::MacAddress),
                Type::INET => IpInet::from_sql(ty, raw).map(PsqlValue::Inet),
                Type::UUID => Uuid::from_sql(ty, raw).map(PsqlValue::Uuid),
                Type::JSON => serde_json::Value::from_sql(ty, raw).map(PsqlValue::Json),
                Type::JSONB => serde_json::Value::from_sql(ty, raw).map(PsqlValue::Jsonb),
                Type::BIT => BitVec::from_sql(ty, raw).map(PsqlValue::Bit),
                Type::VARBIT => BitVec::from_sql(ty, raw).map(PsqlValue::VarBit),
                _ => Ok(PsqlValue::PassThrough(readyset_data::PassThrough {
                    ty: ty.clone(),
                    data: Box::from(raw),
                })),
            },
        }
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(PsqlValue::Null)
    }

    fn accepts(_: &Type) -> bool {
        true // Anything we don't support, we can still wrap in PsqlValue::PassThrough
    }
}
