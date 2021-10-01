use arccstr::ArcCStr;
use bit_vec::BitVec;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use eui48::MacAddress;
use rust_decimal::Decimal;
use uuid::Uuid;

/// A PostgreSQL data value that can be received from, or sent to, a PostgreSQL frontend.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Char(ArcCStr),
    Varchar(ArcCStr),
    Int(i32),
    Bigint(i64),
    Smallint(i16),
    Double(f64),
    Float(f32),
    Numeric(Decimal),
    Text(ArcCStr),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    ByteArray(Vec<u8>),
    MacAddress(MacAddress),
    Uuid(Uuid),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
    Bit(BitVec),
    VarBit(BitVec),
}
