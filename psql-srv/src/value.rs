use bit_vec::BitVec;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use eui48::MacAddress;
use noria::Text;
use rust_decimal::Decimal;
use uuid::Uuid;

/// A PostgreSQL data value that can be received from, or sent to, a PostgreSQL frontend.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Char(Text),
    Varchar(Text),
    Int(i32),
    Bigint(i64),
    Smallint(i16),
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
    Uuid(Uuid),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
    Bit(BitVec),
    VarBit(BitVec),
}
