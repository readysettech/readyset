use arccstr::ArcCStr;
use chrono::NaiveDateTime;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int(i32),
    BigInt(i64),
    Double(f64),
    Text(ArcCStr),
    Timestamp(NaiveDateTime),
    Varchar(ArcCStr),
}
