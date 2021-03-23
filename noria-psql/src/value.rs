use arccstr::ArcCStr;
use noria::DataType;
use psql_srv as ps;
use std::convert::TryFrom;

pub struct Value {
    pub col_type: ps::ColType,
    pub value: DataType,
}

impl TryFrom<Value> for ps::Value {
    type Error = ps::Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let from_tiny_text = |v| {
            // TODO Avoid this allocation by adding a TinyText storage option in psql-srv.
            Ok(ps::Value::Char(
                ArcCStr::try_from(<&str>::from(v)).map_err(|_| {
                    ps::Error::InternalError("unexpected nul within TinyText".to_string())
                })?,
            ))
        };
        match (v.col_type, v.value) {
            (ps::ColType::Char(_), DataType::Text(v)) => Ok(ps::Value::Char(v)),
            (ps::ColType::Char(_), ref v @ DataType::TinyText(_)) => from_tiny_text(v),
            (ps::ColType::Varchar(_), DataType::Text(v)) => Ok(ps::Value::Varchar(v)),
            (ps::ColType::Varchar(_), ref v @ DataType::TinyText(_)) => from_tiny_text(v),
            (ps::ColType::Int(_), DataType::Int(v)) => Ok(ps::Value::Int(v)),
            (ps::ColType::Bigint(_), DataType::BigInt(v)) => Ok(ps::Value::Bigint(v)),
            (ps::ColType::Double, v @ DataType::Real(_, _)) => Ok(ps::Value::Double(v.into())),
            (ps::ColType::Text, DataType::Text(v)) => Ok(ps::Value::Text(v)),
            (ps::ColType::Text, ref v @ DataType::TinyText(_)) => from_tiny_text(v),
            (ps::ColType::Timestamp, DataType::Timestamp(v)) => Ok(ps::Value::Timestamp(v)),
            (t, _) => Err(ps::Error::UnsupportedType(t)),
        }
    }
}
