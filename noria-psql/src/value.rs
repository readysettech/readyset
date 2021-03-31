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
            ArcCStr::try_from(<&str>::from(v))
                .map_err(|_| ps::Error::InternalError("unexpected nul within TinyText".to_string()))
        };
        match (v.col_type, v.value) {
            (ps::ColType::Char(_), DataType::Text(v)) => Ok(ps::Value::Char(v)),
            (ps::ColType::Char(_), ref v @ DataType::TinyText(_)) => {
                Ok(ps::Value::Char(from_tiny_text(v)?))
            }
            (ps::ColType::Varchar(_), DataType::Text(v)) => Ok(ps::Value::Varchar(v)),
            (ps::ColType::Varchar(_), ref v @ DataType::TinyText(_)) => {
                Ok(ps::Value::Varchar(from_tiny_text(v)?))
            }
            (ps::ColType::Int(_), DataType::Int(v)) => Ok(ps::Value::Int(v)),
            (ps::ColType::Bigint(_), DataType::BigInt(v)) => Ok(ps::Value::Bigint(v)),
            (ps::ColType::Double, v @ DataType::Real(_, _)) => Ok(ps::Value::Double(v.into())),
            (ps::ColType::Text, DataType::Text(v)) => Ok(ps::Value::Text(v)),
            (ps::ColType::Text, ref v @ DataType::TinyText(_)) => {
                Ok(ps::Value::Text(from_tiny_text(v)?))
            }
            (ps::ColType::Timestamp, DataType::Timestamp(v)) => Ok(ps::Value::Timestamp(v)),
            (t, _) => Err(ps::Error::UnsupportedType(t)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn tiny_text_char() {
        let val = Value {
            col_type: ps::ColType::Char(15),
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
            col_type: ps::ColType::Varchar(15),
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
            col_type: ps::ColType::Text,
            value: DataType::TinyText([b'a'; 15]),
        };
        assert_eq!(
            ps::Value::try_from(val).unwrap(),
            ps::Value::Text(ArcCStr::try_from("aaaaaaaaaaaaaaa").unwrap())
        );
    }
}
