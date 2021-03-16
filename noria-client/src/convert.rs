use chrono;
use msql_srv::{Value, ValueInner};
use nom_sql::{Literal, Real};
use noria::DataType;

use arccstr::ArcCStr;
use std::convert::TryFrom;

pub(crate) trait ToDataType {
    fn to_datatype(self) -> DataType;
}

const TINYTEXT_WIDTH: usize = 15;

impl ToDataType for Literal {
    fn to_datatype(self) -> DataType {
        match self {
            Literal::Null => DataType::None,
            Literal::String(b) => b.into(),
            Literal::Blob(b) => {
                let len = b.len();
                if len <= TINYTEXT_WIDTH {
                    let mut bytes = [0; TINYTEXT_WIDTH];
                    if len != 0 {
                        let bts = &mut bytes[0..len];
                        bts.copy_from_slice(&b);
                    }
                    DataType::TinyText(bytes)
                } else {
                    DataType::Text(ArcCStr::try_from(&b[..]).unwrap())
                }
            }
            Literal::Integer(i) => i.into(),
            Literal::UnsignedInteger(i) => i.into(),
            Literal::FixedPoint(Real {
                integral,
                fractional,
            }) => DataType::Real(integral as i64, fractional as i32),
            Literal::CurrentDate => {
                DataType::Timestamp(chrono::Local::today().and_hms(0, 0, 0).naive_local())
            }
            Literal::CurrentTime | Literal::CurrentTimestamp => {
                DataType::Timestamp(chrono::Local::now().naive_local())
            }
            Literal::Placeholder(_) => unreachable!(),
        }
    }
}

impl<'a> ToDataType for Value<'a> {
    fn to_datatype(self) -> DataType {
        match self.into_inner() {
            ValueInner::NULL => DataType::None,
            ValueInner::Bytes(b) => DataType::try_from(b).unwrap(),
            ValueInner::Int(i) => i.into(),
            ValueInner::UInt(i) => (i as i32).into(),
            ValueInner::Double(f) => f.into(),
            ValueInner::Datetime(_) => DataType::Timestamp(self.into()),
            ValueInner::Time(_) => DataType::Time(self.into()),
            _ => unimplemented!(),
        }
    }
}
