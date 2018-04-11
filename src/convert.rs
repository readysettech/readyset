use chrono;
use distributary::DataType;
use msql_srv::{Value, ValueInner};
use nom_sql::{Literal, Real};

use arccstr::ArcCStr;
use std::convert::TryFrom;

pub(crate) trait ToDataType {
    fn to_datatype(self) -> DataType;
}

impl ToDataType for Literal {
    fn to_datatype(self) -> DataType {
        match self {
            Literal::Null => DataType::None,
            Literal::String(b) => DataType::Text(ArcCStr::try_from(b).unwrap()),
            Literal::Blob(b) => DataType::Text(ArcCStr::try_from(&b[..]).unwrap()),
            Literal::Integer(i) => i.into(),
            Literal::FixedPoint(Real {
                integral,
                fractional,
            }) => DataType::Real(integral, fractional as i32),
            Literal::CurrentDate => {
                DataType::Timestamp(chrono::Local::today().and_hms(0, 0, 0).naive_local())
            }
            Literal::CurrentTime | Literal::CurrentTimestamp => {
                DataType::Timestamp(chrono::Local::now().naive_local())
            }
            Literal::Placeholder => unreachable!(),
        }
    }
}

impl<'a> ToDataType for Value<'a> {
    fn to_datatype(self) -> DataType {
        match self.into_inner() {
            ValueInner::NULL => DataType::None,
            ValueInner::Bytes(b) => DataType::Text(ArcCStr::try_from(b).unwrap()),
            ValueInner::Int(i) => i.into(),
            ValueInner::UInt(i) => (i as i32).into(),
            ValueInner::Double(f) => f.into(),
            ValueInner::Datetime(_) => DataType::Timestamp(self.into()),
            _ => unimplemented!(),
        }
    }
}
