use distributary::DataType;
use msql_srv::{Value, ValueInner};

use arccstr::ArcCStr;
use std::convert::TryFrom;

pub(crate) trait ToDataType {
    fn to_datatype(self) -> DataType;
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
