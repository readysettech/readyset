use msql_srv::{Value, ValueInner};
use nom_sql::{Literal, Real};
use noria::{DataType, ReadySetError, ReadySetResult};

use arccstr::ArcCStr;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;

pub(crate) trait ToDataType {
    fn into_datatype(self) -> Result<DataType, ReadySetError>;
}

const TINYTEXT_WIDTH: usize = 15;

impl ToDataType for Literal {
    fn into_datatype(self) -> ReadySetResult<DataType> {
        Ok(match self {
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
            Literal::FixedPoint(Real { value, precision }) => DataType::Real(value, precision),
            Literal::CurrentDate => {
                DataType::Timestamp(chrono::Local::today().and_hms(0, 0, 0).naive_local())
            }
            Literal::CurrentTime | Literal::CurrentTimestamp => {
                DataType::Timestamp(chrono::Local::now().naive_local())
            }
            Literal::Placeholder(_) => unreachable!(),
        })
    }
}

impl<'a> ToDataType for Value<'a> {
    fn into_datatype(self) -> Result<DataType, ReadySetError> {
        Ok(match self.into_inner() {
            ValueInner::Null => DataType::None,
            ValueInner::Bytes(b) => DataType::try_from(b).unwrap(),
            ValueInner::Int(i) => i.into(),
            ValueInner::UInt(i) => (i as i32).into(),
            ValueInner::Double(f) => DataType::try_from(f)?,
            ValueInner::Datetime(_) => DataType::Timestamp(self.try_into().map_err(|e| {
                ReadySetError::DataTypeConversionError {
                    val: format!("{:?}", self.into_inner()),
                    src_type: "ValueInner::Datetime".to_string(),
                    target_type: "DataType::Timestamp".to_string(),
                    details: format!("{:?}", e),
                }
            })?),
            ValueInner::Time(_) => DataType::Time(Arc::new(self.try_into().map_err(|e| {
                ReadySetError::DataTypeConversionError {
                    val: format!("{:?}", self.into_inner()),
                    src_type: "ValueInner::Time".to_string(),
                    target_type: "Datatype::Time".to_string(),
                    details: format!("{:?}", e),
                }
            })?)),
            _ => unimplemented!(),
        })
    }
}
