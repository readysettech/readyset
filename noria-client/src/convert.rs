use nom_sql::{Literal, Real};
use noria::{DataType, ReadySetError, ReadySetResult};

use arccstr::ArcCStr;
use std::convert::TryFrom;

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
