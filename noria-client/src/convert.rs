use nom_sql::{Double, Float, Literal};
use noria::{DataType, ReadySetError, ReadySetResult};

use arccstr::ArcCStr;
use rust_decimal::Decimal;
use std::convert::TryFrom;
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
            Literal::Float(Float { value, precision }) => DataType::Float(value, precision),
            Literal::Double(Double { value, precision }) => DataType::Double(value, precision),
            Literal::Numeric(i, s) => Decimal::try_from_i128_with_scale(i, s)
                .map_err(|e| ReadySetError::DataTypeConversionError {
                    val: format!("Mantissa: {} | Scale: {}", i, s),
                    src_type: "Literal".to_string(),
                    target_type: "DataType".to_string(),
                    details: format!("Values out-of-bounds for Numeric type. Error: {}", e),
                })
                .map(DataType::from)?,
            Literal::CurrentDate => {
                DataType::Timestamp(chrono::Local::today().and_hms(0, 0, 0).naive_local())
            }
            Literal::CurrentTime | Literal::CurrentTimestamp => {
                DataType::Timestamp(chrono::Local::now().naive_local())
            }
            Literal::ByteArray(b) => DataType::ByteArray(Arc::new(b)),
            Literal::Placeholder(_) => unreachable!(),
        })
    }
}
