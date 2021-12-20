use nom_sql::{Double, Float, Literal};
use noria_data::DataType;
use noria_errors::{ReadySetError, ReadySetResult};

use bit_vec::BitVec;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::sync::Arc;

pub(crate) trait ToDataType {
    fn into_datatype(self) -> Result<DataType, ReadySetError>;
}

impl ToDataType for Literal {
    fn into_datatype(self) -> ReadySetResult<DataType> {
        Ok(match self {
            Literal::Null => DataType::None,
            Literal::String(b) => b.into(),
            Literal::Blob(b) => DataType::ByteArray(Arc::new(b)),
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
            Literal::CurrentTimestamp | Literal::CurrentTime => {
                let ts = time::OffsetDateTime::now_utc();
                let ndt = NaiveDate::from_ymd(ts.year(), ts.month() as u32, ts.day() as u32)
                    .and_hms_nano(
                        ts.hour() as u32,
                        ts.minute() as u32,
                        ts.second() as u32,
                        ts.nanosecond(),
                    );
                DataType::Timestamp(ndt)
            }
            Literal::CurrentDate => {
                let ts = time::OffsetDateTime::now_utc();
                let nd = NaiveDate::from_ymd(ts.year(), ts.month() as u32, ts.day() as u32)
                    .and_hms(0, 0, 0);
                DataType::Timestamp(nd)
            }
            Literal::ByteArray(b) => DataType::ByteArray(Arc::new(b)),
            Literal::BitVector(b) => DataType::from(BitVec::from_bytes(b.as_slice())),
            Literal::Placeholder(_) => unreachable!(),
        })
    }
}
