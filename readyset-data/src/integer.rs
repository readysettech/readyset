use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use bit_vec::BitVec;

use readyset_errors::{ReadySetError, ReadySetResult};
use rust_decimal::Decimal;

use crate::{r#enum, DfType, DfValue};

/// A convenience trait that implements casts of i64 and u64 to f32 and f64
pub(crate) trait IntAsFloat {
    fn to_f64(self) -> f64;
    fn to_f32(self) -> f32;
}

impl IntAsFloat for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

impl IntAsFloat for u64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

impl IntAsFloat for u32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

/// Coerce an integer value according to MySQL rules to the best of our abilities.
///
/// Note that this only handles converting *integer types*, not other types with values represented
/// as integers (for example, enums). The conversion logic for other such types is implemented
/// elsewhere.
pub(crate) fn coerce_integer<I>(val: I, to_ty: &DfType, from_ty: &DfType) -> ReadySetResult<DfValue>
where
    i8: TryFrom<I>,
    i16: TryFrom<I>,
    i32: TryFrom<I>,
    i64: TryFrom<I>,
    u8: TryFrom<I>,
    u16: TryFrom<I>,
    u32: TryFrom<I>,
    u64: TryFrom<I>,
    Decimal: From<I>,
    usize: TryFrom<I>,
    I: std::ops::BitXor<I, Output = I> + std::cmp::Eq + Copy + fmt::Display + IntAsFloat,
{
    let err = || ReadySetError::DfValueConversionError {
        src_type: from_ty.to_string(),
        target_type: to_ty.to_string(),
        details: "out of bounds".to_string(),
    };

    match *to_ty {
        #[allow(clippy::eq_op)]
        DfType::Bool => Ok(DfValue::from(val ^ val != val)),

        DfType::TinyInt => i8::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedTinyInt => u8::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::SmallInt => i16::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedSmallInt => u16::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::Int => i32::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedInt => u32::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::BigInt => i64::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedBigInt => u64::try_from(val).map_err(|_| err()).map(DfValue::from),

        DfType::Float => Ok(DfValue::Float(val.to_f32())),
        DfType::Double => Ok(DfValue::Double(val.to_f64())),

        DfType::Text(collation) => Ok(DfValue::from_str_and_collation(&val.to_string(), collation)),

        DfType::VarChar(l, ..) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            Ok(val.to_string().into())
        }

        DfType::Char(l, ..) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            val.extend(std::iter::repeat(' ').take((l as usize).saturating_sub(val.len())));
            Ok(val.into())
        }

        DfType::Blob => Ok(val.to_string().into_bytes().into()),

        DfType::VarBinary(l) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            Ok(val.to_string().into_bytes().into())
        }

        DfType::Binary(l) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            val.extend(std::iter::repeat(' ').take((l as usize).saturating_sub(val.len())));
            Ok(val.into_bytes().into())
        }

        DfType::Json | DfType::Jsonb => Ok(format!("\"{}\"", val).into()),

        DfType::Date => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in either YYYYMMDD or YYMMDD format, provided that the number makes sense
            // as a date. For example, 19830905 and 830905 are interpreted as '1983-09-05'.
            let val = u64::try_from(val).map_err(|_| err())?;
            let year = match val / 1_00_00 {
                y @ 00..=69 => y + 2000, // Year values in the range 00-69 become 2000-2069.
                y @ 70..=99 => y + 1900, // Year values in the range 70-99 become 1970-1999.
                year => year,
            };

            let month = val / 1_00 % 1_00;
            let day = val % 1_00;
            Ok(
                chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _)
                    .ok_or_else(err)?
                    .into(),
            )
        }

        DfType::Timestamp { .. } | DfType::TimestampTz { .. } | DfType::DateTime { .. } => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in either YYYYMMDDhhmmss or YYMMDDhhmmss format, provided that the number
            // makes sense as a date. For example, 19830905132800 and 830905132800 are interpreted
            // as '1983-09-05 13:28:00'.
            let val = u64::try_from(val).map_err(|_| err())?;
            let year = match val / 1_00_00_00_00_00 {
                y @ 00..=69 => y + 2000, // Year values in the range 00-69 become 2000-2069.
                y @ 70..=99 => y + 1900, // Year values in the range 70-99 become 1970-1999.
                year => year,
            };
            let month = val / 1_00_00_00_00 % 1_00;
            let day = val / 1_00_00_00 % 1_00;
            let hh = val / 1_00_00 % 1_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(
                chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _)
                    .and_then(|d| d.and_hms_micro_opt(hh as _, mm as _, ss as _, 0))
                    .ok_or_else(err)?
                    .into(),
            )
        }

        DfType::Time { .. } => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in hhmmss format, provided that it makes sense as a time. For example,
            // 101112 is understood as '10:11:12'. The following alternative formats are also
            // understood: ss, mmss, or hhmmss.
            let val = u64::try_from(val).map_err(|_| err())?;
            let hh = val / 1_00_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(mysql_time::MySqlTime::from_hmsus(true, hh as _, mm as _, ss as _, 0).into())
        }

        DfType::Numeric { .. } => Ok(DfValue::Numeric(Arc::new(val.into()))),

        DfType::Enum { ref variants, .. } => {
            // Values above the number of variants are converted to 0 by MySQL, and anything that
            // can't be held in a usize is certainly too high, hence the .unwrap_or(0)
            let idx = usize::try_from(val).unwrap_or(0);
            Ok(DfValue::from(r#enum::apply_enum_limits(idx, variants)))
        }

        DfType::Bit(size) => {
            // Bit values, when treated as numbers, are always unsigned.
            let mut bit_vec = u64::try_from(val).map_err(|_| err())
            .map(|u| BitVec::from_bytes(&u.to_be_bytes()))?;
            // bit_vec.shrink_to_fit();
            let size = size as usize;
            if bit_vec.len() > size {
                // Take the `size` most significant bits (stored in new_bit_vec)
                let new_bit_vec = bit_vec.split_off(bit_vec.len() - size);
                // Check that the remaining bits are all zero. Otherwise we would be truncating, which
                // would mean we were out of bounds to begin with
                if bit_vec.any() {
                    // If there was any non-zero leftover, return an out of bounds error
                    return Err(err());
                }
                bit_vec = new_bit_vec;
            }
            Ok(DfValue::from(bit_vec))
        }

        DfType::Unknown
        | DfType::MacAddr
        | DfType::Inet
        | DfType::Uuid
        | DfType::VarBit(_)
        | DfType::Array(_) => Err(ReadySetError::DfValueConversionError {
            src_type: from_ty.to_string(),
            target_type: to_ty.to_string(),
            details: "Not allowed".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;
    use crate::DfType;
    use crate::integer::coerce_integer;

    /// Test coercion from integer to bit types.
    mod bit {
        use bit_vec::BitVec;
        use proptest::strategy::Strategy;
        use readyset_util::arbitrary::arbitrary_bitvec;
        use super::*;

        fn bit_strategy() -> impl Strategy<Value = (BitVec, u16)> {
            (1..=64usize).prop_flat_map(|size| arbitrary_bitvec(1..=size).prop_map(move |mut vec| {
                // Make it so we can transform this into bytes by prepending zeroes (instead
                // of appending them, as that's the behavior of `BitVec::to_bytes`.
                let mut val = BitVec::with_capacity(64);
                val.grow(64 - vec.len(), false);
                val.append(&mut vec);
                (val, size as u16)
            }))
        }

        #[proptest]
        fn uint_to_bit(#[strategy(bit_strategy())] params: (BitVec, u16)) {
            let (bitvec, size) = params;
            let mut buf = [0u8; 8];
            let bytes = bitvec.to_bytes();
            buf[8-bytes.len()..].copy_from_slice(bytes.as_slice());
            let val = u64::from_be_bytes(buf);
            coerce_integer(val, &DfType::Bit(size), &DfType::Unknown).unwrap();
        }
    }
}
