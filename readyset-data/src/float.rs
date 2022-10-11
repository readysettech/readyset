use std::convert::TryInto;

use readyset_errors::{ReadySetError, ReadySetResult};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::{r#enum, DfType, DfValue};

fn coerce_f64_to_int<I>(val: f64) -> Option<I>
where
    i64: TryInto<I>,
{
    if -0.5 < val && val < 0.5 {
        return 0.try_into().ok();
    }

    if val <= i64::MIN as f64 - 0.5 || val >= i64::MAX as f64 + 0.5 {
        return None;
    }

    (val.round() as i64).try_into().ok()
}

fn coerce_f64_to_uint<I>(val: f64) -> Option<I>
where
    u64: TryInto<I>,
{
    if -0.5 < val && val < 0.5 {
        return 0.try_into().ok();
    }

    if val <= -0.5 || val >= u64::MAX as f64 + 0.5 {
        return None;
    }

    (val.round() as u64).try_into().ok()
}

pub(crate) fn coerce_f64(val: f64, to_ty: &DfType, from_ty: &DfType) -> ReadySetResult<DfValue> {
    let err = |deets: &str| ReadySetError::DfValueConversionError {
        src_type: "Double".to_string(),
        target_type: to_ty.to_string(),
        details: deets.to_string(),
    };

    let bounds_err = || err("out of bounds");

    if val.is_infinite() {
        return Err(err("±inf not allowed"));
    }

    if val.is_nan() {
        return Err(err("Nan not allowed"));
    }

    match *to_ty {
        DfType::Bool => Ok(DfValue::from(val != 0.0)),

        DfType::Double => Ok(DfValue::Double(val)),

        DfType::Float(_) => {
            let val = val as f32;
            if val.is_infinite() {
                return Err(bounds_err());
            }
            Ok(DfValue::Float(val))
        }

        DfType::Numeric { .. } => Decimal::from_f64_retain(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),

        DfType::TinyInt => coerce_f64_to_int::<i8>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::UnsignedTinyInt => coerce_f64_to_uint::<u8>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::SmallInt => coerce_f64_to_int::<i16>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::UnsignedSmallInt => coerce_f64_to_uint::<u16>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::Int => coerce_f64_to_int::<i32>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::UnsignedInt => coerce_f64_to_uint::<u32>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::BigInt => coerce_f64_to_int::<i64>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        DfType::UnsignedBigInt => coerce_f64_to_uint::<u64>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),

        // The numeric cast from f64 to usize will round down, which is what we want for enums:
        DfType::Enum(ref elements, _) => {
            Ok(r#enum::apply_enum_limits(val as usize, elements).into())
        }

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

        DfType::Blob(_) => Ok(DfValue::ByteArray(val.to_string().into_bytes().into())),

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

        DfType::Json(_) | DfType::Jsonb => Ok(val.to_string().into()),

        DfType::Time { .. } => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in hhmmss format, provided that it makes sense as a time. For
            // example, 101112 is understood as '10:11:12'. The following
            // alternative formats are also understood: ss, mmss, or hhmmss.
            let ms = (val.fract() * 1000000.0).round() as u64;
            let val = val.trunc() as u64;
            let hh = val / 1_00_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(mysql_time::MySqlTime::from_hmsus(true, hh as _, mm as _, ss as _, ms).into())
        }

        DfType::Date
        | DfType::DateTime { .. }
        | DfType::Timestamp { .. }
        | DfType::TimestampTz { .. } => crate::integer::coerce_integer(
            coerce_f64_to_int::<i64>(val).ok_or_else(bounds_err)?,
            to_ty,
            from_ty,
        ),

        DfType::Unknown
        | DfType::MacAddr
        | DfType::Inet
        | DfType::Uuid
        | DfType::Bit(_)
        | DfType::VarBit(_)
        | DfType::Array(_) => Err(err("not allowed")),
        DfType::PassThrough(ref ty) => Err(ReadySetError::PassThroughConversionError {
            src_type: "Double".to_string(),
            target_contained: ty.to_string(),
        }),
    }
}

pub(crate) fn coerce_decimal(
    val: &Decimal,
    to_ty: &DfType,
    from_ty: &DfType,
) -> ReadySetResult<DfValue> {
    let err = || ReadySetError::DfValueConversionError {
        src_type: "Decimal".to_string(),
        target_type: to_ty.to_string(),
        details: "out of bounds".to_string(),
    };

    match *to_ty {
        DfType::Numeric { .. } => Ok(DfValue::from(*val)),

        DfType::Bool => Ok(DfValue::from(!val.is_zero())),

        DfType::TinyInt => val.to_i8().ok_or_else(err).map(DfValue::from),
        DfType::UnsignedTinyInt => val.to_u8().ok_or_else(err).map(DfValue::from),
        DfType::SmallInt => val.to_i16().ok_or_else(err).map(DfValue::from),
        DfType::UnsignedSmallInt => val.to_u16().ok_or_else(err).map(DfValue::from),
        DfType::Int => val.to_i32().ok_or_else(err).map(DfValue::from),
        DfType::UnsignedInt => val.to_u32().ok_or_else(err).map(DfValue::from),
        DfType::BigInt => val.to_i64().ok_or_else(err).map(DfValue::from),
        DfType::UnsignedBigInt => val.to_u64().ok_or_else(err).map(DfValue::from),

        DfType::Float(_) => val
            .to_f32()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DfValue::Float),
        DfType::Double => val
            .to_f64()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DfValue::Double),

        DfType::Enum(ref elements, _) => {
            Ok(r#enum::apply_enum_limits(usize::try_from(*val).unwrap_or(0), elements).into())
        }

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

        DfType::Blob(_) => Ok(DfValue::ByteArray(val.to_string().into_bytes().into())),

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

        DfType::Json(_) | DfType::Jsonb => Ok(val.to_string().into()),

        DfType::Date
        | DfType::DateTime { .. }
        | DfType::Time { .. }
        | DfType::Timestamp { .. }
        | DfType::TimestampTz { .. } => {
            crate::integer::coerce_integer(val.to_i64().ok_or_else(err)?, to_ty, from_ty)
        }

        DfType::Unknown
        | DfType::MacAddr
        | DfType::Inet
        | DfType::Uuid
        | DfType::Bit(_)
        | DfType::VarBit(_)
        | DfType::Array(_) => Err(ReadySetError::DfValueConversionError {
            src_type: "Decimal".to_string(),
            target_type: to_ty.to_string(),
            details: "Not allowed".to_string(),
        }),
        DfType::PassThrough(_) => Err(ReadySetError::PassThroughConversionError {
            src_type: "Decimal".to_string(),
            target_contained: to_ty.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;
    use crate::{Collation, Dialect};

    #[proptest]
    fn float_to_tinyint(val: f32) {
        if val < i8::MIN as f32 - 0.5 || val >= i8::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::TinyInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::TinyInt, &DfType::Unknown),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        // Yes, this is indeed the valid range for MySQL float to u8 conversion
        if val < -0.5f32 || val >= u8::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_smallint(val: f32) {
        if val < i16::MIN as f32 - 0.5 || val >= i16::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::SmallInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::SmallInt, &DfType::Unknown),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f32 || val >= u16::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::UnsignedSmallInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::UnsignedSmallInt, &DfType::Unknown),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_int(val: f64) {
        if val < i32::MIN as f64 - 0.5 || val >= i32::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::Int, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::Int, &DfType::Unknown),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f64 || val >= u32::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::UnsignedInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::UnsignedInt, &DfType::Unknown),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_bigint(val: f64) {
        if val < i64::MIN as f64 - 0.5 || val >= i64::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::BigInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::BigInt, &DfType::Unknown),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f64 || val >= u64::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown)
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[test]
    fn float_to_text() {
        assert_eq!(
            DfValue::Double(312.0)
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
                .unwrap(),
            DfValue::from("312")
        );

        assert_eq!(
            DfValue::Double(312.222222)
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
                .unwrap(),
            DfValue::from("312.222222")
        );

        assert_eq!(
            DfValue::Double(1e4)
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
                .unwrap(),
            DfValue::from("10000")
        );

        assert_eq!(
            DfValue::Double(1e4)
                .coerce_to(&DfType::VarChar(2, Collation::default()), &DfType::Unknown)
                .unwrap(),
            DfValue::from("10")
        );

        assert_eq!(
            DfValue::Double(1e4)
                .coerce_to(
                    &DfType::Char(8, Collation::default(), Dialect::DEFAULT_MYSQL),
                    &DfType::Unknown
                )
                .unwrap(),
            DfValue::from("10000   ")
        );

        assert_eq!(
            DfValue::Double(-1.0 / 3.0)
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
                .unwrap(),
            DfValue::from("-0.3333333333333333")
        );
    }

    #[test]
    fn float_to_json() {
        assert_eq!(
            DfValue::Double(-5000.0)
                .coerce_to(&DfType::Json(Dialect::DEFAULT_MYSQL), &DfType::Unknown)
                .unwrap(),
            DfValue::from("-5000")
        );
    }

    #[test]
    fn float_to_int_manual() {
        assert_eq!(
            DfValue::Float(-0.6874218).coerce_to(&DfType::TinyInt, &DfType::Unknown),
            Ok(DfValue::Int(-1))
        );

        assert_eq!(
            DfValue::Float(0.6874218).coerce_to(&DfType::TinyInt, &DfType::Unknown),
            Ok(DfValue::Int(1))
        );

        DfValue::Float(-128.9039)
            .coerce_to(&DfType::TinyInt, &DfType::Unknown)
            .unwrap_err();

        DfValue::Float(127.9039)
            .coerce_to(&DfType::TinyInt, &DfType::Unknown)
            .unwrap_err();

        assert_eq!(
            DfValue::Float(-128.4039).coerce_to(&DfType::TinyInt, &DfType::Unknown),
            Ok(DfValue::Int(-128))
        );

        assert_eq!(
            DfValue::Float(127.4039).coerce_to(&DfType::TinyInt, &DfType::Unknown),
            Ok(DfValue::Int(127))
        );

        assert_eq!(
            DfValue::Float(0.5).coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(1))
        );

        assert_eq!(
            DfValue::Float(0.687_421_9).coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(1))
        );

        assert_eq!(
            DfValue::Float(255.49).coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(255))
        );

        DfValue::Float(255.5)
            .coerce_to(&DfType::UnsignedTinyInt, &DfType::Unknown)
            .unwrap_err();

        assert_eq!(
            DfValue::Float(65535.49).coerce_to(&DfType::UnsignedSmallInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(65535))
        );

        DfValue::Float(65535.5)
            .coerce_to(&DfType::UnsignedSmallInt, &DfType::Unknown)
            .unwrap_err();

        assert_eq!(
            DfValue::Double(4294967295.49).coerce_to(&DfType::UnsignedInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(4294967295))
        );

        DfValue::Double(4294967295.5)
            .coerce_to(&DfType::UnsignedInt, &DfType::Unknown)
            .unwrap_err();

        // Since u64 max is not accurately representable as a doulbe, there are no exact conversions
        DfValue::Double(18446744073709551613.49)
            .coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown)
            .unwrap_err();

        assert_eq!(
            DfValue::Double(17946744073709551610.49)
                .coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown),
            Ok(DfValue::UnsignedInt(17946744073709551616))
        );
    }
}
