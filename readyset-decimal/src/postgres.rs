use std::io::Cursor;

use bigdecimal::{
    BigDecimal, ToPrimitive as _, Zero as _,
    num_bigint::{BigInt, BigUint, Sign},
};
use byteorder::{BE, ReadBytesExt};
use bytes::{BufMut as _, BytesMut};
use num_integer::Integer;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

use crate::Decimal;

const NEGATIVE_SIGN: u16 = 0x4000;
const NAN: u16 = 0xC000;
const POSITIVE_INFINITY: u16 = 0xD000;
const NEGATIVE_INFINITY: u16 = 0xF000;

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        debug_assert!(ty == &Type::NUMERIC);

        let mut c = Cursor::new(raw);

        let ndigits = c.read_u16::<BE>()? as i64;
        let weight = c.read_i16::<BE>()? as i64;

        let sign_flags = c.read_u16::<BE>()?;

        // Check for special values
        if sign_flags == POSITIVE_INFINITY {
            return Ok(Self::Infinity);
        } else if sign_flags == NEGATIVE_INFINITY {
            return Ok(Self::NegativeInfinity);
        } else if sign_flags == NAN {
            return Ok(Self::NaN);
        }

        let sign = if sign_flags == NEGATIVE_SIGN {
            Sign::Minus
        } else {
            Sign::Plus
        };

        let dscale = c.read_u16::<BE>()?;

        // XXX(mvzink): This becomes slow for large numbers. Postgres sends a u16 for each base-10k
        // digit. We are converting it to a BigUint, which means it becomes a Vec<u64> scaled to
        // match that base. For example, the multiplier for the 50th digit is stored as `[0, 0, 0,
        // 9405896725437276416, 9922620113683420819, 7515739520245860318, 16079138824858084876,
        // 11075552318717488147, 6341854167220437486, 9040454683355271665, 21918093]`. This must
        // then be multiplied by the digit itself, and then added to the accumulator. This would
        // also mean a lot of allocations, but luckily we start with the bigger digits, so each
        // intermediary allocation gets smaller, and subsequent allocations shouldn't require
        // reallocating the accumulator's backing vec.
        //
        // Furthermore, we can only hit this with real data when using unconstrained numerics.
        // Otherwise, the max precision is 1000. See [PG docs].
        //
        // So this mostly affects testing, as debug builds are especially slow here, and we can
        // cause pretty long test times by generating large numbers.
        //
        // [PG docs]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
        if ndigits == 0 {
            // Postgres doesn't normalize the weight (which would remove the scale for 0), so we can
            // preserve the scale it gives us.
            return Ok(Self::Number(BigDecimal::zero().with_scale(dscale as _)));
        }
        let mut accumulator = BigUint::zero();
        let mut base = BigUint::from(10_000u32).pow((ndigits - 1) as u32);
        for _ in 0..ndigits {
            let digit = c.read_u16::<BE>()?;
            accumulator += digit * &base;
            base /= 10_000u32;
        }

        // Some magical correction based on the weight, due to `rust-pg_bigdecimal` and `diesel`.
        let correction_exponent = 4 * (weight - ndigits + 1);
        let bigint = BigInt::from_biguint(sign, accumulator);
        let bigdecimal = BigDecimal::new(bigint, -correction_exponent).with_scale(dscale as _);

        Ok(Self::Number(bigdecimal))
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::NUMERIC
    }
}

impl ToSql for Decimal {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        debug_assert!(ty == &Type::NUMERIC);

        match self {
            Decimal::NaN => {
                write_special(out, NAN)?;
                Ok(IsNull::No)
            }
            Decimal::NegativeInfinity => {
                write_special(out, NEGATIVE_INFINITY)?;
                Ok(IsNull::No)
            }
            Decimal::Infinity => {
                write_special(out, POSITIVE_INFINITY)?;
                Ok(IsNull::No)
            }
            Decimal::Number(big_decimal) => {
                let components = bigdecimal_to_pg_numeric(big_decimal)
                    .ok_or("numeric value out of range for Postgres wire format")?;

                let ndigits = u16::try_from(components.digits.len())?;
                let sign_flags = match components.sign {
                    Sign::Minus => NEGATIVE_SIGN,
                    _ => 0,
                };

                out.reserve(8 + 2 * components.digits.len());
                out.put_u16(ndigits);
                out.put_i16(components.weight);
                out.put_u16(sign_flags);
                out.put_u16(components.dscale);
                for digit in &components.digits {
                    out.put_i16(*digit);
                }
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        ty == &Type::NUMERIC
    }

    to_sql_checked!();
}

fn write_special(
    out: &mut BytesMut,
    sign_flags: u16,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    out.reserve(8);
    out.put_u16(0); // ndigits
    out.put_i16(0); // weight
    out.put_u16(sign_flags);
    out.put_u16(0); // dscale
    Ok(())
}

struct PgNumericComponents {
    digits: Vec<i16>,
    weight: i16,
    dscale: u16,
    sign: Sign,
}

fn bigdecimal_to_pg_numeric(big_decimal: &BigDecimal) -> Option<PgNumericComponents> {
    let (mut bigint, scale) = big_decimal.as_bigint_and_exponent();

    if big_decimal.is_zero() {
        let dscale = u16::try_from(scale).unwrap_or(0);
        return Some(PgNumericComponents {
            digits: vec![],
            weight: 0,
            dscale,
            sign: Sign::Plus,
        });
    }

    let dscale: u16 = if scale < 0 {
        for _ in 0..(-scale) {
            bigint *= 10;
        }
        0
    } else {
        u16::try_from(scale).ok()?
    };

    for _ in 0..(4 - dscale % 4) {
        bigint *= 10;
    }

    let (sign, mut biguint) = bigint.into_parts();

    let mut digits = Vec::new();
    let base: BigUint = 10_000u16.into();
    while !biguint.is_zero() {
        let (div, rem) = biguint.div_rem(&base);
        digits.push(rem.to_i16()?);
        biguint = div;
    }

    let digits_after_decimal_point = dscale / 4 + 1;
    let weight =
        i16::try_from(digits.len()).ok()? - i16::try_from(digits_after_decimal_point).ok()? - 1;

    let trailing_zeroes = digits.iter().take_while(|i| **i == 0).count();
    digits.reverse();
    digits.truncate(digits.len() - trailing_zeroes);

    Some(PgNumericComponents {
        digits,
        weight,
        dscale,
        sign,
    })
}

/// Postgres-style base-10000 numeric summary (magnitude only; sign is not included).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgWeightInfo {
    pub weight: i16,
    pub first_digit: u16,
    pub dscale: u16,
}

impl Decimal {
    /// Compute the Postgres-style base-10000 weight and first non-zero digit.
    ///
    /// Returns weight, first_digit, and dscale matching PostgreSQL's `NumericVar`
    /// representation. Sign is ignored; negative values produce the same result
    /// as their absolute value. For special values (NaN, Infinity), all fields
    /// are zero. For zero, weight and first_digit are zero but dscale is preserved.
    // TODO: This computes the full base-10000 digit vector just to read the first
    // element. A zero-allocation fast path would be more efficient for callers
    // that only need weight/first_digit.
    pub fn pg_weight_and_first_digit(&self) -> PgWeightInfo {
        match self {
            Decimal::Number(big_decimal) => {
                let Some(components) = bigdecimal_to_pg_numeric(big_decimal) else {
                    return PgWeightInfo {
                        weight: 0,
                        first_digit: 0,
                        dscale: 0,
                    };
                };
                if components.digits.is_empty() {
                    return PgWeightInfo {
                        weight: 0,
                        first_digit: 0,
                        dscale: components.dscale,
                    };
                }
                let first = components.digits[0];
                debug_assert!(
                    first >= 0,
                    "base-10000 digit from BigUint cannot be negative"
                );
                PgWeightInfo {
                    weight: components.weight,
                    first_digit: first as u16,
                    dscale: components.dscale,
                }
            }
            _ => PgWeightInfo {
                weight: 0,
                first_digit: 0,
                dscale: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bytes::BytesMut;
    use postgres_types::{FromSql, ToSql, Type};

    use super::PgWeightInfo;
    use crate::Decimal;

    fn info(weight: i16, first_digit: u16, dscale: u16) -> PgWeightInfo {
        PgWeightInfo {
            weight,
            first_digit,
            dscale,
        }
    }

    #[test]
    fn pg_weight_integers() {
        assert_eq!(
            Decimal::from_str("1").unwrap().pg_weight_and_first_digit(),
            info(0, 1, 0)
        );
        assert_eq!(
            Decimal::from_str("42").unwrap().pg_weight_and_first_digit(),
            info(0, 42, 0)
        );
        assert_eq!(
            Decimal::from_str("40000")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(1, 4, 0)
        );
        assert_eq!(
            Decimal::from_str("100000")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(1, 10, 0)
        );
    }

    #[test]
    fn pg_weight_fractional_lt_one() {
        assert_eq!(
            Decimal::from_str("0.01")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 100, 2)
        );
        assert_eq!(
            Decimal::from_str("0.123")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 1230, 3)
        );
        assert_eq!(
            Decimal::from_str("0.0001")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 1, 4)
        );
        assert_eq!(
            Decimal::from_str("0.00001")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-2, 1000, 5)
        );
        assert_eq!(
            Decimal::from_str("0.000000001")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-3, 1000, 9)
        );
        assert_eq!(
            Decimal::from_str("0.9999")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 9999, 4)
        );
    }

    #[test]
    fn pg_weight_mixed() {
        assert_eq!(
            Decimal::from_str("33437.314618")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(1, 3, 6)
        );
    }

    #[test]
    fn pg_weight_dscale_multiple_of_4() {
        assert_eq!(
            Decimal::from_str("1.2345")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(0, 1, 4)
        );
        assert_eq!(
            Decimal::from_str("1.23456789")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(0, 1, 8)
        );
        assert_eq!(
            Decimal::from_str("0.12345678")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 1234, 8)
        );
    }

    #[test]
    fn pg_weight_zero() {
        assert_eq!(
            Decimal::from_str("0").unwrap().pg_weight_and_first_digit(),
            info(0, 0, 0)
        );
        assert_eq!(
            Decimal::from_str("0.000")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(0, 0, 3)
        );
    }

    #[test]
    fn pg_weight_special() {
        assert_eq!(Decimal::NaN.pg_weight_and_first_digit(), info(0, 0, 0));
        assert_eq!(Decimal::Infinity.pg_weight_and_first_digit(), info(0, 0, 0));
        assert_eq!(
            Decimal::NegativeInfinity.pg_weight_and_first_digit(),
            info(0, 0, 0)
        );
    }

    #[test]
    fn pg_weight_negative() {
        assert_eq!(
            Decimal::from_str("-42")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(0, 42, 0)
        );
        assert_eq!(
            Decimal::from_str("-0.01")
                .unwrap()
                .pg_weight_and_first_digit(),
            info(-1, 100, 2)
        );
    }

    #[test]
    fn pg_weight_large_value_exceeding_i128() {
        let large = Decimal::from_str("1234567890123456789012345678901234567890").unwrap();
        assert!(large.mantissa_and_scale().is_none());
        assert_eq!(large.pg_weight_and_first_digit(), info(9, 1234, 0));
    }

    #[test]
    fn to_sql_round_trip() {
        for val in ["0", "1", "42", "0.01", "33437.314618", "-42", "0.000"] {
            let dec = Decimal::from_str(val).unwrap();
            let mut buf = BytesMut::new();
            dec.to_sql(&Type::NUMERIC, &mut buf).unwrap();
            let result = Decimal::from_sql(&Type::NUMERIC, &buf).unwrap();
            assert_eq!(
                dec.to_string(),
                result.to_string(),
                "round-trip failed for {val}"
            );
        }
    }
}
