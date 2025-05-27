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
                // Much of this is due to `diesel`'s implementation.
                let (mut bigint, scale) = big_decimal.as_bigint_and_exponent();

                let scale_u16 = u16::try_from(scale);

                // Special case to match Postgres to the byte: for zero values, Postgres doesn't
                // send a weight, but does send a scale. The normal path will normalize the weight
                // and end up with no scale.
                if big_decimal.is_zero() && scale_u16.is_ok() {
                    let dscale = scale_u16?;
                    out.reserve(8);
                    out.put_u16(0); // ndigits
                    out.put_i16(0); // weight
                    out.put_u16(0);
                    out.put_u16(dscale);
                    return Ok(IsNull::No);
                }

                // `BigDecimal` supports negative sign (trailing zeroes), but Postgres does not:
                // normalize to 0 scale in that case.
                let dscale: u16 = if scale < 0 {
                    for _ in 0..(-scale) {
                        bigint *= 10;
                    }
                    0
                } else {
                    scale_u16?
                };

                // Ensure decimal point will be between one of the base-10000 digits.
                for _ in 0..(4 - dscale % 4) {
                    bigint *= 10;
                }

                let (sign, mut biguint) = bigint.into_parts();

                let mut digits = Vec::new();
                let base = 10_000u16.into();
                while !biguint.is_zero() {
                    let (div, rem) = biguint.div_rem(&base);
                    digits.push(rem.to_i16().expect("remainder should always be < 2**16"));
                    biguint = div;
                }

                let digits_after_decimal_point = dscale / 4 + 1;
                let weight =
                    i16::try_from(digits.len())? - i16::try_from(digits_after_decimal_point)? - 1;

                let trailing_zeroes = digits.iter().take_while(|i| i.is_zero()).count();
                digits.reverse();
                digits.truncate(digits.len() - trailing_zeroes);

                let ndigits = u16::try_from(digits.len())?;
                let sign_flags = match sign {
                    Sign::Minus => NEGATIVE_SIGN,
                    _ => 0,
                };

                // Actually write out the value
                out.reserve(8 + 2 * digits.len());
                out.put_u16(ndigits);
                out.put_i16(weight);
                out.put_u16(sign_flags);
                out.put_u16(dscale);
                for digit in digits {
                    out.put_i16(digit);
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
