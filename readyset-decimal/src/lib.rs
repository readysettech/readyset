use std::{
    fmt::Display,
    ops::{Add, Div, Mul, Sub},
    str::FromStr,
};

use bigdecimal::{
    BigDecimal, FromPrimitive as _, ParseBigDecimalError, ToPrimitive as _, Zero as _,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Re-export `bigdecimal`'s rounding modes
pub use bigdecimal::RoundingMode;
use strum::{EnumDiscriminants, FromRepr, IntoStaticStr, VariantNames};

mod mysql;
mod postgres;

#[derive(Debug, thiserror::Error)]
pub enum ReadysetDecimalError {
    #[error("Invalid decimal format: {0}")]
    ParseError(#[from] ParseBigDecimalError),
    #[error("Cannot convert {from_ty} to {to_ty}: {value}")]
    ConversionError {
        from_ty: &'static str,
        to_ty: &'static str,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, EnumDiscriminants)]
#[strum_discriminants(vis(pub(crate)))]
#[strum_discriminants(derive(FromRepr, VariantNames, IntoStaticStr, Deserialize))]
pub enum Decimal {
    NegativeInfinity,
    Number(BigDecimal),
    Infinity,
    NaN,
}

macro_rules! impl_to_from_primitive {
    ($ty:ty) => {
        paste::paste! {
            impl From<$ty> for Decimal {
                fn from(value: $ty) -> Self {
                    // SAFETY: The conversion from primitive integer types to BigDecimal has no
                    // failure path.
                    BigDecimal::[<from_ $ty>](value).map(Self::Number).unwrap()
                }
            }

            impl TryFrom<&Decimal> for $ty {
                type Error = ReadysetDecimalError;

                fn try_from(value: &Decimal) -> Result<Self, Self::Error> {
                    let err = || ReadysetDecimalError::ConversionError {
                        from_ty: "Decimal",
                        to_ty: stringify!($ty),
                        value: format!("{value}"),
                    };
                    match value {
                        Decimal::Number(value) => value.[<to_ $ty>]().ok_or_else(err),
                        _ => Err(err()),
                    }
                }
            }

            impl TryFrom<Decimal> for $ty {
                type Error = ReadysetDecimalError;

                fn try_from(value: Decimal) -> Result<Self, Self::Error> {
                    (&value).try_into()
                }
            }
        }
    };
}

impl_to_from_primitive!(u8);
impl_to_from_primitive!(i8);
impl_to_from_primitive!(u16);
impl_to_from_primitive!(i16);
impl_to_from_primitive!(u32);
impl_to_from_primitive!(i32);
impl_to_from_primitive!(u64);
impl_to_from_primitive!(i64);
impl_to_from_primitive!(i128);
impl_to_from_primitive!(usize);

macro_rules! impl_to_from_float {
    ($ty:ty) => {
        paste::paste! {
            impl TryFrom<$ty> for Decimal {
                type Error = ReadysetDecimalError;

                fn try_from(value: $ty) -> Result<Self, Self::Error> {
                    if value.is_infinite() {
                        if value.is_sign_positive() {
                            Ok(Self::Infinity)
                        } else {
                            Ok(Self::NegativeInfinity)
                        }
                    } else if value.is_nan() {
                        Ok(Self::NaN)
                    } else {
                        BigDecimal::[<from_ $ty>](value).map(Self::Number).ok_or_else(|| ReadysetDecimalError::ConversionError {
                            from_ty: stringify!($ty),
                            to_ty: "Decimal",
                            value: format!("{value}"),
                        })
                    }
                }
            }

            impl TryFrom<&Decimal> for $ty {
                type Error = ReadysetDecimalError;

                fn try_from(value: &Decimal) -> Result<Self, Self::Error> {
                    match value {
                        Decimal::Number(value) => value.[<to_ $ty>]().ok_or_else(|| ReadysetDecimalError::ConversionError {
                            from_ty: "Decimal",
                            to_ty: stringify!($ty),
                            value: format!("{value}"),
                        }),
                        Decimal::Infinity => Ok($ty::INFINITY),
                        Decimal::NegativeInfinity => Ok($ty::NEG_INFINITY),
                        Decimal::NaN => Ok($ty::NAN),
                    }
                }
            }

            impl TryFrom<Decimal> for $ty {
                type Error = ReadysetDecimalError;

                fn try_from(value: Decimal) -> Result<Self, Self::Error> {
                    (&value).try_into()
                }
            }
        }
    };
}

impl_to_from_float!(f32);
impl_to_from_float!(f64);

impl Decimal {
    pub const MIN: Self = Self::NegativeInfinity;
    pub const MAX: Self = Self::Infinity;

    pub fn new(mantissa: i128, scale: i64) -> Self {
        Self::Number(BigDecimal::new(mantissa.into(), scale))
    }

    pub fn zero() -> Self {
        Self::Number(BigDecimal::zero())
    }

    pub fn is_zero(&self) -> bool {
        match self {
            Decimal::Number(value) => value.is_zero(),
            _ => false,
        }
    }

    pub fn mantissa_and_scale(&self) -> Option<(i128, i64)> {
        match self {
            Decimal::Number(value) => {
                let (mantissa, scale) = value.as_bigint_and_scale();
                mantissa
                    .as_ref()
                    .to_i128()
                    .map(|mantissa| (mantissa, scale))
            }
            _ => None,
        }
    }

    pub fn scale(&self) -> Option<i64> {
        self.mantissa_and_scale().map(|(_, scale)| scale)
    }

    pub fn normalize(&self) -> Self {
        match self {
            Self::Number(value) => Self::Number(value.normalized()),
            _ => self.clone(),
        }
    }

    /// Add two decimals. Though this is "checked", it can't fail.
    ///
    /// We follow Postgres handling of special values, because MySQL does not support NaN or
    /// +/-Infinity. This and the following arithmetic operations were generated empirically via:
    ///
    /// ```sql
    /// create table foo (x numeric);
    /// insert into foo values (42), ('Infinity'), ('-Infinity'), ('NaN');
    /// select a.x, '+', b.x, '=', a.x + b.x from foo a join foo b on a.x = b.x or a.x <> b.x;
    /// select a.x, '-', b.x, '=', a.x - b.x from foo a join foo b on a.x = b.x or a.x <> b.x;
    /// select a.x, '/', b.x, '=', a.x / b.x from foo a join foo b on a.x = b.x or a.x <> b.x;
    /// select a.x, '*', b.x, '=', a.x * b.x from foo a join foo b on a.x = b.x or a.x <> b.x;
    /// ```
    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        use Decimal::*;
        match (self, other) {
            (Infinity, Infinity) => Some(Infinity),
            (Infinity, NegativeInfinity) => Some(NaN),
            (Infinity, NaN) => Some(NaN),
            (Infinity, Number(..)) => Some(Infinity),
            (NegativeInfinity, Infinity) => Some(NaN),
            (NegativeInfinity, NegativeInfinity) => Some(NegativeInfinity),
            (NegativeInfinity, NaN) => Some(NaN),
            (NegativeInfinity, Number(..)) => Some(NegativeInfinity),
            (NaN, Infinity) => Some(NaN),
            (NaN, NegativeInfinity) => Some(NaN),
            (NaN, NaN) => Some(NaN),
            (NaN, Number(..)) => Some(NaN),
            (Number(..), Infinity) => Some(Infinity),
            (Number(..), NegativeInfinity) => Some(NegativeInfinity),
            (Number(..), NaN) => Some(NaN),
            (Number(a), Number(b)) => Some(Number(a + b)),
        }
    }

    /// Subtract two decimals. See comment on [`Decimal::checked_add`].
    pub fn checked_sub(&self, other: &Self) -> Option<Self> {
        use Decimal::*;
        match (self, other) {
            (Infinity, Infinity) => Some(NaN),
            (Infinity, NegativeInfinity) => Some(Infinity),
            (Infinity, NaN) => Some(NaN),
            (Infinity, Number(..)) => Some(Infinity),
            (NegativeInfinity, Infinity) => Some(NegativeInfinity),
            (NegativeInfinity, NegativeInfinity) => Some(NaN),
            (NegativeInfinity, NaN) => Some(NaN),
            (NegativeInfinity, Number(..)) => Some(NegativeInfinity),
            (NaN, Infinity) => Some(NaN),
            (NaN, NegativeInfinity) => Some(NaN),
            (NaN, NaN) => Some(NaN),
            (NaN, Number(..)) => Some(NaN),
            (Number(..), Infinity) => Some(NegativeInfinity),
            (Number(..), NegativeInfinity) => Some(Infinity),
            (Number(..), NaN) => Some(NaN),
            (Number(a), Number(b)) => Some(Number(a - b)),
        }
    }

    /// Divide two decimals. See comment on [`Decimal::checked_add`].
    ///
    /// Unlike the other "checked" arithmetic implementations, this one can fail (return [`None`])
    /// if the divisor (`other`) is zero.
    pub fn checked_div(&self, other: &Self) -> Option<Self> {
        use Decimal::*;
        match (self, other) {
            (Infinity, Infinity) => Some(NaN),
            (Infinity, NegativeInfinity) => Some(NaN),
            (Infinity, NaN) => Some(NaN),
            (Infinity, Number(..)) => Some(Infinity),
            (NegativeInfinity, Infinity) => Some(NaN),
            (NegativeInfinity, NegativeInfinity) => Some(NaN),
            (NegativeInfinity, NaN) => Some(NaN),
            (NegativeInfinity, Number(..)) => Some(NegativeInfinity),
            (NaN, Infinity) => Some(NaN),
            (NaN, NegativeInfinity) => Some(NaN),
            (NaN, NaN) => Some(NaN),
            (NaN, Number(..)) => Some(NaN),
            (Number(..), Infinity) => Some(Self::zero()),
            (Number(..), NegativeInfinity) => Some(Self::zero()),
            (Number(..), NaN) => Some(NaN),
            (_, Number(b)) if b.is_zero() => None,
            (Number(a), Number(b)) => Some(Number(a / b)),
        }
    }

    /// Compute the remainder of two decimals. See comment on [`Decimal::checked_add`].
    ///
    /// Like [`Decimal::checked_div`], this returns [`None`] if the divisor (`other`) is zero.
    pub fn checked_rem(&self, other: &Self) -> Option<Self> {
        use Decimal::*;
        match (self, other) {
            (Infinity, _) => Some(NaN),
            (NegativeInfinity, _) => Some(NaN),
            (NaN, _) => Some(NaN),
            (_, NaN) => Some(NaN),
            (_, Infinity) => Some(self.clone()),
            (_, NegativeInfinity) => Some(self.clone()),
            (_, Number(b)) if b.is_zero() => None,
            (Number(a), Number(b)) => Some(Number(a % b)),
        }
    }

    /// Multiply two decimals. See comment on [`Decimal::checked_add`].
    pub fn checked_mul(&self, other: &Self) -> Option<Self> {
        use Decimal::*;
        match (self, other) {
            (Infinity, Infinity) => Some(Infinity),
            (Infinity, NegativeInfinity) => Some(NegativeInfinity),
            (Infinity, NaN) => Some(NaN),
            (Infinity, Number(..)) => Some(Infinity),
            (NegativeInfinity, Infinity) => Some(NegativeInfinity),
            (NegativeInfinity, NegativeInfinity) => Some(Infinity),
            (NegativeInfinity, NaN) => Some(NaN),
            (NegativeInfinity, Number(..)) => Some(NegativeInfinity),
            (NaN, Infinity) => Some(NaN),
            (NaN, NegativeInfinity) => Some(NaN),
            (NaN, NaN) => Some(NaN),
            (NaN, Number(..)) => Some(NaN),
            (Number(..), Infinity) => Some(Infinity),
            (Number(..), NegativeInfinity) => Some(NegativeInfinity),
            (Number(..), NaN) => Some(NaN),
            (Number(a), Number(b)) => Some(Number(a * b)),
        }
    }

    /// Round to 0 decimal points.
    ///
    /// Currently follows `rust_decimal`'s default strategy (banker's rounding) to match our previous behavior.
    pub fn round(&self) -> Self {
        self.round_dp_with_strategy(0, RoundingMode::HalfEven)
    }

    /// Round to the specified number of decimal points.
    ///
    /// Currently follows `rust_decimal`'s default strategy (banker's rounding) to match our previous behavior.
    pub fn round_dp(&self, new_scale: i64) -> Self {
        self.round_dp_with_strategy(new_scale, RoundingMode::HalfEven)
    }

    pub fn round_dp_with_strategy(&self, new_scale: i64, mode: RoundingMode) -> Self {
        match self {
            Self::Number(decimal) => Self::Number(decimal.with_scale_round(new_scale, mode)),
            _ => self.clone(),
        }
    }

    pub fn fract(&self) -> Self {
        match self {
            Self::Number(decimal) => {
                Self::Number(decimal - decimal.with_scale_round(0, RoundingMode::Down))
            }
            _ => Self::zero(),
        }
    }
}

impl Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decimal::Number(value) => value.fmt(f),
            Decimal::NaN => f.write_str("NaN"),
            Decimal::Infinity => f.write_str("Infinity"),
            Decimal::NegativeInfinity => f.write_str("-Infinity"),
        }
    }
}

impl FromStr for Decimal {
    type Err = ReadysetDecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "NaN" {
            Ok(Self::NaN)
        } else if s == "Infinity" {
            Ok(Self::Infinity)
        } else if s == "-Infinity" {
            Ok(Self::NegativeInfinity)
        } else {
            Ok(BigDecimal::from_str(s).map(Self::Number)?)
        }
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::zero()
    }
}

impl Add<&Decimal> for &Decimal {
    type Output = Decimal;

    fn add(self, rhs: &Decimal) -> Self::Output {
        // SAFETY: No failure path
        self.checked_add(rhs).unwrap()
    }
}

impl Add<&Decimal> for Decimal {
    type Output = Decimal;

    fn add(self, rhs: &Decimal) -> Self::Output {
        (&self).add(rhs)
    }
}

impl Add<Decimal> for Decimal {
    type Output = Decimal;

    fn add(self, rhs: Decimal) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Sub<&Decimal> for &Decimal {
    type Output = Decimal;

    fn sub(self, rhs: &Decimal) -> Self::Output {
        // SAFETY: No failure path
        self.checked_sub(rhs).unwrap()
    }
}

impl Sub<&Decimal> for Decimal {
    type Output = Decimal;

    fn sub(self, rhs: &Decimal) -> Self::Output {
        (&self).sub(rhs)
    }
}

impl Sub<Decimal> for Decimal {
    type Output = Decimal;

    fn sub(self, rhs: Decimal) -> Self::Output {
        (&self).sub(&rhs)
    }
}

impl Div<&Decimal> for &Decimal {
    type Output = Decimal;

    fn div(self, rhs: &Decimal) -> Self::Output {
        // Not safe, but we would panic dividing by zero on an int too.
        self.checked_div(rhs).unwrap()
    }
}

impl Div<&Decimal> for Decimal {
    type Output = Decimal;

    fn div(self, rhs: &Decimal) -> Self::Output {
        (&self).div(rhs)
    }
}

impl Div<Decimal> for Decimal {
    type Output = Decimal;

    fn div(self, rhs: Decimal) -> Self::Output {
        (&self).div(&rhs)
    }
}

impl Mul<&Decimal> for &Decimal {
    type Output = Decimal;

    fn mul(self, rhs: &Decimal) -> Self::Output {
        // SAFETY: No failure path
        self.checked_mul(rhs).unwrap()
    }
}

impl Mul<&Decimal> for Decimal {
    type Output = Decimal;

    fn mul(self, rhs: &Decimal) -> Self::Output {
        (&self).mul(rhs)
    }
}

impl Mul<Decimal> for Decimal {
    type Output = Decimal;

    fn mul(self, rhs: Decimal) -> Self::Output {
        (&self).mul(&rhs)
    }
}

impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let discriminant = DecimalDiscriminants::from(self);
        match self {
            Decimal::Number(big_decimal) => serializer.serialize_newtype_variant(
                "Decimal",
                discriminant as _,
                discriminant.into(),
                // XXX(mvzink): We use a "plain" formatting method, which never does scientific
                // notation, mainly to preserve the scale on deserialization in situations where we
                // would otherwise lose it. This is basically just to match Postgres behavior, where
                // we include the scale even for zero values. So `0.000` (from `to_plain_string`)
                // retains the scale of `3` on deserialization but `0` (from `to_string`) loses it.
                // Per the docs on [`BigDecimal::to_plain_string`], especially large exponents may
                // cause OOMs or overflow panics. It is not clear to me that any of the other
                // formatting methods are any better in this regard, but this method is certainly
                // wasteful if there are a lot of trailing zeros.
                &big_decimal.to_plain_string(),
            ),
            _ => {
                serializer.serialize_unit_variant("Decimal", discriminant as _, discriminant.into())
            }
        }
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, EnumAccess, VariantAccess, Visitor};
        use std::fmt;

        struct DecimalVisitor;

        impl<'de> Visitor<'de> for DecimalVisitor {
            type Value = Decimal;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("enum Decimal")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                match data.variant()? {
                    (DecimalDiscriminants::NaN, variant) => {
                        variant.unit_variant()?;
                        Ok(Decimal::NaN)
                    }
                    (DecimalDiscriminants::NegativeInfinity, variant) => {
                        variant.unit_variant()?;
                        Ok(Decimal::NegativeInfinity)
                    }
                    (DecimalDiscriminants::Number, variant) => {
                        let decimal_str = variant.newtype_variant::<String>()?;
                        let big_decimal = decimal_str.parse::<BigDecimal>().map_err(|e| {
                            de::Error::custom(format!("Invalid decimal format: {e}"))
                        })?;
                        Ok(Decimal::Number(big_decimal))
                    }
                    (DecimalDiscriminants::Infinity, variant) => {
                        variant.unit_variant()?;
                        Ok(Decimal::Infinity)
                    }
                }
            }
        }

        deserializer.deserialize_enum("Decimal", DecimalDiscriminants::VARIANTS, DecimalVisitor)
    }
}

#[cfg(test)]
mod tests {
    use bincode::Options as _;
    use proptest::prelude::Strategy;
    use readyset_util::arbitrary::arbitrary_decimal_bytes_with_digits;
    use readyset_util::{eq_laws, hash_laws, ord_laws};

    use super::*;

    #[test]
    fn test_serde_all_variants() {
        let test_cases = vec![
            Decimal::NaN,
            Decimal::Infinity,
            Decimal::NegativeInfinity,
            Decimal::new(12345678901234567890, 18),
            Decimal::Number(BigDecimal::from(42)),
            Decimal::Number(BigDecimal::from_str("123.456").unwrap()),
            Decimal::Number(BigDecimal::from_str("-999.999").unwrap()),
            Decimal::zero(),
            Decimal::zero().round_dp(20),
        ];

        for decimal in test_cases {
            let serialized = bincode::options().serialize(&decimal).unwrap();
            let deserialized: Decimal = bincode::options().deserialize(&serialized).unwrap();
            assert_eq!(decimal, deserialized);
            assert_eq!(decimal.scale(), deserialized.scale());
        }
    }

    #[test]
    fn test_to_from_string_retains_scale() {
        let test_cases = vec![
            BigDecimal::from(42),
            BigDecimal::from_str("123.456").unwrap(),
            BigDecimal::from_str("-999.999").unwrap(),
            BigDecimal::zero(),
            BigDecimal::zero().with_scale(20),
        ];

        for decimal in test_cases {
            let serialized = decimal.to_plain_string();
            let deserialized = BigDecimal::from_str(&serialized).unwrap();
            assert_eq!(decimal, deserialized);
            assert_eq!(
                decimal.as_bigint_and_scale().1,
                deserialized.as_bigint_and_scale().1
            );
        }
    }

    #[test]
    fn ordering_matches_postgres() {
        assert!(Decimal::NegativeInfinity < Decimal::from(42));
        assert!(Decimal::from(42) < Decimal::Infinity);
        assert!(Decimal::Infinity < Decimal::NaN);
    }

    #[test]
    fn zero_ignores_scale() {
        let zero = Decimal::zero();
        let scaled_zero = zero.round_dp(5);
        assert_eq!(zero, scaled_zero);
    }

    /// Re-implement this just to avoid a circular dependency on the `Decimal` type causing
    /// resolution to fail if we try to use [`readyset_util::arbitrary::arbitrary_decimal`].
    fn arbitrary_decimal() -> impl Strategy<Value = Decimal> {
        arbitrary_decimal_bytes_with_digits(u16::MAX, u8::MAX)
            .prop_map(|bytes| Decimal::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap())
    }

    // These would normally be covered by the tests on `DfValue`, but it explicitly excludes numeric
    // values due to problems with comparing to floating-point numbers.
    eq_laws!(
        #[strategy(arbitrary_decimal())]
        Decimal
    );
    ord_laws!(
        #[strategy(arbitrary_decimal())]
        Decimal
    );
    hash_laws!(
        #[strategy(arbitrary_decimal())]
        Decimal
    );
}
