use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::net::IpAddr;
use std::num::{IntErrorKind, ParseIntError};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use nom_sql::{Literal, SqlType};
use readyset_errors::{ReadySetError, ReadySetResult};

use crate::{Array, Collation, DfType, DfValue};

const TINYTEXT_WIDTH: usize = 14;

/// A nibble of [`Collation`], and a nibble of length (since length can never be greater than
/// [`TINYTEXT_WIDTH`])
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct LenAndCollation(u8);

impl LenAndCollation {
    #[inline(always)]
    const fn new(len: u8, collation: Collation) -> Self {
        debug_assert!(len < 16);
        Self(len | ((collation as u8) << 4))
    }

    #[inline(always)]
    fn from_len(len: u8) -> Self {
        Self::new(len, Collation::default())
    }

    #[inline(always)]
    const fn len(self) -> u8 {
        self.0 & 0b00001111
    }

    #[inline(always)]
    fn collation(self) -> Collation {
        Collation::from_repr(self.0 >> 4).expect("Internal invariant maintained")
    }

    #[inline(always)]
    fn set_collation(&mut self, collation: Collation) {
        self.0 &= 0b00001111; // zero out the collation first
        self.0 |= (collation as u8) << 4;
    }
}

/// An optimized storage for very short strings
#[derive(Clone, PartialEq, Eq)]
pub struct TinyText {
    len_and_collation: LenAndCollation,
    t: [u8; TINYTEXT_WIDTH],
}

/// A thin pointer over an Arc<[u8]> with lazy UTF-8 validation
#[repr(transparent)]
#[derive(Clone)]
pub struct Text(triomphe::ThinArc<AtomicBool, u8>);

impl TinyText {
    #[allow(clippy::len_without_is_empty)]
    pub const fn len(&self) -> u8 {
        self.len_and_collation.len()
    }

    /// Extracts a string slice containing the entire `TinyText`.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: Always safe, because we always validate when constructing
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Extract the underlying slice
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.t[..self.len() as usize]
    }

    /// A convenience method to create constant ASCII `TinyText`
    /// NOTE: this is not implemented as trait, so it can be a `const fn`
    pub const fn from_arr<const N: usize>(arr: &[u8; N]) -> Self {
        let mut t = [0u8; TINYTEXT_WIDTH];
        let mut i = 0;
        // We are limited by the constructs we can use in a const fn, so nothing fancier
        // than a while loop
        while i < arr.len() && i < TINYTEXT_WIDTH {
            if arr[i] > 127 {
                // If not an ascii character, stop
                break;
            }
            t[i] = arr[i];
            i += 1;
        }

        TinyText {
            len_and_collation: LenAndCollation::new(i as u8, Collation::Utf8),
            t,
        }
    }

    /// Create a new `TinyText` by copying a byte slice.
    /// Errors if slice is too long.
    ///
    /// # Panics
    ///
    /// Panics if not valid UTF-8.
    #[inline]
    pub fn from_slice(v: &[u8]) -> Result<Self, &'static str> {
        if v.len() > TINYTEXT_WIDTH {
            return Err("slice too long");
        }

        std::str::from_utf8(v).expect("Must always be UTF8");

        // For reasons I can't say using MaybeUninit::zeroed() is much faster
        // than assigning an array of zeroes (which uses memset instead). Don't remove
        // this without benchmarking (or at least looking at godbolt first).
        // SAFETY: it is safe because u8 is a zeroable type
        let mut t: [u8; TINYTEXT_WIDTH] = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        t[..v.len()].copy_from_slice(v);
        Ok(TinyText {
            len_and_collation: LenAndCollation::from_len(v.len() as _),
            t,
        })
    }

    /// Set the collation on this [`TinyText`]
    #[inline]
    pub fn set_collation(&mut self, collation: Collation) {
        self.len_and_collation.set_collation(collation);
    }

    /// Returns a version of `self` with the given collation
    #[inline]
    pub fn with_collation(mut self, collation: Collation) -> Self {
        self.set_collation(collation);
        self
    }

    /// Returns the configured collation for this [`TinyText`].
    #[inline]
    pub fn collation(&self) -> Collation {
        self.len_and_collation.collation()
    }
}

impl TryFrom<&str> for TinyText {
    type Error = &'static str;

    /// If an str can fit inside a `TinyText` returns new `TinyText` with that str
    fn try_from(s: &str) -> Result<Self, &'static str> {
        if s.len() > TINYTEXT_WIDTH {
            return Err("slice too long");
        }

        // For reasons I can't say using MaybeUninit::zeroed() is much faster
        // than assigning an array of zeroes (which uses memset instead). Don't remove
        // this without benchmarking (or at least looking at godbolt first).
        // SAFETY: it is safe because u8 is a zeroable type
        let mut t: [u8; TINYTEXT_WIDTH] = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        t[..s.len()].copy_from_slice(s.as_bytes());
        Ok(TinyText {
            len_and_collation: LenAndCollation::from_len(s.len() as _),
            t,
        })
    }
}

impl Text {
    /// Returns the underlying byte slice
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.slice
    }

    /// Returns the underlying byte slice as an `str`
    #[inline]
    pub fn as_str(&self) -> &str {
        // Check if already validated
        if self.0.header.header.load(Relaxed) {
            // SAFETY: Safe because we checked validation flag
            unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
        } else {
            let validated = std::str::from_utf8(self.as_bytes()).expect("Must always be UTF8");
            self.0.header.header.store(true, Relaxed);
            validated
        }
    }

    /// Create a new `Text` by copying a byte slice. It does not check if the
    /// slice contains valid UTF-8 text, and may panic later if `as_str` is
    /// called later if it does not.
    #[inline]
    pub fn from_slice_unchecked(v: &[u8]) -> Self {
        Self(triomphe::ThinArc::from_header_and_slice(
            AtomicBool::new(false),
            v,
        ))
    }
}

impl TryFrom<&[u8]> for Text {
    type Error = std::str::Utf8Error;

    fn try_from(t: &[u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(t).map(Into::into)
    }
}

impl From<&str> for Text {
    fn from(t: &str) -> Self {
        Self(triomphe::ThinArc::from_header_and_slice(
            AtomicBool::new(true),
            t.as_bytes(),
        ))
    }
}

impl PartialOrd for Text {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Text {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Ord for Text {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Eq for Text {}

impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl fmt::Debug for TinyText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl From<Text> for DfValue {
    fn from(text: Text) -> DfValue {
        DfValue::Text(text)
    }
}

impl From<TinyText> for DfValue {
    fn from(text: TinyText) -> DfValue {
        DfValue::TinyText(text)
    }
}

/// This trait implements text to DfValue coercion for any DfValue
/// that can be converted to an str. Currently this is just Text and TinyText, but should also be
/// implemented for ByteArray in the future.
pub(crate) trait TextCoerce: Sized + Clone + Into<DfValue> {
    /// Get the inner str from the DfValue
    fn try_str(&self) -> ReadySetResult<&str>;

    /// Print the DfValue name for error reporting
    fn type_name() -> String;

    /// A convenience constructor for a coerction error from this type
    fn coerce_err<D: ToString>(sql_type: &SqlType, deets: D) -> ReadySetError {
        ReadySetError::DfValueConversionError {
            src_type: Self::type_name(),
            target_type: format!("{:?}", sql_type),
            details: deets.to_string(),
        }
    }

    /// A convenience integer parser that diffirentiates between out of bounds errors and other
    /// parse errors
    fn parse_int<I>(str: &str, sql_type: &SqlType) -> ReadySetResult<DfValue>
    where
        I: FromStr<Err = ParseIntError> + Into<DfValue>,
    {
        match str.trim().parse::<I>() {
            Ok(i) => Ok(i.into()),
            Err(e)
                if *e.kind() == IntErrorKind::PosOverflow
                    || *e.kind() == IntErrorKind::NegOverflow =>
            {
                Err(Self::coerce_err(sql_type, "out of bounds"))
            }
            Err(e) => Err(Self::coerce_err(sql_type, e)),
        }
    }

    /// Coerce this type to a different DfValue.
    fn coerce_to(&self, sql_type: &SqlType, from_ty: &DfType) -> ReadySetResult<DfValue> {
        let str = self.try_str()?;

        match sql_type {
            SqlType::Bool => Ok(DfValue::from(!str.is_empty())),

            SqlType::TinyText
            | SqlType::MediumText
            | SqlType::Text
            | SqlType::LongText
            | SqlType::Char(None)
            | SqlType::VarChar(None) => Ok(self.clone().into()),

            SqlType::VarChar(Some(l)) if *l as usize >= str.len() => {
                // VarChar, but length is sufficient to store current string
                Ok(self.clone().into())
            }

            SqlType::Char(Some(l)) if *l as usize == str.len() => {
                // Char, but length is same as current string
                Ok(self.clone().into())
            }

            SqlType::Char(Some(l)) if *l as usize > str.len() => {
                // Char, but length is greater than the current string, have to pad with whitespace
                let mut new_string = String::with_capacity(*l as usize);
                new_string += str;
                new_string.extend(std::iter::repeat(' ').take(*l as usize - str.len()));
                Ok(DfValue::from(new_string))
            }

            SqlType::VarChar(Some(l)) | SqlType::Char(Some(l)) => {
                // String is too long, so have to truncate and allocate a new one
                // TODO: can we do something smarter, like keep a len field, and clone the existing
                // Arc?
                Ok(DfValue::from(&str[..*l as usize]))
            }

            SqlType::TinyBlob
            | SqlType::MediumBlob
            | SqlType::Blob
            | SqlType::LongBlob
            | SqlType::ByteArray
            | SqlType::Binary(None) => Ok(DfValue::ByteArray(str.as_bytes().to_vec().into())),

            SqlType::Binary(Some(l)) if *l as usize == str.len() => {
                // Binary is sufficent to store whole string
                Ok(DfValue::ByteArray(str.as_bytes().to_vec().into()))
            }

            SqlType::Binary(Some(l)) if *l as usize > str.len() => {
                // Binary is longer than string, pad with zero bytes
                let mut new_vec = Vec::with_capacity(*l as usize);
                new_vec.extend_from_slice(str.as_bytes());
                new_vec.extend(std::iter::repeat(0).take(*l as usize - str.len()));
                Ok(DfValue::ByteArray(new_vec.into()))
            }

            SqlType::VarBinary(l) if *l as usize >= str.len() => {
                // VarBinary is sufficent to store whole string
                Ok(DfValue::ByteArray(str.as_bytes().to_vec().into()))
            }

            SqlType::Binary(Some(l)) | SqlType::VarBinary(l) => {
                // Binary is shorter than string, truncate and convert
                Ok(DfValue::ByteArray(
                    str.as_bytes()[..*l as usize].to_vec().into(),
                ))
            }

            SqlType::Timestamp | SqlType::DateTime(_) | SqlType::TimestampTz | SqlType::Date => str
                .trim()
                .parse::<crate::TimestampTz>()
                .map_err(|e| Self::coerce_err(sql_type, e))
                .map(DfValue::TimestampTz),

            SqlType::TinyInt(_) => Self::parse_int::<i8>(str, sql_type),
            SqlType::SmallInt(_) => Self::parse_int::<i16>(str, sql_type),
            SqlType::Int(_) | SqlType::Serial => Self::parse_int::<i32>(str, sql_type),
            SqlType::BigInt(_) | SqlType::BigSerial => Self::parse_int::<i64>(str, sql_type),

            SqlType::UnsignedTinyInt(_) => Self::parse_int::<u8>(str, sql_type),
            SqlType::UnsignedSmallInt(_) => Self::parse_int::<u16>(str, sql_type),
            SqlType::UnsignedInt(_) => Self::parse_int::<u32>(str, sql_type),
            SqlType::UnsignedBigInt(_) => Self::parse_int::<u64>(str, sql_type),

            SqlType::Json | SqlType::Jsonb => {
                // Currently just validates the json
                // TODO: this is very very wrong as there is no gurantee two equal json objects will
                // be string equal, quite the opposite actually. And we can't just "normalize the
                // json" as we do for MAC and UUID.
                str.parse::<serde_json::Value>()
                    .map_err(|e| Self::coerce_err(sql_type, e))?;
                Ok(self.clone().into())
            }

            SqlType::MacAddr => {
                // Since MAC addresses can be represented in many ways, if we want to store them as
                // a string, we have to at least normalize to the same representation.
                // I.e. we want to make sure that:
                // '08:00:2b:01:02:03'
                // '08-00-2b-01-02-03'
                // '08002b:010203'
                // '08002b-010203'
                // '0800.2b01.0203'
                // '08002b010203' are equal
                let mut mac = str
                    .parse::<eui48::MacAddress>()
                    .map_err(|e| Self::coerce_err(sql_type, e))?
                    .to_string(eui48::MacAddressFormat::HexString);
                mac.make_ascii_lowercase(); // Same as postgres style
                if mac.as_str() == str {
                    Ok(self.clone().into())
                } else {
                    Ok(mac.into())
                }
            }

            SqlType::Inet => {
                // Since MAC addresses can be represented in many ways, if we want to store them as
                // a string, we have to at least normalize to the same representation.
                // I.e. we want to make sure that:
                // '0::beef',
                // '0:0::beef', and
                // '::beef' are equal
                let ip = str
                    .parse::<IpAddr>()
                    .map_err(|e| Self::coerce_err(sql_type, e))?
                    .to_string();
                Ok(ip.into())
            }

            SqlType::Uuid => {
                // Since UUIDs can be represented in many ways, if we want to store them as a
                // string, we have to at least normalize to the same representation.
                // I.e. we want to make sure that
                //'123e4567-e89b-12d3-a456-426614174000',
                //'123E4567-E89b-12D3-A456-426614174000',
                // and '123e4567e89b12d3a456426614174000' are equal.
                let uuid = str
                    .parse::<uuid::Uuid>()
                    .map_err(|e| Self::coerce_err(sql_type, e))?
                    .to_string();

                if uuid.as_str() == str {
                    Ok(self.clone().into())
                } else {
                    Ok(uuid.into())
                }
            }

            SqlType::Time => match str.parse::<mysql_time::MySqlTime>() {
                Ok(t) => Ok(DfValue::Time(t)),
                Err(mysql_time::ConvertError::ParseError) => Ok(DfValue::Time(Default::default())),
                Err(e) => Err(Self::coerce_err(sql_type, e)),
            },

            SqlType::Float | SqlType::Real => str
                .parse::<f32>()
                .map_err(|e| Self::coerce_err(sql_type, e))?
                .try_into(),

            SqlType::Double => str
                .parse::<f64>()
                .map_err(|e| Self::coerce_err(sql_type, e))?
                .try_into(),

            SqlType::Decimal(_, _) | SqlType::Numeric(_) => Ok(str
                .parse::<rust_decimal::Decimal>()
                .map_err(|e| Self::coerce_err(sql_type, e))?
                .into()),

            SqlType::Array(_) => DfValue::from(
                str.parse::<Array>()
                    .map_err(|e| Self::coerce_err(sql_type, e))?,
            )
            .coerce_to(sql_type, from_ty),

            SqlType::Enum(elements) => {
                if let Some(i) = elements
                    .iter()
                    .position(|e| matches!(e, Literal::String(s) if s == str))
                {
                    // MySQL enums use 1-based indexing since a value of 0 is reserved for string
                    // values that do not correspond to valid enum elements. Also, no need to check
                    // for overflow here since enum values can only be 16 bits wide (or maybe 32
                    // bits in Postgres, but that's still TBD depending on how we end up
                    // implementing PG enums).
                    Ok(DfValue::UnsignedInt(i as u64 + 1))
                } else {
                    Ok(DfValue::UnsignedInt(0))
                }
            }

            SqlType::Bit(_) | SqlType::VarBit(_) => Err(Self::coerce_err(sql_type, "Not allowed")),
        }
    }
}

impl TextCoerce for TinyText {
    fn try_str(&self) -> ReadySetResult<&str> {
        Ok(self.as_str())
    }

    fn type_name() -> String {
        "TinyText".into()
    }
}

impl TextCoerce for Text {
    fn try_str(&self) -> ReadySetResult<&str> {
        Ok(self.as_str())
    }

    fn type_name() -> String {
        "Text".into()
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;
    use crate::Collation;

    mod len_and_collation {
        use super::*;

        #[proptest]
        fn len_round_trip(mut len: u8) {
            len &= 0b00001111;
            let lc = LenAndCollation::from_len(len);
            assert_eq!(lc.len(), len);
        }

        #[proptest]
        fn collation_round_trip(mut len: u8, collation: Collation) {
            len &= 0b00001111;
            let lc = LenAndCollation::new(len, collation);
            assert_eq!(lc.len(), len);
            assert_eq!(lc.collation(), collation);
        }

        #[proptest]
        fn set_collation_round_trip(mut len: u8, collation: Collation) {
            len &= 0b00001111;
            let mut lc = LenAndCollation::from_len(len);
            assert_eq!(lc.len(), len);

            lc.set_collation(collation);
            assert_eq!(lc.len(), len);
            assert_eq!(lc.collation(), collation);
        }
    }

    #[proptest]
    fn tiny_str_round_trip(#[strategy("[a-bA-B0-9]{0,14}")] s: String) {
        let tt: TinyText = s.as_str().try_into().unwrap();
        assert_eq!(tt.as_str(), s);
    }

    #[proptest]
    fn text_str_round_trip(s: String) {
        let t: Text = s.as_str().into();
        assert_eq!(t.as_str(), s);
    }

    #[test]
    #[should_panic]
    fn text_panics_non_utf8() {
        let s = [255, 255, 255, 255];
        let t = Text::from_slice_unchecked(&s);
        t.as_str();
    }

    #[test]
    #[should_panic]
    fn tiny_text_panics_non_utf8() {
        let s = [255, 255, 255, 255];
        TinyText::from_slice(&s).expect("ok");
    }

    #[test]
    fn text_coercion() {
        // TEXT to TEXT coercions
        let text = DfValue::from("abcdefgh");
        assert_eq!(
            text.coerce_to(&SqlType::Char(Some(10)), &DfType::Unknown)
                .unwrap(),
            DfValue::from("abcdefgh  ")
        );
        assert_eq!(
            text.coerce_to(&SqlType::Char(Some(4)), &DfType::Unknown)
                .unwrap(),
            DfValue::from("abcd")
        );
        assert_eq!(
            text.coerce_to(&SqlType::VarChar(Some(10)), &DfType::Unknown)
                .unwrap(),
            DfValue::from("abcdefgh")
        );
        assert_eq!(
            text.coerce_to(&SqlType::VarChar(Some(4)), &DfType::Unknown)
                .unwrap(),
            DfValue::from("abcd")
        );

        // TEXT to BINARY
        assert_eq!(
            text.coerce_to(&SqlType::Binary(Some(10)), &DfType::Unknown)
                .unwrap(),
            DfValue::ByteArray(b"abcdefgh\0\0".to_vec().into())
        );
        assert_eq!(
            text.coerce_to(&SqlType::Binary(Some(4)), &DfType::Unknown)
                .unwrap(),
            DfValue::ByteArray(b"abcd".to_vec().into())
        );
        assert_eq!(
            text.coerce_to(&SqlType::VarBinary(10), &DfType::Unknown)
                .unwrap(),
            DfValue::ByteArray(b"abcdefgh".to_vec().into())
        );
        assert_eq!(
            text.coerce_to(&SqlType::VarBinary(4), &DfType::Unknown)
                .unwrap(),
            DfValue::ByteArray(b"abcd".to_vec().into())
        );

        // TEXT to INTEGER
        assert_eq!(
            DfValue::from("50")
                .coerce_to(&SqlType::TinyInt(None), &DfType::Unknown)
                .unwrap(),
            DfValue::Int(50),
        );
        assert!(DfValue::from("500")
            .coerce_to(&SqlType::TinyInt(None), &DfType::Unknown)
            .is_err());
        assert_eq!(
            DfValue::from("-500")
                .coerce_to(&SqlType::Int(None), &DfType::Unknown)
                .unwrap(),
            DfValue::Int(-500),
        );
        assert!(DfValue::from("-500")
            .coerce_to(&SqlType::UnsignedInt(None), &DfType::Unknown)
            .is_err());

        // TEXT to FLOAT
        assert_eq!(
            DfValue::from("50")
                .coerce_to(&SqlType::Real, &DfType::Unknown)
                .unwrap(),
            DfValue::Float(50.0),
        );
        assert_eq!(
            DfValue::from("-50.5")
                .coerce_to(&SqlType::Double, &DfType::Unknown)
                .unwrap(),
            DfValue::Double(-50.5),
        );

        // TEXT to UUID
        assert_eq!(
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                .coerce_to(&SqlType::Uuid, &DfType::Unknown)
                .unwrap(),
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        );
        assert_eq!(
            DfValue::from("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11")
                .coerce_to(&SqlType::Uuid, &DfType::Unknown)
                .unwrap(),
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        );
        assert_eq!(
            DfValue::from("a0eebc999c0b4ef8bb6d6bb9bd380a11")
                .coerce_to(&SqlType::Uuid, &DfType::Unknown)
                .unwrap(),
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        );
        /* TODO: fix the following UUID conversions one day
        assert_eq!(
            DfValue::from("a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11")
                .coerce_to(&SqlType::Uuid, &DfType::Unknown)
                .unwrap(),
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        );
        assert_eq!(
            DfValue::from("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}")
                .coerce_to(&SqlType::Uuid, &DfType::Unknown)
                .unwrap(),
            DfValue::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        );*/

        // TEXT to MAC
        assert_eq!(
            DfValue::from("08:00:2b:01:02:03")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to MAC
        assert_eq!(
            DfValue::from("08-00-2b-01-02-03")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to MAC
        assert_eq!(
            DfValue::from("08002b:010203")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to MAC
        assert_eq!(
            DfValue::from("08002b-010203")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to MAC
        assert_eq!(
            DfValue::from("0800.2b01.0203")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to MAC
        assert_eq!(
            DfValue::from("08002b010203")
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap(),
            DfValue::from("08:00:2b:01:02:03"),
        );
        // TEXT to INET
        assert_eq!(
            DfValue::from("feed:0:0::beef")
                .coerce_to(&SqlType::Inet, &DfType::Unknown)
                .unwrap(),
            DfValue::from("feed::beef")
        );
        // TEXT to ENUM
        let enum_type = SqlType::Enum(
            ["red", "yellow", "green"]
                .map(|s| Literal::String(s.to_string()))
                .into(),
        );
        assert_eq!(
            DfValue::from("green")
                .coerce_to(&enum_type, &DfType::Unknown)
                .unwrap(),
            DfValue::UnsignedInt(3)
        );
        assert_eq!(
            DfValue::from("ultraviolet")
                .coerce_to(&enum_type, &DfType::Unknown)
                .unwrap(),
            DfValue::UnsignedInt(0)
        );
    }
}
