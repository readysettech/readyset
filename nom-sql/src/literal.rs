use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::str;
use std::str::FromStr;

use bit_vec::BitVec;
use eui48::{MacAddress, MacAddressFormat};
use itertools::Itertools;
use launchpad::arbitrary::{
    arbitrary_bitvec, arbitrary_date_time, arbitrary_decimal, arbitrary_json, arbitrary_naive_time,
    arbitrary_positive_naive_date, arbitrary_timestamp_naive_date_time, arbitrary_uuid,
};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take};
use nom::character::complete::digit1;
use nom::combinator::{map, map_parser, map_res, opt, peek, recognize};
use nom::error::ErrorKind;
use nom::multi::{fold_many0, separated_list0};
use nom::sequence::{delimited, pair, preceded, tuple};
use nom::IResult;
use proptest::strategy::Strategy;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::ws_sep_comma;
use crate::whitespace::whitespace0;
use crate::{Dialect, SqlType};

#[derive(Clone, Debug, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub struct Float {
    pub value: f32,
    pub precision: u8,
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.value.to_bits() == other.value.to_bits() && self.precision == other.precision
    }
}

impl Eq for Float {}

impl Hash for Float {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.value.to_bits());
        state.write_u8(self.precision);
    }
}

#[derive(Clone, Debug, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub struct Double {
    pub value: f64,
    pub precision: u8,
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.value.to_bits() == other.value.to_bits() && self.precision == other.precision
    }
}

impl Eq for Double {}

impl Hash for Double {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.value.to_bits());
        state.write_u8(self.precision);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub enum ItemPlaceholder {
    QuestionMark,
    DollarNumber(u32),
    ColonNumber(u32),
}

impl ToString for ItemPlaceholder {
    fn to_string(&self) -> String {
        match *self {
            ItemPlaceholder::QuestionMark => "?".to_string(),
            ItemPlaceholder::DollarNumber(ref i) => format!("${}", i),
            ItemPlaceholder::ColonNumber(ref i) => format!(":{}", i),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub enum Literal {
    Null,
    Boolean(bool),
    /// Signed integer literals
    ///
    /// When parsing, we only return an integer when the value is negative
    Integer(i64),
    /// Unsigned integer literals
    ///
    /// When parsing, we default to unsigned integer if the integer value has no sign
    UnsignedInteger(u64),
    /// Represents an `f32` floating-point number.
    /// This distinction was introduced to avoid numeric error when transforming
    /// a `[Literal]` into another type (`[DfValue]` or `[mysql::Value]`), an back.
    /// As an example, if we read an `f32` from a binlog, we would be transforming that
    /// `f32` into an `f64` (thus, potentially introducing numeric error) if this type
    /// didn't exist.
    Float(Float),
    Double(Double),
    Numeric(i128, u32),
    String(String),
    #[weight(0)]
    Blob(Vec<u8>),
    // Even though `ByteArray` has the same inner representation as `Blob`,
    // we want to distinguish them, so then we can avoid doing a trial-and-error
    // to try to determine to which DfValue it corresponds to.
    // Having this here makes it easy to parse PostgreSQL byte array literals, and
    // then just store the `Vec<u8>` into a DfValue without testing if it's a valid
    // String or not.
    ByteArray(Vec<u8>),
    Placeholder(ItemPlaceholder),
    BitVector(Vec<u8>),
    #[weight(0)]
    Array(Vec<Literal>),
}

impl From<bool> for Literal {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<u64> for Literal {
    fn from(i: u64) -> Self {
        Literal::UnsignedInteger(i as _)
    }
}

impl From<i32> for Literal {
    fn from(i: i32) -> Self {
        Literal::Integer(i.into())
    }
}

impl From<u32> for Literal {
    fn from(i: u32) -> Self {
        Literal::UnsignedInteger(i.into())
    }
}

impl From<String> for Literal {
    fn from(s: String) -> Self {
        Literal::String(s)
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(s: &'a str) -> Self {
        Literal::String(String::from(s))
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        macro_rules! write_real {
            ($real:expr, $prec:expr) => {{
                let precision = if $prec < 30 { $prec } else { 30 };
                let fstr = format!("{:.*}", precision as usize, $real);
                // Trim all trailing zeros, but leave one after the dot if this is a whole number
                let res = fstr.trim_end_matches('0');
                if res.ends_with('.') {
                    write!(f, "{}0", res)
                } else {
                    write!(f, "{}", res)
                }
            }};
        }
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(true) => write!(f, "TRUE"),
            Literal::Boolean(false) => write!(f, "FALSE"),
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::UnsignedInteger(i) => write!(f, "{}", i),
            Literal::Float(float) => write_real!(float.value, float.precision),
            Literal::Double(double) => write_real!(double.value, double.precision),
            Literal::Numeric(val, scale) => {
                write!(f, "{}", Decimal::from_i128_with_scale(*val, *scale))
            }
            Literal::String(ref s) => {
                write!(f, "'{}'", s.replace('\'', "''").replace('\\', "\\\\"))
            }
            Literal::Blob(ref bv) => write!(
                f,
                "{}",
                bv.iter()
                    .map(|v| format!("{:x}", v))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
            Literal::ByteArray(b) => {
                write!(f, "E'\\x{}'", b.iter().map(|v| format!("{:x}", v)).join(""))
            }
            Literal::Placeholder(item) => write!(f, "{}", item.to_string()),
            Literal::BitVector(ref b) => {
                write!(
                    f,
                    "B'{}'",
                    BitVec::from_bytes(b.as_slice())
                        .iter()
                        .map(|bit| if bit { "1" } else { "0" })
                        .join("")
                )
            }
            Literal::Array(elems) => {
                fn write_value(lit: &Literal, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    match lit {
                        Literal::Array(elems) => {
                            write!(f, "[")?;
                            for (i, elem) in elems.iter().enumerate() {
                                if i != 0 {
                                    write!(f, ",")?;
                                }
                                write_value(elem, f)?;
                            }
                            write!(f, "]")
                        }
                        _ => write!(f, "{lit}"),
                    }
                }

                write!(f, "ARRAY[")?;
                for (i, elem) in elems.iter().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    write_value(elem, f)?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Literal {
    pub fn arbitrary_with_type(sql_type: &SqlType) -> impl Strategy<Value = Self> + 'static {
        use proptest::prelude::*;

        match sql_type {
            SqlType::Bool => prop_oneof![Just(Self::Integer(0)), Just(Self::Integer(1)),].boxed(),
            SqlType::Char(_)
            | SqlType::VarChar(_)
            | SqlType::TinyText
            | SqlType::MediumText
            | SqlType::LongText
            | SqlType::Text
            | SqlType::Citext => any::<String>().prop_map(Self::String).boxed(),
            SqlType::Int(_) => any::<i32>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedInt(_) => any::<u32>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::BigInt(_) => any::<i64>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedBigInt(_) => any::<u64>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::TinyInt(_) => any::<i8>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedTinyInt(_) => any::<u8>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::SmallInt(_) => any::<i16>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedSmallInt(_) => any::<u16>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::Blob
            | SqlType::ByteArray
            | SqlType::LongBlob
            | SqlType::MediumBlob
            | SqlType::TinyBlob
            | SqlType::Binary(_)
            | SqlType::VarBinary(_) => any::<Vec<u8>>().prop_map(Self::Blob).boxed(),
            SqlType::Float => any::<Float>().prop_map(Self::Float).boxed(),
            SqlType::Double | SqlType::Real | SqlType::Decimal(_, _) => {
                any::<Double>().prop_map(Self::Double).boxed()
            }
            SqlType::Numeric(_) => arbitrary_decimal()
                .prop_map(|d| Self::Numeric(d.mantissa(), d.scale()))
                .boxed(),
            SqlType::Date => arbitrary_positive_naive_date()
                .prop_map(|nd| Self::String(nd.format("%Y-%m-%d").to_string()))
                .boxed(),
            SqlType::DateTime(_) | SqlType::Timestamp => arbitrary_timestamp_naive_date_time()
                .prop_map(|ndt| Self::String(ndt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .boxed(),
            SqlType::TimestampTz => arbitrary_date_time()
                .prop_map(|dt| Self::String(dt.format("%Y-%m-%d %H:%M:%S %:z").to_string()))
                .boxed(),
            SqlType::Time => arbitrary_naive_time()
                .prop_map(|nt| Self::String(nt.format("%H:%M:%S").to_string()))
                .boxed(),
            SqlType::Enum(_) => unimplemented!("Enums aren't implemented yet"),
            SqlType::Json | SqlType::Jsonb => arbitrary_json()
                .prop_map(|v| Self::String(v.to_string()))
                .boxed(),
            SqlType::Inet => any::<IpAddr>()
                .prop_map(|v| Self::String(v.to_string()))
                .boxed(),
            SqlType::MacAddr => any::<[u8; 6]>()
                .prop_map(|bytes| -> Literal {
                    // We know the length and format of the bytes, so this should always be parsable
                    // as a `MacAddress`.
                    #[allow(clippy::unwrap_used)]
                    Self::String(
                        MacAddress::from_bytes(&bytes[..])
                            .unwrap()
                            .to_string(MacAddressFormat::HexString),
                    )
                })
                .boxed(),
            SqlType::Uuid => arbitrary_uuid()
                .prop_map(|uuid| Self::String(uuid.to_string()))
                .boxed(),
            SqlType::Bit(n) => {
                let size = n.unwrap_or(1) as usize;
                arbitrary_bitvec(size..=size)
                    .prop_map(|bits| Self::BitVector(bits.to_bytes()))
                    .boxed()
            }
            SqlType::VarBit(n) => {
                arbitrary_bitvec(0..n.map(|max_size| max_size as usize).unwrap_or(20_usize))
                    .prop_map(|bits| Self::BitVector(bits.to_bytes()))
                    .boxed()
            }
            SqlType::Serial => any::<i32>().prop_map(Self::from).boxed(),
            SqlType::BigSerial => any::<i64>().prop_map(Self::from).boxed(),
            SqlType::Array(_) => unimplemented!("Arrays aren't implemented yet"),
        }
    }

    /// Returns `true` if `self` is a [`String`] literal
    ///
    /// [`String`]: Literal::String
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(..))
    }
}

// Integer literal value
pub fn integer_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    let (i, sign) = opt(tag("-"))(i)?;
    let (i, num) = map_parser(digit1, nom::character::complete::u64)(i)?;

    // Default to Unsigned unless the value is negative
    let res = if sign.is_some() {
        Literal::Integer(-(num as i64))
    } else {
        Literal::UnsignedInteger(num)
    };

    Ok((i, res))
}

#[allow(clippy::type_complexity)]
pub fn float(i: &[u8]) -> IResult<&[u8], (Option<&[u8]>, &[u8], &[u8], &[u8])> {
    tuple((opt(tag("-")), digit1, tag("."), digit1))(i)
}

// Floating point literal value
#[allow(clippy::type_complexity)]
pub fn float_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(
        pair(
            peek(float),
            map_res(map_res(recognize(float), str::from_utf8), f64::from_str),
        ),
        |f| {
            let (_, _, _, frac) = f.0;
            Literal::Double(Double {
                value: f.1,
                precision: frac.len() as _,
            })
        },
    )(i)
}

fn boolean_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    alt((
        map(tag_no_case("true"), |_| Literal::Boolean(true)),
        map(tag_no_case("false"), |_| Literal::Boolean(false)),
    ))(i)
}

/// String literal value
fn raw_string_quoted(
    quote: &'static [u8],
    escape_quote: &'static [u8],
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<u8>> {
    move |i| {
        delimited(
            tag(quote),
            fold_many0(
                alt((
                    is_not(escape_quote),
                    map(pair(tag(quote), tag(quote)), |_| quote),
                    map(tag("\\\\"), |_| &b"\\"[..]),
                    map(tag("\\b"), |_| &b"\x7f"[..]),
                    map(tag("\\r"), |_| &b"\r"[..]),
                    map(tag("\\n"), |_| &b"\n"[..]),
                    map(tag("\\t"), |_| &b"\t"[..]),
                    map(tag("\\0"), |_| &b"\0"[..]),
                    map(tag("\\Z"), |_| &b"\x1A"[..]),
                    preceded(tag("\\"), take(1usize)),
                )),
                Vec::new,
                |mut acc: Vec<u8>, bytes: &[u8]| {
                    acc.extend(bytes);
                    acc
                },
            ),
            tag(quote),
        )(i)
    }
}

fn raw_string_single_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(b"'", b"\\'")(i)
}

fn raw_string_double_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(b"\"", b"\\\"")(i)
}

/// Specification for how string literals may be quoted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotingStyle {
    /// String literals are quoted with single quotes (`'`)
    Single,
    /// String literals are quoted with double quotes (`"`)
    Double,
    /// String literals may be quoted with either single quotes (`'`) or double quotes (`"`)
    SingleOrDouble,
}

/// Parse a raw (binary) string literal using the given [`QuotingStyle`]
pub fn raw_string_literal(
    quoting_style: QuotingStyle,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<u8>> {
    move |i| match quoting_style {
        QuotingStyle::Single => raw_string_single_quoted(i),
        QuotingStyle::Double => raw_string_double_quoted(i),
        QuotingStyle::SingleOrDouble => {
            alt((raw_string_single_quoted, raw_string_double_quoted))(i)
        }
    }
}

/// Parse a utf8 string literal using the given [`QuotingStyle`]
pub fn utf8_string_literal(
    quoting_style: QuotingStyle,
) -> impl Fn(&[u8]) -> IResult<&[u8], String> {
    move |i| {
        map_res(raw_string_literal(quoting_style), |bytes| {
            String::from_utf8(bytes)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(i, ErrorKind::Fail)))
        })(i)
    }
}

fn simple_literal(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        alt((
            float_literal,
            integer_literal,
            boolean_literal,
            map(dialect.bytes_literal(), Literal::ByteArray),
            map(dialect.bitvec_literal(), |bits| {
                Literal::BitVector(bits.to_bytes())
            }),
            map(tag_no_case("null"), |_| Literal::Null),
        ))(i)
    }
}

fn literal_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Literal>> {
    move |i| {
        let (i, _) = tag("[")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, elems) = separated_list0(
            ws_sep_comma,
            alt((
                non_nested_literal(dialect),
                map(literal_list(dialect), Literal::Array),
            )),
        )(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("]")(i)?;
        Ok((i, elems))
    }
}

fn array_literal(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        let (i, _) = tag_no_case("array")(i)?;
        let (i, _) = whitespace0(i)?;
        map(literal_list(dialect), Literal::Array)(i)
    }
}

fn non_nested_literal(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        alt((
            simple_literal(dialect),
            map(dialect.string_literal(), |bytes| {
                match String::from_utf8(bytes) {
                    Ok(s) => Literal::String(s),
                    Err(err) => Literal::Blob(err.into_bytes()),
                }
            }),
            map(tag("?"), |_| {
                Literal::Placeholder(ItemPlaceholder::QuestionMark)
            }),
            map(
                preceded(
                    tag(":"),
                    map_res(map_res(digit1, str::from_utf8), u32::from_str),
                ),
                |num| Literal::Placeholder(ItemPlaceholder::ColonNumber(num)),
            ),
            map(
                preceded(
                    tag("$"),
                    map_res(map_res(digit1, str::from_utf8), u32::from_str),
                ),
                |num| Literal::Placeholder(ItemPlaceholder::DollarNumber(num)),
            ),
            boolean_literal,
        ))(i)
    }
}

/// Parser for any literal value, including placeholders
pub fn literal(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| alt((non_nested_literal(dialect), array_literal(dialect)))(i)
}

/// Parser for a literal value which may be embedded inside of syntactic constructs within another
/// string literal, such as within the string literal syntax for postgresql arrays, jsonpath
/// expressions, etc.
pub fn embedded_literal(
    dialect: Dialect,
    quoting_style: QuotingStyle,
) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        alt((
            simple_literal(dialect),
            map(
                raw_string_literal(quoting_style),
                |bytes| match String::from_utf8(bytes) {
                    Ok(s) => Literal::String(s),
                    Err(err) => Literal::Blob(err.into_bytes()),
                },
            ),
        ))(i)
    }
}

#[cfg(test)]
mod tests {
    use assert_approx_eq::assert_approx_eq;
    use launchpad::hash::hash;
    use proptest::prop_assume;
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn float_formatting_strips_trailing_zeros() {
        let f = Literal::Double(Double {
            value: 1.5,
            precision: u8::MAX,
        });
        assert_eq!(f.to_string(), "1.5");
    }

    #[test]
    fn float_formatting_leaves_zero_after_dot() {
        let f = Literal::Double(Double {
            value: 0.0,
            precision: u8::MAX,
        });
        assert_eq!(f.to_string(), "0.0");
    }

    #[test]
    fn float_lots_of_zeros() {
        let res = float_literal(b"1.500000000000000000000000000000")
            .unwrap()
            .1;
        if let Literal::Double(Double { value, .. }) = res {
            assert_approx_eq!(value, 1.5);
        } else {
            unreachable!()
        }
    }

    #[proptest]
    fn real_hash_matches_eq(real1: Double, real2: Double) {
        assert_eq!(real1 == real2, hash(&real1) == hash(&real2));
    }

    #[proptest]
    fn literal_to_string_parse_round_trip(lit: Literal) {
        prop_assume!(!matches!(
            lit,
            Literal::Double(_) | Literal::Float(_) | Literal::Numeric(_, _) | Literal::ByteArray(_)
        ));
        match lit {
            Literal::BitVector(_) => {
                let s = lit.to_string();
                assert_eq!(literal(Dialect::PostgreSQL)(s.as_bytes()).unwrap().1, lit)
            }
            // Positive integers are parsed as Unsigned
            Literal::Integer(i) if i > 0 => {
                let s = lit.to_string();
                assert_eq!(
                    literal(Dialect::PostgreSQL)(s.as_bytes()).unwrap().1,
                    Literal::UnsignedInteger(i as u64)
                )
            }
            _ => {
                for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
                    let s = lit.to_string();
                    assert_eq!(literal(dialect)(s.as_bytes()).unwrap().1, lit)
                }
            }
        }
    }

    #[test]
    fn boolean_literals() {
        for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
            assert_eq!(
                test_parse!(literal(dialect), b"true"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"True"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"TruE"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"TRUE"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"false"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"False"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"FalsE"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"FALSE"),
                Literal::Boolean(false)
            );
        }
    }

    #[test]
    fn array_literal_to_string() {
        let lit = Literal::Array(vec![
            Literal::Array(vec![1.into(), 2.into(), 3.into()]),
            Literal::Array(vec![4.into(), 5.into(), 6.into()]),
        ]);
        assert_eq!(lit.to_string(), "ARRAY[[1,2,3],[4,5,6]]");
    }

    #[test]
    fn parse_array_literal() {
        let res = test_parse!(literal(Dialect::PostgreSQL), b"ARRAY[[1,2,3],[4,5,6]]");
        assert_eq!(
            res,
            Literal::Array(vec![
                Literal::Array(vec![1_u32.into(), 2_u32.into(), 3_u32.into()]),
                Literal::Array(vec![4_u32.into(), 5_u32.into(), 6_u32.into()]),
            ])
        );
    }
}
