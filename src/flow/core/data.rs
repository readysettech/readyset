use arccstr::ArcCStr;

use chrono::{self, NaiveDateTime};

use nom_sql::Literal;

#[cfg(feature = "web")]
use serde_json::Value;

use std::hash::{Hash, Hasher};
use std::ops::{Add, Deref, DerefMut, Div, Mul, Sub};
use std::fmt;

const FLOAT_PRECISION: f64 = 1000_000_000.0;
const TINYTEXT_WIDTH: usize = 15;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DataType` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DataType::deep_clone` to
/// clone the *value* of a `DataType` without danger of contention.
#[derive(Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum DataType {
    /// An empty value.
    None,
    /// A 32-bit numeric value.
    Int(i32),
    /// A 64-bit numeric value.
    BigInt(i64),
    /// A fixed point real value. The first field is the integer part, while the second is the
    /// fractional and must be between -999999999 and 999999999.
    Real(i32, i32),
    /// A reference-counted string-like value.
    Text(ArcCStr),
    /// A tiny string that fits in a pointer
    TinyText([u8; TINYTEXT_WIDTH]),
    /// A timestamp for date/time types.
    Timestamp(NaiveDateTime),
}

#[cfg(feature = "web")]
impl DataType {
    /// Lossy representation as JSON value.
    pub fn to_json(&self) -> Value {
        match *self {
            DataType::None => json!(null),
            DataType::Int(n) => json!(n),
            DataType::BigInt(n) => json!(n),
            DataType::Real(i, f) => json!((i as f64) + (f as f64) * 1.0e-9),
            DataType::Text(..) | DataType::TinyText(..) => Value::String(self.into()),
            DataType::Timestamp(ts) => json!(ts.format("%+").to_string()),
        }
    }
}

impl DataType {
    /// Clone the value contained within this `DataType`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    pub fn deep_clone(&self) -> Self {
        match *self {
            DataType::Text(ref cstr) => DataType::Text(ArcCStr::from(&**cstr)),
            ref dt => dt.clone(),
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a == b,
            (&DataType::Int(ref a), &DataType::Int(ref b)) => a == b,
            (&DataType::Int(ref a), &DataType::BigInt(ref b)) => *a as i64 == *b,
            (&DataType::BigInt(ref a), &DataType::Int(ref b)) => *a == *b as i64,
            (&DataType::BigInt(ref a), &DataType::BigInt(ref b)) => a == b,
            (&DataType::Real(ref ai, ref af), &DataType::Real(ref bi, ref bf)) => {
                ai == bi && af == bf
            }
            (&DataType::Timestamp(ref tsa), &DataType::Timestamp(ref tsb)) => *tsa == *tsb,
            (&DataType::None, &DataType::None) => true,
            _ => false,
        }
    }
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The default derived hash function also hashes the variant tag, which turns out to be
        // rather expensive. This version could (but probably won't) have a higher rate of
        // collisions, but the decreased overhead is worth it.
        match *self {
            DataType::None => {}
            DataType::Int(n) => n.hash(state),
            DataType::BigInt(n) => n.hash(state),
            DataType::Real(i, f) => {
                i.hash(state);
                f.hash(state);
            }
            DataType::Text(ref t) => t.hash(state),
            DataType::TinyText(t) => t.hash(state),
            DataType::Timestamp(ts) => ts.hash(state),
        }
    }
}


impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::BigInt(s)
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Int(s as i32)
    }
}

impl From<f64> for DataType {
    fn from(f: f64) -> Self {
        if !f.is_finite() {
            panic!();
        }

        let mut i = f.trunc() as i32;
        let mut frac = (f.fract() * FLOAT_PRECISION).round() as i32;
        if frac == 1000_000_000 {
            i += 1;
            frac = 0;
        } else if frac == -1000_000_000 {
            i -= 1;
            frac = 0;
        }

        DataType::Real(i, frac)
    }
}

impl<'a> From<&'a Literal> for DataType {
    fn from(l: &'a Literal) -> Self {
        match *l {
            Literal::Null => DataType::None,
            Literal::Integer(i) => i.into(),
            Literal::String(ref s) => s.as_str().into(),
            Literal::CurrentTimestamp => {
                let ts = chrono::Local::now().naive_local();
                DataType::Timestamp(ts)
            }
            _ => unimplemented!(),
        }
    }
}

use std::borrow::Cow;
impl<'a> Into<Cow<'a, str>> for &'a DataType {
    fn into(self) -> Cow<'a, str> {
        match *self {
            DataType::Text(ref s) => s.to_string_lossy(),
            DataType::TinyText(ref bts) => {
                if bts[TINYTEXT_WIDTH - 1] == 0 {
                    // NULL terminated CStr
                    use std::ffi::CStr;
                    let null = bts.iter().position(|&i| i == 0).unwrap() + 1;
                    CStr::from_bytes_with_nul(&bts[0..null])
                        .unwrap()
                        .to_string_lossy()
                } else {
                    // String is exactly eight bytes
                    String::from_utf8_lossy(&bts[..])
                }
            }
            _ => unreachable!(),
        }
    }
}

impl<'a> Into<String> for &'a DataType {
    fn into(self) -> String {
        let cow: Cow<str> = self.into();
        cow.to_string()
    }
}

impl Into<String> for DataType {
    fn into(self) -> String {
        (&self).into()
    }
}

impl Into<i64> for DataType {
    fn into(self) -> i64 {
        if let DataType::BigInt(s) = self {
            s
        } else {
            unreachable!();
        }
    }
}

impl Into<f64> for DataType {
    fn into(self) -> f64 {
        match self {
            DataType::Real(i, f) => i as f64 + (f as f64) / FLOAT_PRECISION,
            DataType::Int(i) => i as f64,
            DataType::BigInt(i) => i as f64,
            _ => unreachable!(),
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        let len = s.as_bytes().len();
        if len <= TINYTEXT_WIDTH {
            let mut bytes = [0; TINYTEXT_WIDTH];
            if len != 0 {
                let bts = &mut bytes[0..len];
                bts.copy_from_slice(s.as_bytes());
            }
            DataType::TinyText(bytes)
        } else {
            use std::convert::TryFrom;
            DataType::Text(ArcCStr::try_from(s).unwrap())
        }
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::from(s.to_owned())
    }
}

// Performs an arithmetic operation on two numeric DataTypes,
// returning a new DataType as the result.
macro_rules! arithmetic_operation (
    ($op:tt, $first:ident, $second:ident) => (
        match ($first, $second) {
            (DataType::Int(a), DataType::Int(b)) => (a $op b).into(),
            (DataType::BigInt(a), DataType::BigInt(b)) => (a $op b).into(),
            (DataType::Int(a), DataType::BigInt(b)) => ((a as i64) $op b).into(),
            (DataType::BigInt(a), DataType::Int(b)) => (a $op (b as i64)).into(),

            (first @ DataType::Int(..), second @ DataType::Real(..)) |
            (first @ DataType::Real(..), second @ DataType::Int(..)) |
            (first @ DataType::Real(..), second @ DataType::Real(..)) => {
                let a: f64 = first.into();
                let b: f64 = second.into();
                (a $op b).into()
            }
            (first, second) => panic!(
                format!(
                    "can't {} a {:?} and {:?}",
                    stringify!($op),
                    first,
                    second,
                )
            ),
        }
    );
);

impl Add for DataType {
    type Output = DataType;

    fn add(self, other: DataType) -> DataType {
        arithmetic_operation!(+, self, other)
    }
}

impl Sub for DataType {
    type Output = DataType;

    fn sub(self, other: DataType) -> DataType {
        arithmetic_operation!(-, self, other)
    }
}

impl Mul for DataType {
    type Output = DataType;

    fn mul(self, other: DataType) -> DataType {
        arithmetic_operation!(*, self, other)
    }
}

impl Div for DataType {
    type Output = DataType;

    fn div(self, other: DataType) -> DataType {
        arithmetic_operation!(/, self, other)
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "None"),
            DataType::Text(..) => {
                let text: Cow<str> = self.into();
                write!(f, "Text({:?})", text)
            }
            DataType::TinyText(..) => {
                let text: Cow<str> = self.into();
                write!(f, "TinyText({:?})", text)
            }
            DataType::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            DataType::Real(..) => write!(f, "Real({})", self),
            DataType::Int(n) => write!(f, "Int({})", n),
            DataType::BigInt(n) => write!(f, "BigInt({})", n),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "*"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: Cow<str> = self.into();
                write!(f, "\"{}\"", text)
            }
            DataType::Int(n) => write!(f, "{}", n),
            DataType::BigInt(n) => write!(f, "{}", n),
            DataType::Real(i, frac) => {
                if i == 0 && frac < 0 {
                    // We have to insert the negative sign ourselves.
                    write!(f, "{}", format!("-0.{:09}", frac.abs()))
                } else {
                    write!(f, "{}", format!("{}.{:09}", i, frac.abs()))
                }
            }
            DataType::Timestamp(ts) => write!(f, "{}", format!("{}", ts.format("%c"))),
        }
    }
}

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Record {
    Positive(Vec<DataType>),
    Negative(Vec<DataType>),
    DeleteRequest(Vec<DataType>),
}

impl Record {
    pub fn rec(&self) -> &[DataType] {
        match *self {
            Record::Positive(ref v) | Record::Negative(ref v) => &v[..],
            Record::DeleteRequest(..) => unreachable!(),
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (Vec<DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl Deref for Record {
    type Target = Vec<DataType>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) | Record::Negative(ref r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) | Record::Negative(ref mut r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl From<Vec<DataType>> for Record {
    fn from(other: Vec<DataType>) -> Self {
        Record::Positive(other)
    }
}

impl From<(Vec<DataType>, bool)> for Record {
    fn from(other: (Vec<DataType>, bool)) -> Self {
        if other.1 {
            Record::Positive(other.0)
        } else {
            Record::Negative(other.0)
        }
    }
}

impl Into<Vec<Record>> for Records {
    fn into(self) -> Vec<Record> {
        self.0
    }
}

use std::iter::FromIterator;
impl FromIterator<Record> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Record>,
    {
        Records(iter.into_iter().collect())
    }
}
impl FromIterator<Vec<DataType>> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Vec<DataType>>,
    {
        Records(iter.into_iter().map(Record::Positive).collect())
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = ::std::vec::IntoIter<Record>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<'a> IntoIterator for &'a Records {
    type Item = &'a Record;
    type IntoIter = ::std::slice::Iter<'a, Record>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Represents a set of records returned from a query.
pub type Datas = Vec<Vec<DataType>>;

#[derive(Clone, Default, PartialEq, Debug, Serialize, Deserialize)]
pub struct Records(Vec<Record>);

impl Deref for Records {
    type Target = Vec<Record>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Into<Records> for Record {
    fn into(self) -> Records {
        Records(vec![self])
    }
}

impl Into<Records> for Vec<Record> {
    fn into(self) -> Records {
        Records(self)
    }
}

impl Into<Records> for Vec<Vec<DataType>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

impl Into<Records> for Vec<(Vec<DataType>, bool)> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_to_string() {
        let a: DataType = (2.5).into();
        let b: DataType = (-2.01).into();
        let c: DataType = (-0.012345678).into();
        assert_eq!(a.to_string(), "2.500000000");
        assert_eq!(b.to_string(), "-2.010000000");
        assert_eq!(c.to_string(), "-0.012345678");
    }

    #[test]
    fn real_to_json() {
        let a: DataType = (2.5).into();
        let b: DataType = (-2.01).into();
        let c: DataType = (-0.012345678).into();
        assert_eq!(a.to_json(), json!(2.5));
        assert_eq!(b.to_json(), json!(-2.01));
        assert_eq!(c.to_json(), json!(-0.012345678));
    }

    #[test]
    fn real_to_float() {
        let original = 2.5;
        let data_type: DataType = original.into();
        let converted: f64 = data_type.into();
        assert_eq!(original, converted);
    }

    #[test]
    fn add_data_types() {
        assert_eq!(DataType::from(1) + DataType::from(2), 3.into());
        assert_eq!(DataType::from(1.5) + DataType::from(2), (3.5).into());
        assert_eq!(DataType::from(2) + DataType::from(1.5), (3.5).into());
        assert_eq!(DataType::from(1.5) + DataType::from(2.5), (4.0).into());
        assert_eq!(DataType::BigInt(1) + DataType::BigInt(2), 3.into());
        assert_eq!(DataType::from(1) + DataType::BigInt(2), 3.into());
        assert_eq!(DataType::BigInt(2) + DataType::from(1), 3.into());
    }

    #[test]
    fn subtract_data_types() {
        assert_eq!(DataType::from(2) - DataType::from(1), 1.into());
        assert_eq!(DataType::from(3.5) - DataType::from(2), (1.5).into());
        assert_eq!(DataType::from(2) - DataType::from(1.5), (0.5).into());
        assert_eq!(DataType::from(3.5) - DataType::from(2.0), (1.5).into());
        assert_eq!(DataType::BigInt(1) - DataType::BigInt(2), (-1).into());
        assert_eq!(DataType::from(1) - DataType::BigInt(2), (-1).into());
        assert_eq!(DataType::BigInt(2) - DataType::from(1), 1.into());
    }

    #[test]
    fn multiply_data_types() {
        assert_eq!(DataType::from(2) * DataType::from(1), 2.into());
        assert_eq!(DataType::from(3.5) * DataType::from(2), (7.0).into());
        assert_eq!(DataType::from(2) * DataType::from(1.5), (3.0).into());
        assert_eq!(DataType::from(3.5) * DataType::from(2.0), (7.0).into());
        assert_eq!(DataType::BigInt(1) * DataType::BigInt(2), 2.into());
        assert_eq!(DataType::from(1) * DataType::BigInt(2), 2.into());
        assert_eq!(DataType::BigInt(2) * DataType::from(1), 2.into());
    }

    #[test]
    fn divide_data_types() {
        assert_eq!(DataType::from(2) / DataType::from(1), 2.into());
        assert_eq!(DataType::from(7.5) / DataType::from(2), (3.75).into());
        assert_eq!(DataType::from(7) / DataType::from(2.5), (2.8).into());
        assert_eq!(DataType::from(3.5) / DataType::from(2.0), (1.75).into());
        assert_eq!(DataType::BigInt(4) / DataType::BigInt(2), 2.into());
        assert_eq!(DataType::from(4) / DataType::BigInt(2), 2.into());
        assert_eq!(DataType::BigInt(4) / DataType::from(2), 2.into());
    }

    #[test]
    #[should_panic(expected = "can't + a TinyText(\"hi\") and Int(5)")]
    fn add_invalid_types() {
        let a: DataType = "hi".into();
        let b: DataType = 5.into();
        a + b;
    }

    #[test]
    fn data_type_debug() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "I contain ' and \"".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{:?}", tiny_text), "TinyText(\"hi\")");
        assert_eq!(format!("{:?}", text), "Text(\"I contain \\' and \\\"\")");
        assert_eq!(format!("{:?}", real), "Real(-0.050000000)");
        assert_eq!(
            format!("{:?}", timestamp),
            "Timestamp(1970-01-01T00:00:00.042)"
        );
        assert_eq!(format!("{:?}", int), "Int(5)");
        assert_eq!(format!("{:?}", big_int), "BigInt(5)");
    }

    #[test]
    fn data_type_display() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "this is a very long text indeed".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{}", tiny_text), "\"hi\"");
        assert_eq!(format!("{}", text), "\"this is a very long text indeed\"");
        assert_eq!(format!("{}", real), "-0.050000000");
        assert_eq!(format!("{}", timestamp), "Thu Jan  1 00:00:00 1970");
        assert_eq!(format!("{}", int), "5");
        assert_eq!(format!("{}", big_int), "5");
    }
}
