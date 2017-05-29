use arccstr::ArcCStr;

use nom_sql::Literal;

#[cfg(feature="web")]
use serde_json::Value;

use std::ops::{Deref, DerefMut};
use std::sync;
use std::fmt;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
#[derive(Eq, PartialOrd, Ord, Hash, Debug, Clone, Serialize, Deserialize)]
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
    TinyText([u8; 8]),
}

#[cfg(feature="web")]
impl DataType {
    /// Lossy representation as JSON value.
    pub fn to_json(&self) -> Value {
        match *self {
            DataType::None => json!(null),
            DataType::Int(n) => json!(n),
            DataType::BigInt(n) => json!(n),
            DataType::Real(i, f) => json!((i as f64) + (f as f64) * 1.0e-9),
            DataType::Text(..) |
            DataType::TinyText(..) => Value::String(self.into()),
        }
    }
}

impl DataType {
    pub(crate) fn external_clone(&self) -> Self {
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
            (&DataType::None, &DataType::None) => true,
            _ => false,
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
        let mut frac = (f.fract() * 1000_000_000.0).round() as i32;
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
                if bts[7] == 0 {
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

impl From<String> for DataType {
    fn from(s: String) -> Self {
        let len = s.as_bytes().len();
        if len <= 8 {
            let mut bytes = [0; 8];
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

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "*"),
            DataType::Text(..) |
            DataType::TinyText(..) => {
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
        }
    }
}

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Record {
    Positive(sync::Arc<Vec<DataType>>),
    Negative(sync::Arc<Vec<DataType>>),
    DeleteRequest(Vec<DataType>),
}

impl Record {
    pub fn rec(&self) -> &[DataType] {
        match *self {
            Record::Positive(ref v) |
            Record::Negative(ref v) => &v[..],
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

    pub fn extract(self) -> (sync::Arc<Vec<DataType>>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl Deref for Record {
    type Target = sync::Arc<Vec<DataType>>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) |
            Record::Negative(ref r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) |
            Record::Negative(ref mut r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl From<sync::Arc<Vec<DataType>>> for Record {
    fn from(other: sync::Arc<Vec<DataType>>) -> Self {
        Record::Positive(other)
    }
}

impl From<Vec<DataType>> for Record {
    fn from(other: Vec<DataType>) -> Self {
        Record::Positive(sync::Arc::new(other))
    }
}

impl From<(sync::Arc<Vec<DataType>>, bool)> for Record {
    fn from(other: (sync::Arc<Vec<DataType>>, bool)) -> Self {
        if other.1 {
            Record::Positive(other.0)
        } else {
            Record::Negative(other.0)
        }
    }
}

impl From<(Vec<DataType>, bool)> for Record {
    fn from(other: (Vec<DataType>, bool)) -> Self {
        if other.1 {
            Record::Positive(sync::Arc::new(other.0))
        } else {
            Record::Negative(sync::Arc::new(other.0))
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
        where I: IntoIterator<Item = Record>
    {
        Records(iter.into_iter().collect())
    }
}
impl FromIterator<sync::Arc<Vec<DataType>>> for Records {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = sync::Arc<Vec<DataType>>>
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

impl Into<Records> for Vec<sync::Arc<Vec<DataType>>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
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
}
