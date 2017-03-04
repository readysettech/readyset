#[cfg(feature="web")]
use rustc_serialize::json::{ToJson, Json};
use std::fmt;

use arccstr::ArcCStr;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
#[derive(Eq, PartialOrd, Ord, Hash, Debug, Clone)]
#[cfg_attr(feature="b_netsoup", derive(Serialize, Deserialize))]
pub enum DataType {
    /// A placeholder value -- is considered equal to every other `DataType` value.
    None,
    /// A 32-bit numeric value.
    Int(i32),
    /// A 64-bit numeric value.
    BigInt(i64),
    /// A fixed point real value.
    Real((i32, i16)),
    /// A reference-counted string-like value.
    Text(ArcCStr),
    /// A tiny string that fits in a pointer
    TinyText([u8; 8]),
}

impl DataType {
    /// Detect if this `DataType` is none.
    ///
    /// Since we re-implement `PartialEq` for `DataType` to always return `true` for `None`, it can
    /// actually be somewhat hard to do this right for users.
    pub fn is_none(&self) -> bool {
        if let DataType::None = *self {
            true
        } else {
            false
        }
    }
}

#[cfg(feature="web")]
impl ToJson for DataType {
    fn to_json(&self) -> Json {
        use std::str::FromStr;
        match *self {
            DataType::None => Json::Null,
            DataType::Int(n) => Json::I64(n as i64),
            DataType::BigInt(n) => Json::I64(n),
            DataType::Real((i, f)) => Json::F64(f64::from_str(&format!("{}.{}", i, f)).unwrap()),
            DataType::Text(..) |
            DataType::TinyText(..) => Json::String(self.into()),
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        if let DataType::None = *self {
            return true;
        }
        if let DataType::None = *other {
            return true;
        }

        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a == b,
            (&DataType::Int(ref a), &DataType::Int(ref b)) => a == b,
            (&DataType::Int(ref a), &DataType::BigInt(ref b)) => *a as i64 == *b,
            (&DataType::BigInt(ref a), &DataType::Int(ref b)) => *a == *b as i64,
            (&DataType::BigInt(ref a), &DataType::BigInt(ref b)) => a == b,
            (&DataType::Real((ref ai, ref af)), &DataType::Real((ref bi, ref bf))) => {
                ai == bi && af == bf
            }
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
        if f.is_nan() {
            panic!();
        } else {
            let (i, f, s) = f.integer_decode();
            DataType::Real(((s as i64 * i as i64) as i32, f))
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
                    CStr::from_bytes_with_nul(&bts[0..null]).unwrap().to_string_lossy()
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
            DataType::Real((i, frac)) => write!(f, "{}", format!("{}.{}", i, frac)),
        }
    }
}
