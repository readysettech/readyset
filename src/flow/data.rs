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
impl ToJson for DataType {
    fn to_json(&self) -> Json {
        match *self {
            DataType::None => Json::Null,
            DataType::Int(n) => Json::I64(n as i64),
            DataType::BigInt(n) => Json::I64(n),
            DataType::Real(i, f) => Json::F64((i as f64) + (f as f64) * 1.0e-9),
            DataType::Text(..) |
            DataType::TinyText(..) => Json::String(self.into()),
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
        assert_eq!(a.to_json(), Json::F64(2.5));
        assert_eq!(b.to_json(), Json::F64(-2.01));
        assert_eq!(c.to_json(), Json::F64(-0.012345678));
    }
}
