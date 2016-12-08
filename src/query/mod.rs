#[cfg(feature="web")]
use rustc_serialize::json::{ToJson, Json};
use std::fmt;
use std::sync;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
#[derive(Eq, PartialOrd, Ord, Hash, Debug, Clone)]
#[cfg_attr(feature="b_netsoup", derive(Serialize, Deserialize))]
pub enum DataType {
    /// A placeholder value -- is considered equal to every other `DataType` value.
    None,
    /// A numeric value.
    Number(i64),

    #[cfg(not(feature="no_strings"))]
    /// A reference-counted string-like value.
    Text(sync::Arc<String>),
    #[cfg(feature="no_strings")]
    /// An emulated no-cost string
    Text(&'static str),
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
        use std::ops::Deref;
        match *self {
            DataType::None => Json::Null,
            DataType::Number(n) => Json::I64(n),
            DataType::Text(ref s) => Json::String(s.to_string()),
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
            (&DataType::Number(ref a), &DataType::Number(ref b)) => a == b,
            _ => false,
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::Number(s)
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Number(s as i64)
    }
}

impl Into<String> for DataType {
    fn into(self) -> String {
        if let DataType::Text(s) = self {
            s.to_string()
        } else {
            unreachable!();
        }
    }
}

impl Into<i64> for DataType {
    fn into(self) -> i64 {
        if let DataType::Number(s) = self {
            s
        } else {
            unreachable!();
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        #[cfg(feature = "no_strings")]
        return DataType::Text("");

        #[cfg(not(feature = "no_strings"))]
        return DataType::Text(sync::Arc::new(s));
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
            DataType::Text(ref s) => write!(f, "\"{}\"", s),
            DataType::Number(n) => write!(f, "{}", n),
        }
    }
}
