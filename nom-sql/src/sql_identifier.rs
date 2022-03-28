use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;

use proptest::arbitrary::Arbitrary;

const TINYTEXT_WIDTH: usize = 14;

/// A String especially optimized for inline storage of short strings, and fast cloning of longer
/// strings
#[derive(Clone, PartialEq, Eq)]
pub enum SqlIdentifier {
    Tiny(TinyText),
    Text(Text),
}

/// An optimized storage for very short strings
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TinyText {
    len: u8,
    t: [u8; TINYTEXT_WIDTH],
}

/// A thin pointer over an Arc<[u8]>
#[repr(transparent)]
#[derive(Clone)]
pub struct Text(triomphe::ThinArc<(), u8>);

impl TinyText {
    /// Extracts a string slice containing the entire `TinyText`.
    #[inline]
    fn as_str(&self) -> &str {
        // SAFETY: Always safe, because we always validate when constructing
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Extract the underlying slice
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.t[..self.len as usize]
    }

    /// Create a new `TinyText` by copying a byte slice.
    /// Errors if slice is too long.
    ///
    /// # Panics
    ///
    /// Panics if not valid UTF-8.
    #[inline]
    fn from_slice(v: &[u8]) -> Result<Self, &'static str> {
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
            len: v.len() as _,
            t,
        })
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
            len: s.len() as _,
            t,
        })
    }
}

impl Text {
    /// Returns the underlying byte slice
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.0.slice
    }

    /// Returns the underlying byte slice as an `str`
    #[inline]
    fn as_str(&self) -> &str {
        // SAFETY: Safe because we always create from str
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Create a new `Text` by copying a byte slice.
    ///
    /// # Panics
    ///
    /// Panics if not valid UTF-8.
    #[inline]
    fn from_slice(v: &[u8]) -> Self {
        std::str::from_utf8(v).expect("Must always be UTF8");
        Self(triomphe::ThinArc::from_header_and_slice((), v))
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
        Self(triomphe::ThinArc::from_header_and_slice((), t.as_bytes()))
    }
}

impl PartialOrd for Text {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Text {
    fn eq(&self, other: &Self) -> bool {
        if self.0.as_ptr() == other.0.as_ptr() {
            return true;
        }

        self.as_str().eq(other.as_str())
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

impl SqlIdentifier {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            SqlIdentifier::Tiny(t) => t.as_str(),
            SqlIdentifier::Text(t) => t.as_str(),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SqlIdentifier::Tiny(t) => t.as_bytes(),
            SqlIdentifier::Text(t) => t.as_bytes(),
        }
    }
}

impl Default for SqlIdentifier {
    fn default() -> Self {
        SqlIdentifier::Tiny(TinyText::from_slice(&[]).unwrap())
    }
}

impl From<&str> for SqlIdentifier {
    #[inline]
    fn from(s: &str) -> Self {
        match TinyText::try_from(s) {
            Ok(tt) => SqlIdentifier::Tiny(tt),
            Err(_) => SqlIdentifier::Text(s.into()),
        }
    }
}

impl From<&&str> for SqlIdentifier {
    #[inline]
    fn from(s: &&str) -> Self {
        (*s).into()
    }
}

impl From<String> for SqlIdentifier {
    #[inline]
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl From<&String> for SqlIdentifier {
    #[inline]
    fn from(s: &String) -> Self {
        s.as_str().into()
    }
}

impl From<&SqlIdentifier> for SqlIdentifier {
    #[inline]
    fn from(s: &SqlIdentifier) -> Self {
        s.clone()
    }
}

impl From<SqlIdentifier> for String {
    #[inline]
    fn from(ident: SqlIdentifier) -> Self {
        ident.as_str().to_owned()
    }
}

impl std::ops::Deref for SqlIdentifier {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SqlIdentifier {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for SqlIdentifier {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::borrow::Borrow<str> for SqlIdentifier {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialOrd for SqlIdentifier {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for SqlIdentifier {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq<str> for SqlIdentifier {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq(other)
    }
}

impl PartialEq<&str> for SqlIdentifier {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq(*other)
    }
}

impl PartialEq<&SqlIdentifier> for SqlIdentifier {
    #[inline]
    fn eq(&self, other: &&SqlIdentifier) -> bool {
        self.eq(*other)
    }
}

impl std::fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::fmt::Debug for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tiny(t) => f.debug_tuple("Tiny").field(t).finish(),
            Self::Text(t) => f.debug_tuple("Text").field(t).finish(),
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl std::hash::Hash for SqlIdentifier {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl serde::ser::Serialize for SqlIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de: 'a, 'a> serde::Deserialize<'de> for SqlIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TextVisitor;

        impl<'de> serde::de::Visitor<'de> for TextVisitor {
            type Value = SqlIdentifier;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a byte array")
            }

            fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::SeqAccess<'de>,
            {
                let len = std::cmp::min(visitor.size_hint().unwrap_or(0), 4096);
                let mut bytes = Vec::with_capacity(len);

                while let Some(b) = visitor.next_element()? {
                    bytes.push(b);
                }

                match TinyText::from_slice(&bytes) {
                    Ok(tt) => Ok(SqlIdentifier::Tiny(tt)),
                    _ => Ok(SqlIdentifier::Text(Text::from_slice(&bytes))),
                }
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match TinyText::from_slice(v) {
                    Ok(tt) => Ok(SqlIdentifier::Tiny(tt)),
                    _ => Ok(SqlIdentifier::Text(Text::from_slice(v))),
                }
            }
        }

        deserializer.deserialize_bytes(TextVisitor)
    }
}

impl Arbitrary for SqlIdentifier {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<SqlIdentifier>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use proptest::arbitrary::any;
        use proptest::prelude::*;

        any::<String>().prop_map(Into::into).boxed()
    }
}

#[cfg(test)]
mod tests {
    use launchpad::{eq_laws, hash_laws, ord_laws};
    use test_strategy::proptest;

    use super::SqlIdentifier;

    eq_laws!(SqlIdentifier);
    ord_laws!(SqlIdentifier);
    hash_laws!(SqlIdentifier);

    #[proptest]
    fn serde_roundtrip(val: SqlIdentifier) {
        let ser = bincode::serialize(&val).unwrap();
        let de: SqlIdentifier = bincode::deserialize(&ser).unwrap();
        assert_eq!(val, de);
    }
}
