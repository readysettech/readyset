use std::borrow::Borrow;
use std::convert::TryFrom;
use std::ops::Deref;
use std::{fmt, str};

use bytes::Bytes;

/// A [`Bytes`] wrapper that always contains a valid utf8 string and can be borrowed as a `&str`.
///
/// [`Bytes`]: https://docs.rs/bytes/0.5.6/bytes/struct.Bytes.html
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BytesStr(Bytes);

impl BytesStr {
    fn as_str(&self) -> &str {
        // SAFETY: BytesStr is always validated on construction (in the TryFrom impl)
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl TryFrom<Bytes> for BytesStr {
    type Error = std::str::Utf8Error;

    fn try_from(b: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&b)?;
        Ok(BytesStr(b))
    }
}

impl Borrow<str> for BytesStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for BytesStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for BytesStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl PartialEq<str> for BytesStr {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl fmt::Display for BytesStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: &str = self.borrow();
        s.fmt(f)
    }
}

#[cfg(test)]
mod tests {

    use bytes::{BufMut, BytesMut};

    use super::*;

    #[test]
    fn test_valid_utf8() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo"[..]);
        let s = BytesStr::try_from(buf.freeze());
        s.unwrap();
    }

    #[test]
    fn test_invalid_utf8() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo\xff"[..]);
        let s = BytesStr::try_from(buf.freeze());
        s.unwrap_err();
    }

    #[test]
    fn test_borrow() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo"[..]);
        let s = BytesStr::try_from(buf.freeze()).unwrap();
        let b: &str = s.borrow();
        assert_eq!(b, "foo");
    }

    #[test]
    fn test_to_string() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo"[..]);
        let s = BytesStr::try_from(buf.freeze()).unwrap();
        assert_eq!(s.to_string(), "foo");
    }
}
