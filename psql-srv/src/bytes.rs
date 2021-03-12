use bytes::Bytes;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt;
use std::str;

/// A [`Bytes`] wrapper that always contains a valid utf8 string and can be borrowed as a `&str`.
///
/// [`Bytes`]: https://docs.rs/bytes/0.5.6/bytes/struct.Bytes.html
#[derive(Debug, PartialEq)]
pub struct BytesStr(Bytes);

impl TryFrom<Bytes> for BytesStr {
    type Error = std::str::Utf8Error;

    fn try_from(b: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&b)?;
        Ok(BytesStr(b))
    }
}

impl Borrow<str> for BytesStr {
    fn borrow(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
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

    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_valid_utf8() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo"[..]);
        let s = BytesStr::try_from(buf.freeze());
        assert!(s.is_ok());
    }

    #[test]
    fn test_invalid_utf8() {
        let mut buf = BytesMut::with_capacity(10);
        buf.put(&b"foo\xff"[..]);
        let s = BytesStr::try_from(buf.freeze());
        assert!(s.is_err());
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
