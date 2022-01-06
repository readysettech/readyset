use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;

const TINYTEXT_WIDTH: usize = 14;

/// An optimized storage for very short strings
#[derive(Clone, PartialEq, Eq)]
pub struct TinyText {
    len: u8,
    t: [u8; TINYTEXT_WIDTH],
}

/// A thin pointer over an Arc<[u8]> storing a valid UTF-8 string
#[repr(transparent)]
#[derive(Clone)]
pub struct Text(triomphe::ThinArc<(), u8>);

unsafe impl Send for Text {}

impl TinyText {
    /// Extracts a string slice containing the entire `TinyText`.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: Always safe, because we always validate when constructing
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Extract the underlying slice
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.t[..self.len as usize]
    }

    /// A convinience method to encode the `TinyText` as an `i128`
    pub(crate) fn to_i128(&self) -> i128 {
        let mut b = [0u8; 16];
        b[..TINYTEXT_WIDTH].copy_from_slice(&self.t);
        i128::from_le_bytes(b) | (self.len as i128) << 120
    }

    /// A convinience method to decode the `TinyText` from `i128`
    ///
    /// # Safety
    ///
    /// i128 should have been previousely created using `to_i128`
    /// otherwise the length component might be wrong, or it may
    /// contain non-UTF8 characters.
    pub(crate) unsafe fn from_i128_unchecked(v: i128) -> Self {
        let mut t = [0u8; TINYTEXT_WIDTH];
        let b = v.to_le_bytes();
        t.copy_from_slice(&b[..TINYTEXT_WIDTH]);
        TinyText { len: b[15], t }
    }

    /// A convinience method to create constant ASCII `TinyText`
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

        TinyText { len: i as u8, t }
    }
}

impl TryFrom<&str> for TinyText {
    type Error = ();

    /// If an str can fit inside a `TinyText` returns new `TinyText` with that str
    fn try_from(s: &str) -> Result<Self, ()> {
        if s.len() > TINYTEXT_WIDTH {
            return Err(());
        }

        let b = s.as_bytes();
        let mut t = [0u8; TINYTEXT_WIDTH];
        t[..b.len()].copy_from_slice(b);
        Ok(TinyText {
            len: b.len() as u8,
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
        // SAFETY: Safe because we validate UTF-8 at creation time
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Create a new `Text` by copying a byte slice.
    ///
    /// # Safety
    ///
    /// Does not validate that the slice contains valid UTF-8. The user
    /// must be sure that it does.
    #[inline]
    pub unsafe fn from_slice_unchecked(v: &[u8]) -> Self {
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
        Self(triomphe::ThinArc::from_header_and_iter(
            (),
            t.as_bytes().iter().copied(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::proptest;
    use std::convert::TryInto;

    proptest! {
        #[test]
        fn tiny_i128_round_trip(s in "[a-bA-B0-9]{0,14}") {
            let tt: TinyText = s.as_str().try_into().unwrap();
            assert_eq!(tt.as_str(), s);
            assert_eq!(tt, unsafe{ TinyText::from_i128_unchecked( tt.to_i128())});
        }


        #[test]
        fn text_str_round_trip(s: String) {
            let t: Text = s.as_str().into();
            assert_eq!(t.as_str(), s);
        }
    }
}
