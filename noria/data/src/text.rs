use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

const TINYTEXT_WIDTH: usize = 14;

/// An optimized storage for very short strings
#[derive(Clone, PartialEq, Eq)]
pub struct TinyText {
    len: u8,
    t: [u8; TINYTEXT_WIDTH],
}

/// A thin pointer over an Arc<[u8]> with lazy UTF-8 validation
#[repr(transparent)]
#[derive(Clone)]
pub struct Text(triomphe::ThinArc<AtomicBool, u8>);

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

    /// Create a new `TinyText` by copying a byte slice.
    /// Errors if slice is too long.
    ///
    /// # Panics
    ///
    /// Panics if not valid UTF-8.
    #[inline]
    pub fn from_slice(v: &[u8]) -> Result<Self, &'static str> {
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
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.slice
    }

    /// Returns the underlying byte slice as an `str`
    #[inline]
    pub fn as_str(&self) -> &str {
        // Check if already validated
        if self.0.header.header.load(Relaxed) {
            // SAFETY: Safe because we checked validation flag
            unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
        } else {
            let validated = std::str::from_utf8(self.as_bytes()).expect("Must always be UTF8");
            self.0.header.header.store(true, Relaxed);
            validated
        }
    }

    /// Create a new `Text` by copying a byte slice. It does not check if the
    /// slice contains valid UTF-8 text, and may panic later if `as_str` is
    /// called later if it does not.
    #[inline]
    pub fn from_slice_unchecked(v: &[u8]) -> Self {
        Self(triomphe::ThinArc::from_header_and_slice(
            AtomicBool::new(false),
            v,
        ))
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
            AtomicBool::new(true),
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
        fn tiny_str_round_trip(s in "[a-bA-B0-9]{0,14}") {
            let tt: TinyText = s.as_str().try_into().unwrap();
            assert_eq!(tt.as_str(), s);
        }

        #[test]
        fn text_str_round_trip(s: String) {
            let t: Text = s.as_str().into();
            assert_eq!(t.as_str(), s);
        }
    }

    #[test]
    #[should_panic]
    fn text_panics_non_utf8() {
        let s = [255, 255, 255, 255];
        let t = Text::from_slice_unchecked(&s);
        t.as_str();
    }

    #[test]
    #[should_panic]
    fn tiny_text_panics_non_utf8() {
        let s = [255, 255, 255, 255];
        TinyText::from_slice(&s).expect("ok");
    }
}
