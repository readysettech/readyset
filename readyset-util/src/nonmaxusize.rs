//! This is similar to [`std::num::NonZeroUsize`] except the unused value is [`usize::MAX`], which
//! means that it will be used as a [`None`] for a wrapping [`Option`]

use std::ops::Deref;

/// An integer that is known not to equal [`usize::MAX`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
#[rustc_layout_scalar_valid_range_end(18_446_744_073_709_551_614)]
pub struct NonMaxUsize(usize);

impl NonMaxUsize {
    /// Increment the value by one, panicking if it gets to [`usize::MAX`]
    pub fn inc(&mut self) {
        let new_val = self.0 + 1;
        assert_ne!(new_val, usize::MAX);
        // Safe, since we tested it is not the invalid value
        unsafe {
            self.0 = new_val;
        }
    }

    /// Create a new [`NonMaxUsize`] with the value of `0`.
    pub fn zero() -> Self {
        // Safe, since in valid range
        unsafe { NonMaxUsize(0) }
    }
}

impl Deref for NonMaxUsize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use super::NonMaxUsize;

    #[test]
    fn test_usize_max() {
        assert_eq!(
            std::mem::size_of::<NonMaxUsize>(),
            std::mem::size_of::<usize>()
        );
        assert_eq!(
            std::mem::size_of::<Option<NonMaxUsize>>(),
            std::mem::size_of::<usize>()
        );

        assert!(unsafe { Some(NonMaxUsize(usize::MAX)) }.is_none());
        assert!(unsafe { Some(NonMaxUsize(usize::MAX - 1)) }.is_some());
        assert!(unsafe { Some(NonMaxUsize(0)) }.is_some());
    }
}
