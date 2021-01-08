//! Utilities used across noria

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Bound;

pub mod arbitrary;

/// Allow `map` on a [`Bound`]
///
/// please sir, can I have some GATs?
pub trait BoundFunctor {
    /// The type parameter for this bound
    type Inner;

    /// Map a function over the endpoint of this bound
    ///
    /// ```
    /// use noria::util::BoundFunctor;
    /// use std::ops::Bound::*;
    ///
    /// assert_eq!(Included(1).map(|x: i32| x + 1), Included(2));
    /// assert_eq!(Excluded(1).map(|x: i32| x + 1), Excluded(2));
    /// assert_eq!(Unbounded.map(|x: i32| x + 1), Unbounded);
    /// ```
    fn map<F, B>(self, f: F) -> Bound<B>
    where
        F: FnMut(Self::Inner) -> B;
}

impl<A> BoundFunctor for Bound<A> {
    type Inner = A;
    fn map<F, B>(self, mut f: F) -> Bound<B>
    where
        F: FnMut(Self::Inner) -> B,
    {
        use Bound::*;
        match self {
            Included(a) => Included(f(a)),
            Excluded(a) => Excluded(f(a)),
            Unbounded => Unbounded,
        }
    }
}

/// Provide `as_ref` for [`Bound`]
pub trait BoundAsRef<A> {
    /// Convert a [`&Bound<A>`] into a `Bound<&A>`
    fn as_ref(&self) -> Bound<&A>;
}

impl<A> BoundAsRef<A> for Bound<A> {
    fn as_ref(&self) -> Bound<&A> {
        use Bound::*;
        match self {
            Unbounded => Unbounded,
            Included(ref x) => Included(x),
            Excluded(ref x) => Excluded(x),
        }
    }
}

/// Calculate the hash of `x` using the [`DefaultHasher`]
pub fn hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish()
}
