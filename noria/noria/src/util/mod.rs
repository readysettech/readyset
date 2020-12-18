//! Utilities used across noria

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
    fn map<F, B>(self, f: F) -> std::ops::Bound<B>
    where
        F: FnMut(Self::Inner) -> B;
}

impl<A> BoundFunctor for std::ops::Bound<A> {
    type Inner = A;
    fn map<F, B>(self, mut f: F) -> std::ops::Bound<B>
    where
        F: FnMut(Self::Inner) -> B,
    {
        use std::ops::Bound::*;
        match self {
            Included(a) => Included(f(a)),
            Excluded(a) => Excluded(f(a)),
            Unbounded => Unbounded,
        }
    }
}
