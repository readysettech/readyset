//! Utilities for dealing with ranges and bounds.
use std::hash::Hash;
use std::ops::{self, Deref, Range, RangeInclusive};
use std::str;

use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use quickcheck::Gen;
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents a *bounded* range, encapsulating a lower and upper [`Bound`]. See the documentation
/// for [`Bound`] for more information.
pub type BoundedRange<T> = (Bound<T>, Bound<T>);

/// A trait that mirrors the functionality of [`std::ops::RangeBounds`] but whose bounds are never
/// unbounded.
pub trait RangeBounds<T: ?Sized> {
    /// Returns the start bound of the range.
    fn start_bound(&self) -> Bound<&T>;

    /// Returns the end bound of the range.
    fn end_bound(&self) -> Bound<&T>;

    /// Returns `self` represented as a tuple of [`std::ops::Bound`]s.
    fn as_std_range(&self) -> (ops::Bound<&T>, ops::Bound<&T>) {
        (self.start_bound().into(), self.end_bound().into())
    }

    /// Returns true only if the range contains the given item.
    fn contains<U>(&self, item: &U) -> bool
    where
        T: PartialOrd<U>,
        U: ?Sized + PartialOrd<T>,
    {
        (match self.start_bound() {
            Bound::Included(start) => start <= item,
            Bound::Excluded(start) => start < item,
        }) && (match self.end_bound() {
            Bound::Included(end) => item <= end,
            Bound::Excluded(end) => item < end,
        })
    }
}

impl<T> RangeBounds<T> for Range<T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Included(&self.start)
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Excluded(&self.end)
    }
}

impl<T> RangeBounds<T> for RangeInclusive<T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Included(self.start())
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Included(self.end())
    }
}

impl<T> RangeBounds<T> for BoundedRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1.as_ref()
    }
}

impl<T: ?Sized> RangeBounds<T> for BoundedRange<&T> {
    fn start_bound(&self) -> Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1
    }
}

/// A type similar to [`std::ops::Bound`] that can never be unbounded. Its primary use case is to
/// represent bounds in ranges that are bounded on both sides. Ranges that are unbounded from
/// above and/or below would not be representable using this type.
#[derive(Copy, Debug, Hash, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum Bound<T> {
    /// A bound that includes the wrapped value
    Included(T),
    /// A bound that excludes the wrapped value
    Excluded(T),
}

impl<T> Arbitrary for Bound<T>
where
    T: Arbitrary + 'static,
    T::Parameters: Clone,
{
    type Parameters = T::Parameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any_with::<T>(args.clone()).prop_map(Bound::Included),
            any_with::<T>(args).prop_map(Bound::Excluded)
        ]
        .boxed()
    }
}

impl<T> Bound<T> {
    /// Returns a mutable reference to the value wrapped by the [`Bound`].
    pub fn inner_mut(&mut self) -> &mut T {
        match *self {
            Bound::Included(ref mut b) => b,
            Bound::Excluded(ref mut b) => b,
        }
    }

    /// Applies the given function to the value wrapped by the [`Bound`], returning the
    /// newly-created [`Bound`].
    pub fn map<U, F>(self, f: F) -> Bound<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Bound::Included(b) => Bound::Included(f(b)),
            Bound::Excluded(b) => Bound::Excluded(f(b)),
        }
    }

    /// Converts from `&Bound<T>` to `Bound<&T>`.
    pub fn as_ref(&self) -> Bound<&T> {
        match self {
            Bound::Excluded(b) => Bound::Excluded(b),
            Bound::Included(b) => Bound::Included(b),
        }
    }
}

impl<T> Bound<&T>
where
    T: Clone,
{
    /// Converts from `Bound<&T>` to `Bound<T>` by cloning the value wrapped by the [`Bound`].
    pub fn cloned(&self) -> Bound<T> {
        match *self {
            Bound::Excluded(b) => Bound::Excluded(b.clone()),
            Bound::Included(b) => Bound::Included(b.clone()),
        }
    }
}

impl<T> From<Bound<T>> for ops::Bound<T> {
    fn from(value: Bound<T>) -> Self {
        match value {
            Bound::Included(b) => ops::Bound::Included(b),
            Bound::Excluded(b) => ops::Bound::Excluded(b),
        }
    }
}

impl<T, I> Bound<T>
where
    T: Deref<Target = [I]>,
{
    /// Returns the length of the value wrapped by the [`Bound`].
    pub fn len(&self) -> usize {
        match self {
            Bound::Included(b) | Bound::Excluded(b) => b.len(),
        }
    }

    /// Returns true only if the length of the value wrapped by the [`Bound`] is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The error returned after a failed conversion from [`std::ops::Bound`] to [`Bound`]. This error
/// only occurs if [`std::ops::Bound`] is unbounded.
#[derive(Error, Debug)]
#[error("Cannot convert std::ops::Bound::Unbounded to readyset_data::Bound")]
pub struct BoundConversionError;

impl<T> TryFrom<ops::Bound<T>> for Bound<T> {
    type Error = BoundConversionError;

    fn try_from(value: ops::Bound<T>) -> Result<Self, Self::Error> {
        match value {
            ops::Bound::Included(b) => Ok(Bound::Included(b)),
            ops::Bound::Excluded(b) => Ok(Bound::Excluded(b)),
            ops::Bound::Unbounded => Err(BoundConversionError),
        }
    }
}

impl<T: quickcheck::Arbitrary> quickcheck::Arbitrary for Bound<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> Bound<T> {
        if g.gen::<bool>() {
            Bound::Included(T::arbitrary(g))
        } else {
            Bound::Excluded(T::arbitrary(g))
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Bound<T>>> {
        match *self {
            Bound::Included(ref x) => Box::new(x.shrink().map(Bound::Included)),
            Bound::Excluded(ref x) => Box::new(x.shrink().map(Bound::Excluded)),
        }
    }
}
