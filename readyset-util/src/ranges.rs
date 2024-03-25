//! Utilities for dealing with ranges and bounds.
use std::hash::Hash;
use std::ops::{self, Deref};
use std::str;

use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use quickcheck::Gen;
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

impl<T> RangeBounds<T> for (Bound<T>, Bound<T>) {
    fn start_bound(&self) -> Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1.as_ref()
    }
}

impl<T: ?Sized> RangeBounds<T> for (Bound<&T>, Bound<&T>) {
    fn start_bound(&self) -> Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1
    }
}

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

/// docs
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

// TODO ethan
/// docs
pub type BoundedRange<T> = (Bound<T>, Bound<T>);

/// docs
#[derive(Copy, Debug, Hash, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum Bound<T> {
    /// docs
    Included(T),
    /// docs
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
    /// docs
    pub fn inner(&self) -> &T {
        match self {
            Bound::Included(b) => b,
            Bound::Excluded(b) => b,
        }
    }

    /// docs
    pub fn as_mut(&mut self) -> Bound<&mut T> {
        match *self {
            Bound::Included(ref mut b) => Bound::Included(b),
            Bound::Excluded(ref mut b) => Bound::Excluded(b),
        }
    }

    /// docs
    pub fn map<U, F>(self, f: F) -> Bound<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Bound::Included(b) => Bound::Included(f(b)),
            Bound::Excluded(b) => Bound::Excluded(f(b)),
        }
    }

    /// docs
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
    /// docs
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
    /// docs
    pub fn len(&self) -> usize {
        self.inner().len()
    }

    /// docs
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
