//! docs
use std::hash::Hash;
use std::ops::{self, Deref, RangeBounds};
use std::str;

use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

impl<T> RangeBounds<T> for &BoundRange<T> {
    fn start_bound(&self) -> ops::Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> ops::Bound<&T> {
        self.1.as_ref()
    }
}

impl<T> RangeBounds<T> for BoundRange<T> {
    fn start_bound(&self) -> ops::Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> ops::Bound<&T> {
        self.1.as_ref()
    }
}

impl<T> RangeBounds<T> for BoundRange<&T>
where
    T: ?Sized,
{
    fn start_bound(&self) -> ops::Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> ops::Bound<&T> {
        self.1
    }
}

// TODO ethan deduplicate/fix these/remove clones
impl<T> RangeBounds<T> for &BoundRange<&T>
where
    T: ?Sized,
{
    fn start_bound(&self) -> ops::Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> ops::Bound<&T> {
        self.1
    }
}

/// docs
#[derive(Debug, Copy, Hash, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct BoundRange<T>(pub ops::Bound<T>, pub ops::Bound<T>);

impl<T> BoundRange<T> {
    /// docs
    pub fn start_bound(&self) -> ops::Bound<&T> {
        self.0.as_ref()
    }

    /// docs
    pub fn end_bound(&self) -> ops::Bound<&T> {
        self.1.as_ref()
    }

    /// docs
    pub fn as_ref(&self) -> BoundRange<&T> {
        BoundRange(self.0.as_ref(), self.1.as_ref())
    }
}

impl<T> From<(ops::Bound<T>, ops::Bound<T>)> for BoundRange<T> {
    fn from(value: (ops::Bound<T>, ops::Bound<T>)) -> Self {
        BoundRange(value.0, value.1)
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
