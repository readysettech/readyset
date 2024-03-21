//! docs
use std::hash::Hash;
use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

impl<T> RangeBounds<T> for &BoundRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1.as_ref()
    }
}

impl<T> RangeBounds<T> for BoundRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1.as_ref()
    }
}

impl<T> RangeBounds<T> for BoundRange<&T>
where
    T: ?Sized,
{
    fn start_bound(&self) -> Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1
    }
}

// TODO ethan deduplicate/fix these/remove clones
impl<T> RangeBounds<T> for &BoundRange<&T>
where
    T: ?Sized,
{
    fn start_bound(&self) -> Bound<&T> {
        self.0
    }

    fn end_bound(&self) -> Bound<&T> {
        self.1
    }
}

/// docs
#[derive(Debug, Copy, Hash, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct BoundRange<T>(pub Bound<T>, pub Bound<T>);

impl<T> BoundRange<T> {
    /// docs
    pub fn start_bound(&self) -> Bound<&T> {
        self.0.as_ref()
    }

    /// docs
    pub fn end_bound(&self) -> Bound<&T> {
        self.1.as_ref()
    }

    /// docs
    pub fn as_ref(&self) -> BoundRange<&T> {
        BoundRange(self.0.as_ref(), self.1.as_ref())
    }
}
