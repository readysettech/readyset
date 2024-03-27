//! A collection of utilities to make it easier to create [`readyset_util::ranges::BoundedRanges`]
//! of [`DfValue`]s.
pub use readyset_util::ranges::{Bound, BoundedRange, RangeBounds};
use vec1::Vec1;

use super::DfValue;

/// A trait that eases the creation of [`BoundedRange`]s for its implementors.
pub trait IntoBoundedRange: Sized {
    /// Maps over `self`, returning a new object with all the values replaced with `value`.
    fn map_value(&self, value: DfValue) -> Self;

    /// Returns a [`BoundedRange`] that includes all non-NULL values greater than (but not equal
    /// to) `self`.
    fn range_from(self) -> BoundedRange<Self> {
        let upper = self.map_value(DfValue::MAX);
        (Bound::Excluded(self), Bound::Excluded(upper))
    }

    /// Returns a [`BoundedRange`] that includes all non-NULL values greater than or equal to
    /// `self`.
    fn range_from_inclusive(self) -> BoundedRange<Self> {
        let upper = self.map_value(DfValue::MAX);
        (Bound::Included(self), Bound::Excluded(upper))
    }

    /// Returns a [`BoundedRange`] that includes all non-NULL values less than (but not equal
    /// to) `self`.
    fn range_to(self) -> BoundedRange<Self> {
        let lower = self.map_value(DfValue::MIN);
        (Bound::Excluded(lower), Bound::Excluded(self))
    }

    /// Returns a [`BoundedRange`] and includes all non-NULL values less than or equal to `self`.
    fn range_to_inclusive(self) -> BoundedRange<Self> {
        let lower = self.map_value(DfValue::MIN);
        (Bound::Excluded(lower), Bound::Included(self))
    }
}

impl IntoBoundedRange for Vec1<DfValue> {
    fn map_value(&self, value: DfValue) -> Self {
        self.mapped_ref(|_| value.clone())
    }
}

impl IntoBoundedRange for DfValue {
    fn map_value(&self, value: DfValue) -> Self {
        value
    }
}

impl IntoBoundedRange for (DfValue, DfValue) {
    fn map_value(&self, value: DfValue) -> Self {
        (value.clone(), value)
    }
}

impl IntoBoundedRange for (DfValue, DfValue, DfValue) {
    fn map_value(&self, value: DfValue) -> Self {
        (value.clone(), value.clone(), value)
    }
}

impl IntoBoundedRange for (DfValue, DfValue, DfValue, DfValue) {
    fn map_value(&self, value: DfValue) -> Self {
        (value.clone(), value.clone(), value.clone(), value)
    }
}

impl IntoBoundedRange for (DfValue, DfValue, DfValue, DfValue, DfValue) {
    fn map_value(&self, value: DfValue) -> Self {
        (
            value.clone(),
            value.clone(),
            value.clone(),
            value.clone(),
            value,
        )
    }
}
