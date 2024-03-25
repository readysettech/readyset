//! A collection of utilities to make it easier to create [`readyset_util::ranges::BoundedRanges`]
//! of [`DfValue`]s.
pub use readyset_util::ranges::{Bound, BoundedRange};
use vec1::Vec1;

use super::DfValue;

pub trait IntoBoundedRange: Sized {
    /// Maps over `self`, returning a new object with all the values replaced with `value`.
    fn map_value(&self, value: DfValue) -> Self;

    /// Returns a [`BoundedRange`] that includes all non-NULL values greater than (but not equal
    /// to) `self`.
    fn greater_than(self) -> BoundedRange<Self> {
        let upper = self.map_value(DfValue::MAX_VALUE);
        (Bound::Excluded(self), Bound::Excluded(upper))
    }

    /// Returns a [`BoundedRange`] and includes all non-NULL values greater than or equal to `self`.
    fn greater_than_or_equal_to(self) -> BoundedRange<Self> {
        let upper = self.map_value(DfValue::MAX_VALUE);
        (Bound::Included(self), Bound::Excluded(upper))
    }

    /// Returns a [`BoundedRange`] that includes all non-NULL values less than (but not equal
    /// to) `self`.
    fn less_than(self) -> BoundedRange<Self> {
        let lower = self.map_value(DfValue::MIN_VALUE);
        (Bound::Excluded(lower), Bound::Excluded(self))
    }

    /// Returns a [`BoundedRange`] and includes all non-NULL values less than or equal to `self`.
    fn less_than_or_equal_to(self) -> BoundedRange<Self> {
        let lower = self.map_value(DfValue::MIN_VALUE);
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

#[macro_export]
macro_rules! range {
    (=$lower:expr, infinity) => {
        ::readyset_data::IntoBoundedRange::greater_than_or_equal_to($lower)
    };
    ($lower:expr, infinity) => {
        ::readyset_data::IntoBoundedRange::greater_than($lower)
    };
    (-infinity, =$upper:expr) => {
        ::readyset_data::IntoBoundedRange::less_than_or_equal_to($upper)
    };
    (-infinity, $upper:expr) => {
        ::readyset_data::IntoBoundedRange::less_than($upper)
    };
    ($lower:expr, $upper:expr) => {
        (
            ::readyset_data::Bound::Excluded($lower),
            ::readyset_data::Bound::Excluded($upper),
        )
    };
    ($lower:expr, =$upper:expr) => {
        (
            ::readyset_data::Bound::Excluded($lower),
            ::readyset_data::Bound::Included($upper),
        )
    };
    (=$lower:expr, $upper:expr) => {
        (
            ::readyset_data::Bound::Included($lower),
            ::readyset_data::Bound::Excluded($upper),
        )
    };
    (=$lower:expr, =$upper:expr) => {
        (
            ::readyset_data::Bound::Included($lower),
            ::readyset_data::Bound::Included($upper),
        )
    };
}
