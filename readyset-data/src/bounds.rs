use std::ops;

pub use readyset_util::ranges::BoundRange;
use tuple::TupleElements;
use vec1::Vec1;

use super::DfValue;

pub trait IntoBoundRange: Sized {
    fn map_value(&self, value: DfValue) -> Self;

    fn greater_than(self) -> BoundRange<Self> {
        let upper = self.map_value(DfValue::MAX_VALUE);
        BoundRange(ops::Bound::Excluded(self), ops::Bound::Excluded(upper))
    }

    fn greater_than_or_equal_to(self) -> BoundRange<Self> {
        let upper = self.map_value(DfValue::MAX_VALUE);
        BoundRange(ops::Bound::Excluded(self), ops::Bound::Included(upper))
    }

    fn less_than(self) -> BoundRange<Self> {
        let lower = self.map_value(DfValue::MIN_VALUE);
        BoundRange(ops::Bound::Excluded(lower), ops::Bound::Excluded(self))
    }

    fn less_than_or_equal_to(self) -> BoundRange<Self> {
        let lower = self.map_value(DfValue::MIN_VALUE);
        BoundRange(ops::Bound::Included(lower), ops::Bound::Excluded(self))
    }
}

impl IntoBoundRange for Vec1<DfValue> {
    fn map_value(&self, value: DfValue) -> Self {
        self.mapped_ref(|_| value.clone())
    }
}

impl IntoBoundRange for DfValue {
    fn map_value(&self, value: DfValue) -> Self {
        value
    }
}

// TODO ethan performance?
macro_rules! impl_into_bound_pair_tuple {
    ($tuple:ty) => {
        impl IntoBoundRange for $tuple {
            fn map_value(&self, value: DfValue) -> Self {
                <$tuple>::from_iter(self.elements().map(|_| value.clone())).unwrap()
            }
        }
    };
}

impl_into_bound_pair_tuple!((DfValue,));
impl_into_bound_pair_tuple!((DfValue, DfValue));
impl_into_bound_pair_tuple!((DfValue, DfValue, DfValue));
impl_into_bound_pair_tuple!((DfValue, DfValue, DfValue, DfValue));
impl_into_bound_pair_tuple!((DfValue, DfValue, DfValue, DfValue, DfValue));
