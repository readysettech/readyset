use std::ops::{Bound, RangeBounds};

use common::DfValue;
use launchpad::intervals::BoundPair;
use serde::Serialize;
use tuple::TupleElements;
use vec1::Vec1;

/// An internal type used as the key when performing point lookups and inserts into node state.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PointKey<'a> {
    Single(&'a DfValue),
    Double((DfValue, DfValue)),
    Tri((DfValue, DfValue, DfValue)),
    Quad((DfValue, DfValue, DfValue, DfValue)),
    Quin((DfValue, DfValue, DfValue, DfValue, DfValue)),
    Sex((DfValue, DfValue, DfValue, DfValue, DfValue, DfValue)),
    Multi(Vec<DfValue>),
}

#[allow(clippy::len_without_is_empty)]
impl<'a> PointKey<'a> {
    /// Return the value at the given index within this [`PointKey`].
    ///
    /// # Panics
    ///
    /// * Panics if the index is out-of-bounds
    pub fn get(&self, idx: usize) -> Option<&DfValue> {
        match self {
            PointKey::Single(x) if idx == 0 => Some(x),
            PointKey::Single(_) => None,
            PointKey::Double(x) => TupleElements::get(x, idx),
            PointKey::Tri(x) => TupleElements::get(x, idx),
            PointKey::Quad(x) => TupleElements::get(x, idx),
            PointKey::Quin(x) => TupleElements::get(x, idx),
            PointKey::Sex(x) => TupleElements::get(x, idx),
            PointKey::Multi(arr) => arr.get(idx),
        }
    }

    /// Construct a [`PointKey`] from an iterator of references to [`DfValue`]s. Clones all values
    /// for all but one-element keys.
    ///
    /// # Panics
    ///
    /// * Panics if the iterator returns no results
    #[track_caller]
    pub fn from<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = &'a DfValue>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let mut iter = iter.into_iter();
        let len = iter.len();
        let mut more = move || iter.next().unwrap();
        match len {
            0 => panic!("Empty iterator passed to PointKey::from_iter"),
            1 => PointKey::Single(more()),
            2 => PointKey::Double((more().clone(), more().clone())),
            3 => PointKey::Tri((more().clone(), more().clone(), more().clone())),
            4 => PointKey::Quad((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            5 => PointKey::Quin((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            6 => PointKey::Sex((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            x => PointKey::Multi((0..x).map(|_| more().clone()).collect()),
        }
    }

    /// Return the length of this key
    ///
    /// # Invariants
    ///
    /// This function will never return 0
    pub fn len(&self) -> usize {
        match self {
            PointKey::Single(_) => 1,
            PointKey::Double(_) => 2,
            PointKey::Tri(_) => 3,
            PointKey::Quad(_) => 4,
            PointKey::Quin(_) => 5,
            PointKey::Sex(_) => 6,
            PointKey::Multi(k) => k.len(),
        }
    }

    /// Return true if any of the elements is null
    pub fn has_null(&self) -> bool {
        match self {
            PointKey::Single(e) => e.is_none(),
            PointKey::Double((e0, e1)) => e0.is_none() || e1.is_none(),
            PointKey::Tri((e0, e1, e2)) => e0.is_none() || e1.is_none() || e2.is_none(),
            PointKey::Quad((e0, e1, e2, e3)) => {
                e0.is_none() || e1.is_none() || e2.is_none() || e3.is_none()
            }
            PointKey::Quin((e0, e1, e2, e3, e4)) => {
                e0.is_none() || e1.is_none() || e2.is_none() || e3.is_none() || e4.is_none()
            }
            PointKey::Sex((e0, e1, e2, e3, e4, e5)) => {
                e0.is_none()
                    || e1.is_none()
                    || e2.is_none()
                    || e3.is_none()
                    || e4.is_none()
                    || e5.is_none()
            }
            PointKey::Multi(k) => k.iter().any(DfValue::is_none),
        }
    }
}

#[allow(clippy::type_complexity)]
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub enum RangeKey<'a> {
    /// Key-length-polymorphic double-unbounded range key
    Unbounded,
    Single((Bound<&'a DfValue>, Bound<&'a DfValue>)),
    Double(
        (
            Bound<(&'a DfValue, &'a DfValue)>,
            Bound<(&'a DfValue, &'a DfValue)>,
        ),
    ),
    Tri(
        (
            Bound<(&'a DfValue, &'a DfValue, &'a DfValue)>,
            Bound<(&'a DfValue, &'a DfValue, &'a DfValue)>,
        ),
    ),
    Quad(
        (
            Bound<(&'a DfValue, &'a DfValue, &'a DfValue, &'a DfValue)>,
            Bound<(&'a DfValue, &'a DfValue, &'a DfValue, &'a DfValue)>,
        ),
    ),
    Quin(
        (
            Bound<(
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
            )>,
            Bound<(
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
            )>,
        ),
    ),
    Sex(
        (
            Bound<(
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
            )>,
            Bound<(
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
                &'a DfValue,
            )>,
        ),
    ),
    Multi((Bound<&'a [DfValue]>, Bound<&'a [DfValue]>)),
}

#[allow(clippy::len_without_is_empty)]
impl<'a> RangeKey<'a> {
    /// Build a [`RangeKey`] from a type that implements [`RangeBounds`] over a vector of keys.
    ///
    /// # Panics
    ///
    /// Panics if the lengths of the bounds are different, or if the length is greater than 6
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound::*;
    ///
    /// use dataflow_state::RangeKey;
    /// use readyset_data::DfValue;
    /// use vec1::vec1;
    ///
    /// // Can build RangeKeys from regular range expressions
    /// assert_eq!(RangeKey::from(&(..)), RangeKey::Unbounded);
    /// assert_eq!(
    ///     RangeKey::from(&(vec1![DfValue::from(0)]..vec1![DfValue::from(1)])),
    ///     RangeKey::Single((Included(&(0.into())), Excluded(&(1.into()))))
    /// );
    /// ```
    pub fn from<R>(range: &'a R) -> Self
    where
        R: RangeBounds<Vec1<DfValue>>,
    {
        use Bound::*;
        let len = match (range.start_bound(), range.end_bound()) {
            (Unbounded, Unbounded) => return RangeKey::Unbounded,
            (Included(start) | Excluded(start), Included(end) | Excluded(end)) => {
                assert_eq!(start.len(), end.len());
                start.len()
            }
            (Included(start) | Excluded(start), Unbounded) => start.len(),
            (Unbounded, Included(end) | Excluded(end)) => end.len(),
        };

        macro_rules! make {
            ($variant: ident, |$elem: ident| $make_tuple: expr) => {
                RangeKey::$variant((
                    make!(bound, start_bound, $elem, $make_tuple),
                    make!(bound, end_bound, $elem, $make_tuple),
                ))
            };
            (bound, $bound_type: ident, $elem: ident, $make_tuple: expr) => {
                range.$bound_type().map(|key| {
                    let mut key = key.into_iter();
                    let mut $elem = move || key.next().unwrap();
                    $make_tuple
                })
            };
        }

        match len {
            0 => unreachable!("Vec1 cannot be empty"),
            1 => make!(Single, |elem| elem()),
            2 => make!(Double, |elem| (elem(), elem())),
            3 => make!(Tri, |elem| (elem(), elem(), elem())),
            4 => make!(Quad, |elem| (elem(), elem(), elem(), elem())),
            5 => make!(Quin, |elem| (elem(), elem(), elem(), elem(), elem())),
            6 => make!(Sex, |elem| (elem(), elem(), elem(), elem(), elem(), elem())),
            n => panic!(
                "RangeKey cannot be built from keys of length greater than 6 (got {})",
                n
            ),
        }
    }

    /// Returns the upper bound of the range key
    pub fn upper_bound(&self) -> Bound<Vec<&'a DfValue>> {
        match self {
            RangeKey::Unbounded => Bound::Unbounded,
            RangeKey::Single((_, upper)) => upper.map(|dt| vec![dt]),
            RangeKey::Double((_, upper)) => upper.map(|dts| dts.into_elements().collect()),
            RangeKey::Tri((_, upper)) => upper.map(|dts| dts.into_elements().collect()),
            RangeKey::Quad((_, upper)) => upper.map(|dts| dts.into_elements().collect()),
            RangeKey::Quin((_, upper)) => upper.map(|dts| dts.into_elements().collect()),
            RangeKey::Sex((_, upper)) => upper.map(|dts| dts.into_elements().collect()),
            RangeKey::Multi((_, upper)) => upper.map(|dts| dts.iter().collect()),
        }
    }

    /// Return the length of this range key, or None if the key is Unbounded
    pub fn len(&self) -> Option<usize> {
        match self {
            RangeKey::Unbounded | RangeKey::Multi((Bound::Unbounded, Bound::Unbounded)) => None,
            RangeKey::Single(_) => Some(1),
            RangeKey::Double(_) => Some(2),
            RangeKey::Tri(_) => Some(3),
            RangeKey::Quad(_) => Some(4),
            RangeKey::Quin(_) => Some(5),
            RangeKey::Sex(_) => Some(6),
            RangeKey::Multi(
                (Bound::Included(k), _)
                | (Bound::Excluded(k), _)
                | (_, Bound::Included(k))
                | (_, Bound::Excluded(k)),
            ) => Some(k.len()),
        }
    }
}

impl<'a> RangeKey<'a> {
    pub fn as_bound_pair(&self) -> BoundPair<Vec<DfValue>> {
        match self {
            RangeKey::Unbounded => (Bound::Unbounded, Bound::Unbounded),
            RangeKey::Single((lower, upper)) => (
                lower.map(|dt| vec![dt.clone()]),
                upper.map(|dt| vec![dt.clone()]),
            ),
            RangeKey::Double((lower, upper)) => (
                lower.map(|dts| dts.into_elements().cloned().collect()),
                upper.map(|dts| dts.into_elements().cloned().collect()),
            ),
            RangeKey::Tri((lower, upper)) => (
                lower.map(|dts| dts.into_elements().cloned().collect()),
                upper.map(|dts| dts.into_elements().cloned().collect()),
            ),
            RangeKey::Quad((lower, upper)) => (
                lower.map(|dts| dts.into_elements().cloned().collect()),
                upper.map(|dts| dts.into_elements().cloned().collect()),
            ),
            RangeKey::Quin((lower, upper)) => (
                lower.map(|dts| dts.into_elements().cloned().collect()),
                upper.map(|dts| dts.into_elements().cloned().collect()),
            ),
            RangeKey::Sex((lower, upper)) => (
                lower.map(|dts| dts.into_elements().cloned().collect()),
                upper.map(|dts| dts.into_elements().cloned().collect()),
            ),
            RangeKey::Multi((lower, upper)) => {
                (lower.map(|dts| dts.to_vec()), upper.map(|dts| dts.to_vec()))
            }
        }
    }
}
