use std::ops::{Bound, RangeBounds};

use common::DfValue;
use derive_more::From;
use launchpad::intervals::BoundPair;
use serde::ser::{SerializeSeq, SerializeTuple};
use serde::Serialize;
use tuple::TupleElements;
use vec1::Vec1;

/// An internal type used as the key when performing point lookups and inserts into node state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, From)]
pub enum PointKey {
    Single(DfValue),
    Double((DfValue, DfValue)),
    Tri((DfValue, DfValue, DfValue)),
    Quad((DfValue, DfValue, DfValue, DfValue)),
    Quin((DfValue, DfValue, DfValue, DfValue, DfValue)),
    Sex((DfValue, DfValue, DfValue, DfValue, DfValue, DfValue)),
    Multi(Box<[DfValue]>),
}

#[allow(clippy::len_without_is_empty)]
impl PointKey {
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
        I: IntoIterator<Item = DfValue>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let mut iter = iter.into_iter();
        let len = iter.len();
        let mut more = move || iter.next().unwrap();
        match len {
            0 => panic!("Empty iterator passed to PointKey::from_iter"),
            1 => PointKey::Single(more()),
            2 => PointKey::Double((more(), more())),
            3 => PointKey::Tri((more(), more(), more())),
            4 => PointKey::Quad((more(), more(), more(), more())),
            5 => PointKey::Quin((more(), more(), more(), more(), more())),
            6 => PointKey::Sex((more(), more(), more(), more(), more(), more())),
            x => PointKey::Multi((0..x).map(|_| more()).collect()),
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

impl Serialize for PointKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        macro_rules! serialize_val {
            ($ser: ident, $v: ident) => {{
                let val = $v.transform_for_serialized_key();
                match val.as_str() {
                    Some(s) => $ser.serialize_element(s)?, // Don't serialize collation
                    None => $ser.serialize_element(val.as_ref())?,
                }
            }};
        }

        macro_rules! serialize {
            ($count: expr, $($v:ident),+) => {{
                let mut tup = serializer.serialize_tuple($count)?;
                $({ serialize_val!(tup, $v) })+
                tup.end()
            }};
        }

        match self {
            PointKey::Single(v) => v.transform_for_serialized_key().serialize(serializer),
            PointKey::Double((v1, v2)) => serialize!(2, v1, v2),
            PointKey::Tri((v1, v2, v3)) => serialize!(3, v1, v2, v3),
            PointKey::Quad((v1, v2, v3, v4)) => serialize!(4, v1, v2, v3, v4),
            PointKey::Quin((v1, v2, v3, v4, v5)) => serialize!(5, v1, v2, v3, v4, v5),
            PointKey::Sex((v1, v2, v3, v4, v5, v6)) => serialize!(6, v1, v2, v3, v4, v5, v6),
            PointKey::Multi(vs) => {
                let mut seq = serializer.serialize_seq(Some(vs.len()))?;
                for v in vs.iter() {
                    serialize_val!(seq, v);
                }
                seq.end()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub enum RangeKey {
    /// Key-length-polymorphic double-unbounded range key
    Unbounded,
    Single(BoundPair<DfValue>),
    Double(BoundPair<(DfValue, DfValue)>),
    Tri(BoundPair<(DfValue, DfValue, DfValue)>),
    Quad(BoundPair<(DfValue, DfValue, DfValue, DfValue)>),
    Quin(BoundPair<(DfValue, DfValue, DfValue, DfValue, DfValue)>),
    Sex(BoundPair<(DfValue, DfValue, DfValue, DfValue, DfValue, DfValue)>),
    Multi(BoundPair<Box<[DfValue]>>),
}

#[allow(clippy::len_without_is_empty)]
impl RangeKey {
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
    ///     RangeKey::Single((Included(0.into()), Excluded(1.into())))
    /// );
    /// ```
    pub fn from<R>(range: &R) -> Self
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
                    let mut $elem = move || key.next().unwrap().clone();
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
    pub fn upper_bound(&self) -> Bound<Vec<&DfValue>> {
        match self {
            RangeKey::Unbounded => Bound::Unbounded,
            RangeKey::Single((_, upper)) => upper.as_ref().map(|dt| vec![dt]),
            RangeKey::Double((_, upper)) => upper.as_ref().map(|dts| dts.elements().collect()),
            RangeKey::Tri((_, upper)) => upper.as_ref().map(|dts| dts.elements().collect()),
            RangeKey::Quad((_, upper)) => upper.as_ref().map(|dts| dts.elements().collect()),
            RangeKey::Quin((_, upper)) => upper.as_ref().map(|dts| dts.elements().collect()),
            RangeKey::Sex((_, upper)) => upper.as_ref().map(|dts| dts.elements().collect()),
            RangeKey::Multi((_, upper)) => upper.as_ref().map(|dts| dts.iter().collect()),
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

    /// Convert this [`RangeKey`] into a pair of bounds on [`PointKey`]s, for use during
    /// serialization of lookup keys for ranges
    pub(crate) fn into_point_keys(self) -> BoundPair<PointKey> {
        use Bound::*;

        macro_rules! point_keys {
            ($r:ident, $variant:ident) => {
                ($r.0.map(PointKey::$variant), $r.1.map(PointKey::$variant))
            };
        }

        match self {
            RangeKey::Unbounded => (Unbounded, Unbounded),
            RangeKey::Single((l, u)) => (l.map(PointKey::Single), u.map(PointKey::Single)),
            RangeKey::Double(r) => point_keys!(r, Double),
            RangeKey::Tri(r) => point_keys!(r, Tri),
            RangeKey::Quad(r) => point_keys!(r, Quad),
            RangeKey::Quin(r) => point_keys!(r, Quin),
            RangeKey::Sex(r) => point_keys!(r, Sex),
            RangeKey::Multi(r) => point_keys!(r, Multi),
        }
    }

    pub fn as_bound_pair(&self) -> BoundPair<Vec<DfValue>> {
        fn as_bound_pair<T>(bound_pair: &BoundPair<T>) -> BoundPair<Vec<DfValue>>
        where
            T: TupleElements<Element = DfValue>,
        {
            (
                bound_pair
                    .0
                    .as_ref()
                    .map(|v| v.elements().cloned().collect()),
                bound_pair
                    .1
                    .as_ref()
                    .map(|v| v.elements().cloned().collect()),
            )
        }

        match self {
            RangeKey::Unbounded => (Bound::Unbounded, Bound::Unbounded),
            RangeKey::Single((lower, upper)) => (
                lower.as_ref().map(|dt| vec![dt.clone()]),
                upper.as_ref().map(|dt| vec![dt.clone()]),
            ),
            RangeKey::Double(bp) => as_bound_pair(bp),
            RangeKey::Tri(bp) => as_bound_pair(bp),
            RangeKey::Quad(bp) => as_bound_pair(bp),
            RangeKey::Quin(bp) => as_bound_pair(bp),
            RangeKey::Sex(bp) => as_bound_pair(bp),
            RangeKey::Multi((lower, upper)) => (
                lower.as_ref().map(|dts| dts.to_vec()),
                upper.as_ref().map(|dts| dts.to_vec()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_data::Collation;

    use super::*;

    #[test]
    fn single_point_key_serialize_normalizes_citext() {
        assert_eq!(
            bincode::serialize(&PointKey::Single(DfValue::from_str_and_collation(
                "AbC",
                Collation::Citext
            )))
            .unwrap(),
            bincode::serialize(&PointKey::Single(DfValue::from("abc"))).unwrap(),
        )
    }

    #[test]
    fn double_point_key_serialize_normalizes_citext() {
        assert_eq!(
            bincode::serialize(&PointKey::Double((
                DfValue::from(1),
                DfValue::from_str_and_collation("AbC", Collation::Citext)
            )))
            .unwrap(),
            bincode::serialize(&PointKey::Double((DfValue::from(1), DfValue::from("abc"))))
                .unwrap(),
        )
    }

    #[test]
    fn multi_point_key_serialize_normalizes_citext() {
        assert_eq!(
            bincode::serialize(&PointKey::Multi(Box::new([
                DfValue::from(1),
                DfValue::from(2),
                DfValue::from(3),
                DfValue::from(4),
                DfValue::from(5),
                DfValue::from(6),
                DfValue::from(7),
                DfValue::from_str_and_collation("AbC", Collation::Citext)
            ])))
            .unwrap(),
            bincode::serialize(&PointKey::Multi(Box::new([
                DfValue::from(1),
                DfValue::from(2),
                DfValue::from(3),
                DfValue::from(4),
                DfValue::from(5),
                DfValue::from(6),
                DfValue::from(7),
                DfValue::from_str_and_collation("abc", Collation::Utf8)
            ])))
            .unwrap(),
        );
    }
}
