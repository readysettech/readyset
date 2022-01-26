use ahash::RandomState;
use common::DataType;
use launchpad::intervals::BoundPair;
use noria::consistency::Timestamp;
use noria::KeyComparison;
use reader_map::refs::{Miss, ValuesIter};
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::ops::RangeBounds;
use vec1::{vec1, Vec1};

#[derive(Clone, Debug)]
pub(super) enum Handle {
    Single(reader_map::handles::ReadHandle<DataType, Box<[DataType]>, i64, Timestamp, RandomState>),
    Many(
        reader_map::handles::ReadHandle<
            Vec<DataType>,
            Box<[DataType]>,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
}

/// An error that could occur during an equality or range lookup to a reader node.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Hash)]
pub enum LookupError {
    /// The map is not ready to accept queries
    NotReady,

    /// A single-keyed point query missed
    ///
    /// Second field contains the metadata of the handle
    MissPointSingle(DataType, i64),

    /// A many-keyed point query missed
    ///
    /// Second field contains the metadata of the handle
    MissPointMany(Vec<DataType>, i64),

    /// A single-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeSingle(Vec<BoundPair<DataType>>, i64),

    /// A many-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeMany(Vec<BoundPair<Vec<DataType>>>, i64),
}

impl LookupError {
    /// Returns `true` if this `LookupError` represents a miss on a key, `false` if it represents a
    /// not-ready map
    pub fn is_miss(&self) -> bool {
        matches!(
            self,
            Self::MissPointSingle(_, _)
                | Self::MissPointMany(_, _)
                | Self::MissRangeSingle(_, _)
                | Self::MissRangeMany(_, _)
        )
    }

    /// Returns the metadata referenced in this LookupError, if any
    pub fn meta(&self) -> Option<i64> {
        match self {
            LookupError::MissPointSingle(_, meta)
            | LookupError::MissPointMany(_, meta)
            | LookupError::MissRangeSingle(_, meta)
            | LookupError::MissRangeMany(_, meta) => Some(*meta),
            _ => None,
        }
    }

    /// Returns the misses from this LookupError as a list of KeyComparisons. If this LookupError is
    /// NotReady, the result will be empty
    pub fn into_misses(self) -> Vec<KeyComparison> {
        match self {
            LookupError::NotReady => vec![],
            LookupError::MissPointSingle(x, _) => vec![vec1![x].into()],
            LookupError::MissPointMany(xs, _) => vec![xs.try_into().unwrap()],
            LookupError::MissRangeSingle(ranges, _) => ranges
                .into_iter()
                .map(|(lower, upper)| (lower.map(|x| vec1![x]), upper.map(|x| vec1![x])).into())
                .collect(),
            LookupError::MissRangeMany(ranges, _) => ranges
                .into_iter()
                .map(|(lower, upper)| {
                    (
                        lower.map(|x| Vec1::try_from(x).unwrap()),
                        upper.map(|x| Vec1::try_from(x).unwrap()),
                    )
                        .into()
                })
                .collect(),
        }
    }
}

/// The result of an equality or range lookup to the reader node.
pub type LookupResult<T> = Result<(T, i64), LookupError>;

impl Handle {
    pub(super) fn timestamp(&self) -> Option<Timestamp> {
        match *self {
            Handle::Single(ref h) => h.timestamp(),
            Handle::Many(ref h) => h.timestamp(),
        }
    }

    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h) => h.len(),
            Handle::Many(ref h) => h.len(),
        }
    }

    pub(super) fn keys(&self) -> Vec<Vec<DataType>> {
        match *self {
            Handle::Single(ref h) => h.map_into(|k, _| vec![k.clone()]),
            Handle::Many(ref h) => h.map_into(|ks, _| ks.clone()),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> LookupResult<T>
    where
        F: FnOnce(ValuesIter<'_, Box<[DataType]>, RandomState>) -> T,
    {
        use LookupError::*;

        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                let map = h.enter().ok_or(NotReady)?;
                let m = *map.meta();
                let v = map
                    .get(&key[0])
                    .ok_or_else(|| MissPointSingle(key[0].clone(), m))?;
                Ok((then(v.iter()), m))
            }
            Handle::Many(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let m = *map.meta();
                let v = map.get(key).ok_or_else(|| MissPointMany(key.into(), m))?;
                Ok((then(v.iter()), m))
            }
        }
    }

    /// Returns None if this handle is not ready, Some(true) if this handle contains the given
    /// key, Some(false) if it doesn't
    ///
    /// This is equivalent to testing if `meta_get_and` returns an Err other than `NotReady`
    pub(super) fn contains_key(&self, key: &[DataType]) -> Option<bool> {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                let map = h.enter()?;
                Some(map.contains_key(&key[0]))
            }
            Handle::Many(ref h) => {
                let map = h.enter()?;
                Some(map.contains_key(key))
            }
        }
    }

    /// Retrieve the values corresponding to the given range of keys, apply `then` to them, and
    /// return the results, along with the metadata
    ///
    /// # Panics
    ///
    /// Panics if the vectors in the bounds of `range` are a different size than the length of our
    /// keys.
    pub(super) fn meta_get_range_and<F, T, R>(&self, range: &R, then: F) -> LookupResult<Vec<T>>
    where
        F: Fn(ValuesIter<'_, Box<[DataType]>, RandomState>) -> T,
        R: RangeBounds<Vec<DataType>>,
    {
        use LookupError::*;

        match *self {
            Handle::Single(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let meta = *map.meta();
                let start_bound = range.start_bound().map(|v| {
                    assert!(v.len() == 1);
                    &v[0]
                });
                let end_bound = range.end_bound().map(|v| {
                    assert!(v.len() == 1);
                    &v[0]
                });
                let range = (start_bound, end_bound);
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeSingle(misses, meta))?;
                Ok((records.map(|(_, row)| then(row.iter())).collect(), meta))
            }
            Handle::Many(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let meta = *map.meta();
                let range = (range.start_bound(), range.end_bound());
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeMany(misses, meta))?;
                Ok((records.map(|(_, row)| then(row.iter())).collect(), meta))
            }
        }
    }

    /// Returns None if this handle is not ready, Some(true) if this handle fully contains the given
    /// key range, Some(false) if any of the keys miss
    ///
    /// This is equivalent to testing if `meta_get_and` returns an Err other than `NotReady`
    pub(super) fn contains_range<R>(&self, range: &R) -> Option<bool>
    where
        R: RangeBounds<Vec<DataType>>,
    {
        match *self {
            Handle::Single(ref h) => {
                let map = h.enter()?;
                let start_bound = range.start_bound().map(|v| {
                    assert!(v.len() == 1);
                    &v[0]
                });
                let end_bound = range.end_bound().map(|v| {
                    assert!(v.len() == 1);
                    &v[0]
                });
                Some(map.contains_range(&(start_bound, end_bound)))
            }
            Handle::Many(ref h) => {
                let map = h.enter()?;
                Some(map.contains_range(&(range.start_bound(), range.end_bound())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use reader_map::handles::WriteHandle;

    fn make_single() -> (
        WriteHandle<DataType, Box<[DataType]>, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Single(r))
    }

    fn make_many() -> (
        WriteHandle<Vec<DataType>, Box<[DataType]>, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Many(r))
    }

    proptest! {
        #[test]
        fn get_double(key: [DataType; 2], val: Box<[DataType]>) {
            let (mut w, handle) = make_many();
            w.insert(key.to_vec(), val.clone());
            w.publish();
            handle.meta_get_and(&key[..], |result| {
                assert!(result.eq([&val]));
            }).unwrap();
        }
    }

    #[test]
    fn get_single_range() {
        let (mut w, handle) = make_single();
        w.insert_range(
            (0i32..10)
                .map(|n| ((n.into()), vec![n.into(), n.into()].into_boxed_slice()))
                .collect(),
            (DataType::from(0i32))..(DataType::from(10i32)),
        );
        w.publish();

        let (res, meta) = handle
            .meta_get_range_and(&(vec![2i32.into()]..=vec![3i32.into()]), |vals| {
                vals.into_iter().cloned().collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(
            res,
            (2i32..=3)
                .map(|n| vec![vec![DataType::from(n), DataType::from(n)].into_boxed_slice()])
                .collect::<Vec<_>>()
        );
        assert_eq!(meta, -1);
    }

    #[test]
    fn contains_key_single() {
        let (mut w, handle) = make_single();
        assert_eq!(handle.contains_key(&[1i32.into()]), None);

        w.publish();
        assert_eq!(handle.contains_key(&[1i32.into()]), Some(false));

        w.insert(1i32.into(), vec![1i32.into()].into_boxed_slice());
        w.publish();
        assert_eq!(handle.contains_key(&[1i32.into()]), Some(true));
    }

    #[test]
    fn get_double_range() {
        let (mut w, handle) = make_many();
        w.insert_range(
            (0..10)
                .map(|n: i32| {
                    (
                        vec![n.into(), n.into()],
                        vec![n.into(), n.into()].into_boxed_slice(),
                    )
                })
                .collect(),
            vec![0i32.into(), 0i32.into()]..vec![10i32.into(), 10i32.into()],
        );
        w.publish();

        let (res, meta) = handle
            .meta_get_range_and(
                &(vec![2i32.into(), 2i32.into()]..=vec![3i32.into(), 3i32.into()]),
                |vals| vals.into_iter().cloned().collect::<Vec<_>>(),
            )
            .unwrap();
        assert_eq!(
            res,
            (2i32..=3)
                .map(|n: i32| vec![vec![DataType::from(n), DataType::from(n)].into_boxed_slice()])
                .collect::<Vec<_>>()
        );
        assert_eq!(meta, -1);
    }
}
