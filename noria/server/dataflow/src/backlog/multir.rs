use ahash::RandomState;
use common::DataType;
use launchpad::intervals::BoundPair;
use noria::consistency::Timestamp;
use noria::KeyComparison;
use reader_map::refs::{Miss, Values};
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::mem;
use std::ops::RangeBounds;
use vec1::{vec1, Vec1};

#[derive(Clone, Debug)]
pub(super) enum Handle {
    Single(reader_map::handles::ReadHandle<DataType, Vec<DataType>, i64, Timestamp, RandomState>),
    Double(
        reader_map::handles::ReadHandle<
            (DataType, DataType),
            Vec<DataType>,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
    Many(
        reader_map::handles::ReadHandle<Vec<DataType>, Vec<DataType>, i64, Timestamp, RandomState>,
    ),
}

pub(super) unsafe fn slice_to_2_tuple<T>(slice: &[T]) -> (T, T) {
    assert_eq!(slice.len(), 2);
    // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
    // we're not guaranteed that they have the same memory layout
    // we *could* just clone DataType, but that would mean dealing with string refcounts
    // so instead, we play a trick where we memcopy onto the stack and then forget!
    //
    // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
    use std::ptr;
    let mut res: (mem::MaybeUninit<T>, mem::MaybeUninit<T>) =
        (mem::MaybeUninit::uninit(), mem::MaybeUninit::uninit());
    ptr::copy_nonoverlapping(&slice[0] as *const T, res.0.as_mut_ptr(), 1);
    ptr::copy_nonoverlapping(&slice[1] as *const T, res.1.as_mut_ptr(), 1);
    core::ptr::read(&res as *const (mem::MaybeUninit<T>, mem::MaybeUninit<T>) as *const (T, T))
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

    /// A double-keyed point query missed
    ///
    /// Second field contains the metadata of the handle
    MissPointDouble((DataType, DataType), i64),

    /// A many-keyed point query missed
    ///
    /// Second field contains the metadata of the handle
    MissPointMany(Vec<DataType>, i64),

    /// A single-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeSingle(Vec<BoundPair<DataType>>, i64),

    /// A double-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeDouble(Vec<BoundPair<(DataType, DataType)>>, i64),

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
                | Self::MissPointDouble(_, _)
                | Self::MissPointMany(_, _)
                | Self::MissRangeSingle(_, _)
                | Self::MissRangeDouble(_, _)
                | Self::MissRangeMany(_, _)
        )
    }

    /// Returns the metadata referenced in this LookupError, if any
    pub fn meta(&self) -> Option<i64> {
        match self {
            LookupError::MissPointSingle(_, meta)
            | LookupError::MissPointDouble(_, meta)
            | LookupError::MissPointMany(_, meta)
            | LookupError::MissRangeSingle(_, meta)
            | LookupError::MissRangeDouble(_, meta)
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
            LookupError::MissPointDouble((x, y), _) => vec![vec1![x, y].into()],
            LookupError::MissPointMany(xs, _) => vec![xs.try_into().unwrap()],
            LookupError::MissRangeSingle(ranges, _) => ranges
                .into_iter()
                .map(|(lower, upper)| (lower.map(|x| vec1![x]), upper.map(|x| vec1![x])).into())
                .collect(),
            LookupError::MissRangeDouble(ranges, _) => ranges
                .into_iter()
                .map(|(lower, upper)| {
                    (
                        lower.map(|(x, y)| vec1![x, y]),
                        upper.map(|(x, y)| vec1![x, y]),
                    )
                        .into()
                })
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
            Handle::Double(ref h) => h.timestamp(),
            Handle::Many(ref h) => h.timestamp(),
        }
    }

    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h) => h.len(),
            Handle::Double(ref h) => h.len(),
            Handle::Many(ref h) => h.len(),
        }
    }

    pub(super) fn keys(&self) -> Vec<Vec<DataType>> {
        match *self {
            Handle::Single(ref h) => h.map_into(|k, _| vec![k.clone()]),
            Handle::Double(ref h) => h.map_into(|(k1, k2), _| vec![k1.clone(), k2.clone()]),
            Handle::Many(ref h) => h.map_into(|ks, _| ks.clone()),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> LookupResult<T>
    where
        F: FnOnce(&Values<Vec<DataType>, RandomState>) -> T,
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
                Ok((then(v), m))
            }
            Handle::Double(ref h) => {
                assert_eq!(key.len(), 2);
                unsafe {
                    let tuple_key = slice_to_2_tuple(key);
                    let map = h.enter().ok_or(NotReady)?;
                    let m = *map.meta();
                    let v = map
                        .get(&tuple_key)
                        .ok_or_else(|| MissPointDouble(tuple_key.clone(), m))?;
                    mem::forget(tuple_key);
                    Ok((then(v), m))
                }
            }
            Handle::Many(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let m = *map.meta();
                let v = map.get(key).ok_or_else(|| MissPointMany(key.into(), m))?;
                Ok((then(v), m))
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
            Handle::Double(ref h) => {
                assert_eq!(key.len(), 2);
                unsafe {
                    let tuple_key = slice_to_2_tuple(key);
                    let map = h.enter()?;
                    let res = map.contains_key(&tuple_key);
                    mem::forget(tuple_key);
                    Some(res)
                }
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
    pub(super) fn meta_get_range_and<F, T, R>(&self, range: &R, mut then: F) -> LookupResult<Vec<T>>
    where
        F: FnMut(&Values<Vec<DataType>, RandomState>) -> T,
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
                Ok((records.map(|(_, row)| then(row)).collect(), meta))
            }
            Handle::Double(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let meta = *map.meta();
                let start_bound = range.start_bound().map(|r| {
                    assert_eq!(r.len(), 2);
                    (r[0].clone(), r[1].clone())
                });
                let end_bound = range.end_bound().map(|r| {
                    assert_eq!(r.len(), 2);
                    (r[0].clone(), r[1].clone())
                });
                let range = (start_bound, end_bound);
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeDouble(misses, meta))?;
                Ok((records.map(|(_, row)| then(row)).collect(), meta))
            }
            Handle::Many(ref h) => {
                let map = h.enter().ok_or(NotReady)?;
                let meta = *map.meta();
                let range = (range.start_bound(), range.end_bound());
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeMany(misses, meta))?;
                Ok((records.map(|(_, row)| then(row)).collect(), meta))
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
            Handle::Double(ref h) => {
                let map = h.enter()?;
                let start_bound = range.start_bound().map(|r| {
                    assert_eq!(r.len(), 2);
                    (r[0].clone(), r[1].clone())
                });
                let end_bound = range.end_bound().map(|r| {
                    assert_eq!(r.len(), 2);
                    (r[0].clone(), r[1].clone())
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
        WriteHandle<DataType, Vec<DataType>, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Single(r))
    }

    fn make_double() -> (
        WriteHandle<(DataType, DataType), Vec<DataType>, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Double(r))
    }

    proptest! {
        #[test]
        fn horrible_cursed_transmute(x: i32, y: i32) {
            unsafe {
                let arg = [x, y];
                let result = slice_to_2_tuple(&arg);
                assert_eq!(result, (x, y));
                mem::forget(result)
            }
        }

        #[test]
        fn get_double(key: (DataType, DataType), val: Vec<DataType>) {
            let (mut w, handle) = make_double();
            w.insert(key.clone(), val.clone());
            w.publish();
            handle.meta_get_and(&[key.0, key.1], |result| {
                assert_eq!(result.into_iter().cloned().collect::<Vec<_>>(), vec![val]);
            }).unwrap();
        }
    }

    #[test]
    fn get_single_range() {
        let (mut w, handle) = make_single();
        w.insert_range(
            (0..10)
                .map(|n| ((n.into()), vec![n.into(), n.into()]))
                .collect(),
            (DataType::from(0))..(DataType::from(10)),
        );
        w.publish();

        let (res, meta) = handle
            .meta_get_range_and(&(vec![2.into()]..=vec![3.into()]), |vals| {
                vals.into_iter().cloned().collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(
            res,
            (2..=3)
                .map(|n| vec![vec![DataType::from(n), DataType::from(n)]])
                .collect::<Vec<_>>()
        );
        assert_eq!(meta, -1);
    }

    #[test]
    fn contains_key_single() {
        let (mut w, handle) = make_single();
        assert_eq!(handle.contains_key(&[1.into()]), None);

        w.publish();
        assert_eq!(handle.contains_key(&[1.into()]), Some(false));

        w.insert(1.into(), vec![1.into()]);
        w.publish();
        assert_eq!(handle.contains_key(&[1.into()]), Some(true));
    }

    #[test]
    fn get_double_range() {
        let (mut w, handle) = make_double();
        w.insert_range(
            (0..10)
                .map(|n| ((n.into(), n.into()), vec![n.into(), n.into()]))
                .collect(),
            (0.into(), 0.into())..(10.into(), 10.into()),
        );
        w.publish();

        let (res, meta) = handle
            .meta_get_range_and(
                &(vec![2.into(), 2.into()]..=vec![3.into(), 3.into()]),
                |vals| vals.into_iter().cloned().collect::<Vec<_>>(),
            )
            .unwrap();
        assert_eq!(
            res,
            (2..=3)
                .map(|n| vec![vec![DataType::from(n), DataType::from(n)]])
                .collect::<Vec<_>>()
        );
        assert_eq!(meta, -1);
    }
}
