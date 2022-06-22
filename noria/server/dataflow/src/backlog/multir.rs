use std::convert::{TryFrom, TryInto};
use std::ops::RangeBounds;

use ahash::RandomState;
use common::DataType;
use dataflow_expression::PreInsertion;
use launchpad::intervals::BoundPair;
use noria::consistency::Timestamp;
use noria::results::{SharedResults, SharedRows};
use noria::KeyComparison;
use noria_errors::ReadySetError;
use reader_map::refs::Miss;
use serde::{Deserialize, Serialize};
use vec1::{vec1, Vec1};

#[derive(Clone, Debug)]
pub(super) enum Handle {
    Single(
        reader_map::handles::ReadHandle<
            DataType,
            Box<[DataType]>,
            PreInsertion,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
    Many(
        reader_map::handles::ReadHandle<
            Vec<DataType>,
            Box<[DataType]>,
            PreInsertion,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
}

/// An error that could occur during an equality or range lookup to a reader node.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum LookupError {
    /// The map is not ready to accept queries
    NotReady,

    /// The map has been destroyed
    Destroyed,

    /// Some other error occurred during the lookup
    Error(ReadySetError),

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

impl From<reader_map::Error> for LookupError {
    fn from(e: reader_map::Error) -> Self {
        match e {
            reader_map::Error::NotPublished => Self::NotReady,
            reader_map::Error::Destroyed => Self::Destroyed,
        }
    }
}

impl From<ReadySetError> for LookupError {
    fn from(err: ReadySetError) -> Self {
        Self::Error(err)
    }
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
    /// [`Self::NotReady`] or [`Self::Error`], the result will be empty
    pub fn into_misses(self) -> Vec<KeyComparison> {
        match self {
            LookupError::NotReady | LookupError::Destroyed | LookupError::Error(_) => vec![],
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

impl Handle {
    pub(super) fn timestamp(&self) -> Option<Timestamp> {
        match *self {
            Handle::Single(ref h) => h.timestamp().ok(),
            Handle::Many(ref h) => h.timestamp().ok(),
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

    pub(super) fn get(&self, key: &[DataType]) -> Result<SharedRows, LookupError> {
        match self {
            Handle::Single(h) => {
                let map = h.enter()?;
                let v = map
                    .get(&key[0])
                    .ok_or_else(|| LookupError::MissPointSingle(key[0].clone(), 0))?;
                Ok(v.as_ref().clone())
            }
            Handle::Many(h) => {
                let map = h.enter()?;
                let v = map
                    .get(key)
                    .ok_or_else(|| LookupError::MissPointMany(key.into(), 0))?;
                Ok(v.as_ref().clone())
            }
        }
    }

    pub(super) fn range<R>(&self, range: &R) -> Result<SharedResults, LookupError>
    where
        R: RangeBounds<Vec<DataType>>,
    {
        match self {
            Handle::Single(h) => {
                let map = h.enter()?;
                let start_bound = range.start_bound().map(|v| {
                    assert_eq!(v.len(), 1);
                    &v[0]
                });
                let end_bound = range.end_bound().map(|v| {
                    assert_eq!(v.len(), 1);
                    &v[0]
                });
                let range = (start_bound, end_bound);
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeSingle(misses, 0))?;
                Ok(records.map(|(_, row)| row.as_ref().clone()).collect())
            }
            Handle::Many(h) => {
                let map = h.enter()?;
                let range = (range.start_bound(), range.end_bound());
                let records = map
                    .range(&range)
                    .map_err(|Miss(misses)| LookupError::MissRangeMany(misses, 0))?;
                Ok(records.map(|(_, row)| row.as_ref().clone()).collect())
            }
        }
    }

    /// Returns Ok(true) if this handle contains the given key, Ok(false) if it doesn't, or an error
    /// if the underlying reader map is not able to accept reads
    ///
    /// This is equivalent to testing if `get` returns an Err other than `NotReady`
    pub(super) fn contains_key(&self, key: &[DataType]) -> reader_map::Result<bool> {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                let map = h.enter()?;
                Ok(map.contains_key(&key[0]))
            }
            Handle::Many(ref h) => {
                let map = h.enter()?;
                Ok(map.contains_key(key))
            }
        }
    }

    /// Returns Ok(true) if this handle fully contains the given key range, Ok(false) if any of the
    /// keys miss, or an error if the underlying reader map is not able to accept reads
    ///
    /// This is equivalent to testing if `get` returns an Err other than `NotReady`
    pub(super) fn contains_range<R>(&self, range: &R) -> reader_map::Result<bool>
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
                Ok(map.contains_range(&(start_bound, end_bound)))
            }
            Handle::Many(ref h) => {
                let map = h.enter()?;
                Ok(map.contains_range(&(range.start_bound(), range.end_bound())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use reader_map::handles::WriteHandle;

    use super::*;

    fn make_single() -> (
        WriteHandle<DataType, Box<[DataType]>, PreInsertion, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .with_insertion_order(None)
            .construct();
        (w, Handle::Single(r))
    }

    fn make_many() -> (
        WriteHandle<Vec<DataType>, Box<[DataType]>, PreInsertion, i64, Timestamp, RandomState>,
        Handle,
    ) {
        let (w, r) = reader_map::Options::default()
            .with_meta(-1)
            .with_timestamp(Timestamp::default())
            .with_hasher(RandomState::default())
            .with_insertion_order(None)
            .construct();
        (w, Handle::Many(r))
    }

    proptest! {
        #[test]
        fn get_double(key: [DataType; 2], val: Box<[DataType]>) {
            let (mut w, handle) = make_many();
            w.insert(key.to_vec(), val.clone());
            w.publish();
            assert_eq!(handle.get(&key[..]).unwrap()[0], val);
        }
    }

    #[test]
    fn get_single_range() {
        let (mut w, handle) = make_single();

        (0i32..10)
            .map(|n| ((n.into()), vec![n.into(), n.into()].into_boxed_slice()))
            .for_each(|(k, v)| {
                w.insert(k, v);
            });

        w.insert_range((DataType::from(0i32))..(DataType::from(10i32)));
        w.publish();

        let res = handle
            .range(&(vec![2i32.into()]..=vec![3i32.into()]))
            .unwrap();
        assert_eq!(
            res.iter()
                .map(|rs| rs.iter())
                .flatten()
                .cloned()
                .collect::<Vec<_>>(),
            (2i32..=3)
                .map(|n| vec![DataType::from(n), DataType::from(n)].into_boxed_slice())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn contains_key_single() {
        let (mut w, handle) = make_single();
        assert_eq!(
            handle.contains_key(&[1i32.into()]),
            Err(reader_map::Error::NotPublished)
        );

        w.publish();
        assert_eq!(handle.contains_key(&[1i32.into()]), Ok(false));

        w.insert(1i32.into(), vec![1i32.into()].into_boxed_slice());
        w.publish();
        assert_eq!(handle.contains_key(&[1i32.into()]), Ok(true));
    }

    #[test]
    fn get_double_range() {
        let (mut w, handle) = make_many();
        (0..10)
            .map(|n: i32| {
                (
                    vec![n.into(), n.into()],
                    vec![n.into(), n.into()].into_boxed_slice(),
                )
            })
            .for_each(|(k, v)| {
                w.insert(k, v);
            });
        w.insert_range(vec![0i32.into(), 0i32.into()]..vec![10i32.into(), 10i32.into()]);
        w.publish();

        let res = handle
            .range(&(vec![2i32.into(), 2i32.into()]..=vec![3i32.into(), 3i32.into()]))
            .unwrap();
        assert_eq!(
            res.iter()
                .map(|rs| rs.iter())
                .flatten()
                .cloned()
                .collect::<Vec<_>>(),
            (2i32..=3)
                .map(|n: i32| vec![DataType::from(n), DataType::from(n)].into_boxed_slice())
                .collect::<Vec<_>>()
        );
    }
}
