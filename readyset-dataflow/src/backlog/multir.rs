use std::borrow::Cow;
use std::convert::TryInto;
use std::ops::RangeBounds;

use ahash::RandomState;
use common::DataType;
use dataflow_expression::PreInsertion;
use noria::consistency::Timestamp;
use noria::results::{SharedResults, SharedRows};
use noria::KeyComparison;
use reader_map::refs::Miss;
use readyset_errors::ReadySetError;
use serde::{Deserialize, Serialize};
use tracing::warn;
use vec1::{vec1, Vec1};

/// A [`ReadHandle`] to a map whose key is a single [`DataType`], for faster lookup (compared to a
/// Vec with len == 1)
type HandleSingle = reader_map::handles::ReadHandle<
    DataType,
    Box<[DataType]>,
    PreInsertion,
    i64,
    Timestamp,
    RandomState,
>;

/// A [`ReadHandle`] to a map whose key is a [`Vec<DataType>`]
type HandleMany = reader_map::handles::ReadHandle<
    Vec<DataType>,
    Box<[DataType]>,
    PreInsertion,
    i64,
    Timestamp,
    RandomState,
>;

#[derive(Clone, Debug)]
pub(super) enum Handle {
    Single(HandleSingle),
    Many(HandleMany),
}

/// An error that could occur during an equality or range lookup to a reader node.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum LookupError<'a> {
    /// The map is not ready to accept queries
    NotReady,
    /// The map has been destroyed
    Destroyed,
    /// Some other error occurred during the lookup
    Error(ReadySetError),
    /// Some of the keys in the lookup missed, list of the keys included
    Miss(Vec<Cow<'a, KeyComparison>>),
}

impl<'a> From<reader_map::Error> for LookupError<'a> {
    fn from(e: reader_map::Error) -> Self {
        match e {
            reader_map::Error::NotPublished => Self::NotReady,
            reader_map::Error::Destroyed => Self::Destroyed,
        }
    }
}

impl<'a> From<ReadySetError> for LookupError<'a> {
    fn from(err: ReadySetError) -> Self {
        Self::Error(err)
    }
}

impl<'a> LookupError<'a> {
    /// Returns `true` if this `LookupError` represents a miss on a key, `false` if it represents a
    /// not-ready or destroyed map
    pub fn is_miss(&self) -> bool {
        matches!(self, LookupError::Miss(_))
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

    fn get_multi_single_handle<'a>(
        handle: &HandleSingle,
        keys: &'a [KeyComparison],
    ) -> Result<SharedResults, LookupError<'a>> {
        let mut hits = SharedResults::with_capacity(keys.len());
        let mut misses = Vec::new();
        let map = handle.enter()?;
        for key in keys {
            match key {
                KeyComparison::Equal(k) => match map.get(&k[0]) {
                    Some(v) => hits.push(v.as_ref().clone()),
                    None => misses.push(Cow::Borrowed(key)),
                },
                KeyComparison::Range((start, end)) => {
                    if key.is_reversed_range() {
                        warn!("Reader received lookup for range with start bound above end bound; returning empty result set");
                        hits.push(Default::default());
                        continue;
                    }

                    let start_bound = start.as_ref().map(|v| &v[0]);
                    let end_bound = end.as_ref().map(|v| &v[0]);
                    match map.range(&(start_bound, end_bound)) {
                        Ok(hit) => hits.extend(hit.map(|(_, v)| v.as_ref().clone())),
                        Err(Miss(miss)) => misses.extend(miss.into_iter().map(|(start, end)| {
                            Cow::Owned(KeyComparison::Range((
                                start.map(|s| vec1![s]),
                                end.map(|e| vec1![e]),
                            )))
                        })),
                    }
                }
            }
        }

        if !misses.is_empty() {
            Err(LookupError::Miss(misses))
        } else {
            Ok(hits)
        }
    }

    fn get_multi_many_handle<'a>(
        handle: &HandleMany,
        keys: &'a [KeyComparison],
    ) -> Result<SharedResults, LookupError<'a>> {
        let mut hits = SharedResults::with_capacity(keys.len());
        let mut misses = Vec::new();
        let map = handle.enter()?;
        for key in keys {
            match key {
                KeyComparison::Equal(k) => match map.get(k.as_slice()) {
                    Some(v) => hits.push(v.as_ref().clone()),
                    None => misses.push(Cow::Borrowed(key)),
                },
                KeyComparison::Range((start, end)) => {
                    if key.is_reversed_range() {
                        warn!("Reader received lookup for range with start bound above end bound; returning empty result set");
                        hits.push(Default::default());
                        continue;
                    }

                    match map.range::<_, [DataType]>(&(
                        start.as_ref().map(|v| v.as_slice()),
                        end.as_ref().map(|v| v.as_slice()),
                    )) {
                        Ok(hit) => hits.extend(hit.map(|(_, v)| v.as_ref().clone())),
                        Err(Miss(miss)) => misses.extend(miss.into_iter().map(|(start, end)| {
                            Cow::Owned(KeyComparison::Range((
                                start.map(|s| Vec1::try_from_vec(s).unwrap()),
                                end.map(|e| Vec1::try_from_vec(e).unwrap()),
                            )))
                        })),
                    }
                }
            }
        }

        if !misses.is_empty() {
            Err(LookupError::Miss(misses))
        } else {
            Ok(hits)
        }
    }

    /// Retreive results for multiple keys from the map under the same read guard, assuring that all
    /// of the values refer to the same state map.
    pub(super) fn get_multi<'a>(
        &self,
        keys: &'a [KeyComparison],
    ) -> Result<SharedResults, LookupError<'a>> {
        match self {
            Handle::Single(h) => Self::get_multi_single_handle(h, keys),
            Handle::Many(h) => Self::get_multi_many_handle(h, keys),
        }
    }

    pub(super) fn get<'a>(&self, key: &'a [DataType]) -> Result<SharedRows, LookupError<'a>> {
        match self {
            Handle::Single(h) => {
                let map = h.enter()?;
                let v = map.get(&key[0]).ok_or_else(|| {
                    LookupError::Miss(vec![Cow::Owned(KeyComparison::Equal(
                        vec1![key[0].clone()],
                    ))])
                })?;
                Ok(v.as_ref().clone())
            }
            Handle::Many(h) => {
                let map = h.enter()?;
                let v = map.get(key).ok_or_else(|| {
                    LookupError::Miss(vec![Cow::Owned(KeyComparison::Equal(
                        key.try_into().unwrap(),
                    ))])
                })?;
                Ok(v.as_ref().clone())
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

    /// Returns true if the corresponding write handle has been dropped
    pub(super) fn was_dropped(&self) -> bool {
        match self {
            Handle::Single(h) => h.was_dropped(),
            Handle::Many(h) => h.was_dropped(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

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

        let key = KeyComparison::Range((
            Bound::Included(vec1![2i32.into()]),
            Bound::Included(vec1![3i32.into()]),
        ));

        let res = handle.get_multi(&[key]).unwrap();
        assert_eq!(
            res.iter()
                .flat_map(|rs| rs.iter())
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

        let key = KeyComparison::Range((
            Bound::Included(vec1![2i32.into(), 2i32.into()]),
            Bound::Included(vec1![3i32.into(), 3i32.into()]),
        ));

        let res = handle.get_multi(&[key]).unwrap();
        assert_eq!(
            res.iter()
                .flat_map(|rs| rs.iter())
                .cloned()
                .collect::<Vec<_>>(),
            (2i32..=3)
                .map(|n: i32| vec![DataType::from(n), DataType::from(n)].into_boxed_slice())
                .collect::<Vec<_>>()
        );
    }
}
