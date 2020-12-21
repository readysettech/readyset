use ahash::RandomState;
use common::DataType;
use evbtree::{self, refs::Values};
use noria::util::BoundFunctor;
use std::mem;
use std::ops::{Bound, RangeBounds};
use unbounded_interval_tree::IntervalTree;

#[derive(Clone, Debug)]
pub(super) enum Handle {
    Single(
        evbtree::handles::ReadHandle<DataType, Vec<DataType>, i64, RandomState>,
        IntervalTree<DataType>,
    ),
    Double(
        evbtree::handles::ReadHandle<(DataType, DataType), Vec<DataType>, i64, RandomState>,
        IntervalTree<(DataType, DataType)>,
    ),
    Many(
        evbtree::handles::ReadHandle<Vec<DataType>, Vec<DataType>, i64, RandomState>,
        IntervalTree<Vec<DataType>>,
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
    MissRangeSingle(Vec<(Bound<DataType>, Bound<DataType>)>, i64),

    /// A double-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeDouble(
        Vec<(Bound<(DataType, DataType)>, Bound<(DataType, DataType)>)>,
        i64,
    ),

    /// A many-keyed range query missed
    ///
    /// Second field contains the metadata of the handle
    MissRangeMany(Vec<(Bound<Vec<DataType>>, Bound<Vec<DataType>>)>, i64),
}

impl LookupError {
    /// Returns `true` if this `LookupError` represents a miss on a key, `false` if it represents a
    /// not-ready map
    pub fn is_miss(&self) -> bool {
        matches!(self,
              Self::MissPointSingle(_, _)
            | Self::MissPointDouble(_, _)
            | Self::MissPointMany(_, _)
            | Self::MissRangeSingle(_, _)
            | Self::MissRangeDouble(_, _)
            | Self::MissRangeMany(_, _))
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
}

/// The result of an equality or range lookup to the reader node.
pub(crate) type LookupResult<T> = Result<(T, i64), LookupError>;

impl Handle {
    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h, _) => h.len(),
            Handle::Double(ref h, _) => h.len(),
            Handle::Many(ref h, _) => h.len(),
        }
    }

    pub(super) fn keys(&self) -> Vec<Vec<DataType>> {
        match *self {
            Handle::Single(ref h, _) => h.map_into(|k, _| vec![k.clone()]),
            Handle::Double(ref h, _) => h.map_into(|(k1, k2), _| vec![k1.clone(), k2.clone()]),
            Handle::Many(ref h, _) => h.map_into(|ks, _| ks.clone()),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> LookupResult<T>
    where
        F: FnOnce(&Values<Vec<DataType>, RandomState>) -> T,
    {
        use LookupError::*;

        match *self {
            Handle::Single(ref h, _) => {
                assert_eq!(key.len(), 1);
                let map = h.enter().ok_or(NotReady)?;
                let m = *map.meta();
                let v = map.get(&key[0]).ok_or(MissPointSingle(key[0].clone(), m))?;
                Ok((then(v), m))
            }
            Handle::Double(ref h, _) => {
                assert_eq!(key.len(), 2);
                unsafe {
                    let tuple_key = slice_to_2_tuple(&key);
                    let map = h.enter().ok_or(NotReady)?;
                    let m = *map.meta();
                    let v = map
                        .get(&tuple_key)
                        .ok_or(MissPointDouble(tuple_key.clone(), m))?;
                    mem::forget(tuple_key);
                    Ok((then(v), m))
                }
            }
            Handle::Many(ref h, _) => {
                let map = h.enter().ok_or(NotReady)?;
                let m = *map.meta();
                let v = map.get(key).ok_or(MissPointMany(key.into(), m))?;
                Ok((then(v), m))
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
    pub(super) fn meta_get_range_and<F, T, R>(&self, range: R, mut then: F) -> LookupResult<Vec<T>>
    where
        F: FnMut(&Values<Vec<DataType>, RandomState>) -> T,
        R: RangeBounds<Vec<DataType>>,
    {
        use LookupError::*;

        match *self {
            Handle::Single(ref h, ref t) => {
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
                let range = (start_bound.cloned(), end_bound.cloned());
                let diff = t.get_interval_difference(range.clone());
                if diff.is_empty() {
                    Ok((map.range(range).map(|(_, row)| then(row)).collect(), meta))
                } else {
                    Err(MissRangeSingle(diff, meta))
                }
            }
            Handle::Double(ref h, ref t) => {
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
                let diff = t.get_interval_difference(range.clone());
                if diff.is_empty() {
                    Ok((map.range(range).map(|(_, row)| then(row)).collect(), meta))
                } else {
                    Err(MissRangeDouble(diff, meta))
                }
            }
            Handle::Many(ref h, ref t) => {
                let map = h.enter().ok_or(NotReady)?;
                let meta = *map.meta();
                let range = (range.start_bound().cloned(), range.end_bound().cloned());
                let diff = t.get_interval_difference(range.clone());
                if diff.is_empty() {
                    Ok((map.range(range).map(|(_, row)| then(row)).collect(), meta))
                } else {
                    Err(MissRangeMany(diff, meta))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evbtree::handles::WriteHandle;
    use proptest::prelude::*;

    fn make_single() -> (
        WriteHandle<DataType, Vec<DataType>, i64, RandomState>,
        Handle,
    ) {
        let (w, r) = evbtree::Options::default()
            .with_meta(-1)
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Single(r, IntervalTree::default()))
    }

    fn make_double() -> (
        WriteHandle<(DataType, DataType), Vec<DataType>, i64, RandomState>,
        Handle,
    ) {
        let (w, r) = evbtree::Options::default()
            .with_meta(-1)
            .with_hasher(RandomState::default())
            .construct();
        (w, Handle::Double(r, IntervalTree::default()))
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
        let (mut w, mut handle) = make_single();
        for n in 0..10 {
            w.insert(n.into(), vec![n.into(), n.into()]);
        }
        w.publish();
        match handle {
            Handle::Single(_, ref mut t) => t.insert((
                Bound::Included(DataType::from(2)),
                Bound::Included(DataType::from(3)),
            )),
            _ => unreachable!(),
        }
        let (res, meta) = handle
            .meta_get_range_and(vec![2.into()]..=vec![3.into()], |vals| {
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
    fn get_double_range() {
        let (mut w, mut handle) = make_double();
        for n in 0..10 {
            w.insert((n.into(), n.into()), vec![n.into(), n.into()]);
        }
        w.publish();
        match handle {
            Handle::Double(_, ref mut t) => t.insert((
                Bound::Included((2.into(), 2.into())),
                Bound::Included((3.into(), 3.into())),
            )),
            _ => unreachable!(),
        }
        let (res, meta) = handle
            .meta_get_range_and(
                vec![2.into(), 2.into()]..=vec![3.into(), 3.into()],
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
