use metrics::histogram;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryInto;
use std::time::SystemTime;

use nom_sql::OrderType;
use noria::metrics::recorded;
use noria::util::like::LikePattern;
use noria::ViewQueryFilter;
use noria::{consistency::Timestamp, KeyComparison};
use tracing::warn;

use crate::backlog;
use crate::prelude::*;

/// Operations to perform on the results of a lookup after it's loaded from the map in a
/// reader
///
/// Because of limitations in the data structures we use to store reader state, some operations in a
/// query can't be cached as part of that state, and need to be performed after the results for a
/// query are loaded. We extract these operations as part of migration, and store them on the reader
/// node in this struct.
///
/// A previous version provided these operations as part of [`ViewQuery`] rather than storing them
/// on the reader node - they've been moved here so that the post-lookup operations can be based on
/// the desugared query rather than the original query.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct PostLookup {
    /// Column indices to order by, and whether or not to reverse order on each index.
    ///
    /// If an empty `Vec` is specified, all rows are sorted as if they were equal to each other.
    pub order_by: Option<Vec<(usize, OrderType)>>,
    /// Maximum number of records to return
    pub limit: Option<usize>,
    /// Indices of the columns requested in the query. Reader will filter out all other projected
    /// columns
    pub returned_cols: Option<Vec<usize>>,
    /// Default values to send back, for example if we're aggregating and no rows are found
    pub default_row: Option<Vec<DataType>>,
}

impl PostLookup {
    /// Apply this set of post-lookup operations, plus an optional [`ViewQueryFilter`], to the given
    /// set of results returned from a lookup
    pub fn process<'a, I>(
        &'a self,
        iter: I,
        filter: &Option<ViewQueryFilter>,
    ) -> Vec<Vec<&'a DataType>>
    where
        I: Iterator<Item = &'a Vec<DataType>> + ExactSizeIterator,
        Self: 'a,
    {
        let data = iter.map(|r| r.iter().collect::<Vec<_>>());
        if self.order_by.is_none()
            && self.limit.is_none()
            && filter.is_none()
            && self.returned_cols.is_none()
            && self.default_row.is_none()
        {
            return data.collect::<Vec<_>>();
        }

        // If no data is present AND we have default values (e.g. we're aggregating), we can
        // short-circuit here and just return the defaults.
        if data.len() == 0 {
            if let Some(defaults) = self.default_row.as_ref() {
                return vec![defaults.iter().collect()];
            }
        }

        let ordered_limited = do_order_limit(data, self.order_by.as_deref(), self.limit);
        let like_pattern = filter.as_ref().map(
            |ViewQueryFilter {
                 value,
                 operator,
                 column,
             }| { (LikePattern::new(value, (*operator).into()), *column) },
        );

        let like_filtered = ordered_limited.filter(move |rec| {
            like_pattern
                .as_ref()
                .map(|(pat, col)| {
                    pat.matches(
                        (rec[*col])
                            .try_into()
                            .expect("Type mismatch: LIKE and ILIKE can only be applied to strings"),
                    )
                })
                .unwrap_or(true)
        });

        let returned_cols = match &self.returned_cols {
            Some(c) => c,
            None => {
                return like_filtered.collect::<Vec<_>>();
            }
        };

        let col_filtered =
            like_filtered.map(|row| returned_cols.iter().map(|i| row[*i]).collect::<Vec<_>>());
        col_filtered.collect::<Vec<_>>()
    }
}

/// A container for four different exact-size iterators.
///
/// This type exists to avoid having to return a `dyn Iterator` when applying an ORDER BY / LIMIT
/// to the results of a query. It implements `Iterator` and `ExactSizeIterator` iff all of its
/// type parameters implement `Iterator<Item = Vec<&DataType>>`.
enum OrderedLimitedIter<I, J, K, L> {
    Original(I),
    Ordered(J),
    Limited(K),
    OrderedLimited(L),
}

/// WARNING: This impl does NOT delegate calls to `len()` to the underlying iterators.
impl<'a, I, J, K, L> ExactSizeIterator for OrderedLimitedIter<I, J, K, L>
where
    I: Iterator<Item = Vec<&'a DataType>>,
    J: Iterator<Item = Vec<&'a DataType>>,
    K: Iterator<Item = Vec<&'a DataType>>,
    L: Iterator<Item = Vec<&'a DataType>>,
{
}

impl<'a, I, J, K, L> Iterator for OrderedLimitedIter<I, J, K, L>
where
    I: Iterator<Item = Vec<&'a DataType>>,
    J: Iterator<Item = Vec<&'a DataType>>,
    K: Iterator<Item = Vec<&'a DataType>>,
    L: Iterator<Item = Vec<&'a DataType>>,
{
    type Item = Vec<&'a DataType>;
    fn next(&mut self) -> Option<Self::Item> {
        use self::OrderedLimitedIter::*;
        match self {
            Original(i) => i.next(),
            Ordered(i) => i.next(),
            Limited(i) => i.next(),
            OrderedLimited(i) => i.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        use self::OrderedLimitedIter::*;
        match self {
            Original(i) => i.size_hint(),
            Ordered(i) => i.size_hint(),
            Limited(i) => i.size_hint(),
            OrderedLimited(i) => i.size_hint(),
        }
    }
}

fn do_order<'a, I>(
    iter: I,
    indices: &[(usize, OrderType)],
) -> impl Iterator<Item = Vec<&'a DataType>>
where
    I: Iterator<Item = Vec<&'a DataType>>,
{
    // TODO(eta): is there a way to avoid buffering all the results?
    let mut results = iter.collect::<Vec<_>>();
    results.sort_by(|a, b| {
        // protip: look at what `Ordering::then` does if you're confused by this
        //
        // TODO(eta): Technically, this is inefficient, because you can break out of the fold
        //            early if you hit something that isn't `Ordering::Equal`. In practice though
        //            it's likely to be neglegible.
        // NOTE(grfn): or LLVM / branch prediction just optimizes it away!
        indices
            .iter()
            .map(|&(idx, order_type)| {
                let ret = a[idx].cmp(b[idx]);
                match order_type {
                    OrderType::OrderAscending => ret,
                    OrderType::OrderDescending => ret.reverse(),
                }
            })
            .fold(Ordering::Equal, |acc, next| acc.then(next))
    });
    results.into_iter()
}

fn do_order_limit<'a, I>(
    iter: I,
    order_by: Option<&[(usize, OrderType)]>,
    limit: Option<usize>,
) -> impl Iterator<Item = Vec<&'a DataType>> + ExactSizeIterator
where
    I: Iterator<Item = Vec<&'a DataType>> + ExactSizeIterator,
{
    match (order_by, limit) {
        (None, None) => OrderedLimitedIter::Original(iter),
        (Some(indices), None) => OrderedLimitedIter::Ordered(do_order(iter, indices)),
        (None, Some(lim)) => OrderedLimitedIter::Limited(iter.take(lim)),
        (Some(indices), Some(lim)) => {
            OrderedLimitedIter::OrderedLimited(do_order(iter, indices).take(lim))
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Reader {
    #[serde(skip)]
    writer: Option<backlog::WriteHandle>,

    for_node: NodeIndex,
    index: Option<Index>,

    /// Operations to perform on the result set after the rows are returned from the lookup
    post_lookup: PostLookup,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        debug_assert!(self.writer.is_none());
        Reader {
            writer: None,
            for_node: self.for_node,
            post_lookup: self.post_lookup.clone(),
            index: self.index.clone(),
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex, post_lookup: PostLookup) -> Self {
        Reader {
            writer: None,
            for_node,
            post_lookup,
            index: None,
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    pub(crate) fn writer_mut(&mut self) -> Option<&mut backlog::WriteHandle> {
        self.writer.as_mut()
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Self {
            writer: self.writer.take(),
            for_node: self.for_node,
            post_lookup: self.post_lookup.clone(),
            index: self.index.clone(),
        }
    }

    pub fn is_materialized(&self) -> bool {
        self.index.is_some()
    }

    pub(crate) fn is_partial(&self) -> bool {
        match self.writer {
            None => false,
            Some(ref state) => state.is_partial(),
        }
    }

    pub(crate) fn set_write_handle(&mut self, wh: backlog::WriteHandle) {
        debug_assert!(self.writer.is_none());
        self.writer = Some(wh);
    }

    pub fn index(&self) -> Option<&Index> {
        self.index.as_ref()
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.index.as_ref().map(|s| &s.columns[..])
    }

    pub fn index_type(&self) -> Option<IndexType> {
        self.index.as_ref().map(|index| index.index_type)
    }

    pub fn set_index(&mut self, index: &Index) {
        if let Some(ref m_index) = self.index {
            debug_assert_eq!(m_index, index);
        } else {
            self.index = Some(index.clone());
        }
    }

    pub(crate) fn state_size(&self) -> Option<u64> {
        self.writer.as_ref().map(SizeOf::deep_size_of)
    }

    /// Evict a randomly selected key, returning the number of bytes evicted.
    pub(crate) fn evict_bytes(&mut self, bytes: usize) -> u64 {
        let mut bytes_freed = 0;
        if let Some(ref mut handle) = self.writer {
            let mut rng = rand::thread_rng();
            bytes_freed = handle.evict_bytes(&mut rng, bytes);
            handle.swap();
        }
        bytes_freed
    }

    pub(in crate::node) fn on_eviction(&mut self, keys: &[KeyComparison]) {
        // NOTE: *could* be None if reader has been created but its state hasn't been built yet
        if let Some(w) = self.writer.as_mut() {
            for k in keys {
                w.mark_hole(k);
            }
            w.swap();
        }
    }

    #[allow(clippy::unreachable)]
    pub(in crate::node) fn process(&mut self, m: &mut Option<Box<Packet>>, swap: bool) {
        if let Some(ref mut state) = self.writer {
            let m = m.as_mut().unwrap();
            m.handle_trace(
                |trace| match SystemTime::now().duration_since(trace.start) {
                    Ok(d) => {
                        histogram!(
                            recorded::PACKET_WRITE_PROPAGATION_TIME,
                            d.as_micros() as f64
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "Write latency trace failed");
                    }
                },
            );
            // make sure we don't fill a partial materialization
            // hole with incomplete (i.e., non-replay) state.
            if m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Err(e) if e.is_miss() => {
                                // row would miss in partial state.
                                // leave it blank so later lookup triggers replay.
                                false
                            }
                            Ok(_) => {
                                // state is already present,
                                // so we can safely keep it up to date.
                                true
                            }
                            Err(_) => {
                                // If we got here it means we got a `NotReady` error type. This is
                                // impossible, because when readers are instantiated we issue a
                                // commit to the underlying map, which makes it Ready.
                                unreachable!(
                                    "somehow found a NotReady reader even though we've
                                    already initialized it with a commit"
                                )
                            }
                        }
                    });
                });
            }

            // it *can* happen that multiple readers miss (and thus request replay for) the
            // same hole at the same time. we need to make sure that we ignore any such
            // duplicated replay.
            if !m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Err(e) if e.is_miss() => {
                                // filling a hole with replay -- ok
                                true
                            }
                            Ok(_) => {
                                // a given key should only be replayed to once!
                                false
                            }
                            Err(_) => {
                                // state has not yet been swapped, which means it's new,
                                // which means there are no readers, which means no
                                // requests for replays have been issued by readers, which
                                // means no duplicates can be received.
                                true
                            }
                        }
                    });
                });
            }

            state.add(m.take_data());

            if swap {
                // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
                state.swap();
            }
        }
    }

    pub(in crate::node) fn process_timestamp(&mut self, m: Timestamp) {
        if let Some(ref mut handle) = self.writer {
            handle.set_timestamp(m);

            // Ensure the write is published.
            handle.swap();
        }
    }

    /// Get a reference to the reader's post lookup.
    pub fn post_lookup(&self) -> &PostLookup {
        &self.post_lookup
    }
}
