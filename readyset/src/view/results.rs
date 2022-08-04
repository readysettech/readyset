use std::cmp::Ordering;
use std::sync::Arc;

use dataflow_expression::{Expr, PostLookup, PostLookupAggregates};
use launchpad::nonmaxusize::NonMaxUsize;
use nom_sql::OrderType;
use readyset_data::DataType;
use smallvec::SmallVec;
use streaming_iterator::StreamingIterator;
use tournament_kway::{Comparator, StreamingTournament};

use crate::ReadReplyStats;

/// A lookup key into a reader
pub type Key = Box<[DataType]>;

/// A single row in a result set
pub type Row = Box<[DataType]>;

/// A shared set of rows, returned for a given single key in a reader, under a [`triomphe::Arc`]
pub type SharedRows = triomphe::Arc<SmallVec<[Row; 1]>>;

/// A list of [`SharedRows`], combining the lookup results for multiple keys
pub type SharedResults = SmallVec<[SharedRows; 1]>;

/// A set of uniquely owned results
#[derive(Debug)]
pub struct Results {
    results: Vec<Vec<DataType>>,
    /// When present, contains stats related to the operation
    pub stats: Option<ReadReplyStats>,
}

impl Results {
    #[doc(hidden)]
    pub fn new(results: Vec<Vec<DataType>>) -> Self {
        Self {
            results,
            stats: None,
        }
    }

    #[doc(hidden)]
    pub fn with_stats(results: Vec<Vec<DataType>>, stats: ReadReplyStats) -> Self {
        Self {
            results,
            stats: Some(stats),
        }
    }

    #[doc(hidden)]
    pub fn into_data(self) -> Vec<Vec<DataType>> {
        self.results
    }
}

/// A ['StreamingIterator`] over rows of a noria select response with filters
#[derive(Debug)]
pub struct ResultIterator {
    /// An inner iterator that returns rows of results
    inner: ResultIteratorInner,
    /// The maximum number of elements to return
    limit: Option<usize>,
    /// The number of rows to skip from the beginning
    offset: Option<usize>,
    /// The row to return if the result set is empty
    default_row: Option<Arc<Row>>,
    /// If not the first result to be returned
    non_empty: bool,
    /// A filter expression to ignore rows that don't match
    filter: Option<Expr>,
    /// How many columns to return
    cols: usize,
}

/// A ['StreamingIterator`] over rows of a noria select response
#[derive(Debug)]
enum ResultIteratorInner {
    /// Owned results returned from noria server
    OwnedResults(OwnedResultIterator),
    /// Cached results returned from a ['CachingView`] for more than one key
    MultiKey(MultiKeyIterator),
    /// Cached results returned from a ['CachingView`] for more than one key, merging the set with
    /// an order by clause
    MultiKeyMerge(MergeIterator),
    /// Cached results returned for more than one key, with an aggregate function
    MultiKeyAggregateMerge(AggregateIterator),
}

/// Iterator over owned results returned from noria server
#[derive(Debug)]
struct OwnedResultIterator {
    // Encapsulated data
    data: Vec<Results>,
    // Current position in the data vector
    set: usize,
    row: Option<NonMaxUsize>,
}

/// An iterator over a single set of cached results
#[derive(Debug)]
struct SingleKeyIterator {
    // The underlying data we iterate over
    data: SharedRows,
    // The next row in the current set to return
    row: Option<NonMaxUsize>,
}

/// An iterator over a multiple sets of cached results
#[derive(Debug)]
struct MultiKeyIterator {
    // The underlying data we iterate over
    data: SharedResults,
    // The current set of result in the iterator
    set: usize,
    // The next row in the current set to return
    row: Option<NonMaxUsize>,
}

/// An iterator over multiple sets of cached results with an order by clause
#[derive(Debug)]
struct MergeIterator {
    // Tournament tree that merges multiple ordered sets of results
    inner: StreamingTournament<SingleKeyIterator, RowComparator>,
}

#[derive(Clone, Debug)]
struct RowComparator {
    order_by: Arc<[(usize, OrderType)]>,
}

impl<T> Comparator<[T]> for RowComparator
where
    T: Ord,
{
    fn cmp(&self, a: &[T], b: &[T]) -> Ordering {
        self.order_by
            .iter()
            .map(|&(idx, order_type)| order_type.apply(a[idx].cmp(&b[idx])))
            .fold(Ordering::Equal, |acc, next| acc.then(next))
    }
}

#[derive(Debug)]
struct AggregateIterator {
    inner: Box<ResultIteratorInner>,
    aggregate: PostLookupAggregates,
    out_row: Option<Vec<DataType>>,
    filter: Option<Expr>,
}

impl ResultIterator {
    /// Create a new [`ResultIterator`] from a set of [`SharedRows`] and a [`PostLookup`].
    /// Each individual set of [`SharedRows`] is assumed sorted in regards to the provided
    /// [`PostLookup`], otherwise the iteration order may break.
    /// The parameter for `adapter_limit` is used to override any limit set in the `PostLookup`
    /// provided, in case the adapter requesting this result thinks it needs a different number of
    /// rows.
    pub fn new(
        data: SharedResults,
        post_lookup: &PostLookup,
        adapter_limit: Option<usize>,
        offset: Option<usize>,
        mut filter: Option<Expr>,
    ) -> Self {
        let PostLookup {
            order_by,
            limit,
            returned_cols,
            aggregates,
            default_row,
            ..
        } = post_lookup;

        let limit = adapter_limit.or(*limit); // Limit specifies total number of results to return

        let inner = match (order_by, aggregates) {
            // No specific order is required, simply iterate over each result set one by one
            (None, None) => ResultIteratorInner::MultiKey(MultiKeyIterator::new(data)),
            (Some(order_by), None) => {
                // Order by is specified, merge results using a k-way merge iterator
                let comparator = RowComparator {
                    order_by: order_by.clone().into(),
                };

                debug_assert!(data
                    .iter()
                    .all(|s| { s.is_sorted_by(|a, b| Some(comparator.cmp(a, b))) }));

                ResultIteratorInner::MultiKeyMerge(MergeIterator::new(data, comparator))
            }
            (None, Some(aggregates)) => {
                if aggregates.group_by.is_empty() {
                    // No group by means just iterate over the results and aggregate
                    ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKey(MultiKeyIterator::new(data))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                    })
                } else {
                    // With group by, merge results using a k-way merge iterator on group-by
                    // predicates and aggregate
                    let comparator = RowComparator {
                        order_by: aggregates
                            .group_by
                            .iter()
                            .map(|&col| (col, OrderType::OrderAscending))
                            .collect(),
                    };

                    debug_assert!(data
                        .iter()
                        .all(|s| { s.is_sorted_by(|a, b| Some(comparator.cmp(a, b))) }));

                    ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKeyMerge(MergeIterator::new(
                            data, comparator,
                        ))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                    })
                }
            }
            (Some(order_by), Some(aggregates)) => {
                // When both aggregates and order by are specified it is tricky to lazily evaluate
                // rows, so sadly we end up having to collect all of the rows, aggregate, then sort
                // them
                let comparator = RowComparator {
                    order_by: aggregates
                        .group_by
                        .iter()
                        .map(|&col| (col, OrderType::OrderAscending))
                        .collect(),
                };

                debug_assert!(data
                    .iter()
                    .all(|s| { s.is_sorted_by(|a, b| Some(comparator.cmp(a, b))) }));

                let temp_iter = ResultIterator {
                    inner: ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKeyMerge(MergeIterator::new(
                            data, comparator,
                        ))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                    }),
                    limit,
                    offset: None,
                    default_row: default_row.clone(),
                    non_empty: false,
                    filter: None,
                    cols: usize::MAX,
                };

                let mut results = temp_iter.into_vec();
                results.sort_by(|a, b| {
                    order_by
                        .iter()
                        .map(|&(idx, order_type)| order_type.apply(a[idx].cmp(&b[idx])))
                        .fold(Ordering::Equal, |acc, next| acc.then(next))
                });

                if let Some(offset) = offset {
                    results.drain(offset..);
                }

                return ResultIterator::owned(vec![Results {
                    results,
                    stats: None,
                }]);
            }
        };

        ResultIterator {
            inner,
            limit,
            offset,
            default_row: default_row.clone(),
            non_empty: false,
            // When aggregates (group_by) is present, filtering is processed by the inner
            // aggregating iterator, and its value here would be `None`.
            filter,
            cols: returned_cols
                .as_ref()
                .map(|r| r.len())
                .unwrap_or(usize::MAX),
        }
    }

    /// Create from owned data
    pub fn owned(data: Vec<Results>) -> Self {
        ResultIterator {
            inner: ResultIteratorInner::OwnedResults(OwnedResultIterator {
                data,
                set: 0,
                row: None,
            }),
            limit: None,
            offset: None,
            default_row: None,
            non_empty: false,
            filter: None,
            cols: usize::MAX,
        }
    }

    /// Get aggregated stats for all results in the set
    pub fn total_stats(&self) -> Option<ReadReplyStats> {
        match &self.inner {
            ResultIteratorInner::OwnedResults(OwnedResultIterator { data, .. }) => data
                .iter()
                .map(|r| &r.stats)
                .fold(None, |total, cur| match cur {
                    Some(stats) => Some(ReadReplyStats {
                        cache_misses: stats.cache_misses
                            + total.map(|s| s.cache_misses).unwrap_or(0),
                    }),
                    None => total,
                }),
            _ => None,
        }
    }

    /// Advance the iterator skipping rows which don't pass the filter predicate
    fn advance_filtered(&mut self) {
        loop {
            self.inner.advance();
            if let Some(filter) = &self.filter {
                // Check if the row passes the filter predicate
                if let Some(expr) = self.inner.get() {
                    if !filter.eval(expr).map(|r| r.is_truthy()).unwrap_or(false) {
                        continue;
                    }
                }
            }
            break;
        }
    }
}

impl StreamingIterator for OwnedResultIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        let row = match self.row.as_mut() {
            Some(row) => {
                row.inc();
                row
            }
            None => self.row.get_or_insert(NonMaxUsize::zero()),
        };
        while let Some(rows) = self.data.get(self.set) {
            if rows.results.get(**row).is_some() {
                break;
            }
            self.set += 1;
            *row = NonMaxUsize::zero();
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row
            .and_then(|row| self.data.get(self.set).and_then(|s| s.results.get(*row)))
            .map(|v| v.as_slice())
    }
}

impl SingleKeyIterator {
    pub(crate) fn new(data: SharedRows) -> Self {
        SingleKeyIterator { data, row: None }
    }
}

impl StreamingIterator for SingleKeyIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        match self.row.as_mut() {
            None => self.row = Some(NonMaxUsize::zero()),
            Some(row) => row.inc(),
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row.and_then(|row| self.data.get(*row)).map(|r| &r[..])
    }
}

impl MultiKeyIterator {
    pub(crate) fn new(data: SharedResults) -> Self {
        MultiKeyIterator {
            data,
            set: 0,
            row: None,
        }
    }
}

impl StreamingIterator for MultiKeyIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        let row = match self.row.as_mut() {
            Some(row) => {
                row.inc();
                row
            }
            None => self.row.get_or_insert(NonMaxUsize::zero()),
        };

        while let Some(results) = self.data.get(self.set) {
            if results.get(**row).is_none() {
                // Skip empty sets
                self.set += 1;
                *row = NonMaxUsize::zero();
            } else {
                break;
            }
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row
            .and_then(|row| self.data.get(self.set).and_then(|s| s.get(*row)))
            .map(|r| &r[..])
    }
}

impl MergeIterator {
    pub(crate) fn new(data: SharedResults, comparator: RowComparator) -> Self {
        MergeIterator {
            inner: StreamingTournament::from_iters(
                data.into_iter().map(SingleKeyIterator::new),
                comparator,
            ),
        }
    }
}

impl StreamingIterator for MergeIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        self.inner.advance();
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.inner.get()
    }
}

impl AggregateIterator {
    fn advance_filtered(&mut self) {
        self.inner.advance();

        let filter = match &self.filter {
            None => return,
            Some(filter) => filter,
        };

        while let Some(row) = self.inner.get() {
            if filter.eval(row).map(|r| r.is_truthy()).unwrap_or(false) {
                break;
            }
            self.inner.advance();
        }
    }
}

impl StreamingIterator for AggregateIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        // First assign the next row if possible
        if self.out_row.is_none() {
            // Probably first adavnce
            self.advance_filtered();
        }

        let mut aggregate_row = match self.inner.get() {
            Some(row) => row.to_vec(),
            None => {
                self.out_row = None;
                return;
            }
        };

        self.advance_filtered();
        while let Some(row) = self.inner.get() {
            if self
                .aggregate
                .group_by
                .iter()
                .any(|&i| aggregate_row.get(i) != row.get(i))
            {
                break;
            }

            for agg in &self.aggregate.aggregates {
                let col = agg.column;
                aggregate_row[col] = agg
                    .function
                    .apply(&aggregate_row[col], &row[col])
                    .expect("no fail");
            }

            self.advance_filtered();
        }

        self.out_row = Some(aggregate_row)
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.out_row.as_deref()
    }
}

impl StreamingIterator for ResultIteratorInner {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        match self {
            ResultIteratorInner::OwnedResults(i) => i.advance(),
            ResultIteratorInner::MultiKey(i) => i.advance(),
            ResultIteratorInner::MultiKeyMerge(i) => i.advance(),
            ResultIteratorInner::MultiKeyAggregateMerge(i) => i.advance(),
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        match &self {
            ResultIteratorInner::OwnedResults(i) => i.get(),
            ResultIteratorInner::MultiKey(i) => i.get(),
            ResultIteratorInner::MultiKeyMerge(i) => i.get(),
            ResultIteratorInner::MultiKeyAggregateMerge(i) => i.get(),
        }
    }
}

impl StreamingIterator for ResultIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        if let Some(offset) = self.offset.take() {
            for _ in 0..offset {
                self.advance_filtered();
                if self.inner.get().is_none() {
                    break;
                }
            }
        }

        if let Some(limit) = self.limit.as_mut() {
            *limit = limit.wrapping_sub(1);
        }

        self.advance_filtered();
        // If after the first advance get returns None the default row should be returned, otherwise
        // the default row should be removed
        if self.non_empty {
            self.default_row.take();
        } else {
            self.non_empty = true;
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        if self.limit == Some(usize::MAX) {
            // limit exists, and wraped around, so we are done here
            None
        } else {
            self.inner
                .get()
                .or_else(|| self.default_row.as_ref().map(|r| &r[..]))
                .map(|row| {
                    // Why is there no slice truncate?
                    if row.len() <= self.cols {
                        row
                    } else {
                        &row[..self.cols]
                    }
                })
        }
    }
}

impl IntoIterator for ResultIterator {
    type Item = Vec<DataType>;
    type IntoIter = impl Iterator<Item = Vec<DataType>>;

    /// Convert to an iterator over owned rows (rows are cloned)
    fn into_iter(self) -> Self::IntoIter {
        self.map_deref(|i| i.to_vec())
    }
}

impl ResultIterator {
    /// Collect the results into a vector (rows are cloned)
    pub fn into_vec(self) -> Vec<Vec<DataType>> {
        self.into_iter().collect()
    }
}

impl From<ResultIterator> for Vec<Vec<DataType>> {
    fn from(iter: ResultIterator) -> Self {
        iter.into_vec()
    }
}
