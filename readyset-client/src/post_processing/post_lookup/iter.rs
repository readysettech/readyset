use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use dataflow_expression::grouped::accumulator::AccumulatorData;
use dataflow_expression::Expr;
use metrics::histogram;
use readyset_data::DfValue;
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use streaming_iterator::StreamingIterator;
use tournament_kway::{Comparator, StreamingTournament};

use crate::post_processing::post_lookup::spec::{
    PostLookup, PostLookupAggregates, PostLookupDistinct,
};

/// Stats returned with a reader read reply.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct ReadReplyStats {
    /// The count of cache misses which have occurred
    pub cache_misses: u64,
}

impl ReadReplyStats {
    /// Creates a new [`ReadReplyStats`]
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            cache_misses: self.cache_misses + other.cache_misses,
        }
    }
}

/// A lookup key into a reader
pub type Key = Box<[DfValue]>;

/// A single row in a result set
pub type Row = Box<[DfValue]>;

/// A shared set of rows, returned for a given single key in a reader, under a [`triomphe::Arc`]
pub type SharedRows = triomphe::Arc<SmallVec<[Row; 1]>>;

/// A list of [`SharedRows`], combining the lookup results for multiple keys
pub type SharedResults = SmallVec<[SharedRows; 1]>;

/// A set of uniquely owned results
#[derive(Debug)]
pub struct Results {
    results: Vec<Vec<DfValue>>,
    /// When present, contains stats related to the operation
    pub stats: Option<ReadReplyStats>,
}

impl Results {
    pub fn new(results: Vec<Vec<DfValue>>) -> Self {
        Self {
            results,
            stats: None,
        }
    }

    pub fn with_stats(results: Vec<Vec<DfValue>>, stats: ReadReplyStats) -> Self {
        Self {
            results,
            stats: Some(stats),
        }
    }

    pub fn into_data(self) -> Vec<Vec<DfValue>> {
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
    /// How many columns to return; `None` means emit the row unchanged.
    cols: Option<usize>,
    /// Set of already-emitted rows for DISTINCT deduplication
    /// Only allocated when needed i.e. when dedup uses Unsorted/Hash-based approach
    seen: Option<HashSet<Vec<DfValue>>>,
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
    row: Option<usize>,
}

/// An iterator over a single set of cached results
#[derive(Debug)]
struct SingleKeyIterator {
    // The underlying data we iterate over
    data: SharedRows,
    // The next row in the current set to return
    row: Option<usize>,
}

/// An iterator over a multiple sets of cached results
#[derive(Debug)]
struct MultiKeyIterator {
    // The underlying data we iterate over
    data: SharedResults,
    // The current set of result in the iterator
    set: usize,
    // The next row in the current set to return
    row: Option<usize>,
}

/// An iterator over multiple sets of cached results with an order by clause
#[derive(Debug)]
struct MergeIterator {
    // Tournament tree that merges multiple ordered sets of results
    inner: StreamingTournament<SingleKeyIterator, RowComparator>,
}

#[derive(Clone, Debug)]
struct RowComparator {
    order_by: Arc<[(usize, OrderType, NullOrder)]>,
}

impl Comparator<[DfValue]> for RowComparator {
    fn cmp(&self, a: &[DfValue], b: &[DfValue]) -> Ordering {
        self.order_by
            .iter()
            .map(|&(idx, order_type, null_order)| {
                null_order
                    .apply(a[idx].is_none(), b[idx].is_none())
                    .then(order_type.apply(a[idx].cmp(&b[idx])))
            })
            .fold(Ordering::Equal, |acc, next| acc.then(next))
    }
}

#[derive(Debug)]
struct AggregateIterator {
    inner: Box<ResultIteratorInner>,
    aggregate: PostLookupAggregates,
    out_row: Option<Vec<DfValue>>,
    filter: Option<Expr>,
    /// Cached: true if any aggregate has `raw_values` set.
    has_raw: bool,
}

/// Pick the [`PostLookupAggregates`] the iterator pipeline should aggregate by.
///
/// `Sorted` `DISTINCT` injects a synthetic aggregate (group-by all projected columns,
/// no aggregate functions) so the existing merge-then-collapse machinery handles it
/// for free. Otherwise the real `aggregates` from the spec, or `None` if neither
/// applies. Invariant: `Sorted` is never combined with real aggregates (enforced by
/// `ReaderProcessing::new`).
pub fn effective_aggregates(post_lookup: &PostLookup) -> Option<&PostLookupAggregates> {
    match &post_lookup.distinct {
        PostLookupDistinct::Sorted { dedup_aggregates } => {
            debug_assert!(post_lookup.aggregates.is_none());
            debug_assert!(!dedup_aggregates.group_by.is_empty());
            Some(dedup_aggregates)
        }
        _ => post_lookup.aggregates.as_ref(),
    }
}

impl ResultIterator {
    /// Build the iterator's *inner* pipeline (k-way merge + aggregation), routing
    /// `filter` and `default_row` to the level that gives them the right semantics:
    ///
    /// - When an aggregate is set up, `filter` is pushed *into* the `AggregateIterator`
    ///   so it gates raw rows before they're aggregated (`WHERE` semantics).
    /// - In the eager `(order_by, aggregate)` branch, `default_row` is attached to the
    ///   temp iterator so `into_vec` materialises the default into the sorted output
    ///   when the inner is empty.
    /// - Otherwise both fall through to the outer iterator's fields.
    ///
    /// Limit, offset, projection, and hash-dedup remain outer stages; chain them via
    /// [`with_limit`](Self::with_limit), [`with_offset`](Self::with_offset),
    /// [`with_projection`](Self::with_projection), and
    /// [`with_hash_dedup`](Self::with_hash_dedup). The orchestrator
    /// [`crate::post_processing::post_lookup::run_post_processing_pipeline`] is the
    /// canonical place to see the full sequence.
    ///
    /// `effective_aggregates` is what the caller picks via [`effective_aggregates`]: real
    /// `PostLookupAggregates` for aggregating queries, the synthetic dedup aggregate for
    /// `Sorted` `DISTINCT`, or `None` otherwise. `Sorted` dedup is handled here because it
    /// folds into the merge; `HashBased` dedup is a *separate* outer stage.
    pub fn pipeline(
        data: SharedResults,
        order_by: Option<&Vec<(usize, OrderType, NullOrder)>>,
        effective_aggregates: Option<&PostLookupAggregates>,
        mut filter: Option<Expr>,
        default_row: Option<&Arc<Row>>,
    ) -> Self {
        let inner = match (order_by, effective_aggregates) {
            // No data in the result set, so return as simply as possible.
            (_, _) if data.is_empty() => ResultIteratorInner::MultiKey(MultiKeyIterator::new(data)),
            // No specific order is required, simply iterate over each result set one by one
            (None, None) => ResultIteratorInner::MultiKey(MultiKeyIterator::new(data)),
            // if there's an order by clause, yet the result set has only one row for a single key,
            // return a simple iterator as there's nothing to order (it's a single row)
            (Some(_), None) if data.len() == 1 && data.first().is_some_and(|v| v.len() == 1) => {
                ResultIteratorInner::MultiKey(MultiKeyIterator::new(data))
            }
            (Some(order_by), None) => {
                // Order by is specified, merge results using a k-way merge iterator
                let comparator = RowComparator {
                    order_by: order_by.clone().into(),
                };

                debug_assert!(data
                    .iter()
                    .all(|s| { s.is_sorted_by(|a, b| comparator.cmp(a, b).is_le()) }));

                ResultIteratorInner::MultiKeyMerge(MergeIterator::new(data, comparator))
            }
            // If there's a real aggregation (not just sorted dedup) and only one key in the
            // result set, we can skip re-aggregation since the dataflow graph already computed it.
            // We must NOT skip when aggregates.aggregates is empty — that's the sorted dedup
            // path, which still needs the AggregateIterator to collapse duplicate rows.
            (None, Some(aggregates)) if data.len() == 1 && !aggregates.aggregates.is_empty() => {
                if aggregates.aggregates.iter().any(|a| a.raw_values) {
                    // Raw values need finalization even for single-key lookups
                    let rows: Vec<Vec<DfValue>> = data[0]
                        .iter()
                        .map(|row| {
                            let mut row = row.to_vec();
                            for agg in &aggregates.aggregates {
                                if agg.raw_values {
                                    row[agg.column] = agg
                                        .function
                                        .finalize_raw(&row[agg.column])
                                        .expect("finalize_raw failed");
                                }
                            }
                            row
                        })
                        .collect();
                    ResultIteratorInner::OwnedResults(OwnedResultIterator {
                        data: vec![Results::new(rows)],
                        set: 0,
                        row: None,
                    })
                } else {
                    ResultIteratorInner::MultiKey(MultiKeyIterator::new(data))
                }
            }
            (None, Some(aggregates)) => {
                let has_raw = aggregates.aggregates.iter().any(|a| a.raw_values);

                if aggregates.group_by.is_empty() {
                    // No group by means just iterate over the results and aggregate
                    ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKey(MultiKeyIterator::new(data))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                        has_raw,
                    })
                } else {
                    // With group by, merge results using a k-way merge iterator on group-by
                    // predicates and aggregate
                    let comparator = RowComparator {
                        order_by: aggregates
                            .group_by
                            .iter()
                            .map(|&col| (col, OrderType::OrderAscending, NullOrder::NullsFirst))
                            .collect(),
                    };

                    debug_assert!(data
                        .iter()
                        .all(|s| { s.is_sorted_by(|a, b| comparator.cmp(a, b).is_le()) }));

                    ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKeyMerge(MergeIterator::new(
                            data, comparator,
                        ))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                        has_raw,
                    })
                }
            }
            // if there's an order by clause with an aggregate, yet the result set has only one row
            // for a single key, return a simple iterator as there's nothing to order
            // (it's a single row)
            (Some(_), Some(_)) if data.len() == 1 && data.first().is_some_and(|v| v.len() == 1) => {
                ResultIteratorInner::MultiKey(MultiKeyIterator::new(data))
            }
            (Some(order_by), Some(aggregates)) => {
                // When both aggregates and order by are specified it is tricky to lazily evaluate
                // rows, so sadly we end up having to collect all of the rows, aggregate, then sort
                // them
                let comparator = RowComparator {
                    order_by: aggregates
                        .group_by
                        .iter()
                        .map(|&col| (col, OrderType::OrderAscending, NullOrder::NullsFirst))
                        .collect(),
                };

                debug_assert!(data
                    .iter()
                    .all(|s| { s.is_sorted_by(|a, b| comparator.cmp(a, b).is_le()) }));

                // `default_row` rides on the temp iterator: when the aggregate
                // produces no rows, `into_vec` materialises the default into the
                // sorted output before the outer-stage builders see it.
                let temp_iter = ResultIterator {
                    inner: ResultIteratorInner::MultiKeyAggregateMerge(AggregateIterator {
                        inner: Box::new(ResultIteratorInner::MultiKeyMerge(MergeIterator::new(
                            data, comparator,
                        ))),
                        out_row: None,
                        aggregate: aggregates.clone(),
                        filter: filter.take(),
                        has_raw: aggregates.aggregates.iter().any(|a| a.raw_values),
                    }),
                    limit: None,
                    offset: None,
                    default_row: default_row.cloned(),
                    non_empty: false,
                    filter: None,
                    cols: None,
                    seen: None,
                };

                let mut results = temp_iter.into_vec();

                results.sort_by(|a, b| {
                    order_by
                        .iter()
                        .map(|&(idx, order_type, null_order)| {
                            null_order
                                .apply(a[idx].is_none(), b[idx].is_none())
                                .then(order_type.apply(a[idx].cmp(&b[idx])))
                        })
                        .fold(Ordering::Equal, |acc, next| acc.then(next))
                });

                // The outer-stage builders (`with_hash_dedup`, `with_limit`, `with_offset`,
                // `with_projection`) apply on top of this owned result.
                return ResultIterator::owned(vec![Results {
                    results,
                    stats: None,
                }]);
            }
        };

        ResultIterator {
            inner,
            limit: None,
            offset: None,
            default_row: default_row.cloned(),
            non_empty: false,
            // Filter is `None` here whenever the inner is an `AggregateIterator` (we
            // routed it inside above so it gates raw rows pre-aggregation); otherwise
            // it sits on the outer iterator and runs per emitted row.
            filter,
            cols: None,
            seen: None,
        }
    }

    /// Set the row count cap; `None` clears it.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Skip this many rows from the beginning of the output; `None` clears it.
    pub fn with_offset(mut self, offset: Option<usize>) -> Self {
        self.offset = offset;
        self
    }

    /// Truncate each emitted row to this many columns. `None` keeps the row width unchanged.
    pub fn with_projection(mut self, returned_cols: Option<&[usize]>) -> Self {
        self.cols = returned_cols.map(|r| r.len());
        self
    }

    /// Engage HashBased `DISTINCT` dedup over the rows this iterator yields.
    ///
    /// The same `seen` HashSet machinery the iterator already uses for dedup,
    /// just exposed as a stage so the orchestrator can place it relative to
    /// other stages (notably `postprocess_decompositions`). Streaming, no extra
    /// materialization.
    pub fn with_hash_dedup(mut self) -> Self {
        self.seen = Some(HashSet::new());
        self
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
            cols: None,
            seen: None,
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
    /// or that have already been emitted (when DISTINCT dedup is active).
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
            // HashBased DISTINCT dedup: skip rows we've already emitted. Only consider
            // the projected columns (first `self.cols`), since column projection
            // happens later in get() and non-projected columns (e.g. primary key)
            // would make every row appear unique.
            //
            // Memory note: the HashSet grows to O(unique_rows). This is only used
            // for DISTINCT with aggregates, where the input is post-aggregation
            // output (bounded by the number of groups). For pathological cases,
            // monitor the POST_LOOKUP_DISTINCT_HASH_SET_SIZE metric.
            if let Some(ref mut seen) = self.seen {
                if let Some(row) = self.inner.get() {
                    let width = self.cols.unwrap_or(row.len()).min(row.len());
                    let projected = &row[..width];
                    if !seen.insert(projected.to_vec()) {
                        continue;
                    }
                } else {
                    // End of stream — record the final dedup set size.
                    // Not recorded when the consumer stops early (e.g. LIMIT),
                    // but LIMIT bounds the set size so that's not the case we
                    // need to monitor.
                    histogram!(metric::POST_LOOKUP_DISTINCT_HASH_SET_SIZE)
                        .record(seen.len() as f64);
                }
            }
            break;
        }
    }
}

impl StreamingIterator for OwnedResultIterator {
    type Item = [DfValue];

    #[inline(always)]
    fn advance(&mut self) {
        let row = match self.row {
            Some(ref mut row) => {
                *row += 1;
                row
            }
            None => self.row.get_or_insert(0),
        };
        while let Some(rows) = self.data.get(self.set) {
            if rows.results.get(*row).is_some() {
                break;
            }
            self.set += 1;
            *row = 0;
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row
            .and_then(|row| self.data.get(self.set).and_then(|s| s.results.get(row)))
            .map(|v| v.as_slice())
    }
}

impl SingleKeyIterator {
    pub(crate) fn new(data: SharedRows) -> Self {
        SingleKeyIterator { data, row: None }
    }
}

impl StreamingIterator for SingleKeyIterator {
    type Item = [DfValue];

    #[inline(always)]
    fn advance(&mut self) {
        match self.row {
            None => self.row = Some(0),
            Some(ref mut row) => *row += 1,
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row.and_then(|row| self.data.get(row)).map(|r| &r[..])
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
    type Item = [DfValue];

    #[inline(always)]
    fn advance(&mut self) {
        let row = match self.row {
            Some(ref mut row) => {
                *row += 1;
                row
            }
            None => self.row.get_or_insert(0),
        };

        while let Some(results) = self.data.get(self.set) {
            if results.get(*row).is_none() {
                // Skip empty sets
                self.set += 1;
                *row = 0;
            } else {
                break;
            }
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.row
            .and_then(|row| self.data.get(self.set).and_then(|s| s.get(row)))
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
    type Item = [DfValue];

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

#[derive(Debug)]
enum AggregateHolder {
    Simple(DfValue),
    Accumulated(AccumulatorData),
}

impl AggregateHolder {
    fn finish(
        self,
        function: &crate::post_processing::post_lookup::spec::PostLookupAggregateFunction,
    ) -> DfValue {
        match self {
            AggregateHolder::Simple(v) => v,
            AggregateHolder::Accumulated(mut data) => function
                .finish(&mut data)
                .expect("Accumulated data expected"),
        }
    }
}

impl StreamingIterator for AggregateIterator {
    type Item = [DfValue];

    #[inline(always)]
    fn advance(&mut self) {
        // First assign the next row if possible
        if self.out_row.is_none() {
            // Probably first advance
            self.advance_filtered();
        }

        // Reuse the previous out_row's Vec allocation when possible to avoid
        // a fresh heap allocation on every advance() call.
        let mut aggregate_row = match self.inner.get() {
            Some(row) => {
                if let Some(mut prev) = self.out_row.take() {
                    prev.clear();
                    prev.extend_from_slice(row);
                    prev
                } else {
                    row.to_vec()
                }
            }
            None => {
                self.out_row = None;
                return;
            }
        };

        self.advance_filtered();
        let has_raw = self.has_raw;
        let mut aggs: Option<Vec<AggregateHolder>> = None;
        while let Some(row) = self.inner.get() {
            if self
                .aggregate
                .group_by
                .iter()
                .any(|&i| aggregate_row.get(i) != row.get(i))
            {
                break;
            }

            // Initialize aggregates on first row
            if aggs.is_none() {
                let mut holders = Vec::new();
                for agg in &self.aggregate.aggregates {
                    let col = agg.column;
                    let holder = if agg.function.is_accumulation() {
                        let mut acc_data: AccumulatorData = agg
                            .function
                            .create_accumulator_data()
                            .expect("AccumulatorData");

                        if agg.raw_values {
                            agg.function
                                .apply_raw_accumulated(&mut acc_data, &aggregate_row[col])
                                .expect("Raw accumulate failed");
                        } else {
                            agg.function
                                .apply_accumulated(&mut acc_data, &aggregate_row[col])
                                .expect("Accumulate failed");
                        }
                        AggregateHolder::Accumulated(acc_data)
                    } else {
                        AggregateHolder::Simple(aggregate_row[col].clone())
                    };
                    holders.push(holder);
                }
                aggs = Some(holders);
            }

            // Apply/accumulate for this row
            if let Some(ref mut holders) = aggs {
                for (holder, agg) in holders.iter_mut().zip(&self.aggregate.aggregates) {
                    let col = agg.column;
                    match holder {
                        AggregateHolder::Simple(current) => {
                            *current = agg
                                .function
                                .apply(current, &row[col])
                                .expect("Apply failed");
                        }
                        AggregateHolder::Accumulated(data) => {
                            if agg.raw_values {
                                agg.function
                                    .apply_raw_accumulated(data, &row[col])
                                    .expect("Raw accumulate failed");
                            } else {
                                agg.function
                                    .apply_accumulated(data, &row[col])
                                    .expect("Accumulate failed");
                            }
                        }
                    }
                }
            }

            self.advance_filtered();
        }

        // Finish aggregates and write back to aggregate_row
        if let Some(holders) = aggs {
            for (holder, agg) in holders.into_iter().zip(&self.aggregate.aggregates) {
                let col = agg.column;
                aggregate_row[col] = holder.finish(&agg.function);
            }
        } else if has_raw {
            // Single-row group with raw values: use fast finalize_raw path
            // which avoids creating AccumulatorData entirely.
            for agg in &self.aggregate.aggregates {
                if agg.raw_values {
                    aggregate_row[agg.column] = agg
                        .function
                        .finalize_raw(&aggregate_row[agg.column])
                        .expect("finalize_raw failed");
                }
            }
        }

        self.out_row = Some(aggregate_row)
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.out_row.as_deref()
    }
}

impl StreamingIterator for ResultIteratorInner {
    type Item = [DfValue];

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
    type Item = [DfValue];

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
                .map(|row| match self.cols {
                    Some(cols) if cols < row.len() => &row[..cols],
                    _ => row,
                })
        }
    }
}

impl IntoIterator for ResultIterator {
    type Item = Vec<DfValue>;
    type IntoIter = Box<dyn Iterator<Item = Vec<DfValue>> + Send>;

    /// Convert to an iterator over owned rows (rows are cloned)
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.map_deref(|i| i.to_vec()))
    }
}

impl ResultIterator {
    /// Collect the results into a vector (rows are cloned)
    pub fn into_vec(self) -> Vec<Vec<DfValue>> {
        self.into_iter().collect()
    }
}

impl From<ResultIterator> for Vec<Vec<DfValue>> {
    fn from(iter: ResultIterator) -> Self {
        iter.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_processing::post_lookup::spec::{
        PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    };
    use dataflow_expression::grouped::accumulator::AccumulationOp;

    /// Build SharedResults from a list of key result sets.
    /// Each element of `keys_data` is the set of rows for one lookup key.
    fn make_shared_results(keys_data: Vec<Vec<Vec<DfValue>>>) -> SharedResults {
        keys_data
            .into_iter()
            .map(|rows| {
                let boxed_rows: SmallVec<[Row; 1]> =
                    rows.into_iter().map(|row| row.into_boxed_slice()).collect();
                triomphe::Arc::new(boxed_rows)
            })
            .collect()
    }

    fn make_post_lookup(aggregates: PostLookupAggregates) -> PostLookup {
        PostLookup {
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: Some(aggregates),
            distinct: PostLookupDistinct::None,
        }
    }

    fn make_string_agg_function(separator: &str) -> PostLookupAggregateFunction {
        PostLookupAggregateFunction::StringAgg {
            op: AccumulationOp::StringAgg {
                separator: Some(separator.to_string()),
                distinct: false.into(),
                order_by: None,
            },
        }
    }

    fn make_array_agg_function() -> PostLookupAggregateFunction {
        PostLookupAggregateFunction::ArrayAgg {
            op: AccumulationOp::ArrayAgg {
                distinct: false.into(),
                order_by: None,
            },
        }
    }

    fn make_distinct_string_agg_function(separator: &str) -> PostLookupAggregateFunction {
        PostLookupAggregateFunction::StringAgg {
            op: AccumulationOp::StringAgg {
                separator: Some(separator.to_string()),
                distinct: true.into(),
                order_by: None,
            },
        }
    }

    fn collect_results(
        data: SharedResults,
        post_lookup: &PostLookup,
        offset: Option<usize>,
    ) -> Vec<Vec<DfValue>> {
        let mut iter = ResultIterator::pipeline(
            data,
            post_lookup.order_by.as_ref(),
            effective_aggregates(post_lookup),
            None,
            post_lookup.default_row.as_ref(),
        );
        if matches!(post_lookup.distinct, PostLookupDistinct::HashBased) {
            iter = iter.with_hash_dedup();
        }
        iter.with_limit(post_lookup.limit)
            .with_offset(offset)
            .with_projection(post_lookup.returned_cols.as_deref())
            .into_vec()
    }

    #[test]
    fn test_multi_key_string_agg() {
        // 3 keys, no group_by -> all should merge into one row
        let data = make_shared_results(vec![
            vec![vec![1.into(), "a,b".into()]],
            vec![vec![1.into(), "c,d".into()]],
            vec![vec![1.into(), "e".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        for val in &["a", "b", "c", "d", "e"] {
            assert!(merged.contains(val), "missing '{}' in '{}'", val, merged);
        }
    }

    #[test]
    fn test_multi_key_array_agg() {
        let data = make_shared_results(vec![
            vec![vec![
                1.into(),
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from("a"),
                    DfValue::from("b"),
                ]))),
            ]],
            vec![vec![
                1.into(),
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from("c"),
                ]))),
            ]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_array_agg_function(),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        match &results[0][1] {
            DfValue::Array(arr) => {
                let vals: Vec<_> = arr.values().collect();
                assert_eq!(vals.len(), 3);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_single_key_shortcircuit() {
        // With only 1 key, AggregateIterator is NOT used (line 210 shortcircuit)
        let data = make_shared_results(vec![vec![vec![1.into(), "a,b".into()]]]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][1], DfValue::from("a,b"));
    }

    #[test]
    fn test_multi_key_with_group_by() {
        // 4 keys, 2 groups (group_by column 0)
        let data = make_shared_results(vec![
            vec![vec![1.into(), "a".into()]],
            vec![vec![1.into(), "b".into()]],
            vec![vec![2.into(), "c".into()]],
            vec![vec![2.into(), "d".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![0],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
        let g1 = &results[0];
        let g2 = &results[1];
        assert_eq!(g1[0], DfValue::from(1));
        assert_eq!(g2[0], DfValue::from(2));
        let g1_str = g1[1].to_string();
        assert!(g1_str.contains("a") && g1_str.contains("b"));
        let g2_str = g2[1].to_string();
        assert!(g2_str.contains("c") && g2_str.contains("d"));
    }

    #[test]
    fn test_multi_key_no_group_by() {
        let data = make_shared_results(vec![
            vec![vec![1.into(), "x".into()]],
            vec![vec![2.into(), "y".into()]],
            vec![vec![3.into(), "z".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        assert!(merged.contains("x") && merged.contains("y") && merged.contains("z"));
    }

    #[test]
    fn test_split_lossiness_bug() {
        // Documents a known bug: when a value contains the separator character,
        // split() produces incorrect results.
        let data = make_shared_results(vec![
            // Key 1: value is "a,b" (single value that happens to contain separator)
            vec![vec![1.into(), "a,b".into()]],
            // Key 2: value is "c"
            vec![vec![1.into(), "c".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        // BUG: "a,b" was split into "a" and "b", so we get 3 parts instead of 2.
        let parts: Vec<&str> = merged.split(',').collect();
        assert_eq!(
            parts.len(),
            3,
            "Lossiness bug: 'a,b' was split into parts. Got: {}",
            merged
        );
    }

    #[test]
    fn test_multi_key_with_distinct() {
        // Two keys with overlapping values; distinct should deduplicate
        let data = make_shared_results(vec![
            vec![vec![1.into(), "a".into()]],
            vec![vec![1.into(), "a".into()]], // duplicate value
            vec![vec![1.into(), "b".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_distinct_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        let parts: Vec<&str> = merged.split(',').collect();
        assert_eq!(
            parts.len(),
            2,
            "Expected 2 distinct values, got: {}",
            merged
        );
        assert!(merged.contains("a") && merged.contains("b"));
    }

    #[test]
    fn test_single_row_per_group_passthrough() {
        // With group_by, each group has only one key/row -> lazy init never triggers
        let data = make_shared_results(vec![
            vec![vec![1.into(), "only_one".into()]],
            vec![vec![2.into(), "also_one".into()]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![0],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_string_agg_function(","),
                raw_values: false,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][1], DfValue::from("only_one"));
        assert_eq!(results[1][1], DfValue::from("also_one"));
    }

    fn make_raw_string_agg_agg(separator: &str) -> PostLookupAggregate {
        PostLookupAggregate {
            column: 1,
            function: make_string_agg_function(separator),
            raw_values: true,
        }
    }

    fn make_raw_array(values: Vec<&str>) -> DfValue {
        DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(
            values.into_iter().map(DfValue::from).collect::<Vec<_>>(),
        )))
    }

    #[test]
    fn test_raw_multi_key_string_agg() {
        let data = make_shared_results(vec![
            vec![vec![1.into(), make_raw_array(vec!["a", "b"])]],
            vec![vec![1.into(), make_raw_array(vec!["c", "d"])]],
            vec![vec![1.into(), make_raw_array(vec!["e"])]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![make_raw_string_agg_agg(",")],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        for val in &["a", "b", "c", "d", "e"] {
            assert!(merged.contains(val), "missing '{}' in '{}'", val, merged);
        }
    }

    #[test]
    fn test_raw_single_key_finalization() {
        // Single key with raw values should still finalize
        let data = make_shared_results(vec![vec![vec![
            1.into(),
            make_raw_array(vec!["x", "y", "z"]),
        ]]]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![make_raw_string_agg_agg(",")],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let val = results[0][1].to_string();
        assert!(val.contains("x") && val.contains("y") && val.contains("z"));
        // Should be a string, not an array
        assert!(!matches!(results[0][1], DfValue::Array(_)));
    }

    #[test]
    fn test_raw_single_row_in_group() {
        // With group_by, each group has one row with raw values -> eager init finalizes
        let data = make_shared_results(vec![
            vec![vec![1.into(), make_raw_array(vec!["a", "b"])]],
            vec![vec![2.into(), make_raw_array(vec!["c"])]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![0],
            aggregates: vec![make_raw_string_agg_agg(",")],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
        let g1 = results[0][1].to_string();
        let g2 = results[1][1].to_string();
        assert!(g1.contains("a") && g1.contains("b"));
        assert_eq!(g2, "c");
    }

    #[test]
    fn test_raw_value_containing_separator() {
        // This is the lossiness fix: raw values containing separator are handled correctly
        let data = make_shared_results(vec![
            // Key 1: raw array has "a,b" as a single value
            vec![vec![1.into(), make_raw_array(vec!["a,b"])]],
            // Key 2: raw array has "c"
            vec![vec![1.into(), make_raw_array(vec!["c"])]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![make_raw_string_agg_agg(",")],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        // With raw path, "a,b" stays as one value, so result is "a,b,c" (2 original values)
        // NOT "a,b,c" from 3 values like the split() bug would produce
        assert_eq!(merged, "a,b,c");
    }

    #[test]
    fn test_raw_with_distinct() {
        let data = make_shared_results(vec![
            vec![vec![1.into(), make_raw_array(vec!["a", "b"])]],
            vec![vec![1.into(), make_raw_array(vec!["a", "c"])]],
        ]);
        let post_lookup = make_post_lookup(PostLookupAggregates {
            group_by: vec![],
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: make_distinct_string_agg_function(","),
                raw_values: true,
            }],
        });

        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        let merged = results[0][1].to_string();
        let parts: Vec<&str> = merged.split(',').collect();
        assert_eq!(
            parts.len(),
            3,
            "Expected 3 distinct values (a,b,c), got: {}",
            merged
        );
    }

    #[test]
    fn test_distinct_sorted_dedup() {
        let data = make_shared_results(vec![
            vec![vec![10.into(), "a".into()]],
            vec![vec![20.into(), "b".into()]],
            vec![vec![10.into(), "c".into()]],
            vec![vec![20.into(), "d".into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: Some(vec![0]),
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
        let mut vals: Vec<i64> = results
            .iter()
            .map(|r| i64::try_from(&r[0]).expect("int"))
            .collect();
        vals.sort();
        assert_eq!(vals, vec![10, 20]);
    }

    #[test]
    fn test_distinct_sorted_multi_column() {
        let data = make_shared_results(vec![
            vec![vec![1.into(), 10.into()]],
            vec![vec![2.into(), 20.into()]],
            vec![vec![1.into(), 10.into()]],
            vec![vec![2.into(), 20.into()]],
            vec![vec![3.into(), 30.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0, 1],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_distinct_hash_based_dedup() {
        // 4 groups after aggregation: (cat1,1), (cat2,3), (cat3,3), (cat4,1)
        // returned_cols=[0,1] projects both → all 4 unique → no dedup
        let data = make_shared_results(vec![
            vec![vec![1.into(), 1.into()]],
            vec![vec![2.into(), 3.into()]],
            vec![vec![3.into(), 3.into()]],
            vec![vec![4.into(), 1.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: Some(vec![0, 1]),
            default_row: None,
            aggregates: Some(PostLookupAggregates {
                group_by: vec![0],
                aggregates: vec![PostLookupAggregate {
                    column: 1,
                    function: PostLookupAggregateFunction::Sum,
                    raw_values: false,
                }],
            }),
            distinct: PostLookupDistinct::HashBased,
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_distinct_hash_based_dedup_removes_duplicates() {
        // Simulate SELECT DISTINCT count FROM t WHERE ... GROUP BY cat
        // where the count column is col 0 and the group-by key is col 1.
        // 4 groups: (count=1,cat1), (count=3,cat2), (count=3,cat3), (count=1,cat4)
        // returned_cols=[0] projects only the count → [1],[3],[3],[1] → dedup → {1,3}
        let data = make_shared_results(vec![
            vec![vec![1.into(), 1.into()]],
            vec![vec![3.into(), 2.into()]],
            vec![vec![3.into(), 3.into()]],
            vec![vec![1.into(), 4.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: Some(vec![0]),
            default_row: None,
            aggregates: Some(PostLookupAggregates {
                group_by: vec![1],
                aggregates: vec![PostLookupAggregate {
                    column: 0,
                    function: PostLookupAggregateFunction::Sum,
                    raw_values: false,
                }],
            }),
            distinct: PostLookupDistinct::HashBased,
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_distinct_empty_results() {
        let data = make_shared_results(vec![]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_distinct_hash_based_single_key() {
        // Single key — aggregation is skipped (data already aggregated in dataflow).
        // Pre-aggregated data: one row per group.
        let data = make_shared_results(vec![vec![vec![42.into()]]]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: Some(PostLookupAggregates {
                group_by: vec![],
                aggregates: vec![PostLookupAggregate {
                    column: 0,
                    function: PostLookupAggregateFunction::Sum,
                    raw_values: false,
                }],
            }),
            distinct: PostLookupDistinct::HashBased,
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
        assert_eq!(i64::try_from(&results[0][0]).expect("int"), 42);
    }

    #[test]
    fn test_distinct_sorted_single_key() {
        // Single key with duplicate rows — sorted dedup must still work.
        // Data is pre-sorted by col 0 (as PreInsertion would ensure in production).
        let data = make_shared_results(vec![vec![
            vec![10.into()],
            vec![10.into()],
            vec![20.into()],
        ]]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_distinct_hash_based_with_order_by() {
        let data = make_shared_results(vec![
            vec![vec![1.into(), 1.into()]],
            vec![vec![2.into(), 3.into()]],
            vec![vec![3.into(), 3.into()]],
            vec![vec![4.into(), 1.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: Some(vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)]),
            limit: None,
            returned_cols: Some(vec![0, 1]),
            default_row: None,
            aggregates: Some(PostLookupAggregates {
                group_by: vec![0],
                aggregates: vec![PostLookupAggregate {
                    column: 1,
                    function: PostLookupAggregateFunction::Sum,
                    raw_values: false,
                }],
            }),
            distinct: PostLookupDistinct::HashBased,
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 4);
        let col1_vals: Vec<i64> = results
            .iter()
            .map(|r| i64::try_from(&r[1]).expect("int"))
            .collect();
        assert_eq!(col1_vals, vec![1, 1, 3, 3]);
    }

    #[test]
    fn test_distinct_sorted_with_order_by() {
        // Sorted dedup through the eager path (order_by + aggregates).
        // Data: 5 keys, col 0 has duplicates. Pre-sorted per-key by col 0.
        let data = make_shared_results(vec![
            vec![vec![10.into()]],
            vec![vec![20.into()]],
            vec![vec![10.into()]],
            vec![vec![30.into()]],
            vec![vec![20.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: Some(vec![(0, OrderType::OrderAscending, NullOrder::NullsFirst)]),
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 3);
        let vals: Vec<i64> = results
            .iter()
            .map(|r| i64::try_from(&r[0]).expect("int"))
            .collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn test_distinct_sorted_with_limit() {
        // Sorted dedup + LIMIT: dedup first, then limit.
        // 5 keys with duplicates → 3 unique after dedup → LIMIT 2 → 2 rows.
        let data = make_shared_results(vec![
            vec![vec![10.into()]],
            vec![vec![20.into()]],
            vec![vec![10.into()]],
            vec![vec![30.into()]],
            vec![vec![20.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: Some(2),
            returned_cols: None,
            default_row: None,
            aggregates: None,
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![],
                },
            },
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_distinct_hash_based_with_limit() {
        // HashBased dedup + LIMIT: dedup first, then limit.
        // 4 groups: (1,cat1),(3,cat2),(3,cat3),(1,cat4)
        // returned_cols=[0] → projected: [1],[3],[3],[1] → dedup → {1,3} → LIMIT 1 → 1 row
        let data = make_shared_results(vec![
            vec![vec![1.into(), 1.into()]],
            vec![vec![3.into(), 2.into()]],
            vec![vec![3.into(), 3.into()]],
            vec![vec![1.into(), 4.into()]],
        ]);
        let post_lookup = PostLookup {
            order_by: None,
            limit: Some(1),
            returned_cols: Some(vec![0]),
            default_row: None,
            aggregates: Some(PostLookupAggregates {
                group_by: vec![1],
                aggregates: vec![PostLookupAggregate {
                    column: 0,
                    function: PostLookupAggregateFunction::Sum,
                    raw_values: false,
                }],
            }),
            distinct: PostLookupDistinct::HashBased,
        };
        let results = collect_results(data, &post_lookup, None);
        assert_eq!(results.len(), 1);
    }
}
