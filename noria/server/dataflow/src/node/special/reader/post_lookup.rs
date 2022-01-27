use std::borrow::Cow;
use std::cmp::{self, Ordering};
use std::collections::{hash_map, HashMap};
use std::convert::{TryFrom, TryInto};

use common::DataType;
use itertools::Either;
use launchpad::Indices;
use nom_sql::OrderType;
use noria::util::like::{CaseSensitivityMode, LikePattern};
use noria::{ViewQueryFilter, ViewQueryOperator};
use serde::{Deserialize, Serialize};

/// Representation of an aggregate function
// TODO(grfn): It would be really nice to deduplicate this somehow with the grouped operator itself
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum PostLookupAggregateFunction {
    /// Add together all the input numbers
    ///
    /// Note that this encapsulates both `SUM` *and* `COUNT` in base SQL, as re-aggregating counts
    /// is done by just summing numbers together
    Sum,
    /// Multiply together all the input numbers
    Product,
    /// Concatenate together all the input strings with the given separator
    GroupConcat { separator: String },
    /// Take the maximum input value
    Max,
    /// Take the minimum input value
    Min,
}

impl PostLookupAggregateFunction {
    /// Apply this aggregate function to the two input values
    ///
    /// This forms a semigroup.
    fn apply(&self, val1: &DataType, val2: &DataType) -> DataType {
        match self {
            PostLookupAggregateFunction::Sum => (val1 + val2).unwrap(),
            PostLookupAggregateFunction::Product => (val1 * val2).unwrap(),
            PostLookupAggregateFunction::GroupConcat { separator } => format!(
                "{}{}{}",
                String::try_from(val1).unwrap(),
                separator,
                String::try_from(val2).unwrap()
            )
            .into(),
            PostLookupAggregateFunction::Max => cmp::max(val1, val2).clone(),
            PostLookupAggregateFunction::Min => cmp::min(val1, val2).clone(),
        }
    }
}

/// Representation of a single aggregate function to be performed on a column post-lookup
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregate {
    /// The column index in the result set containing the already-aggregated values
    pub column: usize,
    /// The aggregate function to perform
    pub function: PostLookupAggregateFunction,
}

/// Representation of a set of multiple aggregate functions to be performed post-lookup
///
/// This is used for range queries, where lookups cover multiple grouped keys
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregates {
    /// The set of column indices to group the aggregate by
    pub group_by: Vec<usize>,
    /// The aggregate functions to perform
    pub aggregates: Vec<PostLookupAggregate>,
}

impl PostLookupAggregates {
    /// Process the given set of rows by performing the aggregates in self on all input values
    #[allow(clippy::unwrap_used, clippy::indexing_slicing)]
    // unfortunately, none of the post-lookup path can return an error right now, because of the way
    // misses get reported - we should fix that at some point, but for now the only choice we have
    // is unwrapping and indexing-slicing. Fortunately all of the unwrapping/panicking we're doing
    // will only happen if we get rows that're the wrong length or contain the wrong types.
    fn process<'a, I>(&self, iter: I) -> Vec<Vec<Cow<'a, DataType>>>
    where
        I: Iterator<Item = Vec<Cow<'a, DataType>>>,
    {
        let mut groups: HashMap<Vec<Cow<DataType>>, Vec<Cow<DataType>>> = HashMap::new();
        for row in iter {
            let group_key = row.cloned_indices(self.group_by.iter().copied()).unwrap();
            match groups.entry(group_key) {
                hash_map::Entry::Occupied(entry) => {
                    let out_row = entry.into_mut();
                    for agg in &self.aggregates {
                        out_row[agg.column] =
                            Cow::Owned(agg.function.apply(&out_row[agg.column], &row[agg.column]));
                    }
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(row);
                }
            }
        }
        groups.into_values().collect()
    }
}

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
    /// Aggregates to perform on the result set *after* it's retrieved from the reader.
    ///
    /// Note that currently these are only performed on each key individually, not the overall
    /// result set returned by all keys in a multi-key lookup
    pub aggregates: Option<PostLookupAggregates>,
}

impl PostLookup {
    /// Returns true if this set of post-lookup operations is a no-op
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Apply this set of post-lookup operations, plus an optional [`ViewQueryFilter`], to the given
    /// set of results returned from a lookup
    pub fn process<'a, 'b: 'a, I>(
        &'a self,
        iter: I,
        filters: &[ViewQueryFilter],
    ) -> Vec<Vec<Cow<'a, DataType>>>
    where
        I: Iterator<Item = &'b Box<[DataType]>> + ExactSizeIterator,
        Self: 'a,
    {
        let data = iter.map(|r| r.iter().map(Cow::Borrowed).collect::<Vec<_>>());
        if self.is_empty() && filters.is_empty() {
            return data.collect::<Vec<_>>();
        }

        // If no data is present AND we have default values (e.g. we're aggregating), we can
        // short-circuit here and just return the defaults.
        if data.len() == 0 {
            if let Some(defaults) = self.default_row.as_ref() {
                return vec![defaults.iter().map(Cow::Borrowed).collect()];
            }
        }

        let ordered_limited = do_order_limit(data, self.order_by.as_deref(), self.limit);
        let compiled_filters = filters
            .iter()
            .map(
                |filter @ ViewQueryFilter {
                     value,
                     operator,
                     column,
                 }| {
                    match operator {
                        ViewQueryOperator::Like => Either::Left((
                            LikePattern::new(
                                value
                                    .try_into()
                                    .expect("Unexpected type for value; expected string"),
                                CaseSensitivityMode::CaseSensitive,
                            ),
                            *column,
                        )),
                        ViewQueryOperator::ILike => Either::Left((
                            LikePattern::new(
                                value
                                    .try_into()
                                    .expect("Unexpected type for value; expected string"),
                                CaseSensitivityMode::CaseInsensitive,
                            ),
                            *column,
                        )),
                        _ => Either::Right(filter),
                    }
                },
            )
            .collect::<Vec<_>>();
        let filtered = ordered_limited.filter(move |rec| {
            compiled_filters.iter().all(|filter| {
                match filter {
                    Either::Left((pattern, column)) => pattern.matches(
                        rec[*column]
                            .as_ref()
                            .try_into()
                            .expect("Type mismatch: LIKE and ILIKE can only be applied to strings"),
                    ),
                    Either::Right(ViewQueryFilter {
                        column,
                        operator,
                        value,
                    }) => {
                        match operator {
                            ViewQueryOperator::NotEqual => rec[*column].as_ref() != value,
                            ViewQueryOperator::Like | ViewQueryOperator::ILike => {
                                #[allow(clippy::unreachable)] // actually unreachable
                                {
                                    unreachable!("Already matched on Like and ILike above")
                                }
                            }
                        }
                    }
                }
            })
        });

        let (aggregated, returned_cols) = match (&self.aggregates, &self.returned_cols) {
            (Some(aggs), Some(c)) => (Either::Left(aggs.process(filtered).into_iter()), c),
            (None, Some(c)) => (Either::Right(filtered), c),
            (Some(aggs), None) => return aggs.process(filtered),
            (None, None) => return filtered.collect::<Vec<_>>(),
        };

        aggregated
            .map(|row| {
                returned_cols
                    .iter()
                    .map(|i| row[*i].clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
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
    I: Iterator<Item = Vec<Cow<'a, DataType>>>,
    J: Iterator<Item = Vec<Cow<'a, DataType>>>,
    K: Iterator<Item = Vec<Cow<'a, DataType>>>,
    L: Iterator<Item = Vec<Cow<'a, DataType>>>,
{
}

impl<'a, I, J, K, L> Iterator for OrderedLimitedIter<I, J, K, L>
where
    I: Iterator<Item = Vec<Cow<'a, DataType>>>,
    J: Iterator<Item = Vec<Cow<'a, DataType>>>,
    K: Iterator<Item = Vec<Cow<'a, DataType>>>,
    L: Iterator<Item = Vec<Cow<'a, DataType>>>,
{
    type Item = Vec<Cow<'a, DataType>>;
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
) -> impl Iterator<Item = Vec<Cow<'a, DataType>>>
where
    I: Iterator<Item = Vec<Cow<'a, DataType>>>,
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
                let ret = a[idx].cmp(&b[idx]);
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
) -> impl Iterator<Item = Vec<Cow<'a, DataType>>> + ExactSizeIterator
where
    I: Iterator<Item = Vec<Cow<'a, DataType>>> + ExactSizeIterator,
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

#[cfg(test)]
mod tests {
    use super::*;

    mod post_lookup {
        use super::*;

        #[test]
        fn not_equal_filter() {
            let filter = ViewQueryFilter {
                column: 0,
                operator: ViewQueryOperator::NotEqual,
                value: 1.into(),
            };
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
            ];

            let post_lookup = PostLookup::default();
            let result = post_lookup.process(records.iter(), &[filter]);

            assert_eq!(
                result,
                vec![vec![Cow::Owned(2.into()), Cow::Owned(2.into())]]
            );
        }

        #[test]
        fn sum_aggregate() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![3.into(), 2.into()].into_boxed_slice(),
            ];
            let post_lookup = PostLookup {
                aggregates: Some(PostLookupAggregates {
                    group_by: vec![1],
                    aggregates: vec![PostLookupAggregate {
                        column: 0,
                        function: PostLookupAggregateFunction::Sum,
                    }],
                }),
                ..Default::default()
            };

            let result = post_lookup.process(records.iter(), &[]);

            assert_eq!(
                result,
                vec![vec![Cow::Owned(4.into()), Cow::Owned(2.into())]]
            );
        }

        #[test]
        fn product_aggregate() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![2.into(), 2.into()].into_boxed_slice(),
                vec![3.into(), 2.into()].into_boxed_slice(),
            ];
            let post_lookup = PostLookup {
                aggregates: Some(PostLookupAggregates {
                    group_by: vec![1],
                    aggregates: vec![PostLookupAggregate {
                        column: 0,
                        function: PostLookupAggregateFunction::Product,
                    }],
                }),
                ..Default::default()
            };

            let result = post_lookup.process(records.iter(), &[]);

            assert_eq!(
                result,
                vec![vec![Cow::Owned(6.into()), Cow::Owned(2.into())]]
            );
        }

        #[test]
        fn group_concat_aggregate() {
            let records: Vec<Box<[DataType]>> = vec![
                vec!["a,b,c".into(), 2.into()].into_boxed_slice(),
                vec!["d,e,f".into(), 2.into()].into_boxed_slice(),
            ];
            let post_lookup = PostLookup {
                aggregates: Some(PostLookupAggregates {
                    group_by: vec![1],
                    aggregates: vec![PostLookupAggregate {
                        column: 0,
                        function: PostLookupAggregateFunction::GroupConcat {
                            separator: ",".to_owned(),
                        },
                    }],
                }),
                ..Default::default()
            };

            let result = post_lookup.process(records.iter(), &[]);

            assert_eq!(
                result,
                vec![vec![Cow::Owned("a,b,c,d,e,f".into()), Cow::Owned(2.into())]]
            );
        }

        #[test]
        fn multiple_groups() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 3.into()].into_boxed_slice(),
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 3.into()].into_boxed_slice(),
            ];
            let post_lookup = PostLookup {
                aggregates: Some(PostLookupAggregates {
                    group_by: vec![1],
                    aggregates: vec![PostLookupAggregate {
                        column: 0,
                        function: PostLookupAggregateFunction::Sum,
                    }],
                }),
                ..Default::default()
            };

            let mut result = post_lookup.process(records.iter(), &[]);
            result.sort();

            assert_eq!(
                result,
                vec![
                    vec![Cow::Owned(3.into()), Cow::Owned(2.into())],
                    vec![Cow::Owned(3.into()), Cow::Owned(3.into())],
                ]
            );
        }
    }
}
