use std::borrow::Cow;
use std::cmp::{self, Ordering};
use std::convert::TryFrom;
use std::fmt::Debug;

use common::DataType;
use dataflow_expression::Expr;
use nom_sql::OrderType;
use noria::ReadySetResult;
use noria_errors::internal;
use reader_map::InsertionOrder;
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
    fn apply(&self, val1: &DataType, val2: &DataType) -> ReadySetResult<DataType> {
        match self {
            PostLookupAggregateFunction::Sum => (val1 + val2),
            PostLookupAggregateFunction::Product => (val1 * val2),
            PostLookupAggregateFunction::GroupConcat { separator } => Ok(format!(
                "{}{}{}",
                String::try_from(val1)?,
                separator,
                String::try_from(val2)?
            )
            .into()),
            PostLookupAggregateFunction::Max => Ok(cmp::max(val1, val2).clone()),
            PostLookupAggregateFunction::Min => Ok(cmp::min(val1, val2).clone()),
        }
    }
}

/// Representation of a single aggregate function to be performed on a column post-lookup
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregate<Column = usize> {
    /// The column index in the result set containing the already-aggregated values
    pub column: Column,
    /// The aggregate function to perform
    pub function: PostLookupAggregateFunction,
}

impl<Column> PostLookupAggregate<Column> {
    /// Transform all column references in self by applying a function
    pub fn map_columns<F, C2, E>(self, mut f: F) -> Result<PostLookupAggregate<C2>, E>
    where
        F: FnMut(Column) -> Result<C2, E>,
    {
        Ok(PostLookupAggregate {
            column: f(self.column)?,
            function: self.function,
        })
    }
}

/// Representation of a set of multiple aggregate functions to be performed post-lookup
///
/// This is used for range queries, where lookups cover multiple grouped keys
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregates<Column = usize> {
    /// The set of column indices to group the aggregate by
    pub group_by: Vec<Column>,
    /// The aggregate functions to perform
    pub aggregates: Vec<PostLookupAggregate<Column>>,
}

impl PostLookupAggregates {
    /// Process the given set of rows by performing the aggregates in self on all input values, the
    /// values in iterator are assumed to be sorted according to `group_by`
    fn process<'a, I>(
        &self,
        mut iter: I,
        returned_cols: usize,
        limit: usize,
    ) -> ReadySetResult<Vec<Cow<'a, [DataType]>>>
    where
        I: Iterator<Item = ReadySetResult<&'a Box<[DataType]>>>,
    {
        let mut ret = Vec::new();
        let mut cur_row = match iter.next() {
            Some(row) => row?.to_vec(),
            None => return Ok(vec![]),
        };

        for row in iter {
            let row = row?;

            if self.group_by.iter().all(|g| cur_row[*g] == row[*g]) {
                // Those rows belong to the same group, apply the aggregate
                for agg in &self.aggregates {
                    cur_row[agg.column] =
                        agg.function.apply(&cur_row[agg.column], &row[agg.column])?;
                }
            } else {
                // The next row begins a new group, finish the aggregate row and start a new one
                let mut out_row = std::mem::replace(&mut cur_row, row.to_vec());
                out_row.truncate(returned_cols);
                ret.push(Cow::Owned(out_row));
                if ret.len() == limit {
                    return Ok(ret);
                }
            }
        }

        cur_row.truncate(returned_cols);
        ret.push(Cow::Owned(cur_row));

        Ok(ret)
    }
}

impl<Column> PostLookupAggregates<Column> {
    /// Transform all column references in self by applying a function
    pub fn map_columns<F, C2, E>(self, mut f: F) -> Result<PostLookupAggregates<C2>, E>
    where
        F: FnMut(Column) -> Result<C2, E>,
    {
        Ok(PostLookupAggregates {
            group_by: self
                .group_by
                .into_iter()
                .map(&mut f)
                .collect::<Result<_, E>>()?,
            aggregates: self
                .aggregates
                .into_iter()
                .map(|agg| agg.map_columns(&mut f))
                .collect::<Result<_, E>>()?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
/// Operations to perform on rows before insertion into a reader or after a lookup
pub struct ReaderProcessing {
    /// Pre processing on rows prior to insertion into a reader
    pub pre_processing: PreInsertion,
    /// Post processing on result sets after a lookup is finished
    pub post_processing: PostLookup,
}

impl ReaderProcessing {
    /// Constructs a new [`PostLookup`]
    pub fn new(
        order_by: Option<Vec<(usize, OrderType)>>,
        limit: Option<usize>,
        returned_cols: Option<Vec<usize>>,
        default_row: Option<Vec<DataType>>,
        aggregates: Option<PostLookupAggregates>,
    ) -> ReadySetResult<Self> {
        if let Some(cols) = &returned_cols {
            if cols.iter().enumerate().any(|(i, v)| i != *v) {
                internal!("Returned columns must be projected in order");
            }
        }

        let post_processing = PostLookup {
            order_by,
            limit,
            returned_cols,
            default_row,
            aggregates,
        };

        let pre_processing = PreInsertion {
            order_by: post_processing.order_by.clone(),
            group_by: post_processing
                .aggregates
                .as_ref()
                .map(|v| v.group_by.clone()),
        };

        Ok(ReaderProcessing {
            pre_processing,
            post_processing,
        })
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
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    order_by: Option<Vec<(usize, OrderType)>>,
    /// Maximum number of records to return
    limit: Option<usize>,
    /// Indices of the columns requested in the query. Reader will filter out all other projected
    /// columns
    pub returned_cols: Option<Vec<usize>>,
    /// Default values to send back, for example if we're aggregating and no rows are found
    default_row: Option<Vec<DataType>>,
    /// Aggregates to perform on the result set *after* it's retrieved from the reader.
    ///
    /// Note that currently these are only performed on each key individually, not the overall
    /// result set returned by all keys in a multi-key lookup
    aggregates: Option<PostLookupAggregates>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
/// Operations to perform on a row before it is stored in the map in a reader.
pub struct PreInsertion {
    /// Column indices to order by, and whether or not to reverse order on each index. Only applies
    /// if no `group_by` is supplied, otherwise ordering will be done after aggregates are
    /// processed in `PostLookup`.
    ///
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    order_by: Option<Vec<(usize, OrderType)>>,
    /// The set of column indices to group the aggregate by, `group_by` takes precedence over
    /// `order_by` when determining row order, so that aggregates are proccessed one by one.
    group_by: Option<Vec<usize>>,
}

impl InsertionOrder<Box<[DataType]>> for PreInsertion {
    fn get_insertion_order(
        &self,
        values: &[Box<[DataType]>],
        elem: &Box<[DataType]>,
    ) -> Result<usize, usize> {
        if let Some(cols) = &self.group_by {
            values.binary_search_by(|cur_row| {
                cols.iter()
                    .map(|&idx| cur_row[idx].cmp(&elem[idx]))
                    .try_fold(Ordering::Equal, |acc, next| match acc {
                        Ordering::Equal => Ok(next),
                        ord => Err(ord),
                    })
                    .unwrap_or_else(|ord| ord)
                    .then(cur_row.cmp(elem))
            })
        } else if let Some(indices) = self.order_by.as_deref() {
            values.binary_search_by(|cur_row| {
                indices
                    .iter()
                    .map(|&(idx, order_type)| order_type.apply(cur_row[idx].cmp(&elem[idx])))
                    .try_fold(Ordering::Equal, |acc, next| match acc {
                        Ordering::Equal => Ok(next),
                        ord => Err(ord),
                    })
                    .unwrap_or_else(|ord| ord)
                    .then(cur_row.cmp(elem))
            })
        } else {
            values.binary_search(elem)
        }
    }
}

impl PostLookup {
    /// Apply this set of post-lookup operations, optionally filtering by an [`Expr`], to the
    /// given set of results returned from a lookup. The set of results is assumed to have passed
    /// the associated pre-insertion operations and are properly sorted with reoredered columns.
    pub fn process<'a, 'b: 'a, I>(
        &'a self,
        iter: I,
        filter: &Option<Expr>,
    ) -> ReadySetResult<Vec<Cow<'a, [DataType]>>>
    where
        I: Iterator<Item = &'b Box<[DataType]>> + ExactSizeIterator,
        Self: 'a,
    {
        // If no data is present AND we have default values (e.g. we're aggregating), we can
        // short-circuit here and just return the defaults.
        if iter.len() == 0 {
            return Ok(if let Some(defaults) = self.default_row.as_ref() {
                vec![Cow::Borrowed(defaults)]
            } else {
                // Otherwise simply return an empty vector
                vec![]
            });
        }

        let n_cols = self
            .returned_cols
            .as_ref()
            .map(|r| r.len())
            .unwrap_or(usize::MAX);

        let filtered = iter.filter_map(|rec| {
            if let Some(filter) = filter {
                match filter.eval(rec) {
                    Ok(v) if v.is_truthy() => Some(Ok(rec)),
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            } else {
                Some(Ok(rec))
            }
        });

        if let Some(aggregates) = &self.aggregates {
            aggregates.process(filtered, n_cols, self.limit.unwrap_or(usize::MAX))
        } else {
            filtered
                .map(|r| r.map(|r| Cow::Borrowed(&r[..n_cols.min(r.len())])))
                .take(self.limit.unwrap_or(usize::MAX))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    mod post_lookup {
        use dataflow_expression::utils::{make_int_column, make_literal};
        use nom_sql::{BinaryOperator, SqlType};
        use noria_data::noria_type::Type;
        //use Expr::utils::{make_int_column, make_literal};
        use Expr::Op;

        use super::*;

        #[test]
        fn not_equal_filter() {
            let filter = Expr::Op {
                left: Box::new(make_int_column(0)),
                op: BinaryOperator::NotEqual,
                right: Box::new(make_literal(DataType::from(1))),
                ty: Type::Sql(SqlType::Bool),
            };
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
            ];

            let post_lookup = PostLookup::default();
            let result = post_lookup.process(records.iter(), &Some(filter)).unwrap();

            assert_eq!(result, vec![Cow::Borrowed([2.into(), 2.into()].as_slice())]);
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

            let result = post_lookup.process(records.iter(), &None).unwrap();

            assert_eq!(result, vec![Cow::Borrowed([4.into(), 2.into()].as_slice())]);
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

            let result = post_lookup.process(records.iter(), &None).unwrap();

            assert_eq!(result, vec![Cow::Borrowed([6.into(), 2.into()].as_slice())]);
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

            let result = post_lookup.process(records.iter(), &None).unwrap();

            assert_eq!(
                result,
                vec![Cow::Borrowed(["a,b,c,d,e,f".into(), 2.into()].as_slice())]
            );
        }

        #[test]
        fn multiple_groups() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 3.into()].into_boxed_slice(),
                vec![2.into(), 3.into()].into_boxed_slice(),
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
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

            let mut result = post_lookup.process(records.iter(), &None).unwrap();
            result.sort();

            assert_eq!(
                result,
                vec![
                    Cow::Borrowed([3.into(), 2.into()].as_slice()),
                    Cow::Borrowed([3.into(), 3.into()].as_slice())
                ]
            );
        }

        #[test]
        fn filter_and_order_limit() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 1.into()].into_boxed_slice(),
                vec![2.into(), 1.into()].into_boxed_slice(),
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
                vec![1.into(), 3.into()].into_boxed_slice(),
                vec![2.into(), 3.into()].into_boxed_slice(),
            ];

            let post_lookup = PostLookup {
                order_by: Some(vec![(1, OrderType::OrderAscending)]),
                limit: Some(3),
                ..Default::default()
            };

            let filter = Op {
                left: Box::new(make_int_column(0)),
                op: BinaryOperator::NotEqual,
                right: Box::new(make_literal(DataType::from(1))),
                ty: Type::Sql(SqlType::Bool),
            };

            let result = post_lookup.process(records.iter(), &Some(filter)).unwrap();

            assert_eq!(result.len(), 3);
            assert_eq!(
                result,
                vec![
                    Cow::Borrowed([2.into(), 1.into()].as_slice()),
                    Cow::Borrowed([2.into(), 2.into()].as_slice()),
                    Cow::Borrowed([2.into(), 3.into()].as_slice())
                ]
            );
        }

        #[test]
        fn filter_aggregate_order_limit() {
            let records: Vec<Box<[DataType]>> = vec![
                vec![1.into(), 3.into()].into_boxed_slice(),
                vec![1.into(), 2.into()].into_boxed_slice(),
                vec![1.into(), 1.into()].into_boxed_slice(),
                vec![2.into(), 3.into()].into_boxed_slice(),
                vec![2.into(), 2.into()].into_boxed_slice(),
                vec![2.into(), 1.into()].into_boxed_slice(),
                vec![3.into(), 3.into()].into_boxed_slice(),
                vec![3.into(), 2.into()].into_boxed_slice(),
                vec![3.into(), 1.into()].into_boxed_slice(),
            ];

            // MIN(c1) WHERE c1 != 3 GROUP BY c0 ORDER BY c0 ASC LIMIT 1

            let filter = Op {
                left: Box::new(make_int_column(1)),
                op: BinaryOperator::NotEqual,
                right: Box::new(make_literal(DataType::from(3))),
                ty: Type::Sql(SqlType::Bool),
            };

            let post_lookup = PostLookup {
                aggregates: Some(PostLookupAggregates {
                    group_by: vec![0],
                    aggregates: vec![PostLookupAggregate {
                        column: 1,
                        function: PostLookupAggregateFunction::Min,
                    }],
                }),
                order_by: Some(vec![(0, OrderType::OrderAscending)]),
                limit: Some(1),
                ..Default::default()
            };

            let result = post_lookup.process(records.iter(), &Some(filter)).unwrap();

            assert_eq!(result.len(), 1);
            assert_eq!(result, vec![Cow::Borrowed([1.into(), 1.into()].as_slice())]);
        }
    }
}
