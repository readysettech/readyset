use crate::ops::utils::Order;
use crate::prelude::*;
use crate::processing::ColumnMiss;
use crate::LookupIndex;

use dataflow_state::PointKey;
use enum_kinds::EnumKind;
use itertools::{Either, Itertools};
use readyset_client::internal;
use readyset_data::DfType;
use readyset_errors::{unsupported, ReadySetResult};
use readyset_sql::ast::FunctionExpr;
use readyset_sql::ast::{NullOrder, OrderType};
use readyset_util::Indices;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, EnumKind)]
#[enum_kind(WindowOperationKind, derive(Serialize, Deserialize))]
pub enum WindowOperation {
    RowNumber,
    Rank,
    DenseRank,
    CountStar,
    Count(usize),
    Max(usize),
    Min(usize),
    Avg(usize),
    Sum(usize),
}

impl WindowOperationKind {
    /// Creates a WindowOperationKind from a FunctionExpr. Returns
    /// an unsupported_err if the FunctionExpr is not a supported window function
    pub fn from_fn(fn_expr: FunctionExpr) -> ReadySetResult<Self> {
        Ok(match fn_expr {
            FunctionExpr::RowNumber => WindowOperationKind::RowNumber,
            FunctionExpr::Max { .. } => WindowOperationKind::Max,
            FunctionExpr::Min { .. } => WindowOperationKind::Min,
            FunctionExpr::CountStar => WindowOperationKind::CountStar,
            FunctionExpr::Count { .. } => WindowOperationKind::Count,
            FunctionExpr::Sum { .. } => WindowOperationKind::Sum,
            FunctionExpr::Avg { .. } => WindowOperationKind::Avg,
            FunctionExpr::Rank => WindowOperationKind::Rank,
            FunctionExpr::DenseRank => WindowOperationKind::DenseRank,
            _ => unsupported!("Function {fn_expr:?} is not supported as a window function"),
        })
    }

    /// Return a string representation of the WindowOperationKind
    pub fn display(&self) -> String {
        match self {
            WindowOperationKind::RowNumber => "ROW_NUMBER".into(),
            WindowOperationKind::Rank => "RANK".into(),
            WindowOperationKind::DenseRank => "DENSE_RANK".into(),
            WindowOperationKind::Max => "MAX".into(),
            WindowOperationKind::Min => "MIN".into(),
            WindowOperationKind::Count | WindowOperationKind::CountStar => "COUNT".into(),
            WindowOperationKind::Avg => "AVG".into(),
            WindowOperationKind::Sum => "SUM".into(),
        }
    }
}

impl WindowOperation {
    /// Creates a WindowOperation from a WindowOperationKind and arguments.
    pub fn from_fn(kind: WindowOperationKind, args: Vec<usize>) -> ReadySetResult<Self> {
        use WindowOperationKind::*;

        // Some WFs like lead and lag (which we currently don't support)
        // take more than one argument
        let mut args = args.into_iter();

        let mut next_arg = || -> ReadySetResult<_> {
            args.next().ok_or_else(|| {
                ReadySetError::ArityError(format!(
                    "Missing argument for Window Function {}",
                    kind.display()
                ))
            })
        };

        let window = match kind {
            RowNumber => WindowOperation::RowNumber,
            Rank => WindowOperation::Rank,
            DenseRank => WindowOperation::DenseRank,
            CountStar => WindowOperation::CountStar,
            Max => WindowOperation::Max(next_arg()?),
            Min => WindowOperation::Min(next_arg()?),
            Sum => WindowOperation::Sum(next_arg()?),
            Avg => WindowOperation::Avg(next_arg()?),
            Count => WindowOperation::Count(next_arg()?),
        };

        if args.next().is_some() {
            return Err(ReadySetError::ArityError(format!(
                "Too many arguments for Window Function {}",
                kind.display()
            )));
        }

        Ok(window)
    }

    /// Returns the default value for the given output column type
    /// useful for replacing nulls or initializing the output column
    pub fn default_value(&self, output_col_type: &DfType) -> ReadySetResult<DfValue> {
        Ok(match output_col_type {
            DfType::Int
            | DfType::TinyInt
            | DfType::SmallInt
            | DfType::MediumInt
            | DfType::BigInt
            | DfType::UnsignedTinyInt
            | DfType::UnsignedSmallInt
            | DfType::UnsignedMediumInt
            | DfType::UnsignedInt
            | DfType::UnsignedBigInt => DfValue::Int(0),
            DfType::Float => DfValue::Float(0.0),
            // Not sure why DfType::Numeric rounds up results of type DfValue::Numeric
            // so we'll just use DfValue::Double instead
            DfType::Double | DfType::Numeric { .. } => DfValue::Double(0.0),
            t => unsupported!("Unsupported type {t:?} for window function {self:?}"),
        })
    }

    fn coerce_to_output_type(value: &DfValue, output_col_type: &DfType) -> ReadySetResult<DfValue> {
        if value.is_none() {
            return Ok(DfValue::None);
        }

        let inferred_type = value.infer_dataflow_type();
        if &inferred_type == output_col_type {
            Ok(value.clone())
        } else {
            value.coerce_to(output_col_type, &inferred_type)
        }
    }

    /// Optimized implementation of cumulative window functions.
    /// Only processes rows from the offset onwards,
    /// avoiding unnecessary recomputation of unchanged rows.
    ///
    /// Expects input partition to:
    /// 1. already have the diffs applied to it.
    /// 2. be sorted on the `ORDER BY` columns
    ///
    /// For non-cumulative window functions, see [`WindowOperation::apply`]
    pub fn apply_cumulative(
        &self,
        partition: &mut [Cow<[DfValue]>],
        col_index: usize,
        output_col_type: &DfType,
        order_by: &[usize],
        offset: usize,
    ) -> ReadySetResult<()> {
        if partition.is_empty() {
            return Ok(());
        }

        let default_value = self.default_value(output_col_type)?;

        match self {
            WindowOperation::Sum(arg) => {
                let mut running_sum = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    DfValue::None
                };

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be assigned the same total value
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                // Note that in the example
                // (1, null)
                // (1, 1)
                // the output should be
                // (1, null, null)
                // (1, 1, 1)
                // if the ordering of the rows was swapped, the result should be
                // (1, 1, 1)
                // (1, null, 1)
                // so the null gets treated as a zero

                for partition in partitions {
                    running_sum = partition.iter().try_fold(running_sum, |acc, r| {
                        let arg_value = r.get(*arg).unwrap();

                        if acc.is_none() {
                            if arg_value.is_none() {
                                Ok(acc)
                            } else {
                                Self::coerce_to_output_type(arg_value, output_col_type)
                            }
                        } else if arg_value.is_none() {
                            &acc + &default_value
                        } else {
                            &acc + arg_value
                        }
                    })?;

                    for row in partition.iter_mut() {
                        let old_sum = row.to_mut().get_mut(col_index).unwrap();
                        *old_sum = running_sum.clone();
                    }
                }

                Ok(())
            }

            WindowOperation::CountStar => {
                let mut count = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    default_value
                };

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be treated as a new partition
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                for partition in partitions {
                    // COUNT(*) doesn't care about nulls
                    count = (&count + &DfValue::Int(partition.len() as i64))?;

                    for row in partition.iter_mut() {
                        let old_value = row.to_mut().get_mut(col_index).unwrap();
                        *old_value = count.clone();
                    }
                }

                Ok(())
            }

            WindowOperation::Count(arg) => {
                let mut count = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    default_value
                };

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be treated as a new partition
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                for partition in partitions {
                    count = (&count
                        + &DfValue::Int(
                            partition.iter().filter(|r| !r[*arg].is_none()).count() as i64
                        ))?;

                    for row in partition.iter_mut() {
                        let old_value = row.to_mut().get_mut(col_index).unwrap();
                        *old_value = count.clone();
                    }
                }

                Ok(())
            }

            WindowOperation::Avg(arg) => {
                let mut running_sum = DfValue::None;
                let mut running_count = DfValue::Int(0);

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be treated as a new partition
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                for partition in partitions {
                    for row in partition.iter() {
                        let arg_value = row.get(*arg).unwrap();

                        if !arg_value.is_none() {
                            running_sum = if running_sum.is_none() {
                                // use the default (float) to make
                                // sure the division also returns a float
                                &default_value + arg_value
                            } else {
                                &running_sum + arg_value
                            }?;

                            running_count = (&running_count + &1.into())?;
                        }
                    }

                    for row in partition.iter_mut() {
                        let old_avg = row.to_mut().get_mut(col_index).unwrap();

                        if running_sum.is_none() || running_count.as_int().unwrap() == 0 {
                            *old_avg = DfValue::None;
                        } else {
                            *old_avg = (&running_sum / &running_count)?;
                        }
                    }
                }

                Ok(())
            }

            WindowOperation::RowNumber => {
                for (i, row) in partition[offset..].iter_mut().enumerate() {
                    let row_num = (offset + i + 1) as i64;
                    row.to_mut()[col_index] = DfValue::Int(row_num);
                }

                Ok(())
            }

            WindowOperation::Min(arg) => {
                let mut running_min = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    DfValue::None
                };

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be assigned the same value
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                for partition in partitions {
                    for row in partition.iter() {
                        let val = &row[*arg];

                        if running_min.is_none() && !val.is_none()
                            || (!val.is_none() && val < &running_min)
                        {
                            running_min = val.clone();
                        }
                    }

                    for row in partition.iter_mut() {
                        row.to_mut()[col_index] = running_min.clone();
                    }
                }
                Ok(())
            }

            WindowOperation::Max(arg) => {
                let mut running_max = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    DfValue::None
                };

                // chunk the partition by the order cols, starting from offset
                // rows that have the same order cols will be assigned the same value
                let partitions = partition[offset..].chunk_by_mut(|a, b| {
                    a.iter()
                        .zip(b.iter())
                        .enumerate()
                        .filter(|(i, _)| order_by.contains(i))
                        .all(|(_, (a, b))| a == b)
                });

                for partition in partitions {
                    for row in partition.iter() {
                        let val = &row[*arg];

                        if val > &running_max {
                            running_max = val.clone();
                        }
                    }

                    for row in partition.iter_mut() {
                        row.to_mut()[col_index] = running_max.clone();
                    }
                }
                Ok(())
            }

            WindowOperation::Rank => {
                let (mut rank, mut current_rank, mut prev_order_values) = if offset > 0 {
                    let prev_row = &partition[offset - 1];
                    let prev_rank = prev_row[col_index].clone();

                    let prev_order_values: Vec<DfValue> = order_by
                        .iter()
                        .map(|&col_idx| prev_row[col_idx].clone())
                        .collect();

                    (
                        DfValue::Int((offset + 1) as i64),
                        prev_rank,
                        Some(prev_order_values),
                    )
                } else {
                    (DfValue::Int(1), DfValue::Int(1), None)
                };

                for row in partition[offset..].iter_mut() {
                    let order_values: Vec<DfValue> = order_by
                        .iter()
                        .map(|&col_idx| row[col_idx].clone())
                        .collect();

                    if prev_order_values.is_none() || order_values != prev_order_values.unwrap() {
                        current_rank = rank.clone();
                    }

                    row.to_mut()[col_index] = current_rank.clone();
                    rank = (&rank + &DfValue::Int(1))?;
                    prev_order_values = Some(order_values);
                }
                Ok(())
            }

            WindowOperation::DenseRank => {
                let (mut dense_rank, mut prev_order_values) = if offset > 0 {
                    let prev_row = &partition[offset - 1];
                    let prev_dense_rank = prev_row[col_index].clone();

                    let prev_order_values: Vec<DfValue> = order_by
                        .iter()
                        .map(|&col_idx| prev_row[col_idx].clone())
                        .collect();

                    (prev_dense_rank, Some(prev_order_values))
                } else {
                    (DfValue::Int(0), None)
                };

                for row in partition[offset..].iter_mut() {
                    let order_values: Vec<DfValue> = order_by
                        .iter()
                        .map(|&col_idx| row[col_idx].clone())
                        .collect();

                    if prev_order_values.is_none() || order_values != prev_order_values.unwrap() {
                        dense_rank = (&dense_rank + &DfValue::Int(1))?;
                    }

                    row.to_mut()[col_index] = dense_rank.clone();
                    prev_order_values = Some(order_values);
                }
                Ok(())
            }
        }
    }

    /// Optimized implementation of non-cumulative window functions.
    ///
    /// Unlike [`WindowOperation::apply_cumulative`], this expects the
    /// partition to NOT have the diffs already applied to it. This is
    /// because we try to use the diffs to find the new value where possible
    /// to save on recomputation.
    ///
    /// This also means that this function is responsible for applying the diffs.
    ///
    /// For cumulative window functions, see [`WindowOperation::apply_cumulative`]
    pub fn apply<'a>(
        &self,
        partition: &mut Vec<Cow<'a, [DfValue]>>,
        diffs: Vec<(Cow<'a, [DfValue]>, bool)>,
        col_index: usize,
        output_col_type: &DfType,
    ) -> ReadySetResult<()> {
        let default_value = self.default_value(output_col_type)?;

        match self {
            WindowOperation::CountStar => {
                let mut count = DfValue::Int(partition.len() as i64);

                for (_, is_positive) in &diffs {
                    if *is_positive {
                        count = (&count + &DfValue::Int(1))?;
                    } else {
                        count = (&count - &DfValue::Int(1))?;
                    }
                }

                apply_diffs_to_partition(partition, diffs, col_index)?;

                for r in partition.iter_mut() {
                    let old_value = r.to_mut().get_mut(col_index).unwrap();
                    *old_value = count.clone();
                }

                Ok(())
            }

            WindowOperation::Count(arg) => {
                let prev_count = if partition.is_empty() {
                    default_value
                } else {
                    partition[0][col_index].clone()
                };

                apply_diffs_to_partition(partition, diffs.clone(), col_index)?;

                // nulls don't count towards the final count
                let new_count = diffs
                    .into_iter()
                    .filter_map(|(r, is_positive)| {
                        if r.get(*arg).unwrap().is_none() {
                            return None;
                        }

                        Some(DfValue::Int(if is_positive { 1 } else { -1 }))
                    })
                    // if we get a negative record that doesn't hold a null value
                    // then the prev_count is guaranteed to be non-null (since
                    // that negative record existed in this partition)
                    .try_fold(prev_count, |acc, v| &acc + &v)?;

                for r in partition.iter_mut() {
                    let old_value = r.to_mut().get_mut(col_index).unwrap();
                    *old_value = new_count.clone();
                }

                Ok(())
            }
            WindowOperation::Avg(arg) => {
                apply_diffs_to_partition(partition, diffs, col_index)?;

                let mut sum = DfValue::None;
                let mut count = DfValue::Int(0);

                // can't use the diffs to find the new avg, since we don't
                // have a sum or count. We have to do two passes over the partitions
                for row in partition.iter() {
                    let arg_value = row.get(*arg).unwrap();

                    if !arg_value.is_none() {
                        sum = if sum.is_none() {
                            // use the default (float) to make
                            // sure the division also returns a float
                            &default_value + arg_value
                        } else {
                            &sum + arg_value
                        }?;

                        count = (&count + &1.into())?;
                    }
                }

                for r in partition.iter_mut() {
                    let old_avg = r.to_mut().get_mut(col_index).unwrap();

                    if sum.is_none() || count.as_int().unwrap() == 0 {
                        *old_avg = DfValue::None
                    } else {
                        *old_avg = (&sum / &count)?
                    };
                }

                Ok(())
            }
            WindowOperation::Sum(arg) => {
                let old_sum = if partition.is_empty() {
                    DfValue::None
                } else {
                    partition[0][col_index].clone()
                };

                let new_sum = diffs.iter().try_fold(old_sum, |acc, (r, is_positive)| {
                    let arg_value = r.get(*arg).unwrap();

                    if acc.is_none() {
                        if arg_value.is_none() || !*is_positive {
                            Ok(acc)
                        } else {
                            Self::coerce_to_output_type(arg_value, output_col_type)
                        }
                    } else if arg_value.is_none() {
                        &acc + &default_value
                    } else if *is_positive {
                        &acc + arg_value
                    } else {
                        &acc - arg_value
                    }
                })?;

                apply_diffs_to_partition(partition, diffs, col_index)?;

                for r in partition.iter_mut() {
                    let old_sum = r.to_mut().get_mut(col_index).unwrap();
                    *old_sum = new_sum.clone();
                }

                Ok(())
            }
            WindowOperation::RowNumber => {
                let mut count = 1;

                apply_diffs_to_partition(partition, diffs, col_index)?;

                for r in partition.iter_mut() {
                    let old_count = r.to_mut().get_mut(col_index).unwrap();
                    *old_count = DfValue::Int(count);

                    count += 1;
                }

                Ok(())
            }
            WindowOperation::Max(arg) => {
                let mut max = DfValue::None;
                apply_diffs_to_partition(partition, diffs, col_index)?;

                // it's difficult to find the max without doing a full scan of the partition
                // given that the diffs can contain negative records
                for r in partition.iter() {
                    let arg_value = r.get(*arg).unwrap();

                    if arg_value > &mut max {
                        max = arg_value.clone();
                    }
                }

                for r in partition.iter_mut() {
                    let old_value = r.to_mut().get_mut(col_index).unwrap();
                    *old_value = max.clone();
                }

                Ok(())
            }
            WindowOperation::Min(arg) => {
                let mut min = DfValue::None;
                apply_diffs_to_partition(partition, diffs, col_index)?;

                // it's difficult to find the min without doing a full scan of the partition
                // given that the diffs can contain negative records
                for r in partition.iter() {
                    let arg_value = r.get(*arg).unwrap();

                    if min.is_none() && !arg_value.is_none()
                        || (!arg_value.is_none() && arg_value < &min)
                    {
                        min = arg_value.clone();
                    }
                }

                for r in partition.iter_mut() {
                    let old_value = r.to_mut().get_mut(col_index).unwrap();
                    *old_value = min.clone();
                }

                Ok(())
            }
            WindowOperation::Rank | WindowOperation::DenseRank => {
                apply_diffs_to_partition(partition, diffs, col_index)?;

                for r in partition.iter_mut() {
                    let old_value = r.to_mut().get_mut(col_index).unwrap();
                    *old_value = DfValue::Int(1);
                }

                Ok(())
            }
        }
    }

    pub fn display(&self) -> String {
        match self {
            WindowOperation::RowNumber => "row_number".into(),
            WindowOperation::Rank => "rank".into(),
            WindowOperation::DenseRank => "dense_rank".into(),
            WindowOperation::Max(arg) => format!("max({arg})"),
            WindowOperation::Min(arg) => format!("min({arg})"),
            WindowOperation::CountStar => "count(*)".into(),
            WindowOperation::Count(arg) => format!("count({arg})"),
            WindowOperation::Avg(arg) => format!("avg({arg})"),
            WindowOperation::Sum(arg) => format!("sum({arg})"),
        }
    }
}

impl Display for WindowOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowOperation::RowNumber => write!(f, "row_number"),
            WindowOperation::Rank => write!(f, "rank"),
            WindowOperation::DenseRank => write!(f, "dense_rank"),
            WindowOperation::Max(_) => write!(f, "max"),
            WindowOperation::Min(_) => write!(f, "min"),
            WindowOperation::Count(_) | WindowOperation::CountStar => write!(f, "count"),
            WindowOperation::Avg(_) => write!(f, "avg"),
            WindowOperation::Sum(_) => write!(f, "sum"),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Window {
    src: IndexPair,
    our_index: Option<IndexPair>,
    // mainly used by display
    partition_by: Vec<usize>,
    // This is the group by + partition by.
    //
    // When we say group by, we don't mean the GROUP BY clause in sql,
    // we mean the predicates (view key) that define this group/window.
    //
    // This is an optimization since we don't need to fetch the entire window
    // just to modify a single partition inside of it.
    lookup_key: Vec<usize>,
    order_by: Order,
    function: WindowOperation,
    output_col_index: usize,
    // saves us time recomputing it every time
    output_col_type: DfType,
}

impl Window {
    pub fn new(
        src: NodeIndex,
        group_by: Vec<usize>,
        partition_by: Vec<usize>,
        order_by: Vec<(usize, OrderType, NullOrder)>,
        function: WindowOperation,
        output_col_index: usize,
        output_col_type: DfType,
    ) -> ReadySetResult<Window> {
        Ok(Window {
            src: src.into(),
            our_index: None,
            lookup_key: group_by
                .iter()
                .chain(partition_by.iter())
                .copied()
                .collect(),
            partition_by,
            order_by: Order::from(order_by),
            function,
            output_col_index,
            output_col_type,
        })
    }
}

impl Ingredient for Window {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        // This actually doesn't do what you think it's doing.
        //
        // In theory, this should allow us to lookup a partition without having to
        // fetch and iterate over the entire window.
        // However, this index is actually never populated.
        //
        // It's actually used to trigger the fallback behavior of [`MemoryState::lookup`]
        // that does the iteration over the entire window. In short, this removes the iteration
        // logic from this file and instead uses the existing logic in the lookup function.
        //
        // Hopefully, in the future, we will implement secondary (non-clustered) indexes
        // and then this will be used to lookup the partition without having to iterate over
        // the window.
        //
        // Refer to REA-5866
        HashMap::from([(
            this,
            LookupIndex::Strict(internal::Index::hash_map(self.lookup_key.clone())),
        )])
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        // Check if the output column (the window function result) is in the requested columns
        let non_output_cols: Vec<usize> = cols
            .iter()
            .copied()
            .filter(|c| *c != self.output_col_index)
            .collect();

        if non_output_cols.len() == cols.len() {
            // Early exit: the requested columns don't include the output column; they all belong to the
            // parent and can be traced back directly
            return ColumnSource::ExactCopy(ColumnRef {
                node: self.src.as_global(),
                columns: cols.to_vec(),
            });
        }

        if non_output_cols.is_empty() {
            // Only the output column was requested. We can't create an index on just the
            // generated column, so we need a full replay
            return ColumnSource::RequiresFullReplay(vec1![self.src.as_global()]);
        }

        // Check if the non-output columns are a subset of lookup_key (group_by + partition_by).
        // If yes ->    we can use partial materialization via handle_upquery.
        // If no  ->    we need a full replay because the state isn't keyed appropriately.
        let all_in_lookup_key = non_output_cols
            .iter()
            .all(|col| self.lookup_key.contains(col));

        if all_in_lookup_key {
            let our_index = self
                .our_index
                .expect("our_index not set, should have been set in on_commit")
                .as_global();

            ColumnSource::GeneratedFromColumns(vec1![ColumnRef {
                node: our_index,
                columns: non_output_cols,
            }])
        } else {
            ColumnSource::RequiresFullReplay(vec1![self.src.as_global()])
        }
    }

    fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        let index = *self
            .our_index
            .expect("our_index not set, should have been set in on_commit");

        let output_pos = miss
            .column_indices
            .iter()
            .position(|ci| *ci == self.output_col_index)
            .expect("handle_upquery called but output column not in missed columns");

        Ok(vec![ColumnMiss {
            node: index,
            column_indices: self.lookup_key.clone(),
            missed_keys: miss.missed_keys.mapped(|k| {
                k.map_endpoints(|mut r| {
                    r.remove(output_pos)
                        .expect("output column position out of bounds in key");
                    r
                })
            }),
        }])
    }

    fn description(&self) -> String {
        format!(
            "Window p={} o[{}] f={}",
            self.partition_by.iter().join(", "),
            self.order_by,
            self.function.display()
        )
    }

    impl_replace_sibling!(src);

    fn on_commit(
        &mut self,
        us: NodeIndex,
        remap: &std::collections::HashMap<NodeIndex, IndexPair>,
    ) {
        self.src.remap(remap);
        self.our_index = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        replay: &ReplayContext,
        _nodes: &DomainNodes,
        state: &StateMap,
        _auxiliary_node_states: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        let mut out = Vec::new();
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        let us = self.our_index.unwrap();
        let db = state.get(*us).ok_or_else(|| {
            internal_err!("window operators must have their own state materialized")
        })?;

        // Sort the records by window + partition key for efficient processing
        //
        // Chunk by window + partition key and assign key to each group.
        let grouped_rs: Vec<(_, _)> = rs
            .into_iter()
            .sorted_by(|a, b| {
                self.project_window_partition_key(&***a)
                    .cmp(&self.project_window_partition_key(&***b))
            })
            .chunk_by(|r| {
                self.project_window_partition_key(&***r)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect::<Vec<_>>()))
            .collect();

        for (window_partition_key, diffs) in grouped_rs {
            match db.lookup(
                &self.lookup_key[..],
                &PointKey::from(window_partition_key.clone()),
            ) {
                LookupResult::Some(records) => {
                    if replay.is_partial() {
                        lookups.push(Lookup {
                            on: *us,
                            cols: self.lookup_key.clone(),
                            key: window_partition_key
                                .clone()
                                .try_into()
                                .expect("Expected at least 1 element, found empty key"),
                        });
                    }

                    self.process_partition(diffs, &mut records.into_iter().collect(), &mut out)?;
                }
                LookupResult::Missing => {
                    // If we missed, no need to process further
                    misses.extend(diffs.into_iter().map(|r| {
                        Miss::builder()
                            .on(*us)
                            .lookup_idx(self.lookup_key.clone())
                            .lookup_key(self.lookup_key.clone())
                            .replay(replay)
                            .record(r.into_row())
                            .build()
                    }));

                    continue;
                }
            };
        }

        Ok(ProcessingResult {
            results: out.into(),
            misses,
            lookups,
        })
    }
}

impl Window {
    /// Returns true if the window is cumulative (i.e. an `ORDER BY` was specified)
    fn is_cumulative(&self) -> bool {
        !self.order_by.is_empty()
    }

    /// Project the combined window key (predicate) + partition key out of the given record
    fn project_window_partition_key<'rec, R>(&self, rec: &'rec R) -> Vec<&'rec DfValue>
    where
        R: Indices<'static, usize, Output = DfValue> + ?Sized,
    {
        rec.indices(self.lookup_key.clone())
            .expect("Invalid record length")
    }

    fn process_partition(
        &self,
        diffs: Vec<Record>,
        partition: &mut Vec<Cow<[DfValue]>>,
        out: &mut Vec<Record>,
    ) -> ReadySetResult<()> {
        let partition_changes = diffs
            .into_iter()
            .map(|r| {
                let is_positive = r.is_positive();
                let mut row = r.into_row();

                // Add the output column initialized to None
                row.push(DfValue::None);

                (Cow::from(row), is_positive)
            })
            .collect();

        // Apply window function to the partition
        if self.is_cumulative() {
            partition.sort_by(|a, b| self.order_by.cmp(a, b));

            // apply diffs to the ordered partition, send the
            // negatives out, and record the index of the
            // earliest change so we can save time on recomputation.
            let Some(changes_offset) = apply_diffs_to_ordered_partition(
                partition,
                partition_changes,
                self.output_col_index,
                &self.order_by,
                out,
            )?
            else {
                return Ok(());
            };

            self.function.apply_cumulative(
                partition,
                self.output_col_index,
                &self.output_col_type,
                &self.order_by.columns(),
                changes_offset,
            )?;

            out.extend(
                partition[changes_offset..]
                    .iter()
                    .map(|r| Record::Positive((**r).to_vec())),
            );
        } else {
            // send negatives of the current partition as applying
            // diffs will probably change the values of the entire partition
            out.extend(partition.iter().map(|r| Record::Negative((**r).to_vec())));

            self.function.apply(
                partition,
                partition_changes,
                self.output_col_index,
                &self.output_col_type,
            )?;

            out.extend(partition.iter().map(|r| Record::Positive((**r).to_vec())));
        }

        Ok(())
    }
}

/// Apply the diffs to the given partition
/// Errors if a negative record is not found
/// This should only be used for non-cumulative windows
/// For cumulative windows, use [`apply_diffs_to_ordered_partition`]
fn apply_diffs_to_partition<'a>(
    partition: &mut Vec<Cow<'a, [DfValue]>>,
    diffs: Vec<(Cow<'a, [DfValue]>, bool)>,
    col_index: usize,
) -> ReadySetResult<()> {
    let (mut positives, mut negatives): (Vec<_>, HashSet<_>) =
        diffs.iter().partition_map(|(r, is_positive)| {
            if *is_positive {
                Either::Left(r.clone())
            } else {
                Either::Right(r[..col_index].to_vec())
            }
        });

    positives.retain(|r| !negatives.remove(&r[..col_index]));
    partition.retain(|r| !negatives.remove(&r[..col_index]));

    partition.extend(positives);

    if !negatives.is_empty() {
        return Err(ReadySetError::Internal(
            "Negative record not found".to_string(),
        ));
    }

    Ok(())
}

/// Rewinds an offset to the start of the peer group (rows with the same ORDER BY values),
/// and emits negatives for all rows in the peer group.
///
/// When a row is deleted from within a peer group, the changes_offset might point to
/// the deleted row or later in the group. This function ensures we rewind to the start
/// of that peer group so all peers get recomputed correctly.
fn rewind_to_peer_group_start(
    partition: &[Cow<[DfValue]>],
    offset: usize,
    order: &Order,
    row: &[DfValue],
    out: &mut Vec<Record>,
) -> usize {
    if partition.is_empty() || offset == 0 {
        return 0;
    }

    let mut start = offset;
    while start > 0 {
        let prev_row = &partition[start - 1];

        if order.cmp(prev_row, row) == Ordering::Equal {
            start -= 1;
            out.push(Record::Negative((**prev_row).to_vec()));
        } else {
            break;
        }
    }

    start
}

/// Applies a set of diffs (insertions and deletions) to a sorted partition,
/// maintaining order and tracking the first change.
///
/// Given a sorted `partition` and a list of `diffs`, this function:
/// 1. Separates positive diffs (insertions) and negative diffs (deletions).
/// 2. Cancels out insertions that are directly negated by deletions.
/// 3. Sorts the remaining insertions according to the provided `ORDER BY` clause.
/// 4. Merges the sorted insertions with the original partition while:
///     - Removing rows that match negative diffs,
///     - Emitting `Record::Negative` outputs for affected rows if needed,
///     - Tracking the earliest index at which any change occurs (insert or delete).
///
/// The final partition is written back into the provided `partition` vector.
///
/// If no changes were made to the original partition
/// (either diffs cancel out or were empty), return `Ok(None)`
fn apply_diffs_to_ordered_partition<'a>(
    partition: &mut Vec<Cow<'a, [DfValue]>>,
    diffs: Vec<(Cow<'a, [DfValue]>, bool)>,
    col_index: usize,
    order: &Order,
    out: &mut Vec<Record>,
) -> ReadySetResult<Option<usize>> {
    let mut earliest = usize::MAX;
    let mut earliest_deleted_row = None;

    let (mut positives, mut negatives): (Vec<_>, HashSet<_>) =
        diffs.into_iter().partition_map(|(r, is_positive)| {
            if is_positive {
                Either::Left(r)
            } else {
                // negatives should only contain the original row
                // without the output column, since we default it to None
                Either::Right(r[..col_index].to_vec())
            }
        });

    positives.retain(|r| !negatives.remove(&r[..col_index]));

    positives.sort_by(|a, b| order.cmp(a, b));

    let mut sorted = Vec::with_capacity(partition.len() + positives.len() - negatives.len());

    let mut partition_iter = std::mem::take(partition).into_iter();
    let mut positives_iter = positives.into_iter();

    let mut left = partition_iter.next();
    let mut right = positives_iter.next();

    let mut idx = 0;

    while left.is_some() || right.is_some() {
        match (left.as_ref(), right.as_ref()) {
            (None, Some(_)) => {
                earliest = earliest.min(idx);

                sorted.push(right.take().unwrap());

                right = positives_iter.next();
            }
            (Some(_), None) => {
                let val = left.take().unwrap();

                if !negatives.remove(&val[..col_index]) {
                    sorted.push(val.clone());
                } else if idx < earliest {
                    earliest = idx;
                    earliest_deleted_row = Some(val.clone());
                }

                // an earlier row changed, this row will have
                // a new value and therefore needs a negative
                if earliest != usize::MAX {
                    out.push(Record::Negative(val.to_vec()));
                }

                left = partition_iter.next();
            }
            (Some(l), Some(r)) if order.cmp(l, r) == Ordering::Less => {
                let val = left.take().unwrap();

                if !negatives.remove(&val[..col_index]) {
                    sorted.push(val.clone());
                } else if idx < earliest {
                    earliest = idx;
                    earliest_deleted_row = Some(val.clone());
                }

                // an earlier row changed, this row will have
                // a new value and therefore needs a negative
                if earliest != usize::MAX {
                    out.push(Record::Negative(val.to_vec()));
                }

                left = partition_iter.next();
            }
            (Some(_), Some(_)) => {
                earliest = earliest.min(idx);

                sorted.push(right.take().unwrap());

                right = positives_iter.next();
            }
            _ => break,
        }

        idx += 1;
    }

    *partition = sorted;

    Ok(if earliest == usize::MAX {
        None
    } else {
        if let Some(row) = earliest_deleted_row {
            earliest = rewind_to_peer_group_start(partition, earliest, order, &row, out);
        }

        Some(earliest)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops;
    use readyset_data::DfValue;

    fn setup_row_number(
        group_by: Vec<usize>,
        partition_by: Vec<usize>,
    ) -> (ops::test::MockGraph, IndexPair) {
        let order_by = vec![(2, OrderType::OrderAscending, NullOrder::NullsFirst)];
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["group", "partition", "order", "value"]);
        g.set_op(
            "window",
            &["group", "partition", "order", "value", "row_num"],
            Window::new(
                s.as_global(),
                group_by,
                partition_by,
                order_by,
                WindowOperation::RowNumber,
                4,
                DfType::Int,
            )
            .unwrap(),
            true,
        );
        (g, s)
    }

    fn setup_count_star(
        group_by: Vec<usize>,
        partition_by: Vec<usize>,
    ) -> (ops::test::MockGraph, IndexPair) {
        let order_by = vec![]; // No ORDER BY for non-ordered windows
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["group", "partition", "value"]);
        g.set_op(
            "window",
            &["group", "partition", "value", "count"],
            Window::new(
                s.as_global(),
                group_by,
                partition_by,
                order_by,
                WindowOperation::CountStar,
                3,
                DfType::Int,
            )
            .unwrap(),
            true,
        );
        (g, s)
    }

    #[test]
    fn ordered_window_change_at_top_emits_entire_window() {
        let (mut g, _s) = setup_row_number(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "A".into(), 1.into(), 10.into()];
        let r2: Vec<DfValue> = vec![1.into(), "A".into(), 2.into(), 20.into()];
        let r3: Vec<DfValue> = vec![1.into(), "A".into(), 3.into(), 30.into()];

        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Insert at top - should emit entire window twice
        let r0: Vec<DfValue> = vec![1.into(), "A".into(), 0.into(), 5.into()];
        let emit = g.narrow_one_row(r0.clone(), true);

        let negatives: Vec<_> = emit.iter().filter(|r| !r.is_positive()).collect();
        let positives: Vec<_> = emit.iter().filter(|r| r.is_positive()).collect();

        assert_eq!(negatives.len(), 3);
        assert_eq!(positives.len(), 4);
    }

    #[test]
    fn ordered_window_change_at_bottom_emits_one_positive() {
        let (mut g, _s) = setup_row_number(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "A".into(), 1.into(), 10.into()];
        let r2: Vec<DfValue> = vec![1.into(), "A".into(), 2.into(), 20.into()];
        let r3: Vec<DfValue> = vec![1.into(), "A".into(), 3.into(), 30.into()];

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3, true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Insert at bottom - should emit only 1 positive
        let r4: Vec<DfValue> = vec![1.into(), "A".into(), 4.into(), 40.into()];
        let emit = g.narrow_one_row(r4.clone(), true);

        assert_eq!(emit.len(), 1);
        assert!(emit[0].is_positive());
        assert_eq!(emit[0][4], DfValue::Int(4));
    }

    #[test]
    fn ordered_self_canceling_diffs_emit_nothing() {
        let (mut g, _s) = setup_row_number(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "A".into(), 1.into(), 10.into()];
        let r2: Vec<DfValue> = vec![1.into(), "A".into(), 2.into(), 20.into()];

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);

        assert_eq!(g.states[ni].row_count(), 2);

        let r3: Vec<DfValue> = vec![1.into(), "A".into(), 3.into(), 30.into()];
        let emit = g.narrow_one(
            vec![Record::Positive(r3.clone()), Record::Negative(r3)],
            true,
        );

        assert_eq!(emit.len(), 0);
        assert_eq!(g.states[ni].row_count(), 2);
    }

    #[test]
    fn multiple_partitions_independent_processing() {
        let (mut g, _s) = setup_row_number(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1a: Vec<DfValue> = vec![1.into(), "A".into(), 1.into(), 10.into()];
        let r2a: Vec<DfValue> = vec![1.into(), "A".into(), 2.into(), 20.into()];
        let r1b: Vec<DfValue> = vec![1.into(), "B".into(), 1.into(), 100.into()];
        let r2b: Vec<DfValue> = vec![1.into(), "B".into(), 2.into(), 200.into()];

        g.narrow_one_row(r1a, true);
        g.narrow_one_row(r2a, true);
        g.narrow_one_row(r1b, true);
        g.narrow_one_row(r2b, true);
        assert_eq!(g.states[ni].row_count(), 4);

        // Insert at top of partition A - should not affect partition B
        let r0a: Vec<DfValue> = vec![1.into(), "A".into(), 0.into(), 5.into()];
        let emit = g.narrow_one_row(r0a.clone(), true);

        assert_eq!(emit.len(), 5);
        for record in &emit {
            assert_eq!(record[1], "A".into());
        }
    }

    #[test]
    fn index_construction_empty_group_by() {
        let order_by = vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)];
        let window = Window::new(
            1.into(),
            vec![],  // empty group_by
            vec![0], // partition_by on column 0
            order_by,
            WindowOperation::RowNumber,
            2,
            DfType::Int,
        )
        .unwrap();

        assert_eq!(window.lookup_key, vec![0]);
    }

    #[test]
    fn index_construction_empty_partition_by() {
        let order_by = vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)];
        let window = Window::new(
            1.into(),
            vec![0], // group_by on column 0
            vec![],  // empty partition_by
            order_by,
            WindowOperation::RowNumber,
            2,
            DfType::Int,
        )
        .unwrap();

        assert_eq!(window.lookup_key, vec![0]);
    }

    #[test]
    fn index_construction_both_empty() {
        let order_by = vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)];
        let window = Window::new(
            1.into(),
            vec![], // empty group_by
            vec![], // empty partition_by
            order_by,
            WindowOperation::RowNumber,
            2,
            DfType::Int,
        )
        .unwrap();

        assert_eq!(window.lookup_key, Vec::<usize>::new());
    }

    #[test]
    fn index_construction_non_sequential_columns() {
        let order_by = vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)];
        let window = Window::new(
            1.into(),
            vec![3, 0], // group_by on columns 3, 0
            vec![2, 1], // partition_by on columns 2, 1
            order_by,
            WindowOperation::RowNumber,
            5,
            DfType::Int,
        )
        .unwrap();

        assert_eq!(window.lookup_key, vec![3, 0, 2, 1]);
    }

    /// Test that column_source returns RequiresFullReplay when only the output column is requested.
    /// This prevents creating an index with 0 columns which would cause an error.
    #[test]
    fn column_source_output_only_requires_full_replay() {
        let (g, s) = setup_row_number(vec![0], vec![1]);
        // The output column (row_num) is at index 4
        let src = g.node().column_source(&[4]);
        assert_eq!(src, ColumnSource::RequiresFullReplay(vec1![s.as_global()]));
    }

    /// Test that column_source returns GeneratedFromColumns when the output column is requested
    /// along with columns that ARE in lookup_key (group_by + partition_by).
    /// This enables partial materialization via handle_upquery, which is faster than a full replay.
    #[test]
    fn column_source_output_with_lookup_key_cols_generated() {
        let (g, _s) = setup_row_number(vec![0], vec![1]);
        // Request the partition column (1) and output column (4)
        // Column 1 is in lookup_key (group_by=[0], partition_by=[1] => lookup_key=[0,1])
        let src = g.node().column_source(&[1, 4]);
        assert_eq!(
            src,
            ColumnSource::GeneratedFromColumns(vec1![ColumnRef {
                node: g.node_index().as_global(),
                columns: vec![1], // non-output columns
            }])
        );
    }

    /// Test that column_source returns RequiresFullReplay when the output column is requested
    /// along with columns that are NOT in lookup_key.
    #[test]
    fn column_source_output_with_non_lookup_cols_requires_full_replay() {
        let (g, s) = setup_row_number(vec![0], vec![1]);
        // Request column 2 (order_by column, NOT in lookup_key) and output column (4)
        // lookup_key = [0, 1], so column 2 is not in it
        let src = g.node().column_source(&[2, 4]);
        assert_eq!(src, ColumnSource::RequiresFullReplay(vec1![s.as_global()]));
    }

    /// Test that column_source returns ExactCopy for non-output columns.
    #[test]
    fn column_source_non_output_exact_copy() {
        let (g, s) = setup_row_number(vec![0], vec![1]);
        // Request only the order column (2)
        let src = g.node().column_source(&[2]);
        assert_eq!(
            src,
            ColumnSource::ExactCopy(ColumnRef {
                node: s.as_global(),
                columns: vec![2],
            })
        );
    }

    /// Test that handle_upquery correctly remaps an upquery for generated columns.
    /// It should strip out the output column value and remap to lookup_key columns.
    #[test]
    fn handle_upquery_for_output_col_query() {
        let (g, _s) = setup_row_number(vec![0], vec![1]);
        // Upquery for columns [1, 4] (partition_by col and output col)
        // with key ["A", 3] (partition value "A", row_number 3)
        let res = g
            .node_mut()
            .handle_upquery(ColumnMiss {
                node: *g.node_index(),
                column_indices: vec![1, 4],
                missed_keys: vec1![vec1![DfValue::from("A"), DfValue::from(3)].into()],
            })
            .unwrap();

        assert_eq!(res.len(), 1);
        // Should remap to lookup_key columns [0, 1] with just the partition value ["A"]
        // (output column value stripped)
        assert_eq!(
            *res.first().unwrap(),
            ColumnMiss {
                node: *g.node_index(),
                column_indices: vec![0, 1], // lookup_key
                missed_keys: vec1![vec1![DfValue::from("A")].into()]  // output col value stripped
            }
        );
    }

    #[test]
    fn different_windows_same_partition_keys() {
        let (mut g, _s) = setup_row_number(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1_win1: Vec<DfValue> = vec![1.into(), "A".into(), 1.into(), 10.into()];
        let r2_win1: Vec<DfValue> = vec![1.into(), "A".into(), 2.into(), 20.into()];
        let r1_win2: Vec<DfValue> = vec![2.into(), "A".into(), 1.into(), 100.into()];
        let r2_win2: Vec<DfValue> = vec![2.into(), "A".into(), 2.into(), 200.into()];

        g.narrow_one_row(r1_win1.clone(), true);
        g.narrow_one_row(r2_win1.clone(), true);
        g.narrow_one_row(r1_win2.clone(), true);
        g.narrow_one_row(r2_win2.clone(), true);
        assert_eq!(g.states[ni].row_count(), 4);

        // Insert at top of window 1 - should not affect window 2
        let r0_win1: Vec<DfValue> = vec![1.into(), "A".into(), 0.into(), 5.into()];
        let emit = g.narrow_one_row(r0_win1.clone(), true);

        assert_eq!(emit.len(), 5);
        for record in &emit {
            assert_eq!(record[0], 1.into());
        }
    }

    #[test]
    fn non_ordered_over_entire_table() {
        let (mut g, _s) = setup_count_star(vec![], vec![]);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "A".into(), 10.into()];
        let r2: Vec<DfValue> = vec![2.into(), "B".into(), 20.into()];
        let r3: Vec<DfValue> = vec![3.into(), "C".into(), 30.into()];

        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Add new record - should emit entire table with updated count
        let r4: Vec<DfValue> = vec![4.into(), "D".into(), 40.into()];
        let emit = g.narrow_one_row(r4.clone(), true);

        let (positives, negatives): (Vec<_>, Vec<_>) =
            emit.into_iter().partition(|r| r.is_positive());

        assert_eq!(negatives.len(), 3);
        assert_eq!(positives.len(), 4);

        // All records should have count = 4
        for record in &positives {
            assert_eq!(record[3], DfValue::Int(4));
        }
    }

    #[test]
    fn non_ordered_over_partition_by() {
        let (mut g, _s) = setup_count_star(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1a: Vec<DfValue> = vec![1.into(), "A".into(), 10.into()];
        let r2a: Vec<DfValue> = vec![1.into(), "A".into(), 20.into()];
        let r1b: Vec<DfValue> = vec![1.into(), "B".into(), 100.into()];
        let r2b: Vec<DfValue> = vec![1.into(), "B".into(), 200.into()];

        g.narrow_one_row(r1a.clone(), true);
        g.narrow_one_row(r2a.clone(), true);
        g.narrow_one_row(r1b.clone(), true);
        g.narrow_one_row(r2b.clone(), true);
        assert_eq!(g.states[ni].row_count(), 4);

        // Add to partition A - should only emit partition A records
        let r3a: Vec<DfValue> = vec![1.into(), "A".into(), 30.into()];
        let emit = g.narrow_one_row(r3a.clone(), true);

        assert_eq!(emit.len(), 5); // 2 negatives + 3 positives (partition A only)

        // All emitted records should be for partition A
        for record in &emit {
            assert_eq!(record[1], "A".into());
        }

        // Partition A records should have count = 3
        let positives: Vec<_> = emit.iter().filter(|r| r.is_positive()).collect();
        for record in &positives {
            assert_eq!(record[3], DfValue::Int(3));
        }
    }

    #[test]
    /// TODO:
    /// Non-ordered windows always re-emit entire partition even for self-canceling diffs
    /// because the implementation conservatively assumes function values may change.
    /// I'm not comfortable doing this change this close to the release. Especially
    /// after it was tested by Joe.
    fn non_ordered_self_canceling_still_emits() {
        let (mut g, _s) = setup_count_star(vec![0], vec![1]);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "A".into(), 10.into()];
        let r2: Vec<DfValue> = vec![1.into(), "A".into(), 20.into()];

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        assert_eq!(g.states[ni].row_count(), 2);

        let r3: Vec<DfValue> = vec![1.into(), "A".into(), 30.into()];
        let emit = g.narrow_one(
            vec![Record::Positive(r3.clone()), Record::Negative(r3)],
            true,
        );

        assert_eq!(emit.len(), 4); // 2 negatives + 2 positives
        assert_eq!(g.states[ni].row_count(), 2);
    }
}
