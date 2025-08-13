use crate::ops::utils::Order;
use crate::prelude::*;
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

    /// Optimized implementation of cumulative window functions.
    /// Only processes rows from the offset onwards,
    /// avoiding unnecessary recomputation of unchanged rows.
    ///
    /// Expects input partition to:
    /// 1. already have the diffs applied to it.
    /// 2. be sorted on the `ORDER BY` columns
    ///
    /// For non-cumulative window functions, see [`WindowOperation::apply`]
    #[allow(clippy::too_many_arguments)]
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

                        if acc.is_none() && !arg_value.is_none() {
                            Ok(arg_value.clone())
                        } else if !acc.is_none() && arg_value.is_none() {
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

                for row in partition[offset..].iter_mut() {
                    let val = &row[*arg];

                    if running_min.is_none() && !val.is_none()
                        || (!val.is_none() && val < &running_min)
                    {
                        running_min = val.clone();
                    }

                    row.to_mut()[col_index] = running_min.clone();
                }
                Ok(())
            }

            WindowOperation::Max(arg) => {
                let mut running_max = if offset > 0 {
                    partition[offset - 1][col_index].clone()
                } else {
                    DfValue::None
                };

                for row in partition[offset..].iter_mut() {
                    let val = &row[*arg];

                    if val > &running_max {
                        running_max = val.clone();
                    }

                    row.to_mut()[col_index] = running_max.clone();
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

                    if acc.is_none() && !arg_value.is_none() {
                        Ok(arg_value.clone())
                    } else if !acc.is_none() && arg_value.is_none() {
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
    // this is NOT the query's `GROUP BY` clause.
    // This is the predicates (view keys) that define
    // (or "group") the window
    group_by: Vec<usize>,
    partition_by: Vec<usize>,
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
            group_by,
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
        // TODO: REA-5866 - Changing this index to be group_by + partition_key
        // may be a good idea.
        HashMap::from([(
            this,
            LookupIndex::Strict(internal::Index::hash_map(self.group_by.clone())),
        )])
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        let our_index = self
            .our_index
            .expect("our index not set, should have been set in on_commit");

        let columns = cols
            .iter()
            .copied()
            .filter(|c| *c != self.output_col_index)
            .collect::<Vec<_>>();

        // the cols we're asking for all belong to the parent
        if columns.len() == cols.len() {
            ColumnSource::ExactCopy(ColumnRef {
                node: self.src.as_global(),
                columns: cols.to_vec(),
            })
        } else {
            // asked about the output col - reference this node with the other columns
            ColumnSource::GeneratedFromColumns(vec1![ColumnRef {
                node: our_index.as_global(),
                columns,
            }])
        }
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

        // Sort the records by group for efficient processing
        //
        // Chunk by group and assign group key to each group
        // then sort positives first
        let grouped_rs: Vec<(_, _)> = rs
            .into_iter()
            .sorted_by(|a, b| self.project_group(&***a).cmp(&self.project_group(&***b)))
            .chunk_by(|r| {
                self.project_group(&***r)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .map(|(key, group)| (key, group.into_iter().sorted_by(|a, b| a.cmp(b)).collect()))
            .collect();

        for (group_key, diffs) in grouped_rs {
            let mut current_window = vec![];

            let missed = match db.lookup(&self.group_by[..], &PointKey::from(group_key.clone())) {
                LookupResult::Some(records) => {
                    if replay.is_partial() {
                        lookups.push(Lookup {
                            on: *us,
                            cols: self.group_by.clone(),
                            key: group_key
                                .try_into()
                                .expect("Expected at least 1 element, found empty group"),
                        });
                    }

                    current_window.extend(records);

                    false
                }
                LookupResult::Missing => true,
            };

            self.process_window(
                diffs,
                &mut current_window,
                missed,
                &mut out,
                &mut misses,
                us,
                replay,
            )?;
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

    /// Project the columns we are grouping by out of the given record
    fn project_group<'rec, R>(&self, rec: &'rec R) -> Vec<&'rec DfValue>
    where
        R: Indices<'static, usize, Output = DfValue> + ?Sized,
    {
        rec.indices(self.group_by.clone())
            .expect("Invalid record length")
    }

    /// Project the columns we are partitioning by out of the given record
    fn project_partition<'rec, R>(&self, rec: &'rec R) -> Vec<&'rec DfValue>
    where
        R: Indices<'static, usize, Output = DfValue> + ?Sized,
    {
        rec.indices(self.partition_by.clone())
            .expect("Invalid record length")
    }

    #[allow(clippy::too_many_arguments)]
    fn process_window(
        &self,
        diffs: Vec<Record>,
        window: &mut Vec<Cow<[DfValue]>>,
        missed: bool,
        out: &mut Vec<Record>,
        misses: &mut Vec<Miss>,
        us: IndexPair,
        replay: &ReplayContext,
    ) -> ReadySetResult<()> {
        // Try to be smart about the number of +/- rows emitted
        // keep track of the modified partitions and only emit those
        // instead of emitting the entire window
        let mut modified_partitions = HashSet::new();
        // map partition keys to their records for faster lookups
        // and smarter emissions
        let mut partition_to_records: HashMap<Vec<DfValue>, Vec<Cow<[DfValue]>>> = HashMap::new();
        // map partition keys to changes (+/-) to be applied
        let mut changes: HashMap<Vec<_>, Vec<(_, bool)>> = HashMap::new();

        {
            // keys of the partitions affected by the diffs
            // scoped so it's immediately dropped after the loop
            let required_keys: HashSet<Vec<DfValue>> = diffs
                .iter()
                .map(|r| self.project_partition(&***r).into_iter().cloned().collect())
                .collect();

            for row in &*window {
                let key = self
                    .project_partition(&**row)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();

                // Skip partitions that will not be affected by the diffs
                if !required_keys.contains(&key) {
                    continue;
                }

                partition_to_records
                    .entry(key)
                    .or_default()
                    .push(row.clone());
            }
        }

        for record in diffs {
            if missed {
                misses.push(
                    Miss::builder()
                        .on(*us)
                        .lookup_idx(self.group_by.clone())
                        .lookup_key(self.group_by.clone())
                        .replay(replay)
                        .record(record.row().clone())
                        .build(),
                );
                continue;
            }

            let is_positive = record.is_positive();
            let mut r = record.into_row();

            r.push(DfValue::None);

            let partition_key = self
                .project_partition(&*r)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();

            modified_partitions.insert(partition_key.clone());

            changes
                .entry(partition_key)
                .or_default()
                .push((Cow::from(r), is_positive));
        }

        for partition_key in modified_partitions {
            // can be empty if this is the first +ve row
            // in this partition
            let mut partition = partition_to_records
                .remove(&partition_key)
                .unwrap_or_default();

            let partition_changes = changes.remove(&partition_key).unwrap();

            if self.is_cumulative() {
                partition.sort_by(|a, b| self.order_by.cmp(a, b));

                // apply diffs to the ordered partition, send the
                // negatives out, and record the index of the
                // earliest change so we can save time on recomputation
                let changes_offset = apply_diffs_to_ordered_partition(
                    &mut partition,
                    partition_changes,
                    self.output_col_index,
                    &self.order_by,
                    out,
                )?;

                self.function.apply_cumulative(
                    &mut partition,
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
                    &mut partition,
                    partition_changes,
                    self.output_col_index,
                    &self.output_col_type,
                )?;

                out.extend(partition.iter().map(|r| Record::Positive((**r).to_vec())));
            }
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

    partition.extend(positives);
    partition.retain(|r| !negatives.remove(&r[..col_index]));

    if !negatives.is_empty() {
        return Err(ReadySetError::Internal(
            "Negative record not found".to_string(),
        ));
    }

    Ok(())
}

/// Given the sorted partition and the diffs, sorts the
/// positives by their `ORDER BY` value and merges the
/// diffs with the partition. Also applies the negative diffs.
/// Returns the index of the first change (+ or -)
fn apply_diffs_to_ordered_partition<'a>(
    partition: &mut Vec<Cow<'a, [DfValue]>>,
    diffs: Vec<(Cow<'a, [DfValue]>, bool)>,
    col_index: usize,
    order: &Order,
    out: &mut Vec<Record>,
) -> ReadySetResult<usize> {
    let mut earliest = usize::MAX;

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
                } else {
                    earliest = earliest.min(idx);
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
                } else {
                    earliest = earliest.min(idx);
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

    Ok(if earliest == usize::MAX { 0 } else { earliest })
}
