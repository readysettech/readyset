use std::fmt::{self, Debug, Formatter};

use common::{DataType, IndexType};
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::grouped::extremum::Extremum;
use dataflow::ops::union;
use dataflow::post_lookup::PostLookupAggregates;
use itertools::Itertools;
use nom_sql::{ColumnSpecification, Expr, OrderType, SqlIdentifier};
use noria::ViewPlaceholder;
use noria_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::node::BaseNodeAdaptation;
use crate::{Column, MirNodeRef};

#[derive(Clone, Serialize, Deserialize)]
pub enum MirNodeInner {
    /// Node that computes an aggregate function on a column grouped by another set of columns,
    /// outputting its result as an additional column.
    ///
    /// Converted to [`Aggregator`] when lowering to dataflow.
    ///
    /// [`Aggregator`]: dataflow::ops::grouped::aggregate::Aggregator
    Aggregation {
        /// Column to compute the aggregate function over
        on: Column,
        /// List of columns to group by
        group_by: Vec<Column>,
        /// The column name to use for the result of the aggregate, which will always be the last
        /// column
        output_column: Column,
        /// Which aggregate function we are computing
        kind: Aggregation,
    },
    /// Base node in the graph, corresponding to a snapshot of a full table in the upstream
    /// database.
    ///
    /// Converted to [`Base`] when lowering to dataflow.
    ///
    /// [`Aggregator`]: dataflow::node::special::Base
    Base {
        column_specs: Vec<(ColumnSpecification, Option<usize>)>,
        primary_key: Option<Box<[Column]>>,
        unique_keys: Box<[Box<[Column]>]>,
        adapted_over: Option<BaseNodeAdaptation>,
    },
    /// Node that computes the extreme value (minimum or maximum) of a column grouped by another
    /// set of columns, outputting its result as an additional column.
    ///
    /// Converted to [`ExtremumOperator`] when lowering to dataflow
    ///
    /// [`ExtremumOperator`]: dataflow::ops::grouped::extremum::ExtremumOperator
    Extremum {
        /// Column to compute the extremum of
        on: Column,
        /// List of columns to group by
        group_by: Vec<Column>,
        /// The column name to use for the extreme value, which will always be the last column
        output_column: Column,
        /// Which kind of extreme value to compute (minimum or maximum).
        kind: Extremum,
    },
    /// Node that filters its input to only rows where a particular expression evaluates to a
    /// truthy value.
    ///
    /// Converted to [`Filter`] when lowering to dataflow.
    ///
    /// [`Filter`]: dataflow::ops::filter::Filter
    Filter {
        /// Condition to filter on.
        ///
        /// Note that at this point this is still just the raw AST, so column references use only
        /// name and table (and don't support aliases).
        conditions: Expr,
    },
    /// Node which makes no changes to its input
    ///
    /// Converted to [`Identity`] when lowering to dataflow.
    ///
    /// [`Identity`]: dataflow::ops::identity::Identity
    Identity,
    /// Node which computes a join on its two parents by finding all rows in the left where the
    /// values in `on_left` are equal to the values of `on_right` on the right
    ///
    /// Converted to [`Join`] with [`JoinType::Inner`] when lowering to dataflow.
    ///
    /// [`Join`]: dataflow::ops::join::Join
    /// [`JoinType::Inner`]: dataflow::ops::join::JoinType::Inner
    Join {
        /// Columns in the first parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_right`
        on_left: Vec<Column>,
        /// Columns in the second parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_left`
        on_right: Vec<Column>,
        /// Columns (from both parents) to project in the output.
        project: Vec<Column>,
    },
    /// JoinAggregates is a special type of join for joining two aggregates together. This is
    /// different from other operators in that it doesn't map 1:1 to a SQL operator and there are
    /// several invariants we follow. It is used to support multiple aggregates in queries by
    /// joining pairs of aggregates together using custom join logic. We only join nodes with inner
    /// types of Aggregation or Extremum. For any group of aggregates, we will make N-1
    /// JoinAggregates to join them all back together. The first JoinAggregates will join the first
    /// two aggregates together. The next JoinAggregates will join that JoinAggregates node to the
    /// next aggregate in the list, so on and so forth. Each aggregate will share identical
    /// group_by columns which are deduplicated at every join, so by the end we have every
    /// unique column (the actual aggregate columns) from each aggregate node, and a single
    /// version of each group_by column in the final join.
    JoinAggregates,
    /// Node which computes a *left* join on its two parents by finding all rows in the right where
    /// the values in `on_right` are equal to the values of `on_left` on the left
    ///
    /// Converted to [`Join`] with [`JoinType::Left`] when lowering to dataflow.
    ///
    /// [`Join`]: dataflow::ops::join::Join
    /// [`JoinType::Left`]: dataflow::ops::join::JoinType::Left
    LeftJoin {
        /// Columns in the first parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_right`
        on_left: Vec<Column>,
        /// Columns in the second parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_left`
        on_right: Vec<Column>,
        /// Columns (from both parents) to project in the output.
        project: Vec<Column>,
    },
    /// Join where nodes in the right-hand side depend on columns in the left-hand side
    /// (referencing tables in `dependent_tables`). These are created during compilation for
    /// correlated subqueries, and must be removed entirely by rewrite passes before lowering
    /// to dataflow (any dependent joins occurring during dataflow lowering will cause the
    /// compilation to error).
    ///
    /// See [The Complete Story of Joins (in HyPer), §3.1 Dependent Join][hyper-joins] for more
    /// information.
    ///
    /// [hyper-joins]: http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    DependentJoin {
        /// Columns in the first parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_right`
        on_left: Vec<Column>,
        /// Columns in the second parent to use as the join key.
        ///
        /// # Invariants
        ///
        /// * This must have the same length as `on_left`
        on_right: Vec<Column>,
        /// Columns (from both parents) to project in the output.
        project: Vec<Column>,
    },
    /// group columns
    // currently unused
    #[allow(dead_code)]
    Latest { group_by: Vec<Column> },
    /// Node which outputs a subset of columns from its parent in any order, and can evaluate
    /// expressions.
    ///
    /// Project nodes always emit columns first, then expressions, then literals.
    ///
    /// Converted to [`Project`] when lowering to dataflow.
    ///
    /// [`Project`]: dataflow::ops::project::Project
    Project {
        /// List of columns, in order, to emit verbatim from the parent
        emit: Vec<Column>,
        /// List of pairs of `(alias, expr)`, giving expressions to evaluate and the names for the
        /// columns for the results of those expressions.
        ///
        /// Note that at this point these expressions are still just raw AST, so column references
        /// use only name and table (and don't support aliases).
        expressions: Vec<(SqlIdentifier, Expr)>,
        /// List of pairs of `(alias, value)`, giving literal values to emit in the output
        literals: Vec<(SqlIdentifier, DataType)>,
    },
    /// Node which computes a union of all of its (two or more) parents.
    ///
    /// Converted to [`Union`] when lowering to dataflow
    ///
    /// [`Union`]: dataflow::ops::union::Union
    Union {
        /// Columns to emit from each parent
        ///
        /// # Invariants
        ///
        /// * This will always have the same length as the number of parents
        emit: Vec<Vec<Column>>,
        /// Specification for how the union operator should operate with respect to rows that exist
        /// in all parents.
        duplicate_mode: union::DuplicateMode,
    },
    /// Node which orders its input rows within a group, then emits an extra page number column
    /// (which will always have a name given by [`PAGE_NUMBER_COL`]) for the page number of the
    /// rows within that group, with page size given by `limit`.
    ///
    /// Converted to [`Paginate`] when lowering to dataflow.
    ///
    /// [`PAGE_NUMBER_COL`]: crate::PAGE_NUMBER_COL
    /// [`Paginate`]: dataflow::ops::paginate::Paginate
    Paginate {
        /// Set of columns used for ordering the results
        order: Option<Vec<(Column, OrderType)>>,
        /// Set of columns that are indexed to form a unique grouping of results
        group_by: Vec<Column>,
        /// How many rows per page
        limit: usize,
    },
    /// Node which emits only the top `limit` records per group, ordered by a set of columns
    ///
    /// Converted to [`TopK`] when lowering to dataflow.
    ///
    /// [`TopK`]: dataflow::ops::topk::TopK
    TopK {
        /// Set of columns used for ordering the results
        order: Option<Vec<(Column, OrderType)>>,
        /// Set of columns that are indexed to form a unique grouping of results
        group_by: Vec<Column>,
        /// Numeric literal that determines the number of results stored per group. Taken from the
        /// LIMIT clause
        limit: usize,
    },
    /// Node which emits only distinct rows per some group.
    ///
    /// Converted to [`Aggregator`] with [`Aggregation::Count`] when lowering to dataflow.
    ///
    /// [`Aggregator`]: dataflow::ops::grouped::aggregate::Aggregator
    /// [`Aggregation::Count`]: dataflow::ops::grouped::aggregate::Aggregation::Count
    Distinct { group_by: Vec<Column> },
    /// Reuse a node that's already in the graph.
    ///
    /// Note that this node is used even when query graph reuse is disabled, for the base nodes in
    /// newly created query graphs (either querying from a base table or an explicit named `VIEW`)
    Reuse {
        /// Reference to the node to reuse
        node: MirNodeRef,
    },
    /// Alias all columns in the query to change their table
    ///
    /// This node will not be converted into a dataflow node when lowering MIR to dataflow.
    AliasTable { table: SqlIdentifier },
    /// Leaf node of a query, which specifies the columns to index on, and an optional set of
    /// operations to perform post-lookup.
    ///
    /// Converted to a [`Reader`] node when lowering to dataflow.
    ///
    /// [`Reader`]: dataflow::node::special::reader::Reader
    Leaf {
        /// Keys is a tuple of the key column, and if the column was derived from a SQL
        /// placeholder, the index of the placeholder in the SQL query.
        keys: Vec<(Column, ViewPlaceholder)>,
        index_type: IndexType,

        /// Optional set of columns and direction to order the results of lookups to this leaf
        order_by: Option<Vec<(Column, OrderType)>>,
        /// Optional limit for the set of results to lookups to this leaf
        limit: Option<usize>,
        /// Optional set of expression columns requested in the original query
        returned_cols: Option<Vec<Column>>,
        /// Row of default values to send back, for example if we're aggregating and no rows are
        /// found
        default_row: Option<Vec<DataType>>,
        /// Aggregates to perform in the reader on result sets for keys after performing the lookup
        aggregates: Option<PostLookupAggregates<Column>>,
    },
}

impl MirNodeInner {
    /// Construct a new [`MirNodeInner::Leaf`] with the given keys and index
    /// type, without any post-lookup operations
    pub fn leaf(keys: Vec<(Column, ViewPlaceholder)>, index_type: IndexType) -> Self {
        Self::Leaf {
            keys,
            index_type,
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
        }
    }

    pub(crate) fn description(&self) -> String {
        format!("{:?}", self)
    }

    /// Attempt to add the given column to the set of columns projected by this node.
    ///
    /// If this node is not a node that has control over the columns it projects (such as a filter
    /// node), returns `Ok(false)`
    pub(crate) fn add_column(&mut self, c: Column) -> ReadySetResult<bool> {
        match self {
            MirNodeInner::Aggregation { group_by, .. } => {
                group_by.push(c);
                Ok(true)
            }
            MirNodeInner::Base { column_specs, .. } => {
                if !column_specs.iter().any(|(cs, _)| c == cs.column) {
                    internal!("can't add columns to base nodes!")
                }
                Ok(true)
            }
            MirNodeInner::Extremum { group_by, .. } => {
                group_by.push(c);
                Ok(true)
            }
            MirNodeInner::Join { project, .. }
            | MirNodeInner::LeftJoin { project, .. }
            | MirNodeInner::DependentJoin { project, .. } => {
                if !project.contains(&c) {
                    project.push(c);
                }
                Ok(true)
            }
            MirNodeInner::Project { emit, .. } => {
                emit.push(c);
                Ok(true)
            }
            MirNodeInner::Union { emit, .. } => {
                for e in emit.iter_mut() {
                    e.push(c.clone());
                }
                Ok(true)
            }
            MirNodeInner::Distinct { group_by, .. } => {
                group_by.push(c);
                Ok(true)
            }
            MirNodeInner::Paginate { group_by, .. } => {
                group_by.push(c);
                Ok(true)
            }
            MirNodeInner::TopK { group_by, .. } => {
                group_by.push(c);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(crate) fn can_reuse_as(&self, other: &MirNodeInner) -> bool {
        match *self {
            MirNodeInner::Reuse { .. } => (), // handled below
            _ => {
                // we're not a `Reuse` ourselves, but the other side might be
                if let MirNodeInner::Reuse { ref node } = *other {
                    // it is, so dig deeper
                    // this does not check the projected columns of the inner node for two
                    // reasons:
                    // 1) our own projected columns aren't accessible on `MirNodeInner`, but
                    //    only on the outer `MirNode`, which isn't accessible here; but more
                    //    importantly
                    // 2) since this is already a node reuse, the inner, reused node must have
                    //    *at least* a superset of our own (inaccessible) projected columns.
                    // Hence, it is sufficient to check the projected columns on the parent
                    // `MirNode`, and if that check passes, it also holds for the nodes reused
                    // here.
                    return self.can_reuse_as(&node.borrow().inner);
                } else {
                    // handled below
                }
            }
        }

        match self {
            MirNodeInner::Aggregation {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
                ..
            } => {
                match *other {
                    MirNodeInner::Aggregation {
                        ref on,
                        ref group_by,
                        ref kind,
                        ..
                    } => {
                        // TODO(malte): this is stricter than it needs to be, as it could cover
                        // COUNT-as-SUM-style relationships.
                        our_on == on && our_group_by == group_by && our_kind == kind
                    }
                    _ => false,
                }
            }
            MirNodeInner::Base {
                column_specs: our_column_specs,
                primary_key: our_primary_key,
                unique_keys: our_keys,
                adapted_over: our_adapted_over,
            } => {
                match other {
                    MirNodeInner::Base {
                        column_specs,
                        unique_keys,
                        primary_key,
                        ..
                    } => {
                        // if we are instructed to adapt an earlier base node, we cannot reuse
                        // anything directly; we'll have to keep a new MIR node here.
                        if our_adapted_over.is_some() {
                            // TODO(malte): this is a bit more conservative than it needs to be,
                            // since base node adaptation actually *changes* the underlying base
                            // node, so we will actually reuse. However, returning false here
                            // terminates the reuse search unnecessarily. We should handle this
                            // special case.
                            return false;
                        }
                        // note that as long as we are not adapting a previous base node,
                        // we do *not* need `adapted_over` to *match*, since current reuse
                        // does not depend on how base node was created from an earlier one
                        our_column_specs == column_specs
                            && our_keys == unique_keys
                            && primary_key == our_primary_key
                    }
                    _ => false,
                }
            }
            MirNodeInner::Extremum {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
                ..
            } => match *other {
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => our_on == on && our_group_by == group_by && our_kind == kind,
                _ => false,
            },
            MirNodeInner::Filter {
                conditions: ref our_conditions,
            } => match *other {
                MirNodeInner::Filter { ref conditions } => our_conditions == conditions,
                _ => false,
            },
            MirNodeInner::Join {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeInner::Join {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        // TODO(malte): column order does not actually need to match, but this only
                        // succeeds if it does.
                        our_on_left == on_left && our_on_right == on_right && our_project == project
                    }
                    _ => false,
                }
            }
            MirNodeInner::JoinAggregates => matches!(*other, MirNodeInner::JoinAggregates),
            MirNodeInner::LeftJoin {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeInner::LeftJoin {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        // TODO(malte): column order does not actually need to match, but this only
                        // succeeds if it does.
                        our_on_left == on_left && our_on_right == on_right && our_project == project
                    }
                    _ => false,
                }
            }
            MirNodeInner::Project {
                emit: ref our_emit,
                literals: ref our_literals,
                expressions: ref our_expressions,
            } => match *other {
                MirNodeInner::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => our_emit == emit && our_literals == literals && our_expressions == expressions,
                _ => false,
            },
            MirNodeInner::Distinct {
                group_by: ref our_group_by,
            } => match *other {
                MirNodeInner::Distinct { ref group_by } => group_by == our_group_by,
                _ => false,
            },
            MirNodeInner::Reuse { node: ref us } => {
                match *other {
                    // both nodes are `Reuse` nodes, so we simply compare the both sides' reuse
                    // target
                    MirNodeInner::Reuse { ref node } => us.borrow().can_reuse_as(&*node.borrow()),
                    // we're a `Reuse`, the other side isn't, so see if our reuse target's `inner`
                    // can be reused for the other side. It's sufficient to check the target's
                    // `inner` because reuse implies that our target has at least a superset of our
                    // projected columns (see earlier comment).
                    _ => us.borrow().inner.can_reuse_as(other),
                }
            }
            MirNodeInner::Paginate {
                order: our_order,
                group_by: our_group_by,
                limit: our_limit,
            } => match other {
                MirNodeInner::Paginate {
                    order,
                    group_by,
                    limit,
                } => order == our_order && group_by == our_group_by && limit == our_limit,
                _ => false,
            },
            MirNodeInner::TopK {
                order: our_order,
                group_by: our_group_by,
                limit: our_limit,
            } => match other {
                MirNodeInner::TopK {
                    order,
                    group_by,
                    limit,
                } => order == our_order && group_by == our_group_by && limit == our_limit,
                _ => false,
            },
            MirNodeInner::Leaf {
                keys: ref our_keys, ..
            } => match *other {
                MirNodeInner::Leaf { ref keys, .. } => keys == our_keys,
                _ => false,
            },
            MirNodeInner::Union {
                emit: ref our_emit,
                duplicate_mode: ref our_duplicate_mode,
            } => match *other {
                MirNodeInner::Union {
                    ref emit,
                    ref duplicate_mode,
                } => emit == our_emit && our_duplicate_mode == duplicate_mode,
                _ => false,
            },
            MirNodeInner::AliasTable { .. } => matches!(
                other,
                MirNodeInner::AliasTable { .. } | MirNodeInner::Identity
            ),
            _ => unimplemented!(),
        }
    }

    /// Returns `true` if self is a [`DependentJoin`].
    ///
    /// [`DependentJoin`]: MirNodeInner::DependentJoin
    pub fn is_dependent_join(&self) -> bool {
        matches!(self, Self::DependentJoin { .. })
    }
}

impl Debug for MirNodeInner {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            MirNodeInner::Aggregation {
                ref on,
                ref group_by,
                ref kind,
                ..
            } => {
                let op_string = match *kind {
                    Aggregation::Count { .. } => format!("|*|({})", on.name.as_str()),
                    Aggregation::Sum => format!("𝛴({})", on.name.as_str()),
                    Aggregation::Avg => format!("AVG({})", on.name.as_str()),
                    Aggregation::GroupConcat { separator: ref s } => {
                        format!("||([{}], \"{}\")", on.name.as_str(), s.as_str())
                    }
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Base {
                column_specs,
                unique_keys,
                ..
            } => write!(
                f,
                "B [{}; ⚷: {}]",
                column_specs
                    .iter()
                    .map(|&(ref cs, _)| cs.column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                unique_keys
                    .iter()
                    .map(|k| k
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .join(";")
            ),
            MirNodeInner::Extremum {
                ref on,
                ref group_by,
                ref kind,
                ..
            } => {
                let op_string = match *kind {
                    Extremum::Min => format!("min({})", on.name.as_str()),
                    Extremum::Max => format!("max({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Filter { ref conditions, .. } => {
                write!(f, "σ[{}]", conditions)
            }
            MirNodeInner::Identity => write!(f, "≡"),
            MirNodeInner::Join {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋈ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::JoinAggregates => {
                write!(f, "AGG ⋈")
            }
            MirNodeInner::Leaf { ref keys, .. } => {
                let key_cols = keys
                    .iter()
                    .map(|(column, _)| column.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Leaf [⚷: {}]", key_cols)
            }
            MirNodeInner::LeftJoin {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋉ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::DependentJoin {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                write!(
                    f,
                    "⧑ | {} on: {}",
                    project.iter().map(|c| &c.name).join(", "),
                    on_left
                        .iter()
                        .zip(on_right)
                        .map(|(l, r)| format!("{}:{}", l.name, r.name))
                        .join(", ")
                )
            }
            MirNodeInner::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⧖ γ[{}]", key_cols)
            }
            MirNodeInner::Project {
                ref emit,
                ref literals,
                ref expressions,
            } => write!(
                f,
                "π [{}]",
                emit.iter()
                    .map(|c| c.name.clone())
                    .chain(
                        expressions
                            .iter()
                            .map(|&(ref n, ref e)| format!("{}: {}", n, e).into())
                    )
                    .chain(
                        literals
                            .iter()
                            .map(|&(ref n, ref v)| format!("{}: {}", n, v).into())
                    )
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            MirNodeInner::Reuse { ref node } => write!(
                f,
                "Reuse [{}: {}]",
                node.borrow().versioned_name(),
                node.borrow()
            ),
            MirNodeInner::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Distinct [γ: {}]", key_cols)
            }
            MirNodeInner::Paginate {
                ref order,
                ref limit,
                ..
            } => {
                write!(f, "Paginate [limit: {}, {:?}]", limit, order)
            }
            MirNodeInner::TopK {
                ref order,
                ref limit,
                ..
            } => {
                write!(f, "TopK [k: {}, {:?}]", limit, order)
            }
            MirNodeInner::Union {
                ref emit,
                ref duplicate_mode,
            } => {
                let symbol = match duplicate_mode {
                    union::DuplicateMode::BagUnion => '⊎',
                    union::DuplicateMode::UnionAll => '⋃',
                };
                let cols = emit
                    .iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| e.name.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .join(&format!(" {} ", symbol));

                write!(f, "{}", cols)
            }
            MirNodeInner::AliasTable { ref table } => {
                write!(f, "AliasTable [{}]", table)
            }
        }
    }
}
