use std::fmt::{self, Debug, Display};

use common::{DfValue, IndexType};
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::grouped::extremum::Extremum;
use dataflow::ops::union;
use dataflow::PostLookupAggregates;
use derive_more::From;
use itertools::Itertools;
use nom_sql::{
    BinaryOperator, ColumnSpecification, DialectDisplay, Expr, OrderType, Relation, SqlIdentifier,
};
use readyset_client::{PlaceholderIdx, ViewPlaceholder};
use readyset_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};
use vec1::Vec1;

use crate::Column;

/// Expressions which can be emitted by a [`Project`] node.
///
/// This is an enum so that emitted columns can be MIR [`Column`]s, to allow for them to contain
/// aliases.
///
/// [`Project`]: MirNodeInner::Project
#[derive(Clone, Debug, Serialize, Deserialize, From)]
pub enum ProjectExpr {
    /// Emit a (named) column verbatim from the parent
    Column(Column),

    /// Project an expression, using the given alias to name the resulting column (which will have
    /// no [`table`]). This should probably never contain [`Expr::Column`].
    ///
    /// [`table`]: nom_sql::Column::table
    Expr { expr: Expr, alias: SqlIdentifier },
}

impl Display for ProjectExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProjectExpr::Column(col) => write!(f, "{col}"),
            ProjectExpr::Expr { expr, alias } => {
                write!(f, "{alias}: {}", expr.display(readyset_sql::Dialect::MySQL))
            }
        }
    }
}

/// An individual column in the `key` of a [`ViewKey`]
///
/// [`ViewKey`]: MirNodeInner::ViewKey
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewKeyColumn {
    pub column: Column,
    pub op: BinaryOperator,
    pub placeholder_idx: PlaceholderIdx,
}

impl Display for ViewKeyColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ViewKeyColumn {
            column,
            op,
            placeholder_idx,
        } = self;
        write!(f, "{column} {op} {placeholder_idx}")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        column_specs: Vec<ColumnSpecification>,
        primary_key: Option<Box<[Column]>>,
        unique_keys: Box<[Box<[Column]>]>,
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
        /// Columns to use as the join keys. Each tuple corresponds to a column in the left parent
        /// and column in the right parent.
        on: Vec<(Column, Column)>,
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
        /// Columns to use as the join keys. Each tuple corresponds to a column in the left parent
        /// and column in the right parent.
        on: Vec<(Column, Column)>,
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
        /// Columns to use as the join keys. Each tuple corresponds to a column in the left parent
        /// and column in the right parent.
        on: Vec<(Column, Column)>,
        /// Columns (from both parents) to project in the output.
        project: Vec<Column>,
    },
    /// Left join where nodes in the right-hand side depend on columns in the left-hand side
    /// (referencing tables in `dependent_tables`). These are created during compilation for
    /// correlated subqueries, and must be removed entirely by rewrite passes before lowering
    /// to dataflow (any dependent joins occurring during dataflow lowering will cause the
    /// compilation to error).
    ///
    /// See [The Complete Story of Joins (in HyPer), §3.1 Dependent Join][hyper-joins] for more
    /// information.
    ///
    /// [hyper-joins]: http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    DependentLeftJoin {
        /// Columns to use as the join keys. Each tuple corresponds to a column in the left parent
        /// and column in the right parent.
        on: Vec<(Column, Column)>,
        /// Columns (from both parents) to project in the output.
        project: Vec<Column>,
    },
    /// Represents view key placeholders in a query that have not yet been added to the [`Leaf`][]
    /// node of the query.
    ///
    /// These nodes are created when we can't trivially convert filters using query parameters to
    /// keys in the [`Leaf`][] node of the query, eg in subqueries. If any are encountered in the
    /// final, post-rewrite MIR graph, they will return an error when lowering to dataflow.
    ///
    /// Currently, this is limited to only placeholders that appear on one side of an equal
    /// comparison with a column, but in the future that limitation may be lifted
    ///
    /// [`Leaf`]: MirNodeInner::Leaf
    ViewKey { key: Vec1<ViewKeyColumn> },
    /// Node which outputs a subset of columns from its parent in any order, and can evaluate
    /// expressions.
    ///
    /// Converted to [`Project`] when lowering to dataflow.
    ///
    /// [`Project`]: dataflow::ops::project::Project
    Project {
        /// List of columns and expressions to emit
        emit: Vec<ProjectExpr>,
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
    /// Alias all columns in the query to change their table
    ///
    /// This node will not be converted into a dataflow node when lowering MIR to dataflow.
    AliasTable { table: Relation },
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

        /// Whether or not this leaf node was already lowered to dataflow or not.
        lowered_to_df: bool,

        /// Optional set of columns and direction to order the results of lookups to this leaf
        order_by: Option<Vec<(Column, OrderType)>>,
        /// Optional limit for the set of results to lookups to this leaf
        limit: Option<usize>,
        /// Optional set of expression columns requested in the original query
        returned_cols: Option<Vec<Column>>,
        /// Row of default values to send back, for example if we're aggregating and no rows are
        /// found
        default_row: Option<Vec<DfValue>>,
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
            lowered_to_df: false,
            order_by: None,
            limit: None,
            returned_cols: None,
            default_row: None,
            aggregates: None,
        }
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
                if !column_specs.iter().any(|cs| c == cs.column) {
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
            | MirNodeInner::DependentJoin { project, .. }
            | MirNodeInner::DependentLeftJoin { project, .. } => {
                if !project.contains(&c) {
                    project.push(c);
                }
                Ok(true)
            }
            MirNodeInner::Project { emit, .. } => {
                emit.push(c.into());
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

    /// Returns `true` if self is a [`DependentJoin`] or [`DependentLeftJoin`].
    ///
    /// [`DependentJoin`]: MirNodeInner::DependentJoin
    /// [`DependentLeftJoin`]: MirNodeInner::DependentLeftJoin
    pub fn is_dependent_join(&self) -> bool {
        matches!(
            self,
            Self::DependentJoin { .. } | Self::DependentLeftJoin { .. }
        )
    }

    /// Returns `true` if self is a [`ViewKey`].
    ///
    /// [`ViewKey`]: MirNodeInner::ViewKey
    pub fn is_view_key(&self) -> bool {
        matches!(self, Self::ViewKey { .. })
    }

    pub(crate) fn description(&self) -> String {
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
                format!("{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Base {
                column_specs,
                unique_keys,
                ..
            } => format!(
                "B [{}; ⚷: {}]",
                column_specs
                    .iter()
                    .map(|cs| cs.column.name.as_str())
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
                format!("{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Filter { ref conditions, .. } => {
                format!("σ[{}]", conditions.display(readyset_sql::Dialect::MySQL))
            }
            MirNodeInner::ViewKey { ref key } => {
                format!("σ[{}]", key.iter().join(" AND "))
            }
            MirNodeInner::Identity => "≡".to_string(),
            MirNodeInner::Join {
                ref on,
                ref project,
                ..
            } => {
                let jc = on
                    .iter()
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "⋈ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::JoinAggregates => "AGG ⋈".to_string(),
            MirNodeInner::Leaf { ref keys, .. } => {
                let key_cols = keys
                    .iter()
                    .map(|(column, _)| column.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Leaf [⚷: {}]", key_cols)
            }
            MirNodeInner::LeftJoin {
                ref on,
                ref project,
                ..
            } => {
                let jc = on
                    .iter()
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "⟕ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::DependentJoin {
                ref on,
                ref project,
                ..
            } => {
                format!(
                    "⧑ | {} on: {}",
                    project.iter().map(|c| &c.name).join(", "),
                    on.iter()
                        .map(|(l, r)| format!("{}:{}", l.name, r.name))
                        .join(", ")
                )
            }
            MirNodeInner::DependentLeftJoin {
                ref on,
                ref project,
                ..
            } => {
                format!(
                    "⟕D | {} on: {}",
                    project.iter().map(|c| &c.name).join(", "),
                    on.iter()
                        .map(|(l, r)| format!("{}:{}", l.name, r.name))
                        .join(", ")
                )
            }
            MirNodeInner::Project { ref emit } => format!("π [{}]", emit.iter().join(", ")),
            MirNodeInner::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Distinct [γ: {}]", key_cols)
            }
            MirNodeInner::Paginate {
                ref order,
                ref limit,
                ..
            } => {
                format!("Paginate [limit: {}, {:?}]", limit, order)
            }
            MirNodeInner::TopK {
                ref order,
                ref limit,
                ..
            } => {
                format!("TopK [k: {}, {:?}]", limit, order)
            }
            MirNodeInner::Union {
                ref emit,
                ref duplicate_mode,
            } => {
                let symbol = match duplicate_mode {
                    union::DuplicateMode::BagUnion => '⊎',
                    union::DuplicateMode::UnionAll => '⋃',
                };
                emit.iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| e.name.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .join(&format!(" {} ", symbol))
            }
            MirNodeInner::AliasTable { ref table } => {
                format!("AliasTable [{}]", table.display_unquoted())
            }
        }
    }
}
