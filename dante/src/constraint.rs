//! Constraint vocabulary and supporting descriptor types for the
//! constraint-based query generator.
//!
//! A constraint is a single declarative assertion about the query being
//! generated. Constraints are the primitive building blocks that patterns
//! are composed from.

use readyset_sql::ast::{BinaryOperator, JoinOperator, NullOrder, OrderType, SqlType};

use crate::var::VarId;

/// Categorizes SQL types for constraint matching.
///
/// Used in `ColumnTypeClass` and `TypeCompatible` constraints to express
/// what type a column should have without pinning to a specific SQL type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeClass {
    /// Matches any type
    Any,
    /// Integer types (INT, BIGINT, SMALLINT, TINYINT, etc.)
    Integer,
    /// Numeric types (INTEGER, DECIMAL, FLOAT, DOUBLE, etc.)
    Numeric,
    /// String types (VARCHAR, TEXT, CHAR, etc.)
    String,
    /// Date/time types (DATE, DATETIME, TIMESTAMP, TIME)
    DateTime,
    /// Exact SQL type
    Exact(SqlType),
}

/// Aggregate function specifier for `ProjectAggregate` and `Having` constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFn {
    Count { distinct: bool },
    Sum { distinct: bool },
    Avg { distinct: bool },
    Min,
    Max,
    GroupConcat,
    JsonObjectAgg,
    ArrayAgg,
}

/// Scalar function specifier for `ProjectFunction` constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarFn {
    Coalesce,
    IfNull,
    NullIf,
    Cast,
    Upper,
    Lower,
    Trim,
    Substring,
    Concat,
    ConcatWs,
    Round,
    Length,
    Month,
    DayOfWeek,
    Greatest,
    Least,
}

/// Window function specifier for `WindowFunction` constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
}

/// What kind of literal value a `ProjectLiteral` emits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiteralKind {
    Integer,
    Float,
    String,
    Null,
    Boolean,
}

/// What kind of expression-position subquery this is.
///
/// Expression-position subqueries are used as values inline in WHERE / SELECT
/// (`EXISTS`, `IN`, scalar). They never produce an outer relation alias, so
/// they live in [`Constraint::SubqueryExpr`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubqueryExprKind {
    /// Uncorrelated `EXISTS (SELECT ...)`
    ExistsUncorrelated,
    /// Correlated `EXISTS (SELECT ... WHERE inner.col = outer.col)`
    ExistsCorrelated,
    /// `col IN (SELECT ...)`
    InSubquery,
    /// Scalar subquery in SELECT or WHERE
    ScalarSubquery,
}

/// What kind of relation-position subquery this is.
///
/// Relation-position subqueries produce an outer relation that other
/// constraints can reference by alias VarId — the resolver `env.bind`s the
/// alias to a fresh SQL identifier (`cteN`, `sqN`, ...) so outer
/// `From(alias)` / `ProjectColumn { table: alias, .. }` references resolve
/// through env uniformly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubqueryRelationKind {
    /// Common Table Expression (`WITH cte0 AS (SELECT ...) SELECT ... FROM cte0`)
    Cte,
    /// Inline subquery used as the right side of a JOIN
    /// (`... JOIN (SELECT ...) AS sqN ON ...`). The alias var is referenced
    /// by a sibling `Join { right: JoinRight::Table(alias) }` constraint.
    JoinTarget,
}

/// What the right side of a JOIN is. Always a relation var — base table,
/// CTE alias, FROM-subquery alias, or `JoinTarget` SubqueryRelation alias.
/// JOIN-subqueries and CTE joins are emitted as a sibling
/// `Constraint::SubqueryRelation` whose alias var goes here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinRight {
    /// Any relation var (base table, CTE alias, derived alias, or
    /// `JoinTarget` alias). The resolver looks up the var's binding to
    /// decide whether to emit a table reference or an inline subquery.
    Table(VarId),
}

/// Dialect support for a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectSupport {
    Both,
    MySqlOnly,
    PostgresOnly,
}

impl DialectSupport {
    /// Returns true if this pattern supports the given dialect.
    pub fn supports(&self, dialect: readyset_sql::Dialect) -> bool {
        match self {
            DialectSupport::Both => true,
            DialectSupport::MySqlOnly => dialect == readyset_sql::Dialect::MySQL,
            DialectSupport::PostgresOnly => dialect == readyset_sql::Dialect::PostgreSQL,
        }
    }
}

/// A single declarative assertion about the query being generated.
///
/// Constraints are the primitive building blocks that patterns are composed
/// from. The resolver processes a set of constraints to produce variable
/// bindings and ultimately an AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint {
    // --- Schema-level ---
    /// Variable R must resolve to a relation (base table, alias, or derived).
    BaseTable(VarId),
    /// Relation `alias` is an alias of relation `original`. Both refer to
    /// the same physical table but with different SQL alias names. This
    /// enables self-joins: `FROM t0 AS a0 JOIN t0 AS a1 ON ...`
    AliasOf { alias: VarId, original: VarId },
    /// Variable C must resolve to a column in relation R.
    ColumnExists { col: VarId, table: VarId },
    /// Variable C must have a type compatible with the given type class.
    ColumnTypeClass { col: VarId, type_class: TypeClass },
    /// Two column variables must have compatible types (for joins, comparisons).
    TypeCompatible(VarId, VarId),

    // --- Variable equality/inequality ---
    /// Two variables must resolve to the SAME value (e.g., same table for self-join).
    Eq(VarId, VarId),
    /// Two variables must resolve to DIFFERENT values (e.g., force non-self-join).
    NotEq(VarId, VarId),

    // --- Structural ---
    /// Table T appears in the FROM clause.
    From(VarId),
    /// A JOIN between left table and a right-side entity.
    Join {
        operator: JoinOperator,
        right: JoinRight,
        left_col: VarId,
        right_col: VarId,
    },
    /// Column C from table T appears in the SELECT list.
    ProjectColumn { col: VarId, table: VarId },
    /// An aggregate function over column C appears in the SELECT list.
    ProjectAggregate {
        function: AggregateFn,
        col: VarId,
        table: VarId,
    },
    /// A scalar function call appears in the SELECT list.
    ProjectFunction {
        function: ScalarFn,
        args: Vec<(VarId, VarId)>,
    },
    /// A literal value appears in the SELECT list.
    ProjectLiteral { literal: LiteralKind },
    /// Column C is in the GROUP BY clause.
    GroupBy { col: VarId, table: VarId },
    /// An aggregate expression appears in the HAVING clause.
    Having {
        function: AggregateFn,
        col: VarId,
        table: VarId,
        op: BinaryOperator,
    },
    /// A parametrizable filter in HAVING on a GROUP BY key column.
    /// Unlike `Having` (which wraps an aggregate function), this emits
    /// `HAVING col op ?` where `col` must also be a GROUP BY key.
    /// The hoisting pass can extract these to the outer WHERE.
    HavingKeyFilter {
        col: VarId,
        table: VarId,
        op: BinaryOperator,
    },
    /// A WHERE filter with a parameter placeholder.
    WhereParam {
        col: VarId,
        table: VarId,
        op: BinaryOperator,
    },
    /// An IN-list parameter filter.
    WhereInParam {
        col: VarId,
        table: VarId,
        num_values: u8,
    },
    /// A range parameter (col >= ? AND col <= ?).
    WhereRangeParam { col: VarId, table: VarId },
    /// A LIKE/NOT LIKE filter.
    WhereLike {
        col: VarId,
        table: VarId,
        negated: bool,
    },
    /// An IS NULL / IS NOT NULL filter.
    WhereIsNull {
        col: VarId,
        table: VarId,
        negated: bool,
    },
    /// A BETWEEN filter with parameter placeholders.
    WhereBetweenParam { col: VarId, table: VarId },
    /// A column-vs-column comparison.
    WhereColumnCompare {
        left_col: VarId,
        left_table: VarId,
        op: BinaryOperator,
        right_col: VarId,
        right_table: VarId,
    },
    /// An OR combination of WHERE conditions.
    WhereOr { conditions: Vec<Constraint> },
    /// ORDER BY clause.
    OrderBy {
        col: VarId,
        table: VarId,
        direction: OrderType,
        null_order: Option<NullOrder>,
    },
    /// LIMIT (and optional OFFSET).
    Limit { limit: u64, offset: Option<u64> },
    /// DISTINCT in the SELECT list.
    Distinct,

    // --- Subquery / CTE ---
    /// An expression-position subquery (`EXISTS`, `IN`, scalar). Has no
    /// outer-relation alias because the subquery is consumed as a value
    /// inline in WHERE / SELECT.
    SubqueryExpr {
        kind: SubqueryExprKind,
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },
    /// A relation-position subquery — the subquery produces an outer
    /// relation that other constraints reference by `alias`. The
    /// resolver binds `alias` to a fresh SQL identifier (`cteN`, etc.)
    /// so outer `From(alias)` / `ProjectColumn { table: alias, .. }`
    /// references resolve through env.
    SubqueryRelation {
        kind: SubqueryRelationKind,
        alias: VarId,
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },

    // --- Disjunctive ---
    /// Disjunctive constraint: if all constraints in the first branch are
    /// satisfied, nothing happens. If any constraint in the first branch
    /// fails, the second branch is applied instead.
    ///
    /// Used during pattern composition to express invariants like "these two
    /// table variables must be different tables, or one must be aliased."
    Or(Vec<Constraint>, Vec<Constraint>),

    // --- Window functions ---
    /// A window function in the SELECT list.
    WindowFunction {
        function: WindowFn,
        partition_col: Option<(VarId, VarId)>,
        order_col: Option<(VarId, VarId)>,
        order_type: Option<OrderType>,
    },
}

impl Constraint {
    /// Collect all directly referenced VarIds (non-recursive into subqueries/CTEs).
    pub fn var_ids(&self) -> Vec<VarId> {
        match self {
            Constraint::BaseTable(v) => vec![*v],
            Constraint::AliasOf { alias, original } => vec![*alias, *original],
            Constraint::ColumnExists { col, table } => vec![*col, *table],
            Constraint::ColumnTypeClass { col, .. } => vec![*col],
            Constraint::TypeCompatible(a, b) | Constraint::Eq(a, b) | Constraint::NotEq(a, b) => {
                vec![*a, *b]
            }
            Constraint::From(v) => vec![*v],
            Constraint::Join {
                right: JoinRight::Table(t),
                left_col,
                right_col,
                ..
            } => vec![*left_col, *right_col, *t],
            Constraint::ProjectColumn { col, table }
            | Constraint::GroupBy { col, table }
            | Constraint::WhereRangeParam { col, table }
            | Constraint::WhereBetweenParam { col, table } => vec![*col, *table],
            Constraint::ProjectAggregate { col, table, .. }
            | Constraint::Having { col, table, .. }
            | Constraint::HavingKeyFilter { col, table, .. }
            | Constraint::WhereParam { col, table, .. }
            | Constraint::WhereInParam { col, table, .. }
            | Constraint::WhereLike { col, table, .. }
            | Constraint::WhereIsNull { col, table, .. } => vec![*col, *table],
            Constraint::ProjectFunction { args, .. } => {
                args.iter().flat_map(|(c, t)| [*c, *t]).collect()
            }
            Constraint::ProjectLiteral { .. } | Constraint::Limit { .. } | Constraint::Distinct => {
                vec![]
            }
            Constraint::WhereColumnCompare {
                left_col,
                left_table,
                right_col,
                right_table,
                ..
            } => vec![*left_col, *left_table, *right_col, *right_table],
            Constraint::WhereOr { conditions } => {
                conditions.iter().flat_map(|c| c.var_ids()).collect()
            }
            Constraint::OrderBy { col, table, .. } => vec![*col, *table],
            Constraint::SubqueryExpr { .. } | Constraint::SubqueryRelation { .. } => vec![],
            Constraint::Or(preferred, fallback) => {
                let mut ids: Vec<VarId> = preferred.iter().flat_map(|c| c.var_ids()).collect();
                ids.extend(fallback.iter().flat_map(|c| c.var_ids()));
                ids
            }
            Constraint::WindowFunction {
                partition_col,
                order_col,
                ..
            } => {
                let mut ids = Vec::new();
                if let Some((c, t)) = partition_col {
                    ids.push(*c);
                    ids.push(*t);
                }
                if let Some((c, t)) = order_col {
                    ids.push(*c);
                    ids.push(*t);
                }
                ids
            }
        }
    }

    /// Apply a mapping function to every VarId, returning a new Constraint.
    /// Recurses into nested constraint vecs (subqueries, CTEs, Or, WhereOr).
    pub fn map_var_ids(&self, f: &impl Fn(VarId) -> VarId) -> Constraint {
        match self {
            Constraint::BaseTable(v) => Constraint::BaseTable(f(*v)),
            Constraint::AliasOf { alias, original } => Constraint::AliasOf {
                alias: f(*alias),
                original: f(*original),
            },
            Constraint::ColumnExists { col, table } => Constraint::ColumnExists {
                col: f(*col),
                table: f(*table),
            },
            Constraint::ColumnTypeClass { col, type_class } => Constraint::ColumnTypeClass {
                col: f(*col),
                type_class: type_class.clone(),
            },
            Constraint::TypeCompatible(a, b) => Constraint::TypeCompatible(f(*a), f(*b)),
            Constraint::Eq(a, b) => Constraint::Eq(f(*a), f(*b)),
            Constraint::NotEq(a, b) => Constraint::NotEq(f(*a), f(*b)),
            Constraint::From(v) => Constraint::From(f(*v)),
            Constraint::Join {
                operator,
                right: JoinRight::Table(t),
                left_col,
                right_col,
            } => Constraint::Join {
                operator: *operator,
                right: JoinRight::Table(f(*t)),
                left_col: f(*left_col),
                right_col: f(*right_col),
            },
            Constraint::ProjectColumn { col, table } => Constraint::ProjectColumn {
                col: f(*col),
                table: f(*table),
            },
            Constraint::ProjectAggregate {
                function,
                col,
                table,
            } => Constraint::ProjectAggregate {
                function: function.clone(),
                col: f(*col),
                table: f(*table),
            },
            Constraint::ProjectFunction { function, args } => Constraint::ProjectFunction {
                function: function.clone(),
                args: args.iter().map(|(c, t)| (f(*c), f(*t))).collect(),
            },
            Constraint::ProjectLiteral { literal } => Constraint::ProjectLiteral {
                literal: literal.clone(),
            },
            Constraint::GroupBy { col, table } => Constraint::GroupBy {
                col: f(*col),
                table: f(*table),
            },
            Constraint::Having {
                function,
                col,
                table,
                op,
            } => Constraint::Having {
                function: function.clone(),
                col: f(*col),
                table: f(*table),
                op: *op,
            },
            Constraint::HavingKeyFilter { col, table, op } => Constraint::HavingKeyFilter {
                col: f(*col),
                table: f(*table),
                op: *op,
            },
            Constraint::WhereParam { col, table, op } => Constraint::WhereParam {
                col: f(*col),
                table: f(*table),
                op: *op,
            },
            Constraint::WhereInParam {
                col,
                table,
                num_values,
            } => Constraint::WhereInParam {
                col: f(*col),
                table: f(*table),
                num_values: *num_values,
            },
            Constraint::WhereRangeParam { col, table } => Constraint::WhereRangeParam {
                col: f(*col),
                table: f(*table),
            },
            Constraint::WhereLike {
                col,
                table,
                negated,
            } => Constraint::WhereLike {
                col: f(*col),
                table: f(*table),
                negated: *negated,
            },
            Constraint::WhereIsNull {
                col,
                table,
                negated,
            } => Constraint::WhereIsNull {
                col: f(*col),
                table: f(*table),
                negated: *negated,
            },
            Constraint::WhereBetweenParam { col, table } => Constraint::WhereBetweenParam {
                col: f(*col),
                table: f(*table),
            },
            Constraint::WhereColumnCompare {
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => Constraint::WhereColumnCompare {
                left_col: f(*left_col),
                left_table: f(*left_table),
                op: *op,
                right_col: f(*right_col),
                right_table: f(*right_table),
            },
            Constraint::WhereOr { conditions } => Constraint::WhereOr {
                conditions: conditions.iter().map(|c| c.map_var_ids(f)).collect(),
            },
            Constraint::OrderBy {
                col,
                table,
                direction,
                null_order,
            } => Constraint::OrderBy {
                col: f(*col),
                table: f(*table),
                direction: *direction,
                null_order: *null_order,
            },
            Constraint::Limit { limit, offset } => Constraint::Limit {
                limit: *limit,
                offset: *offset,
            },
            Constraint::Distinct => Constraint::Distinct,
            Constraint::SubqueryExpr {
                kind,
                constraints,
                shared_vars,
            } => Constraint::SubqueryExpr {
                kind: kind.clone(),
                constraints: constraints.iter().map(|c| c.map_var_ids(f)).collect(),
                shared_vars: shared_vars.iter().copied().map(&f).collect(),
            },
            Constraint::SubqueryRelation {
                kind,
                alias,
                constraints,
                shared_vars,
            } => Constraint::SubqueryRelation {
                kind: kind.clone(),
                alias: f(*alias),
                constraints: constraints.iter().map(|c| c.map_var_ids(f)).collect(),
                shared_vars: shared_vars.iter().copied().map(&f).collect(),
            },
            Constraint::Or(preferred, fallback) => Constraint::Or(
                preferred.iter().map(|c| c.map_var_ids(f)).collect(),
                fallback.iter().map(|c| c.map_var_ids(f)).collect(),
            ),
            Constraint::WindowFunction {
                function,
                partition_col,
                order_col,
                order_type,
            } => Constraint::WindowFunction {
                function: function.clone(),
                partition_col: partition_col.map(|(c, t)| (f(c), f(t))),
                order_col: order_col.map(|(c, t)| (f(c), f(t))),
                order_type: *order_type,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_schema_variants() {
        let t = VarId(0);
        let c = VarId(1);
        let c2 = VarId(2);

        let _ = Constraint::BaseTable(t);
        let _ = Constraint::ColumnExists { col: c, table: t };
        let _ = Constraint::ColumnTypeClass {
            col: c,
            type_class: TypeClass::Integer,
        };
        let _ = Constraint::TypeCompatible(c, c2);
        let _ = Constraint::Eq(t, VarId(3));
        let _ = Constraint::NotEq(t, VarId(3));
    }

    #[test]
    fn constraint_structural_variants() {
        let t = VarId(0);
        let c = VarId(1);

        let _ = Constraint::From(t);
        let _ = Constraint::ProjectColumn { col: c, table: t };
        let _ = Constraint::ProjectAggregate {
            function: AggregateFn::Count { distinct: false },
            col: c,
            table: t,
        };
        let _ = Constraint::ProjectFunction {
            function: ScalarFn::Coalesce,
            args: vec![(c, t)],
        };
        let _ = Constraint::ProjectLiteral {
            literal: LiteralKind::Integer,
        };
        let _ = Constraint::GroupBy { col: c, table: t };
        let _ = Constraint::Having {
            function: AggregateFn::Count { distinct: false },
            col: c,
            table: t,
            op: BinaryOperator::Greater,
        };
        let _ = Constraint::HavingKeyFilter {
            col: c,
            table: t,
            op: BinaryOperator::Equal,
        };
    }

    #[test]
    fn constraint_where_variants() {
        let t = VarId(0);
        let c = VarId(1);
        let c2 = VarId(2);

        let _ = Constraint::WhereParam {
            col: c,
            table: t,
            op: BinaryOperator::Equal,
        };
        let _ = Constraint::WhereInParam {
            col: c,
            table: t,
            num_values: 3,
        };
        let _ = Constraint::WhereRangeParam { col: c, table: t };
        let _ = Constraint::WhereLike {
            col: c,
            table: t,
            negated: false,
        };
        let _ = Constraint::WhereIsNull {
            col: c,
            table: t,
            negated: true,
        };
        let _ = Constraint::WhereBetweenParam { col: c, table: t };
        let _ = Constraint::WhereColumnCompare {
            left_col: c,
            left_table: t,
            op: BinaryOperator::Equal,
            right_col: c2,
            right_table: t,
        };
        let _ = Constraint::WhereOr {
            conditions: vec![Constraint::WhereParam {
                col: c,
                table: t,
                op: BinaryOperator::Equal,
            }],
        };
    }

    #[test]
    fn constraint_order_limit_distinct() {
        let t = VarId(0);
        let c = VarId(1);

        let _ = Constraint::OrderBy {
            col: c,
            table: t,
            direction: OrderType::OrderAscending,
            null_order: None,
        };
        let _ = Constraint::OrderBy {
            col: c,
            table: t,
            direction: OrderType::OrderDescending,
            null_order: Some(NullOrder::NullsFirst),
        };
        let _ = Constraint::Limit {
            limit: 10,
            offset: None,
        };
        let _ = Constraint::Limit {
            limit: 10,
            offset: Some(5),
        };
        let _ = Constraint::Distinct;
    }

    #[test]
    fn constraint_join_with_table() {
        let t1 = VarId(0);
        let t2 = VarId(1);
        let c1 = VarId(2);
        let c2 = VarId(3);

        let join = Constraint::Join {
            operator: JoinOperator::InnerJoin,
            right: JoinRight::Table(t2),
            left_col: c1,
            right_col: c2,
        };

        // Verify it matches expected structure
        assert!(matches!(
            join,
            Constraint::Join {
                operator: JoinOperator::InnerJoin,
                right: JoinRight::Table(VarId(1)),
                ..
            }
        ));
        let _ = t1; // used as context
    }

    #[test]
    fn constraint_subquery_and_cte() {
        let outer_col = VarId(0);
        let inner_col = VarId(1);
        let inner_table = VarId(2);

        let _ = Constraint::SubqueryExpr {
            kind: SubqueryExprKind::ExistsCorrelated,
            constraints: vec![
                Constraint::From(inner_table),
                Constraint::ProjectColumn {
                    col: inner_col,
                    table: inner_table,
                },
            ],
            shared_vars: vec![outer_col],
        };

        let cte_alias = VarId(3);
        let _ = Constraint::SubqueryRelation {
            kind: SubqueryRelationKind::Cte,
            alias: cte_alias,
            constraints: vec![Constraint::From(inner_table)],
            shared_vars: vec![],
        };
    }

    #[test]
    fn constraint_window_function() {
        let t = VarId(0);
        let c = VarId(1);

        let _ = Constraint::WindowFunction {
            function: WindowFn::RowNumber,
            partition_col: Some((c, t)),
            order_col: Some((c, t)),
            order_type: Some(OrderType::OrderAscending),
        };

        let _ = Constraint::WindowFunction {
            function: WindowFn::Rank,
            partition_col: None,
            order_col: None,
            order_type: None,
        };
    }

    #[test]
    fn constraint_debug_output_is_readable() {
        let c = Constraint::WhereParam {
            col: VarId(1),
            table: VarId(0),
            op: BinaryOperator::Equal,
        };
        let debug = format!("{c:?}");
        assert!(debug.contains("WhereParam"));
        assert!(debug.contains("VarId(1)"));
        assert!(debug.contains("Equal"));
    }

    #[test]
    fn type_class_equality() {
        assert_eq!(TypeClass::Integer, TypeClass::Integer);
        assert_ne!(TypeClass::Integer, TypeClass::String);
        assert_ne!(TypeClass::Any, TypeClass::Integer);
        assert_eq!(
            TypeClass::Exact(SqlType::Int(None)),
            TypeClass::Exact(SqlType::Int(None))
        );
    }
}
