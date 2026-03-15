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

/// Where a subquery appears in the outer query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubqueryPosition {
    /// Subquery as a join target (e.g., `JOIN (SELECT ...) sub ON ...`)
    JoinSubquery(JoinOperator),
    /// Uncorrelated `EXISTS (SELECT ...)`
    ExistsUncorrelated,
    /// Correlated `EXISTS (SELECT ... WHERE inner.col = outer.col)`
    ExistsCorrelated,
    /// `col IN (SELECT ...)`
    InSubquery,
    /// Scalar subquery in SELECT or WHERE
    ScalarSubquery,
}

/// What the right side of a JOIN is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinRight {
    /// Join to a table variable directly
    Table(VarId),
    /// Join to a subquery
    Subquery {
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },
    /// Join to a CTE by name. `alias` is the variable bound to the CTE's
    /// SQL alias (e.g., `cte0`), so outer references resolve to that name
    /// rather than the underlying base table.
    Cte {
        alias: VarId,
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },
}

/// Dialect support for a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectSupport {
    Both,
    MySqlOnly,
    PostgresOnly,
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
    /// A subquery that appears as a join target or WHERE element.
    Subquery {
        position: SubqueryPosition,
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },
    /// A Common Table Expression (WITH clause). `alias` is the variable
    /// bound to the CTE's SQL alias name, so outer references like
    /// `From(alias)` resolve to the CTE name rather than a base table.
    Cte {
        alias: VarId,
        constraints: Vec<Constraint>,
        shared_vars: Vec<VarId>,
    },

    // --- Window functions ---
    /// A window function in the SELECT list.
    WindowFunction {
        function: WindowFn,
        partition_col: Option<(VarId, VarId)>,
        order_col: Option<(VarId, VarId)>,
        order_type: Option<OrderType>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_class_variants_constructible() {
        let _ = TypeClass::Any;
        let _ = TypeClass::Integer;
        let _ = TypeClass::Numeric;
        let _ = TypeClass::String;
        let _ = TypeClass::DateTime;
        let _ = TypeClass::Exact(SqlType::Int(None));
    }

    #[test]
    fn aggregate_fn_variants_constructible() {
        let _ = AggregateFn::Count { distinct: false };
        let _ = AggregateFn::Count { distinct: true };
        let _ = AggregateFn::Sum { distinct: false };
        let _ = AggregateFn::Avg { distinct: true };
        let _ = AggregateFn::Min;
        let _ = AggregateFn::Max;
        let _ = AggregateFn::GroupConcat;
        let _ = AggregateFn::JsonObjectAgg;
        let _ = AggregateFn::ArrayAgg;
    }

    #[test]
    fn scalar_fn_variants_constructible() {
        let _ = ScalarFn::Coalesce;
        let _ = ScalarFn::IfNull;
        let _ = ScalarFn::NullIf;
        let _ = ScalarFn::Cast;
        let _ = ScalarFn::Upper;
        let _ = ScalarFn::Lower;
        let _ = ScalarFn::Trim;
        let _ = ScalarFn::Substring;
        let _ = ScalarFn::Concat;
    }

    #[test]
    fn window_fn_variants_constructible() {
        let _ = WindowFn::RowNumber;
        let _ = WindowFn::Rank;
        let _ = WindowFn::DenseRank;
    }

    #[test]
    fn literal_kind_variants_constructible() {
        let _ = LiteralKind::Integer;
        let _ = LiteralKind::Float;
        let _ = LiteralKind::String;
        let _ = LiteralKind::Null;
        let _ = LiteralKind::Boolean;
    }

    #[test]
    fn subquery_position_variants_constructible() {
        let _ = SubqueryPosition::JoinSubquery(JoinOperator::InnerJoin);
        let _ = SubqueryPosition::ExistsUncorrelated;
        let _ = SubqueryPosition::ExistsCorrelated;
        let _ = SubqueryPosition::InSubquery;
        let _ = SubqueryPosition::ScalarSubquery;
    }

    #[test]
    fn dialect_support_variants_constructible() {
        let _ = DialectSupport::Both;
        let _ = DialectSupport::MySqlOnly;
        let _ = DialectSupport::PostgresOnly;
    }

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

        let _ = Constraint::Subquery {
            position: SubqueryPosition::ExistsCorrelated,
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
        let _ = Constraint::Cte {
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
