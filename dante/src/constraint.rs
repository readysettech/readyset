//! Constraint vocabulary and supporting descriptor types for the
//! constraint-based query generator.
//!
//! A constraint is a single declarative assertion about the query being
//! generated. Constraints are the primitive building blocks that patterns
//! are composed from.

use readyset_sql::ast::{
    BinaryOperator, CompoundSelectOperator, JoinOperator, NullOrder, OrderType, SqlType,
};

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
    /// Non-integer numeric types (FLOAT, DOUBLE, DECIMAL, NUMERIC).
    /// Use this instead of `Numeric` when integer operands are invalid
    /// for the operation (e.g., ROUND(col, n) in PostgreSQL only accepts
    /// `numeric`, not `integer`).
    Decimal,
    /// Exact fixed-point numeric types ONLY (DECIMAL, NUMERIC) — excludes the
    /// approximate types FLOAT/DOUBLE/REAL. Use when the operation is defined
    /// only for `numeric`: PostgreSQL's two-arg `round(x, scale)` rejects
    /// `round(double precision, integer)` / `round(real, integer)` (42883).
    FixedPoint,
    /// String types (VARCHAR, TEXT, CHAR, etc.)
    String,
    /// Date/time types (DATE, DATETIME, TIMESTAMP, TIME)
    DateTime,
    /// Exact SQL type
    Exact(SqlType),
}

impl TypeClass {
    /// Returns true if this type class accepts the given SQL type.
    pub fn matches(&self, sql_type: &SqlType) -> bool {
        match self {
            TypeClass::Any => true,
            TypeClass::Integer => matches!(
                sql_type,
                SqlType::Int(_)
                    | SqlType::BigInt(_)
                    | SqlType::SmallInt(_)
                    | SqlType::TinyInt(_)
                    | SqlType::MediumInt(_)
            ),
            TypeClass::Numeric => matches!(
                sql_type,
                SqlType::Int(_)
                    | SqlType::BigInt(_)
                    | SqlType::SmallInt(_)
                    | SqlType::TinyInt(_)
                    | SqlType::MediumInt(_)
                    | SqlType::Float
                    | SqlType::Double
                    | SqlType::Real
                    | SqlType::Decimal(_, _)
                    | SqlType::Numeric(_)
            ),
            TypeClass::Decimal => matches!(
                sql_type,
                SqlType::Float
                    | SqlType::Double
                    | SqlType::Real
                    | SqlType::Decimal(_, _)
                    | SqlType::Numeric(_)
            ),
            TypeClass::FixedPoint => {
                matches!(sql_type, SqlType::Decimal(_, _) | SqlType::Numeric(_))
            }
            TypeClass::String => matches!(
                sql_type,
                SqlType::VarChar(_) | SqlType::Char(_) | SqlType::Text | SqlType::TinyText
            ),
            TypeClass::DateTime => matches!(
                sql_type,
                SqlType::DateTime(_) | SqlType::Timestamp | SqlType::Date | SqlType::Time
            ),
            TypeClass::Exact(exact) => sql_type == exact,
        }
    }

    /// Whether a `Constraint::Example` literal is plausibly valid for a column
    /// pinned to this type class. Numeric classes require the literal to parse
    /// as the corresponding number so a mis-authored cell (e.g. `"abc"` on an
    /// integer column) is rejected at registration rather than panicking the
    /// oracle's `parse_literal` mid-run. Non-numeric classes accept any text,
    /// matching how the oracle materializes them.
    pub fn accepts_literal(&self, literal: &str) -> bool {
        let s = literal.trim();
        match self {
            TypeClass::Integer => s.parse::<i64>().is_ok(),
            TypeClass::Numeric | TypeClass::Decimal | TypeClass::FixedPoint => {
                s.parse::<f64>().is_ok()
            }
            TypeClass::Exact(ty) if TypeClass::Integer.matches(ty) => s.parse::<i64>().is_ok(),
            TypeClass::Exact(ty) if TypeClass::Numeric.matches(ty) => s.parse::<f64>().is_ok(),
            // String, DateTime, Any, Orderable, and non-numeric Exact types
            // are not statically checkable here; the oracle parses them as text.
            _ => true,
        }
    }
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
/// constraints can reference by alias VarId -- the resolver `env.bind`s the
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
    /// Derived table in FROM position (`FROM (SELECT ...) AS sq`).
    FromSubquery,
}

/// What the right side of a JOIN is. Always a relation var -- base table,
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

/// One cell of a [`Constraint::Example`]: pins a `VarId` to a literal SQL
/// value for one example run.
#[derive(Debug, Clone, PartialEq)]
pub struct ExampleCell {
    /// A `VarId` whose `VarKind` is `Column { .. }` (-> row override) or
    /// `Param { .. }` (-> param override). Any other kind is a
    /// registration-time error.
    pub var: VarId,
    pub value: ExampleValue,
}

/// Value carried by an [`ExampleCell`].
#[derive(Debug, Clone, PartialEq)]
pub enum ExampleValue {
    /// SQL literal as it would appear in INSERT or as a bound parameter.
    /// Dialect-shaped by the author (e.g., `'2025-01-01'` for MySQL vs
    /// `DATE '2025-01-01'` for PG).
    Literal(&'static str),
}

/// A single declarative assertion about the query being generated.
///
/// Constraints are the primitive building blocks that patterns are composed
/// from. The resolver processes a set of constraints to produce variable
/// bindings and ultimately an AST.
#[derive(Debug, Clone, PartialEq)]
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
    /// A binary-operator expression over two columns appears in the SELECT list.
    /// Renders as `<left_table>.<left_col> <op> <right_table>.<right_col>`.
    /// Used by `registry::expressions` patterns to exercise the dataflow
    /// expression layer's coercion / arithmetic / comparison paths.
    ProjectBinaryOp {
        left_col: VarId,
        left_table: VarId,
        op: BinaryOperator,
        right_col: VarId,
        right_table: VarId,
    },
    /// `CAST(<table>.<col> AS <target_ty>)` in the SELECT list. Used by
    /// `registry::expressions` cast patterns to exercise runtime coercion
    /// (e.g. `Decimal(p1, s1) -> Decimal(p2, s2)`, `Int -> Numeric`).
    ProjectCast {
        col: VarId,
        table: VarId,
        target_ty: SqlType,
    },
    /// `WHERE <lookup_col> <cmp> (<left_col> <op> <right_col>)` --
    /// comparison whose RHS is a binary-op expression. Surfaces the
    /// lookup-key coercion bug class: RS evaluates the RHS expression
    /// through the buggy `arithmetic_operation!` arm and then coerces the
    /// result for comparison against `lookup_col`. The original motivating
    /// case is `WHERE x = r / 3` where `x: DECIMAL` and `r: INT`; RS's
    /// integer-divide produces `Int(2)`, coerces to `Numeric(2.0000)` at
    /// lookup time, and matches the wrong row.
    WhereLookupBinaryOp {
        lookup_col: VarId,
        lookup_table: VarId,
        cmp: BinaryOperator,
        left_col: VarId,
        left_table: VarId,
        op: BinaryOperator,
        right_col: VarId,
        right_table: VarId,
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
        /// `VarKind::Param { col }` for the HAVING RHS placeholder.
        param: VarId,
    },
    /// A parametrizable filter in HAVING on a GROUP BY key column.
    /// Unlike `Having` (which wraps an aggregate function), this emits
    /// `HAVING col op ?` where `col` must also be a GROUP BY key.
    /// The hoisting pass can extract these to the outer WHERE.
    HavingKeyFilter {
        col: VarId,
        table: VarId,
        op: BinaryOperator,
        /// `VarKind::Param { col }` for the HAVING-on-GROUP-BY-key RHS.
        param: VarId,
    },
    /// A WHERE filter with a parameter placeholder.
    WhereParam {
        col: VarId,
        table: VarId,
        op: BinaryOperator,
        /// `VarKind::Param { col }` var that names this placeholder so
        /// `Constraint::Example` cells can override it.
        param: VarId,
    },
    /// An IN-list parameter filter.
    WhereInParam {
        col: VarId,
        table: VarId,
        num_values: u8,
        /// One `VarKind::Param { col }` per placeholder slot. `params.len()`
        /// must equal `num_values`.
        params: Vec<VarId>,
    },
    /// A range parameter (col >= ? AND col <= ?).
    WhereRangeParam {
        col: VarId,
        table: VarId,
        /// `VarKind::Param { col }` for `col >= ?`.
        lo: VarId,
        /// `VarKind::Param { col }` for `col <= ?`.
        hi: VarId,
    },
    /// A LIKE/NOT LIKE filter.
    WhereLike {
        col: VarId,
        table: VarId,
        negated: bool,
        /// `VarKind::Param { col }` for the LIKE pattern placeholder.
        param: VarId,
    },
    /// An IS NULL / IS NOT NULL filter.
    WhereIsNull {
        col: VarId,
        table: VarId,
        negated: bool,
    },
    /// A BETWEEN filter with parameter placeholders.
    WhereBetweenParam {
        col: VarId,
        table: VarId,
        /// `VarKind::Param { col }` for the BETWEEN lower bound.
        lo: VarId,
        /// `VarKind::Param { col }` for the BETWEEN upper bound.
        hi: VarId,
    },
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
    /// A relation-position subquery -- the subquery produces an outer
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

    /// A compound SELECT (UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT).
    /// Each branch is a self-contained set of constraints that resolves to
    /// an independent SelectStatement; the branches are combined with the
    /// given operator. ORDER BY and LIMIT constraints alongside this one
    /// in the outer list apply to the compound result.
    CompoundSelect {
        operator: CompoundSelectOperator,
        branches: Vec<Vec<Constraint>>,
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

    /// Concrete (row + parameter) probe authored alongside a pattern's shape.
    /// Examples don't influence the generated SQL -- they ride through the
    /// resolver and are materialized into `ResolvedExample` overrides for the
    /// oracle to insert + execute.
    Example {
        /// Human-readable note describing the bug shape this example surfaces.
        note: &'static str,
        /// Dialects this example applies to. Examples whose `dialect` excludes
        /// the running dialect are dropped at `QueryOutput::from_resolver`.
        dialect: DialectSupport,
        /// Per-var bindings for this example.
        cells: Vec<ExampleCell>,
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
            | Constraint::WhereIsNull { col, table, .. } => vec![*col, *table],
            Constraint::ProjectAggregate { col, table, .. } => vec![*col, *table],
            Constraint::WhereInParam {
                col, table, params, ..
            } => {
                let mut ids = vec![*col, *table];
                ids.extend(params.iter().copied());
                ids
            }
            Constraint::WhereRangeParam { col, table, lo, hi }
            | Constraint::WhereBetweenParam { col, table, lo, hi } => {
                vec![*col, *table, *lo, *hi]
            }
            Constraint::WhereLike {
                col, table, param, ..
            } => vec![*col, *table, *param],
            Constraint::Having {
                col, table, param, ..
            }
            | Constraint::HavingKeyFilter {
                col, table, param, ..
            } => {
                vec![*col, *table, *param]
            }
            Constraint::WhereParam {
                col, table, param, ..
            } => vec![*col, *table, *param],
            Constraint::ProjectFunction { args, .. } => {
                args.iter().flat_map(|(c, t)| [*c, *t]).collect()
            }
            Constraint::ProjectBinaryOp {
                left_col,
                left_table,
                right_col,
                right_table,
                ..
            } => vec![*left_col, *left_table, *right_col, *right_table],
            Constraint::ProjectCast { col, table, .. } => vec![*col, *table],
            Constraint::WhereLookupBinaryOp {
                lookup_col,
                lookup_table,
                left_col,
                left_table,
                right_col,
                right_table,
                ..
            } => vec![
                *lookup_col,
                *lookup_table,
                *left_col,
                *left_table,
                *right_col,
                *right_table,
            ],
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
            Constraint::SubqueryExpr { .. } => vec![],
            Constraint::SubqueryRelation { alias, .. } => vec![*alias],
            Constraint::CompoundSelect { .. } => vec![],
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
            Constraint::Example { cells, .. } => cells.iter().map(|c| c.var).collect(),
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
            Constraint::ProjectBinaryOp {
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => Constraint::ProjectBinaryOp {
                left_col: f(*left_col),
                left_table: f(*left_table),
                op: *op,
                right_col: f(*right_col),
                right_table: f(*right_table),
            },
            Constraint::ProjectCast {
                col,
                table,
                target_ty,
            } => Constraint::ProjectCast {
                col: f(*col),
                table: f(*table),
                target_ty: target_ty.clone(),
            },
            Constraint::WhereLookupBinaryOp {
                lookup_col,
                lookup_table,
                cmp,
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => Constraint::WhereLookupBinaryOp {
                lookup_col: f(*lookup_col),
                lookup_table: f(*lookup_table),
                cmp: *cmp,
                left_col: f(*left_col),
                left_table: f(*left_table),
                op: *op,
                right_col: f(*right_col),
                right_table: f(*right_table),
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
                param,
            } => Constraint::Having {
                function: function.clone(),
                col: f(*col),
                table: f(*table),
                op: *op,
                param: f(*param),
            },
            Constraint::HavingKeyFilter {
                col,
                table,
                op,
                param,
            } => Constraint::HavingKeyFilter {
                col: f(*col),
                table: f(*table),
                op: *op,
                param: f(*param),
            },
            Constraint::WhereParam {
                col,
                table,
                op,
                param,
            } => Constraint::WhereParam {
                col: f(*col),
                table: f(*table),
                op: *op,
                param: f(*param),
            },
            Constraint::WhereInParam {
                col,
                table,
                num_values,
                params,
            } => Constraint::WhereInParam {
                col: f(*col),
                table: f(*table),
                num_values: *num_values,
                params: params.iter().copied().map(&f).collect(),
            },
            Constraint::WhereRangeParam { col, table, lo, hi } => Constraint::WhereRangeParam {
                col: f(*col),
                table: f(*table),
                lo: f(*lo),
                hi: f(*hi),
            },
            Constraint::WhereLike {
                col,
                table,
                negated,
                param,
            } => Constraint::WhereLike {
                col: f(*col),
                table: f(*table),
                negated: *negated,
                param: f(*param),
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
            Constraint::WhereBetweenParam { col, table, lo, hi } => Constraint::WhereBetweenParam {
                col: f(*col),
                table: f(*table),
                lo: f(*lo),
                hi: f(*hi),
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
            Constraint::CompoundSelect { operator, branches } => Constraint::CompoundSelect {
                operator: operator.clone(),
                branches: branches
                    .iter()
                    .map(|branch| branch.iter().map(|c| c.map_var_ids(f)).collect())
                    .collect(),
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
            Constraint::Example {
                note,
                dialect,
                cells,
            } => Constraint::Example {
                note,
                dialect: *dialect,
                cells: cells
                    .iter()
                    .map(|c| ExampleCell {
                        var: f(c.var),
                        value: c.value.clone(),
                    })
                    .collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var::VarId;
    use readyset_sql::ast::BinaryOperator;

    #[test]
    fn where_param_map_var_ids_shifts_param() {
        let c = Constraint::WhereParam {
            col: VarId(1),
            table: VarId(0),
            op: BinaryOperator::Equal,
            param: VarId(2),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 10));
        match shifted {
            Constraint::WhereParam {
                col,
                table,
                op: _,
                param,
            } => {
                assert_eq!(col, VarId(11));
                assert_eq!(table, VarId(10));
                assert_eq!(param, VarId(12));
            }
            other => panic!("expected WhereParam, got {other:?}"),
        }
    }

    #[test]
    fn where_param_var_ids_includes_param() {
        let c = Constraint::WhereParam {
            col: VarId(1),
            table: VarId(0),
            op: BinaryOperator::Equal,
            param: VarId(2),
        };
        let ids = c.var_ids();
        assert!(
            ids.contains(&VarId(2)),
            "var_ids should include param: {ids:?}"
        );
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
            param: VarId(2),
        };
        let _ = Constraint::HavingKeyFilter {
            col: c,
            table: t,
            op: BinaryOperator::Equal,
            param: VarId(2),
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
            param: VarId(3),
        };
        let _ = Constraint::WhereInParam {
            col: c,
            table: t,
            num_values: 3,
            params: vec![VarId(3), VarId(4), VarId(5)],
        };
        let _ = Constraint::WhereRangeParam {
            col: c,
            table: t,
            lo: VarId(3),
            hi: VarId(4),
        };
        let _ = Constraint::WhereLike {
            col: c,
            table: t,
            negated: false,
            param: VarId(3),
        };
        let _ = Constraint::WhereIsNull {
            col: c,
            table: t,
            negated: true,
        };
        let _ = Constraint::WhereBetweenParam {
            col: c,
            table: t,
            lo: VarId(3),
            hi: VarId(4),
        };
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
                param: VarId(3),
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
            param: VarId(2),
        };
        let debug = format!("{c:?}");
        assert!(debug.contains("WhereParam"));
        assert!(debug.contains("VarId(1)"));
        assert!(debug.contains("Equal"));
    }

    #[test]
    fn where_in_param_map_var_ids_shifts_params() {
        let c = Constraint::WhereInParam {
            col: VarId(1),
            table: VarId(0),
            num_values: 2,
            params: vec![VarId(2), VarId(3)],
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 5));
        match shifted {
            Constraint::WhereInParam { params, .. } => {
                assert_eq!(params, vec![VarId(7), VarId(8)]);
            }
            other => panic!("expected WhereInParam, got {other:?}"),
        }
    }

    #[test]
    fn where_range_param_map_var_ids_shifts_lo_hi() {
        let c = Constraint::WhereRangeParam {
            col: VarId(1),
            table: VarId(0),
            lo: VarId(2),
            hi: VarId(3),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 4));
        match shifted {
            Constraint::WhereRangeParam { lo, hi, .. } => {
                assert_eq!(lo, VarId(6));
                assert_eq!(hi, VarId(7));
            }
            other => panic!("expected WhereRangeParam, got {other:?}"),
        }
    }

    #[test]
    fn where_between_param_map_var_ids_shifts_lo_hi() {
        let c = Constraint::WhereBetweenParam {
            col: VarId(1),
            table: VarId(0),
            lo: VarId(2),
            hi: VarId(3),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 4));
        match shifted {
            Constraint::WhereBetweenParam { lo, hi, .. } => {
                assert_eq!(lo, VarId(6));
                assert_eq!(hi, VarId(7));
            }
            other => panic!("expected WhereBetweenParam, got {other:?}"),
        }
    }

    #[test]
    fn where_like_map_var_ids_shifts_param() {
        let c = Constraint::WhereLike {
            col: VarId(1),
            table: VarId(0),
            negated: false,
            param: VarId(2),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 3));
        match shifted {
            Constraint::WhereLike { param, .. } => assert_eq!(param, VarId(5)),
            other => panic!("expected WhereLike, got {other:?}"),
        }
    }

    #[test]
    fn having_map_var_ids_shifts_param() {
        let c = Constraint::Having {
            function: AggregateFn::Count { distinct: false },
            col: VarId(1),
            table: VarId(0),
            op: BinaryOperator::Equal,
            param: VarId(2),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 3));
        match shifted {
            Constraint::Having { param, .. } => assert_eq!(param, VarId(5)),
            other => panic!("expected Having, got {other:?}"),
        }
    }

    #[test]
    fn having_key_filter_map_var_ids_shifts_param() {
        let c = Constraint::HavingKeyFilter {
            col: VarId(1),
            table: VarId(0),
            op: BinaryOperator::Equal,
            param: VarId(2),
        };
        let shifted = c.map_var_ids(&|v| VarId(v.0 + 3));
        match shifted {
            Constraint::HavingKeyFilter { param, .. } => assert_eq!(param, VarId(5)),
            other => panic!("expected HavingKeyFilter, got {other:?}"),
        }
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

    #[test]
    fn accepts_literal_enforces_numeric_classes_only() {
        use readyset_sql::ast::SqlType;
        assert!(TypeClass::Integer.accepts_literal("8"));
        assert!(!TypeClass::Integer.accepts_literal("8.5"));
        assert!(!TypeClass::Integer.accepts_literal("abc"));
        assert!(TypeClass::Numeric.accepts_literal("2.6667"));
        assert!(!TypeClass::Numeric.accepts_literal("abc"));
        assert!(TypeClass::Exact(SqlType::Decimal(10, 4)).accepts_literal("2.0000"));
        assert!(!TypeClass::Exact(SqlType::Int(None)).accepts_literal("2.0000"));
        // Non-numeric and unconstrained classes accept any text.
        assert!(TypeClass::String.accepts_literal("anything"));
        assert!(TypeClass::DateTime.accepts_literal("2025-01-01"));
        assert!(TypeClass::Any.accepts_literal("whatever"));
    }

    #[test]
    fn example_map_var_ids_shifts_cells() {
        let ex = Constraint::Example {
            note: "test",
            dialect: DialectSupport::Both,
            cells: vec![
                ExampleCell {
                    var: VarId(1),
                    value: ExampleValue::Literal("8"),
                },
                ExampleCell {
                    var: VarId(2),
                    value: ExampleValue::Literal("3"),
                },
            ],
        };
        let shifted = ex.map_var_ids(&|v| VarId(v.0 + 10));
        match shifted {
            Constraint::Example { cells, .. } => {
                assert_eq!(cells[0].var, VarId(11));
                assert_eq!(cells[1].var, VarId(12));
            }
            other => panic!("expected Example, got {other:?}"),
        }
    }

    #[test]
    fn example_var_ids_returns_cell_vars() {
        let ex = Constraint::Example {
            note: "test",
            dialect: DialectSupport::MySqlOnly,
            cells: vec![
                ExampleCell {
                    var: VarId(5),
                    value: ExampleValue::Literal("x"),
                },
                ExampleCell {
                    var: VarId(7),
                    value: ExampleValue::Literal("y"),
                },
            ],
        };
        let ids = ex.var_ids();
        assert_eq!(ids, vec![VarId(5), VarId(7)]);
    }
}
