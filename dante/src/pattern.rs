//! Pattern, PatternBuilder, SubqueryBuilder, and Recipe types.
//!
//! A Pattern is a named, composable set of constraints with a shared variable
//! scope. Patterns are the unit of registration in the constraint registry.

use readyset_sql::ast::{BinaryOperator, JoinOperator, NullOrder, OrderType};

use crate::constraint::{
    AggregateFn, Constraint, DialectSupport, JoinRight, LiteralKind, ScalarFn, SubqueryPosition,
    TypeClass, WindowFn,
};
use crate::var::{VarAllocator, VarId, VarKind};

/// A named, composable set of constraints with a shared variable scope.
#[derive(Debug, Clone)]
pub struct Pattern {
    /// Human-readable name (e.g., "aggregate_with_group_by")
    pub name: &'static str,
    /// The constraints that make up this pattern
    pub constraints: Vec<Constraint>,
    /// Variable metadata (kinds), indexed by VarId.0
    pub vars: Vec<VarKind>,
    /// Tags for categorization and incompatibility checking
    pub tags: Vec<&'static str>,
    /// Minimum subquery depth required (0 for patterns without subqueries)
    pub min_depth: usize,
    /// Weight for random selection (higher = more likely). Default 1.
    /// Must be > 0 for the pattern to ever be picked.
    pub weight: u32,
    /// Dialect support
    pub dialect_support: DialectSupport,
}

impl Pattern {
    /// Number of variables in this pattern.
    pub fn num_vars(&self) -> usize {
        self.vars.len()
    }

    /// Convert this pattern to a Recipe, optionally renumbering variables
    /// by `var_offset` to avoid conflicts when composing multiple patterns.
    /// The offset is applied transiently to constraints; the resulting
    /// Recipe stores `var_kinds` indexed by the post-offset VarId so that
    /// `var_kinds[v.0]` always works for any var that appears in
    /// `constraints`.
    ///
    /// # Padding invariant
    ///
    /// When `var_offset > 0` the first `var_offset` slots in `var_kinds`
    /// are filled with `VarKind::Relation` as cheap padding. The resolver
    /// is correct only if **no constraint references a var with id
    /// `< var_offset`**: every var the resolver indexes via
    /// `var_kinds[v.0]` must come from this pattern's own constraints
    /// (which have all been shifted up by `var_offset`). Composers that
    /// merge two recipes are responsible for filling the padding slots
    /// with the other pattern's real kinds before resolution; reading a
    /// padding slot for a real var would silently mis-classify it as a
    /// `Relation`.
    pub fn to_recipe(&self, var_offset: usize) -> Recipe {
        let constraints = if var_offset == 0 {
            self.constraints.clone()
        } else {
            self.constraints
                .iter()
                .map(|c| offset_constraint(c, var_offset))
                .collect()
        };

        // Padding kept as Relation purely to satisfy `var_kinds[v.0]`
        // indexing. See the doc comment for the invariant that keeps this
        // sound. We also enforce it dynamically in debug builds.
        let mut var_kinds = vec![VarKind::Relation; var_offset];
        var_kinds.extend(self.vars.iter().cloned());

        debug_assert!(
            constraints
                .iter()
                .flat_map(constraint_var_ids)
                .all(|v| v.0 >= var_offset),
            "to_recipe(var_offset={var_offset}) padding invariant violated: \
             a constraint references a var below the offset",
        );

        Recipe {
            constraints,
            num_vars: self.vars.len(),
            var_kinds,
        }
    }
}

/// The flattened, renumbered form of one or more composed patterns, ready
/// for the resolver. `var_kinds` is indexed by absolute VarId — i.e.
/// `var_kinds[v.0]` is the kind of any var `v` referenced in
/// `constraints`.
#[derive(Debug, Clone)]
pub struct Recipe {
    /// The constraints to resolve
    pub constraints: Vec<Constraint>,
    /// Number of variables in this recipe
    pub num_vars: usize,
    /// Variable kinds, indexed by absolute VarId
    pub var_kinds: Vec<VarKind>,
}

/// Builder for constructing patterns ergonomically.
///
/// Variables are allocated on this builder regardless of scope (flat model).
/// For subqueries, create a [`SubqueryBuilder`] to collect inner constraints,
/// then merge it back via [`join_subquery`](PatternBuilder::join_subquery) or
/// similar methods.
#[derive(Debug)]
pub struct PatternBuilder {
    name: &'static str,
    allocator: VarAllocator,
    constraints: Vec<Constraint>,
    tags: Vec<&'static str>,
    weight: u32,
    dialect_support: DialectSupport,
}

impl PatternBuilder {
    /// Create a new pattern builder with the given name.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            allocator: VarAllocator::new(),
            constraints: Vec::new(),
            tags: Vec::new(),
            weight: 1,
            dialect_support: DialectSupport::Both,
        }
    }

    /// Returns the current number of allocated variables.
    pub fn var_count(&self) -> usize {
        self.allocator.len()
    }

    // --- Variable allocation ---

    /// Allocate a relation variable and emit a `BaseTable` constraint.
    pub fn table(&mut self) -> VarId {
        let v = self.allocator.alloc(VarKind::Relation);
        self.constraints.push(Constraint::BaseTable(v));
        v
    }

    /// Allocate a relation variable that is an alias of an existing relation.
    /// Both will refer to the same physical table but with different SQL aliases.
    /// Emits an `AliasOf` constraint.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `original` is not a `Relation` variable —
    /// aliasing a Column or SqlType is a programming error and the resolver
    /// would later surface it as a confusing `Unbound` error rather than a
    /// kind mismatch.
    pub fn alias_of(&mut self, original: VarId) -> VarId {
        debug_assert!(
            matches!(self.allocator.kind(original), VarKind::Relation),
            "alias_of requires a Relation var, got {:?}",
            self.allocator.kind(original)
        );
        let v = self.allocator.alloc(VarKind::Relation);
        self.constraints
            .push(Constraint::AliasOf { alias: v, original });
        v
    }

    /// Allocate a column variable for the given table and emit a
    /// `ColumnExists` constraint.
    pub fn column(&mut self, table: VarId) -> VarId {
        let v = self.allocator.alloc(VarKind::Column { table });
        self.constraints
            .push(Constraint::ColumnExists { col: v, table });
        v
    }

    /// Allocate a SQL type variable.
    pub fn sql_type(&mut self) -> VarId {
        self.allocator.alloc(VarKind::SqlType)
    }

    // --- Constraint emission ---

    /// Add a raw constraint.
    pub fn constraint(&mut self, c: Constraint) {
        self.constraints.push(c);
    }

    /// Emit an `Eq` constraint (two variables must resolve to the same value).
    pub fn eq(&mut self, a: VarId, b: VarId) {
        self.constraints.push(Constraint::Eq(a, b));
    }

    /// Emit a `NotEq` constraint (two variables must differ).
    pub fn not_eq(&mut self, a: VarId, b: VarId) {
        self.constraints.push(Constraint::NotEq(a, b));
    }

    pub fn type_compatible(&mut self, a: VarId, b: VarId) {
        self.constraints.push(Constraint::TypeCompatible(a, b));
    }

    pub fn column_type_class(&mut self, col: VarId, type_class: TypeClass) {
        self.constraints
            .push(Constraint::ColumnTypeClass { col, type_class });
    }

    pub fn from(&mut self, table: VarId) {
        self.constraints.push(Constraint::From(table));
    }

    pub fn project_column(&mut self, col: VarId, table: VarId) {
        self.constraints
            .push(Constraint::ProjectColumn { col, table });
    }

    pub fn project_aggregate(&mut self, function: AggregateFn, col: VarId, table: VarId) {
        self.constraints.push(Constraint::ProjectAggregate {
            function,
            col,
            table,
        });
    }

    pub fn project_function(&mut self, function: ScalarFn, args: Vec<(VarId, VarId)>) {
        self.constraints
            .push(Constraint::ProjectFunction { function, args });
    }

    pub fn project_literal(&mut self, literal: LiteralKind) {
        self.constraints
            .push(Constraint::ProjectLiteral { literal });
    }

    pub fn group_by(&mut self, col: VarId, table: VarId) {
        self.constraints.push(Constraint::GroupBy { col, table });
    }

    pub fn having(&mut self, function: AggregateFn, col: VarId, table: VarId, op: BinaryOperator) {
        self.constraints.push(Constraint::Having {
            function,
            col,
            table,
            op,
        });
    }

    pub fn where_param(&mut self, col: VarId, table: VarId, op: BinaryOperator) {
        self.constraints
            .push(Constraint::WhereParam { col, table, op });
    }

    pub fn where_in_param(&mut self, col: VarId, table: VarId, num_values: u8) {
        self.constraints.push(Constraint::WhereInParam {
            col,
            table,
            num_values,
        });
    }

    pub fn where_range_param(&mut self, col: VarId, table: VarId) {
        self.constraints
            .push(Constraint::WhereRangeParam { col, table });
    }

    pub fn where_like(&mut self, col: VarId, table: VarId, negated: bool) {
        self.constraints.push(Constraint::WhereLike {
            col,
            table,
            negated,
        });
    }

    pub fn where_is_null(&mut self, col: VarId, table: VarId, negated: bool) {
        self.constraints.push(Constraint::WhereIsNull {
            col,
            table,
            negated,
        });
    }

    pub fn where_between_param(&mut self, col: VarId, table: VarId) {
        self.constraints
            .push(Constraint::WhereBetweenParam { col, table });
    }

    pub fn where_column_compare(
        &mut self,
        left_col: VarId,
        left_table: VarId,
        op: BinaryOperator,
        right_col: VarId,
        right_table: VarId,
    ) {
        self.constraints.push(Constraint::WhereColumnCompare {
            left_col,
            left_table,
            op,
            right_col,
            right_table,
        });
    }

    pub fn order_by(
        &mut self,
        col: VarId,
        table: VarId,
        direction: OrderType,
        null_order: Option<NullOrder>,
    ) {
        self.constraints.push(Constraint::OrderBy {
            col,
            table,
            direction,
            null_order,
        });
    }

    pub fn limit(&mut self, limit: u64, offset: Option<u64>) {
        self.constraints.push(Constraint::Limit { limit, offset });
    }

    pub fn distinct(&mut self) {
        self.constraints.push(Constraint::Distinct);
    }

    pub fn window_function(
        &mut self,
        function: WindowFn,
        partition_col: Option<(VarId, VarId)>,
        order_col: Option<(VarId, VarId)>,
        order_type: Option<OrderType>,
    ) {
        self.constraints.push(Constraint::WindowFunction {
            function,
            partition_col,
            order_col,
            order_type,
        });
    }

    /// Emit a `Join` constraint with a table as the right side.
    pub fn join_table(
        &mut self,
        operator: JoinOperator,
        right_table: VarId,
        left_col: VarId,
        right_col: VarId,
    ) {
        self.constraints.push(Constraint::Join {
            operator,
            right: JoinRight::Table(right_table),
            left_col,
            right_col,
        });
    }

    /// Begin building a subquery scope on top of this pattern.
    ///
    /// The returned [`SubqueryBuilder`] borrows this pattern mutably, so
    /// its `table()`, `column()`, and `alias_of()` methods allocate
    /// variables from the same allocator while emitting BaseTable and
    /// ColumnExists constraints into the subquery's inner scope only —
    /// they do not leak into the outer pattern's constraint list. To
    /// commit the subquery, call one of the consuming methods on the
    /// returned builder (`commit_as_where`, `commit_as_join`, or
    /// `commit_as_cte`).
    pub fn subquery(&mut self) -> SubqueryBuilder<'_> {
        SubqueryBuilder::start(self)
    }

    // --- Metadata ---

    /// Set tags for this pattern.
    pub fn tags(&mut self, tags: &[&'static str]) {
        self.tags.extend_from_slice(tags);
    }

    /// Set the weight for random selection. Must be `> 0`; weight 0 means
    /// the pattern will never be picked.
    ///
    /// # Panics
    ///
    /// Panics if `w` is 0 (in debug builds) — a zero-weight pattern is almost
    /// certainly a programming error and `Entropy::choose_weighted` would
    /// panic later if it were ever the only candidate.
    pub fn set_weight(&mut self, w: u32) {
        debug_assert!(w > 0, "Pattern weight must be > 0");
        self.weight = w;
    }

    /// Set the dialect support.
    pub fn set_dialect_support(&mut self, ds: DialectSupport) {
        self.dialect_support = ds;
    }

    /// Build the pattern, deriving `min_depth` and `num_vars` automatically.
    pub fn build(self) -> Pattern {
        let min_depth = compute_min_depth(&self.constraints);
        Pattern {
            name: self.name,
            constraints: self.constraints,
            vars: self.allocator.into_kinds(),
            tags: self.tags,
            min_depth,
            weight: self.weight,
            dialect_support: self.dialect_support,
        }
    }
}

/// A constraint collector for subquery/CTE inner constraints.
///
/// Variables are allocated on the parent [`PatternBuilder`] (flat model).
/// The SubqueryBuilder tracks the scope boundary to determine which
/// variables are inner vs. cross-scope (shared).
/// A constraint collector for subquery / CTE inner scopes.
///
/// Borrows the parent [`PatternBuilder`] so that variable allocation
/// stays in a single shared allocator while constraints emitted from the
/// subquery scope (BaseTable, ColumnExists, From, ProjectColumn, joins,
/// filters, …) accumulate in the subquery's own constraint list and do
/// not leak into the outer pattern's constraints. Commit the collected
/// scope back into the outer pattern via one of `commit_as_where`,
/// `commit_as_join`, or `commit_as_cte`.
#[derive(Debug)]
pub struct SubqueryBuilder<'a> {
    /// Mutable borrow of the enclosing pattern. Allocations are made
    /// against `outer.allocator`, but emitted constraints land in
    /// `self.constraints`, not in `outer.constraints`.
    outer: &'a mut PatternBuilder,
    /// VarId.0 at which inner scope starts. Any VarId < this referenced
    /// in inner constraints is a cross-scope (shared) variable.
    scope_start: usize,
    /// Inner constraints collected for this subquery / CTE.
    constraints: Vec<Constraint>,
}

impl<'a> SubqueryBuilder<'a> {
    /// Begin a subquery scope. The current `var_count()` of the parent
    /// pattern is recorded as `scope_start`; vars allocated through this
    /// builder live in the inner scope, while VarIds smaller than that
    /// (referenced in inner constraints) are detected as shared vars when
    /// the scope is committed.
    pub fn start(outer: &'a mut PatternBuilder) -> Self {
        let scope_start = outer.allocator.len();
        Self {
            outer,
            scope_start,
            constraints: Vec::new(),
        }
    }

    // --- Variable allocation (inner scope) ---

    /// Allocate a relation variable in the inner scope and emit a
    /// `BaseTable` constraint for it. The constraint is pushed only into
    /// the subquery's own list, never into the outer pattern.
    pub fn table(&mut self) -> VarId {
        let v = self.outer.allocator.alloc(VarKind::Relation);
        self.constraints.push(Constraint::BaseTable(v));
        v
    }

    /// Allocate a relation variable that is a SQL alias of an existing
    /// relation. Emits an `AliasOf` constraint into the inner scope.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `original` is not a `Relation` variable.
    pub fn alias_of(&mut self, original: VarId) -> VarId {
        debug_assert!(
            matches!(self.outer.allocator.kind(original), VarKind::Relation),
            "alias_of requires a Relation var, got {:?}",
            self.outer.allocator.kind(original)
        );
        let v = self.outer.allocator.alloc(VarKind::Relation);
        self.constraints
            .push(Constraint::AliasOf { alias: v, original });
        v
    }

    /// Allocate a column variable for the given table (visible in the
    /// inner scope) and emit a `ColumnExists` constraint.
    pub fn column(&mut self, table: VarId) -> VarId {
        let v = self.outer.allocator.alloc(VarKind::Column { table });
        self.constraints
            .push(Constraint::ColumnExists { col: v, table });
        v
    }

    // --- Raw constraint emission ---

    /// Add a raw constraint to the inner scope.
    pub fn constraint(&mut self, c: Constraint) {
        self.constraints.push(c);
    }

    pub fn from(&mut self, table: VarId) {
        self.constraints.push(Constraint::From(table));
    }

    pub fn project_column(&mut self, col: VarId, table: VarId) {
        self.constraints
            .push(Constraint::ProjectColumn { col, table });
    }

    pub fn where_param(&mut self, col: VarId, table: VarId, op: BinaryOperator) {
        self.constraints
            .push(Constraint::WhereParam { col, table, op });
    }

    pub fn project_aggregate(&mut self, function: AggregateFn, col: VarId, table: VarId) {
        self.constraints.push(Constraint::ProjectAggregate {
            function,
            col,
            table,
        });
    }

    pub fn group_by(&mut self, col: VarId, table: VarId) {
        self.constraints.push(Constraint::GroupBy { col, table });
    }

    pub fn column_type_class(&mut self, col: VarId, type_class: TypeClass) {
        self.constraints
            .push(Constraint::ColumnTypeClass { col, type_class });
    }

    /// Compute `shared_vars`: variables that cross the scope boundary.
    ///
    /// Collect outer-scope VarIds referenced by any inner constraint.
    /// Vars don't get remapped at scope boundaries — the inner-scope
    /// reference IS the outer VarId — so a single `Vec<VarId>` per
    /// boundary captures everything.
    fn compute_shared_vars(&self) -> Vec<VarId> {
        let mut shared = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for c in &self.constraints {
            for var_id in constraint_var_ids(c) {
                if var_id.0 < self.scope_start && seen.insert(var_id) {
                    shared.push(var_id);
                }
            }
        }
        shared
    }

    // --- Commit (consume self, push to outer) ---

    /// Commit the subquery scope as a `Constraint::Subquery` in the outer
    /// pattern (e.g., for EXISTS / IN / scalar subqueries).
    pub fn commit_as_where(self, position: SubqueryPosition) {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        outer.constraints.push(Constraint::Subquery {
            position,
            constraints,
            shared_vars,
        });
    }

    /// Commit the subquery scope as the right side of a JOIN.
    pub fn commit_as_join(self, operator: JoinOperator, left_col: VarId, right_col: VarId) {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        outer.constraints.push(Constraint::Join {
            operator,
            right: JoinRight::Subquery {
                constraints,
                shared_vars,
            },
            left_col,
            right_col,
        });
    }

    /// Commit the subquery scope as a CTE. Returns a fresh `VarId` for
    /// the CTE's alias relation; outer constraints (`from`,
    /// `project_column`, joins) should reference this var so the SQL
    /// emits `FROM cte0` rather than the underlying base table.
    pub fn commit_as_cte(self) -> VarId {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        let alias = outer.allocator.alloc(VarKind::Relation);
        outer.constraints.push(Constraint::Cte {
            alias,
            constraints,
            shared_vars,
        });
        alias
    }
}

/// Compute the minimum subquery depth by scanning constraints for
/// Subquery/Cte nesting.
fn compute_min_depth(constraints: &[Constraint]) -> usize {
    let mut max_inner_depth = 0;
    for c in constraints {
        let inner_depth = match c {
            Constraint::Subquery { constraints, .. } => 1 + compute_min_depth(constraints),
            Constraint::Cte { constraints, .. } => 1 + compute_min_depth(constraints),
            Constraint::Join {
                right: JoinRight::Subquery { constraints, .. },
                ..
            } => 1 + compute_min_depth(constraints),
            Constraint::Join {
                right: JoinRight::Cte { constraints, .. },
                ..
            } => 1 + compute_min_depth(constraints),
            _ => 0,
        };
        max_inner_depth = max_inner_depth.max(inner_depth);
    }
    max_inner_depth
}

/// Extract all VarIds referenced by a constraint (non-recursively).
pub fn constraint_var_ids(c: &Constraint) -> Vec<VarId> {
    match c {
        Constraint::BaseTable(v) => vec![*v],
        Constraint::AliasOf { alias, original } => vec![*alias, *original],
        Constraint::ColumnExists { col, table } => vec![*col, *table],
        Constraint::ColumnTypeClass { col, .. } => vec![*col],
        Constraint::TypeCompatible(a, b) | Constraint::Eq(a, b) | Constraint::NotEq(a, b) => {
            vec![*a, *b]
        }
        Constraint::From(v) => vec![*v],
        Constraint::Join {
            right,
            left_col,
            right_col,
            ..
        } => {
            let mut ids = vec![*left_col, *right_col];
            if let JoinRight::Table(t) = right {
                ids.push(*t);
            }
            ids
        }
        Constraint::ProjectColumn { col, table }
        | Constraint::GroupBy { col, table }
        | Constraint::WhereRangeParam { col, table }
        | Constraint::WhereBetweenParam { col, table } => vec![*col, *table],
        Constraint::ProjectAggregate { col, table, .. }
        | Constraint::Having { col, table, .. }
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
            conditions.iter().flat_map(constraint_var_ids).collect()
        }
        Constraint::OrderBy { col, table, .. } => vec![*col, *table],
        Constraint::Subquery { .. } | Constraint::Cte { .. } => {
            // Don't recurse into nested constraint sets
            vec![]
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

/// Offset all VarIds in a constraint by the given amount.
fn offset_constraint(c: &Constraint, offset: usize) -> Constraint {
    let off = |v: VarId| VarId(v.0 + offset);

    match c {
        Constraint::BaseTable(v) => Constraint::BaseTable(off(*v)),
        Constraint::AliasOf { alias, original } => Constraint::AliasOf {
            alias: off(*alias),
            original: off(*original),
        },
        Constraint::ColumnExists { col, table } => Constraint::ColumnExists {
            col: off(*col),
            table: off(*table),
        },
        Constraint::ColumnTypeClass { col, type_class } => Constraint::ColumnTypeClass {
            col: off(*col),
            type_class: type_class.clone(),
        },
        Constraint::TypeCompatible(a, b) => Constraint::TypeCompatible(off(*a), off(*b)),
        Constraint::Eq(a, b) => Constraint::Eq(off(*a), off(*b)),
        Constraint::NotEq(a, b) => Constraint::NotEq(off(*a), off(*b)),
        Constraint::From(v) => Constraint::From(off(*v)),
        Constraint::Join {
            operator,
            right,
            left_col,
            right_col,
        } => Constraint::Join {
            operator: *operator,
            right: match right {
                JoinRight::Table(t) => JoinRight::Table(off(*t)),
                JoinRight::Subquery {
                    constraints,
                    shared_vars,
                } => JoinRight::Subquery {
                    constraints: constraints
                        .iter()
                        .map(|c| offset_constraint(c, offset))
                        .collect(),
                    shared_vars: shared_vars.iter().copied().map(&off).collect(),
                },
                JoinRight::Cte {
                    alias,
                    constraints,
                    shared_vars,
                } => JoinRight::Cte {
                    alias: off(*alias),
                    constraints: constraints
                        .iter()
                        .map(|c| offset_constraint(c, offset))
                        .collect(),
                    shared_vars: shared_vars.iter().copied().map(&off).collect(),
                },
            },
            left_col: off(*left_col),
            right_col: off(*right_col),
        },
        Constraint::ProjectColumn { col, table } => Constraint::ProjectColumn {
            col: off(*col),
            table: off(*table),
        },
        Constraint::ProjectAggregate {
            function,
            col,
            table,
        } => Constraint::ProjectAggregate {
            function: function.clone(),
            col: off(*col),
            table: off(*table),
        },
        Constraint::ProjectFunction { function, args } => Constraint::ProjectFunction {
            function: function.clone(),
            args: args.iter().map(|(c, t)| (off(*c), off(*t))).collect(),
        },
        Constraint::ProjectLiteral { literal } => Constraint::ProjectLiteral {
            literal: literal.clone(),
        },
        Constraint::GroupBy { col, table } => Constraint::GroupBy {
            col: off(*col),
            table: off(*table),
        },
        Constraint::Having {
            function,
            col,
            table,
            op,
        } => Constraint::Having {
            function: function.clone(),
            col: off(*col),
            table: off(*table),
            op: *op,
        },
        Constraint::WhereParam { col, table, op } => Constraint::WhereParam {
            col: off(*col),
            table: off(*table),
            op: *op,
        },
        Constraint::WhereInParam {
            col,
            table,
            num_values,
        } => Constraint::WhereInParam {
            col: off(*col),
            table: off(*table),
            num_values: *num_values,
        },
        Constraint::WhereRangeParam { col, table } => Constraint::WhereRangeParam {
            col: off(*col),
            table: off(*table),
        },
        Constraint::WhereLike {
            col,
            table,
            negated,
        } => Constraint::WhereLike {
            col: off(*col),
            table: off(*table),
            negated: *negated,
        },
        Constraint::WhereIsNull {
            col,
            table,
            negated,
        } => Constraint::WhereIsNull {
            col: off(*col),
            table: off(*table),
            negated: *negated,
        },
        Constraint::WhereBetweenParam { col, table } => Constraint::WhereBetweenParam {
            col: off(*col),
            table: off(*table),
        },
        Constraint::WhereColumnCompare {
            left_col,
            left_table,
            op,
            right_col,
            right_table,
        } => Constraint::WhereColumnCompare {
            left_col: off(*left_col),
            left_table: off(*left_table),
            op: *op,
            right_col: off(*right_col),
            right_table: off(*right_table),
        },
        Constraint::WhereOr { conditions } => Constraint::WhereOr {
            conditions: conditions
                .iter()
                .map(|c| offset_constraint(c, offset))
                .collect(),
        },
        Constraint::OrderBy {
            col,
            table,
            direction,
            null_order,
        } => Constraint::OrderBy {
            col: off(*col),
            table: off(*table),
            direction: *direction,
            null_order: *null_order,
        },
        Constraint::Limit { limit, offset: o } => Constraint::Limit {
            limit: *limit,
            offset: *o,
        },
        Constraint::Distinct => Constraint::Distinct,
        Constraint::Subquery {
            position,
            constraints,
            shared_vars,
        } => Constraint::Subquery {
            position: position.clone(),
            constraints: constraints
                .iter()
                .map(|c| offset_constraint(c, offset))
                .collect(),
            shared_vars: shared_vars.iter().copied().map(&off).collect(),
        },
        Constraint::Cte {
            alias,
            constraints,
            shared_vars,
        } => Constraint::Cte {
            alias: off(*alias),
            constraints: constraints
                .iter()
                .map(|c| offset_constraint(c, offset))
                .collect(),
            shared_vars: shared_vars.iter().copied().map(&off).collect(),
        },
        Constraint::WindowFunction {
            function,
            partition_col,
            order_col,
            order_type,
        } => Constraint::WindowFunction {
            function: function.clone(),
            partition_col: partition_col.map(|(c, t)| (off(c), off(t))),
            order_col: order_col.map(|(c, t)| (off(c), off(t))),
            order_type: *order_type,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_simple_pattern() {
        let mut b = PatternBuilder::new("single_table");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.tags(&["simple"]);

        let pattern = b.build();

        assert_eq!(pattern.name, "single_table");
        assert_eq!(pattern.num_vars(), 2); // t + c
        assert_eq!(pattern.vars[0], VarKind::Relation);
        assert_eq!(pattern.vars[1], VarKind::Column { table: VarId(0) });
        assert_eq!(pattern.tags, vec!["simple"]);
        assert_eq!(pattern.min_depth, 0);
        assert_eq!(pattern.weight, 1);

        // Should have: BaseTable(0), ColumnExists(1,0), From(0), ProjectColumn(1,0)
        assert_eq!(pattern.constraints.len(), 4);
        assert!(matches!(
            pattern.constraints[0],
            Constraint::BaseTable(VarId(0))
        ));
        assert!(matches!(
            pattern.constraints[1],
            Constraint::ColumnExists {
                col: VarId(1),
                table: VarId(0)
            }
        ));
        assert!(matches!(pattern.constraints[2], Constraint::From(VarId(0))));
        assert!(matches!(
            pattern.constraints[3],
            Constraint::ProjectColumn {
                col: VarId(1),
                table: VarId(0)
            }
        ));
    }

    #[test]
    fn build_pattern_with_eq_not_eq() {
        let mut b = PatternBuilder::new("eq_test");
        let t1 = b.table();
        let t2 = b.table();
        b.eq(t1, t2);
        b.not_eq(t1, t2);

        let pattern = b.build();

        assert_eq!(pattern.num_vars(), 2);
        // BaseTable(0), BaseTable(1), Eq(0,1), NotEq(0,1)
        assert_eq!(pattern.constraints.len(), 4);
        assert!(matches!(
            pattern.constraints[2],
            Constraint::Eq(VarId(0), VarId(1))
        ));
        assert!(matches!(
            pattern.constraints[3],
            Constraint::NotEq(VarId(0), VarId(1))
        ));
    }

    #[test]
    fn build_pattern_with_subquery() {
        let mut b = PatternBuilder::new("with_subquery");

        // Outer query
        let outer_t = b.table();
        let outer_col = b.column(outer_t);
        b.from(outer_t);
        b.project_column(outer_col, outer_t);

        // Inner query: tables/columns allocated through the subquery
        // scope so they don't leak BaseTable/ColumnExists into the outer
        // pattern's constraint list.
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_col = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_col, inner_t);
        sq.where_param(inner_col, inner_t, BinaryOperator::Equal);
        sq.commit_as_join(JoinOperator::InnerJoin, outer_col, inner_col);

        // Cross-scope self-join constraint goes on the outer pattern.
        b.eq(outer_t, VarId(2));

        let pattern = b.build();

        assert_eq!(pattern.num_vars(), 4); // outer_t, outer_col, inner_t, inner_col
        assert_eq!(pattern.min_depth, 1); // has one level of subquery

        // Find the Join constraint
        let join = pattern
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::Join { .. }));
        assert!(join.is_some());
        if let Some(Constraint::Join {
            right:
                JoinRight::Subquery {
                    constraints,
                    shared_vars,
                },
            ..
        }) = join
        {
            // Inner constraints: BaseTable, ColumnExists, From, ProjectColumn, WhereParam.
            // All five were emitted via sq's allocation/emission methods, so they all
            // live in the inner scope rather than leaking to the outer pattern.
            assert_eq!(constraints.len(), 5);
            // Inner constraints reference only inner-scope vars (>= scope_start),
            // so there are no shared (cross-scope) references. The cross-scope
            // eq(outer_t, VarId(2)) is on the outer builder, not the sub-builder.
            assert_eq!(shared_vars.len(), 0);
        } else {
            panic!("expected Join with Subquery");
        }
    }

    #[test]
    fn subquery_shared_vars_detected() {
        let mut b = PatternBuilder::new("correlated_subquery");

        let outer_t = b.table(); // VarId(0)
        let outer_col = b.column(outer_t); // VarId(1)
        b.from(outer_t);

        // Inner query: tables/columns allocated through the subquery scope.
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_col = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_col, inner_t);
        // Reference outer variables in an inner constraint to exercise
        // shared-var detection.
        sq.constraint(Constraint::WhereColumnCompare {
            left_col: inner_col,
            left_table: inner_t,
            op: BinaryOperator::Equal,
            right_col: outer_col,
            right_table: outer_t,
        });
        sq.commit_as_where(SubqueryPosition::ExistsCorrelated);

        let pattern = b.build();

        // Find the Subquery constraint
        let sub = pattern
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::Subquery { .. }));
        assert!(sub.is_some());
        if let Some(Constraint::Subquery { shared_vars, .. }) = sub {
            // outer_col (VarId(1)) and outer_t (VarId(0)) are referenced in inner constraints
            assert!(
                shared_vars.len() >= 2,
                "expected at least 2 shared vars, got {}",
                shared_vars.len()
            );
        }
    }

    #[test]
    fn to_recipe_no_offset() {
        let mut b = PatternBuilder::new("test");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);

        let pattern = b.build();
        let recipe = pattern.to_recipe(0);

        assert_eq!(recipe.num_vars, 2);
        assert_eq!(recipe.constraints, pattern.constraints);
        assert_eq!(recipe.var_kinds.len(), 2);
    }

    #[test]
    fn to_recipe_with_offset() {
        let mut b = PatternBuilder::new("test");
        let t = b.table(); // VarId(0)
        let c = b.column(t); // VarId(1)
        b.from(t);
        b.project_column(c, t);

        let pattern = b.build();
        let recipe = pattern.to_recipe(10);

        assert_eq!(recipe.num_vars, 2);
        // var_kinds is indexed by absolute VarId; with offset 10 there
        // are 10 padding slots before the pattern's two vars.
        assert_eq!(recipe.var_kinds.len(), 12);

        // All VarIds should be offset by 10
        assert!(matches!(
            recipe.constraints[0],
            Constraint::BaseTable(VarId(10))
        ));
        assert!(matches!(
            recipe.constraints[1],
            Constraint::ColumnExists {
                col: VarId(11),
                table: VarId(10)
            }
        ));
        assert!(matches!(recipe.constraints[2], Constraint::From(VarId(10))));
        assert!(matches!(
            recipe.constraints[3],
            Constraint::ProjectColumn {
                col: VarId(11),
                table: VarId(10)
            }
        ));
    }

    #[test]
    fn two_patterns_non_overlapping_recipes() {
        let mut b1 = PatternBuilder::new("pattern1");
        let t1 = b1.table();
        b1.from(t1);
        let p1 = b1.build();

        let mut b2 = PatternBuilder::new("pattern2");
        let t2 = b2.table();
        let c2 = b2.column(t2);
        b2.from(t2);
        b2.project_column(c2, t2);
        let p2 = b2.build();

        let r1 = p1.to_recipe(0);
        let r2 = p2.to_recipe(p1.num_vars()); // offset by p1's var count

        // r1 uses VarIds 0
        // r2 uses VarIds 1, 2 (offset by 1)
        assert_eq!(r1.var_kinds.len(), 1);
        assert_eq!(r2.var_kinds.len(), 3); // 1 padding slot + 2 pattern vars
        assert!(matches!(r2.constraints[0], Constraint::BaseTable(VarId(1))));
    }

    #[test]
    fn min_depth_no_subqueries() {
        let mut b = PatternBuilder::new("flat");
        let t = b.table();
        b.from(t);
        let p = b.build();
        assert_eq!(p.min_depth, 0);
    }

    #[test]
    fn min_depth_with_subquery() {
        let mut b = PatternBuilder::new("depth_1");
        let outer_t = b.table();
        let outer_col = b.column(outer_t);
        b.from(outer_t);

        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_col = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_col, inner_t);
        sq.commit_as_join(JoinOperator::InnerJoin, outer_col, inner_col);

        let p = b.build();
        assert_eq!(p.min_depth, 1);
    }

    #[test]
    fn builder_aggregate_pattern() {
        let mut b = PatternBuilder::new("aggregate_with_group_by");
        let t = b.table();
        let agg_col = b.column(t);
        let group_col = b.column(t);
        b.column_type_class(agg_col, TypeClass::Numeric);
        b.from(t);
        b.project_aggregate(AggregateFn::Count { distinct: false }, agg_col, t);
        b.project_column(group_col, t);
        b.group_by(group_col, t);
        b.tags(&["aggregate", "group_by"]);

        let pattern = b.build();

        assert_eq!(pattern.name, "aggregate_with_group_by");
        assert_eq!(pattern.num_vars(), 3); // t, agg_col, group_col
        assert_eq!(pattern.tags, vec!["aggregate", "group_by"]);
        assert_eq!(pattern.min_depth, 0);
    }

    #[test]
    fn builder_window_function_pattern() {
        let mut b = PatternBuilder::new("row_number");
        let t = b.table();
        let partition_col = b.column(t);
        let order_col = b.column(t);
        b.from(t);
        b.window_function(
            WindowFn::RowNumber,
            Some((partition_col, t)),
            Some((order_col, t)),
            Some(OrderType::OrderAscending),
        );
        b.tags(&["window_function"]);

        let pattern = b.build();

        assert_eq!(pattern.num_vars(), 3);
        assert!(
            pattern
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::WindowFunction { .. }))
        );
    }
}
