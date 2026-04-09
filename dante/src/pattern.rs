//! Pattern, PatternBuilder, SubqueryBuilder, and Recipe types.
//!
//! A Pattern is a named, composable set of constraints with a shared variable
//! scope. Patterns are the unit of registration in the constraint registry.

use readyset_sql::ast::{
    BinaryOperator, CompoundSelectOperator, JoinOperator, NullOrder, OrderType,
};

use crate::constraint::{
    AggregateFn, Constraint, DialectSupport, JoinRight, LiteralKind, ScalarFn, SubqueryExprKind,
    SubqueryRelationKind, TypeClass, WindowFn,
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
    /// The primary table variable used for composition unification.
    pub primary_table: VarId,
}

impl Pattern {
    /// Number of variables in this pattern.
    pub fn num_vars(&self) -> usize {
        self.vars.len()
    }

    /// True if this pattern represents a compound SELECT (UNION/INTERSECT/EXCEPT)
    /// — i.e. it carries at least one [`Constraint::CompoundSelect`]. Compound
    /// SELECTs are complete query shapes and are not composable with other
    /// patterns.
    ///
    /// Derived from constraint structure rather than a tag string so that
    /// pattern authors cannot accidentally miscategorize by forgetting (or
    /// duplicating) a tag, and so other patterns that happen to use the
    /// word "compound" descriptively (e.g., `compound_where` for
    /// multi-conjunction filters) are not affected.
    pub fn is_compound(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, Constraint::CompoundSelect { .. }))
    }

    ////// Convert this pattern to a Recipe, optionally renumbering variables
    /// by `var_offset` to avoid conflicts when composing multiple patterns.
    /// The offset is applied transiently to constraints; the resulting
    /// Recipe stores `var_kinds` padded so that `var_kinds[v.0]` is
    /// well-defined for any var that appears in `constraints`.
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
                .map(|c| c.map_var_ids(&|v| VarId(v.0 + var_offset)))
                .collect()
        };

        // Padding kept as Relation purely to satisfy `var_kinds[v.0]`
        // indexing. See the doc comment for the invariant that keeps this
        // sound; the debug_assert below enforces it dynamically.
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
            primary_table: VarId(self.primary_table.0 + var_offset),
        }
    }

    /// Whether a (possibly composed) pattern name should be driven through
    /// the literal-substitution code path rather than parameter autobinding.
    /// A small set of patterns interact poorly with autoparameterization;
    /// listing them here, alongside the patterns themselves, keeps the
    /// knowledge in the dante crate so renames cannot silently disable the
    /// probe in distant callers.
    ///
    /// Accepts a string so that composed pattern names (e.g.
    /// `aggregated_join_subquery_eq_filter+like`) can be tested without
    /// keeping the constituent `Pattern` objects around.
    pub fn name_needs_literal_mode(name: &str) -> bool {
        const NAMES: &[&str] = &[
            "aggregated_join_subquery_eq_filter",
            "aggregated_join_subquery_having_filter",
            "having_to_where_promotion",
            "from_subquery_filter",
            "left_join_with_rhs_filter",
        ];
        if NAMES.contains(&name) {
            return true;
        }
        if name.contains('+') {
            return name.split('+').any(|part| NAMES.contains(&part.trim()));
        }
        false
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
    /// Primary outer relation for this recipe — preserved across composes
    /// so that subsequent partner patterns unify against the actual base
    /// table rather than a hardcoded `VarId(0)`. The previous compose path
    /// assumed `VarId(0)` was always the primary, which broke for any
    /// pattern that allocates a non-table var first or never declares an
    /// outer `BaseTable`.
    pub primary_table: VarId,
}

impl Recipe {
    /// Compose this recipe with another pattern by merging all constraints.
    ///
    /// Both patterns contribute all their constraints (projections, filters,
    /// joins, etc.). The other pattern's variables are offset to avoid
    /// overlap and its primary table is unified with this recipe's primary
    /// table via an `Eq` constraint.
    ///
    /// Only structurally redundant constraints are deduplicated:
    /// - `From` for the unified table (would create a duplicate FROM entry)
    /// - `BaseTable` for the unified table (already declared by base)
    /// - `Limit` (SQL allows only one; the base recipe's limit is kept)
    /// - `Distinct` (redundant duplicate)
    pub fn compose(&self, other: &Pattern) -> Recipe {
        let offset = self.num_vars;
        let other_recipe = other.to_recipe(offset);
        let other_primary = other_recipe.primary_table;
        let base_primary = self.primary_table;

        let mut constraints = self.constraints.clone();

        // A primary that is a `DerivedRelation` (CTE alias, FROM-position
        // derived table, or JOIN subquery) is not interchangeable with a
        // base-table primary — unifying them via Eq/AliasOf would let
        // `resolve_eq` collapse the partner's column references onto the
        // derived alias rep, then `Cte`/`Subquery` build retroactively
        // retargets every column the partner bound on that rep to the
        // synthesized `cteN`/`sqN` name. The resolved query emits
        // `cteN.cX` for columns the CTE body never projects, and MySQL
        // rejects with `ERROR 42S22 (1054): Unknown column`.
        //
        // When either primary is `DerivedRelation`, leave both relations
        // distinct: the resolved query has both FROM entries (an
        // implicit cross join), the derived alias is referenced only
        // where the base pattern explicitly uses it, and the partner's
        // columns stay qualified by the partner's real base table.
        // var_kinds is indexed by absolute VarId (Recipe carries no
        // offset metadata), so no offset arithmetic is needed.
        let base_primary_is_derived = matches!(
            self.var_kinds.get(base_primary.0),
            Some(VarKind::DerivedRelation)
        );
        let other_primary_is_derived = matches!(
            other_recipe.var_kinds.get(other_primary.0),
            Some(VarKind::DerivedRelation)
        );
        let unify_primaries = !(base_primary_is_derived || other_primary_is_derived);

        if unify_primaries {
            // If `other` self-joins on its primary (i.e. has a Join whose
            // right is `Table(other_primary)`), unifying with Eq would
            // make base's FROM and other's JOIN render the same physical
            // table+alias — MySQL rejects that with "Not unique
            // table/alias" (1066). Use AliasOf instead so the JOIN
            // target gets a distinct SQL alias of the same underlying
            // table.
            let other_primary_is_join_target = other_recipe.constraints.iter().any(|c| {
                matches!(
                    c,
                    Constraint::Join {
                        right: JoinRight::Table(t),
                        ..
                    } if *t == other_primary
                )
            });
            if other_primary_is_join_target {
                constraints.push(Constraint::AliasOf {
                    alias: other_primary,
                    original: base_primary,
                });
            } else {
                constraints.push(Constraint::Eq(base_primary, other_primary));
            }
        }

        let (mut base_has_limit, mut base_has_distinct, mut base_has_order_by) =
            (false, false, false);
        for c in &self.constraints {
            match c {
                Constraint::Limit { .. } => base_has_limit = true,
                Constraint::Distinct => base_has_distinct = true,
                Constraint::OrderBy { .. } => base_has_order_by = true,
                _ => {}
            }
        }

        for c in &other_recipe.constraints {
            let skip = match c {
                // Deduplicate From for the unified table. Skip dedup
                // when primaries are NOT unified (derived-alias case):
                // the partner's own base table needs its own FROM in
                // the rendered query.
                Constraint::From(t) if unify_primaries && *t == other_primary => true,
                // Deduplicate BaseTable for the unified table (same caveat).
                Constraint::BaseTable(t) if unify_primaries && *t == other_primary => true,
                // Keep only one Limit
                Constraint::Limit { .. } if base_has_limit => true,
                // Deduplicate Distinct
                Constraint::Distinct if base_has_distinct => true,
                // Keep only one OrderBy
                Constraint::OrderBy { .. } if base_has_order_by => true,
                // Having / HavingKeyFilter: keep all, the resolver
                // AND-merges them into a single HAVING clause. Dedup used
                // to drop them silently — and worse, conflated the two as
                // a single dedup category, so a `Having COUNT(x)>?`
                // composed with a `HavingKeyFilter c=?` would silently
                // drop one of them.
                _ => false,
            };

            if !skip {
                constraints.push(c.clone());
            }
        }

        // Cross-pattern relation references that happen to resolve to the
        // same physical table get auto-aliased by the resolver
        // (`resolve_table_exists`), so compose no longer emits the
        // N×M `Or(NotEq, AliasOf)` cross-pair soup it used to.

        // self.var_kinds is indexed 0..self.num_vars (no padding because
        // self came from to_recipe(0) at the entry point). other_recipe's
        // var_kinds starts with `offset` placeholder slots (the padding
        // added by to_recipe(offset)) followed by other.num_vars real
        // entries — extend past the padding so the merged var_kinds
        // remains indexed by absolute VarId without gaps.
        let mut var_kinds = self.var_kinds.clone();
        var_kinds.extend(other_recipe.var_kinds.into_iter().skip(offset));

        // When the base's primary IS a `DerivedRelation` (CTE alias /
        // FROM-subquery alias) and we just cross-joined a partner whose
        // primary is a real base table, promote that base-table primary
        // as the recipe's new primary. Subsequent `compose` calls then
        // unify their partners against the base table instead of
        // accumulating more cross-joins onto the derived alias —
        // without this, an N-partner fusion against a derived base
        // produces an (N+1)-way cartesian (50^(N+1) rows at the soak's
        // rows_per_table=50, which blows past Readyset's connection
        // budget around N=4).
        let new_primary = if unify_primaries || !base_primary_is_derived || other_primary_is_derived
        {
            base_primary
        } else {
            other_primary
        };

        Recipe {
            constraints,
            num_vars: self.num_vars + other_recipe.num_vars,
            var_kinds,
            primary_table: new_primary,
        }
    }
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
    primary_table: Option<VarId>,
}

impl PatternBuilder {
    /// Create a new pattern builder with the given name.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            allocator: VarAllocator::new(),
            constraints: Vec::new(),
            primary_table: None,
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
        if self.primary_table.is_none() {
            self.primary_table = Some(v);
        }
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
            matches!(self.allocator.kind(original), Some(VarKind::Relation)),
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

    /// Emit a `HavingKeyFilter` constraint — a parametrizable filter on a
    /// GROUP BY key column in the HAVING clause.
    pub fn having_key_filter(&mut self, col: VarId, table: VarId, op: BinaryOperator) {
        self.constraints
            .push(Constraint::HavingKeyFilter { col, table, op });
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
    /// returned builder (`commit_as_where`, `commit_as_from`,
    /// `commit_as_join`, or `commit_as_cte`).
    pub fn subquery(&mut self) -> SubqueryBuilder<'_> {
        SubqueryBuilder::start(self)
    }

    /// Add a compound SELECT (UNION ALL, INTERSECT, etc.).
    ///
    /// Each branch is a list of structural constraints (From, ProjectColumn, etc.)
    /// that resolves against the shared outer env. Schema constraints (BaseTable,
    /// ColumnExists) should be in the outer builder, not in branches.
    pub fn compound_select(
        &mut self,
        operator: CompoundSelectOperator,
        branches: Vec<Vec<Constraint>>,
    ) {
        self.constraints
            .push(Constraint::CompoundSelect { operator, branches });
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
    ///
    /// Panics if no primary outer relation can be located. The previous
    /// behavior of silently defaulting to `VarId(0)` produced
    /// `Unbound(VarId(0))` failures during composition for patterns that
    /// allocate a non-table var first or never declare an outer `BaseTable`.
    pub fn build(self) -> Pattern {
        let min_depth = compute_min_depth(&self.constraints);
        let primary_table = self.primary_table.unwrap_or_else(|| {
            let outer_base = self.constraints.iter().find_map(|c| match c {
                Constraint::BaseTable(v) => Some(*v),
                _ => None,
            });
            if let Some(v) = outer_base {
                return v;
            }
            panic!(
                "PatternBuilder::build() for pattern `{name}`: no primary table set. \
                 Call `.table()` (or otherwise declare an outer BaseTable / CTE / \
                 FROM-subquery) before `.build()` — otherwise composition unifies \
                 against an unallocated variable and the resolver fails with Unbound.",
                name = self.name,
            )
        });
        Pattern {
            name: self.name,
            constraints: self.constraints,
            vars: self.allocator.into_kinds(),
            tags: self.tags,
            min_depth,
            weight: self.weight,
            dialect_support: self.dialect_support,
            primary_table,
        }
    }
}

/// A constraint collector for subquery/CTE inner scopes.
///
/// Variables are allocated against the parent [`PatternBuilder`] so the
/// allocator stays shared, but emitted constraints accumulate in
/// `self.constraints` and don't leak into the outer pattern. Commit via
/// `commit_as_where`, `commit_as_join`, or `commit_as_cte`.
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
            matches!(self.outer.allocator.kind(original), Some(VarKind::Relation)),
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

    pub fn having_key_filter(&mut self, col: VarId, table: VarId, op: BinaryOperator) {
        self.constraints
            .push(Constraint::HavingKeyFilter { col, table, op });
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
            for var_id in c.var_ids() {
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
    pub fn commit_as_where(self, kind: SubqueryExprKind) {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        outer.constraints.push(Constraint::SubqueryExpr {
            kind,
            constraints,
            shared_vars,
        });
    }

    /// Commit the subquery scope as a derived table in the outer FROM
    /// clause (`FROM (SELECT ...) AS sq`). Returns a `VarId` for the
    /// outer relation alias — peer to `commit_as_join` / `commit_as_cte`
    /// which both return their alias var. This lets composing patterns
    /// unify against a real outer relation rather than relying on a
    /// hardcoded `VarId(0)`.
    ///
    /// The alias is allocated as `VarKind::DerivedRelation` so
    /// `compose` refuses to unify it with a partner pattern's
    /// base-table primary — same invariant as `commit_as_cte`. Outer
    /// references via `From(alias)` / `ProjectColumn { table: alias, .. }`
    /// resolve through `env` to the fresh `sqN` SQL identifier the
    /// resolver binds at build time.
    pub fn commit_as_from(self) -> VarId {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        let alias = outer.allocator.alloc(VarKind::DerivedRelation);
        outer.constraints.push(Constraint::SubqueryRelation {
            kind: SubqueryRelationKind::FromSubquery,
            alias,
            constraints,
            shared_vars,
        });
        if outer.primary_table.is_none() {
            outer.primary_table = Some(alias);
        }
        alias
    }

    /// Commit the subquery scope as the right side of a JOIN.
    ///
    /// Emits two constraints: a `SubqueryRelation { kind: JoinTarget,
    /// alias }` (which the resolver processes uniformly with CTE /
    /// FROM-subquery relations) and a sibling `Join { right:
    /// JoinRight::Table(alias) }` that references the alias. This is the
    /// peer of `commit_as_cte` / `commit_as_from`.
    pub fn commit_as_join(self, operator: JoinOperator, left_col: VarId, right_col: VarId) {
        let shared_vars = self.compute_shared_vars();
        let SubqueryBuilder {
            outer, constraints, ..
        } = self;
        let alias = outer.allocator.alloc(VarKind::DerivedRelation);
        outer.constraints.push(Constraint::SubqueryRelation {
            kind: SubqueryRelationKind::JoinTarget,
            alias,
            constraints,
            shared_vars,
        });
        outer.constraints.push(Constraint::Join {
            operator,
            right: JoinRight::Table(alias),
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
        // `DerivedRelation` (not `Relation`) so union-find refuses any
        // attempt to unify the CTE alias with a partner pattern's base
        // table — see `compose` and `VarKind::DerivedRelation` for the
        // failure mode this prevents.
        let alias = outer.allocator.alloc(VarKind::DerivedRelation);
        outer.constraints.push(Constraint::SubqueryRelation {
            kind: SubqueryRelationKind::Cte,
            alias,
            constraints,
            shared_vars,
        });
        // CTE-only outer scopes have no `BaseTable`; the alias *is* the
        // primary outer relation, so adopt it as the primary table when one
        // hasn't been set explicitly. Without this, `PatternBuilder::build`
        // would panic for patterns like `simple_cte`.
        if outer.primary_table.is_none() {
            outer.primary_table = Some(alias);
        }
        alias
    }
}

/// Compute the minimum subquery depth by scanning constraints for
/// SubqueryExpr / SubqueryRelation nesting.
fn compute_min_depth(constraints: &[Constraint]) -> usize {
    let mut max_inner_depth = 0;
    for c in constraints {
        let inner_depth = match c {
            Constraint::SubqueryExpr { constraints, .. } => 1 + compute_min_depth(constraints),
            Constraint::SubqueryRelation { constraints, .. } => 1 + compute_min_depth(constraints),
            Constraint::CompoundSelect { branches, .. } => branches
                .iter()
                .map(|b| compute_min_depth(b))
                .max()
                .unwrap_or(0),
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
            conditions.iter().flat_map(constraint_var_ids).collect()
        }
        Constraint::OrderBy { col, table, .. } => vec![*col, *table],
        Constraint::SubqueryExpr { .. }
        | Constraint::SubqueryRelation { .. }
        | Constraint::CompoundSelect { .. } => {
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
        Constraint::Or(preferred, fallback) => {
            let mut ids: Vec<VarId> = preferred.iter().flat_map(constraint_var_ids).collect();
            ids.extend(fallback.iter().flat_map(constraint_var_ids));
            ids
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;

    #[test]
    fn is_compound_distinguishes_compound_select_from_compound_where() {
        // The dispatch must derive "is compound SELECT" from the constraint
        // structure (Constraint::CompoundSelect), not from a "compound" tag
        // string — otherwise filter patterns like `compound_where` (which
        // describe multi-conjunction WHERE clauses) get conflated with
        // UNION/INTERSECT and silently lose composition.
        assert!(
            registry::compound::union_all_same_table().is_compound(),
            "UNION ALL pattern is a compound SELECT"
        );
        assert!(
            !registry::filters::compound_where().is_compound(),
            "compound_where is a multi-WHERE filter, not a compound SELECT"
        );
    }

    #[test]
    fn recipe_carries_primary_table_after_compose() {
        // Two patterns each with their own primary table; after compose,
        // the resulting recipe's primary_table is the BASE pattern's.
        let mut a = PatternBuilder::new("a");
        let ta = a.table();
        let ca = a.column(ta);
        a.from(ta);
        a.project_column(ca, ta);
        let a = a.build();
        assert_eq!(a.primary_table, VarId(0));

        let mut b = PatternBuilder::new("b");
        let tb = b.table();
        let cb = b.column(tb);
        b.from(tb);
        b.project_column(cb, tb);
        let b = b.build();
        assert_eq!(b.primary_table, VarId(0));

        let recipe = a.to_recipe(0).compose(&b);
        assert_eq!(
            recipe.primary_table,
            VarId(0),
            "composed recipe should carry the base pattern's primary table"
        );
        // The Eq/AliasOf unification must reference base_primary, not a
        // stale hardcoded VarId(0). Since base_primary is already VarId(0)
        // here, also assert the offset arithmetic is correct on the other
        // side: other's primary at offset = a.num_vars() = 2.
        assert!(recipe.constraints.iter().any(|c| matches!(
            c,
            Constraint::Eq(VarId(0), VarId(2))
                | Constraint::AliasOf {
                    alias: VarId(2),
                    original: VarId(0),
                }
        )));
    }

    #[test]
    fn pattern_with_column_var_before_table_var_composes_correctly() {
        // A pattern whose first allocation is a column (via a subquery)
        // and only later allocates a table at the outer level. The old
        // compose code assumed VarId(0) was the table; now compose uses
        // the explicit primary_table so this still composes.
        let mut b = PatternBuilder::new("col_before_table");
        // Inner subquery first — this allocates a relation var (its own
        // table) at VarId(0), so column comes at VarId(1).
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let _inner_c = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(_inner_c, inner_t);
        sq.commit_as_cte();
        // Outer table allocated AFTER the CTE — its var index is not 0.
        let outer_t = b.table();
        let outer_c = b.column(outer_t);
        b.from(outer_t);
        b.project_column(outer_c, outer_t);
        b.tags(&["cte"]);
        let p = b.build();

        // primary_table should NOT be VarId(0) — the CTE alias was set
        // first by commit_as_cte, but the outer .table() doesn't override
        // an explicit primary; either is fine as long as it's a real
        // outer relation, not a hardcoded zero.
        assert!(
            matches!(p.primary_table, VarId(_)),
            "primary_table must be set to a real var, not hardcoded VarId(0)"
        );

        // Composing it with itself doesn't panic. When the primary IS a
        // CTE alias (DerivedRelation kind), `compose` deliberately
        // skips the Eq/AliasOf to avoid unifying a derived alias with a
        // partner pattern's base-table primary; the partner's relation
        // stays distinct in a cross-join shape. When the primary isn't
        // a derived alias, the unification constraint must still
        // reference the actual primary_table — never a hardcoded
        // `VarId(0)`.
        let recipe = p.to_recipe(0).compose(&p);
        let primary = recipe.primary_table;
        let primary_is_derived = matches!(
            p.vars[p.primary_table.0],
            crate::var::VarKind::DerivedRelation
        );
        assert_eq!(
            primary, p.primary_table,
            "composed recipe must carry the base pattern's primary_table"
        );
        if !primary_is_derived {
            assert!(
                recipe.constraints.iter().any(|c| match c {
                    Constraint::Eq(a, _) | Constraint::AliasOf { original: a, .. } => *a == primary,
                    _ => false,
                }),
                "composed recipe must unify against the recipe's primary_table"
            );
        }
    }

    #[test]
    fn commit_as_from_returns_outer_alias_var_and_sets_primary() {
        // commit_as_from must allocate and return an outer relation var,
        // peer to commit_as_join / commit_as_cte, and adopt it as primary
        // when no primary has been set yet. Without this, composition
        // would unify against VarId(0) which isn't allocated to anything.
        let mut b = PatternBuilder::new("from_subquery_only");
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_c = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_c, inner_t);
        let derived_alias = sq.commit_as_from();
        b.tags(&["subquery"]);
        let p = b.build();
        assert_eq!(
            p.primary_table, derived_alias,
            "commit_as_from should adopt its alias as primary_table"
        );
        // Composing with a peer pattern produces a unification constraint
        // anchored on the derived alias, not VarId(0).
        let mut b2 = PatternBuilder::new("simple");
        let t2 = b2.table();
        let c2 = b2.column(t2);
        b2.from(t2);
        b2.project_column(c2, t2);
        let p2 = b2.build();
        let recipe = p.to_recipe(0).compose(&p2);
        // FROM-subquery alias is `DerivedRelation`, so `compose` skips
        // the Eq/AliasOf unification with the base-table partner —
        // both relations stay distinct (cross-join shape). When the
        // partner's primary is a base table, `compose` also promotes
        // the partner as the recipe's new primary so subsequent
        // composes unify against a real table rather than the derived
        // alias (which would accumulate cross-joins).
        let p2_primary = VarId(p.num_vars() + p2.primary_table.0);
        assert_eq!(
            recipe.primary_table, p2_primary,
            "compose with derived base + base-table partner must \
             promote the partner as new primary"
        );
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::From(t) if *t == p2_primary)),
            "partner From must survive when FROM-subquery primary doesn't unify"
        );
        let _ = derived_alias;
    }

    #[test]
    #[should_panic(expected = "no primary table set")]
    fn pattern_with_no_outer_relation_panics_on_build() {
        // No table(), no commit_as_cte, no FromSubquery. Build must
        // surface a clear panic rather than silently defaulting to
        // VarId(0) (which would later fail with `Unbound(VarId(0))` deep
        // in the resolver).
        let mut b = PatternBuilder::new("empty");
        b.tags(&["bogus"]);
        let _ = b.build();
    }

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

    /// `commit_as_join` should emit a `SubqueryRelation { kind: JoinTarget,
    /// alias }` paired with a `Join { right: JoinRight::Table(alias) }` —
    /// not a self-contained `Join { right: JoinRight::Subquery { .. } }`.
    /// This shape lets the resolver process the inner subquery via the
    /// uniform `SubqueryRelation` arm and reference it from the join via
    /// the alias var, the same way `commit_as_cte` works.
    #[test]
    fn commit_as_join_emits_subquery_relation_pair() {
        let mut b = PatternBuilder::new("join_pair");
        let outer_t = b.table();
        let outer_col = b.column(outer_t);
        b.from(outer_t);
        b.project_column(outer_col, outer_t);

        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_col = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_col, inner_t);
        sq.commit_as_join(JoinOperator::InnerJoin, outer_col, inner_col);

        let pattern = b.build();

        // Find the SubqueryRelation { kind: JoinTarget } constraint.
        let jt_alias = pattern
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::SubqueryRelation {
                    kind: SubqueryRelationKind::JoinTarget,
                    alias,
                    ..
                } => Some(*alias),
                _ => None,
            })
            .expect("expected SubqueryRelation { kind: JoinTarget }");

        // Find the Join constraint and assert its right side references the
        // alias var (not a nested subquery).
        let join_right_var = pattern
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::Join {
                    right: JoinRight::Table(t),
                    ..
                } => Some(*t),
                _ => None,
            })
            .expect("expected Join { right: JoinRight::Table(_) }");

        assert_eq!(
            join_right_var, jt_alias,
            "Join right table should reference the JoinTarget SubqueryRelation alias"
        );
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

        // outer_t, outer_col, inner_t, inner_col, plus the JoinTarget
        // alias allocated by `commit_as_join`.
        assert_eq!(pattern.num_vars(), 5);
        assert_eq!(pattern.min_depth, 1); // has one level of subquery

        // Inner constraints live on the SubqueryRelation { kind: JoinTarget }
        // sibling, not inside the Join itself.
        let (constraints, shared_vars) = pattern
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::SubqueryRelation {
                    kind: SubqueryRelationKind::JoinTarget,
                    constraints,
                    shared_vars,
                    ..
                } => Some((constraints, shared_vars)),
                _ => None,
            })
            .expect("expected SubqueryRelation { kind: JoinTarget }");

        // Inner constraints: BaseTable, ColumnExists, From, ProjectColumn, WhereParam.
        // All five were emitted via sq's allocation/emission methods, so they all
        // live in the inner scope rather than leaking to the outer pattern.
        assert_eq!(constraints.len(), 5);
        // Inner constraints reference only inner-scope vars (>= scope_start),
        // so there are no shared (cross-scope) references. The cross-scope
        // eq(outer_t, VarId(2)) is on the outer builder, not the sub-builder.
        assert_eq!(shared_vars.len(), 0);
    }

    #[test]
    fn commit_as_cte_allocates_derived_relation_var() {
        // The CTE alias var must be `VarKind::DerivedRelation`, not
        // `Relation`. Union-find then refuses to unify it with a
        // partner pattern's base-table primary, which is what causes
        // the `cteN.cX` corruption in the resolver.
        let mut b = PatternBuilder::new("cte_kind");
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_col = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_col, inner_t);
        let cte_alias = sq.commit_as_cte();

        b.from(cte_alias);
        b.project_column(inner_col, cte_alias);

        let pattern = b.build();
        assert_eq!(
            pattern.vars[cte_alias.0],
            crate::var::VarKind::DerivedRelation,
            "CTE alias var must be DerivedRelation, got: {:?}",
            pattern.vars[cte_alias.0]
        );
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
        sq.commit_as_where(SubqueryExprKind::ExistsCorrelated);

        let pattern = b.build();

        // Find the Subquery constraint
        let sub = pattern
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::SubqueryExpr { .. }));
        assert!(sub.is_some());
        if let Some(Constraint::SubqueryExpr { shared_vars, .. }) = sub {
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

    // --- Tests for Recipe::compose (symmetric composition) ---

    /// Helper: build a simple "filter" pattern with a WHERE BETWEEN.
    fn make_between_pattern() -> Pattern {
        let mut b = PatternBuilder::new("between");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.where_between_param(c, t);
        b.tags(&["filter"]);
        b.build()
    }

    /// Helper: build a simple aggregate pattern (COUNT with GROUP BY).
    fn make_aggregate_pattern() -> Pattern {
        let mut b = PatternBuilder::new("count_grouped");
        let t = b.table();
        let agg_col = b.column(t);
        let group_col = b.column(t);
        b.column_type_class(agg_col, TypeClass::Numeric);
        b.from(t);
        b.project_aggregate(AggregateFn::Count { distinct: false }, agg_col, t);
        b.project_column(group_col, t);
        b.group_by(group_col, t);
        b.tags(&["aggregate", "group_by"]);
        b.build()
    }

    /// Helper: build a simple single_table pattern.
    fn make_single_table_pattern() -> Pattern {
        let mut b = PatternBuilder::new("single_table");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.tags(&["base"]);
        b.build()
    }

    /// Helper: build an inner_join pattern.
    fn make_inner_join_pattern() -> Pattern {
        let mut b = PatternBuilder::new("inner_join");
        let t1 = b.table();
        let c1 = b.column(t1);
        let t2 = b.table();
        let c2 = b.column(t2);
        b.type_compatible(c1, c2);
        b.from(t1);
        b.join_table(JoinOperator::InnerJoin, t2, c1, c2);
        b.project_column(c1, t1);
        b.project_column(c2, t2);
        b.tags(&["join", "two_table"]);
        b.build()
    }

    /// Helper: build a minimal CTE pattern whose primary is the CTE
    /// alias (a `DerivedRelation` var, not a base table).
    fn make_cte_pattern() -> Pattern {
        let mut b = PatternBuilder::new("cte_primary");
        let mut sq = b.subquery();
        let inner_t = sq.table();
        let inner_c = sq.column(inner_t);
        sq.from(inner_t);
        sq.project_column(inner_c, inner_t);
        let cte_alias = sq.commit_as_cte();

        b.from(cte_alias);
        b.project_column(inner_c, cte_alias);
        b.tags(&["cte"]);
        b.build()
    }

    /// Helper: build a topk pattern (ORDER BY + LIMIT).
    fn make_topk_pattern() -> Pattern {
        let mut b = PatternBuilder::new("topk");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.order_by(c, t, OrderType::OrderDescending, None);
        b.limit(10, None);
        b.tags(&["ordering", "limit"]);
        b.build()
    }

    #[test]
    fn compose_merges_all_constraints() {
        // Compose single_table + between. Both patterns' constraints should
        // be present in the result (modulo dedup of From/BaseTable for the
        // unified table).
        let base = make_single_table_pattern();
        let addon = make_between_pattern();

        let recipe = base.to_recipe(0).compose(&addon);

        // base has 2 vars, addon has 2 vars → 4 total
        assert_eq!(recipe.num_vars, 4);

        // Should have WhereBetweenParam from addon
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereBetweenParam { .. })),
            "compose should keep WHERE from second pattern"
        );

        // Should have ProjectColumn from BOTH patterns
        let project_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ProjectColumn { .. }))
            .count();
        assert_eq!(
            project_count, 2,
            "compose should keep projections from both patterns"
        );

        // Should have Eq unifying the two primary tables
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::Eq(VarId(0), VarId(2)))),
            "compose should unify primary tables"
        );
    }

    #[test]
    fn compose_with_cte_primary_skips_unification() {
        // When the recipe's primary is a CTE alias (DerivedRelation kind),
        // compose must NOT emit Eq/AliasOf unifying it with a partner's
        // base-table primary. Unifying would let union-find collapse
        // the partner's columns onto the CTE alias and emit `cteN.cX`
        // for columns the CTE body never projects.
        let base = make_cte_pattern();
        let partner = make_single_table_pattern();

        let recipe = base.to_recipe(0).compose(&partner);
        let cte_primary = base.primary_table;

        // No Eq referencing the CTE primary.
        assert!(
            !recipe.constraints.iter().any(|c| matches!(
                c,
                Constraint::Eq(a, _) | Constraint::Eq(_, a) if *a == cte_primary
            )),
            "compose must not Eq a CTE-primary recipe with a base-table partner; constraints: {:#?}",
            recipe.constraints
        );

        // No AliasOf referencing the CTE primary as `original`.
        assert!(
            !recipe.constraints.iter().any(|c| matches!(
                c,
                Constraint::AliasOf { original, .. } if *original == cte_primary
            )),
            "compose must not AliasOf-against a CTE primary; constraints: {:#?}",
            recipe.constraints
        );

        // Partner's base table must keep its own From/BaseTable in the
        // composed constraints — without dedup-suppression, the partner
        // would have lost both and the resolver would have no way to
        // build the partner's relation.
        let partner_primary = VarId(base.num_vars() + partner.primary_table.0);
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::From(t) if *t == partner_primary)),
            "partner From must survive when CTE primary doesn't unify"
        );
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::BaseTable(t) if *t == partner_primary)),
            "partner BaseTable must survive when CTE primary doesn't unify"
        );
    }

    #[test]
    fn compose_deduplicates_from_for_unified_table() {
        let base = make_single_table_pattern();
        let addon = make_between_pattern();

        let recipe = base.to_recipe(0).compose(&addon);

        // Only ONE From constraint for the unified primary table.
        let from_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::From(_)))
            .count();
        assert_eq!(
            from_count, 1,
            "should have exactly one From for unified table, got {from_count}"
        );
    }

    #[test]
    fn compose_keeps_non_unified_from() {
        // Compose single_table + inner_join. The join's second table should
        // keep its From (via the Join constraint — inner_join only has From
        // for the left table, but it has a Join for the right table).
        let base = make_single_table_pattern();
        let join = make_inner_join_pattern();

        let recipe = base.to_recipe(0).compose(&join);

        // base has 2 vars, join has 4 vars → 6 total
        assert_eq!(recipe.num_vars, 6);

        // Should have Join constraint from the join pattern
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::Join { .. })),
            "compose should keep Join from second pattern"
        );

        // The join pattern's primary table From is deduplicated, but the
        // Join constraint (which handles the right table) is kept.
        let from_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::From(_)))
            .count();
        assert_eq!(
            from_count, 1,
            "should have one From (join's second table comes via Join, not From)"
        );
    }

    #[test]
    fn compose_keeps_both_projections_aggregate_plus_filter() {
        // Compose aggregate + between. The old compose_addon would drop
        // between's ProjectColumn. The new compose should keep it.
        let agg = make_aggregate_pattern();
        let filter = make_between_pattern();

        let recipe = agg.to_recipe(0).compose(&filter);

        // Should have ProjectAggregate from aggregate pattern
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::ProjectAggregate { .. })),
            "compose should keep aggregate projection"
        );

        // Should have ProjectColumn from BOTH patterns (group_col from agg
        // + projected col from filter)
        let project_col_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ProjectColumn { .. }))
            .count();
        assert_eq!(
            project_col_count, 2,
            "compose should keep ProjectColumn from both patterns"
        );

        // Should have WhereBetweenParam
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereBetweenParam { .. })),
            "compose should keep WHERE from filter pattern"
        );

        // Should have GroupBy from aggregate
        assert!(
            recipe
                .constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. })),
            "compose should keep GroupBy from aggregate pattern"
        );
    }

    #[test]
    fn compose_handles_limit_conflict() {
        // Compose topk (has LIMIT 10) with another pattern that also has LIMIT.
        let topk = make_topk_pattern();
        // Build a pattern with a different limit
        let mut b = PatternBuilder::new("limited");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.limit(5, Some(10));
        let limited = b.build();

        let recipe = topk.to_recipe(0).compose(&limited);

        // Should have exactly one Limit constraint (from the base)
        let limit_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Limit { .. }))
            .count();
        assert_eq!(
            limit_count, 1,
            "compose should keep only one Limit, got {limit_count}"
        );

        // The kept limit should be the base's (limit=10, no offset)
        assert!(
            recipe.constraints.iter().any(|c| matches!(
                c,
                Constraint::Limit {
                    limit: 10,
                    offset: None
                }
            )),
            "compose should keep base pattern's Limit"
        );
    }

    #[test]
    fn compose_deduplicates_distinct() {
        let mut b1 = PatternBuilder::new("distinct1");
        let t = b1.table();
        let c = b1.column(t);
        b1.from(t);
        b1.project_column(c, t);
        b1.distinct();
        let p1 = b1.build();

        let mut b2 = PatternBuilder::new("distinct2");
        let t2 = b2.table();
        let c2 = b2.column(t2);
        b2.from(t2);
        b2.project_column(c2, t2);
        b2.distinct();
        let p2 = b2.build();

        let recipe = p1.to_recipe(0).compose(&p2);

        let distinct_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Distinct))
            .count();
        assert_eq!(
            distinct_count, 1,
            "compose should deduplicate Distinct, got {distinct_count}"
        );
    }

    #[test]
    fn compose_deduplicates_base_table_for_unified() {
        let base = make_single_table_pattern();
        let addon = make_between_pattern();

        let recipe = base.to_recipe(0).compose(&addon);

        // Should have only one BaseTable for VarId(0) — the second pattern's
        // BaseTable for its primary table (which maps to VarId(2) after offset)
        // should be dropped since it's unified.
        let base_table_for_primary = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::BaseTable(VarId(0))))
            .count();
        assert_eq!(
            base_table_for_primary, 1,
            "base's primary table BaseTable should appear exactly once"
        );

        // VarId(2) (addon's primary table after offset) should NOT have BaseTable
        let base_table_for_addon_primary = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::BaseTable(VarId(2))))
            .count();
        assert_eq!(
            base_table_for_addon_primary, 0,
            "addon's primary table BaseTable should be dropped (unified with base)"
        );
    }

    #[test]
    fn compose_keeps_both_havings_for_and_merge() {
        // Regression: compose used to dedup Having (and conflate it with
        // HavingKeyFilter under the same dedup category), silently dropping
        // one. The resolver now AND-merges every Having/HavingKeyFilter
        // into one HAVING clause, so compose must keep both.
        use crate::constraint::AggregateFn;
        use readyset_sql::ast::BinaryOperator;

        let mut b1 = PatternBuilder::new("having1");
        let t = b1.table();
        let c_group = b1.column(t);
        let c_agg = b1.column(t);
        b1.from(t);
        b1.project_column(c_group, t);
        b1.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
        b1.group_by(c_group, t);
        b1.having(
            AggregateFn::Count { distinct: false },
            c_agg,
            t,
            BinaryOperator::Greater,
        );
        let p1 = b1.build();

        let mut b2 = PatternBuilder::new("having2");
        let t2 = b2.table();
        let c2_group = b2.column(t2);
        let c2_agg = b2.column(t2);
        b2.from(t2);
        b2.project_column(c2_group, t2);
        b2.project_aggregate(AggregateFn::Count { distinct: false }, c2_agg, t2);
        b2.group_by(c2_group, t2);
        b2.having(
            AggregateFn::Count { distinct: false },
            c2_agg,
            t2,
            BinaryOperator::Greater,
        );
        let p2 = b2.build();

        let recipe = p1.to_recipe(0).compose(&p2);

        let having_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Having { .. }))
            .count();
        assert_eq!(
            having_count, 2,
            "compose must keep both Having constraints — resolver AND-merges \
             them; the old dedup silently dropped one"
        );
    }

    #[test]
    fn compose_keeps_having_and_having_key_filter_together() {
        // Regression: compose used to put Having and HavingKeyFilter under
        // the same dedup category and drop one when the other was present.
        use crate::constraint::AggregateFn;
        use readyset_sql::ast::BinaryOperator;

        let mut b1 = PatternBuilder::new("having_agg");
        let t = b1.table();
        let c_group = b1.column(t);
        let c_agg = b1.column(t);
        b1.from(t);
        b1.project_column(c_group, t);
        b1.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
        b1.group_by(c_group, t);
        b1.having(
            AggregateFn::Count { distinct: false },
            c_agg,
            t,
            BinaryOperator::Greater,
        );
        let p1 = b1.build();

        let mut b2 = PatternBuilder::new("having_key_filter");
        let t2 = b2.table();
        let c2_group = b2.column(t2);
        b2.from(t2);
        b2.project_column(c2_group, t2);
        b2.group_by(c2_group, t2);
        b2.having_key_filter(c2_group, t2, BinaryOperator::Equal);
        let p2 = b2.build();

        let recipe = p1.to_recipe(0).compose(&p2);
        let having_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Having { .. }))
            .count();
        let key_filter_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::HavingKeyFilter { .. }))
            .count();
        assert_eq!(having_count, 1, "Having from base must survive");
        assert_eq!(
            key_filter_count, 1,
            "HavingKeyFilter from partner must survive — conflating the two \
             dedup categories used to drop it"
        );
    }

    #[test]
    fn compose_deduplicates_order_by() {
        let topk = make_topk_pattern();
        // Build a pattern with a different ORDER BY
        let mut b = PatternBuilder::new("ordered");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.order_by(c, t, OrderType::OrderAscending, None);
        let ordered = b.build();

        let recipe = topk.to_recipe(0).compose(&ordered);

        let order_count = recipe
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::OrderBy { .. }))
            .count();
        assert_eq!(
            order_count, 1,
            "compose should keep only one OrderBy, got {order_count}"
        );
    }

    #[test]
    fn primary_table_is_first_table() {
        let mut b = PatternBuilder::new("test");
        let t = b.table();
        let _c = b.column(t);
        let _t2 = b.table();
        let p = b.build();
        assert_eq!(p.primary_table, VarId(0));
    }

    #[test]
    fn compose_aliases_when_other_primary_is_join_target() {
        use rand::SeedableRng;
        use rand::rngs::SmallRng;
        use readyset_sql::{Dialect, DialectDisplay};

        use crate::entropy::Entropy;
        use crate::resolver::resolve;
        use crate::state::{GenerationState, GeneratorConfig};

        // A pattern whose `primary_table` is the JOIN target rather than its
        // FROM relation. Real patterns with this shape (e.g. cte_with_join)
        // arrive later in the stack, but the bug lives entirely in compose
        // and a hand-built minimal pattern is enough to drive it.
        let other = {
            let mut b = PatternBuilder::new("synth_join_target_primary");
            let t_join = b.table(); // VarId(0) — auto-set as primary
            let t_from = b.table(); // VarId(1)
            let c_left = b.column(t_from);
            let c_right = b.column(t_join);
            b.column_type_class(c_left, TypeClass::Integer);
            b.column_type_class(c_right, TypeClass::Integer);
            b.from(t_from);
            b.project_column(c_left, t_from);
            b.join_table(JoinOperator::InnerJoin, t_join, c_left, c_right);
            b.build()
        };
        assert_eq!(
            other.primary_table,
            VarId(0),
            "sanity: primary is the join target"
        );

        let composed = make_single_table_pattern().to_recipe(0).compose(&other);

        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        let output = resolve(
            &composed.constraints,
            &composed.var_kinds,
            &mut state,
            &mut entropy,
        )
        .expect("composed recipe should resolve");

        // Without the AliasOf-on-self-join fix, base's From and other's Join
        // both render as the same `tN` with no AS clause, and MySQL rejects
        // with 1066 "Not unique table/alias". The fix forces an alias on the
        // join target — assert the rendered SQL JOIN target carries " AS ".
        let sql = output.query.display(Dialect::MySQL).to_string();
        let join_idx = sql
            .find("INNER JOIN ")
            .expect("synth pattern must produce an INNER JOIN");
        let join_tail = &sql[join_idx + "INNER JOIN ".len()..];
        let join_chunk = join_tail.split(" ON ").next().unwrap_or(join_tail);
        assert!(
            join_chunk.contains(" AS "),
            "INNER JOIN target must be aliased to avoid duplicate-table SQL: \
             chunk={join_chunk:?}\nsql={sql}"
        );
        // The cross-pattern Or fallback aliases the FROM-side reference too,
        // so the rendered SQL has at least two " AS " clauses (one for base's
        // From, one for other's Join target).
        assert!(
            sql.matches(" AS `").count() >= 2,
            "expected aliased self-join with two `AS` clauses: {sql}"
        );
    }
}
