//! Resolver: schema constraint resolution and variable binding.
//!
//! The resolver takes a Recipe and a GenerationState, and binds variables
//! to concrete schema elements (tables and columns).

mod ast_builder;
pub(crate) mod schema;

use data_generator::ColumnGenerationSpec;
use readyset_sql::ast::{SelectSpecification, SqlIdentifier, SqlType};

use crate::constraint::{Constraint, ExampleCell};
use crate::entropy::Entropy;
use crate::state::{ColumnMeta, GenerationState, TableSchema};
use crate::var::{UnionFind, VarId, VarKind};

pub(crate) use schema::resolve_schema;

/// A binding for a resolved variable.
#[derive(Debug, Clone)]
pub(crate) enum Binding {
    Table {
        name: SqlIdentifier,
        alias: Option<SqlIdentifier>,
    },
    Column {
        name: SqlIdentifier,
        sql_type: SqlType,
        /// The table VarId that owns this column. Resolved lazily through
        /// the table's Binding to get the current effective name (which may
        /// change if the table is later aliased by an Or fallback).
        table_var: VarId,
    },
}

impl Binding {
    /// For a Table binding, return the effective SQL name (alias if present,
    /// otherwise the table name). This is what column references should use.
    fn effective_table_name(&self) -> Option<&SqlIdentifier> {
        match self {
            Binding::Table { alias: Some(a), .. } => Some(a),
            Binding::Table { name, .. } => Some(name),
            Binding::Column { .. } => None,
        }
    }
}

/// DDL step produced by the resolver when synthesis is needed.
#[derive(Debug, Clone)]
pub enum DdlStep {
    CreateTable {
        name: SqlIdentifier,
        schema: TableSchema,
    },
    AddColumn {
        table: SqlIdentifier,
        column_name: SqlIdentifier,
        meta: ColumnMeta,
    },
}

/// Parameter metadata for generated query parameters.
#[derive(Debug, Clone)]
pub struct ParamMeta {
    pub sql_type: SqlType,
    pub gen_spec: ColumnGenerationSpec,
    pub count: u32,
}

/// Environment: the resolver's working state during constraint resolution.
#[derive(Debug)]
pub(crate) struct Env {
    bindings: Vec<Option<Binding>>,
    union_find: UnionFind,
    ddl_steps: Vec<DdlStep>,
    /// Tables created during this resolution (not yet existing in the DB).
    /// Columns added to these tables are included in the CreateTable DDL
    /// rather than emitted as separate AddColumn steps.
    new_tables: Vec<SqlIdentifier>,
}

impl Env {
    /// Create a new environment for `num_vars` variables.
    pub(crate) fn new(num_vars: usize) -> Self {
        Self {
            bindings: vec![None; num_vars],
            union_find: UnionFind::new(num_vars),
            ddl_steps: Vec::new(),
            new_tables: Vec::new(),
        }
    }

    /// Get the binding for a variable, following the union-find representative.
    pub(crate) fn get(&self, v: VarId) -> Option<&Binding> {
        let rep = self.union_find.find_readonly(v.0);
        self.bindings[rep].as_ref()
    }

    /// Bind a variable (using its union-find representative).
    pub(crate) fn bind(&mut self, v: VarId, binding: Binding) {
        let rep = self.union_find.find(v.0);
        self.bindings[rep] = Some(binding);
    }

    /// Check if a variable is bound.
    pub(crate) fn is_bound(&self, v: VarId) -> bool {
        let rep = self.union_find.find_readonly(v.0);
        self.bindings[rep].is_some()
    }

    /// Returns true if any already-bound relation var holds the given
    /// physical table name. Used by `resolve_table_exists` to detect
    /// duplicate physical references that need to be disambiguated with a
    /// fresh SQL alias — replaces the N×M `Or(NotEq, AliasOf)` cross-pair
    /// constraints that compose used to emit.
    pub(crate) fn relation_with_name_exists(&self, name: &SqlIdentifier) -> bool {
        self.bindings.iter().any(|b| {
            matches!(
                b,
                Some(Binding::Table { name: n, .. }) if n == name
            )
        })
    }

    /// Get the DDL steps produced during resolution.
    #[cfg(test)]
    pub(crate) fn ddl_steps(&self) -> &[DdlStep] {
        &self.ddl_steps
    }

    /// Build the final DDL step list, looking up final schemas for newly
    /// created tables from `state`.
    ///
    /// Returns `EmptyTableSchema` if any new table ended up with no columns
    /// (an internal invariant violation — `resolve_table_exists` always seeds
    /// a primary-key column).
    pub(crate) fn into_ddl_steps(
        self,
        state: &GenerationState,
    ) -> Result<Vec<DdlStep>, ResolveError> {
        let mut steps = Vec::new();
        // Emit CreateTable steps with the final (populated) schema.
        for table_name in &self.new_tables {
            if let Some(schema) = state.table(table_name) {
                if schema.columns.is_empty() {
                    return Err(ResolveError::EmptyTableSchema {
                        table: table_name.clone(),
                    });
                }
                steps.push(DdlStep::CreateTable {
                    name: table_name.clone(),
                    schema: schema.clone(),
                });
            }
        }
        // Emit AddColumn steps, skipping any for newly created tables
        // (those columns are already included in the CreateTable DDL).
        for step in self.ddl_steps {
            match &step {
                DdlStep::AddColumn { table, .. } if self.new_tables.contains(table) => {}
                _ => steps.push(step),
            }
        }
        Ok(steps)
    }

    /// Capture a checkpoint of the current environment for backtracking.
    pub(crate) fn checkpoint(&self) -> EnvCheckpoint {
        EnvCheckpoint {
            bindings: self.bindings.clone(),
            union_find: self.union_find.clone(),
            ddl_steps: self.ddl_steps.clone(),
            new_tables: self.new_tables.clone(),
        }
    }

    /// Restore environment from a previously captured checkpoint.
    pub(crate) fn restore(&mut self, cp: EnvCheckpoint) {
        self.bindings = cp.bindings;
        self.union_find = cp.union_find;
        self.ddl_steps = cp.ddl_steps;
        self.new_tables = cp.new_tables;
    }
}

/// A snapshot of Env state for checkpoint/restore (backtracking).
#[derive(Debug, Clone)]
pub(crate) struct EnvCheckpoint {
    bindings: Vec<Option<Binding>>,
    union_find: UnionFind,
    ddl_steps: Vec<DdlStep>,
    new_tables: Vec<SqlIdentifier>,
}

/// Errors during resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("variable {0:?} is not bound")]
    Unbound(VarId),
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("cannot satisfy NotEq: both variables bound to {0}")]
    NotEqViolation(String),
    #[error("unification error: {0}")]
    UnifyError(#[from] crate::var::UnifyError),
    #[error("limit/offset {value} exceeds i64::MAX; cannot emit as SQL integer literal")]
    LimitOverflow { value: u64 },
    /// A constraint variant reached a handler that doesn't yet support it.
    /// Used to be `_ => {}` arms that silently dropped the constraint and
    /// produced SQL that could spuriously match upstream by coincidence.
    #[error("unsupported constraint variant {variant} in {location}")]
    Unsupported {
        variant: &'static str,
        location: &'static str,
    },
    #[error(
        "inner subquery references {referenced} variables but the outer recipe declares only {declared} kinds"
    )]
    InnerVarKindsTruncated { referenced: usize, declared: usize },
    #[error("internal error: synthesized table {table} has no columns")]
    EmptyTableSchema { table: SqlIdentifier },
}

/// One resolved example, ready for the oracle to materialize.
#[derive(Debug, Clone)]
pub struct ResolvedExample {
    pub note: &'static str,
    pub dialect: crate::constraint::DialectSupport,
    pub row_overrides: Vec<RowOverride>,
    pub param_overrides: Vec<ParamOverride>,
}

/// Row-level override: pin a specific column in a specific table to a value.
#[derive(Debug, Clone)]
pub struct RowOverride {
    pub table: SqlIdentifier,
    pub column: SqlIdentifier,
    pub value: crate::constraint::ExampleValue,
}

/// Parameter override: pin a placeholder (by 0-based index) to a value.
#[derive(Debug, Clone)]
pub struct ParamOverride {
    /// 0-based index into the query's placeholder list (placeholder $1
    /// has `placeholder_index = 0`).
    pub placeholder_index: usize,
    pub value: crate::constraint::ExampleValue,
}

/// Output of the full resolution pipeline.
#[derive(Debug)]
pub struct ResolverOutput {
    /// The generated query (simple SELECT or compound SELECT).
    pub query: SelectSpecification,
    /// DDL steps needed before executing the query.
    pub ddl: Vec<DdlStep>,
    /// Parameter metadata for data generation.
    pub params: Vec<ParamMeta>,
    /// Resolved examples from `Constraint::Example` constraints.
    pub examples: Vec<ResolvedExample>,
}

/// Phase-2 example resolution: walk `Constraint::Example` entries and emit
/// `ResolvedExample` values with row/param overrides resolved against `env`
/// and `param_map`.
fn resolve_examples(
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    env: &Env,
    param_map: &[Option<usize>],
    pattern_name: &str,
) -> Result<Vec<ResolvedExample>, ResolveError> {
    let mut out = Vec::new();
    for constraint in constraints {
        let Constraint::Example {
            note,
            dialect,
            cells,
        } = constraint
        else {
            continue;
        };
        let mut row_overrides = Vec::new();
        let mut param_overrides = Vec::new();
        for ExampleCell { var, value } in cells {
            let kind = var_kinds
                .get(var.0)
                .ok_or_else(|| ResolveError::Unbound(*var))?;
            match kind {
                VarKind::Column { .. } => {
                    let binding = env.get(*var).ok_or_else(|| ResolveError::Unbound(*var))?;
                    let Binding::Column {
                        name: col_name,
                        table_var,
                        ..
                    } = binding
                    else {
                        return Err(ResolveError::TypeMismatch {
                            expected: "Column binding".into(),
                            actual: format!("{binding:?}"),
                        });
                    };
                    // Resolve the table name through the table's own binding.
                    let table_binding = env
                        .get(*table_var)
                        .ok_or_else(|| ResolveError::Unbound(*table_var))?;
                    let table_name = table_binding.effective_table_name().ok_or_else(|| {
                        ResolveError::TypeMismatch {
                            expected: "Table binding with effective name".into(),
                            actual: format!("{table_binding:?}"),
                        }
                    })?;
                    row_overrides.push(RowOverride {
                        table: table_name.clone(),
                        column: col_name.clone(),
                        value: value.clone(),
                    });
                }
                VarKind::Param { .. } => {
                    let idx = param_map
                        .get(var.0)
                        .copied()
                        .flatten()
                        .ok_or_else(|| ResolveError::Unbound(*var))?;
                    param_overrides.push(ParamOverride {
                        placeholder_index: idx,
                        value: value.clone(),
                    });
                }
                _ => {
                    return Err(ResolveError::TypeMismatch {
                        expected: "VarKind::Column or VarKind::Param".into(),
                        actual: format!("{kind:?} in pattern `{pattern_name}` (example `{note}`)"),
                    });
                }
            }
        }
        out.push(ResolvedExample {
            note,
            dialect: *dialect,
            row_overrides,
            param_overrides,
        });
    }
    Ok(out)
}

/// Full resolution pipeline: resolve schema constraints, then build AST.
pub fn resolve(
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> Result<ResolverOutput, ResolveError> {
    resolve_named(constraints, var_kinds, state, entropy, "")
}

/// Full resolution pipeline with an optional pattern name for error messages.
pub(crate) fn resolve_named(
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    pattern_name: &str,
) -> Result<ResolverOutput, ResolveError> {
    let (mut env, expanded) = resolve_schema(constraints, var_kinds, state, entropy)?;

    // Check if any constraint is a CompoundSelect — if so, use the compound path.
    let compound = expanded.iter().find_map(|c| match c {
        Constraint::CompoundSelect { operator, branches } => Some((operator, branches)),
        _ => None,
    });

    let (query, params, param_map) = if let Some((operator, branches)) = compound {
        ast_builder::build_compound_select(
            &mut env, &expanded, var_kinds, operator, branches, state, entropy,
        )?
    } else {
        let (stmt, params, param_map) =
            ast_builder::build_select(&mut env, &expanded, var_kinds, state, entropy)?;
        (stmt.into(), params, param_map)
    };

    let examples = resolve_examples(constraints, var_kinds, &env, &param_map, pattern_name)?;
    let ddl = env.into_ddl_steps(state)?;
    Ok(ResolverOutput {
        query,
        ddl,
        params,
        examples,
    })
}

/// Attempt to resolve a recipe against the current state.
///
/// On success, the state is updated with any new tables/columns and the
/// `ResolverOutput` is returned. On failure, the state is unchanged.
///
/// This is the core of incremental resolution for Mode 3: callers can
/// checkpoint the state before calling this, and restore on failure.
pub fn try_resolve(
    recipe: &crate::pattern::Recipe,
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> Result<ResolverOutput, ResolveError> {
    let state_cp = state.checkpoint();
    match resolve_named(
        &recipe.constraints,
        &recipe.var_kinds,
        state,
        entropy,
        recipe.name,
    ) {
        Ok(output) => Ok(output),
        Err(e) => {
            state.restore(state_cp);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::ast::{
        BinaryOperator, Expr, FieldDefinitionExpr, InValue, JoinConstraint, SelectSpecification,
        SelectStatement, SqlIdentifier, SqlType,
    };
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;
    use crate::pattern::Pattern;
    use crate::state::GeneratorConfig;
    use crate::var::VarKind;

    fn test_env(dialect: Dialect) -> (GenerationState, SmallRng) {
        let config = GeneratorConfig {
            reuse_preference: 0.0, // always synthesize
            ..Default::default()
        };
        let state = GenerationState::new(dialect, config);
        let rng = SmallRng::seed_from_u64(42);
        (state, rng)
    }

    // -- Env checkpoint tests --

    #[test]
    fn env_checkpoint_restore_round_trips() {
        let mut env = Env::new(4);
        env.bind(
            VarId(0),
            Binding::Table {
                name: SqlIdentifier::from("t0"),
                alias: None,
            },
        );
        env.bind(
            VarId(1),
            Binding::Column {
                name: SqlIdentifier::from("c0"),
                sql_type: SqlType::Int(None),
                table_var: VarId(0),
            },
        );

        let cp = env.checkpoint();

        // Modify state after checkpoint
        env.bind(
            VarId(2),
            Binding::Table {
                name: SqlIdentifier::from("t1"),
                alias: None,
            },
        );
        env.ddl_steps.push(DdlStep::CreateTable {
            name: SqlIdentifier::from("t1"),
            schema: TableSchema::new(SqlIdentifier::from("t1")),
        });

        assert!(env.is_bound(VarId(2)));
        assert_eq!(env.ddl_steps().len(), 1);

        // Restore
        env.restore(cp);

        assert!(env.is_bound(VarId(0)));
        assert!(env.is_bound(VarId(1)));
        assert!(!env.is_bound(VarId(2)));
        assert_eq!(env.ddl_steps().len(), 0);
    }

    #[test]
    fn env_checkpoint_restores_union_find() {
        let mut env = Env::new(4);

        // Checkpoint before unification
        let cp = env.checkpoint();

        // Unify vars 0 and 1
        let kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Relation,
        ];
        env.union_find.union(0, 1, &kinds).unwrap();
        assert!(env.union_find.same_set(0, 1));

        // Restore
        env.restore(cp);

        // After restore, 0 and 1 should no longer be in the same set
        assert!(!env.union_find.same_set(0, 1));
    }

    // -- Incremental resolution / try_resolve tests --

    fn make_single_table_pattern() -> Pattern {
        use crate::pattern::PatternBuilder;
        let mut b = PatternBuilder::new("single_table");
        let t = b.table();
        let c = b.column(t);
        b.from(t);
        b.project_column(c, t);
        b.build()
    }

    fn make_param_pattern() -> Pattern {
        use crate::pattern::PatternBuilder;
        let mut b = PatternBuilder::new("param");
        let t = b.table();
        let c1 = b.column(t);
        let c2 = b.column(t);
        b.from(t);
        b.project_column(c1, t);
        b.where_param(c2, t, BinaryOperator::Equal);
        b.build()
    }

    #[test]
    fn try_resolve_succeeds_for_compatible_recipe() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let pattern = make_single_table_pattern();
        let recipe = pattern.to_recipe(0);

        let result = try_resolve(&recipe, &mut state, &mut entropy);
        assert!(result.is_ok(), "try_resolve should succeed: {result:?}");

        let output = result.unwrap();
        assert!(!output.ddl.is_empty());
    }

    #[test]
    fn try_resolve_state_rollback_on_failure() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        // Build a pattern that Eq's two tables then NotEq's them — guaranteed failure
        use crate::pattern::PatternBuilder;
        let mut b = PatternBuilder::new("contradictory");
        let t1 = b.table();
        let t2 = b.table();
        b.eq(t1, t2);
        b.not_eq(t1, t2);
        let c1 = b.column(t1);
        b.from(t1);
        b.project_column(c1, t1);
        let pattern = b.build();
        let recipe = pattern.to_recipe(0);

        let result = try_resolve(&recipe, &mut state, &mut entropy);
        assert!(result.is_err(), "should fail due to Eq+NotEq contradiction");

        // The contract is that try_resolve rolls back state internally on
        // failure — there must be no tables left over from the partial
        // resolution. No outer restore here on purpose: that would mask a
        // missing internal rollback by clobbering whatever state try_resolve
        // left behind.
        assert_eq!(
            state.tables().len(),
            0,
            "try_resolve must roll back state on Err"
        );
    }

    #[test]
    fn phase2_resolves_example_column_cell_to_row_override() {
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
        use crate::pattern::PatternBuilder;
        use readyset_sql::Dialect;

        let mut b = PatternBuilder::new("p");
        let t = b.table();
        let c = b.column(t);
        b.column_type_class(c, TypeClass::Integer);
        b.from(t);
        b.project_column(c, t);
        b.example(
            "row bait",
            DialectSupport::Both,
            vec![ExampleCell {
                var: c,
                value: ExampleValue::Literal("42"),
            }],
        );
        let pattern = b.build();

        let recipe = pattern.to_recipe(0);
        let config = crate::state::GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut gen_state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xC0DE);
        let mut entropy = Entropy::new(&mut rng);
        let resolved = resolve(
            &recipe.constraints,
            &recipe.var_kinds,
            &mut gen_state,
            &mut entropy,
        )
        .unwrap();

        assert_eq!(resolved.examples.len(), 1);
        let ex = &resolved.examples[0];
        assert_eq!(ex.note, "row bait");
        assert_eq!(ex.row_overrides.len(), 1);
        assert_eq!(ex.row_overrides[0].value, ExampleValue::Literal("42"));
        assert_eq!(ex.param_overrides.len(), 0);
    }

    #[test]
    fn phase2_resolves_example_param_cell_to_param_override() {
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
        use crate::pattern::PatternBuilder;
        use readyset_sql::Dialect;
        use readyset_sql::ast::BinaryOperator;

        let mut b = PatternBuilder::new("p");
        let t = b.table();
        let c = b.column(t);
        b.column_type_class(c, TypeClass::Integer);
        b.from(t);
        b.project_column(c, t);
        let pv = b.where_param(c, t, BinaryOperator::Equal);
        b.example(
            "param bait",
            DialectSupport::Both,
            vec![ExampleCell {
                var: pv,
                value: ExampleValue::Literal("99"),
            }],
        );
        let pattern = b.build();

        let recipe = pattern.to_recipe(0);
        let config = crate::state::GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut gen_state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xC0DE);
        let mut entropy = Entropy::new(&mut rng);
        let resolved = resolve(
            &recipe.constraints,
            &recipe.var_kinds,
            &mut gen_state,
            &mut entropy,
        )
        .unwrap();

        assert_eq!(resolved.examples.len(), 1);
        let ex = &resolved.examples[0];
        assert_eq!(ex.param_overrides.len(), 1);
        assert_eq!(ex.param_overrides[0].placeholder_index, 0);
        assert_eq!(ex.param_overrides[0].value, ExampleValue::Literal("99"));
    }

    #[test]
    fn resolve_examples_unbound_var_returns_err() {
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
        use crate::pattern::PatternBuilder;
        use readyset_sql::Dialect;

        // Build a pattern, then manually inject an Example cell that references
        // VarId(99) which does not exist in this pattern's vars.
        let mut b = PatternBuilder::new("p");
        let t = b.table();
        let c = b.column(t);
        b.column_type_class(c, TypeClass::Integer);
        b.from(t);
        b.project_column(c, t);
        let pattern = b.build();

        let mut constraints = pattern.constraints.clone();
        constraints.push(crate::constraint::Constraint::Example {
            note: "bad",
            dialect: DialectSupport::Both,
            cells: vec![ExampleCell {
                var: crate::var::VarId(99),
                value: ExampleValue::Literal("1"),
            }],
        });

        let config = crate::state::GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut gen_state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xCAFE);
        let mut entropy = Entropy::new(&mut rng);
        let err = resolve(&constraints, &pattern.vars, &mut gen_state, &mut entropy)
            .expect_err("should fail with Unbound for out-of-range var");

        assert!(
            matches!(err, ResolveError::Unbound(v) if v.0 == 99),
            "expected Unbound(VarId(99)), got {err:?}"
        );
    }

    #[test]
    fn resolve_examples_type_mismatch_for_table_var_in_cell() {
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
        use crate::pattern::PatternBuilder;
        use readyset_sql::Dialect;

        // Build a pattern, then manually inject an Example cell pointing at a
        // Relation var (not Column or Param) to trigger TypeMismatch.
        let mut b = PatternBuilder::new("p");
        let t = b.table();
        let c = b.column(t);
        b.column_type_class(c, TypeClass::Integer);
        b.from(t);
        b.project_column(c, t);
        let pattern = b.build();

        let mut constraints = pattern.constraints.clone();
        // t is VarId(0) which has VarKind::Relation — invalid for Example cells.
        constraints.push(crate::constraint::Constraint::Example {
            note: "bad-kind",
            dialect: DialectSupport::Both,
            cells: vec![ExampleCell {
                var: t,
                value: ExampleValue::Literal("1"),
            }],
        });

        let config = crate::state::GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut gen_state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xCAFE);
        let mut entropy = Entropy::new(&mut rng);
        let err = resolve(&constraints, &pattern.vars, &mut gen_state, &mut entropy)
            .expect_err("should fail with TypeMismatch for Relation var in Example cell");

        assert!(
            matches!(err, ResolveError::TypeMismatch { .. }),
            "expected TypeMismatch, got {err:?}"
        );
    }

    #[test]
    fn two_where_params_assigned_distinct_placeholder_indices() {
        // Two WhereParam vars must map to placeholder indices 0 and 1.
        // Verified end-to-end through resolve_examples param_overrides.
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
        use crate::pattern::PatternBuilder;
        use readyset_sql::Dialect;
        use readyset_sql::ast::BinaryOperator;

        let mut b = PatternBuilder::new("two_params");
        let t = b.table();
        let c = b.column(t);
        b.column_type_class(c, TypeClass::Integer);
        b.from(t);
        b.project_column(c, t);
        let p0 = b.where_param(c, t, BinaryOperator::Greater);
        let p1 = b.where_param(c, t, BinaryOperator::Less);
        b.example(
            "range",
            DialectSupport::Both,
            vec![
                ExampleCell {
                    var: p0,
                    value: ExampleValue::Literal("10"),
                },
                ExampleCell {
                    var: p1,
                    value: ExampleValue::Literal("20"),
                },
            ],
        );
        let pattern = b.build();

        let recipe = pattern.to_recipe(0);
        let config = crate::state::GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut gen_state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xBEEF);
        let mut entropy = Entropy::new(&mut rng);
        let resolved = resolve(
            &recipe.constraints,
            &recipe.var_kinds,
            &mut gen_state,
            &mut entropy,
        )
        .unwrap();

        assert_eq!(resolved.params.len(), 2, "expected two params");
        assert_eq!(resolved.examples.len(), 1);
        let ex = &resolved.examples[0];
        assert_eq!(ex.param_overrides.len(), 2, "expected two param overrides");
        let mut indices: Vec<usize> = ex
            .param_overrides
            .iter()
            .map(|po| po.placeholder_index)
            .collect();
        indices.sort();
        assert_eq!(indices, vec![0, 1], "placeholder indices must be 0 and 1");
    }

    #[test]
    fn multiple_try_resolve_cycles() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        // First try: succeeds
        let p1 = make_single_table_pattern();
        let r1 = p1.to_recipe(0);

        let state_cp1 = state.checkpoint();
        let result1 = try_resolve(&r1, &mut state, &mut entropy);
        assert!(result1.is_ok());
        // State now has 1 table
        assert_eq!(state.tables().len(), 1);

        // Second try: also succeeds
        let p2 = make_param_pattern();
        let r2 = p2.to_recipe(0);

        let state_cp2 = state.checkpoint();
        let result2 = try_resolve(&r2, &mut state, &mut entropy);
        assert!(result2.is_ok());
        // State now has 2 tables (fresh synthesis, reuse_preference=0)
        assert_eq!(state.tables().len(), 2);

        // Roll back second try
        state.restore(state_cp2);
        assert_eq!(state.tables().len(), 1);

        // Roll back first try
        state.restore(state_cp1);
        assert_eq!(state.tables().len(), 0);
    }

    /// Collect column-name pairs that PostgreSQL will compare with `=`:
    /// JOIN ON equalities, correlated WHERE `col = col`, and IN-subquery
    /// (lhs col, subquery's first projected col). Recurses into CTEs and
    /// nested subqueries. Column names only -- callers map names to types
    /// in controlled single-base-table scenarios.
    fn eq_operand_column_pairs(stmt: &SelectStatement) -> Vec<(SqlIdentifier, SqlIdentifier)> {
        let mut pairs = Vec::new();
        collect_from_select(stmt, &mut pairs);
        pairs
    }

    fn first_projected_column(stmt: &SelectStatement) -> Option<SqlIdentifier> {
        stmt.fields.iter().find_map(|f| match f {
            FieldDefinitionExpr::Expr {
                expr: Expr::Column(c),
                ..
            } => Some(c.name.clone()),
            _ => None,
        })
    }

    fn collect_from_select(stmt: &SelectStatement, out: &mut Vec<(SqlIdentifier, SqlIdentifier)>) {
        for cte in &stmt.ctes {
            collect_from_select(&cte.statement, out);
        }
        for j in &stmt.join {
            if let JoinConstraint::On(e) = &j.constraint {
                collect_from_expr(e, out);
            }
        }
        if let Some(w) = &stmt.where_clause {
            collect_from_expr(w, out);
        }
    }

    fn collect_from_expr(e: &Expr, out: &mut Vec<(SqlIdentifier, SqlIdentifier)>) {
        match e {
            Expr::BinaryOp { lhs, op, rhs } => {
                if *op == BinaryOperator::Equal
                    && let (Expr::Column(l), Expr::Column(r)) = (lhs.as_ref(), rhs.as_ref())
                {
                    out.push((l.name.clone(), r.name.clone()));
                }
                collect_from_expr(lhs, out);
                collect_from_expr(rhs, out);
            }
            Expr::In {
                lhs,
                rhs: InValue::Subquery(sub),
                ..
            } => {
                if let Expr::Column(l) = lhs.as_ref()
                    && let Some(rc) = first_projected_column(sub)
                {
                    out.push((l.name.clone(), rc));
                }
                collect_from_select(sub, out);
            }
            Expr::Exists(sub) | Expr::NestedSelect(sub) => collect_from_select(sub, out),
            _ => {}
        }
    }

    /// Resolve a pattern against a pre-seeded single-table state with forced
    /// reuse and assert every `=` operand pair references columns of
    /// PG-compatible type. All columns resolve to `table`'s schema by name.
    fn assert_pattern_operand_types_compatible(
        pattern: &crate::pattern::Pattern,
        table: &str,
        columns: &[(&str, SqlType)],
        seeds: std::ops::Range<u64>,
    ) {
        for seed in seeds {
            let config = crate::state::GeneratorConfig {
                reuse_preference: 1.0,
                ..Default::default()
            };
            let mut state = GenerationState::new(Dialect::PostgreSQL, config);
            let mut ts = TableSchema::new(SqlIdentifier::from(table));
            for (name, ty) in columns {
                ts.add_column(
                    SqlIdentifier::from(*name),
                    ColumnMeta {
                        sql_type: ty.clone(),
                        gen_spec: ColumnGenerationSpec::Random,
                    },
                );
            }
            ts.primary_key = Some(SqlIdentifier::from(columns[0].0));
            state.add_table(ts);

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut entropy = Entropy::new(&mut rng);
            let output = resolve(
                &pattern.constraints,
                &pattern.vars,
                &mut state,
                &mut entropy,
            )
            .expect("should resolve");

            let stmt = match &output.query {
                SelectSpecification::Simple(s) => s.clone(),
                SelectSpecification::Compound(_) => continue,
            };
            let type_of = |n: &SqlIdentifier| -> SqlType {
                state
                    .table(&SqlIdentifier::from(table))
                    .and_then(|t| t.columns.iter().find(|(cn, _)| *cn == n))
                    .map(|(_, m)| m.sql_type.clone())
                    .unwrap_or_else(|| {
                        panic!(
                            "seed {seed}: unknown column {n} in {}",
                            output.query.display(Dialect::PostgreSQL)
                        )
                    })
            };
            for (a, b) in eq_operand_column_pairs(&stmt) {
                let (ta, tb) = (type_of(&a), type_of(&b));
                assert!(
                    crate::resolver::schema::types_compatible_pub(&ta, &tb),
                    "seed {seed}: {a}({ta:?}) = {b}({tb:?}) incompatible in: {}",
                    output.query.display(Dialect::PostgreSQL)
                );
            }
        }
    }

    #[test]
    fn cte_with_join_operands_type_compatible_under_reuse() {
        assert_pattern_operand_types_compatible(
            &crate::registry::ctes::cte_with_join(),
            "t0",
            &[
                ("c0", SqlType::Int(None)),
                ("c1", SqlType::Text),
                ("c2", SqlType::Double),
            ],
            0..96,
        );
    }

    #[test]
    fn in_subquery_operands_type_compatible_under_reuse() {
        assert_pattern_operand_types_compatible(
            &crate::registry::subqueries::in_subquery(),
            "t0",
            &[
                ("c0", SqlType::Int(None)),
                ("c1", SqlType::Text),
                ("c2", SqlType::Double),
            ],
            0..96,
        );
    }

    #[test]
    fn exists_subquery_operands_type_compatible_under_reuse() {
        assert_pattern_operand_types_compatible(
            &crate::registry::subqueries::exists_subquery(),
            "t0",
            &[
                ("c0", SqlType::Int(None)),
                ("c1", SqlType::Text),
                ("c2", SqlType::Double),
            ],
            0..96,
        );
    }
}
