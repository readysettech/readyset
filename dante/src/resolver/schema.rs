use data_generator::ColumnGenerationSpec;
use readyset_sql::Dialect;
use readyset_sql::ast::SqlType;

use super::{Binding, DdlStep, Env, ResolveError};
use crate::constraint::{Constraint, TypeClass};
use crate::entropy::Entropy;
use crate::state::{ColumnMeta, GenerationState, TableSchema};
use crate::var::{VarId, VarKind};

/// Resolve schema constraints, binding variables to concrete schema elements.
///
/// This implements Phases 1 and 2 of the 4-phase resolver algorithm:
/// classification/ordering and variable binding.
///
/// Returns the resolved environment and the "expanded" constraint list —
/// the original constraints with each `Or` replaced by the winning branch's
/// constraints, so that `build_select` sees the structural output.
pub(crate) fn resolve_schema(
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> Result<(Env, Vec<Constraint>), ResolveError> {
    resolve_schema_with_outer(constraints, var_kinds, state, entropy, None)
}

/// Like [`resolve_schema`], but with an optional parent `outer_env` whose
/// bindings are visible to TypeCompatible anchoring and verification. Used by
/// subquery resolution so a `TypeCompatible` linking an inner column to an
/// outer one is enforced even though the two columns bind in separate passes.
pub(crate) fn resolve_schema_with_outer(
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    outer_env: Option<&Env>,
) -> Result<(Env, Vec<Constraint>), ResolveError> {
    let num_vars = var_kinds.len();
    let mut env = Env::new(num_vars);
    let expanded =
        resolve_constraint_set(&mut env, constraints, var_kinds, state, entropy, outer_env)?;
    Ok((env, expanded))
}

/// Resolve a set of constraints against an existing environment.
///
/// This is the core phased resolution engine, usable both for top-level
/// resolution and recursively within Or branches.  Returns the "expanded"
/// constraint list: the input with Or constraints replaced by whichever
/// branch was selected.
fn resolve_constraint_set(
    env: &mut Env,
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    outer_env: Option<&Env>,
) -> Result<Vec<Constraint>, ResolveError> {
    // Phase 1: Classify and order constraints
    let mut table_exists = Vec::new();
    let mut alias_of = Vec::new();
    let mut eq_constraints = Vec::new();
    let mut column_exists = Vec::new();
    let mut type_constraints = Vec::new();
    let mut not_eq_constraints = Vec::new();
    let mut or_constraints = Vec::new();
    let mut structural = Vec::new();

    for c in constraints {
        match c {
            Constraint::BaseTable(_) => table_exists.push(c),
            Constraint::AliasOf { .. } => alias_of.push(c),
            Constraint::Eq(_, _) => eq_constraints.push(c),
            Constraint::ColumnExists { .. } => column_exists.push(c),
            Constraint::ColumnTypeClass { .. } | Constraint::TypeCompatible(_, _) => {
                type_constraints.push(c)
            }
            Constraint::NotEq(_, _) => not_eq_constraints.push(c),
            Constraint::Or(_, _) => or_constraints.push(c),
            // Structural constraints (From, ProjectColumn, Join, …) are
            // handled by ast_builder, not the schema phase.
            _ => structural.push(c),
        }
    }

    // Phase 2: Resolve in order

    // 2a: Process Eq constraints first (unification)
    for c in &eq_constraints {
        if let Constraint::Eq(a, b) = c {
            resolve_eq(env, *a, *b, var_kinds)?;
        }
    }

    // 2b: Resolve BaseTable
    for c in &table_exists {
        if let Constraint::BaseTable(t) = c {
            resolve_table_exists(env, *t, state, entropy)?;
        }
    }

    // 2b2: Resolve AliasOf (after BaseTable so original is bound)
    for c in &alias_of {
        if let Constraint::AliasOf { alias, original } = c {
            resolve_alias_of(env, *alias, *original, state)?;
        }
    }

    // 2c: Resolve ColumnExists
    for c in &column_exists {
        if let Constraint::ColumnExists { col, table } = c {
            resolve_column_exists(
                env,
                *col,
                *table,
                &type_constraints,
                constraints,
                state,
                entropy,
                outer_env,
            )?;
        }
    }

    // 2d: Process type constraints (verify/apply)
    for c in &type_constraints {
        match c {
            Constraint::ColumnTypeClass { col, type_class } => {
                resolve_type_class(env, *col, type_class)?;
            }
            Constraint::TypeCompatible(a, b) => {
                resolve_type_compatible(env, *a, *b, outer_env)?;
            }
            _ => {}
        }
    }

    // 2e: Process NotEq constraints (verify)
    for c in &not_eq_constraints {
        if let Constraint::NotEq(a, b) = c {
            resolve_not_eq(env, *a, *b)?;
        }
    }

    // 2f: Process Or constraints via backtracking. Try branch_a; on failure,
    // restore env/state and try branch_b. The winning branch replaces the Or.
    let mut expanded: Vec<Constraint> = constraints
        .iter()
        .filter(|c| !matches!(c, Constraint::Or(..)))
        .cloned()
        .collect();
    for c in &or_constraints {
        if let Constraint::Or(branch_a, branch_b) = c {
            let env_cp = env.checkpoint();
            let state_cp = state.checkpoint();
            let entropy_cp = entropy.checkpoint();
            let branch =
                match resolve_constraint_set(env, branch_a, var_kinds, state, entropy, outer_env) {
                    Ok(b) => b,
                    Err(_) => {
                        env.restore(env_cp);
                        state.restore(state_cp);
                        entropy.restore(entropy_cp);
                        resolve_constraint_set(env, branch_b, var_kinds, state, entropy, outer_env)?
                    }
                };
            entropy.release();
            expanded.extend(branch);
        }
    }
    Ok(expanded)
}

/// Process an Eq constraint: unify the two vars in the union-find and
/// propagate any pre-existing binding to the merged representative. If
/// both sides are already bound to *different* tables, we report it as a
/// `TypeMismatch` so the generator's retry loop discards the pattern.
fn resolve_eq(
    env: &mut Env,
    a: VarId,
    b: VarId,
    var_kinds: &[VarKind],
) -> Result<(), ResolveError> {
    // If both are already bound, verify they're compatible
    let rep_a = env.union_find.find(a.0);
    let rep_b = env.union_find.find(b.0);
    if rep_a == rep_b {
        return Ok(()); // already unified
    }

    // Check if both are bound
    let a_bound = env.bindings[rep_a].is_some();
    let b_bound = env.bindings[rep_b].is_some();

    if let (Some(binding_a), Some(binding_b)) =
        (env.bindings[rep_a].as_ref(), env.bindings[rep_b].as_ref())
    {
        // Both bound -- verify compatibility
        match (binding_a, binding_b) {
            (Binding::Table { name: na, .. }, Binding::Table { name: nb, .. }) if na != nb => {
                return Err(ResolveError::TypeMismatch {
                    expected: na.to_string(),
                    actual: nb.to_string(),
                });
            }
            _ => {}
        }
    }

    env.union_find.union(a.0, b.0, var_kinds)?;

    // If one was bound and the other wasn't, the binding propagates via the representative
    let new_rep = env.union_find.find(a.0);
    if a_bound && !b_bound {
        let binding = env.bindings[rep_a].clone();
        env.bindings[new_rep] = binding;
    } else if b_bound && !a_bound {
        let binding = env.bindings[rep_b].clone();
        env.bindings[new_rep] = binding;
    }

    Ok(())
}

fn resolve_table_exists(
    env: &mut Env,
    t: VarId,
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> Result<(), ResolveError> {
    if env.is_bound(t) {
        return Ok(()); // already bound (e.g., via Eq unification)
    }

    let reuse = state.config().reuse_preference;
    let should_reuse = !state.tables().is_empty() && entropy.probability(reuse);

    let name = if should_reuse {
        state
            .pick_random_table(entropy)
            .ok_or(ResolveError::Unbound(t))?
            .name
            .clone()
    } else {
        // Synthesize a new table. Don't emit CreateTable DDL yet — columns
        // will be added during ColumnExists resolution. The final schema is
        // looked up from state in `into_ddl_steps`.
        let name = state.fresh_table_name();
        let mut schema = TableSchema::new(name.clone());
        // Always add a primary key column so the table is valid SQL even if
        // no ColumnExists constraints reference this table (e.g., in
        // project_literal patterns).
        let pk_name = schema.fresh_column_name();
        schema.add_column(
            pk_name.clone(),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        schema.primary_key = Some(pk_name);
        state.add_table(schema);
        env.new_tables.push(name.clone());
        name
    };

    // Auto-alias on collision: if any already-bound relation var is using
    // this physical name without an alias, the new binding gets a fresh
    // alias so MySQL's "Not unique table/alias" (1066) can't fire. This
    // replaces the N×M `Or(NotEq, AliasOf)` soup that `compose` used to
    // emit for cross-pattern relation pairs — single pass, no
    // backtracking.
    let alias = if env.relation_with_name_exists(&name) {
        Some(state.fresh_alias())
    } else {
        None
    };
    env.bind(t, Binding::Table { name, alias });

    Ok(())
}

/// Resolve an AliasOf constraint: bind the alias var to the same physical
/// table as the original, but with a distinct SQL alias. Also retroactively
/// assign an alias to the original if it doesn't have one yet.
fn resolve_alias_of(
    env: &mut Env,
    alias: VarId,
    original: VarId,
    state: &mut GenerationState,
) -> Result<(), ResolveError> {
    // Get the original's physical table name
    let physical_name = super::ast_builder::get_physical_table_name(env, original)?;

    // Ensure the original relation has an alias (needed so SQL references
    // are unambiguous when the same table appears twice). When the original
    // didn't already have one, retroactively assign one and rebind it.
    match env.get(original) {
        Some(Binding::Table { alias: Some(_), .. }) => {}
        Some(Binding::Table { alias: None, .. }) => {
            let a = state.fresh_alias();
            env.bind(
                original,
                Binding::Table {
                    name: physical_name.clone(),
                    alias: Some(a),
                },
            );
        }
        _ => return Err(ResolveError::Unbound(original)),
    }

    // Bind the alias variable to the same physical table with its own alias
    let alias_name = state.fresh_alias();
    env.bind(
        alias,
        Binding::Table {
            name: physical_name,
            alias: Some(alias_name),
        },
    );

    Ok(())
}

/// The role of a column, used to select an appropriate ColumnGenerationSpec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnRole {
    JoinKey,
    FilterKey,
    General,
}

/// Determine a column's role by inspecting all constraints.
fn column_role(col: VarId, constraints: &[Constraint]) -> ColumnRole {
    for c in constraints {
        match c {
            Constraint::Join {
                left_col,
                right_col,
                ..
            } if *left_col == col || *right_col == col => {
                return ColumnRole::JoinKey;
            }
            Constraint::WhereParam { col: wc, .. }
            | Constraint::WhereInParam { col: wc, .. }
            | Constraint::WhereRangeParam { col: wc, .. }
            | Constraint::WhereLike { col: wc, .. }
            | Constraint::WhereBetweenParam { col: wc, .. }
            | Constraint::WhereIsNull { col: wc, .. }
                if *wc == col =>
            {
                return ColumnRole::FilterKey;
            }
            Constraint::WhereLookupBinaryOp { lookup_col: wc, .. } if *wc == col => {
                return ColumnRole::FilterKey;
            }
            _ => {}
        }
    }
    ColumnRole::General
}

/// Choose a ColumnGenerationSpec based on column role and SQL type.
///
/// For types that don't support integer-range specs (DateTime, Bool, String),
/// falls back to `Random`. For small integer types (TinyInt), uses a reduced
/// range to avoid overflow.
fn gen_spec_for_role(
    role: ColumnRole,
    sql_type: &SqlType,
    default: &ColumnGenerationSpec,
) -> ColumnGenerationSpec {
    match role {
        ColumnRole::JoinKey | ColumnRole::FilterKey => {
            // Only integer types support Uniform/Zipfian with Int DfValues.
            // For other types, fall back to Random.
            let max = match sql_type {
                SqlType::TinyInt(_) => 100,
                SqlType::SmallInt(_) | SqlType::Int2 => 1000,
                SqlType::Int(_) | SqlType::Int4 | SqlType::BigInt(_) | SqlType::Int8 => 1000,
                _ => {
                    // DateTime, Bool, String, Float, etc. — can't use integer ranges
                    return ColumnGenerationSpec::Random;
                }
            };
            if role == ColumnRole::JoinKey {
                ColumnGenerationSpec::Uniform(
                    readyset_data::DfValue::Int(1),
                    readyset_data::DfValue::Int(max),
                )
            } else {
                ColumnGenerationSpec::Zipfian {
                    min: readyset_data::DfValue::Int(1),
                    max: readyset_data::DfValue::Int(max),
                    alpha: 1.5,
                }
            }
        }
        ColumnRole::General => default.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_column_exists(
    env: &mut Env,
    col: VarId,
    table: VarId,
    type_constraints: &[&Constraint],
    all_constraints: &[Constraint],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    outer_env: Option<&Env>,
) -> Result<(), ResolveError> {
    if env.is_bound(col) {
        return Ok(());
    }

    // Get the physical table name for schema lookups.
    let table_name = match env.get(table) {
        Some(Binding::Table { name, .. }) => name.clone(),
        _ => return Err(ResolveError::Unbound(table)),
    };

    // Find any type class constraint for this column
    let type_class = type_constraints.iter().find_map(|c| {
        if let Constraint::ColumnTypeClass {
            col: tc_col,
            type_class,
        } = c
        {
            if *tc_col == col {
                Some(type_class.clone())
            } else {
                None
            }
        } else {
            None
        }
    });

    // Compatibility anchors: bound types of TypeCompatible partners of `col`.
    // Choosing a compatible type here (rather than only verifying later)
    // enforces TypeCompatible even across scope/binding-order boundaries,
    // where the phase-2d verify would no-op on an unbound operand.
    let anchors: Vec<SqlType> = type_constraints
        .iter()
        .filter_map(|c| match c {
            Constraint::TypeCompatible(a, b) => {
                let other = if *a == col {
                    Some(*b)
                } else if *b == col {
                    Some(*a)
                } else {
                    None
                }?;
                anchor_type(env, outer_env, other)
            }
            _ => None,
        })
        .collect();

    let reuse = state.config().reuse_preference;
    let table_schema = state.table(&table_name);

    // A var that any `Constraint::Example` cell pins by literal must not
    // collapse onto the auto-allocated PK column. Two examples on the same
    // pattern can legitimately pin different literals for the same logical
    // variable (different `param_overrides` exercising the same row); if
    // that variable resolves to the PK column the seed-data INSERT then
    // rejects with `Duplicate entry` and the run aborts. Pattern authors
    // never intend to pin a PK literal -- they're naming a semantic var
    // ("the integer operand"), and the resolver chose to map it onto the
    // PK only because it happened to match the Int type class.
    let example_pinned = all_constraints.iter().any(
        |c| matches!(c, Constraint::Example { cells, .. } if cells.iter().any(|x| x.var == col)),
    );

    let existing_col = table_schema.and_then(|ts| {
        if ts.columns.is_empty() || !entropy.probability(reuse) {
            return None;
        }
        // Find columns matching the type class
        let matching: Vec<_> = ts
            .columns
            .iter()
            .filter(|(_, meta)| match &type_class {
                Some(tc) => type_matches(&meta.sql_type, tc),
                None => true,
            })
            .filter(|(_, meta)| anchors.iter().all(|a| types_compatible(&meta.sql_type, a)))
            .filter(|(name, _)| {
                if example_pinned {
                    // Drop the PK from candidates so an example-pinned
                    // literal never lands on the PRIMARY KEY column.
                    ts.primary_key.as_ref().is_none_or(|pk| pk != *name)
                } else {
                    true
                }
            })
            .collect();
        if matching.is_empty() {
            None
        } else {
            let idx = entropy.range(0..matching.len());
            let (name, meta) = matching[idx];
            Some((name.clone(), meta.sql_type.clone()))
        }
    });

    if let Some((col_name, sql_type)) = existing_col {
        env.bind(
            col,
            Binding::Column {
                name: col_name,
                sql_type,
                table_var: table,
            },
        );
    } else {
        // Synthesize a new column
        let sql_type = if let Some(anchor) = anchors.first() {
            // Adopt a concrete type compatible with the anchor. If a type class
            // is also present and the anchor does not satisfy it, fall back to
            // the class and let the phase-2d verify reject any residual clash.
            match &type_class {
                Some(tc) if !tc.matches(anchor) => {
                    pick_type_for_class(tc, entropy, state.dialect())
                }
                _ => anchor.clone(),
            }
        } else {
            match &type_class {
                Some(tc) => pick_type_for_class(tc, entropy, state.dialect()),
                None => pick_random_type(entropy, state.dialect()),
            }
        };
        let role = column_role(col, all_constraints);
        let gen_spec = gen_spec_for_role(role, &sql_type, &state.config().default_gen_spec);

        let table_schema = state
            .table_mut(&table_name)
            .ok_or(ResolveError::Unbound(table))?;
        let col_name = table_schema.fresh_column_name();

        let meta = ColumnMeta {
            sql_type: sql_type.clone(),
            gen_spec: gen_spec.clone(),
        };

        table_schema.add_column(col_name.clone(), meta.clone());

        // Only emit AddColumn DDL for pre-existing tables. Columns on newly
        // created tables are included in the CreateTable DDL via into_ddl_steps.
        if !env.new_tables.contains(&table_name) {
            env.ddl_steps.push(DdlStep::AddColumn {
                table: table_name.clone(),
                column_name: col_name.clone(),
                meta,
            });
        }

        env.bind(
            col,
            Binding::Column {
                name: col_name,
                sql_type,
                table_var: table,
            },
        );
    }

    Ok(())
}

fn resolve_type_class(
    env: &mut Env,
    col: VarId,
    type_class: &TypeClass,
) -> Result<(), ResolveError> {
    match env.get(col) {
        Some(Binding::Column { sql_type, .. }) => {
            if !type_matches(sql_type, type_class) {
                return Err(ResolveError::TypeMismatch {
                    expected: format!("{type_class:?}"),
                    actual: format!("{sql_type:?}"),
                });
            }
            Ok(())
        }
        Some(_) => Ok(()), // not a column binding, skip
        None => Ok(()),    // not yet bound, will be checked during column resolution
    }
}

/// The bound SQL type of `v`, looked up in `env` and then, if present, in a
/// parent `outer_env`. Used to find TypeCompatible "anchors" so column binding
/// can choose a compatible type up front instead of relying on a post-hoc
/// verify that silently skips when an operand is unbound in the current env.
fn anchor_type(env: &Env, outer_env: Option<&Env>, v: VarId) -> Option<SqlType> {
    if v.0 < env.bindings.len()
        && let Some(Binding::Column { sql_type, .. }) = env.get(v)
    {
        return Some(sql_type.clone());
    }
    if let Some(oe) = outer_env
        && v.0 < oe.bindings.len()
        && let Some(Binding::Column { sql_type, .. }) = oe.get(v)
    {
        return Some(sql_type.clone());
    }
    None
}

fn resolve_type_compatible(
    env: &mut Env,
    a: VarId,
    b: VarId,
    outer_env: Option<&Env>,
) -> Result<(), ResolveError> {
    let type_a = anchor_type(env, outer_env, a);
    let type_b = anchor_type(env, outer_env, b);

    if let (Some(ta), Some(tb)) = (type_a, type_b)
        && !types_compatible(&ta, &tb)
    {
        return Err(ResolveError::TypeMismatch {
            expected: format!("{ta:?}"),
            actual: format!("{tb:?}"),
        });
    }
    Ok(())
}

fn resolve_not_eq(env: &mut Env, a: VarId, b: VarId) -> Result<(), ResolveError> {
    let rep_a = env.union_find.find(a.0);
    let rep_b = env.union_find.find(b.0);

    if rep_a == rep_b {
        // Already unified -- NotEq violated
        let binding_desc = match &env.bindings[rep_a] {
            Some(Binding::Table { name, .. } | Binding::Column { name, .. }) => name.to_string(),
            None => "<unbound but unified>".to_string(),
        };
        return Err(ResolveError::NotEqViolation(binding_desc));
    }

    // Also check if both are bound to the same effective SQL identifier
    // (e.g., two table variables that both reused the same physical table
    // *and* neither got aliased). When auto-aliasing kicks in, two
    // relations with the same physical name but distinct aliases are
    // SQL-distinct and pass.
    if let (Some(binding_a), Some(binding_b)) = (&env.bindings[rep_a], &env.bindings[rep_b]) {
        let name_a = match binding_a {
            Binding::Table { name, alias } => alias.as_ref().unwrap_or(name),
            Binding::Column { name, .. } => name,
        };
        let name_b = match binding_b {
            Binding::Table { name, alias } => alias.as_ref().unwrap_or(name),
            Binding::Column { name, .. } => name,
        };
        if name_a == name_b {
            return Err(ResolveError::NotEqViolation(name_a.to_string()));
        }
    }

    // Note: NotEq with both vars unbound at this point is a no-op. Both
    // schema-phase and the eager check above already cover the cases where
    // we can detect a violation. A deferred re-check after later binding
    // would require storing `(VarId, VarId)` and re-resolving via `find` at
    // check time; today's resolver binds tables before NotEq runs, so the
    // case isn't reachable.
    let _ = (rep_a, rep_b);
    Ok(())
}

/// Check if a SQL type matches a type class.
fn type_matches(sql_type: &SqlType, type_class: &TypeClass) -> bool {
    type_class.matches(sql_type)
}

/// Test/crate-visible wrapper around the private `types_compatible`.
#[cfg(test)]
pub(crate) fn types_compatible_pub(a: &SqlType, b: &SqlType) -> bool {
    types_compatible(a, b)
}

/// Check if two SQL types are compatible (can be compared/joined).
fn types_compatible(a: &SqlType, b: &SqlType) -> bool {
    if a == b {
        return true;
    }
    // Numeric types are compatible with each other
    let a_numeric = is_numeric(a);
    let b_numeric = is_numeric(b);
    if a_numeric && b_numeric {
        return true;
    }
    // String types are compatible with each other
    let a_string = is_string(a);
    let b_string = is_string(b);
    if a_string && b_string {
        return true;
    }
    false
}

fn is_numeric(t: &SqlType) -> bool {
    type_matches(t, &TypeClass::Numeric)
}

fn is_string(t: &SqlType) -> bool {
    type_matches(t, &TypeClass::String)
}

/// Pick a concrete SQL type for a type class.
///
/// `dialect` controls which physical types are reachable: PostgreSQL uses
/// `TIMESTAMP` where MySQL uses `DATETIME`, so the DateTime slot swaps to
/// `SqlType::Timestamp` under PG. Without this, ALTER TABLE / CREATE TABLE
/// emits `DATETIME` against Postgres and the upstream errors out (PG has
/// no `DATETIME` type).
fn pick_type_for_class(tc: &TypeClass, entropy: &mut Entropy<'_>, dialect: Dialect) -> SqlType {
    match tc {
        TypeClass::Any => pick_random_type(entropy, dialect),
        TypeClass::Integer => entropy
            .choose(&[SqlType::Int(None), SqlType::BigInt(None)])
            .cloned()
            .expect("integer type slice is non-empty"),
        // Spans the integer and approximate/exact decimal types so a
        // `Numeric`-constrained column exercises the float/decimal coercion
        // paths the expr_eval families target, not just int/double. The slice
        // stays a subset of `TypeClass::Numeric::matches`: that predicate is
        // intentionally broader so a *reused* column of a type the synthesizer
        // doesn't mint (e.g. MySQL TinyInt) still satisfies the class.
        TypeClass::Numeric => entropy
            .choose(&[
                SqlType::Int(None),
                SqlType::BigInt(None),
                SqlType::SmallInt(None),
                SqlType::Double,
                SqlType::Float,
                SqlType::Real,
                SqlType::Decimal(10, 2),
            ])
            .cloned()
            .expect("numeric type slice is non-empty"),
        TypeClass::Decimal => entropy
            .choose(&[SqlType::Double, SqlType::Float])
            .cloned()
            .expect("decimal type slice is non-empty"),
        // Exact fixed-point only — never FLOAT/DOUBLE, so e.g. round(x, scale)
        // stays valid on PostgreSQL.
        TypeClass::FixedPoint => SqlType::Decimal(10, 2),
        TypeClass::String => entropy
            .choose(&[SqlType::VarChar(Some(255)), SqlType::Text])
            .cloned()
            .expect("string type slice is non-empty"),
        TypeClass::DateTime => entropy
            .choose(&[datetime_for(dialect), SqlType::Date])
            .cloned()
            .expect("datetime type slice is non-empty"),
        TypeClass::Orderable => {
            let class = entropy
                .choose(&[TypeClass::Numeric, TypeClass::String, TypeClass::DateTime])
                .cloned()
                .expect("orderable class slice is non-empty");
            pick_type_for_class(&class, entropy, dialect)
        }
        TypeClass::Exact(t) => t.clone(),
    }
}

/// Dialect-appropriate "datetime" sql type: `TIMESTAMP` for PG, `DATETIME`
/// for MySQL. Both decode to `Value::DateTime` upstream-side per the
/// `mmjjsbptsxwu` work, so cross-dialect comparison stays well-defined.
fn datetime_for(dialect: Dialect) -> SqlType {
    match dialect {
        Dialect::PostgreSQL => SqlType::Timestamp,
        Dialect::MySQL => SqlType::DateTime(None),
    }
}

/// Pick a random SQL type.
///
/// Includes all types that the data generator can produce valid values for.
/// gen_spec_for_role handles type-appropriate ranges for small/non-integer types.
///
/// `dialect` swaps `DATETIME` for `TIMESTAMP` under PostgreSQL — see
/// `pick_type_for_class` for rationale.
pub(crate) fn pick_random_type(entropy: &mut Entropy<'_>, dialect: Dialect) -> SqlType {
    entropy
        .choose(&[
            SqlType::Int(None),
            SqlType::BigInt(None),
            SqlType::SmallInt(None),
            SqlType::VarChar(Some(255)),
            SqlType::Text,
            SqlType::Double,
            datetime_for(dialect),
            SqlType::Date,
            SqlType::Bool,
        ])
        .cloned()
        .expect("random type slice is non-empty")
}

#[cfg(test)]
mod tests {
    use data_generator::ColumnGenerationSpec;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::ast::{SqlIdentifier, SqlType};
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;
    use crate::constraint::{Constraint, TypeClass};
    use crate::entropy::Entropy;
    use crate::resolver::{Binding, DdlStep};
    use crate::state::{ColumnMeta, GeneratorConfig, TableSchema};
    use crate::var::{VarId, VarKind};

    fn test_env(dialect: Dialect) -> (GenerationState, SmallRng) {
        let config = GeneratorConfig {
            reuse_preference: 0.0, // always synthesize
            ..Default::default()
        };
        let state = GenerationState::new(dialect, config);
        let rng = SmallRng::seed_from_u64(42);
        (state, rng)
    }

    #[test]
    fn resolve_table_exists_creates_table() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let constraints = vec![Constraint::BaseTable(VarId(0))];
        let var_kinds = vec![VarKind::Relation];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Variable should be bound to a table
        let binding = env.get(VarId(0)).expect("should be bound");
        assert!(matches!(binding, Binding::Table { .. }));

        // New table should have been tracked
        assert_eq!(env.new_tables.len(), 1);
        let ddl = env.into_ddl_steps(&state).expect("ddl should build");
        assert_eq!(ddl.len(), 1);
        assert!(matches!(ddl[0], DdlStep::CreateTable { .. }));

        // Table should exist in state
        assert_eq!(state.tables().len(), 1);
    }

    #[test]
    fn resolve_table_exists_reuses_existing() {
        let config = GeneratorConfig {
            reuse_preference: 1.0, // always reuse
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);

        // Add an existing table
        let mut existing = TableSchema::new(SqlIdentifier::from("users"));
        existing.add_column(
            SqlIdentifier::from("id"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        state.add_table(existing);

        let mut entropy = Entropy::new(&mut rng);

        let constraints = vec![Constraint::BaseTable(VarId(0))];
        let var_kinds = vec![VarKind::Relation];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Should reuse existing table
        let binding = env.get(VarId(0)).expect("should be bound");
        if let Binding::Table { name, .. } = binding {
            assert_eq!(*name, "users");
        } else {
            panic!("expected Table binding");
        }

        // No DDL should have been emitted
        assert_eq!(env.ddl_steps().len(), 0);
    }

    #[test]
    fn resolve_column_exists_on_existing_table() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);

        // Add a table with a column
        let mut ts = TableSchema::new(SqlIdentifier::from("t0"));
        ts.add_column(
            SqlIdentifier::from("id"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        state.add_table(ts);

        let mut entropy = Entropy::new(&mut rng);

        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        // reuse_preference = 0 means we'll synthesize, so the column will be new
        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Column should be bound
        let binding = env.get(c).expect("should be bound");
        assert!(matches!(binding, Binding::Column { .. }));
    }

    #[test]
    fn resolve_column_with_type_class() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::ColumnTypeClass {
                col: c,
                type_class: TypeClass::Integer,
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Column should be bound with an integer type
        let binding = env.get(c).expect("should be bound");
        if let Binding::Column { sql_type, .. } = binding {
            assert!(
                type_matches(sql_type, &TypeClass::Integer),
                "expected integer type, got {sql_type:?}"
            );
        } else {
            panic!("expected Column binding");
        }
    }

    #[test]
    fn resolve_eq_unifies_tables() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t1 = VarId(0);
        let t2 = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::Eq(t1, t2),
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Relation];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Both should be bound to the same table
        let b1 = env.get(t1).expect("t1 bound").clone();
        let b2 = env.get(t2).expect("t2 bound").clone();
        if let (Binding::Table { name: n1, .. }, Binding::Table { name: n2, .. }) = (&b1, &b2) {
            assert_eq!(n1, n2, "Eq should make both map to same table");
        }
    }

    #[test]
    fn resolve_not_eq_different_tables() {
        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let t1 = VarId(0);
        let t2 = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::NotEq(t1, t2),
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Relation];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // NotEq should succeed because both are different synthesized tables
        // (the assertion that they really are different is implicit in
        // resolve_schema not erroring; the eager check in resolve_not_eq
        // would have rejected if both bound to the same name).
        let _ = env;
    }

    #[test]
    fn resolve_not_eq_same_table_fails() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t1 = VarId(0);
        let t2 = VarId(1);
        // Eq then NotEq should fail
        let constraints = vec![
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::Eq(t1, t2),
            Constraint::NotEq(t1, t2),
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Relation];

        let result = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ResolveError::NotEqViolation(_)
        ));
    }

    #[test]
    fn resolve_full_recipe() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::ColumnTypeClass {
                col: c1,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c2,
                type_class: TypeClass::String,
            },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::ProjectColumn { col: c2, table: t },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // All three variables should be bound
        assert!(env.is_bound(t));
        assert!(env.is_bound(c1));
        assert!(env.is_bound(c2));

        // c1 should be integer, c2 should be string
        if let Some(Binding::Column { sql_type, .. }) = env.get(c1) {
            assert!(type_matches(sql_type, &TypeClass::Integer));
        }
        if let Some(Binding::Column { sql_type, .. }) = env.get(c2) {
            assert!(type_matches(sql_type, &TypeClass::String));
        }
    }

    #[test]
    fn type_matches_any() {
        assert!(type_matches(&SqlType::Int(None), &TypeClass::Any));
        assert!(type_matches(&SqlType::Text, &TypeClass::Any));
        assert!(type_matches(&SqlType::DateTime(None), &TypeClass::Any));
    }

    #[test]
    fn type_matches_integer() {
        assert!(type_matches(&SqlType::Int(None), &TypeClass::Integer));
        assert!(type_matches(&SqlType::BigInt(None), &TypeClass::Integer));
        assert!(!type_matches(&SqlType::Text, &TypeClass::Integer));
        assert!(!type_matches(&SqlType::Double, &TypeClass::Integer));
    }

    #[test]
    fn type_matches_numeric_includes_integer() {
        assert!(type_matches(&SqlType::Int(None), &TypeClass::Numeric));
        assert!(type_matches(&SqlType::Double, &TypeClass::Numeric));
        assert!(!type_matches(&SqlType::Text, &TypeClass::Numeric));
    }

    #[test]
    fn types_compatible_same() {
        assert!(types_compatible(&SqlType::Int(None), &SqlType::Int(None)));
    }

    #[test]
    fn types_compatible_numeric_cross() {
        assert!(types_compatible(
            &SqlType::Int(None),
            &SqlType::BigInt(None)
        ));
        assert!(types_compatible(&SqlType::Int(None), &SqlType::Double));
    }

    #[test]
    fn types_compatible_string_cross() {
        assert!(types_compatible(
            &SqlType::VarChar(Some(255)),
            &SqlType::Text
        ));
    }

    #[test]
    fn types_not_compatible_cross_class() {
        assert!(!types_compatible(&SqlType::Int(None), &SqlType::Text));
    }

    // --- Distribution selection tests ---

    #[test]
    fn synthesized_join_column_gets_uniform_spec() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t0 = VarId(0);
        let t1 = VarId(1);
        let c0 = VarId(2); // projection column
        let c1 = VarId(3); // join key on t0
        let c2 = VarId(4); // join key on t1
        let constraints = vec![
            Constraint::BaseTable(t0),
            Constraint::BaseTable(t1),
            Constraint::ColumnExists { col: c0, table: t0 },
            Constraint::ColumnExists { col: c1, table: t0 },
            Constraint::ColumnExists { col: c2, table: t1 },
            Constraint::ColumnTypeClass {
                col: c1,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c2,
                type_class: TypeClass::Integer,
            },
            Constraint::From(t0),
            Constraint::Join {
                operator: readyset_sql::ast::JoinOperator::InnerJoin,
                right: crate::constraint::JoinRight::Table(t1),
                left_col: c1,
                right_col: c2,
            },
            Constraint::ProjectColumn { col: c0, table: t0 },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Column { table: t0 },
            VarKind::Column { table: t0 },
            VarKind::Column { table: t1 },
        ];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Join key columns should have been synthesized with Uniform gen_spec
        let ddl = env.into_ddl_steps(&state).expect("ddl should build");

        // At least one column across all DDL should have Uniform spec
        let has_uniform = ddl.iter().any(|s| match s {
            DdlStep::CreateTable { schema, .. } => schema
                .columns
                .values()
                .any(|m| matches!(m.gen_spec, ColumnGenerationSpec::Uniform(_, _))),
            DdlStep::AddColumn { meta, .. } => {
                matches!(meta.gen_spec, ColumnGenerationSpec::Uniform(_, _))
            }
        });
        assert!(
            has_uniform,
            "expected at least one join key column with Uniform spec, got: {ddl:?}",
        );
    }

    #[test]
    fn synthesized_filter_column_gets_zipfian_spec() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t = VarId(0);
        let c_proj = VarId(1);
        let c_filter = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists {
                col: c_proj,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_filter,
                table: t,
            },
            // Constrain filter column to Integer so Zipfian spec is produced
            // (non-integer types fall back to Random)
            Constraint::ColumnTypeClass {
                col: c_filter,
                type_class: TypeClass::Integer,
            },
            Constraint::From(t),
            Constraint::ProjectColumn {
                col: c_proj,
                table: t,
            },
            Constraint::WhereParam {
                col: c_filter,
                table: t,
                op: readyset_sql::ast::BinaryOperator::Equal,
                param: VarId(3),
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Filter column should have Zipfian gen_spec
        let ddl = env.into_ddl_steps(&state).expect("ddl should build");
        let has_zipfian = ddl.iter().any(|s| match s {
            DdlStep::CreateTable { schema, .. } => schema
                .columns
                .values()
                .any(|m| matches!(m.gen_spec, ColumnGenerationSpec::Zipfian { .. })),
            DdlStep::AddColumn { meta, .. } => {
                matches!(meta.gen_spec, ColumnGenerationSpec::Zipfian { .. })
            }
        });
        assert!(
            has_zipfian,
            "expected at least one filter column with Zipfian spec, got: {ddl:?}",
        );
    }

    #[test]
    fn synthesized_general_column_gets_default_spec() {
        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve");

        // Projection-only column should get default gen_spec (Random)
        let ddl = env.into_ddl_steps(&state).expect("ddl should build");
        let has_random = ddl.iter().any(|s| match s {
            DdlStep::CreateTable { schema, .. } => schema
                .columns
                .values()
                .any(|m| matches!(m.gen_spec, ColumnGenerationSpec::Random)),
            DdlStep::AddColumn { meta, .. } => {
                matches!(meta.gen_spec, ColumnGenerationSpec::Random)
            }
        });
        assert!(
            has_random,
            "expected general column with Random spec, got: {ddl:?}",
        );
    }

    #[test]
    fn column_role_detects_join_key() {
        let t0 = VarId(0);
        let t1 = VarId(1);
        let c0 = VarId(2);
        let c1 = VarId(3);
        let constraints = vec![Constraint::Join {
            operator: readyset_sql::ast::JoinOperator::InnerJoin,
            right: crate::constraint::JoinRight::Table(t1),
            left_col: c0,
            right_col: c1,
        }];

        assert_eq!(column_role(c0, &constraints), ColumnRole::JoinKey);
        assert_eq!(column_role(c1, &constraints), ColumnRole::JoinKey);
        assert_eq!(column_role(t0, &constraints), ColumnRole::General);
    }

    #[test]
    fn column_role_detects_filter_key() {
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![Constraint::WhereParam {
            col: c,
            table: t,
            op: readyset_sql::ast::BinaryOperator::Equal,
            param: VarId(2),
        }];

        assert_eq!(column_role(c, &constraints), ColumnRole::FilterKey);
        assert_eq!(column_role(t, &constraints), ColumnRole::General);
    }

    #[test]
    fn column_role_detects_where_lookup_as_filter_key() {
        let t = VarId(0);
        let lookup = VarId(1);
        let c1 = VarId(2);
        let c2 = VarId(3);
        let constraints = vec![Constraint::WhereLookupBinaryOp {
            lookup_col: lookup,
            lookup_table: t,
            cmp: readyset_sql::ast::BinaryOperator::Equal,
            left_col: c1,
            left_table: t,
            op: readyset_sql::ast::BinaryOperator::Divide,
            right_col: c2,
            right_table: t,
        }];

        assert_eq!(column_role(lookup, &constraints), ColumnRole::FilterKey);
        assert_eq!(column_role(c1, &constraints), ColumnRole::General);
        assert_eq!(column_role(c2, &constraints), ColumnRole::General);
    }

    #[test]
    fn pick_type_for_class_numeric_stays_within_matches_and_reaches_decimal() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // Every synthesized Numeric type must satisfy the class predicate, and
        // over many draws the decimal/fixed-point types must be reachable so
        // the float/decimal coercion paths actually get exercised.
        let mut saw_decimal = false;
        for _ in 0..400 {
            let t = pick_type_for_class(&TypeClass::Numeric, &mut entropy, Dialect::MySQL);
            assert!(
                type_matches(&t, &TypeClass::Numeric),
                "pick_type_for_class(Numeric) returned a non-numeric type: {t:?}"
            );
            if matches!(t, SqlType::Decimal(_, _) | SqlType::Real) {
                saw_decimal = true;
            }
        }
        assert!(
            saw_decimal,
            "pick_type_for_class(Numeric) should sometimes return a decimal/real type"
        );
    }

    #[test]
    fn pick_type_for_class_datetime_returns_datetime_type() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let sql_type = pick_type_for_class(&TypeClass::DateTime, &mut entropy, Dialect::MySQL);
        assert!(
            type_matches(&sql_type, &TypeClass::DateTime),
            "expected DateTime type, got: {sql_type:?}"
        );
    }

    #[test]
    fn pick_random_type_can_return_datetime() {
        // Run many iterations to check that DateTime types can appear
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let mut has_datetime = false;
        for _ in 0..200 {
            let t = pick_random_type(&mut entropy, Dialect::MySQL);
            if type_matches(&t, &TypeClass::DateTime) {
                has_datetime = true;
                break;
            }
        }
        assert!(
            has_datetime,
            "pick_random_type should sometimes return DateTime types"
        );
    }

    #[test]
    fn pick_random_type_postgres_never_returns_datetime() {
        // Postgres rejects MySQL's DATETIME type (it uses TIMESTAMP). The
        // generator must swap DateTime → Timestamp under PostgreSQL dialect
        // so DDL is dialect-valid; otherwise ALTER TABLE / CREATE TABLE
        // bombs on first emission of a datetime column.
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        for _ in 0..500 {
            let t = pick_random_type(&mut entropy, Dialect::PostgreSQL);
            assert!(
                !matches!(t, SqlType::DateTime(_)),
                "PostgreSQL pick_random_type must not return DATETIME, got: {t:?}"
            );
        }
    }

    #[test]
    fn pick_random_type_postgres_can_return_timestamp() {
        // The DateTime slot should be filled by Timestamp under PG, not
        // dropped — otherwise we lose datetime coverage entirely.
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        let mut has_timestamp = false;
        for _ in 0..500 {
            if matches!(
                pick_random_type(&mut entropy, Dialect::PostgreSQL),
                SqlType::Timestamp
            ) {
                has_timestamp = true;
                break;
            }
        }
        assert!(
            has_timestamp,
            "PostgreSQL pick_random_type should sometimes return TIMESTAMP"
        );
    }

    #[test]
    fn pick_type_for_class_datetime_postgres_never_returns_mysql_datetime() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        for _ in 0..200 {
            let t = pick_type_for_class(&TypeClass::DateTime, &mut entropy, Dialect::PostgreSQL);
            assert!(
                !matches!(t, SqlType::DateTime(_)),
                "PostgreSQL pick_type_for_class(DateTime) must not return DATETIME, got: {t:?}"
            );
        }
    }

    #[test]
    fn pick_random_type_can_return_small_int() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let mut has_smallint = false;
        for _ in 0..200 {
            let t = pick_random_type(&mut entropy, Dialect::MySQL);
            if matches!(t, SqlType::SmallInt(_)) {
                has_smallint = true;
                break;
            }
        }
        assert!(
            has_smallint,
            "pick_random_type should sometimes return SmallInt"
        );
    }

    #[test]
    fn gen_spec_for_role_smallint_join_key_fits_range() {
        let spec = gen_spec_for_role(
            ColumnRole::JoinKey,
            &SqlType::SmallInt(None),
            &ColumnGenerationSpec::Random,
        );
        // SmallInt max is 32767; the uniform range should fit
        match &spec {
            ColumnGenerationSpec::Uniform(min, max) => {
                if let (readyset_data::DfValue::Int(lo), readyset_data::DfValue::Int(hi)) =
                    (min, max)
                {
                    assert!(
                        *hi <= 32767,
                        "SmallInt join key max {hi} exceeds SmallInt range"
                    );
                    assert!(
                        *lo >= -32768,
                        "SmallInt join key min {lo} below SmallInt range"
                    );
                } else {
                    panic!("expected Int DfValues in Uniform spec");
                }
            }
            _ => panic!("expected Uniform spec for join key, got: {spec:?}"),
        }
    }

    #[test]
    fn gen_spec_for_role_bool_filter_key_uses_random() {
        let spec = gen_spec_for_role(
            ColumnRole::FilterKey,
            &SqlType::Bool,
            &ColumnGenerationSpec::Random,
        );
        // Bool can only be 0/1; Zipfian(1, 1000) would overflow. Should use Random.
        assert!(
            matches!(spec, ColumnGenerationSpec::Random),
            "expected Random spec for Bool filter key, got: {spec:?}"
        );
    }

    #[test]
    fn gen_spec_for_role_datetime_uses_random() {
        let spec = gen_spec_for_role(
            ColumnRole::JoinKey,
            &SqlType::DateTime(None),
            &ColumnGenerationSpec::Random,
        );
        // DateTime doesn't support Uniform(Int, Int). Should use Random.
        assert!(
            matches!(spec, ColumnGenerationSpec::Random),
            "expected Random spec for DateTime join key, got: {spec:?}"
        );
    }

    #[test]
    fn composed_joins_to_same_table_get_aliases() {
        // When two join patterns are composed and both join targets resolve to
        // the same table, the SQL must use aliases to avoid MySQL error 1066.
        let t_from = VarId(0);
        let t_join1 = VarId(1);
        let t_join2 = VarId(2);
        let c_proj = VarId(3);
        let c_jk1_l = VarId(4);
        let c_jk1_r = VarId(5);
        let c_jk2_l = VarId(6);
        let c_jk2_r = VarId(7);

        let constraints = vec![
            Constraint::BaseTable(t_from),
            Constraint::BaseTable(t_join1),
            Constraint::BaseTable(t_join2),
            Constraint::NotEq(t_from, t_join1),
            Constraint::NotEq(t_from, t_join2),
            Constraint::Or(
                vec![Constraint::NotEq(t_join1, t_join2)],
                vec![Constraint::AliasOf {
                    alias: t_join2,
                    original: t_join1,
                }],
            ),
            Constraint::ColumnExists {
                col: c_proj,
                table: t_from,
            },
            Constraint::ColumnExists {
                col: c_jk1_l,
                table: t_from,
            },
            Constraint::ColumnExists {
                col: c_jk1_r,
                table: t_join1,
            },
            Constraint::ColumnExists {
                col: c_jk2_l,
                table: t_from,
            },
            Constraint::ColumnExists {
                col: c_jk2_r,
                table: t_join2,
            },
            Constraint::ColumnTypeClass {
                col: c_jk1_l,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c_jk1_r,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c_jk2_l,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c_jk2_r,
                type_class: TypeClass::Integer,
            },
            Constraint::TypeCompatible(c_jk1_l, c_jk1_r),
            Constraint::TypeCompatible(c_jk2_l, c_jk2_r),
            Constraint::From(t_from),
            Constraint::Join {
                operator: readyset_sql::ast::JoinOperator::InnerJoin,
                right: crate::constraint::JoinRight::Table(t_join1),
                left_col: c_jk1_l,
                right_col: c_jk1_r,
            },
            Constraint::Join {
                operator: readyset_sql::ast::JoinOperator::LeftJoin,
                right: crate::constraint::JoinRight::Table(t_join2),
                left_col: c_jk2_l,
                right_col: c_jk2_r,
            },
            Constraint::ProjectColumn {
                col: c_proj,
                table: t_from,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Column { table: t_from },
            VarKind::Column { table: t_from },
            VarKind::Column { table: t_join1 },
            VarKind::Column { table: t_from },
            VarKind::Column { table: t_join2 },
        ];

        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        for name in ["t0", "t1"] {
            let mut schema = crate::state::TableSchema::new(SqlIdentifier::from(name));
            for i in 0..8 {
                schema.add_column(
                    SqlIdentifier::from(format!("c{i}")),
                    crate::state::ColumnMeta {
                        sql_type: SqlType::Int(None),
                        gen_spec: data_generator::ColumnGenerationSpec::Unique,
                    },
                );
            }
            schema.primary_key = Some(SqlIdentifier::from("c0"));
            state.add_table(schema);
        }

        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = crate::resolver::resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve with Or constraint handling duplicate join targets");
        let sql = output.query.display(Dialect::MySQL).to_string();

        assert!(sql.contains("SELECT"), "should produce valid SQL: {sql}");
        assert!(
            sql.contains(" AS "),
            "duplicate join target must be aliased, but got: {sql}"
        );
    }

    /// When two relation vars resolve to the same physical table, the
    /// resolver should auto-alias the second one — no `Or` constraint
    /// needed. This is the new behavior that lets `compose` skip the
    /// N×M `Or(NotEq, AliasOf)` soup.
    #[test]
    fn auto_aliases_duplicate_table_without_or_constraint() {
        let t1 = VarId(0);
        let t2 = VarId(1);
        let c1 = VarId(2);
        let c2 = VarId(3);
        let c1_jk = VarId(4);
        let c2_jk = VarId(5);

        // No Or, no AliasOf, no NotEq — just two BaseTables and a JOIN.
        // With reuse_preference=1.0 and a single registered table `t0`,
        // both relations must reuse `t0` — the resolver is responsible
        // for aliasing the second one.
        let constraints = vec![
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::ColumnExists { col: c1, table: t1 },
            Constraint::ColumnExists { col: c2, table: t2 },
            Constraint::ColumnExists {
                col: c1_jk,
                table: t1,
            },
            Constraint::ColumnExists {
                col: c2_jk,
                table: t2,
            },
            Constraint::ColumnTypeClass {
                col: c1_jk,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c2_jk,
                type_class: TypeClass::Integer,
            },
            Constraint::TypeCompatible(c1_jk, c2_jk),
            Constraint::From(t1),
            Constraint::Join {
                operator: readyset_sql::ast::JoinOperator::InnerJoin,
                right: crate::constraint::JoinRight::Table(t2),
                left_col: c1_jk,
                right_col: c2_jk,
            },
            Constraint::ProjectColumn { col: c1, table: t1 },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Column { table: t1 },
            VarKind::Column { table: t2 },
            VarKind::Column { table: t1 },
            VarKind::Column { table: t2 },
        ];

        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut schema = crate::state::TableSchema::new(SqlIdentifier::from("t0"));
        for i in 0..8 {
            schema.add_column(
                SqlIdentifier::from(format!("c{i}")),
                crate::state::ColumnMeta {
                    sql_type: SqlType::Int(None),
                    gen_spec: data_generator::ColumnGenerationSpec::Unique,
                },
            );
        }
        schema.primary_key = Some(SqlIdentifier::from("c0"));
        state.add_table(schema);

        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = crate::resolver::resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("resolver should auto-alias without an Or constraint");
        let sql = output.query.display(Dialect::MySQL).to_string();

        assert!(sql.contains("SELECT"), "should produce valid SQL: {sql}");
        // Both relations resolved to `t0`; the second occurrence must be
        // aliased — otherwise MySQL rejects with "Not unique table/alias"
        // (1066).
        assert!(
            sql.contains(" AS "),
            "duplicate base table must be auto-aliased, but got: {sql}"
        );
    }

    #[test]
    fn or_with_type_constraint_selects_branch() {
        let t = VarId(0);
        let c_proj = VarId(1);
        let c_or = VarId(2);

        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::From(t),
            Constraint::ColumnExists {
                col: c_proj,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_or,
                table: t,
            },
            Constraint::ProjectColumn {
                col: c_proj,
                table: t,
            },
            Constraint::Or(
                vec![
                    Constraint::ColumnTypeClass {
                        col: c_or,
                        type_class: TypeClass::String,
                    },
                    Constraint::WhereLike {
                        col: c_or,
                        table: t,
                        negated: false,
                        param: VarId(3),
                    },
                ],
                vec![
                    Constraint::ColumnTypeClass {
                        col: c_or,
                        type_class: TypeClass::Integer,
                    },
                    Constraint::WhereParam {
                        col: c_or,
                        table: t,
                        op: readyset_sql::ast::BinaryOperator::Equal,
                        param: VarId(3),
                    },
                ],
            ),
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c_or },
        ];

        let config = GeneratorConfig::default();
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut schema = crate::state::TableSchema::new(SqlIdentifier::from("t0"));
        schema.add_column(
            SqlIdentifier::from("id"),
            crate::state::ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: data_generator::ColumnGenerationSpec::Unique,
            },
        );
        schema.add_column(
            SqlIdentifier::from("name"),
            crate::state::ColumnMeta {
                sql_type: SqlType::VarChar(Some(255)),
                gen_spec: data_generator::ColumnGenerationSpec::Random,
            },
        );
        schema.primary_key = Some(SqlIdentifier::from("id"));
        state.add_table(schema);

        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = crate::resolver::resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve with general Or constraint");
        let sql = output.query.display(Dialect::MySQL).to_string();

        assert!(
            sql.contains("LIKE"),
            "Or should select string branch with LIKE, but got: {sql}"
        );
    }

    #[test]
    fn or_falls_back_when_first_branch_fails() {
        let t = VarId(0);
        let c_proj = VarId(1);
        let c_or = VarId(2);

        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::From(t),
            Constraint::ColumnExists {
                col: c_proj,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_or,
                table: t,
            },
            Constraint::ProjectColumn {
                col: c_proj,
                table: t,
            },
            Constraint::Or(
                vec![
                    Constraint::ColumnTypeClass {
                        col: c_or,
                        type_class: TypeClass::String,
                    },
                    Constraint::WhereLike {
                        col: c_or,
                        table: t,
                        negated: false,
                        param: VarId(3),
                    },
                ],
                vec![
                    Constraint::ColumnTypeClass {
                        col: c_or,
                        type_class: TypeClass::Integer,
                    },
                    Constraint::WhereParam {
                        col: c_or,
                        table: t,
                        op: readyset_sql::ast::BinaryOperator::Equal,
                        param: VarId(3),
                    },
                ],
            ),
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c_or },
        ];

        let config = GeneratorConfig::default();
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut schema = crate::state::TableSchema::new(SqlIdentifier::from("t0"));
        schema.add_column(
            SqlIdentifier::from("id"),
            crate::state::ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: data_generator::ColumnGenerationSpec::Unique,
            },
        );
        schema.add_column(
            SqlIdentifier::from("score"),
            crate::state::ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: data_generator::ColumnGenerationSpec::Unique,
            },
        );
        schema.primary_key = Some(SqlIdentifier::from("id"));
        state.add_table(schema);

        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = crate::resolver::resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("should resolve with fallback branch");
        let sql = output.query.display(Dialect::MySQL).to_string();

        assert!(
            !sql.contains("LIKE"),
            "should NOT contain LIKE (no string columns), but got: {sql}"
        );
        assert!(
            sql.contains("= ?") || sql.contains("= $"),
            "should contain WHERE = ? from fallback branch, but got: {sql}"
        );
    }

    /// Pattern authors don't pin PK columns intentionally — the bug is that
    /// the resolver collapses an Integer-class pattern var onto the
    /// auto-allocated PK column `c0` (also Int) under high reuse_preference.
    /// Two examples then both pin literals for the same physical PK,
    /// producing duplicate-key INSERT failures at run time. The resolver
    /// must treat columns referenced by `Constraint::Example` as
    /// "not-PK" and exclude the PK column from reuse candidates.
    #[test]
    fn example_pinned_column_does_not_collapse_onto_pk() {
        use crate::constraint::{DialectSupport, ExampleCell, ExampleValue};

        let t_var = VarId(0);
        let c_var = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t_var),
            Constraint::ColumnExists {
                col: c_var,
                table: t_var,
            },
            Constraint::ColumnTypeClass {
                col: c_var,
                type_class: TypeClass::Integer,
            },
            Constraint::From(t_var),
            Constraint::ProjectColumn {
                col: c_var,
                table: t_var,
            },
            Constraint::Example {
                note: "pinned literal must not be the PK",
                dialect: DialectSupport::Both,
                cells: vec![ExampleCell {
                    var: c_var,
                    value: ExampleValue::Literal("8"),
                }],
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t_var }];

        // Pre-register `t0` with `c0` (PK, Int) as the ONLY column. The PK
        // is the only matching Int candidate at reuse time, so without the
        // fix the resolver collapses `c_var` onto `c0` (= the PK). With the
        // fix it must synthesize a fresh non-PK column instead.
        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut schema = TableSchema::new(SqlIdentifier::from("t0"));
        // Use `fresh_column_name` so `column_counter` is advanced; otherwise
        // a later resolver synthesis on the same table also picks `c0` and
        // hits the `add_column` collision panic that masks the real
        // assertion.
        let pk_name = schema.fresh_column_name();
        schema.add_column(
            pk_name.clone(),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        schema.primary_key = Some(pk_name);
        state.add_table(schema);

        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = crate::resolver::resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect("resolve should succeed");

        assert_eq!(
            output.examples.len(),
            1,
            "expected exactly one resolved example"
        );
        let row_override = &output.examples[0].row_overrides[0];
        assert_ne!(
            row_override.column.as_str(),
            "c0",
            "example-pinned column must not bind to the PK column `c0`; \
             got column `{}` for table `{}`",
            row_override.column,
            row_override.table,
        );
    }

    #[test]
    fn type_compatible_narrows_second_column_to_first() {
        // Table has one Integer column and one Text column. Pin c1 to Integer,
        // mark c1/c2 TypeCompatible, force reuse. c2 must land on a numeric
        // (compatible) column every time, not the text one.
        use crate::constraint::TypeClass;
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnTypeClass {
                col: c1,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::TypeCompatible(c1, c2),
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        // GenerationState is not Clone; rebuild the mixed-type table per seed.
        for seed in 0..64u64 {
            let config = GeneratorConfig {
                reuse_preference: 1.0,
                ..Default::default()
            };
            let mut state = GenerationState::new(Dialect::PostgreSQL, config);
            let mut ts = TableSchema::new(SqlIdentifier::from("t0"));
            ts.add_column(
                SqlIdentifier::from("c0"),
                ColumnMeta {
                    sql_type: SqlType::Int(None),
                    gen_spec: ColumnGenerationSpec::Unique,
                },
            );
            ts.add_column(
                SqlIdentifier::from("c1"),
                ColumnMeta {
                    sql_type: SqlType::Text,
                    gen_spec: ColumnGenerationSpec::Random,
                },
            );
            state.add_table(ts);

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut entropy = Entropy::new(&mut rng);
            let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
                .expect("should resolve");
            let ty1 = match env.get(c1) {
                Some(Binding::Column { sql_type, .. }) => sql_type.clone(),
                _ => panic!("c1 unbound"),
            };
            let ty2 = match env.get(c2) {
                Some(Binding::Column { sql_type, .. }) => sql_type.clone(),
                _ => panic!("c2 unbound"),
            };
            assert!(
                types_compatible(&ty1, &ty2),
                "seed {seed}: c1={ty1:?} c2={ty2:?} not compatible"
            );
        }
    }

    #[test]
    fn type_compatible_narrows_against_outer_env() {
        use crate::constraint::TypeClass;
        let t = VarId(0);
        let c1 = VarId(1); // outer column, pinned Integer
        let c2 = VarId(2); // inner column, TypeCompatible(c1, c2)

        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        // GenerationState is not Clone; rebuild the mixed-type table per seed.
        for seed in 0..64u64 {
            let config = GeneratorConfig {
                reuse_preference: 1.0,
                ..Default::default()
            };
            let mut state = GenerationState::new(Dialect::PostgreSQL, config);
            let mut ts = TableSchema::new(SqlIdentifier::from("t0"));
            ts.add_column(
                SqlIdentifier::from("c0"),
                ColumnMeta {
                    sql_type: SqlType::Int(None),
                    gen_spec: ColumnGenerationSpec::Unique,
                },
            );
            ts.add_column(
                SqlIdentifier::from("c1"),
                ColumnMeta {
                    sql_type: SqlType::Text,
                    gen_spec: ColumnGenerationSpec::Random,
                },
            );
            state.add_table(ts);

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut entropy = Entropy::new(&mut rng);

            // Resolve the outer scope first.
            let outer_constraints = vec![
                Constraint::BaseTable(t),
                Constraint::ColumnExists { col: c1, table: t },
                Constraint::ColumnTypeClass {
                    col: c1,
                    type_class: TypeClass::Integer,
                },
            ];
            let (outer_env, _) =
                resolve_schema(&outer_constraints, &var_kinds, &mut state, &mut entropy)
                    .expect("outer resolves");

            // Inner scope references c2 and carries TypeCompatible to the outer c1.
            let inner_constraints = vec![
                Constraint::BaseTable(t),
                Constraint::ColumnExists { col: c2, table: t },
                Constraint::TypeCompatible(c1, c2),
            ];
            let (inner_env, _) = resolve_schema_with_outer(
                &inner_constraints,
                &var_kinds,
                &mut state,
                &mut entropy,
                Some(&outer_env),
            )
            .expect("inner resolves");

            let ty2 = match inner_env.get(c2) {
                Some(Binding::Column { sql_type, .. }) => sql_type.clone(),
                _ => panic!("c2 unbound at seed {seed}"),
            };
            assert!(
                types_compatible(&SqlType::Int(None), &ty2),
                "seed {seed}: inner c2={ty2:?} not compatible with outer Integer c1"
            );
        }
    }

    #[test]
    fn orderable_column_binding_never_resolves_to_bool() {
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::ColumnTypeClass {
                col: c,
                type_class: TypeClass::Orderable,
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        // GenerationState is not Clone, so rebuild the bool-only table per seed.
        for seed in 0..64u64 {
            let config = GeneratorConfig {
                reuse_preference: 1.0,
                ..Default::default()
            };
            let mut state = GenerationState::new(Dialect::PostgreSQL, config);
            // Use a name that won't collide with the resolver's fresh_column_name
            // counter (which starts at c0). The resolver will synthesize a new
            // column because the bool column doesn't match Orderable.
            let mut ts = TableSchema::new(SqlIdentifier::from("t0"));
            ts.add_column(
                SqlIdentifier::from("bool_col"),
                ColumnMeta {
                    sql_type: SqlType::Bool,
                    gen_spec: ColumnGenerationSpec::Random,
                },
            );
            state.add_table(ts);

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut entropy = Entropy::new(&mut rng);
            let (env, _) = resolve_schema(&constraints, &var_kinds, &mut state, &mut entropy)
                .expect("should resolve");
            if let Some(Binding::Column { sql_type, .. }) = env.get(c) {
                assert_ne!(
                    *sql_type,
                    SqlType::Bool,
                    "Orderable bound to bool at seed {seed}"
                );
            } else {
                panic!("column not bound at seed {seed}");
            }
        }
    }
}
