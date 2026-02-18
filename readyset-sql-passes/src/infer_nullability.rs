//! Projection-time nullability inference for SELECT items.
//!
//! This module computes, for a given `SelectStatement` and a target SELECT-list expression,
//! whether the expression is guaranteed to be non-NULL **for the rows that survive** the
//! FROM/JOIN → WHERE → GROUP BY → HAVING pipeline (3VL-correct and conservative).
//!
//! Key ideas:
//! - Derive a set of **non-NULL columns** from null-rejecting predicates in WHERE/HAVING
//!   and from INNER JOIN `ON` predicates (never from an OUTER JOIN's own `ON`).
//! - Track RHS relations of LEFT OUTER JOINs as **null-extended candidates**, and promote
//!   them to **present sides** when a downstream null-rejecting predicate proves their rows
//!   must exist in surviving output. This pass never rewrites join types.
//! - Integrate **schema nullability** (via `NonNullSchema`) and **derived table output
//!   signatures**. Only seed NOT NULL columns for relations that are either always present
//!   or proven present, avoiding false positives from join null-extension.
//!
//! Engine constraints assumed here:
//! - Only LEFT OUTER JOIN is supported (RIGHT/FULL/NATURAL/USING already desugared).
//! - No regex operators; string concatenation is function-based (`concat`, `concat_ws`), not `||`.
//! - `IS DISTINCT FROM` is not supported.
//!
//! The implementation aims for **zero false positives** (never claim non-NULL if it might be NULL),
//! tolerating some false negatives to remain sound under SQL 3VL.
use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias, expect_field_as_expr, get_from_item_reference_name,
};
use crate::unnest_subqueries::NonNullSchema;
use readyset_errors::{ReadySetResult, invariant};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, FunctionExpr, InValue, JoinConstraint, Literal, Relation,
    SelectStatement, SqlIdentifier, TableExpr, TableExprInner, UnaryOperator,
};
use std::collections::{HashMap, HashSet};

/// Identifies **strict** binary operators whose truth value in a filter is
/// null-rejecting (if any operand is NULL, the predicate can't be TRUE).
///
/// Used in two places:
/// 1) Derivation: allows descent into operands when such an operator appears
///    under a null-rejecting parent (e.g., AND, BETWEEN).
/// 2) Inference: marks the operator's result as non-NULL iff both operands are non-NULL.
///
/// Note: Regex and `||` are intentionally absent (unsupported by this engine).
fn is_null_rejecting_binary_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::ILike
            | BinaryOperator::NotILike
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::Greater
            | BinaryOperator::GreaterOrEqual
            | BinaryOperator::Less
            | BinaryOperator::LessOrEqual
            | BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::AtTimeZone
    )
}

/// Walks a predicate and collects **columns proven non-NULL** for surviving rows
/// by recognizing null-rejecting constructs.
///
/// Evidence sources:
/// - Conjunctive descent (`AND`) and strict binary ops (e.g., `=`, `>`, `LIKE`).
/// - `x IS NOT NULL` and normalized `NOT (x IS NULL)`.
/// - `BETWEEN` (operand/min/max must all be non-NULL to be TRUE).
/// - `IN (...)` / `IN (subquery)` (LHS must be non-NULL to be TRUE).
/// - Bare `Column` when reached under a null-rejecting parent.
///
/// Note: Do **not** call this on OUTER JOIN `ON` expressions; only WHERE/HAVING/INNER-ON
/// should flow here to preserve 3VL soundness.
fn derive_from_expr(predicate: &Expr, non_null_columns: &mut HashSet<Column>) {
    match predicate {
        // `AND` and known null rejection binary operators
        Expr::BinaryOp { op, lhs, rhs }
            if matches!(op, BinaryOperator::And) || is_null_rejecting_binary_op(op) =>
        {
            derive_from_expr(lhs.as_ref(), non_null_columns);
            derive_from_expr(rhs.as_ref(), non_null_columns)
        }
        // x IS NOT NULL -> null rejecting
        Expr::BinaryOp {
            op: BinaryOperator::IsNot,
            lhs,
            rhs,
        } if matches!(rhs.as_ref(), Expr::Literal(Literal::Null)) => {
            derive_from_expr(lhs.as_ref(), non_null_columns);
        }
        // NOT (x IS NULL) -> null rejecting
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } => match rhs.as_ref() {
            Expr::BinaryOp {
                op: BinaryOperator::Is,
                lhs,
                rhs,
            } if matches!(rhs.as_ref(), Expr::Literal(Literal::Null)) => {
                derive_from_expr(lhs.as_ref(), non_null_columns);
            }
            _ => {}
        },
        // x BETWEEN min AND max -> null rejecting
        Expr::Between {
            operand, min, max, ..
        } => {
            derive_from_expr(operand.as_ref(), non_null_columns);
            derive_from_expr(min.as_ref(), non_null_columns);
            derive_from_expr(max.as_ref(), non_null_columns);
        }
        // lhs IN (values) / lhs IN (subquery) -> null rejecting
        Expr::In { lhs, .. } => {
            derive_from_expr(lhs.as_ref(), non_null_columns);
        }
        // Survived column
        Expr::Column(col) => {
            non_null_columns.insert(col.clone());
        }
        // CAST: recurse into inner expression
        Expr::Cast { expr, .. } => {
            derive_from_expr(expr.as_ref(), non_null_columns);
        }
        // Function calls
        Expr::Call(func_expr) => match func_expr {
            // Never derive through COALESCE/IFNULL (result may be non-NULL even if some args are NULL)
            FunctionExpr::Coalesce(_) | FunctionExpr::IfNull(_, _) => {
                // intentionally no-op
            }
            // Single-argument functions that propagate NULL: if f(col) is non-NULL in a
            // null-rejecting context, col must be non-NULL.
            FunctionExpr::GroupConcat { expr, .. }
            | FunctionExpr::Extract { expr, .. }
            | FunctionExpr::Lower { expr, .. }
            | FunctionExpr::Upper { expr, .. }
            | FunctionExpr::Avg { expr, .. }
            | FunctionExpr::Max(expr)
            | FunctionExpr::Min(expr)
            | FunctionExpr::Sum { expr, .. }
            | FunctionExpr::ArrayAgg { expr, .. }
            | FunctionExpr::StringAgg { expr, .. } => {
                derive_from_expr(expr.as_ref(), non_null_columns);
            }
            FunctionExpr::Substring { string, pos, len } => {
                derive_from_expr(string.as_ref(), non_null_columns);
                if let Some(pos) = pos {
                    derive_from_expr(pos.as_ref(), non_null_columns);
                }
                if let Some(len) = len {
                    derive_from_expr(len.as_ref(), non_null_columns);
                }
            }
            FunctionExpr::Bucket { expr, interval } => {
                derive_from_expr(expr.as_ref(), non_null_columns);
                derive_from_expr(interval.as_ref(), non_null_columns);
            }
            // Multi-argument strict functions: null input implies null output.
            // This list mirrors the strict-function arm in `infer_expr_nullability`.
            FunctionExpr::ConvertTz(..)
            | FunctionExpr::DayOfWeek(..)
            | FunctionExpr::Month(..)
            | FunctionExpr::Timediff(..)
            | FunctionExpr::Addtime(..)
            | FunctionExpr::DateFormat(..)
            | FunctionExpr::DateTrunc(..)
            | FunctionExpr::Round(..)
            | FunctionExpr::Greatest(..)
            | FunctionExpr::Least(..)
            | FunctionExpr::Concat(..)
            | FunctionExpr::ConcatWs(..)
            | FunctionExpr::SplitPart(..)
            | FunctionExpr::Length(..)
            | FunctionExpr::OctetLength(..)
            | FunctionExpr::CharLength(..)
            | FunctionExpr::Ascii(..)
            | FunctionExpr::Hex(..)
            | FunctionExpr::JsonDepth(..)
            | FunctionExpr::JsonValid(..)
            | FunctionExpr::JsonOverlaps(..)
            | FunctionExpr::JsonQuote(..)
            | FunctionExpr::JsonArrayLength(..)
            | FunctionExpr::JsonStripNulls(..)
            | FunctionExpr::JsonbStripNulls(..)
            | FunctionExpr::JsonbPretty(..)
            | FunctionExpr::ArrayToString(..)
            | FunctionExpr::StAsText(..)
            | FunctionExpr::StAsWkt(..)
            | FunctionExpr::StAsEwkt(..) => {
                for arg in func_expr.arguments() {
                    derive_from_expr(arg, non_null_columns);
                }
            }
            // All other function variants have unknown or non-strict null semantics
            // (e.g. json_typeof(null) = 'null', not SQL NULL). Conservatively no-op
            // to avoid false positives.
            _ => {}
        },
        // Unknown
        _ => {}
    }
}

/// Derives column-level non-NULL facts from `expr`, then promotes any candidate
/// null-extended relations (RHS of LEFT JOINs) to **present** when evidence exists.
/// Returns the set of columns proved non-NULL by `expr`.
fn derive_from_expr_and_denull(
    expr: &Expr,
    null_extending_sides: &mut HashSet<Relation>,
    present_sides: &mut HashSet<Relation>,
) -> HashSet<Column> {
    let mut expr_non_null_columns = HashSet::new();
    derive_from_expr(expr, &mut expr_non_null_columns);

    // Map proved non-NULL columns to their relations and intersect with sides that
    // could have been null-extended by LEFT OUTER JOINs. Only those relations can
    // be safely promoted to "present" (not null-extended for survivors).
    let denull_rels = expr_non_null_columns
        .iter()
        .filter_map(|col| col.table.clone())
        .collect::<HashSet<_>>();

    let to_present: HashSet<Relation> = denull_rels
        .intersection(null_extending_sides)
        .cloned()
        .collect();

    for r in to_present {
        null_extending_sides.remove(&r);
        present_sides.insert(r);
    }

    expr_non_null_columns
}

/// Builds a **derived table output nullability signature** for `(<subquery>) AS alias`.
///
/// Recursively analyzes the subquery with `derive_from_stmt`, then infers nullability
/// for each projected field. Output `Column`s are stamped with the **outer alias**, so
/// the parent query can seed them when the derived relation is present.
///
/// Precondition: projected fields should have stable aliases; otherwise this will
/// `expect` and panic. If aliasing is not guaranteed upstream, synthesize names.
fn derived_output_nonnull(
    subquery_stmt: &SelectStatement,
    subquery_alias: SqlIdentifier,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<HashSet<Column>> {
    let inner_nonnull = derive_from_stmt(subquery_stmt, schema)?;
    let mut out = HashSet::new();
    for field in &subquery_stmt.fields {
        let (field_expr, field_alias) = expect_field_as_expr(field);
        if infer_expr_nullability(field_expr, &inner_nonnull)? {
            out.insert(Column {
                name: alias_for_expr(field_expr, field_alias),
                table: Some(subquery_alias.clone().into()),
            });
        }
    }
    Ok(out)
}

/// Collects relations and (for base tables) records alias→base mapping so schema
/// nullability can be resolved by base name while SELECT columns continue to use the alias.
fn collect_relations<'a>(
    it: impl Iterator<Item = &'a TableExpr> + 'a,
    derived_signatures: &mut HashMap<Relation, HashSet<Column>>,
    alias_to_base: &mut HashMap<Relation, Relation>,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<HashSet<Relation>> {
    let mut relations = HashSet::new();
    for tab_expr in it {
        let tab_expr_alias = get_from_item_reference_name(tab_expr)?;
        if let Some((subquery_stmt, subquery_alias)) = as_sub_query_with_alias(tab_expr) {
            invariant!(
                tab_expr_alias.eq(&subquery_alias.clone().into()),
                "Discrepancy in subquery alias readings"
            );
            let nonnull_sig =
                derived_output_nonnull(subquery_stmt, subquery_alias.clone(), schema)?;
            derived_signatures.insert(subquery_alias.into(), nonnull_sig);
        } else if let TableExpr {
            inner: TableExprInner::Table(base_table),
            alias: Some(alias),
            ..
        } = tab_expr
        {
            alias_to_base.insert(alias.into(), base_table.clone());
        }
        relations.insert(tab_expr_alias);
    }
    Ok(relations)
}

/// Main pass: derive **projection-time non-NULL columns** for a statement.
///
/// Workflow:
/// 1) Gather all relations in FROM/JOINs; for LEFT OUTER JOINs, seed their RHS into
///    `null_extending_sides`. Also compute derived-table output signatures.
/// 2) From INNER JOIN `ON`, WHERE, and HAVING, derive column-level non-NULL facts;
///    promote any RHS relations whose columns are proven non-NULL to `present_sides`.
/// 3) Seed schema NOT NULL columns and derived signatures **only** for relations that are
///    either always present (`all_relations - null_extending_sides`) or proven present
///    (`present_sides`). This avoids false positives due to join-introduced NULLs.
///
/// Returns the set of columns proven non-NULL at projection time.
fn derive_from_stmt(
    stmt: &SelectStatement,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<HashSet<Column>> {
    let mut non_null_columns = HashSet::new();

    let mut null_extending_sides = HashSet::new();
    let mut present_sides = HashSet::new();
    let mut all_relations = HashSet::new();

    let mut derived_signatures = HashMap::new();
    let mut alias_to_base: HashMap<Relation, Relation> = HashMap::new();

    all_relations.extend(collect_relations(
        stmt.tables.iter(),
        &mut derived_signatures,
        &mut alias_to_base,
        schema,
    )?);

    for jc in &stmt.join {
        let rhs_relations = collect_relations(
            jc.right.table_exprs(),
            &mut derived_signatures,
            &mut alias_to_base,
            schema,
        )?;
        // INNER vs LEFT OUTER JOIN handling:
        // - INNER: `ON` predicates are filtering and can yield column non-NULL proofs.
        // - LEFT OUTER: RHS rows may be null-extended; record RHS relations so that
        //   later null-rejecting evidence can promote them to `present_sides`.
        if jc.operator.is_inner_join() {
            if let JoinConstraint::On(expr) = &jc.constraint {
                non_null_columns.extend(derive_from_expr_and_denull(
                    expr,
                    &mut null_extending_sides,
                    &mut present_sides,
                ));
            }
            all_relations.extend(rhs_relations);
        } else {
            for rhs_rel in rhs_relations {
                all_relations.insert(rhs_rel.clone());
                null_extending_sides.insert(rhs_rel);
            }
        }
    }

    if let Some(where_expr) = &stmt.where_clause {
        non_null_columns.extend(derive_from_expr_and_denull(
            where_expr,
            &mut null_extending_sides,
            &mut present_sides,
        ));
    }

    if let Some(having_expr) = &stmt.having {
        non_null_columns.extend(derive_from_expr_and_denull(
            having_expr,
            &mut null_extending_sides,
            &mut present_sides,
        ));
    }

    // Seed schema-driven and derived-table nullability **only** for relations that are
    // guaranteed present for surviving rows:
    //   - always present: never on RHS of a LEFT JOIN
    //   - proven present: RHS of a LEFT JOIN with downstream null-rejecting evidence
    for rel in (&all_relations - &null_extending_sides).union(&present_sides) {
        let lookup_rel = alias_to_base.get(rel).unwrap_or(rel);
        for col in schema.not_null_columns_of(lookup_rel) {
            non_null_columns.insert(Column {
                name: col.name.clone(),
                table: Some(rel.clone()),
            });
        }
        if let Some(derived_sig) = derived_signatures.get(rel) {
            non_null_columns.extend(derived_sig.iter().cloned());
        }
    }

    Ok(non_null_columns)
}

/// Infers whether `expr` is guaranteed non-NULL given the set of columns proven
/// non-NULL at projection time.
///
/// Conservative 3VL-aware rules:
/// - Literals: NULL → false; non-NULL → true
/// - Column: true iff in `non_null_columns`
/// - `IS [NOT] NULL`: expression result is always non-NULL
/// - `AND`/`OR` and other strict binary ops: non-NULL iff both sides are non-NULL
/// - `NOT e`: non-NULL iff `e` is non-NULL
/// - `BETWEEN`: non-NULL iff operand/min/max are non-NULL
/// - `IN (list)`: non-NULL iff LHS and all list items are non-NULL
/// - `CASE`: non-NULL only if every THEN and ELSE is non-NULL (no branch pruning)
/// - `CAST`: mirrors argument nullability
/// - Window: `count/rank/dense_rank/row_number` → non-NULL; others mirror argument nullability per frame (assume no empty frames)
/// - Aggregates: `count` → non-NULL; others mirror argument nullability per group (this function is never invoked for scalar aggregates),
/// - Calls: `coalesce/ifnull` → non-NULL if any arg is non-NULL; otherwise only functions
///   whitelisted by `call_returns_nonnull_if_all_args_nonnull`
///
/// Any unrecognized construct yields `false` to avoid false positives.
fn infer_expr_nullability(expr: &Expr, non_null_columns: &HashSet<Column>) -> ReadySetResult<bool> {
    match expr {
        Expr::Literal(Literal::Null) => Ok(false),
        Expr::Literal(_) => Ok(true),
        Expr::Column(column) => Ok(non_null_columns.contains(column)),
        Expr::BinaryOp {
            op: BinaryOperator::Is | BinaryOperator::IsNot,
            rhs,
            ..
        } if matches!(rhs.as_ref(), Expr::Literal(Literal::Null)) => Ok(true),
        Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            lhs,
            rhs,
        } => Ok(infer_expr_nullability(lhs.as_ref(), non_null_columns)?
            && infer_expr_nullability(rhs.as_ref(), non_null_columns)?),
        Expr::BinaryOp { op, lhs, rhs } if is_null_rejecting_binary_op(op) => {
            Ok(infer_expr_nullability(lhs.as_ref(), non_null_columns)?
                && infer_expr_nullability(rhs.as_ref(), non_null_columns)?)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } => Ok(infer_expr_nullability(rhs.as_ref(), non_null_columns)?),
        // BETWEEN: result non-NULL iff operand, min, and max are non-NULL
        Expr::Between {
            operand, min, max, ..
        } => Ok(infer_expr_nullability(operand.as_ref(), non_null_columns)?
            && infer_expr_nullability(min.as_ref(), non_null_columns)?
            && infer_expr_nullability(max.as_ref(), non_null_columns)?),
        Expr::In { lhs, rhs, .. } if matches!(rhs, InValue::List(_)) => {
            if infer_expr_nullability(lhs.as_ref(), non_null_columns)?
                && let InValue::List(values) = rhs
            {
                for v in values {
                    if !infer_expr_nullability(v, non_null_columns)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Ok(false)
        }
        // CASE is NON-NULL, if ELSE and all THEN branches are NON-NULL
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            for branch in branches {
                if !infer_expr_nullability(&branch.body, non_null_columns)? {
                    return Ok(false);
                }
            }
            if let Some(else_expr) = else_expr {
                if !infer_expr_nullability(else_expr.as_ref(), non_null_columns)? {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
            Ok(true)
        }
        // CAST is NON-NULL if the argument is NON-NULL
        Expr::Cast { expr, .. } => infer_expr_nullability(expr.as_ref(), non_null_columns),
        // These Window Functions are always NON-NULL
        Expr::WindowFunction {
            function:
                FunctionExpr::Count { .. }
                | FunctionExpr::CountStar
                | FunctionExpr::DenseRank
                | FunctionExpr::Rank
                | FunctionExpr::RowNumber,
            ..
        } => Ok(true),
        // These Window Function aggregates are NON-NULL if their arguments are NON-NULL, assuming no empty frames
        Expr::WindowFunction {
            function:
                FunctionExpr::Avg { expr, .. }
                | FunctionExpr::Max(expr)
                | FunctionExpr::Min(expr)
                | FunctionExpr::Sum { expr, .. },
            ..
        } => infer_expr_nullability(expr.as_ref(), non_null_columns),
        // Check each function call individually
        Expr::Call(func_expr) => match func_expr {
            FunctionExpr::GroupConcat { expr, .. }
            | FunctionExpr::Extract { expr, .. }
            | FunctionExpr::Lower { expr, .. }
            | FunctionExpr::Upper { expr, .. } => {
                infer_expr_nullability(expr.as_ref(), non_null_columns)
            }
            FunctionExpr::Substring { string, pos, len } => {
                if infer_expr_nullability(string.as_ref(), non_null_columns)?
                    && let Some(pos) = pos
                {
                    if !infer_expr_nullability(pos.as_ref(), non_null_columns)? {
                        return Ok(false);
                    }
                    if let Some(len) = len
                        && !infer_expr_nullability(len.as_ref(), non_null_columns)?
                    {
                        return Ok(false);
                    }
                    return Ok(true);
                }
                Ok(false)
            }
            // These aggregates are always NON-NULL
            FunctionExpr::Count { .. } | FunctionExpr::CountStar => Ok(true),
            // These aggregates are NON-NULL if their arguments are NON-NULL
            FunctionExpr::Avg { expr, .. }
            | FunctionExpr::Max(expr)
            | FunctionExpr::Min(expr)
            | FunctionExpr::Sum { expr, .. } => {
                infer_expr_nullability(expr.as_ref(), non_null_columns)
            }
            // COALESCE: non-null if any argument is non-null
            FunctionExpr::Coalesce(arguments) => {
                for arg in arguments {
                    if infer_expr_nullability(arg, non_null_columns)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            // IFNULL: non-null if either argument is non-null
            FunctionExpr::IfNull(a, b) => {
                if infer_expr_nullability(a, non_null_columns)? {
                    return Ok(true);
                }
                if infer_expr_nullability(b, non_null_columns)? {
                    return Ok(true);
                }
                Ok(false)
            }
            // These functions are non-null iff all their arguments are non-null.
            // Each entry here is a conscious decision: functions that can return NULL
            // even with non-null inputs (e.g. path-lookup functions) must NOT appear here.
            FunctionExpr::ConvertTz(..)
            | FunctionExpr::DayOfWeek(..)
            | FunctionExpr::Month(..)
            | FunctionExpr::Timediff(..)
            | FunctionExpr::Addtime(..)
            | FunctionExpr::DateFormat(..)
            | FunctionExpr::DateTrunc(..)
            | FunctionExpr::Round(..)
            | FunctionExpr::Greatest(..)
            | FunctionExpr::Least(..)
            | FunctionExpr::Concat(..)
            | FunctionExpr::ConcatWs(..)
            | FunctionExpr::SplitPart(..)
            | FunctionExpr::Length(..)
            | FunctionExpr::OctetLength(..)
            | FunctionExpr::CharLength(..)
            | FunctionExpr::Ascii(..)
            | FunctionExpr::Hex(..)
            | FunctionExpr::JsonDepth(..)
            | FunctionExpr::JsonValid(..)
            | FunctionExpr::JsonOverlaps(..)
            | FunctionExpr::JsonQuote(..)
            | FunctionExpr::JsonArrayLength(..)
            | FunctionExpr::JsonStripNulls(..)
            | FunctionExpr::JsonbStripNulls(..)
            | FunctionExpr::JsonbPretty(..)
            | FunctionExpr::ArrayToString(..)
            | FunctionExpr::StAsText(..)
            | FunctionExpr::StAsWkt(..)
            | FunctionExpr::StAsEwkt(..) => {
                let other = func_expr;
                for arg in other.arguments() {
                    if !infer_expr_nullability(arg, non_null_columns)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            // These functions can return NULL even when all inputs are non-null
            // (e.g. a JSON path lookup returns NULL on missing key).
            // Conservatively return false.
            FunctionExpr::JsonTypeof(..)
            | FunctionExpr::JsonExtractPath(..)
            | FunctionExpr::JsonbExtractPath(..)
            | FunctionExpr::JsonExtractPathText(..)
            | FunctionExpr::JsonObject(..)
            | FunctionExpr::JsonbObject(..)
            | FunctionExpr::JsonBuildObject(..)
            | FunctionExpr::JsonbBuildObject(..)
            | FunctionExpr::JsonbInsert(..)
            | FunctionExpr::JsonbSet(..)
            | FunctionExpr::JsonbSetLax(..) => Ok(false),
            // Aggregates and group functions: mirror their primary expression argument
            FunctionExpr::ArrayAgg { expr, .. } | FunctionExpr::StringAgg { expr, .. } => {
                infer_expr_nullability(expr.as_ref(), non_null_columns)
            }
            // JsonObjectAgg can produce null entries; conservatively nullable
            FunctionExpr::JsonObjectAgg { .. } => Ok(false),
            // Bucket is non-null if both arguments are non-null
            FunctionExpr::Bucket { expr, interval } => {
                Ok(infer_expr_nullability(expr.as_ref(), non_null_columns)?
                    && infer_expr_nullability(interval.as_ref(), non_null_columns)?)
            }
            // Window-function variants used as bare Calls: always non-null
            FunctionExpr::RowNumber | FunctionExpr::Rank | FunctionExpr::DenseRank => Ok(true),
            // No-paren functions that have no arguments and are always non-null
            FunctionExpr::CurrentDate
            | FunctionExpr::CurrentTimestamp(_)
            | FunctionExpr::CurrentTime
            | FunctionExpr::LocalTimestamp
            | FunctionExpr::LocalTime => Ok(true),
            // These no-paren functions can return null depending on context
            FunctionExpr::CurrentUser
            | FunctionExpr::SessionUser
            | FunctionExpr::CurrentCatalog
            | FunctionExpr::SqlUser => Ok(false),
            // UDFs have unknown null semantics; conservatively assume nullable
            FunctionExpr::Udf { .. } => Ok(false),
        },
        // Avoid false positive
        _ => Ok(false),
    }
}

/// Entry point for SELECT-list nullability.
///
/// Runs statement-level derivation to obtain projection-time non-NULL columns,
/// then applies expression-level inference to the target field.
///
/// Soundness: 3VL-sound and conservative (zero false positives).
pub(crate) fn infer_select_field_nullability(
    field_expr: &Expr,
    stmt: &SelectStatement,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<bool> {
    let non_null_columns = derive_from_stmt(stmt, schema)?;
    infer_expr_nullability(field_expr, &non_null_columns)
}
