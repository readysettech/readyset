use std::collections::HashMap;

use data_generator::ColumnGenerationSpec;
use readyset_sql::Dialect;
use readyset_sql::ast::{
    BinaryOperator, Column, CommonTableExpr, CompoundSelectOperator, CompoundSelectStatement, Expr,
    FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause, InValue, ItemPlaceholder,
    JoinClause, JoinConstraint, JoinRightSide, LimitClause, LimitValue, Literal, NullOrder,
    OrderBy, OrderClause, OrderType, Relation, SelectSpecification, SelectStatement, SqlIdentifier,
    SqlType, TableExpr, TableExprInner,
};

use super::{Binding, DdlStep, Env, ParamMeta, ResolveError};
use crate::constraint::{
    AggregateFn, Constraint, LiteralKind, ScalarFn, SubqueryExprKind, SubqueryRelationKind,
    WindowFn,
};
use crate::entropy::Entropy;
use crate::state::GenerationState;
use crate::var::{VarId, VarKind};

/// Return type for functions that build a SELECT statement with parameter metadata
/// and a VarId-to-placeholder-index map.
type SelectBuildResult =
    Result<(SelectStatement, Vec<ParamMeta>, Vec<Option<usize>>), ResolveError>;

/// Return type for functions that build a compound SELECT statement with parameter
/// metadata and a VarId-to-placeholder-index map.
type CompoundBuildResult =
    Result<(SelectSpecification, Vec<ParamMeta>, Vec<Option<usize>>), ResolveError>;

/// Build a SelectStatement from resolved bindings and structural constraints.
///
/// Top-level entry point: starts a fresh placeholder counter at `$1`, and
/// delegates to [`build_select_inner`].
pub(super) fn build_select(
    env: &mut Env,
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> SelectBuildResult {
    let mut placeholder_idx: u32 = 1;
    let mut param_map: Vec<Option<usize>> = vec![None; var_kinds.len()];
    let (stmt, params) = build_select_inner(
        env,
        constraints,
        var_kinds,
        state,
        entropy,
        &mut placeholder_idx,
        &mut param_map,
    )?;
    Ok((stmt, params, param_map))
}

/// Build a SelectStatement, threading a shared `placeholder_idx`.
///
/// `var_kinds` is the recipe-wide kind table indexed by absolute VarId,
/// threaded through to inner subquery resolution so derived-relation
/// aliases keep their authoritative `VarKind` rather than being
/// re-inferred (and possibly mislabeled).
///
/// `placeholder_idx` is the shared parameter counter. PostgreSQL `$N`
/// placeholders are global to the prepared statement; nested subqueries
/// MUST share the same counter or two distinct parameters will collide
/// on `$1`, breaking the bind protocol.
fn build_select_inner(
    env: &mut Env,
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    placeholder_idx: &mut u32,
    param_map: &mut [Option<usize>],
) -> Result<(SelectStatement, Vec<ParamMeta>), ResolveError> {
    let dialect = state.dialect();
    let mut ctes = Vec::new();
    let mut fields = Vec::new();
    let mut tables = Vec::new();
    let mut joins = Vec::new();
    let mut where_clauses = Vec::new();
    let mut group_by_fields = Vec::new();
    let mut having_expr: Option<Expr> = None;
    let mut order_bys = Vec::new();
    // Table of the first ORDER BY column, used to append a primary-key tiebreaker for a total
    // order (see the post-loop append below).
    let mut order_tiebreak_table: Option<VarId> = None;
    let mut limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: None,
    };
    let mut distinct = false;
    let mut params = Vec::new();
    // Stash the resolved inner SelectStatement + sqN alias for each
    // `SubqueryRelation { kind: JoinTarget, alias }`, keyed by the alias
    // VarId. The sibling `Join { right: JoinRight::Table(alias) }` looks
    // it up to emit `JOIN (SELECT ...) AS sqN`.
    let mut join_subqueries: HashMap<VarId, (SelectStatement, SqlIdentifier)> = HashMap::new();

    for c in constraints {
        match c {
            Constraint::From(t) => {
                let (physical_name, alias) = get_table_name_and_alias(env, *t)?;
                tables.push(TableExpr {
                    inner: TableExprInner::Table(Relation {
                        schema: None,
                        name: physical_name,
                    }),
                    alias,
                    column_aliases: vec![],
                });
            }
            Constraint::ProjectColumn { col, table } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                fields.push(FieldDefinitionExpr::Expr {
                    expr: make_column_expr(&col_name, &table_name),
                    alias: None,
                });
            }
            Constraint::ProjectAggregate {
                function,
                col,
                table,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_expr = make_column_expr(&col_name, &table_name);
                let func_expr = make_aggregate_expr(function, col_expr);
                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::Call(func_expr),
                    alias: None,
                });
            }
            Constraint::ProjectLiteral { literal } => {
                let lit = match literal {
                    LiteralKind::Integer => Literal::Integer(1),
                    LiteralKind::Float => Literal::Number("1.0".to_string()),
                    LiteralKind::String => Literal::String("literal".to_string()),
                    LiteralKind::Null => Literal::Null,
                    LiteralKind::Boolean => Literal::Boolean(true),
                };
                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::Literal(lit),
                    alias: None,
                });
            }
            Constraint::ProjectFunction { function, args } => {
                let mut col_exprs = Vec::with_capacity(args.len());
                let mut col_types = Vec::with_capacity(args.len());
                for (col, table) in args {
                    let (cn, tn) = get_col_table(env, *col, *table)?;
                    col_exprs.push(make_column_expr(&cn, &tn));
                    col_types.push(get_col_type(env, *col)?);
                }
                let func_expr = make_scalar_fn_expr(function, col_exprs, &col_types, dialect);
                fields.push(FieldDefinitionExpr::Expr {
                    expr: func_expr,
                    alias: None,
                });
            }
            Constraint::ProjectBinaryOp {
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => {
                let (l_name, l_table) = get_col_table(env, *left_col, *left_table)?;
                let (r_name, r_table) = get_col_table(env, *right_col, *right_table)?;
                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::BinaryOp {
                        lhs: Box::new(make_column_expr(&l_name, &l_table)),
                        op: *op,
                        rhs: Box::new(make_column_expr(&r_name, &r_table)),
                    },
                    alias: None,
                });
            }
            Constraint::ProjectCast {
                col,
                table,
                target_ty,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::Cast {
                        expr: Box::new(make_column_expr(&col_name, &table_name)),
                        ty: target_ty.clone(),
                        style: readyset_sql::ast::CastStyle::As,
                        array: false,
                    },
                    alias: None,
                });
            }
            Constraint::WhereLookupBinaryOp {
                lookup_col,
                lookup_table,
                cmp,
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => {
                let (lookup_name, lookup_tab) = get_col_table(env, *lookup_col, *lookup_table)?;
                let (l_name, l_tab) = get_col_table(env, *left_col, *left_table)?;
                let (r_name, r_tab) = get_col_table(env, *right_col, *right_table)?;
                let rhs_expr = Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&l_name, &l_tab)),
                    op: *op,
                    rhs: Box::new(make_column_expr(&r_name, &r_tab)),
                };
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&lookup_name, &lookup_tab)),
                    op: *cmp,
                    rhs: Box::new(rhs_expr),
                });
            }
            Constraint::WhereParam {
                col,
                table,
                op,
                param,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let placeholder =
                    make_and_record_placeholder(dialect, placeholder_idx, *param, param_map);
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    op: *op,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                });
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 1,
                });
            }
            Constraint::WhereInParam {
                col,
                table,
                num_values,
                params: slot_vars,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let mut placeholders = Vec::new();
                for slot_var in slot_vars {
                    placeholders.push(Expr::Literal(Literal::Placeholder(
                        make_and_record_placeholder(dialect, placeholder_idx, *slot_var, param_map),
                    )));
                }
                where_clauses.push(Expr::In {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    rhs: readyset_sql::ast::InValue::List(placeholders),
                    negated: false,
                });
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: u32::from(*num_values),
                });
            }
            Constraint::WhereRangeParam { col, table, lo, hi } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let p1 = make_and_record_placeholder(dialect, placeholder_idx, *lo, param_map);
                let p2 = make_and_record_placeholder(dialect, placeholder_idx, *hi, param_map);
                // col >= ? AND col <= ?
                let ge = Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    op: BinaryOperator::GreaterOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(p1))),
                };
                let le = Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(p2))),
                };
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(ge),
                    op: BinaryOperator::And,
                    rhs: Box::new(le),
                });
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 2,
                });
            }
            Constraint::WhereBetweenParam { col, table, lo, hi } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let p1 = make_and_record_placeholder(dialect, placeholder_idx, *lo, param_map);
                let p2 = make_and_record_placeholder(dialect, placeholder_idx, *hi, param_map);
                where_clauses.push(Expr::Between {
                    operand: Box::new(make_column_expr(&col_name, &table_name)),
                    min: Box::new(Expr::Literal(Literal::Placeholder(p1))),
                    max: Box::new(Expr::Literal(Literal::Placeholder(p2))),
                    negated: false,
                });
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 2,
                });
            }
            Constraint::WhereLike {
                col,
                table,
                negated,
                param,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let placeholder =
                    make_and_record_placeholder(dialect, placeholder_idx, *param, param_map);
                let op = if *negated {
                    BinaryOperator::NotLike
                } else {
                    BinaryOperator::Like
                };
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    op,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                });
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 1,
                });
            }
            Constraint::WhereIsNull {
                col,
                table,
                negated,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let op = if *negated {
                    BinaryOperator::IsNot
                } else {
                    BinaryOperator::Is
                };
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&col_name, &table_name)),
                    op,
                    rhs: Box::new(Expr::Literal(Literal::Null)),
                });
            }
            Constraint::WhereColumnCompare {
                left_col,
                left_table,
                op,
                right_col,
                right_table,
            } => {
                let (l_name, l_table) = get_col_table(env, *left_col, *left_table)?;
                let (r_name, r_table) = get_col_table(env, *right_col, *right_table)?;
                where_clauses.push(Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&l_name, &l_table)),
                    op: *op,
                    rhs: Box::new(make_column_expr(&r_name, &r_table)),
                });
            }
            Constraint::WhereOr { conditions } => {
                let mut or_parts = Vec::new();
                for cond in conditions {
                    match cond {
                        Constraint::WhereParam {
                            col,
                            table,
                            op,
                            param,
                        } => {
                            let (col_name, table_name) = get_col_table(env, *col, *table)?;
                            let col_type = get_col_type(env, *col)?;
                            let placeholder = make_and_record_placeholder(
                                dialect,
                                placeholder_idx,
                                *param,
                                param_map,
                            );
                            or_parts.push(Expr::BinaryOp {
                                lhs: Box::new(make_column_expr(&col_name, &table_name)),
                                op: *op,
                                rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                            });
                            params.push(ParamMeta {
                                sql_type: col_type,
                                gen_spec: ColumnGenerationSpec::Random,
                                count: 1,
                            });
                        }
                        Constraint::WhereIsNull {
                            col,
                            table,
                            negated,
                        } => {
                            let (col_name, table_name) = get_col_table(env, *col, *table)?;
                            let op = if *negated {
                                BinaryOperator::IsNot
                            } else {
                                BinaryOperator::Is
                            };
                            or_parts.push(Expr::BinaryOp {
                                lhs: Box::new(make_column_expr(&col_name, &table_name)),
                                op,
                                rhs: Box::new(Expr::Literal(Literal::Null)),
                            });
                        }
                        // Silently dropping unsupported conditions inside
                        // WhereOr produced SQL that could spuriously match
                        // upstream — fail loudly instead so generators can
                        // be fixed (or extended) deliberately.
                        _ => {
                            return Err(ResolveError::Unsupported {
                                variant: "non-WhereParam/WhereIsNull condition",
                                location: "WhereOr",
                            });
                        }
                    }
                }
                if !or_parts.is_empty() {
                    let mut combined = or_parts.remove(0);
                    for part in or_parts {
                        combined = Expr::BinaryOp {
                            lhs: Box::new(combined),
                            op: BinaryOperator::Or,
                            rhs: Box::new(part),
                        };
                    }
                    where_clauses.push(combined);
                }
            }
            Constraint::Join {
                operator,
                right: crate::constraint::JoinRight::Table(t),
                left_col,
                right_col,
            } => {
                let (lc_name, lt_name) = get_col_and_table_for_join(env, *left_col)?;

                // The right side is always a relation var. If a sibling
                // `SubqueryRelation { kind: JoinTarget }` already stashed
                // an inner SelectStatement for this var, emit it as an
                // inline subquery; otherwise look up the env binding for
                // a plain table / CTE alias reference.
                let rc_name;
                let effective_rt_name;

                let right_side = if let Some((inner_query, sq_alias)) = join_subqueries.remove(t) {
                    effective_rt_name = sq_alias.clone();
                    rc_name = first_projected_column(&inner_query).ok_or_else(|| {
                        ResolveError::TypeMismatch {
                            expected: "join subquery to project at least one column".into(),
                            actual: "no projected column".into(),
                        }
                    })?;
                    JoinRightSide::Table(TableExpr {
                        inner: TableExprInner::Subquery(Box::new(inner_query)),
                        alias: Some(sq_alias),
                        column_aliases: vec![],
                    })
                } else {
                    let (rcn, rtn) = get_col_and_table_for_join(env, *right_col)?;
                    rc_name = rcn;
                    effective_rt_name = rtn;
                    let (tn, t_alias) = get_table_name_and_alias(env, *t)?;
                    JoinRightSide::Table(TableExpr {
                        inner: TableExprInner::Table(Relation {
                            schema: None,
                            name: tn,
                        }),
                        alias: t_alias,
                        column_aliases: vec![],
                    })
                };

                let on_expr = Expr::BinaryOp {
                    lhs: Box::new(make_column_expr(&lc_name, &lt_name)),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(make_column_expr(&rc_name, &effective_rt_name)),
                };

                joins.push(JoinClause {
                    operator: *operator,
                    right: right_side,
                    constraint: JoinConstraint::On(on_expr),
                });
            }
            Constraint::GroupBy { col, table } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                group_by_fields.push(FieldReference::Expr(make_column_expr(
                    &col_name,
                    &table_name,
                )));
            }
            Constraint::OrderBy {
                col,
                table,
                direction,
                null_order,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                if order_tiebreak_table.is_none() {
                    order_tiebreak_table = Some(*table);
                }
                order_bys.push(OrderBy {
                    field: FieldReference::Expr(make_column_expr(&col_name, &table_name)),
                    order_type: Some(*direction),
                    null_order: null_order.unwrap_or(readyset_sql::ast::NullOrder::NullsLast),
                });
            }
            Constraint::Limit { limit, offset } => {
                let limit_i64 = i64::try_from(*limit)
                    .map_err(|_| ResolveError::LimitOverflow { value: *limit })?;
                let offset_i64 = offset
                    .map(|o| i64::try_from(o).map_err(|_| ResolveError::LimitOverflow { value: o }))
                    .transpose()?;
                limit_clause = LimitClause::LimitOffset {
                    limit: Some(LimitValue::Literal(Literal::Integer(limit_i64))),
                    offset: offset_i64.map(Literal::Integer),
                };
            }
            Constraint::Distinct => {
                distinct = true;
            }
            Constraint::Having {
                function,
                col,
                table,
                op,
                param,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_expr = make_column_expr(&col_name, &table_name);
                let func_expr = make_aggregate_expr(function, col_expr);
                let placeholder =
                    make_and_record_placeholder(dialect, placeholder_idx, *param, param_map);
                let new_expr = Expr::BinaryOp {
                    lhs: Box::new(Expr::Call(func_expr)),
                    op: *op,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                };
                and_merge(&mut having_expr, new_expr);
                // Param compares against the AGGREGATE'S RESULT, not the
                // input column. Postgres prepared statements reject
                // `count(c) > $param` if $param's pg type doesn't match
                // BIGINT; MySQL coerced silently.
                let col_type = get_col_type(env, *col)?;
                params.push(ParamMeta {
                    sql_type: aggregate_result_type(function, &col_type),
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 1,
                });
            }
            Constraint::HavingKeyFilter {
                col,
                table,
                op,
                param,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_expr = make_column_expr(&col_name, &table_name);
                let placeholder =
                    make_and_record_placeholder(dialect, placeholder_idx, *param, param_map);
                let new_expr = Expr::BinaryOp {
                    lhs: Box::new(col_expr),
                    op: *op,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                };
                and_merge(&mut having_expr, new_expr);
                let col_type = get_col_type(env, *col)?;
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 1,
                });
            }
            Constraint::SubqueryExpr {
                kind,
                constraints: inner_constraints,
                shared_vars,
            } => {
                let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                    inner_constraints,
                    env,
                    var_kinds,
                    shared_vars,
                    state,
                    entropy,
                    placeholder_idx,
                    param_map,
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);

                match kind {
                    SubqueryExprKind::ExistsUncorrelated | SubqueryExprKind::ExistsCorrelated => {
                        where_clauses.push(Expr::Exists(Box::new(inner_query)));
                    }
                    SubqueryExprKind::InSubquery => {
                        let outer_var =
                            *shared_vars
                                .first()
                                .ok_or_else(|| ResolveError::TypeMismatch {
                                    expected: "shared_vars for InSubquery".to_string(),
                                    actual: "empty shared_vars".to_string(),
                                })?;
                        match env.get(outer_var).cloned() {
                            Some(Binding::Column {
                                name, table_var, ..
                            }) => {
                                let table_name = get_table_name(env, table_var)?;
                                where_clauses.push(Expr::In {
                                    lhs: Box::new(make_column_expr(&name, &table_name)),
                                    rhs: InValue::Subquery(Box::new(inner_query)),
                                    negated: false,
                                });
                            }
                            _ => return Err(ResolveError::Unbound(outer_var)),
                        }
                    }
                    SubqueryExprKind::ScalarSubquery => {
                        fields.push(FieldDefinitionExpr::Expr {
                            expr: Expr::NestedSelect(Box::new(inner_query)),
                            alias: None,
                        });
                    }
                }
            }
            Constraint::SubqueryRelation {
                kind: SubqueryRelationKind::Cte,
                alias: alias_var,
                constraints: inner_constraints,
                shared_vars,
            } => {
                let cte_alias = state.fresh_cte_alias();
                env.bind(
                    *alias_var,
                    Binding::Table {
                        name: cte_alias.clone(),
                        alias: None,
                    },
                );
                let (inner_query, inner_params, inner_ddl) = resolve_cte_inner_subquery(
                    inner_constraints,
                    env,
                    var_kinds,
                    shared_vars,
                    *alias_var,
                    state,
                    entropy,
                    placeholder_idx,
                    param_map,
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);
                ctes.push(CommonTableExpr {
                    name: cte_alias,
                    statement: inner_query,
                });
            }
            Constraint::SubqueryRelation {
                kind: SubqueryRelationKind::JoinTarget,
                alias: alias_var,
                constraints: inner_constraints,
                shared_vars,
            } => {
                // Resolve the inner subquery and bind the alias var to a
                // fresh `sqN`. Inner column bindings get propagated to the
                // outer env by `resolve_inner_subquery`, so any
                // `right_col` reference on the sibling Join arm resolves
                // through env. The Join arm consumes `join_subqueries`
                // entry to splice the inner query into the JOIN as
                // `(SELECT ...) AS sqN`.
                let sq_alias = state.fresh_subquery_alias();
                env.bind(
                    *alias_var,
                    Binding::Table {
                        name: sq_alias.clone(),
                        alias: None,
                    },
                );
                let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                    inner_constraints,
                    env,
                    var_kinds,
                    shared_vars,
                    state,
                    entropy,
                    placeholder_idx,
                    param_map,
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);
                join_subqueries.insert(*alias_var, (inner_query, sq_alias));
            }
            Constraint::SubqueryRelation {
                kind: SubqueryRelationKind::FromSubquery,
                alias: alias_var,
                constraints: inner_constraints,
                shared_vars,
            } => {
                let sq_alias = state.fresh_subquery_alias();
                // Bind the alias var so outer references (e.g.
                // `From(alias_var)`, `ProjectColumn { table: alias_var, .. }`)
                // resolve through env to `sq_alias` — same env-binding shape
                // as the Cte arm.
                env.bind(
                    *alias_var,
                    Binding::Table {
                        name: sq_alias.clone(),
                        alias: None,
                    },
                );
                let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                    inner_constraints,
                    env,
                    var_kinds,
                    shared_vars,
                    state,
                    entropy,
                    placeholder_idx,
                    param_map,
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);
                for (idx, inner_field) in inner_query.fields.iter().enumerate() {
                    // FieldDefinitionExpr::All / AllInTable used to hit
                    // `let-else continue` and silently disappear from the
                    // outer projection. Reject them so the generator
                    // surfaces the bug instead.
                    let (expr, alias) = match inner_field {
                        FieldDefinitionExpr::Expr { expr, alias } => (expr, alias),
                        FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => {
                            return Err(ResolveError::Unsupported {
                                variant: "All/AllInTable in FROM-subquery projection",
                                location: "FromSubquery",
                            });
                        }
                    };
                    // Index-based fallback name (col0, col1, ...) so multiple
                    // non-Column projections don't collide on a single `"col"`.
                    let name = match (alias, expr) {
                        (Some(a), _) => a.clone(),
                        (None, Expr::Column(c)) => c.name.clone(),
                        _ => SqlIdentifier::from(format!("col{idx}")),
                    };
                    fields.push(FieldDefinitionExpr::Expr {
                        expr: Expr::Column(Column {
                            name,
                            table: Some(Relation {
                                schema: None,
                                name: sq_alias.clone(),
                            }),
                        }),
                        alias: None,
                    });
                }
                tables.push(TableExpr {
                    inner: TableExprInner::Subquery(Box::new(inner_query)),
                    alias: Some(sq_alias),
                    column_aliases: vec![],
                });
            }
            Constraint::WindowFunction {
                function,
                partition_col,
                order_col,
                order_type,
            } => {
                let func_expr = match function {
                    WindowFn::RowNumber => FunctionExpr::RowNumber,
                    WindowFn::Rank => FunctionExpr::Rank,
                    WindowFn::DenseRank => FunctionExpr::DenseRank,
                };

                let partition_by = if let Some((col, table)) = partition_col {
                    let (col_name, table_name) = get_col_table(env, *col, *table)?;
                    vec![make_column_expr(&col_name, &table_name)]
                } else {
                    vec![]
                };

                let mut order_by = if let Some((col, table)) = order_col {
                    let (col_name, table_name) = get_col_table(env, *col, *table)?;
                    let ot = order_type.unwrap_or(OrderType::OrderAscending);
                    vec![(
                        make_column_expr(&col_name, &table_name),
                        ot,
                        NullOrder::NullsLast,
                    )]
                } else {
                    vec![]
                };

                // ROW_NUMBER/RANK/DENSE_RANK are non-deterministic unless the OVER ORDER BY is a
                // total order over each partition. Append the table's primary key as a tiebreaker
                // so the generated query produces identical results on upstream and Readyset.
                if let Some((_, table)) = (*order_col).or(*partition_col) {
                    let physical = get_physical_table_name(env, table)?;
                    let pk = state.table(&physical).and_then(|s| s.primary_key.clone());
                    if let Some(pk) = pk {
                        let emitted_table = get_table_name(env, table)?;
                        let pk_expr = make_column_expr(&pk, &emitted_table);
                        if !order_by.iter().any(|(e, _, _)| *e == pk_expr) {
                            order_by.push((
                                pk_expr,
                                OrderType::OrderAscending,
                                NullOrder::NullsLast,
                            ));
                        }
                    }
                }

                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::WindowFunction {
                        function: func_expr,
                        partition_by,
                        order_by,
                    },
                    alias: None,
                });
            }
            // CompoundSelect is handled at the resolve() level, not here.
            Constraint::CompoundSelect { .. } => {}
            // Schema and unification constraints don't produce AST nodes
            // (they were resolved in `resolve_schema`).
            Constraint::BaseTable(_)
            | Constraint::AliasOf { .. }
            | Constraint::ColumnExists { .. }
            | Constraint::ColumnTypeClass { .. }
            | Constraint::TypeCompatible(_, _)
            | Constraint::Eq(_, _)
            | Constraint::NotEq(_, _) => {}
            // Example constraints are resolved in resolve_examples; nothing to do here.
            Constraint::Example { .. } => {}
            // Or constraints should have been expanded by
            // `resolve_constraint_set` before reaching build_select. Hard
            // panic in release too — the old `debug_assert!(false)` was a
            // no-op outside debug builds and produced silently-corrupt SQL.
            Constraint::Or(_, _) => {
                unreachable!("Or constraint should have been expanded before build_select")
            }
        }
    }

    // Combine WHERE clauses with AND
    let where_clause = if where_clauses.is_empty() {
        None
    } else {
        let mut combined = where_clauses.remove(0);
        for clause in where_clauses {
            combined = Expr::BinaryOp {
                lhs: Box::new(combined),
                op: BinaryOperator::And,
                rhs: Box::new(clause),
            };
        }
        Some(combined)
    };

    let group_by = if group_by_fields.is_empty() {
        None
    } else {
        Some(GroupByClause {
            fields: group_by_fields,
        })
    };

    // Append the ordered table's primary key as a final ascending tiebreaker so ORDER BY
    // establishes a total order. Without it, ties in the ordered column make the row order
    // non-deterministic — and under LIMIT/OFFSET the returned row SET non-deterministic —
    // producing spurious upstream-vs-Readyset mismatches. Skipped when the order already targets
    // the primary key or the table has no resolvable base-table PK (e.g. a derived relation).
    if let Some(tb_table) = order_tiebreak_table
        && let Ok(physical) = get_physical_table_name(env, tb_table)
        && let Some(pk) = state.table(&physical).and_then(|s| s.primary_key.clone())
    {
        let emitted_table = get_table_name(env, tb_table)?;
        let pk_expr = make_column_expr(&pk, &emitted_table);
        let already = order_bys
            .iter()
            .any(|ob| matches!(&ob.field, FieldReference::Expr(e) if *e == pk_expr));
        if !already {
            order_bys.push(OrderBy {
                field: FieldReference::Expr(pk_expr),
                order_type: Some(OrderType::OrderAscending),
                null_order: NullOrder::NullsLast,
            });
        }
    }

    let order = if order_bys.is_empty() {
        None
    } else {
        Some(OrderClause {
            order_by: order_bys,
        })
    };

    // MySQL parses `FROM A, B JOIN C ON ...` as `FROM A, (B JOIN C ON ...)` —
    // A is *not* in scope for the JOIN's ON clause. Convert any extra FROMs
    // into a chain of leading CROSS JOINs so the entire FROM tree is a
    // single left-deep scope and ON clauses can reference any table.
    let (tables, joins) = if tables.len() > 1 {
        let mut iter = tables.into_iter();
        let first = iter.next().expect("len > 1 implies at least one element");
        let cross_joins = iter.map(|t| JoinClause {
            operator: readyset_sql::ast::JoinOperator::CrossJoin,
            right: JoinRightSide::Table(t),
            constraint: JoinConstraint::Empty,
        });
        let combined: Vec<JoinClause> = cross_joins.chain(joins).collect();
        (vec![first], combined)
    } else {
        (tables, joins)
    };

    let query = SelectStatement {
        ctes,
        distinct,
        lateral: false,
        fields,
        tables,
        join: joins,
        where_clause,
        group_by,
        having: having_expr,
        order,
        limit_clause,
        metadata: vec![],
    };

    Ok((query, params))
}

/// Build a CompoundSelectStatement from constraints containing a CompoundSelect variant.
///
/// Each branch's constraints are built against the already-resolved outer env
/// (schema bindings are shared). Any ORDER BY / LIMIT constraints alongside
/// the CompoundSelect in the outer list apply to the compound result.
pub(super) fn build_compound_select(
    env: &mut Env,
    constraints: &[Constraint],
    var_kinds: &[VarKind],
    operator: &CompoundSelectOperator,
    branches: &[Vec<Constraint>],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
) -> CompoundBuildResult {
    let mut all_params = Vec::new();
    let mut all_param_map: Vec<Option<usize>> = vec![None; var_kinds.len()];
    let mut selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)> = Vec::new();

    for (i, branch) in branches.iter().enumerate() {
        // Snapshot env before each branch so sibling branches resolve from
        // the same outer-schema state and don't observe one another's
        // local bindings (CTEs, subquery vars). DDL added by the branch
        // is preserved across the boundary so the outer caller sees every
        // statement the compound depends on.
        let pre = env.checkpoint();
        let (stmt, branch_params, branch_param_map) =
            build_select(env, branch, var_kinds, state, entropy)?;
        let new_ddl: Vec<_> = env.ddl_steps[pre.ddl_steps.len()..].to_vec();
        let new_tables: Vec<_> = env.new_tables[pre.new_tables.len()..].to_vec();
        env.restore(pre);
        env.ddl_steps.extend(new_ddl);
        env.new_tables.extend(new_tables);
        all_params.extend(branch_params);
        for (idx, slot) in branch_param_map.iter().enumerate() {
            if let Some(placeholder) = slot {
                debug_assert!(
                    all_param_map[idx].is_none(),
                    "param VarId {} double-inserted across compound branches",
                    idx
                );
                all_param_map[idx] = Some(*placeholder);
            }
        }

        let op = if i == 0 { None } else { Some(operator.clone()) };
        selects.push((op, stmt));
    }

    // Collect outer ORDER BY / LIMIT from the non-CompoundSelect constraints.
    let mut order_bys = Vec::new();
    let mut limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: None,
    };
    for c in constraints {
        match c {
            Constraint::OrderBy {
                col,
                table,
                direction,
                null_order,
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                order_bys.push(OrderBy {
                    field: FieldReference::Expr(make_column_expr(&col_name, &table_name)),
                    order_type: Some(*direction),
                    null_order: null_order.unwrap_or(NullOrder::NullsLast),
                });
            }
            Constraint::Limit { limit, offset } => {
                limit_clause = LimitClause::LimitOffset {
                    limit: Some(LimitValue::Literal(Literal::Integer(*limit as i64))),
                    offset: offset.map(|o| Literal::Integer(o as i64)),
                };
            }
            // CompoundSelect itself is consumed by the dispatch in
            // `resolve()`; schema/unification constraints don't produce AST
            // at the outer level either.
            Constraint::CompoundSelect { .. }
            | Constraint::BaseTable(_)
            | Constraint::AliasOf { .. }
            | Constraint::ColumnExists { .. }
            | Constraint::ColumnTypeClass { .. }
            | Constraint::TypeCompatible(_, _)
            | Constraint::Eq(_, _)
            | Constraint::NotEq(_, _) => {}
            // Anything else — outer-level predicates, projections, joins —
            // would need a wrapper SELECT around the compound that we don't
            // yet emit. Old code silently dropped them and produced SQL
            // missing the user-intended clauses; fail loudly instead.
            _ => {
                return Err(ResolveError::Unsupported {
                    variant: "non-OrderBy/Limit/schema constraint",
                    location: "outer compound SELECT",
                });
            }
        }
    }

    let order = if order_bys.is_empty() {
        None
    } else {
        Some(OrderClause {
            order_by: order_bys,
        })
    };

    let compound = CompoundSelectStatement {
        selects,
        order,
        limit_clause,
    };

    Ok((
        SelectSpecification::Compound(compound),
        all_params,
        all_param_map,
    ))
}

/// Get the effective name from a Table binding (alias if present, else table name).
fn get_table_name(env: &mut Env, t: VarId) -> Result<SqlIdentifier, ResolveError> {
    match env.get(t) {
        Some(binding) => binding
            .effective_table_name()
            .cloned()
            .ok_or(ResolveError::Unbound(t)),
        _ => Err(ResolveError::Unbound(t)),
    }
}

/// Get the physical table name (ignoring alias) from a Table binding.
pub(super) fn get_physical_table_name(
    env: &mut Env,
    t: VarId,
) -> Result<SqlIdentifier, ResolveError> {
    match env.get(t) {
        Some(Binding::Table { name, .. }) => Ok(name.clone()),
        _ => Err(ResolveError::Unbound(t)),
    }
}

/// Get (physical_table_name, alias) from a Table binding.
fn get_table_name_and_alias(
    env: &mut Env,
    t: VarId,
) -> Result<(SqlIdentifier, Option<SqlIdentifier>), ResolveError> {
    match env.get(t) {
        Some(Binding::Table { name, alias }) => Ok((name.clone(), alias.clone())),
        _ => Err(ResolveError::Unbound(t)),
    }
}

/// Get (column_name, table_name) from Column and Table bindings.
fn get_col_table(
    env: &mut Env,
    col: VarId,
    table: VarId,
) -> Result<(SqlIdentifier, SqlIdentifier), ResolveError> {
    let table_name = get_table_name(env, table)?;
    let col_name = match env.get(col) {
        Some(Binding::Column { name, .. }) => name.clone(),
        _ => return Err(ResolveError::Unbound(col)),
    };
    Ok((col_name, table_name))
}

/// Get the SQL type for a column binding.
fn get_col_type(env: &mut Env, col: VarId) -> Result<SqlType, ResolveError> {
    match env.get(col) {
        Some(Binding::Column { sql_type, .. }) => Ok(sql_type.clone()),
        _ => Err(ResolveError::Unbound(col)),
    }
}

/// SQL return type of an aggregate over a column of type `col_type`.
///
/// Used when building HAVING parameters: `aggregate_fn(col) op $param`
/// compares against the aggregate's RESULT, not the underlying column.
/// Returning the wrong type only manifests on Postgres (prepared-statement
/// type checking is strict); MySQL silently coerces.
///
/// Approximations:
/// - `Sum`/`Avg` collapse to BigInt/Numeric/Double respectively, ignoring
///   precision details; Postgres-correct enough that the param serializes,
///   and the comparison in MySQL still works after silent widening.
/// - `ArrayAgg` returns the column type; HAVING on an array is not a
///   shape the generator emits today, so this stays approximate.
fn aggregate_result_type(function: &AggregateFn, col_type: &SqlType) -> SqlType {
    use AggregateFn::*;
    match function {
        Count { .. } => SqlType::BigInt(None),
        Sum { .. } => match col_type {
            SqlType::TinyInt(_) | SqlType::SmallInt(_) | SqlType::Int(_) | SqlType::BigInt(_) => {
                SqlType::BigInt(None)
            }
            // For Float/Double, sum stays double-precision in both
            // dialects; for Numeric, postgres widens but we don't
            // generate Numeric columns today.
            _ => col_type.clone(),
        },
        Avg { .. } => match col_type {
            SqlType::Float | SqlType::Double => SqlType::Double,
            // Postgres avg(int) → numeric; we don't emit Numeric columns,
            // so default to Double and accept the small mismatch (PG will
            // coerce Double → Numeric on bind).
            _ => SqlType::Double,
        },
        Min | Max => col_type.clone(),
        GroupConcat | JsonObjectAgg => SqlType::Text,
        ArrayAgg => col_type.clone(),
    }
}

/// Get (col_name, table_name) from a Column binding, resolving the table
/// name lazily from the Table binding so that aliases applied after column
/// binding are reflected.
fn get_col_and_table_for_join(
    env: &mut Env,
    col: VarId,
) -> Result<(SqlIdentifier, SqlIdentifier), ResolveError> {
    let (col_name, table_var) = match env.get(col) {
        Some(Binding::Column {
            name, table_var, ..
        }) => (name.clone(), *table_var),
        _ => return Err(ResolveError::Unbound(col)),
    };
    let table_name = get_table_name(env, table_var)?;
    Ok((col_name, table_name))
}

/// Extract the column name from the first projected field of a SELECT.
/// Returns None if the SELECT has no fields or the first field is not a plain column.
fn first_projected_column(stmt: &SelectStatement) -> Option<SqlIdentifier> {
    match stmt.fields.first()? {
        FieldDefinitionExpr::Expr {
            expr: Expr::Column(col),
            ..
        } => Some(col.name.clone()),
        _ => None,
    }
}

/// Make a column expression: table.col
/// AND-merge `new_expr` into the existing optional expression. Used by both
/// `Having` and `HavingKeyFilter` so the two constraints compose
/// symmetrically — emitting one then the other (in either order) keeps both
/// predicates instead of silently overwriting.
fn and_merge(slot: &mut Option<Expr>, new_expr: Expr) {
    *slot = match slot.take() {
        Some(existing) => Some(Expr::BinaryOp {
            lhs: Box::new(existing),
            op: BinaryOperator::And,
            rhs: Box::new(new_expr),
        }),
        None => Some(new_expr),
    };
}

fn make_column_expr(col_name: &SqlIdentifier, table_name: &SqlIdentifier) -> Expr {
    Expr::Column(Column {
        name: col_name.clone(),
        table: Some(Relation {
            schema: None,
            name: table_name.clone(),
        }),
    })
}

/// Make a placeholder for a query parameter.
fn make_placeholder(dialect: Dialect, idx: &mut u32) -> ItemPlaceholder {
    let p = match dialect {
        Dialect::PostgreSQL => ItemPlaceholder::DollarNumber(*idx),
        _ => ItemPlaceholder::QuestionMark,
    };
    *idx += 1;
    p
}

/// Make a placeholder and record the VarId -> zero-based index mapping.
///
/// The zero-based index equals `*idx - 1` before the increment (i.e., the
/// first placeholder gets index 0, the second gets 1, etc.).
fn make_and_record_placeholder(
    dialect: Dialect,
    idx: &mut u32,
    param: VarId,
    param_map: &mut [Option<usize>],
) -> ItemPlaceholder {
    let zero_based = (*idx - 1) as usize;
    let p = make_placeholder(dialect, idx);
    param_map[param.0] = Some(zero_based);
    p
}

/// Resolve inner subquery constraints to produce a SelectStatement.
///
/// `outer_var_kinds` is the recipe-wide kind table indexed by absolute
/// VarId — passed through so inner resolution uses the authoritative
/// kind for every var referenced in the inner constraints, including
/// derived-relation aliases that may appear in nested scopes.
///
/// `placeholder_idx` is the shared parameter counter from the enclosing
/// `build_select_inner` so PostgreSQL `$N` placeholders stay globally
/// unique across the prepared statement.
#[allow(clippy::too_many_arguments)]
fn resolve_inner_subquery(
    inner_constraints: &[Constraint],
    outer_env: &mut Env,
    outer_var_kinds: &[VarKind],
    shared_vars: &[VarId],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    placeholder_idx: &mut u32,
    param_map: &mut [Option<usize>],
) -> Result<(SelectStatement, Vec<ParamMeta>, Vec<DdlStep>), ResolveError> {
    // Collect all VarIds referenced in inner constraints to size num_vars.
    let max_var = inner_constraints
        .iter()
        .flat_map(|c| c.var_ids())
        .map(|v| v.0)
        .max()
        .unwrap_or(0);
    let num_vars = max_var + 1;

    if num_vars > outer_var_kinds.len() {
        return Err(ResolveError::InnerVarKindsTruncated {
            referenced: num_vars,
            declared: outer_var_kinds.len(),
        });
    }

    // Use the outer var_kinds slice directly — it is authoritative for
    // every var in the recipe, including derived-relation aliases. The
    // inner constraints reference vars in the same namespace, so
    // var_kinds[v.0] is correct.
    let var_kinds: &[VarKind] = &outer_var_kinds[..num_vars];

    // Resolve schema constraints for the inner scope
    let (mut inner_env, expanded) =
        super::schema::resolve_schema(inner_constraints, var_kinds, state, entropy)?;

    // The inner env needs to know about tables created in the outer scope
    // so it doesn't emit redundant AddColumn DDL for them (those columns
    // are already included in the CreateTable built by into_ddl_steps).
    for t in &outer_env.new_tables {
        if !inner_env.new_tables.contains(t) {
            inner_env.new_tables.push(t.clone());
        }
    }

    // Propagate shared var bindings from outer to inner where inner is unbound.
    // Vars are not remapped at scope boundaries, so the inner-scope reference
    // is the same VarId as the outer one.
    for &v in shared_vars {
        if v.0 < num_vars
            && let Some(binding) = outer_env.get(v)
        {
            let binding = binding.clone();
            if !inner_env.is_bound(v) {
                inner_env.bind(v, binding);
            }
        }
    }

    // Use `expanded` (Or constraints replaced by their winning branch)
    // and the shared placeholder counter so PostgreSQL `$N` numbering
    // stays globally unique across the prepared statement.
    let (query, params) = build_select_inner(
        &mut inner_env,
        &expanded,
        var_kinds,
        state,
        entropy,
        placeholder_idx,
        param_map,
    )?;

    let ddl = inner_env.into_ddl_steps(state)?;
    Ok((query, params, ddl))
}

/// Resolve a CTE's inner subquery and additionally expose its
/// projected column bindings to the outer environment, retargeting
/// each propagated column's `table_var` to the CTE alias.
///
/// Outer references like `ProjectColumn(c, cte_alias)` and the
/// `left_col` of an outer JOIN whose left side comes from the CTE
/// must resolve to `<cte_alias>.<col_name>` rather than the
/// underlying base table; without this retarget, a CTE whose body
/// happens to reference a base table that the outer query also
/// references would emit references to the base table for those
/// vars instead of through the CTE alias.
///
/// Plain subqueries (EXISTS / IN / scalar) don't expose inner
/// columns by VarId, and join-subquery / join-CTE right columns are
/// resolved by the projected column name in the produced
/// `SelectStatement`, so neither form needs this propagation.
#[allow(clippy::too_many_arguments)]
fn resolve_cte_inner_subquery(
    inner_constraints: &[Constraint],
    outer_env: &mut Env,
    outer_var_kinds: &[VarKind],
    shared_vars: &[VarId],
    cte_alias_var: VarId,
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    placeholder_idx: &mut u32,
    param_map: &mut [Option<usize>],
) -> Result<(SelectStatement, Vec<ParamMeta>, Vec<DdlStep>), ResolveError> {
    let max_var = inner_constraints
        .iter()
        .flat_map(|c| c.var_ids())
        .map(|v| v.0)
        .max()
        .unwrap_or(0);
    let num_vars = max_var + 1;

    if num_vars > outer_var_kinds.len() {
        return Err(ResolveError::InnerVarKindsTruncated {
            referenced: num_vars,
            declared: outer_var_kinds.len(),
        });
    }

    let var_kinds: &[VarKind] = &outer_var_kinds[..num_vars];

    let (mut inner_env, expanded) =
        super::schema::resolve_schema(inner_constraints, var_kinds, state, entropy)?;

    for t in &outer_env.new_tables {
        if !inner_env.new_tables.contains(t) {
            inner_env.new_tables.push(t.clone());
        }
    }

    for &v in shared_vars {
        if v.0 < num_vars
            && let Some(binding) = outer_env.get(v)
        {
            let binding = binding.clone();
            if !inner_env.is_bound(v) {
                inner_env.bind(v, binding);
            }
        }
    }

    let (query, params) = build_select_inner(
        &mut inner_env,
        &expanded,
        var_kinds,
        state,
        entropy,
        placeholder_idx,
        param_map,
    )?;

    for v in 0..num_vars {
        let id = VarId(v);
        if outer_env.is_bound(id) {
            continue;
        }
        if let Some(Binding::Column { name, sql_type, .. }) = inner_env.get(id) {
            outer_env.bind(
                id,
                Binding::Column {
                    name: name.clone(),
                    sql_type: sql_type.clone(),
                    table_var: cte_alias_var,
                },
            );
        }
    }

    let ddl = inner_env.into_ddl_steps(state)?;
    Ok((query, params, ddl))
}

/// Convert an AggregateFn constraint to a FunctionExpr AST node.
fn make_aggregate_expr(function: &AggregateFn, col_expr: Expr) -> FunctionExpr {
    match function {
        AggregateFn::Count { distinct } => FunctionExpr::Count {
            expr: Box::new(col_expr),
            distinct: *distinct,
        },
        AggregateFn::Sum { distinct } => FunctionExpr::Sum {
            expr: Box::new(col_expr),
            distinct: *distinct,
        },
        AggregateFn::Avg { distinct } => FunctionExpr::Avg {
            expr: Box::new(col_expr),
            distinct: *distinct,
        },
        AggregateFn::Min => FunctionExpr::Min(Box::new(col_expr)),
        AggregateFn::Max => FunctionExpr::Max(Box::new(col_expr)),
        AggregateFn::GroupConcat => FunctionExpr::Udf {
            schema: None,
            name: SqlIdentifier::from("GROUP_CONCAT"),
            arguments: vec![col_expr],
        },
        AggregateFn::JsonObjectAgg => FunctionExpr::Udf {
            schema: None,
            name: SqlIdentifier::from("JSON_OBJECT_AGG"),
            arguments: vec![col_expr],
        },
        AggregateFn::ArrayAgg => FunctionExpr::Udf {
            schema: None,
            name: SqlIdentifier::from("ARRAY_AGG"),
            arguments: vec![col_expr],
        },
    }
}

/// Default literal of the right shape for `ty`. Used when `Coalesce` /
/// `IfNull` / `NullIf` need a fallback value to plug into the function
/// call — `Integer(0)` works on integer columns but Postgres rejects it
/// for text/date/time columns at parse time and MySQL silently coerces
/// it, so the comparison oracle drifts.
fn default_literal_for(ty: &SqlType) -> Literal {
    use SqlType::*;
    match ty {
        Bool => Literal::Boolean(false),
        Char(_) | VarChar(_) | Text | TinyText | MediumText | LongText | Citext | QuotedChar => {
            Literal::String(String::new())
        }
        Int(_) | Signed | Unsigned | UnsignedInteger | SignedInteger | IntUnsigned(_)
        | BigInt(_) | BigIntUnsigned(_) | TinyInt(_) | TinyIntUnsigned(_) | SmallInt(_)
        | SmallIntUnsigned(_) | MediumInt(_) | MediumIntUnsigned(_) | Int2 | Int4 | Int8
        | Serial | BigSerial => Literal::Integer(0),
        Double | Float | Real => Literal::Number("0.0".into()),
        Numeric(_) | Decimal(_, _) => Literal::Number("0".into()),
        Date => Literal::String("2000-01-01".into()),
        DateTime(_) | Timestamp => Literal::String("2000-01-01 00:00:00".into()),
        TimestampTz => Literal::String("2000-01-01 00:00:00+00".into()),
        Time => Literal::String("00:00:00".into()),
        Json | Jsonb => Literal::String("{}".into()),
        Blob | LongBlob | MediumBlob | TinyBlob | Binary(_) | VarBinary(_) | ByteArray => {
            Literal::ByteArray(Vec::new())
        }
        Bit(_) | VarBit(_) => Literal::Null,
        Enum(_)
        | Interval { .. }
        | Array(_)
        | MacAddr
        | Inet
        | Uuid
        | Tsvector
        | Point
        | PostgisPoint
        | PostgisPolygon
        | Other(_) => Literal::Null,
    }
}

/// Build an `Expr` for a scalar function call from a `ScalarFn` specifier
/// and resolved column arguments. `col_types[i]` corresponds to `args[i]`
/// — used by `Coalesce`/`IfNull`/`NullIf` to inject a fallback literal of
/// the right type instead of a hard-coded `Integer(0)`.
fn make_scalar_fn_expr(
    function: &ScalarFn,
    args: Vec<Expr>,
    col_types: &[SqlType],
    dialect: Dialect,
) -> Expr {
    let primary_default = col_types
        .first()
        .map(default_literal_for)
        .unwrap_or(Literal::Null);
    // Most scalar functions map to a UDF call with the function name.
    // Some need special handling for argument shape.
    match function {
        ScalarFn::Coalesce => {
            // COALESCE(col, <default>) — fallback matches the column type.
            let mut arguments = args;
            arguments.push(Expr::Literal(primary_default));
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("COALESCE"),
                arguments,
            })
        }
        ScalarFn::IfNull => {
            let mut arguments = args;
            arguments.push(Expr::Literal(primary_default));
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("IFNULL"),
                arguments,
            })
        }
        ScalarFn::NullIf => {
            let mut arguments = args;
            arguments.push(Expr::Literal(primary_default));
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("NULLIF"),
                arguments,
            })
        }
        ScalarFn::Cast => {
            // CAST(col AS CHAR) — use the first column arg
            let col_expr = args
                .into_iter()
                .next()
                .unwrap_or(Expr::Literal(Literal::Null));
            let target_type = if dialect == Dialect::MySQL {
                SqlType::Char(Some(255))
            } else {
                SqlType::Text
            };
            Expr::Cast {
                expr: Box::new(col_expr),
                ty: target_type,
                style: readyset_sql::ast::CastStyle::As,
                array: false,
            }
        }
        ScalarFn::Upper | ScalarFn::Lower | ScalarFn::Trim | ScalarFn::Length => {
            let name = match function {
                ScalarFn::Upper => "UPPER",
                ScalarFn::Lower => "LOWER",
                ScalarFn::Trim => "TRIM",
                _ => "LENGTH",
            };
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from(name),
                arguments: args,
            })
        }
        ScalarFn::Substring => {
            // SUBSTRING(col, 1, 10)
            let mut arguments = args;
            arguments.push(Expr::Literal(Literal::Integer(1)));
            arguments.push(Expr::Literal(Literal::Integer(10)));
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("SUBSTRING"),
                arguments,
            })
        }
        ScalarFn::Concat => Expr::Call(FunctionExpr::Udf {
            schema: None,
            name: SqlIdentifier::from("CONCAT"),
            arguments: args,
        }),
        ScalarFn::ConcatWs => {
            // CONCAT_WS(',', col1, col2)
            let mut arguments = vec![Expr::Literal(Literal::String(",".to_string()))];
            arguments.extend(args);
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("CONCAT_WS"),
                arguments,
            })
        }
        ScalarFn::Round => {
            // ROUND(col) or ROUND(col, 2)
            let mut arguments = args;
            arguments.push(Expr::Literal(Literal::Integer(2)));
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from("ROUND"),
                arguments,
            })
        }
        ScalarFn::Month | ScalarFn::DayOfWeek => {
            let name = match function {
                ScalarFn::Month => "MONTH",
                _ => "DAYOFWEEK",
            };
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from(name),
                arguments: args,
            })
        }
        ScalarFn::Greatest | ScalarFn::Least => {
            let name = match function {
                ScalarFn::Greatest => "GREATEST",
                _ => "LEAST",
            };
            Expr::Call(FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from(name),
                arguments: args,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::ast::{BinaryOperator, SqlType};
    use readyset_sql::{Dialect, DialectDisplay};

    use readyset_sql::ast::CompoundSelectOperator;

    use crate::constraint::{AggregateFn, Constraint, ScalarFn, SubqueryExprKind, TypeClass};
    use crate::entropy::Entropy;
    use crate::resolver::{ResolveError, ResolverOutput, resolve};
    use crate::state::{GenerationState, GeneratorConfig};
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

    /// Helper: run full resolve pipeline on given constraints and return SQL string.
    fn resolve_to_sql(
        constraints: Vec<Constraint>,
        var_kinds: Vec<VarKind>,
        dialect: Dialect,
    ) -> (String, ResolverOutput) {
        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(dialect, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output =
            resolve(&constraints, &var_kinds, &mut state, &mut entropy).expect("should resolve");
        let sql = output.query.display(dialect).to_string();
        (sql, output)
    }

    /// Variant of `resolve_to_sql` that returns the `ResolveError` for tests
    /// that assert on a typed failure rather than successful resolution.
    fn resolve_to_err(
        constraints: Vec<Constraint>,
        var_kinds: Vec<VarKind>,
        dialect: Dialect,
    ) -> ResolveError {
        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(dialect, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        resolve(&constraints, &var_kinds, &mut state, &mut entropy).expect_err("expected Err")
    }

    #[test]
    fn having_count_param_sql_type_is_bigint_not_column_type() {
        // Regression: the HAVING param used to inherit the underlying
        // column's sql_type, but the comparison operand on the LHS is the
        // aggregate's RESULT (e.g., COUNT(c) returns BigInt). On Postgres
        // this caused `error serializing parameter: <col_type> → Int8`
        // when the column wasn't already an integer; MySQL silently
        // coerced. Param sql_type for HAVING must come from the aggregate
        // function's return type, not the column.
        use crate::constraint::AggregateFn;
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            // Force the column to a non-integer type so the bug is visible
            // even when MySQL would coerce.
            Constraint::ColumnTypeClass {
                col: c,
                type_class: TypeClass::DateTime,
            },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::ProjectAggregate {
                function: AggregateFn::Count { distinct: false },
                col: c,
                table: t,
            },
            Constraint::GroupBy { col: c, table: t },
            Constraint::Having {
                function: AggregateFn::Count { distinct: false },
                col: c,
                table: t,
                op: BinaryOperator::Greater,
                param: VarId(2),
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Param { col: c },
        ];
        let (_sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);
        assert_eq!(
            output.params.len(),
            1,
            "HAVING contributes exactly one param"
        );
        assert!(
            matches!(output.params[0].sql_type, SqlType::BigInt(_)),
            "HAVING param for COUNT must be BigInt; got {:?}",
            output.params[0].sql_type
        );
    }

    #[test]
    fn having_then_having_key_filter_emits_both_via_and_merge() {
        // Order A → B: Having first, HavingKeyFilter second.
        use crate::constraint::AggregateFn;
        let t = VarId(0);
        let c_grp = VarId(1);
        let c_agg = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists {
                col: c_grp,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_agg,
                table: t,
            },
            Constraint::From(t),
            Constraint::ProjectColumn {
                col: c_grp,
                table: t,
            },
            Constraint::ProjectAggregate {
                function: AggregateFn::Count { distinct: false },
                col: c_agg,
                table: t,
            },
            Constraint::GroupBy {
                col: c_grp,
                table: t,
            },
            Constraint::Having {
                function: AggregateFn::Count { distinct: false },
                col: c_agg,
                table: t,
                op: BinaryOperator::Greater,
                param: VarId(3),
            },
            Constraint::HavingKeyFilter {
                col: c_grp,
                table: t,
                op: BinaryOperator::Equal,
                param: VarId(4),
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c_agg },
            VarKind::Param { col: c_grp },
        ];
        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);
        assert!(
            sql.contains("HAVING") && sql.contains("AND"),
            "must AND-merge both into a single HAVING clause; sql: {sql}"
        );
        assert!(
            sql.to_lowercase().contains("count("),
            "Having (COUNT) must be present; sql: {sql}"
        );
    }

    #[test]
    fn having_key_filter_then_having_emits_both_via_and_merge() {
        // Order B → A: HavingKeyFilter first, Having second. The old
        // resolver would overwrite having_expr in this order.
        use crate::constraint::AggregateFn;
        let t = VarId(0);
        let c_grp = VarId(1);
        let c_agg = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists {
                col: c_grp,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_agg,
                table: t,
            },
            Constraint::From(t),
            Constraint::ProjectColumn {
                col: c_grp,
                table: t,
            },
            Constraint::ProjectAggregate {
                function: AggregateFn::Count { distinct: false },
                col: c_agg,
                table: t,
            },
            Constraint::GroupBy {
                col: c_grp,
                table: t,
            },
            Constraint::HavingKeyFilter {
                col: c_grp,
                table: t,
                op: BinaryOperator::Equal,
                param: VarId(3),
            },
            Constraint::Having {
                function: AggregateFn::Count { distinct: false },
                col: c_agg,
                table: t,
                op: BinaryOperator::Greater,
                param: VarId(4),
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c_grp },
            VarKind::Param { col: c_agg },
        ];
        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);
        assert!(
            sql.contains("HAVING") && sql.contains("AND"),
            "must AND-merge both into a single HAVING clause; sql: {sql}"
        );
        assert!(
            sql.to_lowercase().contains("count("),
            "Having (COUNT) must be present; sql: {sql}"
        );
    }

    #[test]
    fn coalesce_default_literal_matches_text_column_type() {
        // Postgres rejects `COALESCE(text_col, 0)` at parse time and MySQL
        // silently coerces. The default literal must match the column type.
        let t = VarId(0);
        let c = VarId(1);
        let mut state = GenerationState::new(
            Dialect::PostgreSQL,
            GeneratorConfig {
                reuse_preference: 0.0,
                ..Default::default()
            },
        );
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // Force the column to be Text type.
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::ColumnTypeClass {
                col: c,
                type_class: TypeClass::String,
            },
            Constraint::From(t),
            Constraint::ProjectFunction {
                function: ScalarFn::Coalesce,
                args: vec![(c, t)],
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];
        let output =
            resolve(&constraints, &var_kinds, &mut state, &mut entropy).expect("should resolve");
        let sql = output.query.display(Dialect::PostgreSQL).to_string();
        assert!(
            sql.contains("''") || sql.contains("\"\""),
            "expected an empty-string default in COALESCE on a text column; sql: {sql}"
        );
        assert!(
            !sql.contains("COALESCE(\"t0\".\"c0\", 0)"),
            "must not inject Integer(0) on text column; sql: {sql}"
        );
    }

    #[test]
    fn nullif_default_literal_matches_integer_column_type() {
        // Integer columns continue to get Integer(0) — no regression.
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::ColumnTypeClass {
                col: c,
                type_class: TypeClass::Numeric,
            },
            Constraint::From(t),
            Constraint::ProjectFunction {
                function: ScalarFn::NullIf,
                args: vec![(c, t)],
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];
        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);
        assert!(
            sql.contains(", 0)"),
            "expected Integer(0) for numeric column; sql: {sql}"
        );
    }

    #[test]
    fn where_or_with_unsupported_inner_constraint_returns_typed_error() {
        // Plain `Eq` is a unification constraint and should never appear
        // inside a `WhereOr`; the old `_ => {}` arm silently dropped it and
        // emitted a partial OR. The fix should surface a typed error.
        let t = VarId(0);
        let c = VarId(1);
        let p = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::WhereOr {
                conditions: vec![
                    Constraint::WhereParam {
                        col: c,
                        table: t,
                        op: BinaryOperator::Equal,
                        param: p,
                    },
                    Constraint::Eq(c, c),
                ],
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Param { col: c },
        ];
        let err = resolve_to_err(constraints, var_kinds, Dialect::MySQL);
        assert!(
            matches!(err, ResolveError::Unsupported { location, .. } if location == "WhereOr"),
            "expected Unsupported{{location: WhereOr}}, got {err:?}"
        );
    }

    #[test]
    fn compound_select_with_outer_predicate_returns_typed_error() {
        // The compound resolver only honors outer ORDER BY / LIMIT plus
        // schema metadata; an outer-level WhereParam used to be silently
        // dropped, producing SQL missing the user-intended filter.
        let t = VarId(0);
        let c = VarId(1);
        let p = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::CompoundSelect {
                operator: CompoundSelectOperator::UnionAll,
                branches: vec![
                    vec![
                        Constraint::From(t),
                        Constraint::ProjectColumn { col: c, table: t },
                    ],
                    vec![
                        Constraint::From(t),
                        Constraint::ProjectColumn { col: c, table: t },
                    ],
                ],
            },
            // Predicates at the outer level have no place to land — would
            // require a wrapper SELECT around the compound.
            Constraint::WhereParam {
                col: c,
                table: t,
                op: BinaryOperator::Equal,
                param: p,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Param { col: c },
        ];
        let err = resolve_to_err(constraints, var_kinds, Dialect::MySQL);
        assert!(
            matches!(
                err,
                ResolveError::Unsupported { location, .. }
                    if location == "outer compound SELECT"
            ),
            "expected Unsupported{{location: outer compound SELECT}}, got {err:?}"
        );
    }

    #[test]
    fn multiple_from_constraints_emit_cross_join_not_comma_list() {
        use readyset_sql::ast::JoinOperator;

        // Two FROM relations followed by a JOIN whose ON clause references the
        // *first* FROM relation. Comma-separated FROM (`FROM t0, t1 INNER JOIN
        // t2 ON t0.c = t2.c`) puts t0 *outside* the JOIN's lexical scope —
        // MySQL rejects with `1054 Unknown column 't0.c' in 'on clause'`.
        // The fix turns extra FROMs into chained CROSS JOINs so the entire
        // tree is one scope.
        let t0 = VarId(0);
        let t1 = VarId(1);
        let t2 = VarId(2);
        let c0 = VarId(3); // t0's join column
        let c2 = VarId(4); // t2's join column
        let constraints = vec![
            Constraint::BaseTable(t0),
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::ColumnExists { col: c0, table: t0 },
            Constraint::ColumnExists { col: c2, table: t2 },
            Constraint::ColumnTypeClass {
                col: c0,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c2,
                type_class: TypeClass::Integer,
            },
            Constraint::From(t0),
            Constraint::From(t1),
            Constraint::ProjectColumn { col: c0, table: t0 },
            Constraint::Join {
                operator: JoinOperator::InnerJoin,
                right: crate::constraint::JoinRight::Table(t2),
                left_col: c0,
                right_col: c2,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Column { table: t0 },
            VarKind::Column { table: t2 },
        ];

        let (sql, _output) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        // Two `From` constraints must render as `FROM tA CROSS JOIN tB INNER
        // JOIN tC ON ...`, never comma-separated. Comma-FROM puts the first
        // relation outside the JOIN's lexical scope and MySQL rejects with
        // 1054 "Unknown column".
        assert!(
            sql.contains("CROSS JOIN"),
            "expected CROSS JOIN for second FROM: sql={sql}"
        );
        assert!(
            !sql.contains("`, `") && !sql.contains("`,`"),
            "rendered SQL must not have comma-separated FROM list: sql={sql}"
        );
    }

    #[test]
    fn ast_simple_select_from() {
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        // Should produce: SELECT t0.c0 FROM t0
        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("FROM"), "sql: {sql}");
        assert!(!output.ddl.is_empty()); // at least CreateTable
        assert!(output.params.is_empty());
    }

    #[test]
    fn ast_select_with_where_param() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let p = VarId(3);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereParam {
                col: c2,
                table: t,
                op: BinaryOperator::Equal,
                param: p,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c2 },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("WHERE"), "sql: {sql}");
        assert!(sql.contains("= ?"), "sql: {sql}");
        assert_eq!(output.params.len(), 1);
    }

    #[test]
    fn ast_postgres_dollar_placeholders() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let p = VarId(3);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereParam {
                col: c2,
                table: t,
                op: BinaryOperator::Equal,
                param: p,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c2 },
        ];

        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);

        assert!(sql.contains("$1"), "sql: {sql}");
    }

    #[test]
    fn ast_multiple_where_params_sequential_numbering() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let c3 = VarId(3);
        let p0 = VarId(4);
        let p1 = VarId(5);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::ColumnExists { col: c3, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereParam {
                col: c2,
                table: t,
                op: BinaryOperator::Equal,
                param: p0,
            },
            Constraint::WhereParam {
                col: c3,
                table: t,
                op: BinaryOperator::Greater,
                param: p1,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c2 },
            VarKind::Param { col: c3 },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);

        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$2"), "sql: {sql}");
        assert_eq!(output.params.len(), 2);
    }

    #[test]
    fn ast_join() {
        let t1 = VarId(0);
        let t2 = VarId(1);
        let c1 = VarId(2);
        let c2 = VarId(3);
        let c_proj = VarId(4);
        let constraints = vec![
            Constraint::BaseTable(t1),
            Constraint::BaseTable(t2),
            Constraint::ColumnExists { col: c1, table: t1 },
            Constraint::ColumnExists { col: c2, table: t2 },
            Constraint::ColumnExists {
                col: c_proj,
                table: t1,
            },
            Constraint::ColumnTypeClass {
                col: c1,
                type_class: TypeClass::Integer,
            },
            Constraint::ColumnTypeClass {
                col: c2,
                type_class: TypeClass::Integer,
            },
            Constraint::TypeCompatible(c1, c2),
            Constraint::From(t1),
            Constraint::Join {
                operator: readyset_sql::ast::JoinOperator::InnerJoin,
                right: crate::constraint::JoinRight::Table(t2),
                left_col: c1,
                right_col: c2,
            },
            Constraint::ProjectColumn {
                col: c_proj,
                table: t1,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Relation,
            VarKind::Column { table: t1 },
            VarKind::Column { table: t2 },
            VarKind::Column { table: t1 },
        ];

        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
    }

    #[test]
    fn ast_aggregate_with_group_by() {
        let t = VarId(0);
        let c_agg = VarId(1);
        let c_group = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists {
                col: c_agg,
                table: t,
            },
            Constraint::ColumnExists {
                col: c_group,
                table: t,
            },
            Constraint::From(t),
            Constraint::ProjectAggregate {
                function: AggregateFn::Count { distinct: false },
                col: c_agg,
                table: t,
            },
            Constraint::ProjectColumn {
                col: c_group,
                table: t,
            },
            Constraint::GroupBy {
                col: c_group,
                table: t,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "sql: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "sql: {sql}");
    }

    #[test]
    fn ast_order_by_and_limit() {
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::OrderBy {
                col: c,
                table: t,
                direction: readyset_sql::ast::OrderType::OrderAscending,
                null_order: None,
            },
            Constraint::Limit {
                limit: 10,
                offset: None,
            },
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("LIMIT"), "sql: {sql}");
        assert!(sql.contains("10"), "sql: {sql}");
    }

    #[test]
    fn ast_limit_overflow_is_reported_not_wrapped() {
        let t = VarId(0);
        let c = VarId(1);
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let limit_too_big = (i64::MAX as u64) + 1;
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::Limit {
                limit: limit_too_big,
                offset: None,
            },
        ];

        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let err = resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect_err("limit > i64::MAX must not silently wrap");
        assert!(
            matches!(err, ResolveError::LimitOverflow { value } if value == limit_too_big),
            "expected LimitOverflow, got: {err:?}"
        );
    }

    #[test]
    fn ast_limit_offset_overflow_is_reported() {
        let t = VarId(0);
        let c = VarId(1);
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let offset_too_big = u64::MAX;
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::Limit {
                limit: 10,
                offset: Some(offset_too_big),
            },
        ];

        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let err = resolve(&constraints, &var_kinds, &mut state, &mut entropy)
            .expect_err("offset > i64::MAX must not silently wrap");
        assert!(
            matches!(err, ResolveError::LimitOverflow { value } if value == offset_too_big),
            "expected LimitOverflow, got: {err:?}"
        );
    }

    #[test]
    fn ast_distinct() {
        let t = VarId(0);
        let c = VarId(1);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c, table: t },
            Constraint::Distinct,
        ];
        let var_kinds = vec![VarKind::Relation, VarKind::Column { table: t }];

        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(sql.contains("DISTINCT"), "sql: {sql}");
    }

    #[test]
    fn ast_where_is_null() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereIsNull {
                col: c2,
                table: t,
                negated: false,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);

        assert!(sql.contains("IS NULL"), "sql: {sql}");
        assert!(output.params.is_empty()); // IS NULL doesn't use params
    }

    #[test]
    fn ast_where_in_param() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereInParam {
                col: c2,
                table: t,
                num_values: 3,
                params: vec![VarId(3), VarId(4), VarId(5)],
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c2 },
            VarKind::Param { col: c2 },
            VarKind::Param { col: c2 },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);

        assert!(sql.contains("IN"), "sql: {sql}");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$3"), "sql: {sql}");
        assert_eq!(output.params.len(), 1);
        assert_eq!(output.params[0].count, 3);
    }

    #[test]
    fn ast_between_param() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereBetweenParam {
                col: c2,
                table: t,
                lo: VarId(3),
                hi: VarId(4),
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c2 },
            VarKind::Param { col: c2 },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);

        assert!(sql.contains("BETWEEN"), "sql: {sql}");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$2"), "sql: {sql}");
        assert_eq!(output.params.len(), 1);
        assert_eq!(output.params[0].count, 2);
    }

    // -- Subquery / CTE resolution tests --

    #[test]
    fn ast_join_subquery() {
        use crate::pattern::PatternBuilder;

        let mut b = PatternBuilder::new("join_subquery");
        let t_outer = b.table();
        let c_outer_proj = b.column(t_outer);
        let c_outer_join = b.column(t_outer);

        let mut sq = b.subquery();
        let t_inner = sq.table();
        let c_inner = sq.column(t_inner);
        sq.from(t_inner);
        sq.project_column(c_inner, t_inner);
        sq.commit_as_join(
            readyset_sql::ast::JoinOperator::InnerJoin,
            c_outer_join,
            c_inner,
        );

        b.from(t_outer);
        b.project_column(c_outer_proj, t_outer);

        let pattern = b.build();

        let (sql, _output) = resolve_to_sql(pattern.constraints, pattern.vars, Dialect::MySQL);

        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn ast_where_exists_subquery() {
        use crate::constraint::SubqueryExprKind;
        use crate::pattern::PatternBuilder;

        let mut b = PatternBuilder::new("exists_sq");
        let t_outer = b.table();
        let c_outer = b.column(t_outer);

        let mut sq = b.subquery();
        let t_inner = sq.table();
        let c_inner = sq.column(t_inner);
        sq.from(t_inner);
        sq.project_column(c_inner, t_inner);
        sq.commit_as_where(SubqueryExprKind::ExistsUncorrelated);

        b.from(t_outer);
        b.project_column(c_outer, t_outer);

        let pattern = b.build();

        let (sql, _) = resolve_to_sql(pattern.constraints, pattern.vars, Dialect::MySQL);

        assert!(sql.contains("EXISTS"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn ast_where_in_subquery() {
        use crate::constraint::SubqueryExprKind;
        use crate::pattern::PatternBuilder;

        let mut b = PatternBuilder::new("in_sq");
        let t_outer = b.table();
        let c_outer_proj = b.column(t_outer);
        let c_outer_in = b.column(t_outer);

        let mut sq = b.subquery();
        let t_inner = sq.table();
        let c_inner = sq.column(t_inner);
        sq.from(t_inner);
        sq.project_column(c_inner, t_inner);
        // Reference the outer column inside the inner scope to create a shared var
        sq.constraint(Constraint::TypeCompatible(c_outer_in, c_inner));
        sq.commit_as_where(SubqueryExprKind::InSubquery);

        b.from(t_outer);
        b.project_column(c_outer_proj, t_outer);

        let pattern = b.build();

        let (sql, _) = resolve_to_sql(pattern.constraints, pattern.vars, Dialect::MySQL);

        assert!(sql.contains("IN"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn ast_cte() {
        use crate::pattern::PatternBuilder;

        let mut b = PatternBuilder::new("cte_test");

        let mut sq = b.subquery();
        let t = sq.table();
        let c = sq.column(t);
        sq.from(t);
        sq.project_column(c, t);
        let cte_alias = sq.commit_as_cte();

        // Outer query: project from the CTE alias.
        b.from(cte_alias);
        b.project_column(c, cte_alias);

        let pattern = b.build();

        let (sql, _) = resolve_to_sql(pattern.constraints, pattern.vars, Dialect::MySQL);

        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn in_subquery_with_empty_shared_vars_returns_error() {
        let t_outer = VarId(0);
        let c_outer = VarId(1);
        let t_inner = VarId(2);
        let c_inner = VarId(3);

        let constraints = vec![
            Constraint::BaseTable(t_outer),
            Constraint::BaseTable(t_inner),
            Constraint::ColumnExists {
                col: c_outer,
                table: t_outer,
            },
            Constraint::ColumnExists {
                col: c_inner,
                table: t_inner,
            },
            Constraint::From(t_outer),
            Constraint::ProjectColumn {
                col: c_outer,
                table: t_outer,
            },
            Constraint::SubqueryExpr {
                kind: SubqueryExprKind::InSubquery,
                constraints: vec![
                    Constraint::BaseTable(t_inner),
                    Constraint::ColumnExists {
                        col: c_inner,
                        table: t_inner,
                    },
                    Constraint::From(t_inner),
                    Constraint::ProjectColumn {
                        col: c_inner,
                        table: t_inner,
                    },
                ],
                shared_vars: vec![], // empty — should error, not silently degrade to EXISTS
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t_outer },
            VarKind::Relation,
            VarKind::Column { table: t_inner },
        ];

        let (mut state, mut rng) = test_env(Dialect::MySQL);
        let mut entropy = Entropy::new(&mut rng);

        let result = resolve(&constraints, &var_kinds, &mut state, &mut entropy);
        assert!(
            result.is_err(),
            "InSubquery with empty shared_vars should return an error, not silently degrade to EXISTS"
        );
    }

    #[test]
    fn ast_where_or() {
        let t = VarId(0);
        let c1 = VarId(1);
        let c2 = VarId(2);
        let p0 = VarId(3);
        let p1 = VarId(4);
        let constraints = vec![
            Constraint::BaseTable(t),
            Constraint::ColumnExists { col: c1, table: t },
            Constraint::ColumnExists { col: c2, table: t },
            Constraint::From(t),
            Constraint::ProjectColumn { col: c1, table: t },
            Constraint::WhereOr {
                conditions: vec![
                    Constraint::WhereParam {
                        col: c1,
                        table: t,
                        op: BinaryOperator::Equal,
                        param: p0,
                    },
                    Constraint::WhereParam {
                        col: c2,
                        table: t,
                        op: BinaryOperator::Greater,
                        param: p1,
                    },
                ],
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Param { col: c1 },
            VarKind::Param { col: c2 },
        ];
        let (sql, _) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);
        assert!(sql.contains("OR"), "expected OR in: {sql}");
    }

    #[test]
    fn ast_compound_select_union_all() {
        // Two branches: SELECT t0.c0 FROM t0 UNION ALL SELECT t1.c1 FROM t1
        let t0 = VarId(0);
        let c0 = VarId(1);
        let t1 = VarId(2);
        let c1 = VarId(3);

        let constraints = vec![
            Constraint::BaseTable(t0),
            Constraint::ColumnExists { col: c0, table: t0 },
            Constraint::BaseTable(t1),
            Constraint::ColumnExists { col: c1, table: t1 },
            Constraint::CompoundSelect {
                operator: CompoundSelectOperator::UnionAll,
                branches: vec![
                    vec![
                        Constraint::From(t0),
                        Constraint::ProjectColumn { col: c0, table: t0 },
                    ],
                    vec![
                        Constraint::From(t1),
                        Constraint::ProjectColumn { col: c1, table: t1 },
                    ],
                ],
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t0 },
            VarKind::Relation,
            VarKind::Column { table: t1 },
        ];
        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::MySQL);
        assert!(sql.contains("UNION ALL"), "expected UNION ALL in: {sql}");
        assert!(
            matches!(
                output.query,
                readyset_sql::ast::SelectSpecification::Compound(_)
            ),
            "expected Compound variant, got Simple"
        );
    }
}
