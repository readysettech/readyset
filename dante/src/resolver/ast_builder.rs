use data_generator::ColumnGenerationSpec;
use readyset_sql::Dialect;
use readyset_sql::ast::{
    BinaryOperator, Column, CommonTableExpr, Expr, FieldDefinitionExpr, FieldReference,
    FunctionExpr, GroupByClause, InValue, ItemPlaceholder, JoinClause, JoinConstraint,
    JoinRightSide, LimitClause, LimitValue, Literal, NullOrder, OrderBy, OrderClause, OrderType,
    Relation, SelectStatement, SqlIdentifier, SqlType, TableExpr, TableExprInner,
};

use super::{Binding, DdlStep, Env, ParamMeta, ResolveError};
use crate::constraint::{AggregateFn, Constraint, LiteralKind, SubqueryPosition, WindowFn};
use crate::entropy::Entropy;
use crate::state::GenerationState;
use crate::var::{VarId, VarKind};

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
) -> Result<(SelectStatement, Vec<ParamMeta>), ResolveError> {
    let mut placeholder_idx: u32 = 1;
    build_select_inner(
        env,
        constraints,
        var_kinds,
        state,
        entropy,
        &mut placeholder_idx,
    )
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
    let mut limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: None,
    };
    let mut distinct = false;
    let mut params = Vec::new();

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
            Constraint::WhereParam { col, table, op } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let placeholder = make_placeholder(dialect, placeholder_idx);
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
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let mut placeholders = Vec::new();
                for _ in 0..*num_values {
                    placeholders.push(Expr::Literal(Literal::Placeholder(make_placeholder(
                        dialect,
                        placeholder_idx,
                    ))));
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
            Constraint::WhereRangeParam { col, table } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let p1 = make_placeholder(dialect, placeholder_idx);
                let p2 = make_placeholder(dialect, placeholder_idx);
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
            Constraint::WhereBetweenParam { col, table } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let p1 = make_placeholder(dialect, placeholder_idx);
                let p2 = make_placeholder(dialect, placeholder_idx);
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
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_type = get_col_type(env, *col)?;
                let placeholder = make_placeholder(dialect, placeholder_idx);
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
            Constraint::Join {
                operator,
                right,
                left_col,
                right_col,
            } => {
                let (lc_name, lt_name) = get_col_and_table_for_join(env, *left_col)?;

                // The ON clause's right column / right table varies by
                // the kind of right-hand side. For a plain table join we
                // resolve them up front; for a subquery or CTE we defer
                // to the inner resolution, which both produces the right
                // SQL for the join target and (via column-binding
                // propagation in `resolve_inner_subquery`) makes inner
                // column VarIds visible to the outer scope so the ON
                // clause can name them.
                let rc_name;
                let effective_rt_name;

                let right_side = match right {
                    crate::constraint::JoinRight::Table(t) => {
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
                    }
                    crate::constraint::JoinRight::Subquery {
                        constraints: inner_constraints,
                        shared_vars,
                    } => {
                        let alias = state.fresh_subquery_alias();
                        let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                            inner_constraints,
                            env,
                            var_kinds,
                            shared_vars,
                            state,
                            entropy,
                            placeholder_idx,
                        )?;
                        params.extend(inner_params);
                        env.ddl_steps.extend(inner_ddl);
                        effective_rt_name = alias.clone();
                        // The ON clause must reference the column that the
                        // subquery actually projects, not the outer env's
                        // independent resolution of the same VarId.
                        rc_name = first_projected_column(&inner_query).ok_or_else(|| {
                            ResolveError::TypeMismatch {
                                expected: "join subquery to project at least one column".into(),
                                actual: "no projected column".into(),
                            }
                        })?;
                        JoinRightSide::Table(TableExpr {
                            inner: TableExprInner::Subquery(Box::new(inner_query)),
                            alias: Some(alias),
                            column_aliases: vec![],
                        })
                    }
                    crate::constraint::JoinRight::Cte {
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
                        let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                            inner_constraints,
                            env,
                            var_kinds,
                            shared_vars,
                            state,
                            entropy,
                            placeholder_idx,
                        )?;
                        params.extend(inner_params);
                        env.ddl_steps.extend(inner_ddl);
                        rc_name = first_projected_column(&inner_query).ok_or_else(|| {
                            ResolveError::TypeMismatch {
                                expected: "join CTE to project at least one column".into(),
                                actual: "no projected column".into(),
                            }
                        })?;
                        ctes.push(CommonTableExpr {
                            name: cte_alias.clone(),
                            statement: inner_query,
                        });
                        effective_rt_name = cte_alias.clone();
                        JoinRightSide::Table(TableExpr {
                            inner: TableExprInner::Table(Relation {
                                schema: None,
                                name: cte_alias,
                            }),
                            alias: None,
                            column_aliases: vec![],
                        })
                    }
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
            } => {
                let (col_name, table_name) = get_col_table(env, *col, *table)?;
                let col_expr = make_column_expr(&col_name, &table_name);
                let func_expr = make_aggregate_expr(function, col_expr);
                let placeholder = make_placeholder(dialect, placeholder_idx);
                having_expr = Some(Expr::BinaryOp {
                    lhs: Box::new(Expr::Call(func_expr)),
                    op: *op,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(placeholder))),
                });
                let col_type = get_col_type(env, *col)?;
                params.push(ParamMeta {
                    sql_type: col_type,
                    gen_spec: ColumnGenerationSpec::Random,
                    count: 1,
                });
            }
            Constraint::Subquery {
                position,
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
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);

                match position {
                    SubqueryPosition::ExistsUncorrelated | SubqueryPosition::ExistsCorrelated => {
                        where_clauses.push(Expr::Exists(Box::new(inner_query)));
                    }
                    SubqueryPosition::InSubquery => {
                        let outer_var =
                            *shared_vars
                                .first()
                                .ok_or_else(|| ResolveError::TypeMismatch {
                                    expected: "shared_vars for InSubquery".to_string(),
                                    actual: "empty shared_vars".to_string(),
                                })?;
                        match env.get(outer_var).cloned() {
                            Some(Binding::Column { name, table, .. }) => {
                                where_clauses.push(Expr::In {
                                    lhs: Box::new(make_column_expr(&name, &table)),
                                    rhs: InValue::Subquery(Box::new(inner_query)),
                                    negated: false,
                                });
                            }
                            _ => return Err(ResolveError::Unbound(outer_var)),
                        }
                    }
                    SubqueryPosition::ScalarSubquery => {
                        fields.push(FieldDefinitionExpr::Expr {
                            expr: Expr::NestedSelect(Box::new(inner_query)),
                            alias: None,
                        });
                    }
                    SubqueryPosition::JoinSubquery(_) => {
                        // Join subqueries are handled via JoinRight::Subquery in Constraint::Join
                    }
                }
            }
            Constraint::Cte {
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
                let (inner_query, inner_params, inner_ddl) = resolve_inner_subquery(
                    inner_constraints,
                    env,
                    var_kinds,
                    shared_vars,
                    state,
                    entropy,
                    placeholder_idx,
                )?;
                params.extend(inner_params);
                env.ddl_steps.extend(inner_ddl);
                ctes.push(CommonTableExpr {
                    name: cte_alias,
                    statement: inner_query,
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

                let order_by = if let Some((col, table)) = order_col {
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

                fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::WindowFunction {
                        function: func_expr,
                        partition_by,
                        order_by,
                    },
                    alias: None,
                });
            }
            // Schema and unification constraints don't produce AST nodes
            // (they were resolved in `resolve_schema`).
            Constraint::BaseTable(_)
            | Constraint::AliasOf { .. }
            | Constraint::ColumnExists { .. }
            | Constraint::ColumnTypeClass { .. }
            | Constraint::TypeCompatible(_, _)
            | Constraint::Eq(_, _)
            | Constraint::NotEq(_, _) => {}
            // Variants that the public `PatternBuilder` API can emit but
            // the resolver does not yet implement. Reject loudly so the
            // generator's retry loop discards the pattern instead of
            // shipping SQL that silently omits the construct.
            Constraint::ProjectFunction { .. } => {
                return Err(ResolveError::Unsupported("ProjectFunction"));
            }
            Constraint::WhereOr { .. } => {
                return Err(ResolveError::Unsupported("WhereOr"));
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

/// Get (col_name, table_name) from a Column binding (where table is embedded in the binding).
fn get_col_and_table_for_join(
    env: &mut Env,
    col: VarId,
) -> Result<(SqlIdentifier, SqlIdentifier), ResolveError> {
    match env.get(col) {
        Some(Binding::Column { name, table, .. }) => Ok((name.clone(), table.clone())),
        _ => Err(ResolveError::Unbound(col)),
    }
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
fn resolve_inner_subquery(
    inner_constraints: &[Constraint],
    outer_env: &mut Env,
    outer_var_kinds: &[VarKind],
    shared_vars: &[VarId],
    state: &mut GenerationState,
    entropy: &mut Entropy<'_>,
    placeholder_idx: &mut u32,
) -> Result<(SelectStatement, Vec<ParamMeta>, Vec<DdlStep>), ResolveError> {
    // Collect all VarIds referenced in inner constraints to size num_vars.
    let max_var = inner_constraints
        .iter()
        .flat_map(crate::pattern::constraint_var_ids)
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
    let mut inner_env =
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

    let (query, params) = build_select_inner(
        &mut inner_env,
        inner_constraints,
        var_kinds,
        state,
        entropy,
        placeholder_idx,
    )?;

    // Expose inner-scope *column* bindings to the outer environment.
    //
    // CTE patterns rely on this: an inner-allocated column var `c` is
    // referenced from the outer scope as `ProjectColumn(c, cte_alias)`,
    // which calls `get_col_table(env, c, cte_alias)` and needs `c`'s
    // binding in the outer env. Table bindings stay private so
    // synthesized BaseTables don't leak into outer scope.
    //
    // Caller contract: the outer pattern must only reference inner
    // column vars in contexts where the column genuinely lives — either
    // via the CTE/subquery alias (CTE pattern) or via the original
    // physical table when correlated. The resolver does not (and cannot
    // cheaply) verify that the `(col, table)` pair the outer emits is
    // self-consistent. Pattern authors own that invariant.
    for v in 0..num_vars {
        let id = VarId(v);
        if let Some(binding) = inner_env.get(id)
            && matches!(binding, Binding::Column { .. })
            && !outer_env.is_bound(id)
        {
            let b = binding.clone();
            outer_env.bind(id, b);
        }
    }

    let ddl = inner_env.into_ddl_steps(state)?;
    Ok((query, params, ddl))
}

/// Infer VarKinds from constraint usage.
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
        AggregateFn::GroupConcat | AggregateFn::JsonObjectAgg | AggregateFn::ArrayAgg => {
            // Use a generic UDF call for these
            let name = match function {
                AggregateFn::GroupConcat => "GROUP_CONCAT",
                AggregateFn::JsonObjectAgg => "JSON_OBJECT_AGG",
                AggregateFn::ArrayAgg => "ARRAY_AGG",
                _ => unreachable!(),
            };
            FunctionExpr::Udf {
                schema: None,
                name: SqlIdentifier::from(name),
                arguments: vec![col_expr],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;
    use crate::constraint::TypeClass;
    use crate::resolver::{ResolverOutput, resolve};
    use crate::state::GeneratorConfig;
    use crate::var::VarKind;

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
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
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
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
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
            },
            Constraint::WhereParam {
                col: c3,
                table: t,
                op: BinaryOperator::Greater,
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
            VarKind::Column { table: t },
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
            },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
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
            Constraint::WhereBetweenParam { col: c2, table: t },
        ];
        let var_kinds = vec![
            VarKind::Relation,
            VarKind::Column { table: t },
            VarKind::Column { table: t },
        ];

        let (sql, output) = resolve_to_sql(constraints, var_kinds, Dialect::PostgreSQL);

        assert!(sql.contains("BETWEEN"), "sql: {sql}");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$2"), "sql: {sql}");
        assert_eq!(output.params.len(), 1);
        assert_eq!(output.params[0].count, 2);
    }

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
        use crate::constraint::SubqueryPosition;
        use crate::pattern::PatternBuilder;

        let mut b = PatternBuilder::new("exists_sq");
        let t_outer = b.table();
        let c_outer = b.column(t_outer);

        let mut sq = b.subquery();
        let t_inner = sq.table();
        let c_inner = sq.column(t_inner);
        sq.from(t_inner);
        sq.project_column(c_inner, t_inner);
        sq.commit_as_where(SubqueryPosition::ExistsUncorrelated);

        b.from(t_outer);
        b.project_column(c_outer, t_outer);

        let pattern = b.build();

        let (sql, _) = resolve_to_sql(pattern.constraints, pattern.vars, Dialect::MySQL);

        assert!(sql.contains("EXISTS"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn ast_where_in_subquery() {
        use crate::constraint::SubqueryPosition;
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
        sq.commit_as_where(SubqueryPosition::InSubquery);

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
}
