use itertools::Either;
use readyset_errors::{ReadySetResult, invalid_query, unsupported_err};
use readyset_sql::DialectDisplay;
use readyset_sql::analysis::is_aggregate;
use readyset_sql::ast::{
    ArrayArguments, BinaryOperator, Column, CommonTableExpr, Expr, FieldDefinitionExpr,
    FunctionExpr, InValue, JoinClause, JoinRightSide, Relation, SelectStatement, SqlIdentifier,
    TableExpr, TableExprInner,
};
use std::collections::{HashMap, HashSet};
use std::iter;

pub(crate) fn join_clause_tables(join: &JoinClause) -> impl Iterator<Item = &TableExpr> {
    match &join.right {
        JoinRightSide::Table(table) => Either::Left(iter::once(table)),
        JoinRightSide::Tables(tables) => Either::Right(tables.iter()),
    }
}

/// Returns an iterator over all the tables referred to by the *outermost* query in the given
/// statement (eg not including any subqueries)
pub fn outermost_table_exprs(stmt: &SelectStatement) -> impl Iterator<Item = &TableExpr> {
    stmt.tables
        .iter()
        .chain(stmt.join.iter().flat_map(join_clause_tables))
}

pub(crate) fn outermost_named_tables(
    stmt: &SelectStatement,
) -> impl Iterator<Item = Relation> + '_ {
    outermost_table_exprs(stmt).filter_map(|tbl| {
        tbl.alias
            .clone()
            .map(Relation::from)
            .or_else(|| tbl.inner.as_table().cloned())
    })
}

/// Returns true if the given select statement is *correlated* if used as a subquery, eg if it
/// refers to tables not explicitly mentioned in the query
pub fn is_correlated(statement: &SelectStatement) -> bool {
    let tables: HashSet<_> = outermost_named_tables(statement).collect();
    statement
        .outermost_referred_columns()
        .any(|col| col.table.iter().any(|tbl| !tables.contains(tbl)))
}

fn field_names(
    statement: &mut SelectStatement,
    dialect: readyset_sql::Dialect,
) -> ReadySetResult<Vec<&mut SqlIdentifier>> {
    statement
        .fields
        .iter_mut()
        .map(|field| match field {
            FieldDefinitionExpr::Expr {
                alias: Some(alias), ..
            } => Ok(alias),
            FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column { name, .. }),
                ..
            } => Ok(name),
            FieldDefinitionExpr::Expr { alias, expr } => {
                if let Some(a) = alias {
                    Ok(a)
                } else {
                    *alias = expr.alias(dialect);
                    alias.as_mut().ok_or({
                        unsupported_err!("Expression {} not supported", expr.display(dialect))
                    })
                }
            }
            // TODO: Generate an alias when an Expr (that is not simply an Expr::Column)
            // doesn't have one
            e => Err(unsupported_err!(
                "Expression {} not supported",
                e.display(dialect)
            )),
        })
        .collect()
}

/// Returns a map from subquery aliases to vectors of the fields in those subqueries.
///
/// Takes only the CTEs and join clause so that it doesn't have to borrow the entire statement.
pub(crate) fn subquery_schemas<'a>(
    tables: &'a mut [TableExpr],
    ctes: &'a mut [CommonTableExpr],
    join: &'a mut [JoinClause],
    dialect: readyset_sql::Dialect,
) -> ReadySetResult<HashMap<&'a SqlIdentifier, Vec<&'a SqlIdentifier>>> {
    // First pass: populate auto-generated column aliases for VALUES clauses that don't have
    // explicit column names (e.g., `(VALUES (1, 'a')) AS v` gets columns `column1`, `column2`).
    for table in tables.iter_mut() {
        populate_values_column_aliases(table, dialect)?;
    }
    for jc in join.iter_mut() {
        match &mut jc.right {
            JoinRightSide::Table(te) => populate_values_column_aliases(te, dialect)?,
            JoinRightSide::Tables(tes) => {
                for te in tes {
                    populate_values_column_aliases(te, dialect)?;
                }
            }
        }
    }

    // Second pass: collect schemas from CTEs, subqueries, and VALUES clauses.
    let mut schemas = HashMap::new();

    for cte in ctes.iter_mut() {
        schemas.insert(
            &cte.name,
            field_names(&mut cte.statement, dialect)?
                .into_iter()
                .map(|x| &*x)
                .collect(),
        );
    }

    for te in tables
        .iter_mut()
        .chain(join.iter_mut().flat_map(|j| match &mut j.right {
            JoinRightSide::Table(t) => Either::Left(iter::once(t)),
            JoinRightSide::Tables(ts) => Either::Right(ts.iter_mut()),
        }))
    {
        match &mut te.inner {
            TableExprInner::Subquery(sq) => {
                if let Some(alias) = &te.alias {
                    schemas.insert(
                        alias,
                        field_names(sq.as_mut(), dialect)?
                            .into_iter()
                            .map(|x| &*x)
                            .collect(),
                    );
                }
            }
            TableExprInner::Values { .. } => {
                if let Some(alias) = &te.alias {
                    schemas.insert(alias, te.column_aliases.iter().collect());
                }
            }
            TableExprInner::Table(_) => {}
        }
    }

    Ok(schemas)
}

/// Populate auto-generated column aliases for VALUES table expressions that don't have explicit
/// column names. PostgreSQL uses 1-indexed names (column1, column2, ...) while MySQL uses
/// 0-indexed names (column_0, column_1, ...).
fn populate_values_column_aliases(
    te: &mut TableExpr,
    dialect: readyset_sql::Dialect,
) -> ReadySetResult<()> {
    if let TableExprInner::Values { rows } = &te.inner {
        let num_cols = rows.first().map(|r| r.len()).unwrap_or(0);
        let num_aliases = te.column_aliases.len();

        if num_aliases > num_cols {
            invalid_query!(
                "VALUES clause has {} columns but {} aliases were specified",
                num_cols,
                num_aliases
            );
        } else if num_aliases > 0 && num_aliases < num_cols {
            if dialect == readyset_sql::Dialect::MySQL {
                invalid_query!(
                    "VALUES clause has {} columns but {} aliases were specified",
                    num_cols,
                    num_aliases
                );
            }
            // PostgreSQL allows partial aliases; pad the rest with defaults.
            for i in num_aliases..num_cols {
                te.column_aliases
                    .push(SqlIdentifier::from(format!("column{}", i + 1)));
            }
        } else if num_aliases == 0 {
            // No aliases provided; generate all default names.
            for i in 0..num_cols {
                let name = match dialect {
                    readyset_sql::Dialect::MySQL => format!("column_{}", i),
                    readyset_sql::Dialect::PostgreSQL => format!("column{}", i + 1),
                };
                te.column_aliases.push(SqlIdentifier::from(name));
            }
        }
    }
    Ok(())
}

#[must_use]
pub fn map_aggregates(
    expr: &mut Expr,
    dialect: readyset_sql::Dialect,
) -> Vec<(FunctionExpr, SqlIdentifier)> {
    let mut ret = Vec::new();
    match expr {
        Expr::Call(f) if is_aggregate(f) => {
            let name: SqlIdentifier = Expr::Call(f.clone())
                .alias(dialect)
                .unwrap_or_else(|| f.display(dialect).to_string().into());
            ret.push((f.clone(), name.clone()));
            *expr = Expr::Column(Column { name, table: None });
        }
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            ret.extend(branches.iter_mut().flat_map(|b| {
                map_aggregates(&mut b.condition, dialect)
                    .into_iter()
                    .chain(map_aggregates(&mut b.body, dialect))
            }));
            if let Some(else_expr) = else_expr {
                ret.append(&mut map_aggregates(else_expr, dialect));
            }
        }
        Expr::Call(
            FunctionExpr::Lower { expr, .. }
            | FunctionExpr::Upper { expr, .. }
            | FunctionExpr::Extract { expr, .. }
            | FunctionExpr::Substring { string: expr, .. }
            | FunctionExpr::Bucket { expr, .. },
        ) => {
            ret.append(&mut map_aggregates(expr, dialect));
        }
        Expr::Call(FunctionExpr::Coalesce(exprs)) => {
            ret.extend(exprs.iter_mut().flat_map(|e| map_aggregates(e, dialect)));
        }
        Expr::Call(FunctionExpr::IfNull(a, b)) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
        }
        // Typed variants that are allowed in aggregate context: recurse into arguments
        Expr::Call(FunctionExpr::ConvertTz(a, b, c) | FunctionExpr::SplitPart(a, b, c)) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
            ret.append(&mut map_aggregates(c, dialect));
        }
        Expr::Call(
            FunctionExpr::DayOfWeek(expr)
            | FunctionExpr::Month(expr)
            | FunctionExpr::Length(expr)
            | FunctionExpr::OctetLength(expr)
            | FunctionExpr::CharLength(expr)
            | FunctionExpr::Ascii(expr)
            | FunctionExpr::Hex(expr)
            | FunctionExpr::JsonDepth(expr)
            | FunctionExpr::JsonValid(expr)
            | FunctionExpr::JsonQuote(expr)
            | FunctionExpr::JsonTypeof(expr)
            | FunctionExpr::JsonArrayLength(expr)
            | FunctionExpr::JsonStripNulls(expr)
            | FunctionExpr::JsonbStripNulls(expr)
            | FunctionExpr::JsonbPretty(expr)
            | FunctionExpr::StAsText(expr)
            | FunctionExpr::StAsWkt(expr)
            | FunctionExpr::StAsEwkt(expr),
        ) => {
            ret.append(&mut map_aggregates(expr, dialect));
        }
        Expr::Call(
            FunctionExpr::Timediff(a, b)
            | FunctionExpr::Addtime(a, b)
            | FunctionExpr::DateFormat(a, b)
            | FunctionExpr::DateTrunc(a, b)
            | FunctionExpr::JsonOverlaps(a, b),
        ) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
        }
        Expr::Call(FunctionExpr::Round(expr, prec)) => {
            ret.append(&mut map_aggregates(expr, dialect));
            if let Some(p) = prec {
                ret.append(&mut map_aggregates(p, dialect));
            }
        }
        Expr::Call(
            FunctionExpr::Greatest(exprs)
            | FunctionExpr::Least(exprs)
            | FunctionExpr::Concat(exprs)
            | FunctionExpr::ConcatWs(exprs)
            | FunctionExpr::JsonObject(exprs)
            | FunctionExpr::JsonbObject(exprs)
            | FunctionExpr::JsonBuildObject(exprs)
            | FunctionExpr::JsonbBuildObject(exprs),
        ) => {
            ret.extend(exprs.iter_mut().flat_map(|e| map_aggregates(e, dialect)));
        }
        Expr::Call(
            FunctionExpr::JsonExtractPathText(json, keys)
            | FunctionExpr::JsonExtractPath(json, keys)
            | FunctionExpr::JsonbExtractPath(json, keys),
        ) => {
            ret.append(&mut map_aggregates(json, dialect));
            ret.extend(keys.iter_mut().flat_map(|e| map_aggregates(e, dialect)));
        }
        Expr::Call(FunctionExpr::ArrayToString(a, b, c)) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
            if let Some(c) = c {
                ret.append(&mut map_aggregates(c, dialect));
            }
        }
        // Previously these functions were excluded from aggregate context; they now
        // recurse like all other strict multi-arg functions, allowing patterns such as
        // `SELECT jsonb_insert(col, '{k}', count(*)::text) FROM t`.
        Expr::Call(FunctionExpr::JsonbInsert(a, b, c, d) | FunctionExpr::JsonbSet(a, b, c, d)) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
            ret.append(&mut map_aggregates(c, dialect));
            if let Some(d) = d {
                ret.append(&mut map_aggregates(d, dialect));
            }
        }
        Expr::Call(FunctionExpr::JsonbSetLax(a, b, c, d, e)) => {
            ret.append(&mut map_aggregates(a, dialect));
            ret.append(&mut map_aggregates(b, dialect));
            ret.append(&mut map_aggregates(c, dialect));
            if let Some(d) = d {
                ret.append(&mut map_aggregates(d, dialect));
            }
            if let Some(e) = e {
                ret.append(&mut map_aggregates(e, dialect));
            }
        }
        Expr::Call(_) | Expr::Literal(_) | Expr::Column(_) | Expr::Variable(_) => {}
        Expr::BinaryOp { lhs, rhs, .. }
        | Expr::OpAny { lhs, rhs, .. }
        | Expr::OpSome { lhs, rhs, .. }
        | Expr::OpAll { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs, dialect));
            ret.append(&mut map_aggregates(rhs, dialect));
        }
        Expr::UnaryOp { rhs: expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::ConvertUsing { expr, .. } => {
            ret.append(&mut map_aggregates(expr, dialect));
        }
        Expr::Exists(_) => {}
        Expr::NestedSelect(_) => {}
        Expr::Between {
            operand, min, max, ..
        } => {
            ret.append(&mut map_aggregates(operand, dialect));
            ret.append(&mut map_aggregates(min, dialect));
            ret.append(&mut map_aggregates(max, dialect));
        }
        Expr::In { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs, dialect));
            match rhs {
                InValue::Subquery(_) => {}
                InValue::List(exprs) => {
                    for expr in exprs {
                        ret.append(&mut map_aggregates(expr, dialect));
                    }
                }
            }
        }
        Expr::Row { exprs, .. } => {
            ret.extend(exprs.iter_mut().flat_map(|e| map_aggregates(e, dialect)))
        }
        Expr::Array(args) => match args {
            ArrayArguments::List(exprs) => {
                ret.extend(exprs.iter_mut().flat_map(|e| map_aggregates(e, dialect)))
            }
            ArrayArguments::Subquery(..) => {}
        },
        Expr::Collate { expr, .. } => ret.append(&mut map_aggregates(expr, dialect)),
        // Window functions are handled separately
        // `PARTITION BY` and `ORDER BY` can *NOT* contain aggregates
        Expr::WindowFunction { .. } => {}
    }
    ret
}

/// Returns true if the given binary operator is a (boolean-valued) predicate
///
/// TODO(aspen): Replace this with actual typechecking at some point
pub fn is_predicate(op: &BinaryOperator) -> bool {
    use BinaryOperator::*;

    matches!(
        op,
        Like | NotLike
            | ILike
            | NotILike
            | Equal
            | NotEqual
            | Greater
            | GreaterOrEqual
            | Less
            | LessOrEqual
            | Is
            | IsNot
            | QuestionMark
            | QuestionMarkPipe
            | QuestionMarkAnd
            | AtArrowRight
            | AtArrowLeft
            | DoubleAmpersand
    )
}

/// Returns true if the given binary operator is a (boolean-valued) logical operator
///
/// TODO(aspen): Replace this with actual typechecking at some point
pub fn is_logical_op(op: &BinaryOperator) -> bool {
    use BinaryOperator::*;

    matches!(op, And | Or)
}

/// Boolean-valued logical operators
pub enum LogicalOp {
    And,
    Or,
}

impl TryFrom<BinaryOperator> for LogicalOp {
    type Error = BinaryOperator;

    fn try_from(value: BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            BinaryOperator::And => Ok(Self::And),
            BinaryOperator::Or => Ok(Self::Or),
            _ => Err(value),
        }
    }
}

/// Test helper: parse the given string as a SQL query, panicking if it's anything other than a
/// [`SelectStatement`]
#[cfg(test)]
pub(crate) fn parse_select_statement(q: &str) -> SelectStatement {
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_select;

    parse_select(Dialect::MySQL, q).unwrap()
}

#[cfg(test)]
mod tests {
    use super::is_correlated;
    use readyset_sql_parsing::parse_select;

    mod is_correlated {
        use readyset_sql::Dialect;

        use super::*;

        #[test]
        fn uncorrelated_query() {
            let query = parse_select(
                Dialect::MySQL,
                "SELECT * FROM t JOIN u ON t.w = u.a WHERE t.x = t.y AND t.z = 4",
            )
            .unwrap();
            assert!(!is_correlated(&query));
        }

        #[test]
        fn correlated_query() {
            let query = parse_select(
                Dialect::MySQL,
                "SELECT * FROM t WHERE t.x = t.y AND t.z = 4 AND t.w = u.a",
            )
            .unwrap();
            assert!(is_correlated(&query));
        }

        #[test]
        fn correlated_different_schemas() {
            let query = parse_select(Dialect::MySQL, "SELECT * FROM a.t WHERE a.t = b.t").unwrap();
            assert!(is_correlated(&query));
        }
    }
}
