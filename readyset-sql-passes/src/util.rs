use std::collections::{HashMap, HashSet};
use std::iter;

use itertools::Either;
use nom_sql::analysis::is_aggregate;
use nom_sql::{
    BinaryOperator, Column, CommonTableExpr, Expr, FieldDefinitionExpr, FunctionExpr, InValue,
    JoinClause, JoinRightSide, Relation, SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};

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

fn field_names(statement: &SelectStatement) -> impl Iterator<Item = &SqlIdentifier> {
    statement.fields.iter().filter_map(|field| match &field {
        FieldDefinitionExpr::Expr {
            alias: Some(alias), ..
        } => Some(alias),
        FieldDefinitionExpr::Expr {
            expr: Expr::Column(Column { name, .. }),
            ..
        } => Some(name),
        _ => None,
    })
}

/// Returns a map from subquery aliases to vectors of the fields in those subqueries.
///
/// Takes only the CTEs and join clause so that it doesn't have to borrow the entire statement.
pub(crate) fn subquery_schemas<'a>(
    tables: &'a [TableExpr],
    ctes: &'a [CommonTableExpr],
    join: &'a [JoinClause],
) -> HashMap<&'a SqlIdentifier, Vec<&'a SqlIdentifier>> {
    ctes.iter()
        .map(|cte| (&cte.name, &cte.statement))
        .chain(
            tables
                .iter()
                .chain(join.iter().flat_map(|join| match &join.right {
                    JoinRightSide::Table(t) => Either::Left(iter::once(t)),
                    JoinRightSide::Tables(ts) => Either::Right(ts.iter()),
                }))
                .filter_map(|te| match &te.inner {
                    TableExprInner::Subquery(sq) => {
                        te.alias.as_ref().map(|alias| (alias, sq.as_ref()))
                    }
                    TableExprInner::Table(_) => None,
                }),
        )
        .map(|(name, stmt)| (name, field_names(stmt).collect()))
        .collect()
}

#[must_use]
pub fn map_aggregates(expr: &mut Expr) -> Vec<(FunctionExpr, SqlIdentifier)> {
    let mut ret = Vec::new();
    match expr {
        Expr::Call(f) if is_aggregate(f) => {
            let name: SqlIdentifier = f.to_string().into();
            ret.push((f.clone(), name.clone()));
            *expr = Expr::Column(Column { name, table: None });
        }
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            ret.extend(branches.iter_mut().flat_map(|b| {
                map_aggregates(&mut b.condition)
                    .into_iter()
                    .chain(map_aggregates(&mut b.body))
            }));
            if let Some(else_expr) = else_expr {
                ret.append(&mut map_aggregates(else_expr));
            }
        }
        Expr::Call(_) | Expr::Literal(_) | Expr::Column(_) | Expr::Variable(_) => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs));
            ret.append(&mut map_aggregates(rhs));
        }
        Expr::UnaryOp { rhs: expr, .. } | Expr::Cast { expr, .. } => {
            ret.append(&mut map_aggregates(expr));
        }
        Expr::Exists(_) => {}
        Expr::NestedSelect(_) => {}
        Expr::Between {
            operand, min, max, ..
        } => {
            ret.append(&mut map_aggregates(operand));
            ret.append(&mut map_aggregates(min));
            ret.append(&mut map_aggregates(max));
        }
        Expr::In { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs));
            match rhs {
                InValue::Subquery(_) => {}
                InValue::List(exprs) => {
                    for expr in exprs {
                        ret.append(&mut map_aggregates(expr));
                    }
                }
            }
        }
        Expr::Array(exprs) => ret.extend(exprs.iter_mut().flat_map(map_aggregates)),
    }
    ret
}

/// Returns true if the given binary operator is a (boolean-valued) predicate
///
/// TODO(grfn): Replace this with actual typechecking at some point
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
    )
}

/// Returns true if the given binary operator is a (boolean-valued) logical operator
///
/// TODO(grfn): Replace this with actual typechecking at some point
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
    use nom_sql::{parse_query, Dialect, SqlQuery};

    let q = parse_query(Dialect::MySQL, q).unwrap();
    match q {
        SqlQuery::Select(stmt) => stmt,
        _ => panic!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod is_correlated {
        use super::*;

        #[test]
        fn uncorrelated_query() {
            let query = parse_select_statement(
                "SELECT * FROM t JOIN u ON t.w = u.a WHERE t.x = t.y AND t.z = 4",
            );
            assert!(!is_correlated(&query));
        }

        #[test]
        fn correlated_query() {
            let query =
                parse_select_statement("SELECT * FROM t WHERE t.x = t.y AND t.z = 4 AND t.w = u.a");
            assert!(is_correlated(&query));
        }

        #[test]
        fn correlated_different_schemas() {
            let query = parse_select_statement("SELECT * FROM a.t WHERE a.t = b.t");
            assert!(is_correlated(&query));
        }
    }
}
