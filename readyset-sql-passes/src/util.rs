use itertools::Either;
use readyset_errors::{ReadySetResult, unsupported_err};
use readyset_sql::DialectDisplay;
use readyset_sql::analysis::is_aggregate;
use readyset_sql::ast::{
    ArrayArguments, BinaryOperator, Column, CommonTableExpr, Expr, FieldDefinitionExpr,
    FunctionExpr, InValue, JoinClause, JoinRightSide, Relation, SelectStatement, SqlIdentifier,
    TableExpr, TableExprInner,
};
use std::collections::{HashMap, HashSet};
use std::iter;
use std::sync::OnceLock;

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
    ctes.iter_mut()
        .map(|cte| (&cte.name, &mut cte.statement))
        .chain(
            tables
                .iter_mut()
                .chain(join.iter_mut().flat_map(|join| match &mut join.right {
                    JoinRightSide::Table(t) => Either::Left(iter::once(t)),
                    JoinRightSide::Tables(ts) => Either::Right(ts.iter_mut()),
                }))
                .filter_map(|te| match &mut te.inner {
                    TableExprInner::Subquery(sq) => {
                        te.alias.as_ref().map(|alias| (alias, sq.as_mut()))
                    }
                    TableExprInner::Table(_) => None,
                }),
        )
        .map(|(name, stmt)| {
            Ok((
                name,
                field_names(stmt, dialect)?
                    .into_iter()
                    .map(|x| &*x)
                    .collect::<Vec<&SqlIdentifier>>(),
            ))
        })
        .collect()
}

// Built-in function names that are allowed in expressions containing aggregates.
// All allowed functions must be deterministic, formatting-only, and must not affect result cardinality.
static BUILTIN_FUNCTIONS_ALLOWED_IN_AGG_CONTEXT: OnceLock<HashSet<&'static str>> = OnceLock::new();

fn is_builtin_allowed_in_aggregate_context(name: &str) -> bool {
    BUILTIN_FUNCTIONS_ALLOWED_IN_AGG_CONTEXT
        .get_or_init(builtin_functions_allowed_in_agg_context)
        .contains(name.to_ascii_lowercase().as_str())
}

fn builtin_functions_allowed_in_agg_context() -> HashSet<&'static str> {
    use dataflow_expression::BuiltinFunctionDiscriminants;
    use strum::IntoEnumIterator;
    // Always add explicit arms to this match, do not use default arm here `_ =>`.
    // We have to make sure, any newly added built-ins will be explicitly added here.
    // To exclude a function from the list, the corresponding arm should return empty `!vec[]`.
    BuiltinFunctionDiscriminants::iter()
        .flat_map(|bf| match bf {
            BuiltinFunctionDiscriminants::ConvertTZ => vec!["convert_tz"],
            BuiltinFunctionDiscriminants::DayOfWeek => vec!["dayofweek"],
            BuiltinFunctionDiscriminants::IfNull => vec!["ifnull"],
            BuiltinFunctionDiscriminants::Month => vec!["month"],
            BuiltinFunctionDiscriminants::Timediff => vec!["timediff"],
            BuiltinFunctionDiscriminants::Addtime => vec!["addtime"],
            BuiltinFunctionDiscriminants::DateFormat => vec!["date_format"],
            BuiltinFunctionDiscriminants::Round => vec!["round"],
            BuiltinFunctionDiscriminants::JsonDepth => vec!["json_depth"],
            BuiltinFunctionDiscriminants::JsonValid => vec!["json_valid"],
            BuiltinFunctionDiscriminants::JsonQuote => vec!["json_quote"],
            BuiltinFunctionDiscriminants::JsonOverlaps => vec!["json_overlaps"],
            BuiltinFunctionDiscriminants::JsonTypeof => vec!["json_typeof", "jsonb_typeof"],
            BuiltinFunctionDiscriminants::JsonObject => vec!["json_object"],
            BuiltinFunctionDiscriminants::JsonBuildObject => {
                vec!["json_build_object", "jsonb_build_object"]
            }
            BuiltinFunctionDiscriminants::JsonArrayLength => {
                vec!["json_array_length", "jsonb_array_length"]
            }
            BuiltinFunctionDiscriminants::JsonStripNulls => {
                vec!["json_strip_nulls", "jsonb_strip_nulls"]
            }
            BuiltinFunctionDiscriminants::JsonExtractPath => vec![
                "json_extract_path",
                "jsonb_extract_path",
                "json_extract_path_text",
                "jsonb_extract_path_text",
            ],
            BuiltinFunctionDiscriminants::JsonbInsert => vec![],
            BuiltinFunctionDiscriminants::JsonbSet => vec![],
            BuiltinFunctionDiscriminants::JsonbPretty => vec!["jsonb_pretty"],
            BuiltinFunctionDiscriminants::Coalesce => vec!["coalesce"],
            BuiltinFunctionDiscriminants::Concat => vec!["concat"],
            BuiltinFunctionDiscriminants::ConcatWs => vec!["concat_ws"],
            BuiltinFunctionDiscriminants::Substring => vec!["substring", "substr"],
            BuiltinFunctionDiscriminants::SplitPart => vec!["split_part"],
            BuiltinFunctionDiscriminants::Greatest => vec!["greatest"],
            BuiltinFunctionDiscriminants::Least => vec!["least"],
            BuiltinFunctionDiscriminants::ArrayToString => vec!["array_to_string"],
            BuiltinFunctionDiscriminants::DateTrunc => vec!["date_trunc"],
            BuiltinFunctionDiscriminants::Extract => vec!["extract"],
            BuiltinFunctionDiscriminants::Length => {
                vec!["length", "octet_length", "char_length", "character_length"]
            }
            BuiltinFunctionDiscriminants::Ascii => vec!["ascii"],
            BuiltinFunctionDiscriminants::Lower => vec!["lower"],
            BuiltinFunctionDiscriminants::Upper => vec!["upper"],
            BuiltinFunctionDiscriminants::Hex => vec!["hex"],
            BuiltinFunctionDiscriminants::SpatialAsText => {
                vec!["st_astext", "st_aswkt"]
            }
            BuiltinFunctionDiscriminants::SpatialAsEWKT => vec!["st_asewkt"],
            BuiltinFunctionDiscriminants::Bucket => vec!["bucket"],
        })
        .filter(|t| !t.is_empty())
        .collect::<HashSet<_>>()
}

#[must_use]
pub fn map_aggregates(
    expr: &mut Expr,
    dialect: readyset_sql::Dialect,
) -> Vec<(FunctionExpr, SqlIdentifier)> {
    let mut ret = Vec::new();
    match expr {
        Expr::Call(f) if is_aggregate(f) => {
            let name: SqlIdentifier = f.display(dialect).to_string().into();
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
        Expr::Call(FunctionExpr::Call {
            name,
            arguments: Some(exprs),
        }) if is_builtin_allowed_in_aggregate_context(name.as_str()) => {
            ret.extend(exprs.iter_mut().flat_map(|e| map_aggregates(e, dialect)));
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
            | AtArrowRight
            | AtArrowLeft
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
