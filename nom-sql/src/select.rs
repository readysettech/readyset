use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::error::ErrorKind;
use nom::multi::{many0, separated_list1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use proptest::option;
use proptest::prelude::Arbitrary;
use proptest::strategy::BoxedStrategy;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{
    field_definition_expr, field_list, field_reference_list, terminated_with_statement_terminator,
    ws_sep_comma, FieldDefinitionExpr,
};
use crate::expression::expression;
use crate::join::{join_operator, JoinConstraint, JoinOperator, JoinRightSide};
use crate::literal::literal;
use crate::order::{order_clause, OrderClause};
use crate::table::{table_expr, table_expr_list};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{
    Dialect, Expr, FieldReference, FunctionExpr, Literal, NomSqlError, NomSqlResult, SqlIdentifier,
    TableExpr,
};

#[derive(
    Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Default, Serialize, Deserialize, Arbitrary,
)]
pub struct GroupByClause {
    pub fields: Vec<FieldReference>,
}

impl GroupByClause {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "GROUP BY {}",
                self.fields
                    .iter()
                    .map(|field| field.display(dialect))
                    .join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct JoinClause {
    pub operator: JoinOperator,
    pub right: JoinRightSide,
    pub constraint: JoinConstraint,
}

impl JoinClause {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} {} {}",
                self.operator,
                self.right.display(dialect),
                self.constraint.display(dialect)
            )
        })
    }
}

#[derive(
    Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CommonTableExpr {
    pub name: SqlIdentifier,
    pub statement: SelectStatement,
}

impl CommonTableExpr {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} AS ({})",
                dialect.quote_identifier(&self.name),
                self.statement.display(dialect)
            )
        })
    }
}

/// AST representation of the SQL limit and offset clauses.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LimitClause {
    /// The standard limit and offset syntax: `LIMIT <limit> OFFSET <offset>`.
    LimitOffset {
        limit: Option<Literal>,
        offset: Option<Literal>,
    },
    /// MySQL's alternative limit and offset syntax: `LIMIT <offset>, <limit>`.
    OffsetCommaLimit { offset: Literal, limit: Literal },
}

/// Options for generating arbitrary [`LimitClause`]s
#[derive(Default, Debug, Clone, Copy)]
pub struct LimitClauseArbitraryOptions {
    /// [`LimitClause`] has differences between mysql and postgres, so this dialect controls which
    /// one we use to generate arbitrary [`LimitClause`]s
    pub dialect: Option<Dialect>,
}

impl Arbitrary for LimitClause {
    type Parameters = LimitClauseArbitraryOptions;

    type Strategy = BoxedStrategy<LimitClause>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        use LimitClause::*;

        // limit to positive numbers
        // Don't allow omitted limit (see FIXME above `limit_offset_generic`)
        let limit_offset_strategy = (
            (1i64..=i64::MAX).prop_map(Literal::Integer),
            option::of((1i64..=i64::MAX).prop_map(Literal::Integer)),
        )
            .prop_map(|(limit, offset)| LimitOffset {
                limit: Some(limit),
                offset,
            });

        if args.dialect == Some(Dialect::MySQL) {
            let offset_comma_limit_strategy =
                ((1..i64::MAX), (1..i64::MAX)).prop_map(|(offset, limit)| OffsetCommaLimit {
                    offset: Literal::Integer(offset),
                    limit: Literal::Integer(limit),
                });
            prop_oneof![limit_offset_strategy, offset_comma_limit_strategy].boxed()
        } else {
            limit_offset_strategy.boxed()
        }
    }
}

impl LimitClause {
    /// Returns an [`Option`] with the [`Literal`] value corresponding to the `limit` clause.
    /// Returns [`None`] if there's no `limit`.
    pub fn limit(&self) -> Option<&Literal> {
        match self {
            LimitClause::LimitOffset { limit, .. } => limit.as_ref(),
            LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
        }
    }

    /// Returns an [`Option`] with the [`Literal`] value corresponding to the `offset` clause.
    /// Returns [`None`] if there's no `offset`.
    pub fn offset(&self) -> Option<&Literal> {
        match self {
            LimitClause::LimitOffset { offset, .. } => offset.as_ref(),
            LimitClause::OffsetCommaLimit { offset, .. } => Some(offset),
        }
    }

    /// Returns two [`Option`]s, both with mutable [`Literal`] values corresponding to the `limit`
    /// and `offset` clauses respectively.
    pub fn limit_and_offset_mut(&mut self) -> (Option<&mut Literal>, Option<&mut Literal>) {
        match self {
            LimitClause::LimitOffset { limit, offset } => (limit.as_mut(), offset.as_mut()),
            LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        }
    }

    /// Whether this [`LimitClause`] is empty (has no `limit` and no `offset`) or not.
    pub fn is_empty(&self) -> bool {
        match self {
            LimitClause::LimitOffset { limit, offset } => limit.is_none() && offset.is_none(),
            LimitClause::OffsetCommaLimit { .. } => false,
        }
    }
}

impl Default for LimitClause {
    fn default() -> Self {
        Self::LimitOffset {
            limit: None,
            offset: None,
        }
    }
}

impl LimitClause {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            match self {
                LimitClause::LimitOffset { limit, offset } => {
                    if let Some(limit) = limit {
                        write!(f, "LIMIT {}", limit.display(dialect))?;
                    }
                    if let Some(offset) = offset {
                        if limit.is_some() {
                            write!(f, " ")?;
                        }
                        write!(f, "OFFSET {}", offset.display(dialect))?;
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    write!(
                        f,
                        "LIMIT {}, {}",
                        offset.display(dialect),
                        limit.display(dialect)
                    )?;
                }
            }

            Ok(())
        })
    }
}

#[derive(
    Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SelectStatement {
    pub ctes: Vec<CommonTableExpr>,
    pub distinct: bool,
    pub fields: Vec<FieldDefinitionExpr>,
    pub tables: Vec<TableExpr>,
    pub join: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<Expr>,
    pub order: Option<OrderClause>,
    pub limit_clause: LimitClause,
}

impl SelectStatement {
    pub fn contains_aggregate_select(&self) -> bool {
        self.fields.iter().any(|e| match e {
            FieldDefinitionExpr::Expr { expr, .. } => match expr {
                Expr::Call(func) => matches!(
                    func,
                    FunctionExpr::Avg { .. }
                        | FunctionExpr::Count { .. }
                        | FunctionExpr::CountStar
                        | FunctionExpr::Sum { .. }
                        | FunctionExpr::Max(_)
                        | FunctionExpr::Min(_)
                        | FunctionExpr::GroupConcat { .. }
                ),
                Expr::NestedSelect(select) => select.contains_aggregate_select(),
                _ => false,
            },
            _ => false,
        })
    }
}

impl SelectStatement {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            if !self.ctes.is_empty() {
                write!(
                    f,
                    "WITH {} ",
                    self.ctes.iter().map(|cte| cte.display(dialect)).join(", ")
                )?;
            }

            write!(f, "SELECT ")?;
            if self.distinct {
                write!(f, "DISTINCT ")?;
            }

            write!(
                f,
                "{}",
                self.fields
                    .iter()
                    .map(|field| field.display(dialect))
                    .join(", ")
            )?;

            if !self.tables.is_empty() {
                write!(f, " FROM ")?;
                write!(
                    f,
                    "{}",
                    self.tables
                        .iter()
                        .map(|table| table.display(dialect))
                        .join(", ")
                )?;
            }

            for jc in &self.join {
                write!(f, " {}", jc.display(dialect))?;
            }

            if let Some(where_clause) = &self.where_clause {
                write!(f, " WHERE {}", where_clause.display(dialect))?;
            }
            if let Some(group_by) = &self.group_by {
                write!(f, " {}", group_by.display(dialect))?;
            }
            if let Some(having) = &self.having {
                write!(f, " HAVING {}", having.display(dialect))?;
            }
            if let Some(order) = &self.order {
                write!(f, " {}", order.display(dialect))?;
            }
            if self.limit_clause.limit().is_some() || self.limit_clause.offset().is_some() {
                write!(f, " {}", self.limit_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}

fn having_clause(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("having")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, expr) = expression(dialect)(i)?;

        Ok((i, expr))
    }
}

// Parse GROUP BY clause
pub fn group_by_clause(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], GroupByClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("group")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("by")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, fields) = field_reference_list(dialect)(i)?;
        Ok((i, GroupByClause { fields }))
    }
}

fn offset_clause(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("offset")(i)?;
        let (i, _) = whitespace1(i)?;
        literal(dialect)(i)
    }
}

// Parses a generic SQL `{limit} [OFFSET {offset}]`
// FIXME(#610/REA-3510): This implementation is incomplete
fn limit_offset_generic(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause> {
    move |i| {
        let (i, limit) = literal(dialect)(i)?;
        let (i, offset) = opt(offset_clause(dialect))(i)?;
        Ok((
            i,
            LimitClause::LimitOffset {
                limit: Some(limit),
                offset,
            },
        ))
    }
}

// Parse LIMIT [OFFSET] clause
fn limit_offset(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("limit")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, limit_offset) = alt((dialect.offset_limit(), limit_offset_generic(dialect)))(i)?;

        Ok((i, limit_offset))
    }
}

// Parse LIMIT [OFFSET] clause or a bare OFFSET clause
pub(crate) fn limit_offset_clause(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause> {
    move |i| {
        alt((
            limit_offset(dialect),
            map(offset_clause(dialect), |offset| LimitClause::LimitOffset {
                limit: None,
                offset: Some(offset),
            }),
        ))(i)
    }
}

fn join_constraint(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], JoinConstraint> {
    move |i| {
        let using_clause = map(
            tuple((
                tag_no_case("using"),
                whitespace1,
                delimited(
                    terminated(tag("("), whitespace0),
                    field_list(dialect),
                    preceded(whitespace0, tag(")")),
                ),
            )),
            |t| JoinConstraint::Using(t.2),
        );
        let on_condition = alt((
            delimited(
                terminated(tag("("), whitespace0),
                expression(dialect),
                preceded(whitespace0, tag(")")),
            ),
            preceded(whitespace1, expression(dialect)),
        ));
        let on_clause = map(tuple((tag_no_case("on"), on_condition)), |t| {
            JoinConstraint::On(t.1)
        });

        alt((using_clause, on_clause))(i).or(Ok((i, JoinConstraint::Empty)))
    }
}

// Parse JOIN clause
fn join_clause(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], JoinClause> {
    move |i| {
        let (remaining_input, (_, _natural, operator, _, right, _, constraint)) = tuple((
            whitespace0,
            opt(terminated(tag_no_case("natural"), whitespace1)),
            join_operator,
            whitespace1,
            join_rhs(dialect),
            whitespace0,
            join_constraint(dialect),
        ))(i)?;

        Ok((
            remaining_input,
            JoinClause {
                operator,
                right,
                constraint,
            },
        ))
    }
}

fn join_rhs(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], JoinRightSide> {
    move |i| {
        alt((
            map(table_expr(dialect), JoinRightSide::Table),
            map(
                delimited(tag("("), table_expr_list(dialect), tag(")")),
                JoinRightSide::Tables,
            ),
        ))(i)
    }
}

// Parse WHERE clause of a selection
pub fn where_clause(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (remaining_input, (_, _, _, where_condition)) = tuple((
            whitespace0,
            tag_no_case("where"),
            whitespace1,
            expression(dialect),
        ))(i)?;

        Ok((remaining_input, where_condition))
    }
}

// Parse rule for a SQL selection query.
pub fn selection(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SelectStatement> {
    move |i| terminated_with_statement_terminator(nested_selection(dialect))(i)
}

/// The semantics of SQL natively represent the FROM clause of a query as a fully nested AST of join
/// clauses, but our AST has distinct fields for the tables and a list of joins. To be able to parse
/// parenthesized join clauses with explicit precedence such as `FROM ((t1 JOIN t2) JOIN t3)`, we
/// first parse to a tree then convert to the latter representation afterwards.
#[derive(Debug)]
enum FromClause {
    Tables(Vec<TableExpr>),
    Join {
        lhs: Box<FromClause>,
        join_clause: JoinClause,
    },
}

impl FromClause {
    fn into_tables_and_joins(self) -> Result<(Vec<TableExpr>, Vec<JoinClause>), String> {
        use FromClause::*;

        match self {
            Tables(tables) => Ok((tables, vec![])),
            Join {
                mut lhs,
                join_clause,
            } => {
                let mut joins = vec![join_clause];
                let tables = loop {
                    match *lhs {
                        Tables(tables) => break tables,
                        Join {
                            lhs: new_lhs,
                            join_clause,
                        } => {
                            joins.push(join_clause);
                            lhs = new_lhs;
                        }
                    }
                };
                joins.reverse();
                Ok((tables, joins))
            }
        }
    }
}

fn from_clause_join(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FromClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, lhs) = nested_from_clause(dialect)(i)?;
        let (i, join_clause) = join_clause(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        Ok((
            i,
            FromClause::Join {
                lhs: Box::new(lhs),
                join_clause,
            },
        ))
    }
}

fn nested_from_clause(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FromClause> {
    move |i| {
        alt((
            map(table_expr_list(dialect), FromClause::Tables),
            delimited(tag("("), from_clause_join(dialect), tag(")")),
        ))(i)
    }
}

fn from_clause_tree(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FromClause> {
    move |i| {
        alt((
            delimited(tag("("), from_clause_join(dialect), tag(")")),
            from_clause_join(dialect),
            map(table_expr_list(dialect), FromClause::Tables),
        ))(i)
    }
}

fn from_clause(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FromClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("from")(i)?;
        let (i, _) = whitespace1(i)?;
        from_clause_tree(dialect)(i)
    }
}

fn cte(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CommonTableExpr> {
    move |i| {
        let (i, name) = dialect.identifier()(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("as")(i)?;
        let (i, _) = whitespace0(i)?;

        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, statement) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        Ok((i, CommonTableExpr { name, statement }))
    }
}

fn ctes(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<CommonTableExpr>> {
    move |i| {
        let (i, _) = tag_no_case("with")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, ctes) = separated_list1(ws_sep_comma, cte(dialect))(i)?;
        let (i, _) = whitespace0(i)?;

        Ok((i, ctes))
    }
}

pub fn nested_selection(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SelectStatement> {
    move |i| {
        let (i, ctes) = opt(ctes(dialect))(i)?;
        let (i, _) = tag_no_case("select")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, distinct) = opt(tag_no_case("distinct"))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, fields) = field_definition_expr(dialect)(i)?;

        let (i, from_clause) = opt(move |i| {
            let (i, from) = from_clause(dialect)(i)?;
            let (i, extra_joins) = many0(join_clause(dialect))(i)?;
            let (i, where_clause) = opt(where_clause(dialect))(i)?;
            let (i, group_by) = opt(group_by_clause(dialect))(i)?;
            let (i, having) = opt(having_clause(dialect))(i)?;
            let (i, order) = opt(order_clause(dialect))(i)?;
            let (i, limit_clause) = opt(limit_offset_clause(dialect))(i)?;

            Ok((
                i,
                (
                    from,
                    extra_joins,
                    where_clause,
                    having,
                    group_by,
                    order,
                    limit_clause.unwrap_or_default(),
                ),
            ))
        })(i)?;

        let mut result = SelectStatement {
            ctes: ctes.unwrap_or_default(),
            distinct: distinct.is_some(),
            fields,
            ..Default::default()
        };

        if let Some((from, extra_joins, where_clause, having, group_by, order, limit_clause)) =
            from_clause
        {
            let (tables, mut join) = from.into_tables_and_joins().map_err(|_| {
                nom::Err::Error(NomSqlError {
                    input: i,
                    kind: ErrorKind::Tag,
                })
            })?;

            join.extend(extra_joins);

            result.tables = tables;
            result.join = join;
            result.where_clause = where_clause;
            result.group_by = group_by;
            result.having = having;
            result.order = order;
            result.limit_clause = limit_clause;
        }

        Ok((i, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::common::FieldDefinitionExpr;
    use crate::expression::CaseWhenBranch;
    use crate::table::Relation;
    use crate::{
        to_nom_result, BinaryOperator, Expr, FunctionExpr, InValue, ItemPlaceholder, OrderBy,
        SqlType, TableExprInner,
    };

    fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpr> {
        cols.iter()
            .map(|c| FieldDefinitionExpr::from(Column::from(*c)))
            .collect()
    }

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: columns(&["id", "name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_without_table() {
        let qstring = "SELECT * FROM;";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert!(res.is_err(), "!{:?}.is_err()", res);
    }

    #[test]
    fn bare_expression_select() {
        let qstring = "SELECT 1";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                fields: vec![FieldDefinitionExpr::Expr {
                    expr: Expr::Literal(Literal::Integer(1)),
                    alias: None
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn more_involved_select() {
        let qstring = "SELECT users.id, users.name FROM users;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: columns(&["users.id", "users.name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all() {
        let qstring = "SELECT * FROM users;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: vec![FieldDefinitionExpr::All],
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all_in_table() {
        let qstring = "SELECT users.* FROM users, votes;";

        let res = test_parse!(selection(Dialect::MySQL), qstring.as_bytes());
        assert_eq!(
            res,
            SelectStatement {
                tables: vec![
                    TableExpr::from(Relation::from("users")),
                    TableExpr::from(Relation::from("votes"))
                ],
                fields: vec![FieldDefinitionExpr::AllInTable("users".into())],
                ..Default::default()
            }
        );
    }

    #[test]
    fn spaces_optional() {
        let qstring = "SELECT id,name FROM users;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: columns(&["id", "name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn case_sensitivity() {
        let qstring_lc = "select id, name from users;";
        let qstring_uc = "SELECT id, name FROM users;";

        assert_eq!(
            selection(Dialect::MySQL)(LocatedSpan::new(qstring_lc.as_bytes())).unwrap(),
            selection(Dialect::MySQL)(LocatedSpan::new(qstring_uc.as_bytes())).unwrap()
        );
    }

    #[test]
    fn termination() {
        let qstring_sem = "select id, name from users;";
        let qstring_nosem = "select id, name from users";
        let qstring_linebreak = "select id, name from users\n";

        let r1 = to_nom_result(selection(Dialect::MySQL)(LocatedSpan::new(
            qstring_sem.as_bytes(),
        )))
        .unwrap();
        let r2 = to_nom_result(selection(Dialect::MySQL)(LocatedSpan::new(
            qstring_nosem.as_bytes(),
        )))
        .unwrap();
        let r3 = to_nom_result(selection(Dialect::MySQL)(LocatedSpan::new(
            qstring_linebreak.as_bytes(),
        )))
        .unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    #[test]
    fn where_clause() {
        where_clause_with_variable_placeholder(
            "select * from ContactInfo where email=?;",
            Literal::Placeholder(ItemPlaceholder::QuestionMark),
        );
    }

    #[test]
    fn where_clause_with_dollar_variable() {
        where_clause_with_variable_placeholder(
            "select * from ContactInfo where email= $3;",
            Literal::Placeholder(ItemPlaceholder::DollarNumber(3)),
        );
    }

    #[test]
    fn where_clause_with_colon_variable() {
        where_clause_with_variable_placeholder(
            "select * from ContactInfo where email= :5;",
            Literal::Placeholder(ItemPlaceholder::ColonNumber(5)),
        );
    }

    fn where_clause_with_variable_placeholder(qstring: &str, literal: Literal) {
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(Expr::Column("email".into())),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(literal)),
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("ContactInfo"))],
                fields: vec![FieldDefinitionExpr::All],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn limit_clause() {
        let qstring1 = "select * from users limit 10\n";
        let qstring2 = "select * from users limit 10 offset 10\n";
        let qstring3 = "select * from users limit 5, 10\n";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        let res2 = test_parse!(selection(Dialect::MySQL), qstring2.as_bytes());
        let res3 = test_parse!(selection(Dialect::MySQL), qstring3.as_bytes());
        let res3_pgsql = selection(Dialect::PostgreSQL)(LocatedSpan::new(qstring3.as_bytes()));

        assert_eq!(
            res1.limit_clause,
            LimitClause::LimitOffset {
                limit: Some(10.into()),
                offset: None
            }
        );
        assert_eq!(
            res2.limit_clause,
            LimitClause::LimitOffset {
                limit: Some(10.into()),
                offset: Some(10.into())
            }
        );
        assert_eq!(
            res3.limit_clause,
            LimitClause::OffsetCommaLimit {
                limit: 10.into(),
                offset: 5.into()
            }
        );
        res3_pgsql.unwrap_err();
    }

    #[test]
    fn table_alias() {
        let qstring1 = "select * from PaperTag as t;";
        // let qstring2 = "select * from PaperTag t;";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        assert_eq!(
            res1,
            SelectStatement {
                tables: vec![TableExpr {
                    inner: TableExprInner::Table(Relation {
                        schema: None,
                        name: "PaperTag".into(),
                    }),
                    alias: Some("t".into()),
                }],
                fields: vec![FieldDefinitionExpr::All],
                ..Default::default()
            }
        );
        // let res2 = selection(Dialect::MySQL)(qstring2.as_bytes());
        // assert_eq!(res1.unwrap().1, res2.unwrap().1);
    }

    #[test]
    fn table_schema() {
        let qstring1 = "select * from db1.PaperTag as t;";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        assert_eq!(
            res1,
            SelectStatement {
                tables: vec![TableExpr {
                    inner: TableExprInner::Table(Relation {
                        name: "PaperTag".into(),
                        schema: Some("db1".into()),
                    }),
                    alias: Some("t".into()),
                },],
                fields: vec![FieldDefinitionExpr::All],
                ..Default::default()
            }
        );
        // let res2 = selection(Dialect::MySQL)(qstring2.as_bytes());
        // assert_eq!(res1.unwrap().1, res2.unwrap().1);
    }

    #[test]
    fn column_alias() {
        let qstring1 = "select name as TagName from PaperTag;";
        let qstring2 = "select PaperTag.name as TagName from PaperTag;";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        assert_eq!(
            res1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperTag"))],
                fields: vec![FieldDefinitionExpr::Expr {
                    alias: Some("TagName".into()),
                    expr: Expr::Column(Column::from("name"))
                }],
                ..Default::default()
            }
        );
        let res2 = test_parse!(selection(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(
            res2,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperTag"))],
                fields: vec![FieldDefinitionExpr::Expr {
                    expr: Expr::Column(Column::from("PaperTag.name")),
                    alias: Some("TagName".into()),
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn column_alias_no_as() {
        let qstring1 = "select name TagName from PaperTag;";
        let qstring2 = "select PaperTag.name TagName from PaperTag;";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        assert_eq!(
            res1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperTag"))],
                fields: vec![FieldDefinitionExpr::Expr {
                    alias: Some("TagName".into()),
                    expr: Expr::Column(Column {
                        name: "name".into(),
                        table: None,
                    })
                }],
                ..Default::default()
            }
        );
        let res2 = test_parse!(selection(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(
            res2,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperTag"))],
                fields: vec![FieldDefinitionExpr::Expr {
                    alias: Some("TagName".into()),
                    expr: Expr::Column(Column::from("PaperTag.name"))
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn distinct() {
        let qstring = "select distinct tag from PaperTag where paperId=?;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(Expr::Column("paperId".into())),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperTag"))],
                distinct: true,
                fields: columns(&["tag"]),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_condition_expr() {
        let qstring = "select infoJson from PaperStorage where paperId=? and paperStorageId=?;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let left_comp = Box::new(Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("paperId"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        let right_comp = Box::new(Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("paperStorageId"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: left_comp,
            op: BinaryOperator::And,
            rhs: right_comp,
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperStorage"))],
                fields: columns(&["infoJson"]),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn where_and_limit_clauses() {
        let qstring = "select * from users where id = ? limit 10\n";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let ct = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("id"))),
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        };
        let expected_where_cond = Some(ct);

        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: vec![FieldDefinitionExpr::All],
                where_clause: expected_where_cond,
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(10.into()),
                    offset: None
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn limit_placeholder() {
        let res = test_parse!(selection(Dialect::MySQL), b"select * from users limit ?");
        assert_eq!(
            res.limit_clause,
            LimitClause::LimitOffset {
                limit: Some(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                offset: None
            }
        )
    }

    #[test]
    fn aggregation_column() {
        let qstring = "SELECT max(addr_id) FROM address;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let agg_expr = FunctionExpr::Max(Box::new(Expr::Column(Column::from("addr_id"))));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("address"))],
                fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr),),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn aggregation_column_with_alias() {
        let qstring = "SELECT max(addr_id) AS max_addr FROM address;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let agg_expr = FunctionExpr::Max(Box::new(Expr::Column(Column::from("addr_id"))));
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("address"))],
            fields: vec![FieldDefinitionExpr::Expr {
                alias: Some("max_addr".into()),
                expr: Expr::Call(agg_expr),
            }],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_all() {
        let qstring = "SELECT COUNT(*) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let agg_expr = FunctionExpr::CountStar;
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from("aid")))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_distinct() {
        let qstring = "SELECT COUNT(DISTINCT vote_id) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let agg_expr = FunctionExpr::Count {
            expr: Box::new(Expr::Column(Column::from("vote_id"))),
            distinct: true,
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from("aid")))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_filter() {
        let qstring =
            "SELECT COUNT(CASE WHEN vote_id > 10 THEN vote_id END) FROM votes GROUP BY aid;";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let filter_cond = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("vote_id"))),
            op: BinaryOperator::Greater,
            rhs: Box::new(Expr::Literal(Literal::Integer(10))),
        };
        let agg_expr = FunctionExpr::Count {
            expr: Box::new(Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: filter_cond,
                    body: Expr::Column(Column::from("vote_id")),
                }],
                else_expr: None,
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from("aid")))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn sum_filter() {
        let qstring = "SELECT SUM(CASE WHEN sign = 1 THEN vote_id END) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let filter_cond = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("sign"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
        };
        let agg_expr = FunctionExpr::Sum {
            expr: Box::new(Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: filter_cond,
                    body: Expr::Column(Column::from("vote_id")),
                }],
                else_expr: None,
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from("aid")))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn sum_filter_else_literal() {
        let qstring =
            "SELECT SUM(CASE WHEN sign = 1 THEN vote_id ELSE 6 END) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let filter_cond = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("sign"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
        };
        let agg_expr = FunctionExpr::Sum {
            expr: Box::new(Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: filter_cond,
                    body: Expr::Column(Column::from("vote_id")),
                }],
                else_expr: Some(Box::new(Expr::Literal(Literal::Integer(6)))),
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from("aid")))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_filter_lobsters() {
        let qstring = "SELECT
            COUNT(CASE WHEN votes.story_id IS NULL AND votes.vote = 0 THEN votes.vote END) as votes
            FROM votes
            GROUP BY votes.comment_id;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let filter_cond = Expr::BinaryOp {
            lhs: Box::new(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("votes.story_id"))),
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(Literal::Null)),
            }),
            rhs: Box::new(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("votes.vote"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(Literal::Integer(0))),
            }),
            op: BinaryOperator::And,
        };
        let agg_expr = FunctionExpr::Count {
            expr: Box::new(Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: filter_cond,
                    body: Expr::Column(Column::from("votes.vote")),
                }],
                else_expr: None,
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("votes"))],
            fields: vec![FieldDefinitionExpr::Expr {
                alias: Some("votes".into()),
                expr: Expr::Call(agg_expr),
            }],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column(Column::from(
                    "votes.comment_id",
                )))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn generic_function_query() {
        let qstring = "SELECT coalesce(a, b,c) as x,d FROM sometable;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let agg_expr = FunctionExpr::Call {
            name: "coalesce".into(),
            arguments: vec![
                Expr::Column(Column {
                    name: "a".into(),
                    table: None,
                }),
                Expr::Column(Column {
                    name: "b".into(),
                    table: None,
                }),
                Expr::Column(Column {
                    name: "c".into(),
                    table: None,
                }),
            ],
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("sometable"))],
            fields: vec![
                FieldDefinitionExpr::Expr {
                    alias: Some("x".into()),
                    expr: Expr::Call(agg_expr),
                },
                FieldDefinitionExpr::from(Column {
                    name: "d".into(),
                    table: None,
                }),
            ],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn moderately_complex_selection() {
        let qstring = "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND \
                       item.i_subject = ? ORDER BY item.i_title limit 50;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("item.i_a_id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Column(Column::from("author.a_id"))),
            }),
            rhs: Box::new(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("item.i_subject"))),
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            }),
            op: BinaryOperator::And,
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![
                    TableExpr::from(Relation::from("item")),
                    TableExpr::from(Relation::from("author"))
                ],
                fields: vec![FieldDefinitionExpr::All],
                where_clause: expected_where_cond,
                order: Some(OrderClause {
                    order_by: vec![OrderBy {
                        field: FieldReference::Expr(Expr::Column("item.i_title".into())),
                        order_type: None,
                        null_order: None
                    }],
                }),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(50.into()),
                    offset: None
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_joins() {
        let qstring = "select paperId from PaperConflict join PCMember using (contactId);";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("PaperConflict"))],
            fields: columns(&["paperId"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::Table(TableExpr::from(Relation::from("PCMember"))),
                constraint: JoinConstraint::Using(vec![Column::from("contactId")]),
            }],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);

        // slightly simplified from
        // "select PCMember.contactId, group_concat(reviewType separator '')
        // from PCMember left join PaperReview on (PCMember.contactId=PaperReview.contactId)
        // group by PCMember.contactId"
        let qstring = "select PCMember.contactId \
                       from PCMember \
                       join PaperReview on PCMember.contactId=PaperReview.contactId \
                       order by contactId;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("PCMember"))],
            fields: columns(&["PCMember.contactId"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::Table(TableExpr::from(Relation::from("PaperReview"))),
                constraint: JoinConstraint::On(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("PCMember.contactId"))),
                    rhs: Box::new(Expr::Column(Column::from("PaperReview.contactId"))),
                    op: BinaryOperator::Equal,
                }),
            }],
            order: Some(OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column("contactId".into())),
                    order_type: None,
                    null_order: None,
                }],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn multi_join() {
        // simplified from
        // "select max(conflictType), PaperReview.contactId as reviewer, PCMember.contactId as
        //  pcMember, ChairAssistant.contactId as assistant, Chair.contactId as chair,
        //  max(PaperReview.reviewNeedsSubmit) as reviewNeedsSubmit from ContactInfo
        //  left join PaperReview using (contactId) left join PaperConflict using (contactId)
        //  left join PCMember using (contactId) left join ChairAssistant using (contactId)
        //  left join Chair using (contactId) where ContactInfo.contactId=?
        //  group by ContactInfo.contactId;";
        let qstring = "select PCMember.contactId, ChairAssistant.contactId, \
                       Chair.contactId from ContactInfo left join PaperReview using (contactId) \
                       left join PaperConflict using (contactId) left join PCMember using \
                       (contactId) left join ChairAssistant using (contactId) left join Chair \
                       using (contactId) where ContactInfo.contactId=?;";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let ct = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("ContactInfo.contactId"))),
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        };
        let expected_where_cond = Some(ct);
        let mkjoin = |tbl: &str, col: &str| -> JoinClause {
            JoinClause {
                operator: JoinOperator::LeftJoin,
                right: JoinRightSide::Table(TableExpr::from(Relation::from(tbl))),
                constraint: JoinConstraint::Using(vec![Column::from(col)]),
            }
        };
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("ContactInfo"))],
                fields: columns(&[
                    "PCMember.contactId",
                    "ChairAssistant.contactId",
                    "Chair.contactId"
                ]),
                join: vec![
                    mkjoin("PaperReview", "contactId"),
                    mkjoin("PaperConflict", "contactId"),
                    mkjoin("PCMember", "contactId"),
                    mkjoin("ChairAssistant", "contactId"),
                    mkjoin("Chair", "contactId"),
                ],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn nested_select() {
        let qstr = "SELECT ol_i_id FROM orders, order_line \
                    WHERE orders.o_c_id IN (SELECT o_c_id FROM orders, order_line \
                    WHERE orders.o_id = order_line.ol_o_id);";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));
        let inner_where_clause = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Column(Column::from("order_line.ol_o_id"))),
        };

        let inner_select = SelectStatement {
            tables: vec![
                TableExpr::from(Relation::from("orders")),
                TableExpr::from(Relation::from("order_line")),
            ],
            fields: columns(&["o_c_id"]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = Expr::In {
            lhs: Box::new(Expr::Column(Column::from("orders.o_c_id"))),
            rhs: InValue::Subquery(Box::new(inner_select)),
            negated: false,
        };

        let outer_select = SelectStatement {
            tables: vec![
                TableExpr::from(Relation::from("orders")),
                TableExpr::from(Relation::from("order_line")),
            ],
            fields: columns(&["ol_i_id"]),
            where_clause: Some(outer_where_clause),
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }

    #[test]
    fn recursive_nested_select() {
        let qstr = "SELECT ol_i_id FROM orders, order_line WHERE orders.o_c_id \
                    IN (SELECT o_c_id FROM orders, order_line \
                    WHERE orders.o_id = order_line.ol_o_id \
                    AND orders.o_id > (SELECT MAX(o_id) FROM orders));";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let agg_expr = FunctionExpr::Max(Box::new(Expr::Column(Column::from("o_id"))));
        let recursive_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("orders"))],
            fields: vec![FieldDefinitionExpr::from(Expr::Call(agg_expr))],
            ..Default::default()
        };

        let cop1 = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Column(Column::from("order_line.ol_o_id"))),
        };

        let cop2 = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Greater,
            rhs: Box::new(Expr::NestedSelect(Box::new(recursive_select))),
        };

        let inner_where_clause = Expr::BinaryOp {
            lhs: Box::new(cop1),
            op: BinaryOperator::And,
            rhs: Box::new(cop2),
        };

        let inner_select = SelectStatement {
            tables: vec![
                TableExpr::from(Relation::from("orders")),
                TableExpr::from(Relation::from("order_line")),
            ],
            fields: columns(&["o_c_id"]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = Expr::In {
            lhs: Box::new(Expr::Column(Column::from("orders.o_c_id"))),
            rhs: InValue::Subquery(Box::new(inner_select)),
            negated: false,
        };

        let outer_select = SelectStatement {
            tables: vec![
                TableExpr::from(Relation::from("orders")),
                TableExpr::from(Relation::from("order_line")),
            ],
            fields: columns(&["ol_i_id"]),
            where_clause: Some(outer_where_clause),
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }

    #[test]
    fn select_from_subquery() {
        let res = test_parse!(
            selection(Dialect::MySQL),
            b"SELECT x FROM (SELECT x FROM t) sq WHERE x = 1"
        );
        assert_eq!(
            res,
            SelectStatement {
                tables: vec![TableExpr {
                    inner: TableExprInner::Subquery(Box::new(SelectStatement {
                        tables: vec![TableExpr::from(Relation::from("t"))],
                        fields: columns(&["x"]),
                        ..Default::default()
                    })),
                    alias: Some("sq".into())
                }],
                fields: columns(&["x"]),
                where_clause: Some(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(1.into()))
                }),
                ..Default::default()
            }
        )
    }

    #[test]
    fn join_against_nested_select() {
        let t1 = b"(SELECT ol_i_id FROM order_line) AS ids";

        join_rhs(Dialect::MySQL)(LocatedSpan::new(t1)).unwrap();

        let t1 = b"JOIN (SELECT ol_i_id FROM order_line) AS ids ON (orders.o_id = ids.ol_i_id)";

        join_clause(Dialect::MySQL)(LocatedSpan::new(t1)).unwrap();

        let qstr_with_alias = "SELECT o_id, ol_i_id FROM orders JOIN \
                               (SELECT ol_i_id FROM order_line) AS ids \
                               ON (orders.o_id = ids.ol_i_id);";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr_with_alias.as_bytes()));

        // N.B.: Don't alias the inner select to `inner`, which is, well, a SQL keyword!
        let inner_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("order_line"))],
            fields: columns(&["ol_i_id"]),
            ..Default::default()
        };

        let outer_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("orders"))],
            fields: columns(&["o_id", "ol_i_id"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::Table(TableExpr {
                    inner: TableExprInner::Subquery(Box::new(inner_select)),
                    alias: Some("ids".into()),
                }),
                constraint: JoinConstraint::On(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("orders.o_id"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Column(Column::from("ids.ol_i_id"))),
                }),
            }],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }

    #[test]
    fn project_arithmetic_expressions() {
        let qstr = "SELECT MAX(o_id)-3333 FROM orders;";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let expected = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("orders"))],
            fields: vec![FieldDefinitionExpr::from(Expr::BinaryOp {
                lhs: Box::new(Expr::Call(FunctionExpr::Max(Box::new(Expr::Column(
                    "o_id".into(),
                ))))),
                op: BinaryOperator::Subtract,
                rhs: Box::new(Expr::Literal(3333.into())),
            })],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn project_arithmetic_expressions_with_aliases() {
        let qstr = "SELECT max(o_id) * 2 as double_max FROM orders;";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let expected = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("orders"))],
            fields: vec![FieldDefinitionExpr::Expr {
                alias: Some("double_max".into()),
                expr: Expr::BinaryOp {
                    lhs: Box::new(Expr::Call(FunctionExpr::Max(Box::new(Expr::Column(
                        "o_id".into(),
                    ))))),
                    op: BinaryOperator::Multiply,
                    rhs: Box::new(Expr::Literal(2.into())),
                },
            }],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn where_call_in_list() {
        let qstr = b"SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr));
        let (rem, res) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            res.where_clause,
            Some(Expr::In {
                lhs: Box::new(Expr::Call(FunctionExpr::Avg {
                    expr: Box::new(Expr::Column("y".into())),
                    distinct: false
                })),
                rhs: InValue::List(vec![
                    Expr::Literal(Literal::Placeholder(
                        ItemPlaceholder::QuestionMark
                    ));
                    3
                ]),
                negated: false
            })
        );
    }

    #[test]
    fn alias_cast() {
        let qstr = "SELECT id, CAST(created_at AS date) AS created_day FROM users WHERE id = ?;";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));
        assert!(res.is_ok(), "!{:?}.is_ok()", res);
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![TableExpr::from(Relation::from("users"))],
                fields: vec![
                    FieldDefinitionExpr::from(Column::from("id")),
                    FieldDefinitionExpr::Expr {
                        alias: Some("created_day".into()),
                        expr: Expr::Cast {
                            expr: Box::new(Expr::Column(Column::from("created_at"))),
                            ty: SqlType::Date,
                            postgres_style: false,
                        },
                    },
                ],
                where_clause: Some(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("id"))),
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(
                        ItemPlaceholder::QuestionMark
                    ))),
                    op: BinaryOperator::Equal,
                }),
                ..Default::default()
            }
        )
    }

    #[test]
    fn simple_cte() {
        let qstr = b"WITH max_val AS (SELECT max(value) as value FROM t1)
            SELECT name FROM t2 JOIN max_val ON max_val.value = t2.value";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr));
        assert!(res.is_ok(), "error parsing query: {}", res.err().unwrap());
        let (rem, query) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(query.ctes.len(), 1);
        assert_eq!(query.ctes.first().unwrap().name, "max_val");
    }

    #[test]
    fn multiple_ctes() {
        let qstr = b"WITH
              max_val AS (SELECT max(value) as value FROM t1),
              min_val AS (SELECT min(value) as value FROM t1)
            SELECT name FROM t2
            JOIN max_val ON max_val.value = t2.max_value
            JOIN min_val ON min_val.value = t2.min_value";
        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr));
        assert!(res.is_ok(), "error parsing query: {}", res.err().unwrap());
        let (rem, query) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(query.ctes.len(), 2);
        assert_eq!(query.ctes[0].name, "max_val");
        assert_eq!(query.ctes[1].name, "min_val");
    }

    mod mysql {
        use super::*;
        use crate::column::Column;
        use crate::common::FieldDefinitionExpr;
        use crate::table::Relation;
        use crate::{BinaryOperator, Expr, FunctionExpr, InValue};

        #[test]
        fn alias_generic_function() {
            let qstr = "SELECT id, coalesce(a, \"b\",c) AS created_day FROM users;";
            let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![TableExpr::from(Relation::from("users"))],
                    fields: vec![
                        FieldDefinitionExpr::from(Column::from("id")),
                        FieldDefinitionExpr::Expr {
                            alias: Some("created_day".into()),
                            expr: Expr::Call(FunctionExpr::Call {
                                name: "coalesce".into(),
                                arguments: vec![
                                    Expr::Column(Column::from("a")),
                                    Expr::Literal(Literal::String("b".to_owned())),
                                    Expr::Column(Column::from("c"))
                                ]
                            }),
                        },
                    ],
                    where_clause: None,
                    ..Default::default()
                }
            )
        }

        #[test]
        fn select_literals() {
            let qstring = "SELECT NULL, 1, \"foo\", CURRENT_TIME FROM users;";
            // TODO: doesn't support selecting literals without a FROM clause, which is still valid
            // SQL        let qstring = "SELECT NULL, 1, \"foo\";";

            let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![TableExpr::from(Relation::from("users"))],
                    fields: vec![
                        FieldDefinitionExpr::from(Expr::Literal(Literal::Null,)),
                        FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(1),)),
                        FieldDefinitionExpr::from(
                            Expr::Literal(Literal::String("foo".to_owned()),)
                        ),
                        FieldDefinitionExpr::from(Expr::Call(FunctionExpr::Call {
                            name: "CURRENT_TIME".into(),
                            arguments: vec![]
                        })),
                    ],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn where_in_clause() {
            let qstr = "SELECT `auth_permission`.`content_type_id`, `auth_permission`.`codename`
                    FROM `auth_permission`
                    JOIN `django_content_type`
                      ON ( `auth_permission`.`content_type_id` = `django_content_type`.`id` )
                    WHERE `auth_permission`.`content_type_id` IN (0);";
            let res = selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

            let expected_where_clause = Some(Expr::In {
                lhs: Box::new(Expr::Column(Column::from(
                    "auth_permission.content_type_id",
                ))),
                rhs: InValue::List(vec![Expr::Literal(0.into())]),
                negated: false,
            });

            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("auth_permission"))],
                fields: vec![
                    FieldDefinitionExpr::from(Column::from("auth_permission.content_type_id")),
                    FieldDefinitionExpr::from(Column::from("auth_permission.codename")),
                ],
                join: vec![JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(TableExpr::from(Relation::from(
                        "django_content_type",
                    ))),
                    constraint: JoinConstraint::On(Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(Column::from(
                            "auth_permission.content_type_id",
                        ))),
                        rhs: Box::new(Expr::Column(Column::from("django_content_type.id"))),
                    }),
                }],
                where_clause: expected_where_clause,
                ..Default::default()
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn group_by_column_number() {
            let res = test_parse!(
                selection(Dialect::MySQL),
                b"SELECT id, count(*) FROM t GROUP BY 1"
            );
            assert_eq!(
                res.group_by,
                Some(GroupByClause {
                    fields: vec![FieldReference::Numeric(1)]
                })
            )
        }

        #[test]
        fn order_by_column_number() {
            let res = test_parse!(selection(Dialect::MySQL), b"SELECT id FROM t ORDER BY 1");
            assert_eq!(
                res.order,
                Some(OrderClause {
                    order_by: vec![OrderBy {
                        field: FieldReference::Numeric(1),
                        order_type: None,
                        null_order: None
                    }]
                })
            )
        }

        #[test]
        fn format_ctes() {
            let query = SelectStatement {
                ctes: vec![CommonTableExpr {
                    name: "foo".into(),
                    statement: SelectStatement {
                        fields: vec![FieldDefinitionExpr::Expr {
                            expr: Expr::Column("x".into()),
                            alias: None,
                        }],
                        tables: vec![TableExpr::from(Relation::from("t"))],
                        ..Default::default()
                    },
                }],
                fields: vec![FieldDefinitionExpr::Expr {
                    expr: Expr::Column("x".into()),
                    alias: None,
                }],
                tables: vec![TableExpr::from(Relation::from("foo"))],
                ..Default::default()
            };
            let res = query.display(Dialect::MySQL).to_string();
            assert_eq!(
                res,
                "WITH `foo` AS (SELECT `x` FROM `t`) SELECT `x` FROM `foo`"
            );
        }

        #[test]
        fn display_query_with_limit_and_offset() {
            let query = SelectStatement {
                fields: vec![FieldDefinitionExpr::Expr {
                    expr: Expr::Column("x".into()),
                    alias: None,
                }],
                tables: vec![TableExpr::from(Relation::from("t"))],
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(10.into()),
                    offset: Some(0.into()),
                },
                ..Default::default()
            };
            let res = query.display(Dialect::MySQL).to_string();
            assert_eq!(res, "SELECT `x` FROM `t` LIMIT 10 OFFSET 0");
        }

        #[test]
        fn bare_having() {
            let res = test_parse!(
                selection(Dialect::MySQL),
                b"select x, count(*) from t having count(*) > 1"
            );
            assert_eq!(
                res.having,
                Some(Expr::BinaryOp {
                    lhs: Box::new(Expr::Call(FunctionExpr::CountStar)),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(1.into()))
                })
            );
            let stringified = res.display(Dialect::MySQL).to_string();
            assert_eq!(
                stringified,
                "SELECT `x`, count(*) FROM `t` HAVING (count(*) > 1)"
            );
        }

        #[test]
        fn left_join_inner_join() {
            let res = test_parse!(
                selection(Dialect::MySQL),
                b"select * from t1 left join t2 on x inner join t3 on z"
            );
            assert_eq!(
                res.join,
                vec![
                    JoinClause {
                        operator: JoinOperator::LeftJoin,
                        right: JoinRightSide::Table(TableExpr {
                            inner: TableExprInner::Table("t2".into()),
                            alias: None
                        }),
                        constraint: JoinConstraint::On(Expr::Column("x".into()))
                    },
                    JoinClause {
                        operator: JoinOperator::InnerJoin,
                        right: JoinRightSide::Table(TableExpr {
                            inner: TableExprInner::Table("t3".into()),
                            alias: None
                        }),
                        constraint: JoinConstraint::On(Expr::Column("z".into()))
                    },
                ]
            )
        }
    }

    mod postgres {
        use super::*;
        use crate::column::Column;
        use crate::common::FieldDefinitionExpr;
        use crate::table::Relation;
        use crate::{BinaryOperator, Double, Expr, FunctionExpr, InValue};

        test_format_parse_round_trip!(
            rt_limit_clause(limit_offset, LimitClause, Dialect::PostgreSQL);
        );

        #[test]
        fn alias_generic_function() {
            let qstr = "SELECT id, coalesce(a, 'b',c) AS created_day FROM users;";
            let res = selection(Dialect::PostgreSQL)(LocatedSpan::new(qstr.as_bytes()));
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![TableExpr::from(Relation::from("users"))],
                    fields: vec![
                        FieldDefinitionExpr::from(Column::from("id")),
                        FieldDefinitionExpr::Expr {
                            alias: Some("created_day".into()),
                            expr: Expr::Call(FunctionExpr::Call {
                                name: "coalesce".into(),
                                arguments: vec![
                                    Expr::Column(Column::from("a")),
                                    Expr::Literal(Literal::String("b".to_owned())),
                                    Expr::Column(Column::from("c"))
                                ]
                            }),
                        },
                    ],
                    where_clause: None,
                    ..Default::default()
                }
            )
        }

        #[test]
        fn select_literals() {
            let qstring = "SELECT NULL, 1, 'foo', CURRENT_TIME FROM users;";
            // TODO: doesn't support selecting literals without a FROM clause, which is still valid
            // SQL        let qstring = "SELECT NULL, 1, \"foo\";";

            let res = selection(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![TableExpr::from(Relation::from("users"))],
                    fields: vec![
                        FieldDefinitionExpr::from(Expr::Literal(Literal::Null,)),
                        FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(1),)),
                        FieldDefinitionExpr::from(
                            Expr::Literal(Literal::String("foo".to_owned()),)
                        ),
                        FieldDefinitionExpr::from(Expr::Call(FunctionExpr::Call {
                            name: "CURRENT_TIME".into(),
                            arguments: vec![]
                        })),
                    ],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn where_in_clause() {
            let qstr = "SELECT \"auth_permission\".\"content_type_id\", \"auth_permission\".\"codename\"
                    FROM \"auth_permission\"
                    JOIN \"django_content_type\"
                      ON ( \"auth_permission\".\"content_type_id\" = \"django_content_type\".\"id\" )
                    WHERE \"auth_permission\".\"content_type_id\" IN (0);";
            let res = selection(Dialect::PostgreSQL)(LocatedSpan::new(qstr.as_bytes()));

            let expected_where_clause = Some(Expr::In {
                lhs: Box::new(Expr::Column(Column::from(
                    "auth_permission.content_type_id",
                ))),
                rhs: InValue::List(vec![Expr::Literal(0.into())]),
                negated: false,
            });

            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("auth_permission"))],
                fields: vec![
                    FieldDefinitionExpr::from(Column::from("auth_permission.content_type_id")),
                    FieldDefinitionExpr::from(Column::from("auth_permission.codename")),
                ],
                join: vec![JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(TableExpr::from(Relation::from(
                        "django_content_type",
                    ))),
                    constraint: JoinConstraint::On(Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(Column::from(
                            "auth_permission.content_type_id",
                        ))),
                        rhs: Box::new(Expr::Column(Column::from("django_content_type.id"))),
                    }),
                }],
                where_clause: expected_where_clause,
                ..Default::default()
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn format_ctes() {
            let query = SelectStatement {
                ctes: vec![CommonTableExpr {
                    name: "foo".into(),
                    statement: SelectStatement {
                        fields: vec![FieldDefinitionExpr::Expr {
                            expr: Expr::Column("x".into()),
                            alias: None,
                        }],
                        tables: vec![TableExpr::from(Relation::from("t"))],
                        ..Default::default()
                    },
                }],
                fields: vec![FieldDefinitionExpr::Expr {
                    expr: Expr::Column("x".into()),
                    alias: None,
                }],
                tables: vec![TableExpr::from(Relation::from("foo"))],
                ..Default::default()
            };
            let res = query.display(Dialect::PostgreSQL).to_string();
            assert_eq!(
                res,
                "WITH \"foo\" AS (SELECT \"x\" FROM \"t\") SELECT \"x\" FROM \"foo\""
            );
        }

        #[test]
        fn bare_having() {
            let res = test_parse!(
                selection(Dialect::PostgreSQL),
                b"select x, count(*) from t having count(*) > 1"
            );
            assert_eq!(
                res.having,
                Some(Expr::BinaryOp {
                    lhs: Box::new(Expr::Call(FunctionExpr::CountStar)),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(1.into()))
                })
            );
            let stringified = res.display(Dialect::PostgreSQL).to_string();
            assert_eq!(
                stringified,
                "SELECT \"x\", count(*) FROM \"t\" HAVING (count(*) > 1)"
            );
        }

        #[test]
        fn flarum_select_roundtrip_1() {
            let qstr = "select exists(select * from \"groups\" where \"id\" = ?) as \"exists\"";
            let res = test_parse!(selection(Dialect::PostgreSQL), qstr.as_bytes());
            let qstr = res.display(Dialect::PostgreSQL).to_string();
            println!("{}", qstr);
            assert_eq!(
                qstr,
                "SELECT EXISTS (SELECT * FROM \"groups\" WHERE (\"id\" = ?)) AS \"exists\""
            );
        }

        #[test]
        fn prisma_select() {
            let qstr = br#"SELECT "public"."User"."id", "public"."User"."email", "public"."User"."name" FROM "public"."User" WHERE 1=1 OFFSET $1"#;
            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(
                res.tables,
                vec![TableExpr {
                    inner: TableExprInner::Table(Relation {
                        schema: Some("public".into()),
                        name: "User".into(),
                    }),
                    alias: None,
                }]
            );
        }

        #[test]
        fn select_with_decimal_limit() {
            let qstr = br#"SELECT id FROM a LIMIT 0.0"#;
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Double(Double {
                        value: 0.0,
                        precision: 1,
                    })),
                    offset: None,
                },
                ..Default::default()
            };
            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(res, expected);
        }

        #[test]
        fn select_with_decimal_limit_zero_no_right_hand_side() {
            let qstr = br#"SELECT id FROM a LIMIT 0.;"#;
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Double(Double {
                        value: 0.0,
                        precision: 0,
                    })),
                    offset: None,
                },
                ..Default::default()
            };
            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(res, expected);
        }

        #[test]
        fn select_with_decimal_limit_non_zero() {
            let qstr = br#"SELECT id FROM a LIMIT 1.1"#;
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Double(Double {
                        value: 1.1,
                        precision: 1,
                    })),
                    offset: None,
                },
                ..Default::default()
            };
            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(res, expected);
        }

        #[test]
        fn select_with_decimal_limit_non_zero_no_right_hand_side() {
            let qstr = br#"SELECT id FROM a LIMIT 1."#;
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Double(Double {
                        value: 1.0,
                        precision: 0,
                    })),
                    offset: None,
                },
                ..Default::default()
            };

            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(res, expected);
        }

        #[test]
        fn select_with_decimal_limit_negative_zero() {
            let qstr = br#"SELECT id FROM a LIMIT -0.0"#;
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Double(Double {
                        value: -0.0,
                        precision: 1,
                    })),
                    offset: None,
                },
                ..Default::default()
            };

            let res = test_parse!(selection(Dialect::PostgreSQL), qstr);
            assert_eq!(res, expected);
        }

        #[test]
        fn select_with_decimal_limit_very_large_limit() {
            let qstr = "SELECT id FROM a LIMIT 3791947566539531989 OFFSET -0.0";
            let expected = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("a"))],
                fields: columns(&["id"]),
                limit_clause: LimitClause::LimitOffset {
                    limit: Some(Literal::Integer(3791947566539531989)),
                    offset: Some(Literal::Double(Double {
                        value: -0.0,
                        precision: 1,
                    })),
                },
                ..Default::default()
            };

            let res = test_parse!(selection(Dialect::PostgreSQL), qstr.as_bytes());
            assert_eq!(res, expected);
        }
    }
}
