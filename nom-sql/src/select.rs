use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::error::ErrorKind;
use nom::multi::{many0, separated_list1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{
    as_alias, field_definition_expr, field_list, field_reference_list, literal,
    schema_table_reference, table_list, terminated_with_statement_terminator, ws_sep_comma,
    FieldDefinitionExpression,
};
use crate::expression::expression;
use crate::join::{join_operator, JoinConstraint, JoinOperator, JoinRightSide};
use crate::order::{order_clause, OrderClause};
use crate::table::Table;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Expression, FieldReference, FunctionExpression, Literal, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupByClause {
    pub fields: Vec<FieldReference>,
}

impl fmt::Display for GroupByClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GROUP BY ")?;
        write!(
            f,
            "{}",
            self.fields
                .iter()
                .map(|c| format!("{}", c))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub operator: JoinOperator,
    pub right: JoinRightSide,
    pub constraint: JoinConstraint,
}

impl fmt::Display for JoinClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.operator)?;
        write!(f, " {}", self.right)?;
        write!(f, " {}", self.constraint)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    pub limit: Literal,
    pub offset: Option<Literal>,
}

impl fmt::Display for LimitClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LIMIT {}", self.limit)?;
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {}", offset)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CommonTableExpression {
    pub name: SqlIdentifier,
    pub statement: SelectStatement,
}

impl fmt::Display for CommonTableExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` AS ({})", self.name, self.statement)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    pub ctes: Vec<CommonTableExpression>,
    pub tables: Vec<Table>,
    pub distinct: bool,
    pub fields: Vec<FieldDefinitionExpression>,
    pub join: Vec<JoinClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<Expression>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

impl SelectStatement {
    pub fn contains_aggregate_select(&self) -> bool {
        self.fields.iter().any(|e| match e {
            FieldDefinitionExpression::Expression { expr, .. } => match expr {
                Expression::Call(func) => matches!(
                    func,
                    FunctionExpression::Avg { .. }
                        | FunctionExpression::Count { .. }
                        | FunctionExpression::CountStar
                        | FunctionExpression::Sum { .. }
                        | FunctionExpression::Max(_)
                        | FunctionExpression::Min(_)
                        | FunctionExpression::GroupConcat { .. }
                ),
                Expression::NestedSelect(select) => select.contains_aggregate_select(),
                _ => false,
            },
            _ => false,
        })
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.ctes.is_empty() {
            write!(f, "WITH {} ", self.ctes.iter().join(", "))?;
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
                .map(|field| format!("{}", field))
                .collect::<Vec<_>>()
                .join(", ")
        )?;

        if !self.tables.is_empty() {
            write!(f, " FROM ")?;
            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|table| format!("{}", table))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        for jc in &self.join {
            write!(f, " {}", jc)?;
        }
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE ")?;
            write!(f, "{}", where_clause)?;
        }
        if let Some(ref group_by) = self.group_by {
            write!(f, " {}", group_by)?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING {}", having)?;
        }
        if let Some(ref order) = self.order {
            write!(f, " {}", order)?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " {}", limit)?;
        }
        Ok(())
    }
}

fn having_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expression> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("having")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, expr) = expression(dialect)(i)?;

        Ok((i, expr))
    }
}

// Parse GROUP BY clause
pub fn group_by_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], GroupByClause> {
    move |i| -> Result<(&[u8], GroupByClause), _> {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("group")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("by")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, fields) = field_reference_list(dialect)(i)?;
        Ok((i, GroupByClause { fields }))
    }
}

fn offset_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("offset")(i)?;
        let (i, _) = whitespace1(i)?;
        literal(dialect)(i)
    }
}

// Parse LIMIT clause
pub fn limit_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], LimitClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("limit")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, limit) = literal(dialect)(i)?;
        let (i, offset) = opt(offset_clause(dialect))(i)?;

        Ok((i, LimitClause { limit, offset }))
    }
}

fn join_constraint(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], JoinConstraint> {
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
fn join_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], JoinClause> {
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

fn join_rhs(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], JoinRightSide> {
    move |i| {
        let nested_select = map(
            tuple((
                delimited(
                    terminated(tag("("), whitespace0),
                    nested_selection(dialect),
                    preceded(whitespace0, tag(")")),
                ),
                as_alias(dialect),
            )),
            |(statement, alias)| JoinRightSide::NestedSelect(Box::new(statement), alias),
        );
        let table = map(schema_table_reference(dialect), JoinRightSide::Table);
        let tables = map(
            delimited(tag("("), table_list(dialect), tag(")")),
            JoinRightSide::Tables,
        );
        alt((nested_select, table, tables))(i)
    }
}

// Parse WHERE clause of a selection
pub fn where_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expression> {
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
pub fn selection(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SelectStatement> {
    move |i| terminated_with_statement_terminator(nested_selection(dialect))(i)
}

/// The semantics of SQL natively represent the FROM clause of a query as a fully nested AST of join
/// clauses, but our AST has distinct fields for the tables and a list of joins. To be able to parse
/// parenthesized join clauses with explicit precedence such as `FROM ((t1 JOIN t2) JOIN t3)`, we
/// first parse to a tree then convert to the latter representation afterwards.
#[derive(Debug)]
enum FromClause {
    Tables(Vec<Table>),
    NestedSelect(Box<SelectStatement>, Option<SqlIdentifier>),
    Join {
        lhs: Box<FromClause>,
        join_clause: JoinClause,
    },
}

impl FromClause {
    fn into_tables_and_joins(self) -> Result<(Vec<Table>, Vec<JoinClause>), String> {
        use FromClause::*;

        // The current representation means that a nested select on the lhs of a join is never
        // allowed
        let lhs_nested_select = Err("SELECT from subqueries is not currently supported".to_owned());

        match self {
            Tables(tables) => Ok((tables, vec![])),
            NestedSelect(_, _) => lhs_nested_select,
            Join {
                mut lhs,
                join_clause,
            } => {
                let mut joins = vec![join_clause];
                let tables = loop {
                    match *lhs {
                        Tables(tables) => break tables,
                        NestedSelect(_, _) => return lhs_nested_select,
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

fn nested_select(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FromClause> {
    move |i| {
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, selection) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, FromClause::NestedSelect(Box::new(selection), alias)))
    }
}

fn from_clause_join(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FromClause> {
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

fn nested_from_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FromClause> {
    move |i| {
        alt((
            delimited(tag("("), from_clause_join(dialect), tag(")")),
            map(table_list(dialect), FromClause::Tables),
            nested_select(dialect),
        ))(i)
    }
}

fn from_clause_tree(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FromClause> {
    move |i| {
        alt((
            delimited(tag("("), from_clause_join(dialect), tag(")")),
            from_clause_join(dialect),
            map(table_list(dialect), FromClause::Tables),
            nested_select(dialect),
        ))(i)
    }
}

fn from_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FromClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("from")(i)?;
        let (i, _) = whitespace1(i)?;
        from_clause_tree(dialect)(i)
    }
}

fn cte(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CommonTableExpression> {
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

        Ok((i, CommonTableExpression { name, statement }))
    }
}

fn ctes(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<CommonTableExpression>> {
    move |i| {
        let (i, _) = tag_no_case("with")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, ctes) = separated_list1(ws_sep_comma, cte(dialect))(i)?;
        let (i, _) = whitespace0(i)?;

        Ok((i, ctes))
    }
}

pub fn nested_selection(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SelectStatement> {
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
            let (i, limit) = opt(limit_clause(dialect))(i)?;

            Ok((
                i,
                (
                    from,
                    extra_joins,
                    where_clause,
                    having,
                    group_by,
                    order,
                    limit,
                ),
            ))
        })(i)?;

        let mut result = SelectStatement {
            ctes: ctes.unwrap_or_default(),
            distinct: distinct.is_some(),
            fields,
            ..Default::default()
        };

        if let Some((from, extra_joins, where_clause, having, group_by, order, limit)) = from_clause
        {
            let (tables, mut join) = from
                .into_tables_and_joins()
                .map_err(|_| nom::Err::Error(nom::error::Error::new(i, ErrorKind::Tag)))?;

            join.extend(extra_joins);

            result.tables = tables;
            result.join = join;
            result.where_clause = where_clause;
            result.group_by = group_by;
            result.having = having;
            result.order = order;
            result.limit = limit;
        }

        Ok((i, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::common::{FieldDefinitionExpression, ItemPlaceholder, Literal, SqlType};
    use crate::table::Table;
    use crate::{BinaryOperator, Expression, FunctionExpression, InValue};

    fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpression> {
        cols.iter()
            .map(|c| FieldDefinitionExpression::from(Column::from(*c)))
            .collect()
    }

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: columns(&["id", "name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_without_table() {
        let qstring = "SELECT * FROM;";
        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert!(res.is_err(), "!{:?}.is_err()", res);
    }

    #[test]
    fn bare_expression_select() {
        let qstring = "SELECT 1";
        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                fields: vec![FieldDefinitionExpression::Expression {
                    expr: Expression::Literal(Literal::Integer(1)),
                    alias: None
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn more_involved_select() {
        let qstring = "SELECT users.id, users.name FROM users;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: columns(&["users.id", "users.name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all() {
        let qstring = "SELECT * FROM users;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: vec![FieldDefinitionExpression::All],
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all_in_table() {
        let qstring = "SELECT users.* FROM users, votes;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users"), Table::from("votes")],
                fields: vec![FieldDefinitionExpression::AllInTable("users".into())],
                ..Default::default()
            }
        );
    }

    #[test]
    fn spaces_optional() {
        let qstring = "SELECT id,name FROM users;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
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
            selection(Dialect::MySQL)(qstring_lc.as_bytes()).unwrap(),
            selection(Dialect::MySQL)(qstring_uc.as_bytes()).unwrap()
        );
    }

    #[test]
    fn termination() {
        let qstring_sem = "select id, name from users;";
        let qstring_nosem = "select id, name from users";
        let qstring_linebreak = "select id, name from users\n";

        let r1 = selection(Dialect::MySQL)(qstring_sem.as_bytes()).unwrap();
        let r2 = selection(Dialect::MySQL)(qstring_nosem.as_bytes()).unwrap();
        let r3 = selection(Dialect::MySQL)(qstring_linebreak.as_bytes()).unwrap();
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
        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(Expression::Column("email".into())),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(literal)),
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("ContactInfo")],
                fields: vec![FieldDefinitionExpression::All],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn limit_clause() {
        let qstring1 = "select * from users limit 10\n";
        let qstring2 = "select * from users limit 10 offset 10\n";

        let expected_lim1 = LimitClause {
            limit: 10.into(),
            offset: None,
        };
        let expected_lim2 = LimitClause {
            limit: 10.into(),
            offset: Some(10.into()),
        };

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        let res2 = test_parse!(selection(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(res1.limit, Some(expected_lim1));
        assert_eq!(res2.limit, Some(expected_lim2));
    }

    #[test]
    fn table_alias() {
        let qstring1 = "select * from PaperTag as t;";
        // let qstring2 = "select * from PaperTag t;";

        let res1 = test_parse!(selection(Dialect::MySQL), qstring1.as_bytes());
        assert_eq!(
            res1,
            SelectStatement {
                tables: vec![Table {
                    name: "PaperTag".into(),
                    alias: Some("t".into()),
                    schema: None,
                },],
                fields: vec![FieldDefinitionExpression::All],
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
                tables: vec![Table {
                    name: "PaperTag".into(),
                    alias: Some("t".into()),
                    schema: Some("db1".into()),
                },],
                fields: vec![FieldDefinitionExpression::All],
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
                tables: vec![Table::from("PaperTag")],
                fields: vec![FieldDefinitionExpression::Expression {
                    alias: Some("TagName".into()),
                    expr: Expression::Column(Column::from("name"))
                }],
                ..Default::default()
            }
        );
        let res2 = test_parse!(selection(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(
            res2,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                fields: vec![FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column::from("PaperTag.name")),
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
                tables: vec![Table::from("PaperTag")],
                fields: vec![FieldDefinitionExpression::Expression {
                    alias: Some("TagName".into()),
                    expr: Expression::Column(Column {
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
                tables: vec![Table::from("PaperTag")],
                fields: vec![FieldDefinitionExpression::Expression {
                    alias: Some("TagName".into()),
                    expr: Expression::Column(Column::from("PaperTag.name"))
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn distinct() {
        let qstring = "select distinct tag from PaperTag where paperId=?;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(Expression::Column("paperId".into())),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
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

        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let left_comp = Box::new(Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("paperId"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        let right_comp = Box::new(Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("paperStorageId"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
        });
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: left_comp,
            op: BinaryOperator::And,
            rhs: right_comp,
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperStorage")],
                fields: columns(&["infoJson"]),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn where_and_limit_clauses() {
        let qstring = "select * from users where id = ? limit 10\n";
        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let expected_lim = Some(LimitClause {
            limit: 10.into(),
            offset: None,
        });
        let ct = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("id"))),
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        };
        let expected_where_cond = Some(ct);

        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: vec![FieldDefinitionExpression::All],
                where_clause: expected_where_cond,
                limit: expected_lim,
                ..Default::default()
            }
        );
    }

    #[test]
    fn limit_placeholder() {
        let res = test_parse!(selection(Dialect::MySQL), b"select * from users limit ?");
        assert_eq!(
            res.limit.unwrap().limit,
            Literal::Placeholder(ItemPlaceholder::QuestionMark)
        )
    }

    #[test]
    fn aggregation_column() {
        let qstring = "SELECT max(addr_id) FROM address;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let agg_expr =
            FunctionExpression::Max(Box::new(Expression::Column(Column::from("addr_id"))));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("address")],
                fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr),),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn aggregation_column_with_alias() {
        let qstring = "SELECT max(addr_id) AS max_addr FROM address;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let agg_expr =
            FunctionExpression::Max(Box::new(Expression::Column(Column::from("addr_id"))));
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("address")],
            fields: vec![FieldDefinitionExpression::Expression {
                alias: Some("max_addr".into()),
                expr: Expression::Call(agg_expr),
            }],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_all() {
        let qstring = "SELECT COUNT(*) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let agg_expr = FunctionExpression::CountStar;
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("aid"),
                ))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_distinct() {
        let qstring = "SELECT COUNT(DISTINCT vote_id) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let agg_expr = FunctionExpression::Count {
            expr: Box::new(Expression::Column(Column::from("vote_id"))),
            distinct: true,
            count_nulls: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("aid"),
                ))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_filter() {
        let qstring =
            "SELECT COUNT(CASE WHEN vote_id > 10 THEN vote_id END) FROM votes GROUP BY aid;";
        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let filter_cond = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("vote_id"))),
            op: BinaryOperator::Greater,
            rhs: Box::new(Expression::Literal(Literal::Integer(10.into()))),
        };
        let agg_expr = FunctionExpression::Count {
            expr: Box::new(Expression::CaseWhen {
                then_expr: Box::new(Expression::Column(Column::from("vote_id"))),
                else_expr: None,
                condition: Box::new(filter_cond),
            }),
            distinct: false,
            count_nulls: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("aid"),
                ))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn sum_filter() {
        let qstring = "SELECT SUM(CASE WHEN sign = 1 THEN vote_id END) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let filter_cond = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("sign"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(Literal::Integer(1.into()))),
        };
        let agg_expr = FunctionExpression::Sum {
            expr: Box::new(Expression::CaseWhen {
                then_expr: Box::new(Expression::Column(Column::from("vote_id"))),
                else_expr: None,
                condition: Box::new(filter_cond),
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("aid"),
                ))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn sum_filter_else_literal() {
        let qstring =
            "SELECT SUM(CASE WHEN sign = 1 THEN vote_id ELSE 6 END) FROM votes GROUP BY aid;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let filter_cond = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("sign"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Literal(Literal::Integer(1.into()))),
        };
        let agg_expr = FunctionExpression::Sum {
            expr: Box::new(Expression::CaseWhen {
                then_expr: Box::new(Expression::Column(Column::from("vote_id"))),
                else_expr: Some(Box::new(Expression::Literal(Literal::Integer(6)))),
                condition: Box::new(filter_cond),
            }),
            distinct: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("aid"),
                ))],
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

        let res = selection(Dialect::MySQL)(qstring.as_bytes());

        let filter_cond = Expression::BinaryOp {
            lhs: Box::new(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("votes.story_id"))),
                op: BinaryOperator::Is,
                rhs: Box::new(Expression::Literal(Literal::Null)),
            }),
            rhs: Box::new(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("votes.vote"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Literal(Literal::Integer(0))),
            }),
            op: BinaryOperator::And,
        };
        let agg_expr = FunctionExpression::Count {
            expr: Box::new(Expression::CaseWhen {
                then_expr: Box::new(Expression::Column(Column::from("votes.vote"))),
                else_expr: None,
                condition: Box::new(filter_cond),
            }),
            distinct: false,
            count_nulls: false,
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![FieldDefinitionExpression::Expression {
                alias: Some("votes".into()),
                expr: Expression::Call(agg_expr),
            }],
            group_by: Some(GroupByClause {
                fields: vec![FieldReference::Expression(Expression::Column(
                    Column::from("votes.comment_id"),
                ))],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn generic_function_query() {
        let qstring = "SELECT coalesce(a, b,c) as x,d FROM sometable;";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let agg_expr = FunctionExpression::Call {
            name: "coalesce".to_owned(),
            arguments: vec![
                Expression::Column(Column {
                    name: "a".into(),
                    table: None,
                }),
                Expression::Column(Column {
                    name: "b".into(),
                    table: None,
                }),
                Expression::Column(Column {
                    name: "c".into(),
                    table: None,
                }),
            ],
        };
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("sometable")],
            fields: vec![
                FieldDefinitionExpression::Expression {
                    alias: Some("x".into()),
                    expr: Expression::Call(agg_expr),
                },
                FieldDefinitionExpression::from(Column {
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

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("item.i_a_id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Column(Column::from("author.a_id"))),
            }),
            rhs: Box::new(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("item.i_subject"))),
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            }),
            op: BinaryOperator::And,
        });
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("item"), Table::from("author")],
                fields: vec![FieldDefinitionExpression::All],
                where_clause: expected_where_cond,
                order: Some(OrderClause {
                    order_by: vec![(
                        FieldReference::Expression(Expression::Column("item.i_title".into())),
                        None
                    )],
                }),
                limit: Some(LimitClause {
                    limit: 50.into(),
                    offset: None,
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_joins() {
        let qstring = "select paperId from PaperConflict join PCMember using (contactId);";

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("PaperConflict")],
            fields: columns(&["paperId"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::Table(Table::from("PCMember")),
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

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let expected = SelectStatement {
            tables: vec![Table::from("PCMember")],
            fields: columns(&["PCMember.contactId"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::Table(Table::from("PaperReview")),
                constraint: JoinConstraint::On(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("PCMember.contactId"))),
                    rhs: Box::new(Expression::Column(Column::from("PaperReview.contactId"))),
                    op: BinaryOperator::Equal,
                }),
            }],
            order: Some(OrderClause {
                order_by: vec![(
                    FieldReference::Expression(Expression::Column("contactId".into())),
                    None,
                )],
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

        let res = selection(Dialect::MySQL)(qstring.as_bytes());
        let ct = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("ContactInfo.contactId"))),
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        };
        let expected_where_cond = Some(ct);
        let mkjoin = |tbl: &str, col: &str| -> JoinClause {
            JoinClause {
                operator: JoinOperator::LeftJoin,
                right: JoinRightSide::Table(Table::from(tbl)),
                constraint: JoinConstraint::Using(vec![Column::from(col)]),
            }
        };
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("ContactInfo")],
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

        let res = selection(Dialect::MySQL)(qstr.as_bytes());
        let inner_where_clause = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Column(Column::from("order_line.ol_o_id"))),
        };

        let inner_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&["o_c_id"]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = Expression::In {
            lhs: Box::new(Expression::Column(Column::from("orders.o_c_id"))),
            rhs: InValue::Subquery(Box::new(inner_select)),
            negated: false,
        };

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
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

        let res = selection(Dialect::MySQL)(qstr.as_bytes());

        let agg_expr = FunctionExpression::Max(Box::new(Expression::Column(Column::from("o_id"))));
        let recursive_select = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: vec![FieldDefinitionExpression::from(Expression::Call(agg_expr))],
            ..Default::default()
        };

        let cop1 = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Column(Column::from("order_line.ol_o_id"))),
        };

        let cop2 = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("orders.o_id"))),
            op: BinaryOperator::Greater,
            rhs: Box::new(Expression::NestedSelect(Box::new(recursive_select))),
        };

        let inner_where_clause = Expression::BinaryOp {
            lhs: Box::new(cop1),
            op: BinaryOperator::And,
            rhs: Box::new(cop2),
        };

        let inner_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&["o_c_id"]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = Expression::In {
            lhs: Box::new(Expression::Column(Column::from("orders.o_c_id"))),
            rhs: InValue::Subquery(Box::new(inner_select)),
            negated: false,
        };

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&["ol_i_id"]),
            where_clause: Some(outer_where_clause),
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }

    #[test]
    fn join_against_nested_select() {
        let t1 = b"(SELECT ol_i_id FROM order_line) AS ids";

        assert!(join_rhs(Dialect::MySQL)(t1).is_ok());

        let t1 = b"JOIN (SELECT ol_i_id FROM order_line) AS ids ON (orders.o_id = ids.ol_i_id)";

        assert!(join_clause(Dialect::MySQL)(t1).is_ok());

        let qstr_with_alias = "SELECT o_id, ol_i_id FROM orders JOIN \
                               (SELECT ol_i_id FROM order_line) AS ids \
                               ON (orders.o_id = ids.ol_i_id);";
        let res = selection(Dialect::MySQL)(qstr_with_alias.as_bytes());

        // N.B.: Don't alias the inner select to `inner`, which is, well, a SQL keyword!
        let inner_select = SelectStatement {
            tables: vec![Table::from("order_line")],
            fields: columns(&["ol_i_id"]),
            ..Default::default()
        };

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: columns(&["o_id", "ol_i_id"]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::NestedSelect(Box::new(inner_select), "ids".into()),
                constraint: JoinConstraint::On(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("orders.o_id"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expression::Column(Column::from("ids.ol_i_id"))),
                }),
            }],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }

    #[test]
    fn project_arithmetic_expressions() {
        let qstr = "SELECT MAX(o_id)-3333 FROM orders;";
        let res = selection(Dialect::MySQL)(qstr.as_bytes());

        let expected = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: vec![FieldDefinitionExpression::from(Expression::BinaryOp {
                lhs: Box::new(Expression::Call(FunctionExpression::Max(Box::new(
                    Expression::Column("o_id".into()),
                )))),
                op: BinaryOperator::Subtract,
                rhs: Box::new(Expression::Literal(3333.into())),
            })],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn project_arithmetic_expressions_with_aliases() {
        let qstr = "SELECT max(o_id) * 2 as double_max FROM orders;";
        let res = selection(Dialect::MySQL)(qstr.as_bytes());

        let expected = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: vec![FieldDefinitionExpression::Expression {
                alias: Some("double_max".into()),
                expr: Expression::BinaryOp {
                    lhs: Box::new(Expression::Call(FunctionExpression::Max(Box::new(
                        Expression::Column("o_id".into()),
                    )))),
                    op: BinaryOperator::Multiply,
                    rhs: Box::new(Expression::Literal(2.into())),
                },
            }],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn where_call_in_list() {
        let qstr = b"SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)";
        let res = selection(Dialect::MySQL)(qstr);
        let (rem, res) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            res.where_clause,
            Some(Expression::In {
                lhs: Box::new(Expression::Call(FunctionExpression::Avg {
                    expr: Box::new(Expression::Column("y".into())),
                    distinct: false
                })),
                rhs: InValue::List(vec![
                    Expression::Literal(Literal::Placeholder(
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
        let res = selection(Dialect::MySQL)(qstr.as_bytes());
        assert!(res.is_ok(), "!{:?}.is_ok()", res);
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec!["users".into()],
                fields: vec![
                    FieldDefinitionExpression::from(Column::from("id")),
                    FieldDefinitionExpression::Expression {
                        alias: Some("created_day".into()),
                        expr: Expression::Cast {
                            expr: Box::new(Expression::Column(Column::from("created_at"))),
                            ty: SqlType::Date,
                            postgres_style: false,
                        },
                    },
                ],
                where_clause: Some(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("id"))),
                    rhs: Box::new(Expression::Literal(Literal::Placeholder(
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
        let res = selection(Dialect::MySQL)(qstr);
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
        let res = selection(Dialect::MySQL)(qstr);
        assert!(res.is_ok(), "error parsing query: {}", res.err().unwrap());
        let (rem, query) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(query.ctes.len(), 2);
        assert_eq!(query.ctes[0].name, "max_val");
        assert_eq!(query.ctes[1].name, "min_val");
    }

    #[test]
    fn format_ctes() {
        let query = SelectStatement {
            ctes: vec![CommonTableExpression {
                name: "foo".into(),
                statement: SelectStatement {
                    fields: vec![FieldDefinitionExpression::Expression {
                        expr: Expression::Column("x".into()),
                        alias: None,
                    }],
                    tables: vec!["t".into()],
                    ..Default::default()
                },
            }],
            fields: vec![FieldDefinitionExpression::Expression {
                expr: Expression::Column("x".into()),
                alias: None,
            }],
            tables: vec!["foo".into()],
            ..Default::default()
        };
        let res = query.to_string();
        assert_eq!(
            res,
            "WITH `foo` AS (SELECT `x` FROM `t`) SELECT `x` FROM `foo`"
        );
    }

    #[test]
    fn bare_having() {
        let res = test_parse!(
            selection(Dialect::MySQL),
            b"select x, count(*) from t having count(*) > 1"
        );
        assert_eq!(
            res.having,
            Some(Expression::BinaryOp {
                lhs: Box::new(Expression::Call(FunctionExpression::CountStar)),
                op: BinaryOperator::Greater,
                rhs: Box::new(Expression::Literal(1.into()))
            })
        );
        let stringified = res.to_string();
        assert_eq!(
            stringified,
            "SELECT `x`, count(*) FROM `t` HAVING (count(*) > 1)"
        );
    }

    mod mysql {
        use super::*;
        use crate::column::Column;
        use crate::common::{FieldDefinitionExpression, Literal};
        use crate::table::Table;
        use crate::{BinaryOperator, Expression, FunctionExpression, InValue};

        #[test]
        fn alias_generic_function() {
            let qstr = "SELECT id, coalesce(a, \"b\",c) AS created_day FROM users;";
            let res = selection(Dialect::MySQL)(qstr.as_bytes());
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec!["users".into()],
                    fields: vec![
                        FieldDefinitionExpression::from(Column::from("id")),
                        FieldDefinitionExpression::Expression {
                            alias: Some("created_day".into()),
                            expr: Expression::Call(FunctionExpression::Call {
                                name: "coalesce".to_owned(),
                                arguments: vec![
                                    Expression::Column(Column::from("a")),
                                    Expression::Literal(Literal::String("b".to_owned())),
                                    Expression::Column(Column::from("c"))
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
            use crate::common::Literal;

            let qstring = "SELECT NULL, 1, \"foo\", CURRENT_TIME FROM users;";
            // TODO: doesn't support selecting literals without a FROM clause, which is still valid
            // SQL        let qstring = "SELECT NULL, 1, \"foo\";";

            let res = selection(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![Table::from("users")],
                    fields: vec![
                        FieldDefinitionExpression::from(Expression::Literal(Literal::Null,)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(1),)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::String(
                            "foo".to_owned()
                        ),)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::CurrentTime,)),
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
            let res = selection(Dialect::MySQL)(qstr.as_bytes());

            let expected_where_clause = Some(Expression::In {
                lhs: Box::new(Expression::Column(Column::from(
                    "auth_permission.content_type_id",
                ))),
                rhs: InValue::List(vec![Expression::Literal(0.into())]),
                negated: false,
            });

            let expected = SelectStatement {
                tables: vec![Table::from("auth_permission")],
                fields: vec![
                    FieldDefinitionExpression::from(Column::from(
                        "auth_permission.content_type_id",
                    )),
                    FieldDefinitionExpression::from(Column::from("auth_permission.codename")),
                ],
                join: vec![JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(Table::from("django_content_type")),
                    constraint: JoinConstraint::On(Expression::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expression::Column(Column::from(
                            "auth_permission.content_type_id",
                        ))),
                        rhs: Box::new(Expression::Column(Column::from("django_content_type.id"))),
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
                    order_by: vec![(FieldReference::Numeric(1), None)]
                })
            )
        }
    }

    mod postgres {
        use super::*;
        use crate::column::Column;
        use crate::common::{FieldDefinitionExpression, Literal};
        use crate::table::Table;
        use crate::{BinaryOperator, Expression, FunctionExpression, InValue};

        #[test]
        fn alias_generic_function() {
            let qstr = "SELECT id, coalesce(a, 'b',c) AS created_day FROM users;";
            let res = selection(Dialect::PostgreSQL)(qstr.as_bytes());
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec!["users".into()],
                    fields: vec![
                        FieldDefinitionExpression::from(Column::from("id")),
                        FieldDefinitionExpression::Expression {
                            alias: Some("created_day".into()),
                            expr: Expression::Call(FunctionExpression::Call {
                                name: "coalesce".to_owned(),
                                arguments: vec![
                                    Expression::Column(Column::from("a")),
                                    Expression::Literal(Literal::String("b".to_owned())),
                                    Expression::Column(Column::from("c"))
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
            use crate::common::Literal;

            let qstring = "SELECT NULL, 1, 'foo', CURRENT_TIME FROM users;";
            // TODO: doesn't support selecting literals without a FROM clause, which is still valid
            // SQL        let qstring = "SELECT NULL, 1, \"foo\";";

            let res = selection(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                SelectStatement {
                    tables: vec![Table::from("users")],
                    fields: vec![
                        FieldDefinitionExpression::from(Expression::Literal(Literal::Null,)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(1),)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::String(
                            "foo".to_owned()
                        ),)),
                        FieldDefinitionExpression::from(Expression::Literal(Literal::CurrentTime,)),
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
            let res = selection(Dialect::PostgreSQL)(qstr.as_bytes());

            let expected_where_clause = Some(Expression::In {
                lhs: Box::new(Expression::Column(Column::from(
                    "auth_permission.content_type_id",
                ))),
                rhs: InValue::List(vec![Expression::Literal(0.into())]),
                negated: false,
            });

            let expected = SelectStatement {
                tables: vec![Table::from("auth_permission")],
                fields: vec![
                    FieldDefinitionExpression::from(Column::from(
                        "auth_permission.content_type_id",
                    )),
                    FieldDefinitionExpression::from(Column::from("auth_permission.codename")),
                ],
                join: vec![JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(Table::from("django_content_type")),
                    constraint: JoinConstraint::On(Expression::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expression::Column(Column::from(
                            "auth_permission.content_type_id",
                        ))),
                        rhs: Box::new(Expression::Column(Column::from("django_content_type.id"))),
                    }),
                }],
                where_clause: expected_where_clause,
                ..Default::default()
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn flarum_select_roundtrip_1() {
            let qstr = "select exists(select * from `groups` where `id` = ?) as `exists`";
            let res = test_parse!(selection(Dialect::MySQL), qstr.as_bytes());
            let qstr = res.to_string();
            println!("{}", qstr);
            assert_eq!(
                qstr,
                "SELECT EXISTS (SELECT * FROM `groups` WHERE (`id` = ?)) AS `exists`"
            );
        }
    }
}
