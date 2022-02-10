use nom_sql::analysis::visit::{walk_expression, walk_join_clause, Visitor};
use nom_sql::{Expression, InValue, JoinClause, JoinRightSide, SqlQuery};
use noria::ReadySetError;
use noria_errors::{unsupported, ReadySetResult};

#[derive(Debug, PartialEq)]
pub enum SubqueryPosition<'a> {
    /// Subqueries on the right hand side of a join
    ///
    /// Invariant: This will always contain [`JoinRightSide::NestedSelect`]
    Join(&'a mut JoinRightSide),

    /// Subqueries on the right hand side of an IN
    ///
    /// Invariant: This will always contain [`InValue::Subquery`]
    In(&'a mut InValue),

    /// Subqueries in expressions.
    ///
    /// Invariant: This will always contain [`Expression::NestedSelect`]
    Expr(&'a mut Expression),
}

pub trait SubQueries {
    fn extract_subqueries(&mut self) -> ReadySetResult<Vec<SubqueryPosition>>;
}

#[derive(Default, Debug)]
struct ExtractSubqueriesVisitor<'ast> {
    out: Vec<SubqueryPosition<'ast>>,
}

impl<'ast> Visitor<'ast> for ExtractSubqueriesVisitor<'ast> {
    type Error = ReadySetError;

    fn visit_expression(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        match expression {
            Expression::Exists(_) => unsupported!("EXISTS not supported yet"),
            Expression::In {
                lhs,
                rhs: rhs @ InValue::Subquery(_),
                ..
            } => {
                walk_expression(self, lhs)?;
                self.out.push(SubqueryPosition::In(rhs))
            }
            Expression::NestedSelect(_) => self.out.push(SubqueryPosition::Expr(expression)),
            _ => walk_expression(self, expression)?,
        }
        Ok(())
    }

    fn visit_join_clause(&mut self, join: &'ast mut JoinClause) -> Result<(), Self::Error> {
        if matches!(join.right, JoinRightSide::NestedSelect(_, _)) {
            self.out.push(SubqueryPosition::Join(&mut join.right))
        } else {
            walk_join_clause(self, join)?;
        }
        Ok(())
    }
}

impl SubQueries for SqlQuery {
    fn extract_subqueries(&mut self) -> ReadySetResult<Vec<SubqueryPosition>> {
        let mut visitor = ExtractSubqueriesVisitor::default();
        if let SqlQuery::Select(stmt) = self {
            visitor.visit_select_statement(stmt)?;
        }

        Ok(visitor.out)
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{
        BinaryOperator, Column, FieldDefinitionExpression, SelectStatement, SqlQuery, Table,
    };

    use super::*;

    #[test]
    fn it_extracts_subqueries() {
        // select userid from role where type=1
        let sq = SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldDefinitionExpression::from(Column::from("userid"))],
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column(Column::from("type"))),
                rhs: Box::new(Expression::Literal(1.into())),
            }),
            ..Default::default()
        };

        let mut expected = InValue::Subquery(Box::new(sq));

        // select pid from post where author in (select userid from role where type=1)
        let st = SelectStatement {
            tables: vec![Table::from("post")],
            fields: vec![FieldDefinitionExpression::from(Column::from("pid"))],
            where_clause: Some(Expression::In {
                lhs: Box::new(Expression::Column(Column::from("author"))),
                rhs: expected.clone(),
                negated: false,
            }),
            ..Default::default()
        };

        let mut q = SqlQuery::Select(st);
        let res = q.extract_subqueries().unwrap();

        assert_eq!(res, vec![SubqueryPosition::In(&mut expected)]);
    }

    #[test]
    fn it_does_nothing_for_flat_queries() {
        // select userid from role where type=1
        let mut q = SqlQuery::Select(SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldDefinitionExpression::from(Column::from("userid"))],
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column(Column::from("type"))),
                rhs: Box::new(Expression::Literal(1.into())),
            }),
            ..Default::default()
        });

        let res = q.extract_subqueries().unwrap();
        let expected: Vec<SubqueryPosition> = Vec::new();

        assert_eq!(res, expected);
    }

    #[test]
    fn it_works_with_complex_queries() {
        // select users.name, articles.title, votes.uid \
        //          from articles, users, votes
        //          where users.id = articles.author \
        //          and votes.aid = articles.aid;

        let mut q = SqlQuery::Select(SelectStatement {
            tables: vec![
                Table::from("articles"),
                Table::from("users"),
                Table::from("votes"),
            ],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("users.name")),
                FieldDefinitionExpression::from(Column::from("articles.title")),
                FieldDefinitionExpression::from(Column::from("votes.uid")),
            ],
            where_clause: Some(Expression::BinaryOp {
                lhs: Box::new(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("users.id"))),
                    rhs: Box::new(Expression::Column(Column::from("articles.author"))),
                    op: BinaryOperator::Equal,
                }),
                rhs: Box::new(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("votes.aid"))),
                    rhs: Box::new(Expression::Column(Column::from("articles.aid"))),
                    op: BinaryOperator::Equal,
                }),
                op: BinaryOperator::And,
            }),
            ..Default::default()
        });

        let expected: Vec<SubqueryPosition> = Vec::new();

        let res = q.extract_subqueries().unwrap();

        assert_eq!(res, expected);
    }
}
