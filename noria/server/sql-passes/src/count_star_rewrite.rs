use std::collections::HashMap;

use nom_sql::analysis::visit::{walk_select_statement, Visitor};
use nom_sql::{Column, Expression, FunctionExpression, SelectStatement, SqlQuery, Table};
use noria_errors::{internal_err, ReadySetError, ReadySetResult};

#[derive(Debug)]
pub struct CountStarRewriteVisitor<'schema> {
    schemas: &'schema HashMap<String, Vec<String>>,
    tables: Option<Vec<Table>>,
}

impl<'ast, 'schema> Visitor<'ast> for CountStarRewriteVisitor<'schema> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.tables = Some(select_statement.tables.clone());
        walk_select_statement(self, select_statement)?;
        Ok(())
    }

    fn visit_function_expression(
        &mut self,
        function_expression: &'ast mut FunctionExpression,
    ) -> Result<(), Self::Error> {
        if *function_expression == FunctionExpression::CountStar {
            let bogo_table = self
                .tables
                .as_ref()
                .and_then(|ts| ts.first())
                .cloned()
                .ok_or_else(|| internal_err("Tables should be set first"))?;

            #[allow(clippy::unwrap_used)]
            // We've already checked that all the tables referenced in the query exist
            let mut schema_iter = self.schemas.get(&bogo_table.name).unwrap().iter();
            // The columns in the write_schemas map are actually columns as seen from the
            // current mir node. In this case, we've already passed star expansion, which
            // means the list of columns in the passed in write_schemas map contains all
            // columns for the table in question. This means that we are garaunteed to have
            // at least one result in this columns list, and can simply choose the first
            // column.
            let bogo_column = schema_iter.next().unwrap();

            *function_expression = FunctionExpression::Count {
                expr: Box::new(Expression::Column(Column {
                    name: bogo_column.clone(),
                    table: Some(bogo_table.name.clone()),
                    function: None,
                })),
                distinct: false,
                count_nulls: true,
            };
        }

        Ok(())
    }
}

pub trait CountStarRewrite: Sized {
    fn rewrite_count_star(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<Self>;
}

impl CountStarRewrite for SqlQuery {
    fn rewrite_count_star(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery> {
        match self {
            SqlQuery::Select(mut sq) => {
                let mut visitor = CountStarRewriteVisitor {
                    schemas: write_schemas,
                    tables: None,
                };

                visitor.visit_select_statement(&mut sq)?;
                Ok(SqlQuery::Select(sq))
            }
            // nothing to do for other query types, as they cannot have aliases
            x => Ok(x),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nom_sql::parser::parse_query;
    use nom_sql::{
        BinaryOperator, Column, Dialect, FieldDefinitionExpression, FunctionExpression, Literal,
        SqlQuery,
    };

    use super::*;

    #[test]
    fn it_expands_count_star() {
        // SELECT COUNT(*) FROM users;
        // -->
        // SELECT COUNT(users.id) FROM users;
        let q = parse_query(Dialect::MySQL, "SELECT COUNT(*) FROM users;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema).unwrap();
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(Expression::Call(
                        FunctionExpression::Count {
                            expr: Box::new(Expression::Column(Column::from("users.id"))),
                            distinct: false,
                            count_nulls: true,
                        }
                    ))]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_count_star_with_group_by() {
        // SELECT COUNT(*) FROM users GROUP BY id;
        // -->
        // SELECT COUNT(users.name) FROM users GROUP BY id;
        let q = parse_query(Dialect::MySQL, "SELECT COUNT(*) FROM users GROUP BY id;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema).unwrap();
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(Expression::Call(
                        FunctionExpression::Count {
                            expr: Box::new(Expression::Column(Column::from("users.id"))),
                            distinct: false,
                            count_nulls: true,
                        }
                    ))]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn nested_in_expression() {
        let q = parse_query(Dialect::MySQL, "SELECT COUNT(*) + 1 FROM users;").unwrap();
        let schema = HashMap::from([(
            "users".to_owned(),
            vec!["id".to_owned(), "name".to_owned(), "age".to_owned()],
        )]);

        let res = q.rewrite_count_star(&schema).unwrap();
        match res {
            SqlQuery::Select(stmt) => {
                assert_eq!(
                    stmt.fields,
                    vec![FieldDefinitionExpression::from(Expression::BinaryOp {
                        lhs: Box::new(Expression::Call(FunctionExpression::Count {
                            expr: Box::new(Expression::Column("users.id".into())),
                            distinct: false,
                            count_nulls: true,
                        })),
                        op: BinaryOperator::Add,
                        rhs: Box::new(Expression::Literal(Literal::Integer(1)))
                    })]
                );
            }
            _ => panic!(),
        }
    }
}
