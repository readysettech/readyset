//! AST walker for SQL, inspired by [rustc's AST visitor][rustc-ast-visit].
//!
//! [rustc-ast-visit]: https://doc.rust-lang.org/stable/nightly-rustc/rustc_ast/visit/index.html
#![warn(clippy::todo, clippy::unimplemented)]

use crate::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, FunctionExpression,
    GroupByClause, InValue, JoinClause, JoinRightSide, LimitClause, Literal, OrderClause,
    SelectStatement, SqlType, Table,
};

/// Each method of the `Visitor` trait is a hook to be potentially overridden when recursively
/// traversing SQL statements. The default implementation of each method recursively visits the
/// substructure of the input via the corresponding `walk` method, eg `visit_expression` by default
/// calls `visit::walk_expression`. This allows defining algorithms that depend on recursively
/// traversing ASTs without having to reimplement AST traversal every time.
///
/// Currently only partially implemented for the AST rooted at [`SelectStatement`] - in the future,
/// we should support everything that can go into a [`nom_sql::SqlQuery`].
///
/// # Examples
///
/// The following implements a Visitor that counts all occurrences of placeholder literals in a
/// statement.
///
/// ```
/// #![feature(never_type)]
///
/// use nom_sql::{parse_query, Literal, SqlQuery, Dialect};
/// use nom_sql::analysis::visit::Visitor;
///
/// fn count_placeholders(query: &str) -> usize {
///     #[derive(Default)]
///     struct PlaceholderCounter(usize);
///
///     impl<'ast> Visitor<'ast> for PlaceholderCounter {
///         type Error = !;
///
///         fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
///             if matches!(literal, Literal::Placeholder(_)) {
///                 self.0 += 1;
///             }
///             Ok(())
///        }
///     }
///
///     let mut query = match parse_query(Dialect::MySQL, query).unwrap() {
///         SqlQuery::Select(query) => query,
///         _ => panic!("unexpected query type"),
///     };
///
///     let mut counter = PlaceholderCounter::default();
///     counter.visit_select_statement(&mut query).unwrap();
///     counter.0
/// }
///
///
/// assert_eq!(count_placeholders("SELECT id FROM users WHERE name = ? AND age = ?"), 2);
/// ```
pub trait Visitor<'ast>: Sized {
    /// Errors that can be thrown during execution of this visitor
    type Error;

    fn visit_column(&mut self, _column: &'ast mut Column) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_table(&mut self, _table: &'ast mut Table) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_literal(&mut self, _literal: &'ast mut Literal) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_sql_type(&mut self, _sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_function_expression(
        &mut self,
        function_expression: &'ast mut FunctionExpression,
    ) -> Result<(), Self::Error> {
        walk_function_expression(self, function_expression)
    }

    fn visit_in_value(&mut self, in_value: &'ast mut InValue) -> Result<(), Self::Error> {
        walk_in_value(self, in_value)
    }

    fn visit_expression(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        walk_expression(self, expression)
    }

    fn visit_common_table_expression(
        &mut self,
        cte: &'ast mut CommonTableExpression,
    ) -> Result<(), Self::Error> {
        walk_common_table_expression(self, cte)
    }

    fn visit_field_definition_expression(
        &mut self,
        fde: &'ast mut FieldDefinitionExpression,
    ) -> Result<(), Self::Error> {
        walk_field_definition_expression(self, fde)
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        self.visit_expression(expression)
    }

    fn visit_join_clause(&mut self, join: &'ast mut JoinClause) -> Result<(), Self::Error> {
        walk_join_clause(self, join)
    }

    fn visit_group_by_clause(
        &mut self,
        group_by: &'ast mut GroupByClause,
    ) -> Result<(), Self::Error> {
        walk_group_by_clause(self, group_by)
    }

    fn visit_order_clause(&mut self, order: &'ast mut OrderClause) -> Result<(), Self::Error> {
        walk_order_clause(self, order)
    }

    fn visit_limit_clause(&mut self, limit: &'ast mut LimitClause) -> Result<(), Self::Error> {
        walk_limit_clause(self, limit)
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        walk_select_statement(self, select_statement)
    }
}

pub fn walk_expression<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    expression: &'ast mut Expression,
) -> Result<(), V::Error> {
    match expression {
        Expression::Call(fexpr) => visitor.visit_function_expression(fexpr),
        Expression::Literal(lit) => visitor.visit_literal(lit),
        Expression::BinaryOp { lhs, rhs, .. } => {
            visitor.visit_expression(lhs.as_mut())?;
            visitor.visit_expression(rhs.as_mut())
        }
        Expression::UnaryOp { rhs, .. } => visitor.visit_expression(rhs.as_mut()),
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            visitor.visit_expression(condition.as_mut())?;
            visitor.visit_expression(then_expr.as_mut())?;
            if let Some(else_expr) = else_expr {
                visitor.visit_expression(else_expr)?;
            }
            Ok(())
        }
        Expression::Column(col) => visitor.visit_column(col),
        Expression::Exists(statement) => visitor.visit_select_statement(statement.as_mut()),
        Expression::Between {
            operand, min, max, ..
        } => {
            visitor.visit_expression(operand.as_mut())?;
            visitor.visit_expression(min.as_mut())?;
            visitor.visit_expression(max.as_mut())
        }
        Expression::NestedSelect(statement) => visitor.visit_select_statement(statement.as_mut()),
        Expression::In { lhs, rhs, .. } => {
            visitor.visit_expression(lhs.as_mut())?;
            visitor.visit_in_value(rhs)
        }
        Expression::Cast { expr, ty, .. } => {
            visitor.visit_expression(expr.as_mut())?;
            visitor.visit_sql_type(ty)
        }
    }
}

pub fn walk_function_expression<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    function_expression: &'ast mut FunctionExpression,
) -> Result<(), V::Error> {
    match function_expression {
        FunctionExpression::Avg { expr, .. } => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::Count { expr, .. } => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::CountStar => Ok(()),
        FunctionExpression::Sum { expr, .. } => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::Max(expr) => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::Min(expr) => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::GroupConcat { expr, .. } => visitor.visit_expression(expr.as_mut()),
        FunctionExpression::Call { arguments, .. } => {
            for arg in arguments {
                visitor.visit_expression(arg)?;
            }
            Ok(())
        }
    }
}

pub fn walk_in_value<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    in_value: &'ast mut InValue,
) -> Result<(), V::Error> {
    match in_value {
        InValue::Subquery(statement) => visitor.visit_select_statement(statement.as_mut()),
        InValue::List(exprs) => {
            for expr in exprs {
                visitor.visit_expression(expr)?;
            }
            Ok(())
        }
    }
}

pub fn walk_common_table_expression<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    cte: &'ast mut CommonTableExpression,
) -> Result<(), V::Error> {
    visitor.visit_select_statement(&mut cte.statement)
}

pub fn walk_field_definition_expression<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    fde: &'ast mut FieldDefinitionExpression,
) -> Result<(), V::Error> {
    match fde {
        FieldDefinitionExpression::All => Ok(()),
        FieldDefinitionExpression::AllInTable(_) => Ok(()),
        FieldDefinitionExpression::Expression { expr, .. } => visitor.visit_expression(expr),
    }
}

pub fn walk_join_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    join: &'ast mut JoinClause,
) -> Result<(), V::Error> {
    match &mut join.right {
        JoinRightSide::Table(table) => visitor.visit_table(table),
        JoinRightSide::Tables(tables) => {
            for table in tables {
                visitor.visit_table(table)?;
            }
            Ok(())
        }
        JoinRightSide::NestedSelect(statement, _) => {
            visitor.visit_select_statement(statement.as_mut())
        }
    }
}

pub fn walk_group_by_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    group_by_clause: &'ast mut GroupByClause,
) -> Result<(), V::Error> {
    for column in &mut group_by_clause.columns {
        visitor.visit_column(column)?;
    }
    for expr in &mut group_by_clause.having {
        visitor.visit_expression(expr)?;
    }
    Ok(())
}

pub fn walk_order_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    order_clause: &'ast mut OrderClause,
) -> Result<(), V::Error> {
    for (col, _) in &mut order_clause.columns {
        visitor.visit_column(col)?;
    }
    Ok(())
}

pub fn walk_limit_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    limit_clause: &'ast mut LimitClause,
) -> Result<(), V::Error> {
    visitor.visit_expression(&mut limit_clause.limit)?;
    if let Some(offset) = &mut limit_clause.offset {
        visitor.visit_expression(offset)?;
    }
    Ok(())
}

pub fn walk_select_statement<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    select_statement: &'ast mut SelectStatement,
) -> Result<(), V::Error> {
    for cte in &mut select_statement.ctes {
        visitor.visit_common_table_expression(cte)?;
    }
    for table in &mut select_statement.tables {
        visitor.visit_table(table)?;
    }
    for field in &mut select_statement.fields {
        visitor.visit_field_definition_expression(field)?;
    }
    for join in &mut select_statement.join {
        visitor.visit_join_clause(join)?;
    }
    if let Some(where_clause) = &mut select_statement.where_clause {
        visitor.visit_where_clause(where_clause)?;
    }
    if let Some(group_by_clause) = &mut select_statement.group_by {
        visitor.visit_group_by_clause(group_by_clause)?;
    }
    if let Some(order_clause) = &mut select_statement.order {
        visitor.visit_order_clause(order_clause)?;
    }
    if let Some(limit_clause) = &mut select_statement.limit {
        visitor.visit_limit_clause(limit_clause)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{select::selection, Dialect};

    use super::*;

    #[derive(Default, Debug, PartialEq, Eq)]
    struct NodeCounter(usize);

    impl<'ast> Visitor<'ast> for NodeCounter {
        type Error = ();

        fn visit_column(&mut self, _column: &'ast mut Column) -> Result<(), Self::Error> {
            self.0 += 1;
            Ok(())
        }

        fn visit_table(&mut self, _table: &'ast mut Table) -> Result<(), Self::Error> {
            self.0 += 1;
            Ok(())
        }

        fn visit_literal(&mut self, _literal: &'ast mut Literal) -> Result<(), Self::Error> {
            self.0 += 1;
            Ok(())
        }

        fn visit_function_expression(
            &mut self,
            function_expression: &'ast mut FunctionExpression,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_function_expression(self, function_expression)
        }

        fn visit_expression(
            &mut self,
            expression: &'ast mut Expression,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_expression(self, expression)
        }

        fn visit_common_table_expression(
            &mut self,
            cte: &'ast mut CommonTableExpression,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_common_table_expression(self, cte)
        }

        fn visit_field_definition_expression(
            &mut self,
            fde: &'ast mut FieldDefinitionExpression,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_field_definition_expression(self, fde)
        }

        fn visit_join_clause(&mut self, join: &'ast mut JoinClause) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_join_clause(self, join)
        }

        fn visit_group_by_clause(
            &mut self,
            group_by: &'ast mut GroupByClause,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_group_by_clause(self, group_by)
        }

        fn visit_order_clause(&mut self, order: &'ast mut OrderClause) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_order_clause(self, order)
        }

        fn visit_limit_clause(&mut self, limit: &'ast mut LimitClause) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_limit_clause(self, limit)
        }

        fn visit_select_statement(
            &mut self,
            select_statement: &'ast mut SelectStatement,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_select_statement(self, select_statement)
        }

        fn visit_sql_type(&mut self, _sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
            self.0 += 1;
            Ok(())
        }

        fn visit_in_value(&mut self, in_value: &'ast mut InValue) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_in_value(self, in_value)
        }
    }

    fn node_count(query: &str) -> usize {
        let mut counter = NodeCounter::default();
        counter
            .visit_select_statement(&mut test_parse!(
                selection(Dialect::MySQL),
                query.as_bytes()
            ))
            .unwrap();
        counter.0
    }

    #[test]
    fn simple_select() {
        assert_eq!(node_count("SELECT id FROM users"), 5)
    }

    #[test]
    fn binary_op() {
        assert_eq!(node_count("SELECT id + name FROM users"), 8);
    }

    #[test]
    fn join_subquery() {
        assert_eq!(
            node_count(
                "SELECT id, name FROM users join (select id from users) s on users.id = s.id"
            ),
            14
        )
    }
}
