//! AST walker for SQL, inspired by [rustc's AST visitor][rustc-ast-visit].
//!
//! [rustc-ast-visit]: https://doc.rust-lang.org/stable/nightly-rustc/rustc_ast/visit/index.html
#![warn(clippy::todo, clippy::unimplemented)]

use crate::set::Variable;
use crate::{
    Column, CommonTableExpr, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr,
    GroupByClause, InValue, JoinClause, JoinConstraint, JoinRightSide, Literal, OrderClause,
    SelectStatement, SqlType, Table, TableExpr,
};

/// Each method of the `Visitor` trait is a hook to be potentially overridden when recursively
/// traversing SQL statements. The default implementation of each method recursively visits the
/// substructure of the input via the corresponding `walk` method, eg `visit_expr` by default calls
/// `visit::walk_expr`. This allows defining algorithms that depend on recursively traversing ASTs
/// without having to reimplement AST traversal every time.
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
/// use nom_sql::analysis::visit::Visitor;
/// use nom_sql::{parse_query, Dialect, Literal, SqlQuery};
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
///         }
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
/// assert_eq!(
///     count_placeholders("SELECT id FROM users WHERE name = ? AND age = ?"),
///     2
/// );
/// ```
pub trait Visitor<'ast>: Sized {
    /// Errors that can be thrown during execution of this visitor
    type Error;

    fn visit_table(&mut self, _table: &'ast mut Table) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_literal(&mut self, _literal: &'ast mut Literal) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_sql_type(&mut self, _sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        walk_column(self, column)
    }

    fn visit_table_expr(&mut self, table_expr: &'ast mut TableExpr) -> Result<(), Self::Error> {
        walk_table_expr(self, table_expr)
    }

    fn visit_function_expr(
        &mut self,
        function_expr: &'ast mut FunctionExpr,
    ) -> Result<(), Self::Error> {
        walk_function_expr(self, function_expr)
    }

    fn visit_in_value(&mut self, in_value: &'ast mut InValue) -> Result<(), Self::Error> {
        walk_in_value(self, in_value)
    }

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        walk_expr(self, expr)
    }

    fn visit_common_table_expr(
        &mut self,
        cte: &'ast mut CommonTableExpr,
    ) -> Result<(), Self::Error> {
        walk_common_table_expr(self, cte)
    }

    fn visit_field_definition_expr(
        &mut self,
        fde: &'ast mut FieldDefinitionExpr,
    ) -> Result<(), Self::Error> {
        walk_field_definition_expr(self, fde)
    }

    fn visit_where_clause(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        self.visit_expr(expr)
    }

    fn visit_join_clause(&mut self, join: &'ast mut JoinClause) -> Result<(), Self::Error> {
        walk_join_clause(self, join)
    }

    fn visit_join_constraint(
        &mut self,
        join_constraint: &'ast mut JoinConstraint,
    ) -> Result<(), Self::Error> {
        walk_join_constraint(self, join_constraint)
    }

    fn visit_field_reference(
        &mut self,
        field_reference: &'ast mut FieldReference,
    ) -> Result<(), Self::Error> {
        walk_field_reference(self, field_reference)
    }

    fn visit_group_by_clause(
        &mut self,
        group_by: &'ast mut GroupByClause,
    ) -> Result<(), Self::Error> {
        walk_group_by_clause(self, group_by)
    }

    fn visit_having_clause(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        self.visit_expr(expr)
    }

    fn visit_order_clause(&mut self, order: &'ast mut OrderClause) -> Result<(), Self::Error> {
        walk_order_clause(self, order)
    }

    fn visit_limit_clause(&mut self, limit: &'ast mut Option<Literal>) -> Result<(), Self::Error> {
        walk_limit_clause(self, limit)
    }

    fn visit_offset_clause(
        &mut self,
        offset: &'ast mut Option<Literal>,
    ) -> Result<(), Self::Error> {
        walk_offset_clause(self, offset)
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        walk_select_statement(self, select_statement)
    }

    fn visit_variable(&mut self, _literal: &'ast mut Variable) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub fn walk_expr<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) -> Result<(), V::Error> {
    match expr {
        Expr::Call(fexpr) => visitor.visit_function_expr(fexpr),
        Expr::Literal(lit) => visitor.visit_literal(lit),
        Expr::BinaryOp { lhs, rhs, .. } => {
            visitor.visit_expr(lhs.as_mut())?;
            visitor.visit_expr(rhs.as_mut())
        }
        Expr::UnaryOp { rhs, .. } => visitor.visit_expr(rhs.as_mut()),
        Expr::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            visitor.visit_expr(condition.as_mut())?;
            visitor.visit_expr(then_expr.as_mut())?;
            if let Some(else_expr) = else_expr {
                visitor.visit_expr(else_expr)?;
            }
            Ok(())
        }
        Expr::Column(col) => visitor.visit_column(col),
        Expr::Exists(statement) => visitor.visit_select_statement(statement.as_mut()),
        Expr::Between {
            operand, min, max, ..
        } => {
            visitor.visit_expr(operand.as_mut())?;
            visitor.visit_expr(min.as_mut())?;
            visitor.visit_expr(max.as_mut())
        }
        Expr::NestedSelect(statement) => visitor.visit_select_statement(statement.as_mut()),
        Expr::In { lhs, rhs, .. } => {
            visitor.visit_expr(lhs.as_mut())?;
            visitor.visit_in_value(rhs)
        }
        Expr::Cast { expr, ty, .. } => {
            visitor.visit_expr(expr.as_mut())?;
            visitor.visit_sql_type(ty)
        }
        Expr::Variable(var) => visitor.visit_variable(var),
    }
}

pub fn walk_function_expr<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    function_expr: &'ast mut FunctionExpr,
) -> Result<(), V::Error> {
    match function_expr {
        FunctionExpr::Avg { expr, .. } => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::Count { expr, .. } => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::CountStar => Ok(()),
        FunctionExpr::Sum { expr, .. } => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::Max(expr) => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::Min(expr) => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::GroupConcat { expr, .. } => visitor.visit_expr(expr.as_mut()),
        FunctionExpr::Call { arguments, .. } => {
            for arg in arguments {
                visitor.visit_expr(arg)?;
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
                visitor.visit_expr(expr)?;
            }
            Ok(())
        }
    }
}

pub fn walk_common_table_expr<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    cte: &'ast mut CommonTableExpr,
) -> Result<(), V::Error> {
    visitor.visit_select_statement(&mut cte.statement)
}

pub fn walk_field_definition_expr<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    fde: &'ast mut FieldDefinitionExpr,
) -> Result<(), V::Error> {
    match fde {
        FieldDefinitionExpr::All => Ok(()),
        FieldDefinitionExpr::AllInTable(_) => Ok(()),
        FieldDefinitionExpr::Expr { expr, .. } => visitor.visit_expr(expr),
    }
}

pub fn walk_join_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    join: &'ast mut JoinClause,
) -> Result<(), V::Error> {
    match &mut join.right {
        JoinRightSide::Table(table_expr) => visitor.visit_table_expr(table_expr)?,
        JoinRightSide::Tables(table_exprs) => {
            for table_expr in table_exprs {
                visitor.visit_table_expr(table_expr)?;
            }
        }
        JoinRightSide::NestedSelect(statement, _) => {
            visitor.visit_select_statement(statement.as_mut())?
        }
    }

    visitor.visit_join_constraint(&mut join.constraint)
}

pub fn walk_join_constraint<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    join_constraint: &'ast mut JoinConstraint,
) -> Result<(), V::Error> {
    match join_constraint {
        JoinConstraint::On(expr) => visitor.visit_expr(expr),
        JoinConstraint::Using(cols) => {
            for col in cols {
                visitor.visit_column(col)?;
            }
            Ok(())
        }
        JoinConstraint::Empty => Ok(()),
    }
}

fn walk_field_reference<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    field_reference: &'ast mut FieldReference,
) -> Result<(), V::Error> {
    match field_reference {
        FieldReference::Numeric(_) => Ok(()),
        FieldReference::Expr(expr) => visitor.visit_expr(expr),
    }
}

pub fn walk_group_by_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    group_by_clause: &'ast mut GroupByClause,
) -> Result<(), V::Error> {
    for field in &mut group_by_clause.fields {
        visitor.visit_field_reference(field)?;
    }
    Ok(())
}

pub fn walk_order_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    order_clause: &'ast mut OrderClause,
) -> Result<(), V::Error> {
    for (field, _) in &mut order_clause.order_by {
        visitor.visit_field_reference(field)?;
    }
    Ok(())
}

pub fn walk_limit_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    limit_clause: &'ast mut Option<Literal>,
) -> Result<(), V::Error> {
    if let Some(limit) = limit_clause {
        visitor.visit_literal(limit)?;
    }
    Ok(())
}

pub fn walk_offset_clause<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    offset_clause: &'ast mut Option<Literal>,
) -> Result<(), V::Error> {
    if let Some(offset) = offset_clause {
        visitor.visit_literal(offset)?;
    }
    Ok(())
}

pub fn walk_column<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    column: &'ast mut Column,
) -> Result<(), V::Error> {
    if let Some(table) = &mut column.table {
        visitor.visit_table(table)?;
    }
    Ok(())
}

pub fn walk_table_expr<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    table_expr: &'ast mut TableExpr,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut table_expr.table)
}

pub fn walk_select_statement<'ast, V: Visitor<'ast>>(
    visitor: &mut V,
    select_statement: &'ast mut SelectStatement,
) -> Result<(), V::Error> {
    for cte in &mut select_statement.ctes {
        visitor.visit_common_table_expr(cte)?;
    }
    for table_expr in &mut select_statement.tables {
        visitor.visit_table_expr(table_expr)?;
    }
    for field in &mut select_statement.fields {
        visitor.visit_field_definition_expr(field)?;
    }
    for join in &mut select_statement.join {
        visitor.visit_join_clause(join)?;
    }
    if let Some(where_clause) = &mut select_statement.where_clause {
        visitor.visit_where_clause(where_clause)?;
    }
    if let Some(having_clause) = &mut select_statement.having {
        visitor.visit_having_clause(having_clause)?;
    }
    if let Some(group_by_clause) = &mut select_statement.group_by {
        visitor.visit_group_by_clause(group_by_clause)?;
    }
    if let Some(order_clause) = &mut select_statement.order {
        visitor.visit_order_clause(order_clause)?;
    }
    visitor.visit_limit_clause(&mut select_statement.limit)?;
    visitor.visit_offset_clause(&mut select_statement.offset)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::selection;
    use crate::Dialect;

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

        fn visit_function_expr(
            &mut self,
            function_expr: &'ast mut FunctionExpr,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_function_expr(self, function_expr)
        }

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_expr(self, expr)
        }

        fn visit_common_table_expr(
            &mut self,
            cte: &'ast mut CommonTableExpr,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_common_table_expr(self, cte)
        }

        fn visit_field_definition_expr(
            &mut self,
            fde: &'ast mut FieldDefinitionExpr,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_field_definition_expr(self, fde)
        }

        fn visit_join_clause(&mut self, join: &'ast mut JoinClause) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_join_clause(self, join)
        }

        fn visit_join_constraint(
            &mut self,
            join_constraint: &'ast mut JoinConstraint,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_join_constraint(self, join_constraint)
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

        fn visit_limit_clause(
            &mut self,
            limit: &'ast mut Option<Literal>,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_limit_clause(self, limit)
        }

        fn visit_offset_clause(
            &mut self,
            offset: &'ast mut Option<Literal>,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_offset_clause(self, offset)
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
        assert_eq!(node_count("SELECT id FROM users"), 7)
    }

    #[test]
    fn binary_op() {
        assert_eq!(node_count("SELECT id + name FROM users"), 10);
    }

    #[test]
    fn join_subquery() {
        assert_eq!(
            node_count(
                "SELECT id, name FROM users join (select id from users) s on users.id = s.id"
            ),
            24
        )
    }
}
