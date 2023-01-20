//! AST walker for SQL taking mutable references, inspired by [rustc's AST
//! visitor][rustc-ast-visit].
//!
//! [rustc-ast-visit]: https://doc.rust-lang.org/stable/nightly-rustc/rustc_ast/visit/index.html
//!
//! For an equivalent AST walker trait over **shared references**, see [`nom_sql::analysis::visit`].

// NOTE: remember that this file is effectively duplicated to `visit.rs` - any changes made here
// should be mirrored there!

#![warn(clippy::todo, clippy::unimplemented)]

use crate::create_table_options::CreateTableOption;
use crate::rename::{RenameTableOperation, RenameTableStatement};
use crate::select::LimitClause;
use crate::set::Variable;
use crate::transaction::{CommitStatement, RollbackStatement, StartTransactionStatement};
use crate::{
    AlterColumnOperation, AlterTableDefinition, AlterTableStatement, CacheInner, CaseWhenBranch,
    Column, ColumnConstraint, ColumnSpecification, CommonTableExpr, CompoundSelectStatement,
    CreateCacheStatement, CreateTableStatement, CreateViewStatement, DeleteStatement,
    DropAllCachesStatement, DropCacheStatement, DropTableStatement, DropViewStatement,
    ExplainStatement, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause,
    InValue, InsertStatement, JoinClause, JoinConstraint, JoinRightSide, Literal, OrderClause,
    Relation, SelectSpecification, SelectStatement, SetNames, SetPostgresParameter, SetStatement,
    SetVariables, ShowStatement, SqlIdentifier, SqlQuery, SqlType, TableExpr, TableExprInner,
    TableKey, UpdateStatement, UseStatement,
};

/// Each method of the `VisitorMut` trait is a hook to be potentially overridden when recursively
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
/// The following implements a VisitorMut that counts all occurrences of placeholder literals in a
/// statement.
///
/// ```
/// #![feature(never_type)]
///
/// use nom_sql::analysis::visit_mut::VisitorMut;
/// use nom_sql::{parse_query, Dialect, Literal, SqlQuery};
///
/// fn count_placeholders(query: &str) -> usize {
///     #[derive(Default)]
///     struct PlaceholderCounter(usize);
///
///     impl<'ast> VisitorMut<'ast> for PlaceholderCounter {
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
pub trait VisitorMut<'ast>: Sized {
    /// Errors that can be thrown during execution of this visitor
    type Error;

    fn visit_sql_identifier(
        &mut self,
        _sql_ident: &'ast mut SqlIdentifier,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        walk_relation(self, table)
    }

    fn visit_literal(&mut self, _literal: &'ast mut Literal) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_sql_type(&mut self, sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        walk_sql_type(self, sql_type)
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        walk_column(self, column)
    }

    fn visit_variable(&mut self, variable: &'ast mut Variable) -> Result<(), Self::Error> {
        self.visit_sql_identifier(&mut variable.name)
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

    fn visit_case_when_branch(
        &mut self,
        branch: &'ast mut CaseWhenBranch,
    ) -> Result<(), Self::Error> {
        walk_case_when_branch(self, branch)
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

    fn visit_limit_clause(
        &mut self,
        limit_clause: &'ast mut LimitClause,
    ) -> Result<(), Self::Error> {
        walk_limit_clause(self, limit_clause)
    }

    fn visit_limit(&mut self, limit: &'ast mut Literal) -> Result<(), Self::Error> {
        walk_limit(self, limit)
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        walk_offset(self, offset)
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        walk_select_statement(self, select_statement)
    }

    fn visit_create_table_statement(
        &mut self,
        create_table_statement: &'ast mut CreateTableStatement,
    ) -> Result<(), Self::Error> {
        walk_create_table_statement(self, create_table_statement)
    }

    fn visit_column_specification(
        &mut self,
        column_specification: &'ast mut ColumnSpecification,
    ) -> Result<(), Self::Error> {
        walk_column_specification(self, column_specification)
    }

    fn visit_table_key(&mut self, table_key: &'ast mut TableKey) -> Result<(), Self::Error> {
        walk_table_key(self, table_key)
    }

    fn visit_create_table_option(
        &mut self,
        _create_table_option: &'ast mut CreateTableOption,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_column_constraint(
        &mut self,
        column_constraint: &'ast mut ColumnConstraint,
    ) -> Result<(), Self::Error> {
        walk_column_constraint(self, column_constraint)
    }

    fn visit_create_view_statement(
        &mut self,
        create_view_statement: &'ast mut CreateViewStatement,
    ) -> Result<(), Self::Error> {
        walk_create_view_statement(self, create_view_statement)
    }

    fn visit_alter_table_statement(
        &mut self,
        alter_table_statement: &'ast mut AlterTableStatement,
    ) -> Result<(), Self::Error> {
        walk_alter_table_statement(self, alter_table_statement)
    }

    fn visit_alter_table_definition(
        &mut self,
        alter_table_definition: &'ast mut AlterTableDefinition,
    ) -> Result<(), Self::Error> {
        walk_alter_table_definition(self, alter_table_definition)
    }

    fn visit_alter_column_operation(
        &mut self,
        alter_column_operation: &'ast mut AlterColumnOperation,
    ) -> Result<(), Self::Error> {
        walk_alter_column_operation(self, alter_column_operation)
    }

    fn visit_insert_statement(
        &mut self,
        insert_statement: &'ast mut InsertStatement,
    ) -> Result<(), Self::Error> {
        walk_insert_statement(self, insert_statement)
    }

    fn visit_compound_select_statement(
        &mut self,
        compound_select_statement: &'ast mut CompoundSelectStatement,
    ) -> Result<(), Self::Error> {
        walk_compound_select_statement(self, compound_select_statement)
    }

    fn visit_delete_statement(
        &mut self,
        delete_statement: &'ast mut DeleteStatement,
    ) -> Result<(), Self::Error> {
        walk_delete_statement(self, delete_statement)
    }

    fn visit_drop_table_statement(
        &mut self,
        drop_table_statement: &'ast mut DropTableStatement,
    ) -> Result<(), Self::Error> {
        walk_drop_table_statement(self, drop_table_statement)
    }

    fn visit_update_statement(
        &mut self,
        update_statement: &'ast mut UpdateStatement,
    ) -> Result<(), Self::Error> {
        walk_update_statement(self, update_statement)
    }

    fn visit_set_statement(
        &mut self,
        set_statement: &'ast mut SetStatement,
    ) -> Result<(), Self::Error> {
        walk_set_statement(self, set_statement)
    }

    fn visit_set_variables(
        &mut self,
        set_variables: &'ast mut SetVariables,
    ) -> Result<(), Self::Error> {
        walk_set_variables(self, set_variables)
    }

    fn visit_set_names(&mut self, _set_names: &'ast mut SetNames) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_set_postgres_parameter(
        &mut self,
        set_postgres_parameter: &'ast mut SetPostgresParameter,
    ) -> Result<(), Self::Error> {
        self.visit_sql_identifier(&mut set_postgres_parameter.name)
    }

    fn visit_start_transaction_statement(
        &mut self,
        _start_transaction_statement: &'ast mut StartTransactionStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_commit_statement(
        &mut self,
        _commit_statement: &'ast mut CommitStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_rollback_statement(
        &mut self,
        _rollback_statement: &'ast mut RollbackStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_rename_table_statement(
        &mut self,
        rename_table_statement: &'ast mut RenameTableStatement,
    ) -> Result<(), Self::Error> {
        walk_rename_table_statement(self, rename_table_statement)
    }

    fn visit_rename_table_operation(
        &mut self,
        rename_table_operation: &'ast mut RenameTableOperation,
    ) -> Result<(), Self::Error> {
        walk_rename_table_operation(self, rename_table_operation)
    }

    fn visit_create_cache_statement(
        &mut self,
        create_cache_statement: &'ast mut CreateCacheStatement,
    ) -> Result<(), Self::Error> {
        walk_create_cache_statement(self, create_cache_statement)
    }

    fn visit_drop_cache_statement(
        &mut self,
        drop_cache_statement: &'ast mut DropCacheStatement,
    ) -> Result<(), Self::Error> {
        walk_relation(self, &mut drop_cache_statement.name)
    }

    fn visit_drop_all_caches_statement(
        &mut self,
        _drop_all_caches_statement: &'ast mut DropAllCachesStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_drop_view_statement(
        &mut self,
        drop_view_statement: &'ast mut DropViewStatement,
    ) -> Result<(), Self::Error> {
        walk_drop_view_statement(self, drop_view_statement)
    }

    fn visit_use_statement(
        &mut self,
        use_statement: &'ast mut UseStatement,
    ) -> Result<(), Self::Error> {
        self.visit_sql_identifier(&mut use_statement.database)
    }

    fn visit_show_statement(
        &mut self,
        _show_statement: &'ast mut ShowStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_explain_statement(
        &mut self,
        _explain_statement: &'ast mut ExplainStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_sql_query(&mut self, sql_query: &'ast mut SqlQuery) -> Result<(), Self::Error> {
        walk_sql_query(self, sql_query)
    }
}

pub fn walk_expr<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) -> Result<(), V::Error> {
    match expr {
        Expr::Call(fexpr) => visitor.visit_function_expr(fexpr),
        Expr::Literal(lit) => visitor.visit_literal(lit),
        Expr::BinaryOp { lhs, rhs, .. }
        | Expr::OpAny { lhs, rhs, .. }
        | Expr::OpSome { lhs, rhs, .. }
        | Expr::OpAll { lhs, rhs, .. } => {
            visitor.visit_expr(lhs.as_mut())?;
            visitor.visit_expr(rhs.as_mut())
        }
        Expr::UnaryOp { rhs, .. } => visitor.visit_expr(rhs.as_mut()),
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            for branch in branches {
                visitor.visit_case_when_branch(branch)?;
            }
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
        Expr::Array(exprs) => {
            for expr in exprs {
                visitor.visit_expr(expr)?;
            }
            Ok(())
        }
        Expr::Variable(var) => visitor.visit_variable(var),
    }
}

pub fn walk_case_when_branch<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    branch: &'ast mut CaseWhenBranch,
) -> Result<(), V::Error> {
    visitor.visit_expr(&mut branch.condition)?;
    visitor.visit_expr(&mut branch.body)
}

pub fn walk_function_expr<'ast, V: VisitorMut<'ast>>(
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
        FunctionExpr::Substring { string, pos, len } => {
            visitor.visit_expr(string.as_mut())?;
            if let Some(pos) = pos {
                visitor.visit_expr(pos.as_mut())?;
            }
            if let Some(len) = len {
                visitor.visit_expr(len.as_mut())?;
            }
            Ok(())
        }
    }
}

pub fn walk_in_value<'ast, V: VisitorMut<'ast>>(
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

pub fn walk_common_table_expr<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    cte: &'ast mut CommonTableExpr,
) -> Result<(), V::Error> {
    visitor.visit_sql_identifier(&mut cte.name)?;
    visitor.visit_select_statement(&mut cte.statement)
}

pub fn walk_field_definition_expr<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    fde: &'ast mut FieldDefinitionExpr,
) -> Result<(), V::Error> {
    match fde {
        FieldDefinitionExpr::All => Ok(()),
        FieldDefinitionExpr::AllInTable(t) => visitor.visit_table(t),
        FieldDefinitionExpr::Expr { expr, alias } => {
            visitor.visit_expr(expr)?;
            if let Some(alias) = alias {
                visitor.visit_sql_identifier(alias)?;
            }
            Ok(())
        }
    }
}

pub fn walk_join_clause<'ast, V: VisitorMut<'ast>>(
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
    }

    visitor.visit_join_constraint(&mut join.constraint)
}

pub fn walk_join_constraint<'ast, V: VisitorMut<'ast>>(
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

fn walk_field_reference<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    field_reference: &'ast mut FieldReference,
) -> Result<(), V::Error> {
    match field_reference {
        FieldReference::Numeric(_) => Ok(()),
        FieldReference::Expr(expr) => visitor.visit_expr(expr),
    }
}

pub fn walk_group_by_clause<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    group_by_clause: &'ast mut GroupByClause,
) -> Result<(), V::Error> {
    for field in &mut group_by_clause.fields {
        visitor.visit_field_reference(field)?;
    }
    Ok(())
}

pub fn walk_order_clause<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    order_clause: &'ast mut OrderClause,
) -> Result<(), V::Error> {
    for (field, _) in &mut order_clause.order_by {
        visitor.visit_field_reference(field)?;
    }
    Ok(())
}

pub fn walk_limit_clause<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    limit_clause: &'ast mut LimitClause,
) -> Result<(), V::Error> {
    let (limit, offset) = limit_clause.limit_and_offset_mut();
    if let Some(limit) = limit {
        visitor.visit_limit(limit)?;
    }
    if let Some(offset) = offset {
        visitor.visit_offset(offset)?;
    }

    Ok(())
}

pub fn walk_limit<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    limit: &'ast mut Literal,
) -> Result<(), V::Error> {
    visitor.visit_literal(limit)?;
    Ok(())
}

pub fn walk_offset<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    offset: &'ast mut Literal,
) -> Result<(), V::Error> {
    visitor.visit_literal(offset)?;
    Ok(())
}

pub fn walk_relation<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    relation: &'ast mut Relation,
) -> Result<(), V::Error> {
    if let Some(schema) = &mut relation.schema {
        visitor.visit_sql_identifier(schema)?;
    }
    visitor.visit_sql_identifier(&mut relation.name)
}

pub fn walk_sql_type<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    sql_type: &'ast mut SqlType,
) -> Result<(), V::Error> {
    match sql_type {
        SqlType::Array(t) => visitor.visit_sql_type(t),
        _ => Ok(()),
    }
}

pub fn walk_column<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    column: &'ast mut Column,
) -> Result<(), V::Error> {
    visitor.visit_sql_identifier(&mut column.name)?;
    if let Some(table) = &mut column.table {
        visitor.visit_table(table)?;
    }
    Ok(())
}

pub fn walk_table_expr<'ast, V: VisitorMut<'ast>>(
    visitor: &mut V,
    table_expr: &'ast mut TableExpr,
) -> Result<(), V::Error> {
    match &mut table_expr.inner {
        TableExprInner::Table(table) => visitor.visit_table(table)?,
        TableExprInner::Subquery(sq) => visitor.visit_select_statement(sq)?,
    }
    if let Some(ref mut alias) = table_expr.alias {
        visitor.visit_sql_identifier(alias)?;
    }
    Ok(())
}

pub fn walk_select_statement<'ast, V: VisitorMut<'ast>>(
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
    visitor.visit_limit_clause(&mut select_statement.limit_clause)?;
    Ok(())
}

pub fn walk_create_table_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    create_table_statement: &'a mut CreateTableStatement,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut create_table_statement.table)?;

    if let Ok(body) = &mut create_table_statement.body {
        for field in &mut body.fields {
            visitor.visit_column_specification(field)?;
        }

        if let Some(keys) = &mut body.keys {
            for key in keys {
                visitor.visit_table_key(key)?;
            }
        }
    }

    if let Ok(options) = &mut create_table_statement.options {
        for option in options {
            visitor.visit_create_table_option(option)?;
        }
    }

    Ok(())
}

pub fn walk_column_specification<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    column_specification: &'a mut ColumnSpecification,
) -> Result<(), V::Error> {
    visitor.visit_column(&mut column_specification.column)?;
    visitor.visit_sql_type(&mut column_specification.sql_type)?;
    for constraint in &mut column_specification.constraints {
        visitor.visit_column_constraint(constraint)?;
    }

    Ok(())
}

pub fn walk_table_key<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    table_key: &'a mut TableKey,
) -> Result<(), V::Error> {
    match table_key {
        TableKey::PrimaryKey {
            constraint_name,
            index_name,
            columns,
        } => {
            if let Some(constraint_name) = constraint_name {
                visitor.visit_sql_identifier(constraint_name)?;
            }

            if let Some(index_name) = index_name {
                visitor.visit_sql_identifier(index_name)?;
            }

            for column in columns {
                visitor.visit_column(column)?;
            }
        }
        TableKey::UniqueKey {
            constraint_name,
            index_name,
            columns,
            index_type: _,
        } => {
            if let Some(constraint_name) = constraint_name {
                visitor.visit_sql_identifier(constraint_name)?;
            }
            if let Some(index_name) = index_name {
                visitor.visit_sql_identifier(index_name)?;
            }
            for column in columns {
                visitor.visit_column(column)?;
            }
        }
        TableKey::FulltextKey {
            index_name,
            columns,
        } => {
            if let Some(index_name) = index_name {
                visitor.visit_sql_identifier(index_name)?;
            }
            for column in columns {
                visitor.visit_column(column)?;
            }
        }
        TableKey::Key {
            constraint_name,
            index_name,
            columns,
            index_type: _,
        } => {
            if let Some(constraint_name) = constraint_name {
                visitor.visit_sql_identifier(constraint_name)?;
            }
            if let Some(index_name) = index_name {
                visitor.visit_sql_identifier(index_name)?;
            }
            for column in columns {
                visitor.visit_column(column)?;
            }
        }
        TableKey::ForeignKey {
            constraint_name,
            index_name,
            columns,
            target_table,
            target_columns,
            on_delete: _,
            on_update: _,
        } => {
            if let Some(constraint_name) = constraint_name {
                visitor.visit_sql_identifier(constraint_name)?;
            }
            if let Some(index_name) = index_name {
                visitor.visit_sql_identifier(index_name)?;
            }
            for column in columns {
                visitor.visit_column(column)?;
            }
            visitor.visit_table(target_table)?;
            for column in target_columns {
                visitor.visit_column(column)?;
            }
        }
        TableKey::CheckConstraint {
            constraint_name,
            expr,
            enforced: _,
        } => {
            if let Some(constraint_name) = constraint_name {
                visitor.visit_sql_identifier(constraint_name)?;
            }
            visitor.visit_expr(expr)?;
        }
    }
    Ok(())
}

pub fn walk_column_constraint<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    column_constraint: &'a mut ColumnConstraint,
) -> Result<(), V::Error> {
    match column_constraint {
        ColumnConstraint::DefaultValue(expr) => visitor.visit_expr(expr),
        ColumnConstraint::Null
        | ColumnConstraint::NotNull
        | ColumnConstraint::CharacterSet(_)
        | ColumnConstraint::Collation(_)
        | ColumnConstraint::AutoIncrement
        | ColumnConstraint::PrimaryKey
        | ColumnConstraint::Unique
        | ColumnConstraint::OnUpdateCurrentTimestamp => Ok(()),
    }
}

pub fn walk_create_view_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    create_view_statement: &'a mut CreateViewStatement,
) -> Result<(), V::Error> {
    walk_relation(visitor, &mut create_view_statement.name)?;

    for column in &mut create_view_statement.fields {
        visitor.visit_column(column)?;
    }

    if let Ok(definition) = &mut create_view_statement.definition {
        match definition.as_mut() {
            SelectSpecification::Compound(stmt) => visitor.visit_compound_select_statement(stmt)?,
            SelectSpecification::Simple(stmt) => visitor.visit_select_statement(stmt)?,
        }
    }

    Ok(())
}

pub fn walk_alter_table_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    alter_table_statement: &'a mut AlterTableStatement,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut alter_table_statement.table)?;
    if let Ok(ref mut definitions) = alter_table_statement.definitions {
        for definition in definitions {
            visitor.visit_alter_table_definition(definition)?;
        }
    }
    Ok(())
}

pub fn walk_alter_table_definition<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    alter_table_definition: &'a mut AlterTableDefinition,
) -> Result<(), V::Error> {
    match alter_table_definition {
        AlterTableDefinition::AddColumn(spec) => visitor.visit_column_specification(spec),
        AlterTableDefinition::AddKey(key) => visitor.visit_table_key(key),
        AlterTableDefinition::AlterColumn { name: _, operation } => {
            visitor.visit_alter_column_operation(operation)
        }
        AlterTableDefinition::ChangeColumn { name: _, spec } => {
            visitor.visit_column_specification(spec)
        }
        AlterTableDefinition::DropColumn {
            name: _,
            behavior: _,
        }
        | AlterTableDefinition::RenameColumn {
            name: _,
            new_name: _,
        }
        | AlterTableDefinition::DropConstraint {
            name: _,
            drop_behavior: _,
        } => Ok(()),
    }
}

pub fn walk_alter_column_operation<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    alter_column_operation: &'a mut AlterColumnOperation,
) -> Result<(), V::Error> {
    match alter_column_operation {
        AlterColumnOperation::SetColumnDefault(lit) => visitor.visit_literal(lit),
        AlterColumnOperation::DropColumnDefault => Ok(()),
    }
}

pub fn walk_insert_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    insert_statement: &'a mut InsertStatement,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut insert_statement.table)?;
    if let Some(fields) = &mut insert_statement.fields {
        for column in fields {
            visitor.visit_column(column)?;
        }
    }

    for row in &mut insert_statement.data {
        for val in row {
            visitor.visit_expr(val)?;
        }
    }

    if let Some(on_duplicate) = &mut insert_statement.on_duplicate {
        for (column, expr) in on_duplicate {
            visitor.visit_column(column)?;
            visitor.visit_expr(expr)?;
        }
    }

    Ok(())
}

pub fn walk_compound_select_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    compound_select_statement: &'a mut CompoundSelectStatement,
) -> Result<(), V::Error> {
    for (_, stmt) in &mut compound_select_statement.selects {
        visitor.visit_select_statement(stmt)?;
    }

    if let Some(order) = &mut compound_select_statement.order {
        visitor.visit_order_clause(order)?;
    }

    visitor.visit_limit_clause(&mut compound_select_statement.limit_clause)?;

    Ok(())
}

pub fn walk_delete_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    delete_statement: &'a mut DeleteStatement,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut delete_statement.table)?;
    if let Some(expr) = &mut delete_statement.where_clause {
        visitor.visit_where_clause(expr)?;
    }
    Ok(())
}

pub fn walk_drop_table_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    drop_table_statement: &'a mut DropTableStatement,
) -> Result<(), V::Error> {
    for table in &mut drop_table_statement.tables {
        visitor.visit_table(table)?;
    }
    Ok(())
}

pub fn walk_update_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    update_statement: &'a mut UpdateStatement,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut update_statement.table)?;
    for (col, expr) in &mut update_statement.fields {
        visitor.visit_column(col)?;
        visitor.visit_expr(expr)?;
    }

    if let Some(expr) = &mut update_statement.where_clause {
        visitor.visit_where_clause(expr)?;
    }

    Ok(())
}

pub fn walk_set_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    set_statement: &'a mut SetStatement,
) -> Result<(), V::Error> {
    match set_statement {
        SetStatement::Variable(set_vars) => visitor.visit_set_variables(set_vars),
        SetStatement::Names(set_names) => visitor.visit_set_names(set_names),
        SetStatement::PostgresParameter(set_postgres_parameter) => {
            visitor.visit_set_postgres_parameter(set_postgres_parameter)
        }
    }
}

pub fn walk_set_variables<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    set_variables: &'a mut SetVariables,
) -> Result<(), V::Error> {
    for (var, expr) in &mut set_variables.variables {
        visitor.visit_variable(var)?;
        visitor.visit_expr(expr)?;
    }
    Ok(())
}

pub fn walk_rename_table_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    rename_table_statement: &'a mut RenameTableStatement,
) -> Result<(), V::Error> {
    for op in &mut rename_table_statement.ops {
        visitor.visit_rename_table_operation(op)?;
    }

    Ok(())
}

pub fn walk_rename_table_operation<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    rename_table_operation: &'a mut RenameTableOperation,
) -> Result<(), V::Error> {
    visitor.visit_table(&mut rename_table_operation.from)?;
    visitor.visit_table(&mut rename_table_operation.to)
}

pub fn walk_create_cache_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    create_cache_statement: &'a mut CreateCacheStatement,
) -> Result<(), V::Error> {
    match &mut create_cache_statement.inner {
        Ok(CacheInner::Statement(stmt)) => visitor.visit_select_statement(stmt)?,
        Ok(CacheInner::Id(_)) => {}
        Err(_) => {}
    }

    Ok(())
}

pub fn walk_drop_view_statement<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    drop_view_statement: &'a mut DropViewStatement,
) -> Result<(), V::Error> {
    for view in &mut drop_view_statement.views {
        visitor.visit_table(view)?;
    }

    Ok(())
}

pub fn walk_sql_query<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    sql_query: &'a mut SqlQuery,
) -> Result<(), V::Error> {
    match sql_query {
        SqlQuery::CreateTable(statement) => visitor.visit_create_table_statement(statement),
        SqlQuery::CreateView(statement) => visitor.visit_create_view_statement(statement),
        SqlQuery::AlterTable(statement) => visitor.visit_alter_table_statement(statement),
        SqlQuery::Insert(statement) => visitor.visit_insert_statement(statement),
        SqlQuery::CompoundSelect(statement) => visitor.visit_compound_select_statement(statement),
        SqlQuery::Select(statement) => visitor.visit_select_statement(statement),
        SqlQuery::Delete(statement) => visitor.visit_delete_statement(statement),
        SqlQuery::DropTable(statement) => visitor.visit_drop_table_statement(statement),
        SqlQuery::Update(statement) => visitor.visit_update_statement(statement),
        SqlQuery::Set(statement) => visitor.visit_set_statement(statement),
        SqlQuery::StartTransaction(statement) => {
            visitor.visit_start_transaction_statement(statement)
        }
        SqlQuery::Commit(statement) => visitor.visit_commit_statement(statement),
        SqlQuery::Rollback(statement) => visitor.visit_rollback_statement(statement),
        SqlQuery::RenameTable(statement) => visitor.visit_rename_table_statement(statement),
        SqlQuery::CreateCache(statement) => visitor.visit_create_cache_statement(statement),
        SqlQuery::DropCache(statement) => visitor.visit_drop_cache_statement(statement),
        SqlQuery::DropAllCaches(statement) => visitor.visit_drop_all_caches_statement(statement),
        SqlQuery::DropView(statement) => visitor.visit_drop_view_statement(statement),
        SqlQuery::Use(statement) => visitor.visit_use_statement(statement),
        SqlQuery::Show(statement) => visitor.visit_show_statement(statement),
        SqlQuery::Explain(statement) => visitor.visit_explain_statement(statement),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::selection;
    use crate::Dialect;

    #[derive(Default, Debug, PartialEq, Eq)]
    struct NodeCounter(usize);

    impl<'ast> VisitorMut<'ast> for NodeCounter {
        type Error = ();

        fn visit_column(&mut self, _column: &'ast mut Column) -> Result<(), Self::Error> {
            self.0 += 1;
            Ok(())
        }

        fn visit_table(&mut self, _table: &'ast mut Relation) -> Result<(), Self::Error> {
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
            limit_clause: &'ast mut LimitClause,
        ) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_limit_clause(self, limit_clause)
        }

        fn visit_limit(&mut self, limit: &'ast mut Literal) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_limit(self, limit)
        }

        fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
            self.0 += 1;
            walk_offset(self, offset)
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
        assert_eq!(node_count("SELECT id FROM users"), 6)
    }

    #[test]
    fn binary_op() {
        assert_eq!(node_count("SELECT id + name FROM users"), 9);
    }

    #[test]
    fn join_subquery() {
        assert_eq!(
            node_count(
                "SELECT id, name FROM users join (select id from users) s on users.id = s.id"
            ),
            22
        )
    }

    #[test]
    fn limit() {
        assert_eq!(node_count("SELECT id + name FROM users LIMIT 3"), 11);
    }

    #[test]
    fn limit_offset() {
        assert_eq!(
            node_count("SELECT id + name FROM users LIMIT 3 OFFSET 5"),
            13
        );
    }

    #[test]
    fn limit_comma_offset() {
        assert_eq!(node_count("SELECT id + name FROM users LIMIT 5, 3"), 13);
    }
}
