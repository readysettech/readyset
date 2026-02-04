use crate::expr::constant_fold::constant_fold_expr;
use itertools::Either;
use readyset_data::dialect;
use readyset_errors::{
    ReadySetError, ReadySetResult, internal, internal_err, invalid_query, invalid_query_err,
    invariant,
};
use readyset_sql::analysis::visit::{Visitor, walk_function_expr, walk_select_statement};
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_expr};
use readyset_sql::analysis::{ReferredColumns, is_aggregate, visit, visit_mut};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause,
    InValue, JoinConstraint, JoinRightSide, LimitClause, Literal, OrderBy, OrderClause, OrderType,
    Relation, SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{HashMap, HashSet};
use std::{iter, mem};

const INNER_STMT_ALIAS: &str = "INNER";

/// Iterate over all FROM items, including JOIN right-hand tables (mutable).
#[macro_export]
macro_rules! get_local_from_items_iter_mut {
    ($stmt:expr) => {
        $stmt
            .tables
            .iter_mut()
            .chain(
                $stmt
                    .join
                    .iter_mut()
                    .flat_map(|join| match &mut join.right {
                        JoinRightSide::Table(table) => Either::Left(iter::once(table)),
                        JoinRightSide::Tables(tables) => Either::Right(tables.iter_mut()),
                    }),
            )
    };
}

/// Iterate over all FROM items, including JOIN right-hand tables.
#[macro_export]
macro_rules! get_local_from_items_iter {
    ($stmt:expr) => {
        $stmt
            .tables
            .iter()
            .chain($stmt.join.iter().flat_map(|join| match &join.right {
                JoinRightSide::Table(table) => Either::Left(iter::once(table)),
                JoinRightSide::Tables(tables) => Either::Right(tables.iter()),
            }))
    };
}

#[macro_export]
macro_rules! is_single_from_item {
    ($stmt:expr) => {
        ($stmt).tables.len() == 1 && ($stmt).join.is_empty()
    };
}

#[macro_export]
macro_rules! is_column_of {
    ($col:expr,$rel:expr) => {
        matches!(&$col.table, Some(t) if *t == $rel)
    };
}

#[macro_export]
macro_rules! as_column {
    ($expr: expr) => {
        if let Expr::Column(column) = $expr {
            column
        } else {
            unreachable!("Must be Column")
        }
    };
}

/// Classification of a single JOIN `ON` atom under the supported-join policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnAtom {
    /// Equality between columns from **different** relations.
    CrossEq { lhs: Relation, rhs: Relation },
    /// A *simple parametrizable filter* that touches **exactly one** relation.
    SingleRelFilter { rel: Relation },
    /// Anything else (OR/NOT/subqueries/functions at top-level, 3+ relations, etc.).
    Other,
}

#[derive(Default, Copy, Clone, Debug)]
pub struct RewriteStatus {
    rewrite_count: i32,
    rollback_count: i32,
}

pub const NO_REWRITES_STATUS: RewriteStatus = RewriteStatus {
    rewrite_count: 0,
    rollback_count: 0,
};

pub const SINGLE_REWRITE_STATUS: RewriteStatus = RewriteStatus {
    rewrite_count: 1,
    rollback_count: 0,
};

impl RewriteStatus {
    pub fn rewrite(&mut self) {
        self.rewrite_count += 1;
    }

    pub fn rollback(&mut self) {
        self.rollback_count += 1;
    }

    pub fn combine(self, status: Self) -> Self {
        Self {
            rewrite_count: self.rewrite_count + status.rewrite_count,
            rollback_count: self.rollback_count + status.rollback_count,
        }
    }

    pub fn has_rollbacks(&self) -> bool {
        self.rollback_count > 0
    }

    pub fn has_rewrites(&self) -> bool {
        self.rewrite_count > 0
    }
}

enum ConstraintKind<'a> {
    EqualityComparison(&'a Expr, &'a Expr),
    OrderingComparison(&'a Expr, BinaryOperator, &'a Expr),
    Other(&'a Expr),
}

impl<'a> ConstraintKind<'a> {
    fn new(constraint: &'a Expr) -> Self {
        match constraint {
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::Equal,
                rhs,
            } => ConstraintKind::EqualityComparison(lhs.as_ref(), rhs.as_ref()),
            Expr::BinaryOp { lhs, op, rhs } if op.is_ordering_comparison() => {
                ConstraintKind::OrderingComparison(lhs.as_ref(), *op, rhs.as_ref())
            }
            _ => ConstraintKind::Other(constraint),
        }
    }

    fn is_same_as(&self, expr: &'a Expr) -> bool {
        match &self {
            ConstraintKind::EqualityComparison(c_lhs, c_rhs) => match expr {
                Expr::BinaryOp {
                    lhs,
                    op: BinaryOperator::Equal,
                    rhs,
                } => {
                    let (e_lhs, e_rhs) = (lhs.as_ref(), rhs.as_ref());
                    (e_lhs.eq(c_lhs) && e_rhs.eq(c_rhs)) || (e_rhs.eq(c_lhs) && e_lhs.eq(c_rhs))
                }
                _ => false,
            },
            ConstraintKind::OrderingComparison(c_lhs, c_op, c_rhs) => match expr {
                Expr::BinaryOp { lhs, op, rhs } if op.is_ordering_comparison() => {
                    let (e_lhs, e_rhs) = (lhs.as_ref(), rhs.as_ref());
                    (op.eq(c_op) && e_lhs.eq(c_lhs) && e_rhs.eq(c_rhs))
                        || (op.flip_ordering_comparison().is_ok_and(|flipped_op| {
                            flipped_op.eq(c_op) && e_rhs.eq(c_lhs) && e_lhs.eq(c_rhs)
                        }))
                }
                _ => false,
            },
            ConstraintKind::Other(c) => expr.eq(c),
        }
    }

    /// Check if constraint associated with `self` is contained in `expr`
    fn is_contained_in(&self, expr: &'a Expr) -> bool {
        let mut is_contained = false;
        split_expr_mut(
            expr,
            &mut |e| {
                if !is_contained && self.is_same_as(e) {
                    is_contained = true;
                }
                false
            },
            &mut vec![],
        );
        is_contained
    }
}

pub(crate) fn has_alias(stmt: &SelectStatement, alias: &SqlIdentifier) -> bool {
    stmt.fields.iter().any(|f| {
        matches!(f,
            FieldDefinitionExpr::Expr { alias: Some(a), .. } if a == alias
        )
    })
}

/// Return the alias or table name for a FROM item; error if a subquery lacks an alias.
pub(crate) fn get_from_item_reference_name(tab_expr: &TableExpr) -> ReadySetResult<Relation> {
    match tab_expr {
        // If this table expr has an explicit alias, use it.
        TableExpr {
            alias: Some(alias), ..
        } => Ok(alias.into()),
        // Otherwise, if it's a plain table, use its name.
        TableExpr {
            inner: TableExprInner::Table(tab_name),
            ..
        } => Ok(tab_name.clone()),
        // Reject unaliased subqueries.
        TableExpr {
            inner: TableExprInner::Subquery(_),
            ..
        } => invalid_query!("All subqueries must have an alias"),
    }
}

/// Mutably iterators over all columns referred to by an expression.
/// Used for scanning and rewriting column references as projections are hoisted.
pub(crate) fn columns_iter_mut(expr: &mut Expr) -> impl Iterator<Item = &mut Column> {
    expr.referred_columns_mut()
}

/// Immutably iterators over all columns referred to by an expression.
/// Used for scanning and rewriting column references as projections are hoisted.
pub(crate) fn columns_iter(expr: &Expr) -> impl Iterator<Item = &Column> {
    expr.referred_columns()
}

/// Remove all TRUE literals from an AND expression, collapsing empty results to TRUE.
pub(crate) fn remove_literals_true(expr: &Expr) -> Option<Expr> {
    // Extract and discard any TRUE literals.
    let mut split = Vec::new();
    split_expr(
        expr,
        &|pred| matches!(pred, Expr::Literal(Literal::Boolean(true))),
        &mut split,
    )
    // If no predicates remain, collapse to a single TRUE.
    .or_else(|| Some(Expr::Literal(Literal::Boolean(true))))
}

/// AND two predicates, omitting any redundant TRUEs in the result.
pub(crate) fn and_predicates_skip_true(acc_expr: Option<Expr>, constraint: Expr) -> Option<Expr> {
    and_predicates(acc_expr, constraint)
        .and_then(|existing_expr| remove_literals_true(&existing_expr))
}

/// Return only that part of `sub_expr` which is not contained in `expr`, or
/// None if entirely contained.
pub(crate) fn expr_difference(expr: &Expr, sub_expr: Expr) -> Option<Expr> {
    //
    let mut constraints = Vec::new();
    if let Some(remaining_expr) = split_expr(&sub_expr, &|_| true, &mut constraints) {
        constraints.push(remaining_expr);
    }

    let mut sub_expr_diff = None;
    for c in constraints.iter().enumerate().filter_map(|(idx, c)| {
        let c_kind = ConstraintKind::new(c);
        if !c_kind.is_contained_in(expr)
            && !constraints.iter().take(idx).any(|c1| c_kind.is_same_as(c1))
        {
            Some(c)
        } else {
            None
        }
    }) {
        sub_expr_diff = match sub_expr_diff {
            Some(existing_expr) => Some(and_expr(existing_expr, c.clone())),
            None => Some(c.clone()),
        };
    }

    sub_expr_diff
}

/// Combines 2 expressions with predicate AND.
pub(crate) fn and_expr(expr1: Expr, expr2: Expr) -> Expr {
    Expr::BinaryOp {
        lhs: Box::new(expr1),
        op: BinaryOperator::And,
        rhs: Box::new(expr2),
    }
}

/// Combines an optional accumulator expression with a new constraint using AND.
/// Adds only those pieces of `constraint` which are not already contained.
/// Returns a new composite predicate or just the new constraint if the accumulator is None.
pub(crate) fn and_predicates(acc_expr: Option<Expr>, constraint: Expr) -> Option<Expr> {
    if let Some(existing_expr) = acc_expr {
        if let Some(diff_constraint) = expr_difference(&existing_expr, constraint) {
            Some(and_expr(existing_expr, diff_constraint))
        } else {
            Some(existing_expr)
        }
    } else {
        Some(constraint)
    }
}

/// Split an AND-expression into predicates matching `predicate` and the remainder.
pub(crate) fn split_expr(
    expr: &Expr,
    predicate: &impl Fn(&Expr) -> bool,
    split: &mut Vec<Expr>,
) -> Option<Expr> {
    split_expr_mut(expr, &mut |expr| predicate(expr), split)
}

/// Mutable version of `split_expr`: move matching sub-predicates into `split`.
pub(crate) fn split_expr_mut(
    expr: &Expr,
    predicate: &mut impl FnMut(&Expr) -> bool,
    split: &mut Vec<Expr>,
) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            lhs,
            op: BinaryOperator::And,
            rhs,
        } => {
            let mut remaining_expr = split_expr_mut(lhs, predicate, split);
            if let Some(right_side_remaining_expr) = split_expr_mut(rhs, predicate, split) {
                remaining_expr = and_predicates(remaining_expr, right_side_remaining_expr);
            }
            remaining_expr
        }
        constraint if predicate(constraint) => {
            split.push(constraint.clone());
            None
        }
        _ => Some(expr.clone()),
    }
}

pub(crate) fn is_column_eq_column(
    expr: &Expr,
    mut predicate: impl FnMut(&Column, &Column) -> bool,
) -> bool {
    match expr {
        Expr::BinaryOp {
            lhs,
            op: BinaryOperator::Equal,
            rhs,
        } => match (lhs.as_ref(), rhs.as_ref()) {
            (Expr::Column(left_col), Expr::Column(right_col))
                if left_col.table.is_some() && right_col.table.is_some() =>
            {
                predicate(left_col, right_col)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Checks if an expression is an equality comparison between two columns
/// and satisfies a given predicate.
pub(crate) fn matches_eq_constraint(
    expr: &Expr,
    mut predicate: impl FnMut(&Relation, &Relation) -> bool,
) -> bool {
    is_column_eq_column(expr, |left_col, right_col| {
        match (&left_col.table, &right_col.table) {
            (Some(left_table), Some(right_table)) => predicate(left_table, right_table),
            _ => false,
        }
    })
}

/// Check if an expression is a simple column-based comparison for parameterization.
pub(crate) fn is_simple_parametrizable_filter(
    expr: &Expr,
    mut predicate: impl FnMut(&Relation, &SqlIdentifier) -> bool,
) -> bool {
    // Delegate to generic filter candidate checker.
    is_parametrizable_filter_candidate(expr, |expr| {
        if let Expr::Column(Column {
            table: Some(table),
            name,
        }) = expr
        {
            predicate(table, name)
        } else {
            false
        }
    })
}

/// Determine if an expression is a candidate for parameterizable filtering (e.g., =, <, >, BETWEEN, IN).
pub(crate) fn is_parametrizable_filter_candidate(
    expr: &Expr,
    mut predicate: impl FnMut(&Expr) -> bool,
) -> bool {
    match expr {
        // Handle equality or ordering comparisons
        Expr::BinaryOp { lhs, op, rhs }
            if matches!(op, BinaryOperator::Equal) || op.is_ordering_comparison() =>
        {
            match (lhs.as_ref(), rhs.as_ref()) {
                (operand, Expr::Literal(_)) | (Expr::Literal(_), operand)
                    if !contains_select(operand) =>
                {
                    predicate(operand)
                }
                _ => false,
            }
        }
        // Handle BETWEEN with literals
        Expr::Between {
            operand, min, max, ..
        } if !contains_select(operand.as_ref())
            && matches!(min.as_ref(), Expr::Literal(_))
            && matches!(max.as_ref(), Expr::Literal(_)) =>
        {
            predicate(operand.as_ref())
        }
        // Handle IN lists of literals
        Expr::In {
            lhs,
            rhs: InValue::List(values),
            ..
        } if !contains_select(lhs.as_ref())
            && values.iter().all(|v| matches!(v, Expr::Literal(_))) =>
        {
            predicate(lhs.as_ref())
        }
        _ => false,
    }
}

/// Classify a leaf `atom` taken from an AND-conjunction in `JOIN ... ON`.
/// Uses the same primitives as the splitter/checker.
///
/// Policy recap:
/// - Cross-table `Column = Column` → `CrossEq`
/// - Simple parametrizable filter over exactly **one** relation → `SingleRelFilter`
/// - Otherwise → `Other`
pub fn classify_on_atom(atom: &Expr) -> OnAtom {
    // 1) Cross-table equality: capture the pair while validating lt != rt
    let mut pair: Option<(Relation, Relation)> = None;
    if matches_eq_constraint(atom, |lt, rt| {
        if lt != rt {
            pair = Some((lt.clone(), rt.clone()));
            true
        } else {
            false
        }
    }) {
        let (lhs, rhs) = pair.expect("predicate must set pair for cross equality");
        return OnAtom::CrossEq { lhs, rhs };
    }

    // 2) Single-relation simple filter: capture the sole relation
    let mut rel: Option<Relation> = None;
    if is_simple_parametrizable_filter(atom, |t, _| {
        // `is_simple_parametrizable_filter` only exposes one column operand,
        // so this closure is called once; just capture it.
        rel = Some(t.clone());
        true
    }) && let Some(r) = rel
    {
        return OnAtom::SingleRelFilter { rel: r };
    }

    // 3) Anything else is unsupported
    OnAtom::Other
}

/// Decompose an expression into **pure AND** conjuncts (cloned).
/// Returns `None` if the expression is not a pure AND-conjunction.
pub fn decompose_conjuncts(expr: &Expr) -> Option<Vec<Expr>> {
    let mut atoms = Vec::new();
    // Accept every leaf; if `split_expr_mut` returns a remainder, it wasn't a pure AND.
    let remainder = split_expr_mut(expr, &mut |_e| true, &mut atoms);
    if remainder.is_some() {
        None
    } else {
        Some(atoms)
    }
}

/// Splits a predicate expression into a correlated part (references outer tables)
/// and a non-correlated part, based on `is_outer_rel` predicate
pub(crate) fn split_correlated_expression(
    expr: &Expr,
    is_outer_rel: &impl Fn(&Relation) -> bool,
) -> (Option<Expr>, Option<Expr>) {
    let mut correlated_constraints: Vec<Expr> = Vec::new();
    let remaining_expr = split_expr(
        expr,
        &|constraint| {
            matches_eq_constraint(constraint, |left_table, right_table| {
                is_outer_rel(left_table) || is_outer_rel(right_table)
            }) || is_simple_parametrizable_filter(constraint, |table, _| is_outer_rel(table))
        },
        &mut correlated_constraints,
    );

    let mut correlated_expr = None;
    for e in correlated_constraints {
        correlated_expr = and_predicates(correlated_expr, e);
    }

    (correlated_expr, remaining_expr)
}

/// Return true if the expression contains any subquery.
pub(crate) fn contains_select(expr: &Expr) -> bool {
    struct Vis {
        yes: bool,
    }
    impl<'a> Visitor<'a> for Vis {
        type Error = ReadySetError;
        fn visit_select_statement(&mut self, _: &'a SelectStatement) -> Result<(), Self::Error> {
            self.yes = true;
            Ok(())
        }
    }
    let mut vis = Vis { yes: false };
    let _ = vis.visit_expr(expr);
    vis.yes
}

/// Returns true if any subquery within the SELECT statement contains a LIMIT clause.
/// Used to detect cases where LATERAL join rewriting may be unsupported.
pub(crate) fn contain_subqueries_with_limit_clause(stmt: &SelectStatement) -> ReadySetResult<bool> {
    struct LookupVisitor {
        contains_limit_clause: bool,
    }

    impl<'ast> Visitor<'ast> for LookupVisitor {
        type Error = ReadySetError;

        fn visit_select_statement(
            &mut self,
            select_statement: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            if !select_statement.limit_clause.is_empty() {
                self.contains_limit_clause = true;
            }
            walk_select_statement(self, select_statement)
        }
    }

    let mut visitor = LookupVisitor {
        contains_limit_clause: false,
    };
    visitor.visit_select_statement(stmt)?;

    Ok(visitor.contains_limit_clause)
}

/// Return the **alias** of the first projected field if present; otherwise set it to a default,
/// update the statement in place, and return it.
pub fn ensure_first_field_alias(stmt: &mut SelectStatement) -> SqlIdentifier {
    let (expr, alias) = match stmt.fields.first_mut() {
        Some(FieldDefinitionExpr::Expr { expr, alias }) => (expr, alias),
        _ => panic!(
            "Expected first select field to be an expression in:\n{}",
            stmt.display(Dialect::PostgreSQL)
        ),
    };
    if alias.is_none() {
        *alias = Some(default_alias_for_select_item_expression(expr));
    }
    alias.as_ref().unwrap().clone()
}

/// Unwrap a select field into its expression and optional alias.
pub(crate) fn expect_field_as_expr(fde: &FieldDefinitionExpr) -> (&Expr, &Option<SqlIdentifier>) {
    match fde {
        FieldDefinitionExpr::Expr { expr, alias } => (expr, alias),
        _ => unreachable!("Expected field definition expression"),
    }
}

/// Mutable version: unwrap a select field into expr and alias.
pub(crate) fn expect_field_as_expr_mut(
    fde: &mut FieldDefinitionExpr,
) -> (&mut Expr, &mut Option<SqlIdentifier>) {
    match fde {
        FieldDefinitionExpr::Expr { expr, alias } => (expr, alias),
        _ => unreachable!("Expected field definition expression"),
    }
}

/// Return mutable (subquery, alias) if a FROM item is an aliased subquery.
pub(crate) fn as_sub_query_with_alias_mut(
    tab_expr: &mut TableExpr,
) -> Option<(&mut SelectStatement, SqlIdentifier)> {
    if let TableExpr {
        inner: TableExprInner::Subquery(sq),
        alias: Some(stmt_alias),
        ..
    } = tab_expr
    {
        Some((sq.as_mut(), stmt_alias.clone()))
    } else {
        None
    }
}

/// Return immutable (subquery, alias) if a FROM item is an aliased subquery.
pub(crate) fn as_sub_query_with_alias(
    tab_expr: &TableExpr,
) -> Option<(&SelectStatement, SqlIdentifier)> {
    if let TableExpr {
        inner: TableExprInner::Subquery(sq),
        alias: Some(stmt_alias),
        ..
    } = tab_expr
    {
        Some((sq.as_ref(), stmt_alias.clone()))
    } else {
        None
    }
}

/// Mutable unwrap of an aliased subquery, panic if missing.
pub(crate) fn expect_sub_query_with_alias_mut(
    tab_expr: &mut TableExpr,
) -> (&mut SelectStatement, SqlIdentifier) {
    as_sub_query_with_alias_mut(tab_expr).expect("Expected a subquery with alias")
}

/// Immutable unwrap of an aliased subquery, panic if missing.
pub(crate) fn expect_sub_query_with_alias(
    tab_expr: &TableExpr,
) -> (&SelectStatement, SqlIdentifier) {
    as_sub_query_with_alias(tab_expr).expect("Expected a subquery with alias")
}

/// Locate a grouping key in GROUP BY by alias, expression, or position.
pub(crate) fn find_group_by_key(
    fields: &[FieldDefinitionExpr],
    group_by: &[FieldReference],
    key: &Expr,
    key_alias: &SqlIdentifier,
) -> ReadySetResult<Option<usize>> {
    for (pos, g) in group_by.iter().enumerate() {
        match g {
            // `g` is a select item's alias, so match by the alias
            FieldReference::Expr(Expr::Column(alias))
                if alias.table.is_none() && alias.name.eq(key_alias) =>
            {
                return Ok(Some(pos));
            }
            // `g` is an expression, so match by the expression
            FieldReference::Expr(expr) if expr.eq(key) => return Ok(Some(pos)),
            // `g` is a select item's 1-based index, sp match by the expression
            FieldReference::Numeric(proj_idx) => {
                if *proj_idx < 1 || *proj_idx > fields.len() as u64 {
                    return Err(invalid_query_err!(
                        "GROUP BY position {} is not in select list",
                        *proj_idx
                    ));
                }
                let (expr, _) = expect_field_as_expr(&fields[(*proj_idx - 1) as usize]);
                if key.eq(expr) {
                    return Ok(Some(pos));
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Add a grouping expression if it's not already in GROUP BY.
pub(crate) fn add_group_by_key_if_not_exists(
    fields: &[FieldDefinitionExpr],
    group_by: &mut Vec<FieldReference>,
    add_key: &Expr,
    key_alias: &SqlIdentifier,
) -> ReadySetResult<()> {
    if find_group_by_key(fields, group_by, add_key, key_alias)?.is_none() {
        // Append a new grouping key based on select list position.
        let proj_idx = fields
            .iter()
            .position(|fe| get_select_item_alias(fe).eq(key_alias))
            .ok_or(internal_err!("Grouping key not found in select list"))?;
        let (expr, _) = expect_field_as_expr(&fields[proj_idx]);
        group_by.push(FieldReference::Expr(expr.clone()));
    }
    Ok(())
}

/// Generate a default alias for an expression (column name or truncated expr text).
pub(crate) fn default_alias_for_select_item_expression(proj_col: &Expr) -> SqlIdentifier {
    match proj_col {
        // Use column name directly
        Expr::Column(c) => c.name.clone(),
        // Derive alias from expression string.
        expr => {
            let dialect = Dialect::PostgreSQL;
            if let Some(alias) = expr.alias(dialect) {
                alias
            } else {
                expr.display(dialect)
                    .to_string()
                    .chars()
                    .filter_map(|c| {
                        if c == '"' {
                            None
                        } else {
                            Some(if c == '.' || c.is_whitespace() {
                                '_'
                            } else {
                                c
                            })
                        }
                    })
                    .take(64)
                    .collect::<String>()
                    .into()
            }
        }
    }
}

/// Return explicit alias or derive a default for an expression.
pub(crate) fn alias_for_expr(expr: &Expr, may_be_alias: &Option<SqlIdentifier>) -> SqlIdentifier {
    if let Some(alias) = may_be_alias {
        alias.clone()
    } else {
        default_alias_for_select_item_expression(expr)
    }
}

/// Get or generate an alias for a select field.
pub(crate) fn get_select_item_alias(fe: &FieldDefinitionExpr) -> SqlIdentifier {
    let (expr, maybe_alias) = expect_field_as_expr(fe);
    alias_for_expr(expr, maybe_alias)
}

fn inc_alias(alias: &SqlIdentifier, inc_val: usize) -> SqlIdentifier {
    let mut s = alias.to_string();
    s.push_str(inc_val.to_string().as_str());
    s.into()
}

/// Ensure unique select aliases by appending numeric suffixes on duplicates.
pub(crate) fn fix_duplicate_aliases(fields: &mut [FieldDefinitionExpr]) -> Vec<usize> {
    let mut fixed_fields_indexes = Vec::new();
    for idx in 0..fields.len() {
        let fe_alias = get_select_item_alias(&fields[idx]);
        let mut dup_fields_indexes = Vec::new();
        fields
            .iter_mut()
            .skip(idx + 1)
            .enumerate()
            .for_each(|(i, fe_inner)| {
                if fe_alias.eq(&get_select_item_alias(fe_inner)) {
                    dup_fields_indexes.push(idx + 1 + i);
                }
            });
        for (suffix, dup_idx) in dup_fields_indexes.into_iter().enumerate() {
            let (expr, alias) = expect_field_as_expr_mut(&mut fields[dup_idx]);
            if let Some(alias) = alias {
                *alias = inc_alias(alias, suffix);
            } else {
                *alias = Some(inc_alias(
                    &default_alias_for_select_item_expression(expr),
                    suffix,
                ));
            }
            fixed_fields_indexes.push(dup_idx);
        }
    }
    fixed_fields_indexes
}

/// Add a projection for `expr` if missing, return its index and alias.
pub(crate) fn project_column_if_not_exists(
    fields: &mut Vec<FieldDefinitionExpr>,
    proj_item: &Expr,
) -> (usize, SqlIdentifier) {
    for (idx, fe) in fields.iter().enumerate() {
        let (expr, alias) = expect_field_as_expr(fe);
        if proj_item.eq(expr) {
            return (idx, alias_for_expr(proj_item, alias));
        }
    }
    let proj_alias = default_alias_for_select_item_expression(proj_item);
    fields.push(FieldDefinitionExpr::Expr {
        expr: proj_item.clone(),
        alias: Some(proj_alias.clone()),
    });
    (fields.len() - 1, proj_alias)
}

/// Project multiple expressions and fix any resulting alias duplicates.
pub(crate) fn project_columns_if_not_exist_fix_duplicate_aliases(
    stmt: &mut SelectStatement,
    proj_items: &[Expr],
) -> Vec<(usize, SqlIdentifier)> {
    // Add `proj_cols` to the select list and collect select item's aliases
    let mut proj_aliases = proj_items
        .iter()
        .map(|c| project_column_if_not_exists(&mut stmt.fields, c))
        .collect::<Vec<_>>();

    // Resolve duplicate aliases if there are any, and update the projected aliases if they have changed
    fix_duplicate_aliases(&mut stmt.fields)
        .into_iter()
        .for_each(|idx| {
            for (proj_alias_idx, proj_alias) in proj_aliases.iter_mut() {
                if *proj_alias_idx == idx {
                    *proj_alias = get_select_item_alias(&stmt.fields[idx]);
                }
            }
        });

    proj_aliases
}

/// Ensure expressions are selected, fix aliases, and update GROUP BY if needed.
pub(crate) fn project_columns(
    stmt: &mut SelectStatement,
    proj_items: &[Expr],
) -> ReadySetResult<Vec<SqlIdentifier>> {
    // Add `proj_cols` to the select list and collect select item's aliases,
    // resolve duplicate aliases if there are any, and update the projected aliases if they have changed
    let proj_aliases = project_columns_if_not_exist_fix_duplicate_aliases(stmt, proj_items);

    // Extend the GROUP BY with the new items, we've just added to the select list
    if is_aggregated_select(stmt)? || stmt.group_by.is_some() {
        if stmt.group_by.is_none() {
            stmt.group_by = Some(GroupByClause {
                fields: Vec::with_capacity(proj_items.len()),
            });
        }
        let group_by = stmt.group_by.as_mut().unwrap();
        for (col, (_, alias)) in proj_items.iter().zip(&proj_aliases) {
            add_group_by_key_if_not_exists(&stmt.fields, &mut group_by.fields, col, alias)?;
        }
    }

    Ok(proj_aliases.into_iter().map(|(_, alias)| alias).collect())
}

pub(crate) fn project_columns_if(
    tab_expr: &mut TableExpr,
    expr: &mut Expr,
    f: impl Fn(&Column) -> bool,
) -> ReadySetResult<()> {
    let (tab_expr_stmt, tab_expr_alias) = expect_sub_query_with_alias_mut(tab_expr);
    project_statement_columns_if(tab_expr_stmt, tab_expr_alias, expr, f)
}

pub(crate) fn project_statement_columns_if(
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    expr: &mut Expr,
    f: impl Fn(&Column) -> bool,
) -> ReadySetResult<()> {
    let the_columns_refs = columns_iter_mut(expr).filter(|c| f(c)).collect::<Vec<_>>();

    let projected_columns_alias = project_columns(
        stmt,
        &the_columns_refs
            .iter()
            .map(|col| Expr::Column((*col).clone()))
            .collect::<Vec<_>>(),
    )?;

    the_columns_refs
        .into_iter()
        .zip(projected_columns_alias)
        .for_each(|(col_ref, proj)| {
            col_ref.table = Some(stmt_alias.clone().into());
            col_ref.name = proj;
        });

    Ok(())
}

/// Find the join clause and position for a given FROM item index.
pub(crate) fn find_rhs_join_clause(
    stmt: &SelectStatement,
    from_item_idx: usize,
) -> Option<(usize, usize)> {
    let mut rhs_beg_idx = stmt.tables.len();
    for (join_clause_idx, join_clause) in stmt.join.iter().enumerate() {
        let items_number = match &join_clause.right {
            JoinRightSide::Table(_) => 1,
            JoinRightSide::Tables(tables) => tables.len(),
        };
        if from_item_idx >= rhs_beg_idx && from_item_idx < rhs_beg_idx + items_number {
            return Some((join_clause_idx, from_item_idx - rhs_beg_idx));
        }
        rhs_beg_idx += items_number;
    }
    None
}

/// Check if filters can be safely pushed for the JOIN clause at `join_clause_idx`,
/// i.e., the current join and all joins to its right are INNER.
pub(crate) fn is_filter_pushable_from_join_clause(
    stmt: &SelectStatement,
    join_clause_idx: usize,
) -> bool {
    stmt.join
        .iter()
        .skip(join_clause_idx)
        .all(|jc| jc.operator.is_inner_join())
}

/// Check if filters can be safely pushed for the FROM item based on join types.
pub(crate) fn is_filter_pushable_from_item(
    stmt: &SelectStatement,
    from_item_idx: usize,
) -> ReadySetResult<bool> {
    let join_clause_idx = if from_item_idx < stmt.tables.len() {
        0
    } else {
        let Some((join_clause_idx, _)) = find_rhs_join_clause(stmt, from_item_idx) else {
            internal!("FROM item index outside of range: {from_item_idx}")
        };
        join_clause_idx
    };
    Ok(is_filter_pushable_from_join_clause(stmt, join_clause_idx))
}

/// Add a new predicate to a join’s ON constraint, combining with AND or creating a new ON.
pub(crate) fn add_expression_to_join_constraint(
    join_constraint: JoinConstraint,
    expr: Expr,
) -> JoinConstraint {
    if let Some(expr) = match join_constraint {
        JoinConstraint::On(existing_expr) => and_predicates_skip_true(Some(existing_expr), expr),
        JoinConstraint::Empty => and_predicates_skip_true(None, expr),
        JoinConstraint::Using(_) => {
            unreachable!("USING should have been rewritten earlier")
        }
    } {
        JoinConstraint::On(expr)
    } else {
        JoinConstraint::Empty
    }
}

/// Collect all top-level expressions (SELECT items, JOIN ON, WHERE, HAVING, GROUP BY,
/// ORDER BY) for immutable analysis.
pub(crate) fn outermost_expression(stmt: &SelectStatement) -> impl Iterator<Item = &Expr> {
    stmt.fields
        .iter()
        .filter_map(|fde| match fde {
            FieldDefinitionExpr::Expr { expr, .. } => Some(expr),
            FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => None,
        })
        .chain(stmt.join.iter().filter_map(|join| match &join.constraint {
            JoinConstraint::On(expr) => Some(expr),
            JoinConstraint::Using(_) => None,
            JoinConstraint::Empty => None,
        }))
        .chain(&stmt.where_clause)
        .chain(&stmt.having)
        .chain(stmt.group_by.iter().flat_map(|gb| {
            gb.fields.iter().filter_map(|f| match f {
                FieldReference::Expr(expr) => Some(expr),
                _ => None,
            })
        }))
        .chain(stmt.order.iter().flat_map(|oc| {
            oc.order_by
                .iter()
                .filter_map(|OrderBy { field, .. }| match field {
                    FieldReference::Expr(expr) => Some(expr),
                    _ => None,
                })
        }))
}

/// Collect all top-level expressions (SELECT items, JOIN ON, WHERE, HAVING, GROUP BY,
/// ORDER BY) for mutation/analysis.
pub(crate) fn outermost_expression_mut(
    stmt: &mut SelectStatement,
) -> impl Iterator<Item = &mut Expr> {
    stmt.fields
        .iter_mut()
        .filter_map(|fde| match fde {
            FieldDefinitionExpr::Expr { expr, .. } => Some(expr),
            FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => None,
        })
        .chain(
            stmt.join
                .iter_mut()
                .filter_map(|join| match &mut join.constraint {
                    JoinConstraint::On(expr) => Some(expr),
                    JoinConstraint::Using(_) => None,
                    JoinConstraint::Empty => None,
                }),
        )
        .chain(&mut stmt.where_clause)
        .chain(&mut stmt.having)
        .chain(stmt.group_by.iter_mut().flat_map(|gb| {
            gb.fields.iter_mut().filter_map(|f| match f {
                FieldReference::Expr(expr) => Some(expr),
                _ => None,
            })
        }))
        .chain(stmt.order.iter_mut().flat_map(|oc| {
            oc.order_by
                .iter_mut()
                .filter_map(|OrderBy { field, .. }| match field {
                    FieldReference::Expr(expr) => Some(expr),
                    _ => None,
                })
        }))
}

/// Gather those as a flat `Vec<&mut Expr::Column(column)>` so we can inspect or replace columns.
pub(crate) fn collect_outermost_columns_mut(
    stmt: &mut SelectStatement,
) -> ReadySetResult<Vec<&mut Expr>> {
    struct TheVisitor<'a> {
        expr_columns: Vec<&'a mut Expr>,
    }

    impl<'a> VisitorMut<'a> for TheVisitor<'a> {
        type Error = ReadySetError;
        fn visit_expr(&mut self, expr: &'a mut Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::Column(_)) {
                self.expr_columns.push(expr);
            } else {
                walk_expr(self, expr)?;
            }
            Ok(())
        }
        fn visit_select_statement(
            &mut self,
            _: &'a mut SelectStatement,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let mut visitor = TheVisitor {
        expr_columns: Vec::new(),
    };

    for expr in outermost_expression_mut(stmt) {
        visitor.visit_expr(expr)?;
    }

    Ok(visitor.expr_columns)
}

pub(crate) fn for_each_aggregate<'a>(
    expr: &'a Expr,
    visit_window_functions: bool,
    func_visitor: &'a mut impl FnMut(&FunctionExpr),
) -> ReadySetResult<()> {
    struct ForEachVisitor<'a> {
        func_visitor: &'a mut dyn FnMut(&FunctionExpr),
        visit_window_functions: bool,
    }

    impl<'ast> Visitor<'ast> for ForEachVisitor<'ast> {
        type Error = ReadySetError;

        fn visit_function_expr(
            &mut self,
            function_expr: &'ast FunctionExpr,
        ) -> Result<(), Self::Error> {
            if is_aggregate(function_expr) {
                (self.func_visitor)(function_expr);
            }
            walk_function_expr(self, function_expr)
        }

        fn visit_expr(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
            if !self.visit_window_functions && matches!(expr, Expr::WindowFunction { .. }) {
                // Skip window function
                Ok(())
            } else {
                visit::walk_expr(self, expr)
            }
        }

        fn visit_select_statement(&mut self, _: &'ast SelectStatement) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    ForEachVisitor {
        func_visitor,
        visit_window_functions,
    }
    .visit_expr(expr)
}

pub(crate) fn for_each_window_function<'a>(
    expr: &'a Expr,
    window_func_visitor: &'a mut impl FnMut(&'a Expr),
) -> ReadySetResult<()> {
    struct ForEachVisitor<'a> {
        window_func_visitor: &'a mut dyn FnMut(&'a Expr),
    }

    impl<'ast> Visitor<'ast> for ForEachVisitor<'ast> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::WindowFunction { .. }) {
                (self.window_func_visitor)(expr);
                Ok(())
            } else {
                visit::walk_expr(self, expr)
            }
        }

        fn visit_select_statement(&mut self, _: &'ast SelectStatement) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    ForEachVisitor {
        window_func_visitor,
    }
    .visit_expr(expr)
}

pub(crate) fn for_each_window_function_mut<'a>(
    expr: &'a mut Expr,
    window_func_visitor: &'a mut impl FnMut(&'a mut Expr),
) -> ReadySetResult<()> {
    struct ForEachVisitor<'a> {
        window_func_visitor: &'a mut dyn FnMut(&'a mut Expr),
    }

    impl<'ast> VisitorMut<'ast> for ForEachVisitor<'ast> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::WindowFunction { .. }) {
                (self.window_func_visitor)(expr);
                Ok(())
            } else {
                walk_expr(self, expr)
            }
        }

        fn visit_select_statement(
            &mut self,
            _: &'ast mut SelectStatement,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    ForEachVisitor {
        window_func_visitor,
    }
    .visit_expr(expr)
}

pub(crate) fn is_aggregated_expr(expr: &Expr) -> ReadySetResult<bool> {
    let mut has_aggregates = false;
    for_each_aggregate(expr, false, &mut |_| has_aggregates = true)?;
    Ok(has_aggregates)
}

pub(crate) fn is_aggregated_select(stmt: &SelectStatement) -> ReadySetResult<bool> {
    for fe in stmt.fields.iter() {
        let (expr, _) = expect_field_as_expr(fe);
        if is_aggregated_expr(expr)? {
            return Ok(true);
        }
    }
    if let Some(having_expr) = &stmt.having
        && is_aggregated_expr(having_expr)?
    {
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn is_aggregate_only_without_group_by(stmt: &SelectStatement) -> ReadySetResult<bool> {
    Ok(is_aggregated_select(stmt)? && stmt.group_by.is_none())
}

fn calculate_aggregate_only_expression_for_empty_data_set(mut expr: Expr) -> ReadySetResult<Expr> {
    struct ForEachVisitor {}

    impl<'ast> VisitorMut<'ast> for ForEachVisitor {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            match expr {
                Expr::WindowFunction { .. } => {
                    // Skip window function
                    Ok(())
                }
                Expr::Call(func) if is_aggregate(func) => {
                    // Replace `count()` with `0`, other aggregates with `NULL`
                    let _ = mem::replace(
                        expr,
                        Expr::Literal(
                            if matches!(
                                expr,
                                Expr::Call(FunctionExpr::Count { .. } | FunctionExpr::CountStar)
                            ) {
                                Literal::Integer(0)
                            } else {
                                Literal::Null
                            },
                        ),
                    );
                    Ok(())
                }
                _ => {
                    // Walk down into expression
                    walk_expr(self, expr)
                }
            }
        }

        fn visit_select_statement(
            &mut self,
            _: &'ast mut SelectStatement,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    ForEachVisitor {}.visit_expr(&mut expr)?;

    constant_fold_expr(&mut expr, dialect::Dialect::DEFAULT_POSTGRESQL);

    Ok(expr)
}

/// For aggregate-only subqueries without GROUP BY, analyzes each SELECT field and
/// populates `fields_map` so that COUNT and literal fields are coalesced to default values.
///
/// # Arguments
/// * `stmt` - The aggregate-only subquery SELECT statement.
/// * `stmt_alias` - The alias used for the subquery, used to qualify projected columns.
/// * `fields_map` - A map to be populated with columns and their coalesced expressions or errors.
///
/// For pure COUNT aggregate expressions, insert COALESCE (col, <literal>).
/// For literal fields, insert COALESCE (col, literal).
pub(crate) fn analyse_lone_aggregates_subquery_fields(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
    fields_map: &mut HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    for fe in &stmt.fields {
        let (f_expr, f_alias) = expect_field_as_expr(fe);
        if let Some(f_alias) = f_alias {
            match calculate_aggregate_only_expression_for_empty_data_set(f_expr.clone())? {
                Expr::Literal(lit) if !matches!(lit, Literal::Null) => {
                    let f_col = Column {
                        name: f_alias.clone(),
                        table: Some(stmt_alias.clone().into()),
                    };
                    fields_map.insert(
                        f_col.clone(),
                        Ok(Expr::Call(FunctionExpr::Call {
                            name: "coalesce".into(),
                            arguments: Some(vec![Expr::Column(f_col), Expr::Literal(lit)]),
                        })),
                    );
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// **IMPORTANT**: this function is safe to call only if structural verification for `current_stmt` returned ExactlyOne.
/// See: `unnest_subqueries::agg_only_no_gby_cardinality()`
///
/// Extracts the fallback value for aggregate expressions when operating on empty result sets.
///
/// Analyzes an expression in a SELECT that wraps a subquery containing aggregates, attempting
/// to determine what constant value the expression evaluates to if the aggregate operated on
/// zero rows (e.g., `COUNT(*) + 5` → `5`, `COALESCE(SUM(x), 0)` → `0`).
///
/// Returns `Some(literal)` if the expression simplifies to a constant, `None` otherwise.
pub(crate) fn extract_aggregate_fallback_for_expr(
    expr: &Expr,
    current_stmt: &SelectStatement,
) -> ReadySetResult<Option<Expr>> {
    // If this SELECT is a single projecting wrapper over a subquery, get its inner.
    let inner = expect_only_subquery_from_with_alias(current_stmt).ok();

    // Helper to check if a column belongs to the inner alias.
    let mut inner_rel: Option<Relation> = None;
    let mut inner_stmt_opt: Option<&SelectStatement> = None;
    let mut inner_alias_opt: Option<SqlIdentifier> = None;
    if let Some((inner_stmt, inner_alias)) = inner {
        inner_rel = Some(inner_alias.clone().into());
        inner_stmt_opt = Some(inner_stmt);
        inner_alias_opt = Some(inner_alias);
    }

    // Build a map from inner subquery columns (that contain aggregates) to their fallback values.
    // For example, if the inner projects `COALESCE(COUNT(*), 0)`, we extract the `0` as the fallback.
    let inner_zero_map: HashMap<Column, Expr> = if let (Some(inner_stmt), Some(inner_alias)) =
        (inner_stmt_opt, inner_alias_opt.as_ref())
    {
        let mut map = HashMap::new();
        analyse_lone_aggregates_subquery_fields(inner_stmt, inner_alias.clone(), &mut map)?;

        let inner_rel_for_extract: Relation = inner_alias.clone().into();
        map.into_iter().filter_map(|(k, v)| match v {
            Ok(mapped_expr) => {
                if let Expr::Call(FunctionExpr::Call {
                                      name,
                                      arguments: Some(args),
                                  }) = mapped_expr
                    && name.as_str().eq_ignore_ascii_case("coalesce")
                    && args.len() == 2
                    && matches!(&args[0], Expr::Column(c0) if c0.name == k.name && is_column_of!(c0, inner_rel_for_extract))
                {
                    return Some((k, args[1].clone()));
                }
                None
            }
            Err(_) => None,
        }).collect()
    } else {
        return Ok(None);
    };

    struct ForEachVisitor<'ast> {
        stmt: Option<&'ast SelectStatement>,
        stmt_rel: &'ast Option<Relation>,
        fallback_map: &'ast HashMap<Column, Expr>,
    }

    impl<'ast> VisitorMut<'ast> for ForEachVisitor<'ast> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> ReadySetResult<()> {
            if let Expr::Column(column) = expr
                && let Some(rel) = self.stmt_rel.as_ref()
                && column.table.as_ref() == Some(rel)
            {
                if let Some(zero_expr) = self.fallback_map.get(column) {
                    let _ = mem::replace(expr, zero_expr.clone());
                } else if let Some(inner_stmt) = self.stmt
                    && let Some(inner_expr) = resolve_field_expr_by_alias(inner_stmt, &column.name)
                    && let Ok(Some(zero_expr)) =
                        extract_aggregate_fallback_for_expr(inner_expr, inner_stmt)
                {
                    let _ = mem::replace(expr, zero_expr);
                }
                Ok(())
            } else {
                walk_expr(self, expr)
            }
        }

        fn visit_select_statement(&mut self, _: &'ast mut SelectStatement) -> ReadySetResult<()> {
            Ok(())
        }
    }

    let mut expr = expr.clone();
    ForEachVisitor {
        stmt: inner_stmt_opt,
        stmt_rel: &inner_rel,
        fallback_map: &inner_zero_map,
    }
    .visit_expr(&mut expr)?;

    constant_fold_expr(&mut expr, dialect::Dialect::DEFAULT_POSTGRESQL);

    Ok(if matches!(expr, Expr::Literal(_)) {
        Some(expr)
    } else {
        None
    })
}

pub(crate) fn resolve_field_expr_by_alias<'a>(
    stmt: &'a SelectStatement,
    alias_name: &SqlIdentifier,
) -> Option<&'a Expr> {
    // Match the projected field by its explicit alias when present;
    // otherwise, compute the *default* alias for the expression and match on that.
    // This keeps consistency with `make_first_field_ref_name` /
    // `as_joinable_derived_table_with_opts`, which assign the same default later.
    stmt.fields.iter().find_map(|f| match f {
        FieldDefinitionExpr::Expr { expr, alias } => match alias {
            Some(a) if a == alias_name => Some(expr),
            None if default_alias_for_select_item_expression(expr) == alias_name => Some(expr),
            _ => None,
        },
        _ => None,
    })
}

pub(crate) fn construct_is_not_null_expr(rhs: Expr, negated: bool) -> Expr {
    Expr::BinaryOp {
        lhs: Box::new(rhs),
        op: if negated {
            BinaryOperator::Is
        } else {
            BinaryOperator::IsNot
        },
        rhs: Box::new(Expr::Literal(Literal::Null)),
    }
}

pub(crate) fn construct_scalar_expr(lhs: Expr, op: BinaryOperator, rhs: Expr) -> Expr {
    Expr::BinaryOp {
        lhs: Box::new(lhs),
        op,
        rhs: Box::new(rhs),
    }
}

pub(crate) fn get_unique_select_item_alias(
    fields: &[FieldDefinitionExpr],
    base: &str,
) -> SqlIdentifier {
    let base: SqlIdentifier = base.into();
    let mut unique_alias = base.clone();
    let mut inc_val = 0;
    while fields
        .iter()
        .any(|field| get_select_item_alias(field).eq(&unique_alias))
    {
        unique_alias = inc_alias(&base, inc_val);
        inc_val += 1;
    }
    unique_alias
}

pub(crate) fn get_unique_alias(from_items: &HashSet<Relation>, base: &str) -> SqlIdentifier {
    let base: SqlIdentifier = base.into();
    let mut unique_alias = base.clone();
    let mut inc_val = 1;
    while from_items.iter().any(|rel| rel.name.eq(&unique_alias)) {
        unique_alias = inc_alias(&base, inc_val);
        inc_val += 1;
    }
    unique_alias
}

pub(crate) fn collect_local_from_items(
    stmt: &SelectStatement,
) -> ReadySetResult<HashSet<Relation>> {
    get_local_from_items_iter!(stmt)
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<HashSet<_>>>()
}

pub(crate) fn make_first_field_ref_name(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
) -> ReadySetResult<Expr> {
    if let Some((field_expr, field_alias)) = stmt.fields.first().map(expect_field_as_expr) {
        Ok(Expr::Column(Column {
            name: field_alias
                .clone()
                .unwrap_or_else(|| default_alias_for_select_item_expression(field_expr)),
            table: Some(stmt_alias.into()),
        }))
    } else {
        internal!("Subquery has empty select fields, or the first field not aliased")
    }
}

pub(crate) fn move_correlated_constraints_from_join_to_where(
    stmt: &mut SelectStatement,
    is_outer_rel: &impl Fn(&Relation) -> bool,
) -> ReadySetResult<()> {
    let mut add_to_where_clause = None;
    let mut correlated_join_clauses = Vec::new();

    for (join_clause_idx, join_clause) in stmt.join.iter().enumerate() {
        match &join_clause.constraint {
            // Safe to move ON → WHERE when the **current** join is INNER.
            // The moved correlated atoms reference only the current RHS and/or
            // preceding LHS (ON cannot see future tables), i.e., the left input
            // of all later joins; selections over left input commute with later LEFT joins.
            // Never move out of a LEFT join (would null-reject).
            JoinConstraint::On(on_expr) if join_clause.operator.is_inner_join() => {
                if let (Some(correlated_expr), remaining_expr) =
                    split_correlated_expression(on_expr, is_outer_rel)
                {
                    add_to_where_clause =
                        and_predicates_skip_true(add_to_where_clause, correlated_expr);
                    correlated_join_clauses.push((join_clause_idx, remaining_expr));
                }
            }
            _ => {}
        }
    }

    for (join_clause_idx, remaining_expr) in correlated_join_clauses {
        let rem = remaining_expr.and_then(|e| remove_literals_true(&e));
        stmt.join[join_clause_idx].constraint = if let Some(e) = rem {
            JoinConstraint::On(e)
        } else {
            JoinConstraint::Empty
        };
    }

    if let Some(add_to_where_clause) = add_to_where_clause {
        stmt.where_clause =
            and_predicates_skip_true(stmt.where_clause.clone(), add_to_where_clause);
    }

    Ok(())
}

pub(crate) fn split_correlated_constraint(
    expr: &Expr,
    local_from_items: &HashSet<Relation>,
) -> ReadySetResult<HashSet<(Column, Column)>> {
    struct EqConstraintsVisitor<'a> {
        cols_set: HashSet<(Column, Column)>, // (local_column : correlated_column)
        local_from_items: &'a HashSet<Relation>, // local FROM items
    }

    impl<'a> Visitor<'a> for EqConstraintsVisitor<'a> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'a Expr) -> Result<(), Self::Error> {
            if is_column_eq_column(expr, |lhs_col, rhs_col| {
                match (&lhs_col.table, &rhs_col.table) {
                    (Some(lhs_tab), Some(_)) => {
                        let (local_col, correlated_col) = if self.local_from_items.contains(lhs_tab)
                        {
                            (lhs_col, rhs_col)
                        } else {
                            (rhs_col, lhs_col)
                        };
                        self.cols_set
                            .insert((local_col.clone(), correlated_col.clone()));
                        true
                    }
                    _ => false,
                }
            }) {
                return Ok(());
            }
            visit::walk_expr(self, expr)
        }
    }

    let mut visitor = EqConstraintsVisitor {
        cols_set: HashSet::new(),
        local_from_items,
    };
    visitor.visit_expr(expr)?;

    Ok(visitor.cols_set)
}

pub(crate) fn expect_only_subquery_from_with_alias_mut(
    stmt: &mut SelectStatement,
) -> ReadySetResult<(&mut SelectStatement, SqlIdentifier)> {
    if !is_single_from_item!(stmt) {
        internal!("Expected a single FROM item in shaped probe");
    }
    as_sub_query_with_alias_mut(&mut stmt.tables[0]).ok_or(internal_err!(
        "Expected FROM to be a subquery with alias in shaped probe"
    ))
}

pub(crate) fn expect_only_subquery_from_with_alias(
    stmt: &SelectStatement,
) -> ReadySetResult<(&SelectStatement, SqlIdentifier)> {
    if !is_single_from_item!(stmt) {
        internal!("Expected a single FROM item in shaped probe");
    }
    as_sub_query_with_alias(&stmt.tables[0]).ok_or(internal_err!(
        "Expected FROM to be a subquery with alias in shaped probe"
    ))
}

pub(crate) fn construct_projecting_wrapper(
    mut derived_table: TableExpr,
) -> ReadySetResult<TableExpr> {
    let (stmt, stmt_alias) = expect_sub_query_with_alias_mut(&mut derived_table);

    for fe in stmt.fields.iter_mut() {
        let (fe_expr, fe_alias) = expect_field_as_expr_mut(fe);
        if fe_alias.is_none() {
            *fe_alias = Some(default_alias_for_select_item_expression(fe_expr));
        }
    }
    fix_duplicate_aliases(&mut stmt.fields);

    Ok(TableExpr {
        inner: TableExprInner::Subquery(Box::new(SelectStatement {
            fields: stmt
                .fields
                .iter()
                .map(|fe| {
                    let (_, fe_alias) = expect_field_as_expr(fe);
                    FieldDefinitionExpr::Expr {
                        expr: Expr::Column(Column {
                            name: fe_alias.clone().expect("Already checked"),
                            table: Some(stmt_alias.clone().into()),
                        }),
                        alias: fe_alias.clone(),
                    }
                })
                .collect(),
            tables: vec![derived_table],
            ..Default::default()
        })),
        alias: Some(stmt_alias),
    })
}

pub(crate) fn get_first_field_expr(stmt: &SelectStatement) -> ReadySetResult<&Expr> {
    if let Some((first_expr, _)) = stmt.fields.first().map(expect_field_as_expr) {
        Ok(first_expr)
    } else {
        invalid_query!("Subquery has empty select fields")
    }
}

/// Adds (or reuses) a ROW_NUMBER() select item and determines whether an
/// **inner wrapper** is required due to ORDER BY referencing aggregates/WFs.
///
/// Returns `(rn_alias, rn_user_defined, inner_wrapper_inserted)`.
/// - `inner_wrapper_inserted == true` when ORDER BY could not be applied
///   directly (agg/WF in ORDER BY), so a projecting wrapper was inserted.
///   This does **not** imply double wrapping by itself; double wrapping
///   occurs once `rewrite_top_k_in_place_impl` subsequently adds the outer wrapper.
fn project_row_number_field(
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
) -> ReadySetResult<(SqlIdentifier, bool, bool)> {
    //
    macro_rules! is_window_function_expr {
        ($expr:expr) => {{
            let mut contains_window_functions = false;
            for_each_window_function($expr, &mut |_| contains_window_functions = true)?;
            contains_window_functions
        }};
    }
    let mut require_wrapper = false;
    let mut rn_order_by = if let Some(order_by_clause) = &stmt.order {
        let mut rn_order_by = Vec::new();
        for o in order_by_clause.order_by.iter() {
            let FieldReference::Expr(expr) = &o.field else {
                internal!("Numerical ORDER BY references should have already been resolved")
            };
            if let Some(order_by_expr) = match expr {
                Expr::Column(maybe_alias) if maybe_alias.table.is_none() => {
                    stmt.fields.iter().find_map(|fe| {
                        if let FieldDefinitionExpr::Expr {
                            expr,
                            alias: Some(fe_alias),
                        } = fe
                            && fe_alias.eq(&maybe_alias.name)
                        {
                            return Some(expr);
                        }
                        None
                    })
                }
                order_by_expr => Some(order_by_expr),
            } {
                if is_aggregated_expr(order_by_expr)? || is_window_function_expr!(order_by_expr) {
                    require_wrapper = true;
                }
                rn_order_by.push((
                    order_by_expr.clone(),
                    o.order_type.unwrap_or(OrderType::OrderAscending),
                    o.null_order,
                ));
            } else {
                invalid_query!(
                    "Invalid ORDER BY item: {}",
                    expr.display(Dialect::PostgreSQL)
                );
            }
        }
        rn_order_by
    } else {
        vec![]
    };

    if require_wrapper {
        let order_by_refs = project_columns_if_not_exist_fix_duplicate_aliases(
            stmt,
            &rn_order_by
                .iter()
                .map(|(expr, _, _)| expr.clone())
                .collect::<Vec<_>>(),
        );

        let limit_clause = mem::take(&mut stmt.limit_clause);
        stmt.order = None;

        let mut dt = construct_projecting_wrapper(TableExpr {
            inner: TableExprInner::Subquery(Box::new(mem::take(stmt))),
            alias: Some(stmt_alias.clone()),
        })?;

        let (wrapper_stmt, _) = expect_sub_query_with_alias_mut(&mut dt);

        wrapper_stmt.order = Some(OrderClause {
            order_by: order_by_refs
                .into_iter()
                .zip(rn_order_by.iter_mut())
                .map(|((_, lb), (rn_expr, order_type, null_order))| {
                    *rn_expr = Expr::Column(Column {
                        name: lb,
                        table: Some(stmt_alias.clone().into()),
                    });
                    OrderBy {
                        field: FieldReference::Expr(rn_expr.clone()),
                        order_type: Some(*order_type),
                        null_order: *null_order,
                    }
                })
                .collect(),
        });

        wrapper_stmt.limit_clause = limit_clause;

        *stmt = mem::take(wrapper_stmt);
    }

    let rn_alias = get_unique_select_item_alias(&stmt.fields, "__rn");

    let rn_expr = Expr::WindowFunction {
        function: FunctionExpr::RowNumber,
        order_by: rn_order_by,
        partition_by: vec![],
    };

    if let Some(rn_field) = stmt
        .fields
        .iter_mut()
        .find(|field| matches!(field, FieldDefinitionExpr::Expr { expr, .. } if expr.eq(&rn_expr)))
    {
        let (_, alias) = expect_field_as_expr_mut(rn_field);
        Ok((
            if let Some(alias) = alias {
                alias.clone()
            } else {
                *alias = Some(rn_alias.clone());
                rn_alias
            },
            // User defined column
            true,
            require_wrapper,
        ))
    } else {
        stmt.fields.push(FieldDefinitionExpr::Expr {
            expr: rn_expr,
            alias: Some(rn_alias.clone()),
        });
        Ok((rn_alias, false, require_wrapper))
    }
}

fn literal_into_positive_number(lit: &Literal, title: &str) -> ReadySetResult<i64> {
    macro_rules! out_of_range {
        ($title:expr) => {
            invalid_query!(
                "{} should be positive number from 0 to {}",
                $title,
                i64::MAX
            )
        };
    }
    let n: i64 = match lit {
        Literal::Integer(i) => *i,
        Literal::UnsignedInteger(i) => {
            if *i <= i64::MAX as u64 {
                *i as i64
            } else {
                out_of_range!(title)
            }
        }
        Literal::Number(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|err| invalid_query_err!("Invalid {title}: {err}"))?,
        _ => out_of_range!(title),
    };
    if n < 0 {
        out_of_range!(title)
    }
    Ok(n)
}

/// Returns `(rn_alias, rn_user_defined, rn_filter, inner_wrapper_inserted)`.
/// - `inner_wrapper_inserted` propagates whether the ORDER BY wrapper was added here.
///   `rewrite_top_k_in_place_impl` will convert that into `double_wrapped`.
fn rewrite_top_k_subquery(
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
) -> ReadySetResult<(SqlIdentifier, bool, Expr, bool)> {
    let limit_value = if let Some(limit) = stmt.limit_clause.limit() {
        literal_into_positive_number(limit, "LIMIT")?
    } else {
        -1
    };

    let offset_value = if let Some(offset) = stmt.limit_clause.offset() {
        literal_into_positive_number(offset, "OFFSET")?
    } else {
        0
    };

    // DISTINCT under explicit LIMIT 1 is redundant (RN <= 1 enforces single-row shape).
    // Drop it **before** projecting ORDER BY refs / inserting wrappers to avoid
    // DISTINCT interacting with temporary projected columns.
    if limit_value == 1 {
        stmt.distinct = false;
    }

    let (rn_alias, user_defined, inner_wrapper_inserted) =
        project_row_number_field(stmt, stmt_alias.clone())?;

    let rn_col = Expr::Column(Column {
        name: rn_alias.clone(),
        table: Some(stmt_alias.clone().into()),
    });

    let rn_check = if limit_value >= 0 && offset_value > 0 {
        Expr::BinaryOp {
            lhs: Box::new(construct_scalar_expr(
                rn_col.clone(),
                BinaryOperator::Greater,
                Expr::Literal(offset_value.into()),
            )),
            op: BinaryOperator::And,
            rhs: Box::new(construct_scalar_expr(
                rn_col,
                BinaryOperator::LessOrEqual,
                Expr::Literal((limit_value + offset_value).into()),
            )),
        }
    } else if limit_value >= 0 {
        construct_scalar_expr(
            rn_col,
            BinaryOperator::LessOrEqual,
            Expr::Literal(limit_value.into()),
        )
    } else if offset_value > 0 {
        construct_scalar_expr(
            rn_col,
            BinaryOperator::Greater,
            Expr::Literal(offset_value.into()),
        )
    } else {
        internal!("Either LIMIT or OFFSET expected, but both not set")
    };

    stmt.limit_clause = LimitClause::default();
    stmt.order = None;

    Ok((rn_alias, user_defined, rn_check, inner_wrapper_inserted))
}

/// Rewrites LIMIT/OFFSET into a ROW_NUMBER()-based filter and wraps the current
/// statement in a projecting derived table.
///
/// Returns `(rn_alias, rn_user_defined, double_wrapped)`.
/// - `rn_alias`: the alias of the RN column we target (usually `"__rn"` or a disambiguated variant).
/// - `rn_user_defined`: true iff the select list already contained a matching
///   ROW_NUMBER() expression; in that case we do **not** remove or change it.
/// - `double_wrapped`: true iff an **inner** wrapper was inserted earlier to
///   support ORDER BY on aggregates/WFs (by `project_row_number_field`). Given
///   this function always adds an **outer** wrapper, `double_wrapped == true`
///   means the final shape is: `OUTER(wrapper) -> INNER(wrapper) -> CHILD(original)`.
///
/// # Postconditions (invariants established on `stmt`)
/// - `stmt` is a projecting wrapper whose **first** FROM item is an aliased
///   subquery named `"INNER"`.
/// - The synthetic RN column **lives inside** that `INNER` subquery after this
///   function returns.
/// - If `double_wrapped == true`, then `INNER` is itself a projecting wrapper
///   over exactly **one** child subquery (the original statement rewritten for ORDER BY).
/// - If `rn_user_defined == false`, any synthetic RN leaked at the **outer** level
///   is removed from `stmt.fields` (RN remains available inside `INNER` for filtering).
///
/// These invariants are relied upon by `rewrite_top_k_in_place_with_partition`, which
/// patches the RN window’s PARTITION BY in the correct scope.
fn rewrite_top_k_in_place_impl(
    stmt: &mut SelectStatement,
) -> ReadySetResult<(SqlIdentifier, bool, bool)> {
    // Take `lateral` attribute away
    let lateral = mem::take(&mut stmt.lateral);

    let (rn_alias, rn_user_defined, rn_filter, double_wrapped) =
        rewrite_top_k_subquery(stmt, INNER_STMT_ALIAS.into())?;

    // Always wrap after top-k rewrite
    let mut outer_derived_table = construct_projecting_wrapper(TableExpr {
        inner: TableExprInner::Subquery(Box::new(mem::take(stmt))),
        alias: Some(INNER_STMT_ALIAS.into()),
    })?;
    let (outer_stmt, _) = expect_sub_query_with_alias_mut(&mut outer_derived_table);
    *stmt = mem::take(outer_stmt);

    // Apply rn_filter in-place on the existing wrapper
    stmt.where_clause = and_predicates_skip_true(mem::take(&mut stmt.where_clause), rn_filter);

    // Drop synthetic RN field if we added it
    if !rn_user_defined && let Some(i) = stmt.fields.iter().position(|fe|
        matches!(fe, FieldDefinitionExpr::Expr { alias: Some(alias), .. } if alias.eq(&rn_alias))
    ) {
        stmt.fields.remove(i);
    }

    // Restore lateral from before we entered
    stmt.lateral = lateral;

    Ok((rn_alias, rn_user_defined, double_wrapped))
}

fn ensure_partition_keys_visible_and_qualified(
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    partition_keys: &[Expr],
) -> ReadySetResult<Vec<Expr>> {
    // 1) Try to resolve existing aliases without mutating the child.
    //    Only exact `Expr::Column(..)` matches are considered here by design.
    let mut resolved: Vec<Option<SqlIdentifier>> = Vec::with_capacity(partition_keys.len());
    let mut missing: Vec<Expr> = Vec::new();

    for rc in partition_keys {
        if let Expr::Column(want_col) = rc {
            // Search for a select item that is exactly this column.
            let mut found_alias: Option<SqlIdentifier> = None;
            for fe in &stmt.fields {
                if let FieldDefinitionExpr::Expr {
                    expr,
                    alias: Some(a),
                } = fe
                    && matches!(expr, Expr::Column(have_col) if have_col == want_col)
                {
                    found_alias = Some(a.clone());
                    break;
                }
            }
            if let Some(a) = found_alias {
                resolved.push(Some(a));
            } else {
                resolved.push(None);
                missing.push(rc.clone());
            }
        } else {
            // Required keys should be columns by precondition; keep shape robust.
            resolved.push(None);
            missing.push(rc.clone());
        }
    }

    // 2) Project only the missing ones (maintains GROUP BY if needed).
    let mut projected_aliases: Vec<SqlIdentifier> = Vec::new();
    if !missing.is_empty() {
        projected_aliases = project_columns(stmt, &missing)?;
    }

    // 3) Stitch final aliases back together in the original order
    //    (resolved first, then consume from `projected_aliases` for missing ones).
    let mut it_proj = projected_aliases.into_iter();
    let final_aliases: Vec<SqlIdentifier> = resolved
        .into_iter()
        .map(|maybe_a| maybe_a.unwrap_or_else(|| it_proj.next().expect("alias for missing col")))
        .collect();

    // 4) Qualify with stmt_alias for use at the parent scope.
    Ok(final_aliases
        .into_iter()
        .map(|a| {
            Expr::Column(Column {
                name: a,
                table: Some(stmt_alias.clone().into()),
            })
        })
        .collect())
}

/// Rewrites TOP-K (LIMIT/OFFSET) and injects a `PARTITION BY` list into the
/// ROW_NUMBER() window, so the top-k is computed per correlated partition.
///
/// # Preconditions (required from the caller)
/// - Any correlated columns referenced by ON/WHERE were already **retargeted**
///   to the correct scope prior to calling this function (e.g., via projection
///   or alias patching). This function does not perform retargeting.
/// - `required_partition_by` contains only `Expr::Column` items that represent
///   the intended partition keys in the **pre-wrap** local scope; this function
///   will project/qualify them into the scope where RN actually lives.
///
/// # Invariants relied upon (established by `rewrite_top_k_in_place_impl`)
/// - After calling `rewrite_top_k_in_place_impl`, the RN column (alias `rn_alias`)
///   **lives inside** the `INNER` subquery (the first FROM item of the current `stmt`).
/// - If `double_wrapped == true`, then `INNER` is a projecting wrapper over a
///   **single** child subquery (the ORDER BY wrapper). In this case, required
///   partition keys must be **projected into the child** and referenced as
///   `child_alias.<key>` in the RN’s `PARTITION BY`.
/// - If `double_wrapped == false`, the required partition keys are already valid
///   at `INNER` scope and can be used directly.
///
/// # Behavior
/// - If the RN was user-defined (`rn_user_defined == true`), this function does
///   not override its existing `PARTITION BY`.
/// - Otherwise, it injects the correct `PARTITION BY` at the exact level where
///   the RN lives, projecting keys through the appropriate child only when
///   double-wrapped.
///
/// Errors if it cannot locate and patch the RN field.
pub(crate) fn rewrite_top_k_in_place_with_partition(
    stmt: &mut SelectStatement,
    required_partition_by: Vec<Expr>,
) -> ReadySetResult<()> {
    fn inject_partition_by(
        expr: &mut Expr,
        partition_by_columns: Vec<Expr>,
    ) -> ReadySetResult<bool> {
        let mut patched = false;
        for_each_window_function_mut(expr, &mut |wf| {
            if let Expr::WindowFunction {
                function: FunctionExpr::RowNumber,
                partition_by,
                ..
            } = wf
            {
                *partition_by = partition_by_columns.clone();
                patched = true;
            }
        })?;
        Ok(patched)
    }

    fn get_rn_column_mut<'a>(
        fields: &'a mut [FieldDefinitionExpr],
        rn_alias: &SqlIdentifier,
    ) -> Option<&'a mut Expr> {
        fields.iter_mut().find_map(|fe| match fe {
            FieldDefinitionExpr::Expr { expr, alias } if alias.as_deref() == Some(rn_alias) => {
                Some(expr)
            }
            _ => None,
        })
    }

    // Canonicalize the required partition keys (dedupe, stable order)
    let mut req_cols: Vec<Column> = required_partition_by
        .iter()
        .filter_map(|e| {
            if let Expr::Column(c) = e {
                Some(c.clone())
            } else {
                None
            }
        })
        .collect();
    req_cols.sort();
    req_cols.dedup();

    // Recollect them as Expr::Column(c)
    let partition_by_columns = req_cols.into_iter().map(Expr::Column).collect::<Vec<_>>();

    // Do the standard TOP‑K rewrite (adds RN + wrapper around the current statement).
    // Also tells us if the rewrite resulted in a double wrap (ORDER BY wrapper + our wrapper).
    let (rn_alias, rn_user_defined, double_wrapped) = rewrite_top_k_in_place_impl(stmt)?;

    // If the RN is user-defined, its PARTITION BY was already validated/fixed upstream
    // (see hoist_correlated_from_where_clause_and_rewrite_top_k). Do not override it here.
    if rn_user_defined {
        return Ok(());
    }

    let mut patched = false;

    // Deterministic patch: RN always lives inside the INNER subquery after TOP-K rewrite.
    // We know whether we are double-wrapped from `double_wrapped`.
    // SAFETY: `rewrite_top_k_in_place_impl` (just called above) guarantees:
    // - `stmt.tables[0]` is the aliased subquery "INNER" (outer projecting wrapper).
    // - The RN column we must patch lives **inside** this INNER.
    // - If `double_wrapped == true`, INNER is itself a projecting wrapper over a single child.
    let (inner, _inner_alias) = expect_sub_query_with_alias_mut(&mut stmt.tables[0]);

    // Compute the correct PARTITION BY scope:
    // - double_wrapped: INNER is itself a projecting wrapper; project keys into its child and qualify them
    // - single wrap: keys are valid at INNER scope; use them directly
    let scope_partition_by = if double_wrapped {
        debug_assert!(
            is_single_from_item!(inner),
            "double_wrapped implies INNER is a projecting wrapper over a single child"
        );
        let (child_stmt, child_alias) = expect_sub_query_with_alias_mut(&mut inner.tables[0]);
        ensure_partition_keys_visible_and_qualified(child_stmt, child_alias, &partition_by_columns)?
    } else {
        // keys valid at INNER scope
        partition_by_columns.clone()
    };

    // Find RN inside INNER and inject the partition keys
    if let Some(rn_expr) = get_rn_column_mut(&mut inner.fields, &rn_alias) {
        debug_assert!(matches!(
            rn_expr,
            Expr::WindowFunction {
                function: FunctionExpr::RowNumber,
                ..
            }
        ));
        // RN is guaranteed to be a WindowFunction::RowNumber here;
        // we only rewrite its partition_by list, not order_by.
        if inject_partition_by(rn_expr, scope_partition_by)? {
            patched = true;
        }
    }

    invariant!(patched, "Expected to patch ROW_NUMBER() window for __rn");

    Ok(())
}

pub(crate) fn rewrite_top_k_in_place(stmt: &mut SelectStatement) -> ReadySetResult<()> {
    rewrite_top_k_in_place_impl(stmt).map(|_| {})
}

pub(crate) fn analyse_fix_correlated_subquery_group_by(
    cols_set: &HashSet<(Column, Column)>,
    group_by: &mut GroupByClause,
) -> ReadySetResult<bool> {
    let mut local_cols = cols_set
        .iter()
        .map(|(local_col, _)| local_col)
        .collect::<HashSet<_>>();

    let mut constraint_columns_group_by_only = true;
    for f in group_by.fields.iter_mut() {
        match f {
            FieldReference::Expr(Expr::Column(col)) => {
                if let Some((local_col, _)) = cols_set.iter().find(|(local_col, correlated_col)| {
                    if local_col.eq(col) {
                        true
                    } else if correlated_col.eq(col) {
                        let _ = mem::replace(col, local_col.clone());
                        true
                    } else {
                        false
                    }
                }) {
                    local_cols.remove(local_col);
                } else {
                    constraint_columns_group_by_only = false;
                }
            }
            FieldReference::Expr(Expr::Literal(_)) => {}
            _ => constraint_columns_group_by_only = false,
        }
    }

    Ok(constraint_columns_group_by_only && local_cols.is_empty())
}

fn fix_correlated_subquery_with_window_functions(
    cols_set: &HashSet<(Column, Column)>,
    stmt: &mut SelectStatement,
) -> ReadySetResult<()> {
    for field in stmt.fields.iter_mut() {
        let (field_expr, _) = expect_field_as_expr_mut(field);
        for_each_window_function_mut(field_expr, &mut |wf| {
            let Expr::WindowFunction { partition_by, .. } = wf else {
                unreachable!("Already checked")
            };
            for (local_col, _) in cols_set.iter() {
                if !partition_by.iter().any(|part_expr| {
                    if let Expr::Column(col) = part_expr {
                        col.eq(local_col)
                    } else {
                        false
                    }
                }) {
                    partition_by.push(Expr::Column(local_col.clone()));
                }
            }
        })?
    }
    Ok(())
}

pub(crate) fn align_group_by_and_windows_with_correlation(
    stmt: &mut SelectStatement,
    cols_set: &HashSet<(Column, Column)>, // (local_column = correlated_column)
) -> ReadySetResult<bool> {
    // Get a flag if either `stmt` has no GROUP BY or all local columns are the only grouping keys,
    // and fix group by correlated columns occurrences.
    let are_local_columns_eq_grouping_keys = if let Some(group_by) = &mut stmt.group_by {
        analyse_fix_correlated_subquery_group_by(cols_set, group_by)?
    } else {
        true
    };

    // Make the WF, if present, be partitioned by the local columns equated to the correlated keys.
    fix_correlated_subquery_with_window_functions(cols_set, stmt)?;

    Ok(are_local_columns_eq_grouping_keys)
}

/// Ensure `ff_alias` exists at the anchor *top* by re-projecting it through any
/// single-subquery projecting wrappers. This does **not** recompute the value;
/// it just exposes the same named column from the child at each parent level.
///
/// Invariant: either the current `stmt` already has `ff_alias` in its select
/// list, or it is a pure projecting wrapper over exactly one subquery.
pub(crate) fn bubble_alias_to_anchor_top(
    anchor_top: &mut SelectStatement,
    ff_alias: &SqlIdentifier,
) -> ReadySetResult<()> {
    fn ensure_here(stmt: &mut SelectStatement, ff_alias: &SqlIdentifier) -> ReadySetResult<()> {
        // If this level already projects the alias, we’re done.
        if has_alias(stmt, ff_alias) {
            return Ok(());
        }

        // Otherwise this must be a projecting wrapper over a single subquery.
        if !is_single_from_item!(stmt) {
            internal!(
                "bubble_alias_to_anchor_top: anchor topology is not a single-subquery chain and alias `{}` is missing",
                ff_alias
            );
        }

        // Get child SELECT and its alias.
        let Some((child_stmt, child_alias)) = as_sub_query_with_alias_mut(&mut stmt.tables[0])
        else {
            internal!("bubble_alias_to_anchor_top: expected subquery with alias")
        };

        // First ensure the child exposes the alias…
        ensure_here(child_stmt, ff_alias)?;

        // …then (re)project it here if still missing (another pass may have added it).
        if !has_alias(stmt, ff_alias) {
            stmt.fields.push(FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    table: Some(child_alias.into()),
                    name: ff_alias.clone(),
                }),
                alias: Some(ff_alias.clone()),
            });
        }

        Ok(())
    }

    ensure_here(anchor_top, ff_alias)
}

/// Hoist simple join ON predicates into the top-level WHERE clause.
pub(crate) fn hoist_parametrizable_join_filters_to_where(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    let mut add_to_where = Vec::new();

    for jc in stmt.join.iter_mut() {
        // Only hoist from INNER joins
        if !jc.operator.is_inner_join() {
            continue;
        }
        if let JoinConstraint::On(join_expr) = &jc.constraint {
            let mut cands = Vec::new();
            let rem = split_expr(
                join_expr,
                &|e| is_simple_parametrizable_filter(e, |_, _| true),
                &mut cands,
            );
            if !cands.is_empty() {
                jc.constraint = rem.map_or(JoinConstraint::Empty, JoinConstraint::On);
                add_to_where.extend(cands);
            }
        }
    }

    if add_to_where.is_empty() {
        return Ok(false);
    }

    let mut acc = None;
    for e in add_to_where {
        acc = and_predicates_skip_true(acc, e);
    }
    stmt.where_clause = and_predicates_skip_true(mem::take(&mut stmt.where_clause), acc.unwrap());

    Ok(true)
}

/// Returns true if this SELECT defines a FROM-item whose relation "matches" `rhs_rel`.
///
/// We treat that as a lexical shadowing of the outer RHS alias and **never** descend
/// into such a subquery when collecting or rebinding RHS references. This prevents us
/// from accidentally rewriting columns that belong to a new scope with the same alias.
///
/// Correctness of this guard relies on `get_from_item_reference_name` and `is_column_of!`
/// using the same `Relation` identity that the resolver assigned to aliases.
fn shadows_rhs_alias(stmt: &SelectStatement, rhs_rel: &Relation) -> bool {
    get_local_from_items_iter!(stmt)
        .any(|t| get_from_item_reference_name(t).is_ok_and(|rel| rel == *rhs_rel))
}

pub(crate) fn deep_columns_visitor_mut(
    stmt: &mut SelectStatement,
    shadow_rel: &Relation,
    visitor: &mut impl FnMut(&mut Expr),
) -> ReadySetResult<()> {
    struct ExprVisitor<'a> {
        shadow_rel: &'a Relation,
        visitor: &'a mut dyn FnMut(&mut Expr),
        depth: usize,
    }

    impl<'a> VisitorMut<'a> for ExprVisitor<'a> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'a mut Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::Column(_)) {
                (self.visitor)(expr);
                Ok(())
            } else {
                walk_expr(self, expr)
            }
        }

        fn visit_select_statement(
            &mut self,
            stmt: &'a mut SelectStatement,
        ) -> Result<(), Self::Error> {
            self.depth += 1;
            if self.depth == 1 || !shadows_rhs_alias(stmt, self.shadow_rel) {
                visit_mut::walk_select_statement(self, stmt)?;
            }
            self.depth -= 1;
            Ok(())
        }
    }

    ExprVisitor {
        shadow_rel,
        visitor,
        depth: 0,
    }
    .visit_select_statement(stmt)
}

pub(crate) fn deep_columns_visitor(
    stmt: &SelectStatement,
    shadow_rel: &Relation,
    visitor: &mut impl FnMut(&Expr),
) -> ReadySetResult<()> {
    struct ExprVisitor<'a> {
        shadow_rel: &'a Relation,
        visitor: &'a mut dyn FnMut(&Expr),
        depth: usize,
    }

    impl<'a> Visitor<'a> for ExprVisitor<'a> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'a Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::Column(_)) {
                (self.visitor)(expr);
                Ok(())
            } else {
                visit::walk_expr(self, expr)
            }
        }

        fn visit_select_statement(&mut self, stmt: &'a SelectStatement) -> Result<(), Self::Error> {
            self.depth += 1;
            if self.depth == 1 || !shadows_rhs_alias(stmt, self.shadow_rel) {
                walk_select_statement(self, stmt)?;
            }
            self.depth -= 1;
            Ok(())
        }
    }

    ExprVisitor {
        shadow_rel,
        visitor,
        depth: 0,
    }
    .visit_select_statement(stmt)
}
