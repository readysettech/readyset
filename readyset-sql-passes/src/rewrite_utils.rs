use readyset_errors::{
    internal, internal_err, invalid_query, invalid_query_err, ReadySetError, ReadySetResult,
};
use readyset_sql::analysis::visit::{walk_function_expr, Visitor};
use readyset_sql::analysis::visit_mut::{walk_expr, VisitorMut};
use readyset_sql::analysis::{is_aggregate, ReferredColumns};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause,
    InValue, JoinConstraint, JoinRightSide, Literal, OrderBy, Relation, SelectStatement,
    SqlIdentifier, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};

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

/// Checks if an expression is an equality comparison between two columns
/// and satisfies a given predicate.
pub(crate) fn matches_eq_constraint<P>(expr: &Expr, predicate: P) -> bool
where
    P: FnOnce(&Relation, &Relation) -> bool,
{
    // Only binary equalities between two columns are considered.
    match expr {
        Expr::BinaryOp {
            lhs,
            op: BinaryOperator::Equal,
            rhs,
        } => {
            let (
                Expr::Column(Column {
                    table: Some(left_table),
                    ..
                }),
                Expr::Column(Column {
                    table: Some(right_table),
                    ..
                }),
            ) = (lhs.as_ref(), rhs.as_ref())
            else {
                return false;
            };
            predicate(left_table, right_table)
        }
        _ => false,
    }
}

/// Check if an expression is a simple column-based comparison for parameterization.
pub(crate) fn is_simple_parametrizable_filter(
    expr: &Expr,
    predicate: impl Fn(&Relation, &SqlIdentifier) -> bool,
) -> bool {
    // Delegate to generic filter candidate checker.
    is_parametrizable_filter_candidate(expr, &mut |expr| {
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
    predicate: &mut impl FnMut(&Expr) -> bool,
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

/// Return true if the expression contains any subquery.
fn contains_select(expr: &Expr) -> bool {
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

/// Locate a grouping key in GROUP BY by alias, expression, or position.
pub(crate) fn find_group_by_key(
    fields: &[FieldDefinitionExpr],
    group_by: &mut [FieldReference],
    key: &Expr,
    key_alias: &SqlIdentifier,
) -> ReadySetResult<Option<usize>> {
    for (pos, g) in group_by.iter().enumerate() {
        match g {
            // `g` is a select item's alias, so match by the alias
            FieldReference::Expr(Expr::Column(alias))
                if alias.table.is_none() && alias.name.eq(key_alias) =>
            {
                return Ok(Some(pos))
            }
            // `g` is an expression, so match by the expression
            FieldReference::Expr(expr) if expr.eq(key) => return Ok(Some(pos)),
            // `g` is a select item's 1-based index, sp match by the expression
            FieldReference::Numeric(proj_idx) => {
                if *proj_idx < 1 || *proj_idx >= fields.len() as u64 {
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
    s.push('_');
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
    Ok(stmt
        .join
        .iter()
        .skip(join_clause_idx)
        .all(|join_clause| join_clause.operator.is_inner_join()))
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

pub(crate) fn for_each_function_call<'a>(
    expr: &'a Expr,
    func_visitor: &'a mut impl FnMut(&FunctionExpr),
) -> ReadySetResult<()> {
    struct ForEachVisitor<'a> {
        func_visitor: &'a mut dyn FnMut(&FunctionExpr),
    }

    impl<'ast> Visitor<'ast> for ForEachVisitor<'ast> {
        type Error = ReadySetError;

        fn visit_function_expr(
            &mut self,
            function_expr: &'ast FunctionExpr,
        ) -> Result<(), Self::Error> {
            (self.func_visitor)(function_expr);
            walk_function_expr(self, function_expr)
        }

        fn visit_select_statement(&mut self, _: &'ast SelectStatement) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    ForEachVisitor { func_visitor }.visit_expr(expr)?;

    Ok(())
}

pub(crate) fn is_aggregated_expr(expr: &Expr) -> ReadySetResult<bool> {
    let mut has_aggregates = false;

    if matches!(expr, Expr::WindowFunction { .. }) {
        return Ok(false);
    }

    for_each_function_call(expr, &mut |f| {
        if is_aggregate(f) {
            has_aggregates = true;
        }
    })?;

    Ok(has_aggregates)
}

pub(crate) fn is_aggregated_select(stmt: &SelectStatement) -> ReadySetResult<bool> {
    for fe in stmt.fields.iter() {
        let (expr, _) = expect_field_as_expr(fe);
        if is_aggregated_expr(expr)? {
            return Ok(true);
        }
    }
    Ok(false)
}
