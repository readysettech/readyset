//! Support for the `CREATE CACHE WITH (AUTOPARAM (EXCLUDE_*))` scopes: a pre-rewrite pass that
//! marks literals in the excluded clause origins so autoparameterization keeps them inline.
//!
//! # Disabled
//!
//! Nothing in the product calls this today. A cache built from marked literals takes a form no
//! read produces on its own, so reaching it needs the read matched against that form -- which is
//! what [`LiteralSlots`] does, and a scope's marks are not expressible as slots: a marked literal
//! is skipped rather than counted, so it holds no canonical position for a slot to name. Wiring
//! the two together means giving marked literals positions of their own, which is a change to how
//! every query is numbered. Until then `CREATE CACHE` rejects the option. The pass and its tests
//! stay so that work has a starting point.
//!
//! [`LiteralSlots`]: super::autoparameterize::LiteralSlots
//!
//! The marking must happen before the rewrite pipeline runs because the unnest/hoist passes
//! relocate predicates from `EXISTS`, `JOIN ON`, and subqueries into the top-level WHERE, erasing
//! their origin by the time autoparameterization sees them. The marker ([`Literal::Preserved`])
//! rides those passes (they clone expressions), is skipped by both autoparameterization phases,
//! and is unwrapped by the sweep at the end of `rewrite_for_readyset`.

use std::mem;

use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    AutoparamControl, BinaryOperator, Expr, InValue, JoinConstraint, Literal, SelectStatement,
    TableExpr, TableExprInner,
};

/// Wrap comparison literals inside the clause scopes named by `control`'s `EXCLUDE_*` flags in
/// [`Literal::Preserved`], so autoparameterization keeps them inline after the unnest/hoist
/// passes relocate them into the top-level WHERE.
///
/// Only literals in positions autoparameterization could touch are wrapped: `col = lit` and
/// `col <ordering op> lit`, in either operand order. Placeholders and IN lists are never
/// wrapped (an `IN` list's literals are never wrapped).
pub fn wrap_autoparam_exclusions(stmt: &mut SelectStatement, control: &AutoparamControl) {
    if control.off
        || (!control.exclude_joins && !control.exclude_exists && !control.exclude_subqueries)
    {
        return;
    }
    let mut visitor = ExclusionWrapper {
        control: *control,
        excluded_depth: 0,
    };
    visitor.visit_select_statement(stmt);
}

struct ExclusionWrapper {
    control: AutoparamControl,
    /// Number of enclosing excluded scopes; comparison literals are wrapped when positive.
    excluded_depth: usize,
}

impl ExclusionWrapper {
    fn wrap(literal: &mut Literal) {
        if !matches!(literal, Literal::Placeholder(_) | Literal::Preserved(_)) {
            let inner = mem::replace(literal, Literal::Null);
            *literal = Literal::Preserved(Box::new(inner));
        }
    }

    /// Wrap the literal operand of a `col = lit` / `col <ordering> lit` comparison when inside
    /// an excluded scope. Mirrors the shapes `AutoParameterizeVisitor` parameterizes.
    fn maybe_wrap_comparison(&self, expression: &mut Expr) {
        if self.excluded_depth == 0 {
            return;
        }
        if let Expr::BinaryOp { lhs, op, rhs } = expression
            && (*op == BinaryOperator::Equal || op.is_ordering_comparison())
        {
            match (lhs.as_mut(), rhs.as_mut()) {
                (Expr::Column(_), Expr::Literal(lit)) | (Expr::Literal(lit), Expr::Column(_)) => {
                    Self::wrap(lit)
                }
                _ => {}
            }
        }
    }

    fn walk_excluded<'ast, T>(
        &mut self,
        node: &'ast mut T,
        excluded: bool,
        walk: impl FnOnce(&mut Self, &'ast mut T) -> Result<(), !>,
    ) -> Result<(), !> {
        if excluded {
            self.excluded_depth += 1;
        }
        walk(self, node)?;
        if excluded {
            self.excluded_depth -= 1;
        }
        Ok(())
    }
}

impl<'ast> VisitorMut<'ast> for ExclusionWrapper {
    type Error = !;

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        self.maybe_wrap_comparison(expression);
        let excluded = match expression {
            Expr::Exists(_) => self.control.exclude_exists,
            Expr::NestedSelect(_) => self.control.exclude_subqueries,
            _ => false,
        };
        self.walk_excluded(expression, excluded, visit_mut::walk_expr)
    }

    fn visit_in_value(&mut self, in_value: &'ast mut InValue) -> Result<(), Self::Error> {
        let excluded = self.control.exclude_subqueries && matches!(in_value, InValue::Subquery(_));
        self.walk_excluded(in_value, excluded, visit_mut::walk_in_value)
    }

    fn visit_join_constraint(
        &mut self,
        join_constraint: &'ast mut JoinConstraint,
    ) -> Result<(), Self::Error> {
        // Only the ON expression is the "join condition"; literals in a joined subquery are
        // governed by EXCLUDE_SUBQUERIES via visit_table_expr.
        self.walk_excluded(
            join_constraint,
            self.control.exclude_joins,
            visit_mut::walk_join_constraint,
        )
    }

    fn visit_table_expr(&mut self, table_expr: &'ast mut TableExpr) -> Result<(), Self::Error> {
        let excluded = self.control.exclude_subqueries
            && matches!(table_expr.inner, TableExprInner::Subquery(_));
        self.walk_excluded(table_expr, excluded, visit_mut::walk_table_expr)
    }
}

/// Collect every literal in the statement in visit order.
#[cfg(test)]
fn collect_literals(stmt: &SelectStatement) -> Vec<Literal> {
    use readyset_sql::analysis::visit::Visitor;

    struct CollectLiterals(Vec<Literal>);

    impl<'ast> Visitor<'ast> for CollectLiterals {
        type Error = !;

        fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
            self.0.push(literal.clone());
            Ok(())
        }
    }

    let mut visitor = CollectLiterals(vec![]);
    visitor.visit_select_statement(stmt);
    visitor.0
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;

    fn parse(q: &str) -> SelectStatement {
        readyset_sql_parsing::parse_select(Dialect::MySQL, q).unwrap()
    }

    fn wrapped_count(stmt: &SelectStatement) -> usize {
        collect_literals(stmt)
            .iter()
            .filter(|l| matches!(l, Literal::Preserved(_)))
            .count()
    }

    #[test]
    fn wraps_exists_literals_only() {
        let mut stmt = parse(
            "SELECT v FROM t WHERE id = 1 AND EXISTS \
             (SELECT 1 FROM u WHERE u.t_id = t.id AND u.status = 'active')",
        );
        wrap_autoparam_exclusions(
            &mut stmt,
            &AutoparamControl {
                exclude_exists: true,
                ..Default::default()
            },
        );
        // Only `u.status = 'active'` is a comparison literal inside the EXISTS; the projected
        // `1` is not in a comparison and the outer `id = 1` is not excluded.
        assert_eq!(wrapped_count(&stmt), 1, "{}", stmt.display(Dialect::MySQL));
        assert!(collect_literals(&stmt).contains(&Literal::Preserved(Box::new("active".into()))));
    }

    #[test]
    fn wraps_join_on_literals_only() {
        let mut stmt = parse(
            "SELECT t.v FROM t \
             JOIN (SELECT * FROM u WHERE u.x = 5) sub ON sub.t_id = t.id AND sub.kind = 'k' \
             WHERE t.id = 1",
        );
        wrap_autoparam_exclusions(
            &mut stmt,
            &AutoparamControl {
                exclude_joins: true,
                ..Default::default()
            },
        );
        // Only the ON-clause literal is wrapped; the joined subquery's literal and the outer
        // WHERE literal are not.
        assert_eq!(wrapped_count(&stmt), 1, "{}", stmt.display(Dialect::MySQL));
        assert!(collect_literals(&stmt).contains(&Literal::Preserved(Box::new("k".into()))));
    }

    #[test]
    fn wraps_subquery_literals() {
        let mut stmt = parse(
            "SELECT t.v FROM t, (SELECT * FROM u WHERE u.x = 5) sub \
             WHERE t.id = sub.t_id AND t.id = 1",
        );
        wrap_autoparam_exclusions(
            &mut stmt,
            &AutoparamControl {
                exclude_subqueries: true,
                ..Default::default()
            },
        );
        assert_eq!(wrapped_count(&stmt), 1, "{}", stmt.display(Dialect::MySQL));
        assert!(collect_literals(&stmt).contains(&Literal::Preserved(Box::new(5.into()))));
    }

    #[test]
    fn never_wraps_placeholders_or_in_lists() {
        let mut stmt = parse(
            "SELECT v FROM t WHERE EXISTS \
             (SELECT 1 FROM u WHERE u.a = ? AND u.b IN (1, 2) AND u.c = 3)",
        );
        wrap_autoparam_exclusions(
            &mut stmt,
            &AutoparamControl {
                exclude_exists: true,
                ..Default::default()
            },
        );
        // Only `u.c = 3` qualifies: the placeholder stays a placeholder and IN-list literals
        // are not comparison operands.
        assert_eq!(wrapped_count(&stmt), 1, "{}", stmt.display(Dialect::MySQL));
        assert!(collect_literals(&stmt).contains(&Literal::Preserved(Box::new(3.into()))));
    }
}
