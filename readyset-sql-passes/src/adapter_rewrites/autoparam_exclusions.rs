//! Support for the `CREATE CACHE WITH (AUTOPARAM (EXCLUDE_*))` scopes: a pre-rewrite pass that
//! marks literals in the excluded clause origins so autoparameterization keeps them inline, and
//! the lockstep diff that derives the frozen positions from the rewritten forms.
//!
//! The marking must happen before the rewrite pipeline runs because the unnest/hoist passes
//! relocate predicates from `EXISTS`, `JOIN ON`, and subqueries into the top-level WHERE, erasing
//! their origin by the time autoparameterization sees them. The marker ([`Literal::Preserved`])
//! rides those passes (they clone expressions), is skipped by both autoparameterization phases,
//! and is unwrapped by the sweep at the end of `rewrite_for_readyset`.

use std::mem;

use readyset_errors::{ReadySetResult, unsupported};
use readyset_sql::analysis::visit::{self, Visitor};
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
/// wrapped (frozen IN-list literals are unsupported in v1).
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
    let Ok(()) = visitor.visit_select_statement(stmt);
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
        walk: impl FnOnce(&mut Self, &'ast mut T) -> Result<(), std::convert::Infallible>,
    ) -> Result<(), std::convert::Infallible> {
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
    type Error = std::convert::Infallible;

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

/// Derive the frozen positions for a manually parameterized cache by walking the rewritten
/// standard (fully autoparameterized) and manual forms in lockstep.
///
/// Both forms come out of the same rewrite pipeline over the same source statement, so they
/// should be structurally identical except at literal leaves: where the standard form has a
/// placeholder and the manual form has a literal, that literal is frozen at the standard form's
/// parameter position. The marker the manual form carries through the pipeline could in principle
/// perturb a pass into reshaping it differently, so the structural identity is verified rather
/// than assumed -- a mismatch yields an `unsupported` error, never a guessed position map.
/// Comparing the verified-aligned forms makes the derivation exact even when the two passes'
/// equals/range gating diverges or when equal literal values appear at several positions.
pub fn derive_frozen(
    standard: &SelectStatement,
    manual: &SelectStatement,
) -> ReadySetResult<Vec<(usize, Literal)>> {
    // The lockstep pairing below assumes the two forms are structurally identical apart from the
    // frozen literal leaves, so that the i-th literal of one corresponds to the i-th of the other.
    // The exclusion pre-pass rewrites the manual form with `Literal::Preserved` markers in place,
    // and that marker is value-distinct from its inner literal, so a dedup or ordering pass could
    // act on it and reshape predicates differently from the standard form. Comparing literal
    // skeletons (every literal blanked to a single value) catches any such divergence -- including
    // an equal-count reordering that a length check alone would miss -- before we trust the
    // pairing. On divergence we reject the cache rather than risk freezing a value against the
    // wrong column. A user-facing limitation of AUTOPARAM, not an internal bug.
    if literal_skeleton(standard) != literal_skeleton(manual) {
        unsupported!(
            "AUTOPARAM cannot build a cache for this query: the requested EXCLUDE options change \
             how it rewrites, so incoming queries could not be matched to the cache reliably. \
             Create the cache with AUTOPARAM OFF and write the placeholders explicitly instead."
        );
    }

    // Structurally aligned, so the two literal walks visit corresponding positions in lockstep.
    let standard_literals = collect_literals(standard);
    let manual_literals = collect_literals(manual);
    let mut frozen = vec![];
    let mut standard_param_pos = 0_usize;
    for (s, m) in standard_literals.into_iter().zip(manual_literals) {
        match (s, m) {
            (Literal::Placeholder(_), Literal::Placeholder(_)) => standard_param_pos += 1,
            (Literal::Placeholder(_), m) => {
                frozen.push((standard_param_pos, m));
                standard_param_pos += 1;
            }
            (s, m) if s == m => {}
            (_, _) => {
                unsupported!(
                    "AUTOPARAM cannot be applied to this query: excluding the selected clauses \
                     changes how it rewrites. Rephrase the query or use fewer exclusions."
                );
            }
        }
    }
    Ok(frozen)
}

/// A copy of `stmt` with every literal (placeholders included) blanked to a single canonical
/// value. Two statements share a skeleton iff they have identical structure regardless of literal
/// values, which is exactly the precondition [`derive_frozen`]'s positional pairing relies on.
fn literal_skeleton(stmt: &SelectStatement) -> SelectStatement {
    struct Blank;

    impl<'ast> VisitorMut<'ast> for Blank {
        type Error = std::convert::Infallible;

        fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
            *literal = Literal::Null;
            Ok(())
        }
    }

    let mut stmt = stmt.clone();
    let Ok(()) = Blank.visit_select_statement(&mut stmt);
    stmt
}

/// Collect every literal in the statement in visit order.
fn collect_literals(stmt: &SelectStatement) -> Vec<Literal> {
    struct CollectLiterals(Vec<Literal>);

    impl<'ast> Visitor<'ast> for CollectLiterals {
        type Error = std::convert::Infallible;

        fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
            self.0.push(literal.clone());
            Ok(())
        }
    }

    let mut visitor = CollectLiterals(vec![]);
    let Ok(()) = visit::Visitor::visit_select_statement(&mut visitor, stmt);
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

    /// The lockstep diff pairs positions exactly even when equal literal values appear both
    /// frozen and parameterized.
    #[test]
    fn derive_frozen_disambiguates_equal_values() {
        // Standard form: both literals parameterized. Manual form: `x` kept inline.
        let standard = parse("SELECT v FROM t WHERE x = ? AND y = ?");
        let manual = parse("SELECT v FROM t WHERE x = 5 AND y = ?");
        let frozen = derive_frozen(&standard, &manual).unwrap();
        assert_eq!(frozen, vec![(0, 5.into())]);

        // And the mirror case: `y` kept inline, same value.
        let manual = parse("SELECT v FROM t WHERE x = ? AND y = 5");
        let frozen = derive_frozen(&standard, &manual).unwrap();
        assert_eq!(frozen, vec![(1, 5.into())]);
    }

    #[test]
    fn derive_frozen_counts_shared_placeholders_and_literals() {
        let standard = parse("SELECT v FROM t WHERE a = ? AND b = ? AND c = ? AND d = 'kept'");
        let manual = parse("SELECT v FROM t WHERE a = ? AND b = 'frozen' AND c = ? AND d = 'kept'");
        let frozen = derive_frozen(&standard, &manual).unwrap();
        assert_eq!(frozen, vec![(1, "frozen".into())]);
    }

    #[test]
    fn derive_frozen_rejects_divergent_forms() {
        let standard = parse("SELECT v FROM t WHERE a = ?");
        let manual = parse("SELECT v FROM t WHERE a = ? AND b = 1");
        assert!(derive_frozen(&standard, &manual).is_err());

        let standard = parse("SELECT v FROM t WHERE a = 1");
        let manual = parse("SELECT v FROM t WHERE a = 2");
        assert!(derive_frozen(&standard, &manual).is_err());
    }

    #[test]
    fn derive_frozen_rejects_equal_count_reordering() {
        // Equal literal count, but the manual form's predicates are reordered (column `b` first).
        // Pairing literals positionally would freeze `b`'s value against `a`'s placeholder slot --
        // a wrong-context match. A length check passes here; the skeleton guard rejects it.
        //
        // These forms are hand-built. The rewrite passes preserve predicate order today (see
        // probe_exclude_forms_alignment_through_pipeline in adapter_rewrites tests), so this
        // guards the alignment invariant against future order-affecting passes rather than
        // fixing a currently reachable misroute.
        let standard = parse("SELECT v FROM t WHERE a = ? AND b = ?");
        let manual = parse("SELECT v FROM t WHERE b = 5 AND a = ?");
        assert_eq!(
            collect_literals(&standard).len(),
            collect_literals(&manual).len()
        );
        assert!(derive_frozen(&standard, &manual).is_err());
    }
}
