//! Rewrite `A RIGHT JOIN B ON p` as `B LEFT JOIN A ON p`.
//!
//! `RIGHT JOIN` is semantically the mirror of `LEFT JOIN`: rows of the right-side relation are
//! preserved, and null-extended for missing left-side matches.  Readyset's downstream pipeline
//! only handles `LEFT [OUTER] JOIN`, so without this pass `validate_query_semantics` rejects
//! every query containing a `RIGHT JOIN`.
//!
//! Scope (V1): when the leading FROM list contains exactly one table and the first JOIN clause
//! is a `RIGHT [OUTER] JOIN` with a single-table right side, swap the leading table with the
//! join's right side and flip the operator to `LEFT [OUTER] JOIN`.  The ON predicate is
//! unaffected — column references survive the swap because they are qualified by relation, not
//! by position.  Non-leading `RIGHT JOIN`s are left for `validate_query_semantics` to reject;
//! their "left side" is a join chain, not a single relation, so the simple swap is unsound
//! without restructuring the FROM list.
//!
//! Recurses into derived-table subqueries and CTEs via the default `visit_mut` walker.
//!
//! ## ON-predicate operand orientation
//!
//! Post-swap, an equality predicate like `A.k = B.k` (where A is now the RHS of the LEFT JOIN)
//! has "backward" operand order relative to the post-swap join direction.  This is intentionally
//! not fixed here: `canonicalize_join_syntax_and_predicates` (`rewrite_joins.rs`, invoked via
//! `normalize_joins_shape` during `derived_tables_rewrite`) walks each JOIN's ON and swaps the
//! operands of any cross-table equality where the LHS-of-`=` column refers to the join's RHS
//! relation.  Range predicates (`<`, `>`, `<=`, `>=`) are not normalized by either pass; they
//! are unsupported by the engine in cross-table position and rejected downstream regardless of
//! orientation.  Every analyzer between this pass and `derived_tables_rewrite`
//! (`is_column_eq_column`, `matches_eq_constraint`, `collect_cross_eqs`,
//! `is_supported_join_condition`) accepts either operand order via its predicate closure, so
//! the temporary "backward" form is invisible to them.

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{JoinOperator, JoinRightSide, SelectStatement};
use std::mem;

pub trait NormalizeRightJoin: Sized {
    fn normalize_right_join(&mut self) -> ReadySetResult<&mut Self>;
}

impl NormalizeRightJoin for SelectStatement {
    fn normalize_right_join(&mut self) -> ReadySetResult<&mut Self> {
        let mut visitor = NormalizeRightJoinVisitor;
        visitor.visit_select_statement(self)?;
        Ok(self)
    }
}

struct NormalizeRightJoinVisitor;

impl<'ast> VisitorMut<'ast> for NormalizeRightJoinVisitor {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        stmt: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Recurse first so nested subqueries are normalized bottom-up.  The check below only
        // examines `stmt.join[0]` at this level; nested SELECTs are handled by the recursive call.
        visit_mut::walk_select_statement(self, stmt)?;

        let Some(first_join) = stmt.join.first_mut() else {
            return Ok(());
        };

        let is_right = matches!(
            first_join.operator,
            JoinOperator::RightJoin | JoinOperator::RightOuterJoin
        );
        if !is_right {
            return Ok(());
        }

        // Only the leading-table shape is sound to swap: the LHS of the JOIN is a single
        // relation that lives in `stmt.tables[0]`, which we can exchange with the JOIN's
        // right side.  Multi-FROM or non-leading RIGHT JOIN shapes need more invasive
        // restructuring and stay for `validate_query_semantics` to reject.
        if stmt.tables.len() != 1 {
            return Ok(());
        }
        let JoinRightSide::Table(rhs_table) = &mut first_join.right else {
            return Ok(());
        };

        let new_operator = match first_join.operator {
            JoinOperator::RightJoin => JoinOperator::LeftJoin,
            JoinOperator::RightOuterJoin => JoinOperator::LeftOuterJoin,
            _ => return Ok(()),
        };

        mem::swap(&mut stmt.tables[0], rhs_table);
        first_join.operator = new_operator;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;
    use readyset_sql::DialectDisplay;
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    fn parse(sql: &str) -> SelectStatement {
        parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
            .expect("parse")
    }

    fn rewrite_to_string(sql: &str) -> String {
        let mut stmt = parse(sql);
        stmt.normalize_right_join().expect("rewrite");
        stmt.display(Dialect::PostgreSQL).to_string()
    }

    fn assert_equivalent(input: &str, expected: &str) {
        let got = rewrite_to_string(input);
        let expected_normalized = parse(expected).display(Dialect::PostgreSQL).to_string();
        assert_eq!(
            got, expected_normalized,
            "\ninput: {input}\nexpected (normalized): {expected_normalized}\ngot:                  {got}",
        );
    }

    #[test]
    fn basic_right_join_becomes_left_join() {
        assert_equivalent(
            r#"SELECT * FROM a RIGHT JOIN b ON a.k = b.k"#,
            r#"SELECT * FROM b LEFT JOIN a ON a.k = b.k"#,
        );
    }

    #[test]
    fn right_outer_join_becomes_left_outer_join() {
        assert_equivalent(
            r#"SELECT * FROM a RIGHT OUTER JOIN b ON a.k = b.k"#,
            r#"SELECT * FROM b LEFT OUTER JOIN a ON a.k = b.k"#,
        );
    }

    #[test]
    fn right_join_with_aliases_preserves_qualified_refs() {
        assert_equivalent(
            r#"SELECT aa.x, bb.y FROM a AS aa RIGHT JOIN b AS bb ON aa.k = bb.k"#,
            r#"SELECT aa.x, bb.y FROM b AS bb LEFT JOIN a AS aa ON aa.k = bb.k"#,
        );
    }

    #[test]
    fn right_join_in_derived_table_recurses() {
        assert_equivalent(
            r#"SELECT s.x FROM (SELECT a.x FROM a RIGHT JOIN b ON a.k = b.k) AS s"#,
            r#"SELECT s.x FROM (SELECT a.x FROM b LEFT JOIN a ON a.k = b.k) AS s"#,
        );
    }

    #[test]
    fn right_join_followed_by_inner_join_swaps_the_right_only() {
        // After swapping `a RIGHT JOIN b` → `b LEFT JOIN a`, the trailing `INNER JOIN c`
        // still joins to the same row set as before (column references are qualified by
        // relation, not position).
        assert_equivalent(
            r#"SELECT * FROM a RIGHT JOIN b ON a.k = b.k INNER JOIN c ON b.k = c.k"#,
            r#"SELECT * FROM b LEFT JOIN a ON a.k = b.k INNER JOIN c ON b.k = c.k"#,
        );
    }

    #[test]
    fn left_join_passes_through_unchanged() {
        assert_equivalent(
            r#"SELECT * FROM a LEFT JOIN b ON a.k = b.k"#,
            r#"SELECT * FROM a LEFT JOIN b ON a.k = b.k"#,
        );
    }

    #[test]
    fn inner_join_passes_through_unchanged() {
        assert_equivalent(
            r#"SELECT * FROM a INNER JOIN b ON a.k = b.k"#,
            r#"SELECT * FROM a INNER JOIN b ON a.k = b.k"#,
        );
    }

    #[test]
    fn from_without_joins_passes_through() {
        assert_equivalent(r#"SELECT * FROM a"#, r#"SELECT * FROM a"#);
    }

    #[test]
    fn non_leading_right_join_is_left_for_vqs_to_reject() {
        // RIGHT JOIN at position 1 (after an INNER JOIN) is left unmodified.  Swapping a
        // non-leading RIGHT JOIN would require restructuring the join chain (the LHS is the
        // partial join result, not a single relation).  `validate_query_semantics` continues
        // to reject this shape downstream.
        let input = r#"SELECT * FROM a INNER JOIN b ON a.k = b.k RIGHT JOIN c ON b.k = c.k"#;
        let got = rewrite_to_string(input);
        let unchanged = parse(input).display(Dialect::PostgreSQL).to_string();
        assert_eq!(got, unchanged, "non-leading RIGHT JOIN should pass through");
    }
}
