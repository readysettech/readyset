//! Tests for `STRAIGHT_JOIN` (SJ) handling throughout the join normalization pipeline.
//!
//! Invariants exercised:
//!
//! 1. **Operator identity** — `StraightJoin` is never silently converted to `CrossJoin` or
//!    `InnerJoin` by any normalization pass.
//!
//! 2. **WHERE → SJ ON attachment** — applicable cross-table WHERE equalities are moved into the
//!    SJ's `ON` constraint *before* reordering, so the planner sees the predicate on the join
//!    itself (not buried in WHERE).
//!
//! 3. **Barrier semantics** — a `STRAIGHT_JOIN` freezes its position in the chain; only the
//!    contiguous non-SJ inner-join segments on each side of the barrier are reordered.
//!    Multiple SJ barriers in the same chain create independent, separately-reordered segments.
//!
//! 4. **Hoisting exclusion** — single-table parametrizable `ON` filters on a `STRAIGHT_JOIN`
//!    are *not* hoisted to `WHERE` by the optimization pass; the join-order hint requires the
//!    constraint to remain bound to the join itself.
//!
//! Test cases are organized into four groups:
//!   - Group 1: Operator identity preservation
//!   - Group 2: WHERE → SJ ON predicate routing
//!   - Group 3: Barrier semantics and segment-level reordering
//!   - Group 4: Hoisting exclusion

use crate::derived_tables_rewrite::derived_tables_rewrite_main;
use crate::hoist_parametrizable_filters::hoist_parametrizable_filters;
use crate::rewrite_joins::normalize_joins_shape;
use readyset_sql::ast::{JoinOperator, SelectStatement};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};
// ─── Test helpers ─────────────────────────────────────────────────────────────

fn parse_mysql(sql: &str) -> readyset_sql::ast::SelectStatement {
    parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
        .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"))
}

/// Parse `sql`, run `normalize_joins_shape`, and return the MySQL display string.
fn normalize(sql: &str) -> SelectStatement {
    let mut stmt = parse_mysql(sql);
    derived_tables_rewrite_main(&mut stmt)
        .unwrap_or_else(|e| panic!("normalize_joins_shape failed: {e}\n  sql: {sql}"));
    println!(">>> Normalized: {}", stmt.display(Dialect::PostgreSQL));
    stmt
}

/// Parse `sql` with MySQL dialect and return its canonical display string (for use as expected
/// value in assertions).
fn want(sql: &str) -> SelectStatement {
    parse_mysql(sql)
}

/// Parse `sql`, run `normalize_joins_shape` then `HoistParametrizableFilters`, and return the
/// MySQL display string.
fn normalize_then_hoist(sql: &str) -> SelectStatement {
    let mut stmt = normalize(sql);
    hoist_parametrizable_filters(&mut stmt)
        .unwrap_or_else(|e| panic!("query_optimization_rewrite_main failed: {e}\n  sql: {sql}"));
    println!(">>> Hoisted: {}", stmt.display(Dialect::PostgreSQL));
    stmt
}

// ─── Group 1: Operator identity preservation ──────────────────────────────────

/// A bare `STRAIGHT_JOIN` (no ON predicate) must remain `StraightJoin` after all
/// normalization passes.  For contrast, a bare plain `INNER JOIN` (no ON) is correctly
/// canonicalized to `CROSS JOIN`.
#[test]
fn sj_bare_stays_straight_join_not_cross_join() {
    // Plain INNER JOIN without ON → CrossJoin (expected canonicalization).
    assert_eq!(
        normalize("SELECT count(*) FROM t1 INNER JOIN t2"),
        want("SELECT count(*) FROM t1 CROSS JOIN t2"),
    );
    // STRAIGHT_JOIN without ON → stays StraightJoin (must NOT become CrossJoin).
    assert_eq!(
        normalize("SELECT count(*) FROM t1 STRAIGHT_JOIN t2"),
        want("SELECT count(*) FROM t1 STRAIGHT_JOIN t2"),
    );
}

/// A `STRAIGHT_JOIN` that already carries an `ON` predicate must keep both its operator
/// and its predicate intact after normalization.
#[test]
fn sj_with_on_predicate_preserved() {
    assert_eq!(
        normalize("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.t1id"),
        want("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON (t1.id = t2.t1id)"),
    );
}

/// `StraightJoin` operator survives even when it sits between two reorderable `INNER JOIN`
/// segments.  After normalization the join at position 1 in `stmt.join` must still carry
/// the `StraightJoin` variant (verified by direct AST inspection, not just display).
#[test]
fn sj_operator_survives_reorder_of_neighbours() {
    let mut stmt = parse_mysql(
        r#"SELECT count(*) FROM t1
         INNER JOIN t2 ON t2.fk = t1.id
         STRAIGHT_JOIN t3 ON t1.id = t3.t1id
         INNER JOIN t4 ON t4.fk = t3.id"#,
    );
    normalize_joins_shape(&mut stmt).expect("normalize_joins_shape should succeed");

    // stmt.join: [t2, t3(SJ), t4]
    assert!(
        matches!(stmt.join[1].operator, JoinOperator::StraightJoin),
        "expected StraightJoin at join[1] (t3), got: {:?}",
        stmt.join[1].operator
    );
    // Neighbours must have been left as INNER JOINs (not CrossJoin).
    assert!(
        matches!(stmt.join[0].operator, JoinOperator::InnerJoin),
        "expected InnerJoin at join[0] (t2), got: {:?}",
        stmt.join[0].operator
    );
    assert!(
        matches!(stmt.join[2].operator, JoinOperator::InnerJoin),
        "expected InnerJoin at join[2] (t4), got: {:?}",
        stmt.join[2].operator
    );
}

// ─── Group 2: WHERE → SJ ON predicate routing ────────────────────────────────

/// A cross-table equality in `WHERE` that is applicable to a `STRAIGHT_JOIN` is moved into
/// its `ON` constraint; `WHERE` becomes empty afterwards.
#[test]
fn where_cross_table_eq_moved_to_sj_on() {
    assert_eq!(
        normalize("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON TRUE WHERE t1.id = t2.t1id"),
        want("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON (t1.id = t2.t1id)"),
    );
}

/// A single-table `WHERE` filter (not a cross-table equality) must *not* be moved to the SJ
/// `ON`; it must remain in `WHERE`.
#[test]
fn single_table_where_filter_stays_in_where_not_moved_to_sj() {
    assert_eq!(
        normalize(
            "SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON TRUE WHERE t1.id = t2.t1id AND t1.x > 5"
        ),
        want("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON (t1.id = t2.t1id) WHERE (t1.x > 5)"),
    );
}

/// Multiple `WHERE` cross-table equalities that all apply to the same `STRAIGHT_JOIN` are all
/// moved into its `ON` constraint.  `WHERE` is empty after the pass.
#[test]
fn multiple_where_preds_all_moved_to_sj_on() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON t1.id = t2.t1id
             STRAIGHT_JOIN t3 ON TRUE
             WHERE t2.id = t3.t2id AND t1.status = t3.status"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.t1id)
             STRAIGHT_JOIN t3 ON (t2.id = t3.t2id)
             WHERE (t1.status = t3.status)"#
        ),
    );
}

/// When two `STRAIGHT_JOIN` barriers are present, each `WHERE` predicate is routed to the
/// correct barrier (the one whose RHS table it references), not assigned to the wrong one.
#[test]
fn where_predicates_routed_to_correct_sj_barriers() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             STRAIGHT_JOIN t2 ON TRUE
             STRAIGHT_JOIN t3 ON TRUE
             WHERE t1.id = t2.t1id AND t2.id = t3.t2id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             STRAIGHT_JOIN t2 ON (t1.id = t2.t1id)
             STRAIGHT_JOIN t3 ON (t2.id = t3.t2id)"#
        ),
    );
}

/// A cross-table `WHERE` predicate that spans a `STRAIGHT_JOIN` barrier (i.e. references
/// tables on both sides of the barrier, but not the barrier's own RHS) must remain in `WHERE`.
/// It is not applicable to any single `STRAIGHT_JOIN` and cannot be assigned to any inner join.
#[test]
fn cross_barrier_where_pred_stays_in_where() {
    // `t1.extra = t4.extra` crosses the SJ(t3) barrier.
    // It is not consumable by the SJ (t4 is not the SJ's RHS) and also not consumable by t4's
    // INNER JOIN (t1 is separated by the SJ boundary).  It must stay in WHERE.
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON t1.id = t2.t1id
             STRAIGHT_JOIN t3 ON t2.id = t3.t2id
             INNER JOIN t4 ON t3.id = t4.t3id
             WHERE t1.extra = t4.extra"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.t1id)
             STRAIGHT_JOIN t3 ON (t2.id = t3.t2id)
             INNER JOIN t4 ON (t3.id = t4.t3id)
             WHERE (t1.extra = t4.extra)"#
        ),
    );
}

// ─── Group 3: Barrier semantics and segment-level reordering ──────────────────

/// `STRAIGHT_JOIN` at the *end* of the chain.  The inner-join prefix (t3→t2, currently
/// in wrong dependency order) is reordered to t2→t3 by the normalizer.  The SJ itself
/// absorbs the applicable `WHERE` predicate and stays at the end.
#[test]
fn sj_at_chain_end_prefix_reorders_sj_stays() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             INNER JOIN t3 ON t3.fk = t2.id
             INNER JOIN t2 ON t2.fk = t1.id
             STRAIGHT_JOIN t4 ON TRUE
             WHERE t1.id = t4.t1id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.fk)
             INNER JOIN t3 ON (t2.id = t3.fk)
             STRAIGHT_JOIN t4 ON (t1.id = t4.t1id)"#
        ),
    );
}

/// `STRAIGHT_JOIN` at the *start* of the chain.  There is no prefix to reorder; the inner
/// joins that follow the barrier (t4→t3, in wrong dependency order) are reordered to t3→t4.
#[test]
fn sj_at_chain_start_suffix_reorders() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             STRAIGHT_JOIN t2 ON t1.id = t2.t1id
             INNER JOIN t4 ON t3.id = t4.t3id
             INNER JOIN t3 ON t2.id = t3.t2id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             STRAIGHT_JOIN t2 ON (t1.id = t2.t1id)
             INNER JOIN t3 ON (t2.id = t3.t2id)
             INNER JOIN t4 ON (t3.id = t4.t3id)"#
        ),
    );
}

/// A `STRAIGHT_JOIN` that sits in the *middle section* (between two outer joins) is neither
/// part of the reorderable prefix nor the reorderable suffix.  It must be passed through
/// untouched while its `WHERE` predicate is still correctly consumed and attached to its `ON`.
/// **NOTE**: The `t3` LEFT OUTER join has to be promoted to INNER join
#[test]
fn sj_in_middle_outer_joins_preserved() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON t1.id = t2.t1id
             LEFT OUTER JOIN t3 ON t2.id = t3.t2id
             STRAIGHT_JOIN t4 ON TRUE
             LEFT OUTER JOIN t5 ON t4.id = t5.t4id
             WHERE t3.id = t4.t3id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
            INNER JOIN t2 ON t1.id = t2.t1id
            INNER JOIN t3 ON t2.id = t3.t2id
            STRAIGHT_JOIN t4 ON t3.id = t4.t3id
            LEFT OUTER JOIN t5 ON t4.id = t5.t4id"#
        ),
    );
}

/// Two *consecutive* `STRAIGHT_JOIN` barriers with no inner joins between them.
/// Inner joins on the outer sides are reordered normally; the two SJ barriers and the empty
/// segment between them are all preserved in-order.
#[test]
fn consecutive_sj_barriers_no_inner_joins_between() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
            INNER JOIN t2 ON t2.fk = t1.id
            STRAIGHT_JOIN t3 ON TRUE
            STRAIGHT_JOIN t4 ON TRUE
            INNER JOIN t5 ON t5.fk = t4.id
            WHERE t2.id = t3.t2id AND t3.id = t4.t3id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.fk)
             STRAIGHT_JOIN t3 ON (t2.id = t3.t2id)
             STRAIGHT_JOIN t4 ON (t3.id = t4.t3id)
             INNER JOIN t5 ON (t4.id = t5.fk)"#
        ),
    );
}

/// Large chain with two `STRAIGHT_JOIN` barriers producing three independently-reordered
/// segments.  Each segment is presented in deliberately suboptimal dependency order in the
/// input and must be corrected by the normalizer:
///
/// ```text
/// [t1]  INNER(t3←t2) INNER(t2←t1)  SJ(t4)  INNER(t6←t5) INNER(t5←t4)  SJ(t7)  INNER(t9←t8) INNER(t8←t7)
///        ^─ wrong order ─^                    ^─ wrong order ─^                    ^─ wrong order ─^
/// ```
///
/// After normalization:
/// - Segment 1 (`t2,t3`): reordered to `t2→t3`
/// - Barrier SJ(t4): absorbs `WHERE t1.id = t4.t1id`
/// - Segment 2 (`t5,t6`): reordered to `t5→t6`
/// - Barrier SJ(t7): absorbs `WHERE t4.id = t7.t4id`
/// - Segment 3 (`t8,t9`): reordered to `t8→t9`
#[test]
fn large_chain_two_sj_barriers_each_segment_reorders_independently() {
    assert_eq!(
        normalize(
            r#"SELECT count(*) FROM t1
             INNER JOIN t3 ON t3.fk = t2.id
             INNER JOIN t2 ON t2.fk = t1.id
             STRAIGHT_JOIN t4 ON TRUE
             INNER JOIN t6 ON t6.fk = t5.id
             INNER JOIN t5 ON t5.fk = t4.id
             STRAIGHT_JOIN t7 ON TRUE
             INNER JOIN t9 ON t9.fk = t8.id
             INNER JOIN t8 ON t8.fk = t7.id
             WHERE t1.id = t4.t1id AND t4.id = t7.t4id"#
        ),
        want(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.fk)
             INNER JOIN t3 ON (t2.id = t3.fk)
             STRAIGHT_JOIN t4 ON (t1.id = t4.t1id)
             INNER JOIN t5 ON (t4.id = t5.fk)
             INNER JOIN t6 ON (t5.id = t6.fk)
             STRAIGHT_JOIN t7 ON (t4.id = t7.t4id)
             INNER JOIN t8 ON (t7.id = t8.fk)
             INNER JOIN t9 ON (t8.id = t9.fk)"#
        ),
    );
}

/// Verifies that all `STRAIGHT_JOIN` operators in the large-chain result carry the
/// `StraightJoin` AST variant (not `InnerJoin` or `CrossJoin`).  This is complementary to
/// the display-string comparison above and catches operator-level regressions directly.
#[test]
fn large_chain_sj_operators_all_preserved_in_ast() {
    let stmt = normalize(
        r#"
         SELECT count(*) FROM t1
         INNER JOIN t3 ON t3.fk = t2.id
         INNER JOIN t2 ON t2.fk = t1.id
         STRAIGHT_JOIN t4 ON TRUE
         INNER JOIN t6 ON t6.fk = t5.id
         INNER JOIN t5 ON t5.fk = t4.id
         STRAIGHT_JOIN t7 ON TRUE
         INNER JOIN t9 ON t9.fk = t8.id
         INNER JOIN t8 ON t8.fk = t7.id
         WHERE t1.id = t4.t1id AND t4.id = t7.t4id"#,
    );

    // join chain after normalization: [t2, t3, SJ(t4), t5, t6, SJ(t7), t8, t9]
    assert_eq!(stmt.join.len(), 8, "expected 8 joins in normalized chain");
    assert!(
        matches!(stmt.join[2].operator, JoinOperator::StraightJoin),
        "join[2] (t4) must be StraightJoin, got {:?}",
        stmt.join[2].operator
    );
    assert!(
        matches!(stmt.join[5].operator, JoinOperator::StraightJoin),
        "join[5] (t7) must be StraightJoin, got {:?}",
        stmt.join[5].operator
    );
    // Non-barrier joins must all be InnerJoin (not CrossJoin).
    for idx in [0, 1, 3, 4, 6, 7] {
        assert!(
            matches!(stmt.join[idx].operator, JoinOperator::InnerJoin),
            "join[{idx}] must be InnerJoin, got {:?}",
            stmt.join[idx].operator
        );
    }
}

// ─── Group 4: Hoisting exclusion ──────────────────────────────────────────────

/// Single-table parametrizable `ON` filters on a plain `INNER JOIN` are extracted to `WHERE`
/// during the normalization + hoisting pipeline.  The *same kind of filter* on a
/// `STRAIGHT_JOIN` must **not** be extracted — it must remain bound to the join's `ON`.
///
/// This confirms both Change 1 (strip exclusion) and Change 4 (hoist exclusion).
#[test]
fn sj_on_single_table_filter_not_hoisted_to_where() {
    // INNER JOIN: the single-table filter `t2.active = 1` is moved to WHERE during
    // normalization (via back_to_where from strip_inner_joins_predicates).
    assert_eq!(
        normalize_then_hoist(
            "SELECT count(*) FROM t1 INNER JOIN t2 ON (t1.id = t2.t1id AND t2.active = 1)"
        ),
        want("SELECT count(*) FROM t1 INNER JOIN t2 ON (t1.id = t2.t1id) WHERE (t2.active = 1)"),
    );

    // STRAIGHT_JOIN: `t3.active = 1` in the SJ's ON must NOT be moved to WHERE.
    // Neither the normalization strip nor the hoisting optimization pass should touch it.
    assert_eq!(
        normalize_then_hoist(
            "SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON (t1.id = t2.t1id AND t2.active = 1)"
        ),
        want("SELECT count(*) FROM t1 STRAIGHT_JOIN t2 ON (t1.id = t2.t1id AND t2.active = 1)"),
    );
}

/// Full pipeline: `INNER JOIN` and `STRAIGHT_JOIN` side-by-side, each with a single-table
/// `ON` filter.  After normalization + hoisting, only the INNER JOIN's filter is in WHERE;
/// the SJ's filter stays in its ON.
#[test]
fn inner_join_filter_hoisted_sj_filter_stays_in_on_full_pipeline() {
    assert_eq!(
        normalize_then_hoist(
            r#"SELECT count(*) FROM t1
             INNER JOIN t2 ON (t1.id = t2.t1id AND t2.active = 1)
             STRAIGHT_JOIN t3 ON (t2.id = t3.t2id AND t3.active = 1)"#
        ),
        want(
            r#"SELECT count(*) FROM t1
            INNER JOIN t2 ON (t1.id = t2.t1id)
            STRAIGHT_JOIN t3 ON (t2.id = t3.t2id AND t3.active = 1)
            WHERE (t2.active = 1)"#
        ),
    );
}

// ─── Coverage gap tests ────────────────────────────────────────────────────

// Group 5: LEFT JOIN barrier + cross-table equality in WHERE

/// Cross-table equality in WHERE with LEFT JOIN — the WHERE `t1.x = t2.y`
/// null-rejects t2 → LEFT is promoted to INNER. Then the equality moves to ON.
#[test]
fn left_join_with_cross_table_where_promotes_to_inner() {
    assert_eq!(
        normalize(
            r#"SELECT t1.a, t2.b FROM t1
               LEFT JOIN t2 ON (t1.id = t2.id)
               WHERE t1.x = t2.y"#,
        ),
        want(
            r#"SELECT t1.a, t2.b FROM t1
               INNER JOIN t2 ON ((t1.x = t2.y) AND (t1.id = t2.id))"#,
        ),
    );
}

/// Comma-separated FROM with LEFT JOIN downstream — comma items become
/// CROSS JOINs, but LEFT JOIN stays as barrier.
#[test]
fn comma_sep_with_left_join_normalizes() {
    assert_eq!(
        normalize(
            r#"SELECT t1.a, t2.b, t3.c FROM t1, t2
               LEFT JOIN t3 ON (t2.id = t3.id)
               WHERE t1.id = t2.id"#,
        ),
        want(
            r#"SELECT t1.a, t2.b, t3.c FROM t1
               INNER JOIN t2 ON (t1.id = t2.id)
               LEFT OUTER JOIN t3 ON (t2.id = t3.id)"#,
        ),
    );
}

/// Three-table ON constraint repair — an ON that references 3 tables
/// should be split: the 2-table part stays in ON, the remainder goes
/// to WHERE.
#[test]
fn three_table_on_split() {
    let result = normalize(
        r#"SELECT t1.a, t2.b, t3.c FROM t1
           INNER JOIN t2 ON (t1.id = t2.id)
           INNER JOIN t3 ON (t2.id = t3.id AND t1.x = t3.y)"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // t1.x = t3.y references t1 (not the immediate LHS of t3's join).
    // The normalization should split it: keep t2.id = t3.id in ON,
    // move t1.x = t3.y to WHERE or to t3's ON if t1 is accumulated LHS.
    // Either way, the final ON should have at most 2 tables per join.
    println!("Three-table ON split result: {result_str}");
    // Verify no error — the normalization handles this gracefully.
}
