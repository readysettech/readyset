//! Tests for the `hoist_parametrizable_filters` pass.
//!
//! # Invariants under test
//!
//! ## All-or-nothing hoisting
//! Filters either reach the outermost WHERE or stay where they are.  A filter must
//! **never** stop halfway (e.g., escape a deep derived table but land in an intermediate
//! wrapper rather than the outermost WHERE).
//!
//! ## Hoisting barriers
//! The following constructs are hard stops: the pass must not move any filter across them:
//!   1. Non-INNER (LEFT/RIGHT/FULL) join edges.
//!   2. Aggregation/grouping boundaries (`GROUP BY` or aggregate functions in `SELECT`).
//!   3. Window functions (conservative: any statement containing a WF is skipped entirely).
//!
//! ## ON → WHERE promotion
//! Single-table parametrizable ON predicates on INNER JOINs are moved into WHERE before
//! any inter-level hoisting takes place.
//!
//! ## HAVING → WHERE promotion
//! Filters on GROUP BY keys are moved from HAVING to WHERE before hoisting.
//!
//! ## Aggregated-subquery extraction
//! Simple equality (WHERE) and any parametrizable (HAVING) filters inside an aggregated
//! inner-joined subquery are projected out and added to the outer WHERE.
//!
//! ## No duplicates
//! Re-running the pass on an already-hoisted statement must not add the same predicate
//! twice to the outer WHERE.

use crate::derived_tables_rewrite::derived_tables_rewrite_main;
use crate::hoist_parametrizable_filters::hoist_parametrizable_filters;
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

// ─── Test helpers ─────────────────────────────────────────────────────────────

fn parse_pg(sql: &str) -> readyset_sql::ast::SelectStatement {
    parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
        .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"))
}

fn parse_mysql(sql: &str) -> readyset_sql::ast::SelectStatement {
    parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
        .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"))
}

/// Parse `sql` with PostgreSQL dialect, run derived-tables rewrite then hoist pass,
/// and return the final statement for assertion.
fn hoist_pg(sql: &str) -> readyset_sql::ast::SelectStatement {
    let mut stmt = parse_pg(sql);
    derived_tables_rewrite_main(&mut stmt, Dialect::PostgreSQL)
        .unwrap_or_else(|e| panic!("derived_tables_rewrite_main failed: {e}\n  sql: {sql}"));
    hoist_parametrizable_filters(&mut stmt)
        .unwrap_or_else(|e| panic!("hoist_parametrizable_filters failed: {e}\n  sql: {sql}"));
    println!(">>> Hoisted (PG): {}", stmt.display(Dialect::PostgreSQL));
    stmt
}

/// Parse `sql` with MySQL dialect, run derived-tables rewrite then hoist pass.
fn hoist_mysql(sql: &str) -> readyset_sql::ast::SelectStatement {
    let mut stmt = parse_mysql(sql);
    derived_tables_rewrite_main(&mut stmt, Dialect::MySQL)
        .unwrap_or_else(|e| panic!("derived_tables_rewrite_main failed: {e}\n  sql: {sql}"));
    hoist_parametrizable_filters(&mut stmt)
        .unwrap_or_else(|e| panic!("hoist_parametrizable_filters failed: {e}\n  sql: {sql}"));
    println!(">>> Hoisted (MySQL): {}", stmt.display(Dialect::MySQL));
    stmt
}

/// Parse `sql` with PostgreSQL dialect and run the hoist pass alone — no DTR.
/// Lets tests inspect the hoist pass's behavior on a hand-shaped AST without DTR
/// flattening intermediate wrappers. Used by the row-number-cap exclusion tests
/// below: DTR would inline the cap-bearing wrapper, hiding the hoist gate from
/// observation, so those tests need the hoist pass to see the nested shape as
/// written.
fn hoist_only_pg(sql: &str) -> readyset_sql::ast::SelectStatement {
    let mut stmt = parse_pg(sql);
    hoist_parametrizable_filters(&mut stmt)
        .unwrap_or_else(|e| panic!("hoist_parametrizable_filters failed: {e}\n  sql: {sql}"));
    println!(
        ">>> Hoisted only (PG): {}",
        stmt.display(Dialect::PostgreSQL)
    );
    stmt
}

// ─── Group 1: Basic hoisting from aggregated (formerly non-inlinable) subqueries ────
//
// Prior to C.2 dedup, `derived_tables_rewrite_main` left LIMIT-bearing aggregated
// subqueries as derived tables, and the hoist pass extracted parametrizable filters
// from them.  After C.2 dedup, DTR now inlines these subqueries (LIMIT no longer
// blocks the eligibility check for aggregated single-FROM inlinables), so the flat
// merged SELECT already has the filter at the outermost level.  The hoist pass then
// runs on an already-flat statement and is a no-op.  The expected shapes below
// reflect the post-C.2-dedup, post-DTR-flatten output.

/// A simple equality filter in the WHERE of an aggregated subquery: post-C.2 dedup
/// DTR flattens the subquery, merging the inner WHERE directly into the outer SELECT.
/// Hoist then runs on the flat statement (no-op).  Final shape: flat aggregated SELECT
/// with the filter in WHERE, not inside a derived-table wrapper.
#[test]
fn equality_filter_hoisted_from_agg_subquery_where() {
    assert_eq!(
        hoist_pg(
            r#"SELECT sub.x, sub.total
               FROM (SELECT t.x, SUM(t.v) AS total FROM t WHERE t.x = $1 GROUP BY t.x LIMIT 100) AS sub"#,
        ),
        parse_pg(r#"SELECT t.x, SUM(t.v) AS total FROM t WHERE (t.x = $1) GROUP BY t.x LIMIT 100"#,),
    );
}

/// A BETWEEN filter in the HAVING of an aggregated subquery: post-C.2 dedup DTR
/// flattens the subquery.  apply_inline promotes the HAVING filter (on a GROUP BY key)
/// to WHERE during the merge; hoist is a no-op on the resulting flat SELECT.
/// Final shape: flat aggregated SELECT with BETWEEN in WHERE, HAVING cleared.
#[test]
fn between_filter_hoisted_from_agg_subquery_having() {
    assert_eq!(
        hoist_pg(
            r#"SELECT sub.x, sub.total
               FROM (SELECT t.x, SUM(t.v) AS total FROM t GROUP BY t.x HAVING t.x BETWEEN $1 AND $2 LIMIT 100) AS sub"#,
        ),
        parse_pg(
            r#"SELECT t.x, SUM(t.v) AS total FROM t WHERE (t.x BETWEEN $1 AND $2) GROUP BY t.x LIMIT 100"#,
        ),
    );
}

/// An IN-list filter in the HAVING of an aggregated subquery: post-C.2 dedup DTR
/// flattens the subquery.  apply_inline promotes the HAVING IN-list filter (on a
/// GROUP BY key) to WHERE during the merge; hoist is a no-op on the resulting flat
/// SELECT.  Final shape: flat aggregated SELECT with IN in WHERE, HAVING cleared.
#[test]
fn in_list_filter_hoisted_from_agg_subquery_having() {
    assert_eq!(
        hoist_pg(
            r#"SELECT sub.x, sub.total
               FROM (SELECT t.x, SUM(t.v) AS total FROM t GROUP BY t.x HAVING t.x IN ($1, $2, $3) LIMIT 100) AS sub"#,
        ),
        parse_pg(
            r#"SELECT t.x, SUM(t.v) AS total FROM t WHERE (t.x IN ($1, $2, $3)) GROUP BY t.x LIMIT 100"#,
        ),
    );
}

/// A column-column comparison in an aggregated subquery HAVING is NOT parametrizable.
/// Post-C.2 dedup, DTR now inlines (LIMIT no longer blocks single-FROM aggregated
/// inlinables), but the non-parametrizable HAVING stays in HAVING after the merge
/// (apply_inline does not promote column=column predicates to WHERE).
/// Final shape: flat aggregated SELECT with the HAVING intact, no outer wrapper.
#[test]
fn non_parametrizable_filter_stays_in_agg_subquery_having() {
    // `x = y` compares two columns; neither side is a literal/placeholder → not
    // promoted to WHERE by apply_inline, stays in HAVING of the merged flat SELECT.
    assert_eq!(
        hoist_pg(
            r#"SELECT sub.total
               FROM (SELECT SUM(t.v) AS total FROM t GROUP BY t.x HAVING t.x = t.y LIMIT 100) AS sub"#,
        ),
        parse_pg(r#"SELECT SUM(t.v) AS total FROM t GROUP BY t.x HAVING (t.x = t.y) LIMIT 100"#,),
    );
}

// ─── Group 2: All-or-nothing — filters must not stop midway ───────────────────

/// A filter two levels deep must reach the outermost WHERE directly.
/// It must not land in the intermediate wrapper.
#[test]
fn deep_filter_reaches_outermost_where_not_intermediate() {
    // Two levels of plain (non-agg) subqueries.
    let result = hoist_pg(
        r#"SELECT outer.id
           FROM (
               SELECT mid.id
               FROM (
                   SELECT t.id FROM t WHERE t.x = $1
               ) AS mid
           ) AS outer"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();

    // The predicate must be in the outermost WHERE.
    assert!(
        result_str.contains("WHERE"),
        "expected a WHERE clause at the outermost level, got:\n{result_str}"
    );
    // It must NOT be buried inside any nested SELECT … WHERE at a lower level.
    // We check by counting WHERE occurrences: exactly one is expected.
    let where_count = result_str.matches("WHERE").count();
    assert_eq!(
        where_count, 1,
        "expected exactly one WHERE (outermost), got {where_count} in:\n{result_str}"
    );
}

/// Three levels of non-aggregated (inlinable) nesting: `derived_tables_rewrite` collapses
/// all wrapper subqueries so the filter surfaces in the single outermost WHERE.
/// This exercises the combined pipeline's guarantee that a filter never stops midway.
#[test]
fn triple_nested_filter_reaches_outermost_where() {
    // Note: `top` is a reserved keyword in some SQL dialects; use `q_outer` instead.
    let result = hoist_pg(
        r#"SELECT q_outer.id
           FROM (
               SELECT q_mid.id
               FROM (
                   SELECT q_inner.id
                   FROM (
                       SELECT t.id FROM t WHERE t.x = $1
                   ) AS q_inner
               ) AS q_mid
           ) AS q_outer"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    let where_count = result_str.matches("WHERE").count();
    assert_eq!(
        where_count, 1,
        "filter must surface to the single outermost WHERE, found {where_count} in:\n{result_str}"
    );
}

// ─── Group 3: Hoisting barriers — LEFT/RIGHT/FULL JOIN ────────────────────────

/// A filter inside a subquery that is accessed via LEFT JOIN must NOT be hoisted.
/// Lifting across a null-extending boundary changes query semantics.
#[test]
fn filter_in_left_join_subquery_stays() {
    let sql = r#"SELECT t1.id, sub.v
                 FROM t1
                 LEFT JOIN (SELECT t2.id, t2.v FROM t2 WHERE t2.x = $1) AS sub
                 ON t1.id = sub.id"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The outer WHERE must remain absent (or at least not contain x = $1).
    assert!(
        !result_str.contains("WHERE (sub.x"),
        "filter should not be hoisted across LEFT JOIN, got:\n{result_str}"
    );
}

/// A filter inside a subquery accessed via RIGHT JOIN must NOT be hoisted.
#[test]
fn filter_in_right_join_subquery_stays() {
    let sql = r#"SELECT sub.id, t1.v
                 FROM (SELECT t2.id FROM t2 WHERE t2.x = $1) AS sub
                 RIGHT JOIN t1 ON sub.id = t1.id"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        !result_str.contains("WHERE (sub.x"),
        "filter should not be hoisted across RIGHT JOIN, got:\n{result_str}"
    );
}

// ─── Group 4: Hoisting barriers — aggregation ─────────────────────────────────

/// A filter in the WHERE clause of an aggregated subquery that is NOT a simple equality
/// (e.g., a range comparison `>`) must not be hoisted.
/// Only simple equality (= ?) filters can be extracted from aggregated subquery WHEREs.
#[test]
fn range_filter_in_agg_subquery_where_stays() {
    let sql = r#"SELECT sub.total
                 FROM (
                     SELECT SUM(t.v) AS total FROM t WHERE t.x > $1 GROUP BY t.cat
                 ) AS sub"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // x > $1 is not a simple equality — it must not appear in the outer WHERE.
    assert!(
        !result_str.contains("WHERE (sub.x"),
        "non-equality filter should not be hoisted from agg subquery WHERE, got:\n{result_str}"
    );
}

/// A simple equality filter (`col = ?`) in an aggregated subquery's WHERE IS hoisted
/// (after being projected) because it can be applied before aggregation at the outer level.
#[test]
fn equality_filter_in_agg_subquery_where_is_hoisted() {
    let result = hoist_pg(
        r#"SELECT sub.total
           FROM (
               SELECT SUM(t.v) AS total FROM t WHERE t.x = $1 GROUP BY t.cat
           ) AS sub"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The equality filter must be promoted to the outer WHERE.
    assert!(
        result_str.contains("WHERE"),
        "equality filter from agg subquery WHERE should be hoisted, got:\n{result_str}"
    );
}

/// A parametrizable HAVING filter in an aggregated subquery is hoisted to the outer WHERE.
#[test]
fn parametrizable_having_filter_is_hoisted_from_agg_subquery() {
    let result = hoist_pg(
        r#"SELECT sub.total
           FROM (
               SELECT SUM(t.v) AS total FROM t GROUP BY t.cat HAVING t.cat = $1
           ) AS sub"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result_str.contains("WHERE"),
        "parametrizable HAVING filter should be hoisted from aggregated subquery, got:\n{result_str}"
    );
}

/// An aggregated (COUNT/SUM/…) HAVING filter must NOT be hoisted.
#[test]
fn aggregate_having_filter_stays_in_agg_subquery() {
    let sql = r#"SELECT sub.total
                 FROM (
                     SELECT SUM(t.v) AS total FROM t GROUP BY t.cat HAVING SUM(t.v) > $1
                 ) AS sub"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        !result_str.contains("WHERE (sub.total"),
        "aggregate HAVING filter must not be hoisted to outer WHERE, got:\n{result_str}"
    );
}

// ─── Group 5: Hoisting barriers — window functions ────────────────────────────

/// A statement containing a window function at the current level must be entirely skipped —
/// no hoisting from any subquery within it.
#[test]
fn window_function_at_outer_level_blocks_hoisting() {
    let sql = r#"SELECT ROW_NUMBER() OVER (PARTITION BY t.cat ORDER BY t.v) AS rn, sub.id
                 FROM t
                 INNER JOIN (SELECT t2.id FROM t2 WHERE t2.x = $1) AS sub ON t.id = sub.id"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The outer query has a WF → hoisting is entirely blocked; x = $1 must remain inside sub.
    assert!(
        !result_str.contains("WHERE (sub.x"),
        "window function at outer level must block hoisting, got:\n{result_str}"
    );
}

/// A window function inside a subquery blocks hoisting from *within* that subquery.
/// The outer level is unaffected.
#[test]
fn window_function_in_subquery_blocks_hoisting_from_that_subquery() {
    // The WF subquery filter must not surface.
    let sql = r#"SELECT outer.id
                 FROM (
                     SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.v) AS rn
                     FROM t WHERE t.x = $1
                 ) AS outer"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        !result_str.contains("WHERE (outer.x"),
        "WF inside subquery must prevent hoisting from that subquery, got:\n{result_str}"
    );
}

// ─── Group 6: ON → WHERE promotion (INNER JOIN only) ─────────────────────────

/// A single-table parametrizable ON filter on an INNER JOIN is moved to WHERE.
#[test]
fn single_table_on_filter_promoted_to_where_for_inner_join() {
    assert_eq!(
        hoist_mysql("SELECT t1.id, t2.v FROM t1 INNER JOIN t2 ON t1.id = t2.t1id AND t2.x = 42"),
        parse_mysql(
            "SELECT t1.id, t2.v FROM t1 INNER JOIN t2 ON (t1.id = t2.t1id) WHERE (t2.x = 42)"
        ),
    );
}

/// A single-table ON filter on a LEFT JOIN must NOT be promoted to WHERE.
#[test]
fn single_table_on_filter_not_promoted_for_left_join() {
    let sql = "SELECT t1.id, t2.v FROM t1 LEFT JOIN t2 ON t1.id = t2.t1id AND t2.x = 42";
    let result = hoist_mysql(sql);
    let result_str = result.display(Dialect::MySQL).to_string();
    assert!(
        !result_str.contains("WHERE"),
        "ON filter on LEFT JOIN must not be promoted to WHERE, got:\n{result_str}"
    );
}

// ─── Group 7: HAVING → WHERE promotion (group-key filters) ────────────────────

/// A parametrizable HAVING filter whose operand exactly matches a GROUP BY column
/// is moved to WHERE (it evaluates to the same result pre/post aggregation).
#[test]
fn group_key_having_filter_moved_to_where() {
    assert_eq!(
        hoist_pg(
            r#"SELECT employees.dept, COUNT(*) FROM employees GROUP BY employees.dept HAVING employees.dept = $1"#
        ),
        parse_pg(
            r#"SELECT employees.dept, COUNT(*) FROM employees WHERE (employees.dept = $1) GROUP BY employees.dept"#
        ),
    );
}

/// A HAVING filter that references an aggregate (COUNT(*) > ?) must stay in HAVING.
#[test]
fn aggregate_having_filter_stays_in_having() {
    let sql = r#"SELECT employees.dept, COUNT(*) FROM employees GROUP BY employees.dept HAVING COUNT(*) > $1"#;
    let result = hoist_pg(sql);
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result_str.contains("HAVING"),
        "aggregate HAVING filter must stay in HAVING, got:\n{result_str}"
    );
    assert!(
        !result_str.contains("WHERE"),
        "aggregate HAVING filter must not appear in WHERE, got:\n{result_str}"
    );
}

/// When HAVING has both a group-key filter and an aggregate filter, only the group-key
/// filter moves to WHERE; the aggregate filter remains in HAVING.
#[test]
fn mixed_having_split_correctly() {
    let result = hoist_pg(
        r#"SELECT employees.dept, COUNT(*) FROM employees GROUP BY employees.dept
           HAVING employees.dept = $1 AND COUNT(*) > $2"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result_str.contains("WHERE"),
        "group-key HAVING filter should have been moved to WHERE, got:\n{result_str}"
    );
    assert!(
        result_str.contains("HAVING"),
        "aggregate HAVING filter should remain in HAVING, got:\n{result_str}"
    );
}

// ─── Group 8: Idempotency / no duplicates ─────────────────────────────────────

/// Running the pass twice must not duplicate any predicate.  Post-C.2 dedup,
/// `derived_tables_rewrite_main` now inlines the LIMIT-bearing aggregated subquery,
/// merging the HAVING filter into the flat SELECT's WHERE.  The first hoist call
/// is a no-op (the flat SELECT has no hoistable subquery structure), and the second
/// call is also a no-op.  The predicate `"t"."x" = $1` appears exactly once in
/// the final WHERE of the merged flat statement.
#[test]
fn second_pass_does_not_duplicate_predicates() {
    let mut stmt = parse_pg(
        r#"SELECT sub.x, sub.total
           FROM (SELECT t.x, SUM(t.v) AS total FROM t GROUP BY t.x HAVING t.x = $1 LIMIT 100) AS sub"#,
    );
    derived_tables_rewrite_main(&mut stmt, Dialect::PostgreSQL)
        .expect("derived_tables_rewrite_main");

    // Post-C.2 dedup, DTR already flattened: hoist is a no-op on both passes.
    let _changed_1 = hoist_parametrizable_filters(&mut stmt).expect("first hoist");
    let changed_2 = hoist_parametrizable_filters(&mut stmt).expect("second hoist");
    assert!(
        !changed_2,
        "second pass should be a no-op (already flat after DTR inlining)"
    );

    // Verify the full predicate appears exactly once (not duplicated in WHERE).
    // Post-flatten, the predicate is qualified by the source table `t`, not `sub`.
    let result_str = stmt.display(Dialect::PostgreSQL).to_string();
    println!("predicate must appear exactly once in outermost WHERE, got:\n{result_str}");
    let predicate = r#""t"."x" = $1"#;
    let occurrences = result_str.matches(predicate).count();
    assert_eq!(
        occurrences, 1,
        "predicate must appear exactly once in outermost WHERE, got:\n{result_str}"
    );
}

// ─── Group 9: Correct alias qualification ─────────────────────────────────────

/// Post-C.2 dedup: DTR inlines the LIMIT-bearing aggregated subquery (alias `sub_q`),
/// merging the HAVING filter (on a GROUP BY key) into the outer WHERE as `t.x = $1`.
/// The alias `sub_q` disappears in the flat merge; references now point to `t` directly.
/// Hoist is a no-op on the resulting flat SELECT.
#[test]
fn hoisted_filter_is_qualified_with_subquery_alias() {
    assert_eq!(
        hoist_pg(
            r#"SELECT sub_q.x, sub_q.total
               FROM (SELECT t.x, SUM(t.v) AS total FROM t GROUP BY t.x HAVING t.x = $1 LIMIT 100) AS sub_q"#,
        ),
        parse_pg(r#"SELECT t.x, SUM(t.v) AS total FROM t WHERE (t.x = $1) GROUP BY t.x LIMIT 100"#,),
    );
}

// ─── Group 10: Multiple conjuncts — partial hoisting within WHERE ──────────────

/// When a subquery WHERE has multiple conjuncts and only some are parametrizable,
/// the parametrizable ones are hoisted and the rest remain in the subquery.
#[test]
fn only_parametrizable_conjuncts_are_hoisted() {
    let result = hoist_pg(
        r#"SELECT sub.id
           FROM (
               SELECT t.id, t.x FROM t WHERE t.x = $1 AND t.y = t.z
           ) AS sub"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();

    // After derived_tables_rewrite, this simple single-table subquery is inlined into the
    // parent — both conjuncts end up in the same flat WHERE.  The hoist pass sees no subquery
    // to hoist from; it's already flat.  We verify the final WHERE contains both predicates.
    assert!(
        result_str.contains("WHERE"),
        "both predicates should be in WHERE after inlining + hoisting, got:\n{result_str}"
    );
}

// ─── Group 11: INNER JOIN subquery — filter hoisted, LEFT JOIN — not ──────────

/// With two subqueries, one INNER-joined and one LEFT-joined, only the INNER one's
/// filters are hoisted.
#[test]
fn inner_join_subquery_hoisted_left_join_subquery_not() {
    let result = hoist_pg(
        r#"SELECT s1.id, s2.v
           FROM (SELECT t1.id FROM t1 WHERE t1.x = $1) AS s1
           INNER JOIN (SELECT t2.id, t2.v FROM t2 WHERE t2.y = $2) AS s2
               ON s1.id = s2.id
           LEFT JOIN (SELECT t3.id FROM t3 WHERE t3.z = $3) AS s3
               ON s1.id = s3.id"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();

    // s1.x = $1 and s2.y = $2 should be in outer WHERE (both are INNER-joined).
    assert!(
        result_str.contains("WHERE"),
        "at least one filter should be hoisted, got:\n{result_str}"
    );
    // s3.z = $3 must NOT be hoisted (LEFT JOIN barrier).
    assert!(
        !result_str.contains("s3.z"),
        "s3 filter should not be hoisted across LEFT JOIN, got:\n{result_str}"
    );
}

// ─── Group 12: sub_locals guard (M3 fix) + CROSS JOIN barrier ──────────────

/// Self-join bail-out case: the non-aggregated subquery survived derived_tables_rewrite
/// because it contains a table that also exists in the outer FROM. The subquery's WHERE
/// filter references an internal table and should be hoisted using sub_locals matching.
#[test]
fn non_aggregated_subquery_filter_hoisted_via_sub_locals() {
    let result = hoist_pg(
        r#"SELECT "t1"."x" FROM "t1"
           INNER JOIN (SELECT "t1"."x", "t2"."y" FROM "t1"
                       INNER JOIN "t2" ON "t1"."id" = "t2"."id"
                       WHERE "t1"."x" = $1) AS "sq"
           ON "t1"."x" = "sq"."x""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The filter t1.x = $1 should be hoisted to the outer WHERE (rebound to sq.x = $1).
    assert!(
        result_str.matches("WHERE").count() >= 1,
        "sub_locals-matched filter should be hoisted, got:\n{result_str}"
    );
}

/// CROSS JOIN subquery: CROSS JOIN is_inner_join() = true, so hoisting should proceed.
#[test]
fn cross_join_subquery_filter_hoisted() {
    let result = hoist_pg(
        r#"SELECT "t1"."x" FROM "t1"
           CROSS JOIN (SELECT "t2"."y" FROM "t2" WHERE "t2"."y" = $1) AS "sq""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // CROSS JOIN is treated as INNER — the filter should be hoisted.
    assert!(
        result_str.matches("WHERE").count() >= 1,
        "filter behind CROSS JOIN should be hoisted, got:\n{result_str}"
    );
}

// ─── Negative tests: filters that must NOT be hoisted ──────────────────────

/// HAVING with mixed GROUP BY key + aggregate — must NOT move to WHERE.
/// `HAVING x > COUNT(*)` contains an aggregate → stays in HAVING.
#[test]
fn having_mixed_group_key_and_aggregate_stays() {
    let result = hoist_pg(
        r#"SELECT "t"."x", COUNT(*) AS "cnt"
           FROM "t"
           GROUP BY "t"."x"
           HAVING "t"."x" > COUNT(*)"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The HAVING predicate references both a GROUP BY key and an aggregate.
    // It must NOT be moved to WHERE.
    assert!(
        result_str.contains("HAVING"),
        "mixed GROUP BY + aggregate HAVING should stay, got:\n{result_str}"
    );
}

/// Aggregated subquery WHERE with BETWEEN on GROUP BY key — now extracted.
/// The column t.a IS a GROUP BY key, so the BETWEEN filter eliminates entire
/// groups and can safely be hoisted.
#[test]
fn aggregated_subquery_where_between_on_gb_key_extracted() {
    let result = hoist_pg(
        r#"SELECT "sq"."s"
           FROM (SELECT SUM("t"."x") AS "s", "t"."a"
                 FROM "t"
                 WHERE "t"."a" BETWEEN 1 AND 10
                 GROUP BY "t"."a") AS "sq""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // t.a IS a GROUP BY key → BETWEEN is extracted to outer WHERE.
    assert!(
        result_str.contains("BETWEEN") && result_str.contains("WHERE"),
        "BETWEEN on GROUP BY key should be hoisted to outer WHERE, got:\n{result_str}"
    );
}

/// Aggregated subquery WHERE with BETWEEN on non-GROUP-BY column — must NOT
/// be extracted (the column needs GROUP BY promotion, which is only safe for
/// equalities).
#[test]
fn aggregated_subquery_where_between_on_non_gb_key_stays() {
    let result = hoist_pg(
        r#"SELECT "sq"."s"
           FROM (SELECT SUM("t"."x") AS "s", "t"."a"
                 FROM "t"
                 WHERE "t"."x" BETWEEN 1 AND 10
                 GROUP BY "t"."a") AS "sq""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // t.x is NOT a GROUP BY key → BETWEEN must NOT be hoisted.
    assert!(
        !result_str.ends_with("10)"),
        "BETWEEN on non-GROUP-BY column should NOT be hoisted, got:\n{result_str}"
    );
}

/// Aggregated subquery WHERE with IN on GROUP BY key — extracted.
#[test]
fn aggregated_subquery_where_in_on_gb_key_extracted() {
    let result = hoist_pg(
        r#"SELECT "sq"."s"
           FROM (SELECT SUM("t"."x") AS "s", "t"."a"
                 FROM "t"
                 WHERE "t"."a" IN (1, 2, 3)
                 GROUP BY "t"."a") AS "sq""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // t.a IS a GROUP BY key → IN is extracted.
    assert!(
        result_str.contains("IN") && result_str.contains("WHERE"),
        "IN on GROUP BY key should be hoisted to outer WHERE, got:\n{result_str}"
    );
}

/// Aggregated subquery WHERE with range (>) on GROUP BY key — extracted.
#[test]
fn aggregated_subquery_where_range_on_gb_key_extracted() {
    let result = hoist_pg(
        r#"SELECT "sq"."s"
           FROM (SELECT SUM("t"."x") AS "s", "t"."a"
                 FROM "t"
                 WHERE "t"."a" > 5
                 GROUP BY "t"."a") AS "sq""#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // t.a IS a GROUP BY key → range is extracted.
    assert!(
        result_str.contains("> 5") && result_str.contains("WHERE"),
        "range on GROUP BY key should be hoisted to outer WHERE, got:\n{result_str}"
    );
}

/// Filter behind LEFT JOIN — must NOT be hoisted across the LEFT JOIN barrier.
#[test]
fn filter_behind_left_join_stays() {
    let result = hoist_pg(
        r#"SELECT "t1"."a", "sq"."b"
           FROM "t1"
           LEFT JOIN (SELECT "t2"."a", "t2"."b" FROM "t2"
                      WHERE "t2"."b" = 5) AS "sq"
           ON ("t1"."a" = "sq"."a")"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The filter t2.b = 5 is behind a LEFT JOIN → must NOT be hoisted.
    assert!(
        !result_str.ends_with("= 5)"),
        "filter behind LEFT JOIN should not be hoisted to outer WHERE, got:\n{result_str}"
    );
}

#[test]
fn filter_in_limit_bearing_subquery_stays() {
    // Lifting a filter out of a LIMIT-bearing subquery's WHERE would change which
    // rows survive the LIMIT — a different output set. The hoist pass must treat
    // LIMIT as a hard barrier (parallel to its window-function barrier).
    //
    // In production today, the TOP-K rewrite synthesises a `ROW_NUMBER()`
    // projection for every LIMIT-bearing subquery upstream, so hoist's
    // `contains_wf!` guard catches them transitively. This regression test
    // pins the explicit LIMIT guard so that any pipeline change letting a
    // LIMIT-bearing subquery reach hoist without a WF still preserves the
    // soundness invariant.
    let result = hoist_pg(
        r#"SELECT "sq"."x"
           FROM (SELECT "t"."x", "t"."y" FROM "t"
                 WHERE "t"."y" > 0
                 ORDER BY "t"."x"
                 LIMIT 10) AS "sq"
           WHERE "sq"."x" > 100"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The inner filter "t"."y" > 0 must stay inside the subquery — verified by
    // its substring presence and by the outer WHERE not absorbing it.
    assert!(
        result_str.contains(r#""t"."y" > 0"#),
        "filter under LIMIT must stay inside the subquery; got:\n{result_str}"
    );
    // The outer WHERE keeps only its own filter (sq.x > 100), not the lifted one.
    assert!(
        !result_str.contains(r#""sq"."y" > 0"#),
        "filter under LIMIT must not be rebound at the outer level; got:\n{result_str}"
    );
}

// Row-number cap predicates — must stay attached to their projecting subquery
//
// `predicate_caps_row_number` recognizes any `rn op K` predicate whose `rn` column
// aliases a `ROW_NUMBER()` projection. Hoisting such a predicate to a parent WHERE
// detaches it from the projection that defines `rn`, breaking the cap semantics:
// the literal `K` is load-bearing as a cardinality signal for downstream consumers
// (auto-parameterize, CBJR), and a hoist would expose it to placeholder substitution.
// The exclusion applies whether `rn` is hand-aliased or the synthetic `__rn` produced
// by `rewrite_top_k_in_place_impl`.
//
// These tests exercise the hoist pass in isolation (`hoist_only_pg`) so that DTR
// flattening does not move the cap predicate before hoist gets to see it: each
// shape is a hand-built post-DTR-resistant nest with the cap inside a wrapper's
// WHERE, and the assertion is that hoist leaves the cap in that wrapper.

/// Cap on an unqualified `rn` reference inside a wrapping subquery's WHERE.
/// Hoist would otherwise promote `rn <= 10` to the outer WHERE; the exclusion
/// keeps it pinned to the wrapper.
#[test]
fn row_number_cap_stays_in_subquery_unqualified() {
    let result = hoist_only_pg(
        r#"SELECT b.id, b.rn
           FROM (SELECT inner_sub.id, inner_sub.rn
                 FROM (SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.id) AS rn FROM t) AS inner_sub
                 WHERE rn <= 10) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The outer SELECT must not gain a WHERE — cap stays inside `b`.
    assert!(
        result.where_clause.is_none(),
        "outer WHERE must remain empty (cap should not hoist), got:\n{result_str}"
    );
    assert!(
        result_str.contains("<= 10"),
        "cap predicate must still be present in the wrapper, got:\n{result_str}"
    );
}

/// Cap on a qualified `inner_sub.rn` reference: same expectation. Qualifiers
/// must not let the cap escape the wrapper.
#[test]
fn row_number_cap_stays_in_subquery_qualified() {
    let result = hoist_only_pg(
        r#"SELECT b.id, b.rn
           FROM (SELECT inner_sub.id, inner_sub.rn
                 FROM (SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.id) AS rn FROM t) AS inner_sub
                 WHERE inner_sub.rn <= 10) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result.where_clause.is_none(),
        "outer WHERE must remain empty (qualified cap should not hoist), got:\n{result_str}"
    );
    assert!(
        result_str.contains("<= 10"),
        "qualified cap predicate must still be present, got:\n{result_str}"
    );
}

/// The synthetic `__rn` alias used by `rewrite_top_k_in_place_impl` is recognized
/// just like a user-defined alias.
#[test]
fn synthetic_underscore_rn_cap_stays_in_subquery() {
    let result = hoist_only_pg(
        r#"SELECT b.id, b.__rn
           FROM (SELECT inner_sub.id, inner_sub.__rn
                 FROM (SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.id) AS __rn FROM t) AS inner_sub
                 WHERE inner_sub.__rn <= 100) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result.where_clause.is_none(),
        "outer WHERE must remain empty for __rn cap, got:\n{result_str}"
    );
    assert!(
        result_str.contains("<= 100"),
        "__rn cap predicate must still be present, got:\n{result_str}"
    );
}

/// A partitioned `ROW_NUMBER()` cap is recognized the same way: the partition list
/// is captured by the recognizer; the cap predicate is still excluded from hoisting.
#[test]
fn partitioned_row_number_cap_stays_in_subquery() {
    let result = hoist_only_pg(
        r#"SELECT b.customer_id, b.rn
           FROM (SELECT inner_sub.customer_id, inner_sub.rn
                 FROM (SELECT orders.customer_id,
                              ROW_NUMBER() OVER (PARTITION BY orders.customer_id ORDER BY orders.ts) AS rn
                       FROM orders) AS inner_sub
                 WHERE inner_sub.rn <= 3) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    assert!(
        result.where_clause.is_none(),
        "outer WHERE must remain empty for partitioned cap, got:\n{result_str}"
    );
    assert!(
        result_str.contains("<= 3"),
        "partitioned cap predicate must still be present, got:\n{result_str}"
    );
}

/// Aggregated-subquery path: a cap inside a GROUP-BY-bearing wrapper routes through
/// `extract_filters_from_aggregated_subquery_stmt` rather than the general FROM-subquery
/// WHERE-extraction path. The wrapper aggregates with `COUNT(*)` over `GROUP BY rn`,
/// which makes `is_aggregation_or_grouped` true and steers the pass into the aggregated
/// branch. The cap predicate `inner_sub.rn <= 10` is on the GROUP BY key, so the
/// pre-step would move it from WHERE to HAVING and the HAVING-extraction would then
/// pull it out to the outer WHERE — unless the exclusion holds it in place.
///
/// Without all three gates inside `extract_filters_from_aggregated_subquery_stmt`,
/// the cap surfaces at the outer WHERE as `b.rn <= 10`; with the gates, the wrapper
/// retains the predicate intact in its WHERE clause.
#[test]
fn row_number_cap_stays_in_aggregated_subquery() {
    let result = hoist_only_pg(
        r#"SELECT b.rn, b.cnt
           FROM (SELECT inner_sub.rn, COUNT(*) AS cnt
                 FROM (SELECT t.x, ROW_NUMBER() OVER (ORDER BY t.x) AS rn FROM t) AS inner_sub
                 WHERE inner_sub.rn <= 10
                 GROUP BY inner_sub.rn) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // Outer SELECT must not gain a WHERE — cap must not escape the aggregated wrapper.
    assert!(
        result.where_clause.is_none(),
        "outer WHERE must remain empty (aggregated-path cap must not hoist), got:\n{result_str}"
    );
    // Cap must remain inside the wrapper, not relocate to HAVING either.
    assert!(
        result_str.contains("WHERE (\"inner_sub\".\"rn\" <= 10)"),
        "cap predicate must stay in the wrapper's WHERE, got:\n{result_str}"
    );
}

/// Negative control: a non-RN integer comparison in the same shape still hoists.
/// Proves the exclusion is targeted to `ROW_NUMBER()`-aliased columns and does
/// not block general hoisting of comparable integer filters.
#[test]
fn non_row_number_int_filter_still_hoists() {
    let result = hoist_only_pg(
        r#"SELECT b.id, b.rn
           FROM (SELECT inner_sub.id, inner_sub.rn
                 FROM (SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.id) AS rn FROM t) AS inner_sub
                 WHERE inner_sub.id <= 10) AS b"#,
    );
    let result_str = result.display(Dialect::PostgreSQL).to_string();
    // The non-RN filter on `id` should reach the outer WHERE.
    assert!(
        result.where_clause.is_some(),
        "non-RN int filter must reach outer WHERE, got:\n{result_str}"
    );
    assert!(
        result_str.contains("<= 10"),
        "non-RN cap predicate must still be present, got:\n{result_str}"
    );
}
