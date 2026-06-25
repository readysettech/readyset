//! Tests for `normalize_subquery_positions`.  Stage 1 focuses on HAVING wrap.

use readyset_sql::ast::{SelectStatement, SqlQuery};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

use crate::normalize_subquery_positions::NormalizeSubqueryPositions;

const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

fn parse(sql: &str) -> SelectStatement {
    parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql).expect("parses")
}

fn normalize(sql: &str) -> SelectStatement {
    let mut stmt = parse(sql);
    stmt.normalize_subquery_positions()
        .expect("normalize_subquery_positions succeeds");
    stmt
}

fn rendered(stmt: &SelectStatement) -> String {
    stmt.display(Dialect::PostgreSQL).to_string()
}

// HAVING wrap -- correlated scalar subquery

#[test]
fn having_correlated_scalar_subquery_wraps() {
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > (SELECT max(o.v) FROM outer_t AS o WHERE o.k = a.x)",
    );

    let s = rendered(&stmt);

    // Outer wrapper appears.
    assert!(s.contains("\"_NSP_W_"), "wrapper alias absent: {s}");

    // Outer WHERE references the wrapper-projected aggregate.
    assert!(
        s.to_lowercase().contains("where"),
        "outer WHERE absent: {s}"
    );

    // HAVING is gone from the outer (post-wrap, outer has no HAVING).
    let outer_having_present = stmt.having.is_some();
    assert!(
        !outer_having_present,
        "outer HAVING should be empty post-wrap"
    );

    // The wrap must have fired -- the outer's leading FROM is a wrap
    // derived table (alias prefix `_NSP_W_`).
    assert!(wrap_fired_at(&stmt), "wrap should have fired: {s}");
}

#[test]
fn having_uncorrelated_scalar_subquery_wraps() {
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > (SELECT count(*) FROM unrelated_t)",
    );

    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_"), "wrapper absent: {s}");
    assert!(stmt.having.is_none(), "outer HAVING should be empty");
    assert!(wrap_fired_at(&stmt));
}

#[test]
fn having_multi_conjunct_per_conjunct_split() {
    // count(*) > 5 is non-subquery -- should remain in inner HAVING.
    // count(*) IN (...) is subquery-bearing -- moves to outer WHERE.
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > 5 AND count(*) IN (SELECT n FROM ns)",
    );

    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_"), "wrapper absent: {s}");

    // The inner SELECT (one inside the FROM derived table) should still
    // have HAVING -- the non-subquery conjunct stays there.
    let inner_having_present = first_derived_table_inner(&stmt)
        .map(|inner| inner.having.is_some())
        .unwrap_or(false);
    assert!(
        inner_having_present,
        "inner HAVING should retain the non-subquery conjunct"
    );
}

#[test]
fn having_subquery_body_local_columns_not_rebound() {
    // Invariant: the subquery body's WHERE references `o.k` (body-local
    // alias for `outer_t`).  The body-rebind closure filters by the OUTER
    // stmt's local relations, so body-local column refs stay as-is and
    // the wrap inner SELECT-list does not project a column whose source
    // FROM item is absent from the inner.
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > (SELECT max(o.v) FROM outer_t AS o WHERE o.k = a.x)",
    );

    let inner = first_derived_table_inner(&stmt).expect("wrap inner SELECT exists");

    // Wrap inner SELECT-list must NOT carry any reference to `outer_t.*`
    // -- that relation is in the subquery body's FROM, not the inner's.
    let inner_fields_render = inner
        .fields
        .iter()
        .map(|f| f.display(Dialect::PostgreSQL).to_string())
        .collect::<Vec<_>>();
    assert!(
        !inner_fields_render
            .iter()
            .any(|f| f.contains("outer_t") || f.starts_with("o.")),
        "wrap inner SELECT-list must not project body-local columns; got {inner_fields_render:?}"
    );

    // The subquery in the outer WHERE must still reference its own
    // FROM (i.e. `outer_t.k` resolves locally, not `_NSP_W.k`).
    let rendered_outer = stmt
        .where_clause
        .as_ref()
        .map(|w| w.display(Dialect::PostgreSQL).to_string())
        .unwrap_or_default();
    let wrap_alias = wrap_alias_of(&stmt).expect("wrap should have fired");
    let wrap_k = format!("\"{wrap_alias}\".\"k\"");
    let wrap_v = format!("\"{wrap_alias}\".\"v\"");
    assert!(
        !rendered_outer.contains(&wrap_k),
        "body-local `o.k` was wrongly rebound to {wrap_alias}: {rendered_outer}"
    );
    assert!(
        !rendered_outer.contains(&wrap_v),
        "body-local `o.v` was wrongly rebound to {wrap_alias}: {rendered_outer}"
    );
}

#[test]
fn having_subquery_body_shadows_outer_alias() {
    // Regression: when the subquery body's own FROM aliases a different
    // relation under the same name as an outer FROM item, columns inside the
    // body that use that name resolve to the body's local FROM, NOT to a
    // correlation against outer.  The body-rebind closure must exclude such
    // shadowed names from `effective_outer = stmt_rels - body_locals`.
    //
    // Shape: outer has TWO FROM items `a` (shadowed name) and `b` (non-
    // shadowed).  Body redeclares `a` under a different underlying relation
    // and references both `a.x` (body-local via shadow) and `b.k`
    // (correlation to outer's `b`).  A shadow-blind implementation would
    // rebind body's `a.x` to `_NSP_W.x`, projecting `x` from outer's `a`
    // (wrong).  The correct behavior projects only `b.k` and leaves `a.x`
    // pointing at the body's own FROM.
    let stmt = normalize(
        "SELECT a.city, b.k, count(*) AS cnt \
         FROM t AS a, u AS b \
         GROUP BY a.city, b.k \
         HAVING count(*) > (SELECT min(a.x) FROM other AS a WHERE b.k > 0)",
    );

    let inner = first_derived_table_inner(&stmt).expect("wrap inner SELECT exists");

    // The inner SELECT-list must carry b.k (correlation projected) but NOT
    // a.x (body-local, shadow subtracted).
    let inner_fields_render = inner
        .fields
        .iter()
        .map(|f| f.display(Dialect::PostgreSQL).to_string())
        .collect::<Vec<_>>();
    assert!(
        inner_fields_render
            .iter()
            .any(|f| f.contains("\"b\".\"k\"")),
        "correlation `b.k` should be projected into wrap inner; got {inner_fields_render:?}"
    );
    assert!(
        !inner_fields_render
            .iter()
            .any(|f| f.contains("\"a\".\"x\"")),
        "shadowed body-local `a.x` must NOT be projected into wrap inner; got {inner_fields_render:?}"
    );

    // Outer WHERE: rendered form should contain _NSP_W.k (from b.k rebind)
    // but not _NSP_W.x (which would indicate the shadowed a.x was rebound).
    let rendered_outer = stmt
        .where_clause
        .as_ref()
        .map(|w| w.display(Dialect::PostgreSQL).to_string())
        .unwrap_or_default();
    let wrap_alias = wrap_alias_of(&stmt).expect("wrap should have fired");
    let wrap_k = format!("\"{wrap_alias}\".\"k\"");
    let wrap_x = format!("\"{wrap_alias}\".\"x\"");
    assert!(
        rendered_outer.contains(&wrap_k),
        "correlation `b.k` should have been rebound to {wrap_alias}.k in outer WHERE; got {rendered_outer}"
    );
    assert!(
        !rendered_outer.contains(&wrap_x),
        "shadowed body-local `a.x` must NOT be rebound to {wrap_alias}.x; got {rendered_outer}"
    );
}

// Unified post-GROUP-BY migration (HAVING + TOP-K)

#[test]
fn having_subquery_with_limit_migrates_limit_to_wrapper() {
    // Critical soundness invariant: when wrap fires, LIMIT moves to wrapper.
    // Inner LIMIT clamping rows BEFORE the wrapper's moved HAVING applies
    // would produce a different result set than the original semantics.
    let stmt = normalize(
        "SELECT a.city, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.city \
         HAVING count(*) > (SELECT count(*) FROM other AS o WHERE o.k = a.city) \
         LIMIT 5",
    );

    // Wrapper has the LIMIT.
    assert_eq!(
        stmt.limit_clause.limit().and_then(|l| match l {
            readyset_sql::ast::Literal::Integer(n) => Some(*n),
            _ => None,
        }),
        Some(5),
        "wrapper.limit_clause should carry LIMIT 5; got {:?}",
        stmt.limit_clause
    );

    // Inner has no LIMIT.
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(
        inner.limit_clause.limit().is_none(),
        "inner.limit_clause should be empty; got {:?}",
        inner.limit_clause
    );
}

#[test]
fn having_subquery_with_order_by_and_limit_migrates_both() {
    // Combined ORDER BY + LIMIT migration alongside HAVING wrap.
    let stmt = normalize(
        "SELECT a.city, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.city \
         HAVING count(*) > (SELECT count(*) FROM other AS o WHERE o.k = a.city) \
         ORDER BY count(*) DESC \
         LIMIT 2",
    );

    // Wrapper has both ORDER BY and LIMIT.
    assert!(
        stmt.order.is_some(),
        "wrapper.order should carry ORDER BY; got None"
    );
    assert_eq!(
        stmt.limit_clause.limit().and_then(|l| match l {
            readyset_sql::ast::Literal::Integer(n) => Some(*n),
            _ => None,
        }),
        Some(2),
    );

    // Inner has neither.
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(inner.order.is_none(), "inner.order should be None");
    assert!(
        inner.limit_clause.limit().is_none(),
        "inner.limit_clause should be empty"
    );

    // Wrapper ORDER BY references _NSP_W.<projected_alias>, not the raw aggregate.
    let order_rendered = stmt
        .order
        .as_ref()
        .map(|o| {
            o.order_by
                .iter()
                .map(|item| item.field.display(Dialect::PostgreSQL).to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(
        order_rendered.iter().any(|s| s.contains("_NSP_W_")),
        "wrapper ORDER BY should reference _NSP_W_<N> alias; got {order_rendered:?}"
    );
}

#[test]
fn having_subquery_with_order_by_no_limit_migrates_order_by_too() {
    // Uniform migration: even without LIMIT, ORDER BY moves to wrapper.
    // (Leaving ORDER BY in inner would be sound here -- wrapper filter
    // preserves order -- but the unified mechanism migrates uniformly.)
    let stmt = normalize(
        "SELECT a.city, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.city \
         HAVING count(*) > (SELECT count(*) FROM other AS o WHERE o.k = a.city) \
         ORDER BY count(*) DESC",
    );

    assert!(stmt.order.is_some(), "wrapper.order should carry ORDER BY");
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(inner.order.is_none(), "inner.order should be None");
}

// ORDER BY-subquery trigger (no HAVING)

#[test]
fn order_by_correlated_subquery_wraps() {
    // ORDER BY-only trigger: no HAVING, but ORDER BY contains a correlated
    // subquery.  Wrap fires, ORDER BY item projected to inner SELECT-list,
    // wrapper ORDER BY references the projected alias.
    let stmt = normalize(
        "SELECT a.x \
         FROM t AS a \
         ORDER BY (SELECT max(o.v) FROM outer_t AS o WHERE o.k = a.x)",
    );

    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_"), "wrapper alias absent: {s}");

    // Inner has no ORDER BY.
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(
        inner.order.is_none(),
        "inner.order must be None post-wrap; got {:?}",
        inner.order
    );

    // Wrapper has the ORDER BY.
    assert!(stmt.order.is_some(), "wrapper.order must carry ORDER BY");
}

#[test]
fn order_by_compound_expression_with_subquery_wraps() {
    // Compound ORDER BY expression that contains a subquery (`a.x + (SELECT ...)`).
    // Projection wraps the WHOLE expression, not just the subquery part.
    let stmt = normalize(
        "SELECT a.x \
         FROM t AS a \
         ORDER BY a.x + (SELECT max(o.v) FROM outer_t AS o)",
    );

    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_"), "wrapper absent: {s}");

    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(inner.order.is_none(), "inner.order must be None");
}

#[test]
fn order_by_no_subquery_no_wrap_without_having() {
    // No HAVING, ORDER BY without subquery -- neither trigger fires; no wrap.
    let stmt = normalize("SELECT a.x FROM t AS a ORDER BY a.x");
    let s = rendered(&stmt);
    assert!(
        !s.contains("\"_NSP_W_"),
        "no trigger fired, should not wrap: {s}"
    );
}

#[test]
fn order_by_subquery_with_limit_migrates_limit_to_wrapper() {
    // ORDER BY-trigger + LIMIT -- wrap fires, LIMIT migrates to wrapper.
    let stmt = normalize(
        "SELECT a.x \
         FROM t AS a \
         ORDER BY (SELECT max(o.v) FROM outer_t AS o WHERE o.k = a.x) \
         LIMIT 5",
    );

    assert_eq!(
        stmt.limit_clause.limit().and_then(|l| match l {
            readyset_sql::ast::Literal::Integer(n) => Some(*n),
            _ => None,
        }),
        Some(5)
    );
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(inner.limit_clause.limit().is_none());
}

// HAVING wrap -- no-op cases

#[test]
fn no_having_no_wrap() {
    let stmt = normalize("SELECT a.x FROM t AS a");
    let s = rendered(&stmt);
    assert!(!s.contains("\"_NSP_W_"), "should not wrap: {s}");
}

#[test]
fn triple_nested_partial_shadow_rebinds_non_shadowed_outer_ref() {
    // Multi-outer scenario where a nested body shadows SOME but not ALL of
    // the outer relations.  Under a coarse "stop-descend-on-any-shadow"
    // policy, refs to the non-shadowed outer relations at deeper levels
    // would silently escape rebinding.  `deep_columns_visitor_mut_in_set`
    // tracks per-level scope so `b.k` at level 2 (below a level-2 shadow
    // of outer's `a` but with `b` still live) still rebinds correctly.
    //
    // Shape:
    //   outer:   FROM t AS a, u AS b                stmt_rels = {a, b}
    //   body:    FROM v AS c                        body_locals = {c}
    //   level 2: FROM w AS a WHERE ... > b.k        body_locals = {a}
    //                                                 (a shadowed, b live)
    let stmt = normalize(
        "SELECT a.x, b.k, count(*) AS cnt \
         FROM t AS a, u AS b \
         GROUP BY a.x, b.k \
         HAVING count(*) > (\
             SELECT max(c.v) \
             FROM v AS c \
             WHERE c.v > (SELECT max(w.q) FROM w AS a WHERE w.q > b.k)\
         )",
    );
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    let inner_fields_render = inner
        .fields
        .iter()
        .map(|f| f.display(Dialect::PostgreSQL).to_string())
        .collect::<Vec<_>>();

    // Level 2 references `b.k` (correlation to outer's `b`, not shadowed).
    // The wrap must project `b.k` into the inner SELECT-list so the
    // decorrelation can bind the correlation.
    assert!(
        inner_fields_render
            .iter()
            .any(|f| f.contains("\"b\".\"k\"")),
        "level-2 correlation `b.k` should be projected into wrap inner; got {inner_fields_render:?}"
    );

    // Level 2 also references `w.q` (body-local at level 2 via the `w AS a`
    // shadow).  Under proper per-level scope, that ref is NOT an outer
    // correlation and must NOT be projected into inner.
    assert!(
        !inner_fields_render
            .iter()
            .any(|f| f.contains("\"w\".\"q\"") || f.contains("\"a\".\"q\"")),
        "level-2 shadowed body-local `w.q` must NOT be projected into wrap inner; got {inner_fields_render:?}"
    );
}

#[test]
fn distinct_with_having_subquery_lifts_distinct_to_wrapper() {
    // Wrapping a `SELECT DISTINCT` widens the inner tuple with synthetic
    // projections; running DISTINCT there would admit rows the original
    // collapses.  Instead, DISTINCT lifts to the wrapper, which projects
    // only the user-original fields -- same dedup tuple as the original.
    let stmt = normalize(
        "SELECT DISTINCT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > (SELECT count(*) FROM other)",
    );
    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_"), "wrapper alias absent: {s}");

    // Wrapper carries DISTINCT.
    assert!(stmt.distinct, "wrapper.distinct should be true after lift");

    // Inner is stripped of DISTINCT.
    let inner = first_derived_table_inner(&stmt).expect("wrap fired");
    assert!(
        !inner.distinct,
        "inner.distinct should be false after lift; got {}",
        inner.distinct
    );
}

#[test]
fn order_by_alias_ref_to_select_list_subquery_does_not_wrap() {
    // ORDER BY trigger fires only when the ORDER BY item syntactically
    // contains a subquery in the user query.  An alias-ref to a SELECT-list
    // subquery expression is handled by the ordinary SELECT-list processing
    // (the subquery is projected once per row and sorted on) and does not
    // need the wrap layer.  Denormalize substitutes the alias with the
    // subquery expression, so a post-denormalize check would mistake this
    // for an ORDER BY subquery.  The pre-denormalize check in
    // `visit_select_statement` preserves the distinction.
    let stmt = normalize(
        "SELECT a.x, (SELECT max(v) FROM u) AS m \
         FROM t AS a \
         ORDER BY m",
    );
    let s = rendered(&stmt);
    assert!(
        !s.contains("\"_NSP_W_"),
        "ORDER BY on alias-ref to SELECT-list subquery must not wrap: {s}"
    );
}

#[test]
fn top_level_or_having_subquery_does_not_wrap() {
    // `collect_subquery_predicates` splits AND-conjuncts.  A top-level OR
    // between a subquery-bearing predicate and a plain predicate is one
    // atomic constraint, so the whole predicate goes to
    // `is_supported_subquery_predicate`, which rejects it (only single-
    // relation atomic shapes are supported).  The wrap therefore does not
    // fire, and the C1 backstop gate at `unnest_all_subqueries` rejects
    // the unwrapped shape cleanly rather than letting it drift downstream.
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > (SELECT count(*) FROM other) OR a.x > 0",
    );
    let s = rendered(&stmt);
    assert!(
        !s.contains("\"_NSP_W_"),
        "OR-conjunct HAVING+subquery must not wrap: {s}"
    );
}

#[test]
fn having_without_subquery_no_wrap() {
    let stmt = normalize(
        "SELECT a.x, count(*) AS cnt \
         FROM t AS a \
         GROUP BY a.x \
         HAVING count(*) > 5",
    );
    let s = rendered(&stmt);
    assert!(!s.contains("\"_NSP_W_"), "should not wrap: {s}");
    // HAVING is preserved.
    assert!(stmt.having.is_some());
}

// Recursion: HAVING inside a nested derived table

#[test]
fn nested_select_having_wraps_at_its_own_level() {
    // The inner SELECT (inside `dt`) has HAVING + subquery.  Bottom-up
    // recursion should wrap that level; the outer is left untouched.
    let stmt = normalize(
        "SELECT dt.x, dt.cnt \
         FROM (\
            SELECT a.x, count(*) AS cnt \
            FROM t AS a \
            GROUP BY a.x \
            HAVING count(*) > (SELECT max(v) FROM other AS o) \
         ) AS dt",
    );

    let s = rendered(&stmt);
    // Wrapper alias should appear inside the rendered SQL, even though
    // the OUTER stmt itself is untouched.
    assert!(
        s.contains("\"_NSP_W_"),
        "nested SELECT should be wrapped: {s}"
    );
}

// Alias uniqueness

#[test]
fn nested_wraps_get_distinct_aliases() {
    // Two wrappable levels nested within one statement: the outer wraps its
    // own HAVING-subquery, and its body contains a nested SELECT that also
    // has a HAVING-subquery.  `WrapVisitor` walks bottom-up, so the inner
    // level's wrap mints `_NSP_W_0` and the outer level's mints `_NSP_W_1`.
    // Both aliases must appear in the rendered SQL.
    let stmt = normalize(
        "SELECT dt.x, dt.cnt \
         FROM (\
            SELECT a.x, count(*) AS cnt \
            FROM t AS a \
            GROUP BY a.x \
            HAVING count(*) > (SELECT max(v) FROM other AS o) \
         ) AS dt \
         GROUP BY dt.x, dt.cnt \
         HAVING count(*) > (SELECT max(v) FROM other2 AS o2)",
    );

    let s = rendered(&stmt);
    assert!(s.contains("\"_NSP_W_0\""), "inner wrap alias absent: {s}");
    assert!(s.contains("\"_NSP_W_1\""), "outer wrap alias absent: {s}");
}

#[test]
fn wrap_alias_skips_user_reserved_names() {
    // If the user query already contains a subquery aliased `_NSP_W_0`, the
    // wrap must skip that name and mint `_NSP_W_1` instead -- otherwise ILDT
    // could confuse the user's derived table for a wrap output.
    // The user alias is quoted to preserve case through the parser; without
    // quotes it would fold to `_nsp_w_0` (an unrelated identifier from
    // the collision-check standpoint).
    let stmt = normalize(
        "SELECT \"_NSP_W_0\".a, count(*) AS cnt \
         FROM (SELECT 1 AS a) AS \"_NSP_W_0\" \
         GROUP BY \"_NSP_W_0\".a \
         HAVING count(*) > (SELECT max(v) FROM other AS o)",
    );

    let s = rendered(&stmt);
    assert!(
        s.contains("\"_NSP_W_1\""),
        "wrap should have picked `_NSP_W_1` past the user's `_NSP_W_0`: {s}"
    );
}

#[test]
fn duplicate_outer_aliases() {
    // Two bare `.city` refs in the SELECT-list default-alias to the same
    // name.  Without upfront dedup, the wrap would emit an ambiguous
    // `<wrap_alias>.city` ref.  The fire path dedups the inner (second
    // occurrence gets a suffixed inner alias) while snapshotting the
    // outer aliases beforehand -- the outer's `AS <alias>` preserves the
    // user's original (duplicated) alias while the qualified ref uses
    // the deduped inner name.
    let stmt = normalize(
        r#"SELECT s.city, j.city, count(*) AS cnt FROM s, j GROUP BY s.city, j.city
               HAVING count(*) > (SELECT count(*) FROM p);"#,
    );
    let wrap_alias = wrap_alias_of(&stmt).expect("wrap should have fired");
    let s = rendered(&stmt);

    // Outer AS aliases preserved: both bare-`.city` refs project as
    // `city` on the wrapper, matching the user's expected output shape.
    let outer_first = format!("\"{wrap_alias}\".\"city\" AS \"city\"");
    assert!(
        s.contains(&outer_first),
        "outer first projection must be `{wrap_alias}.city AS city`: {s}"
    );
    // Second outer projection references a suffixed inner alias but
    // still exports as `city`.
    let outer_second = format!("\"{wrap_alias}\".\"city0\" AS \"city\"");
    assert!(
        s.contains(&outer_second),
        "outer second projection must reference a deduped inner and still \
         export as `city`: {s}"
    );

    // The moved HAVING predicate references the aggregate's projected
    // alias via the wrap alias.
    assert!(
        s.contains(&format!("\"{wrap_alias}\".\"cnt\"")),
        "moved HAVING predicate should reference `{wrap_alias}.cnt`: {s}"
    );
}

#[test]
fn duplicate_explicit_outer_aliases_preserved() {
    // User explicitly wrote two `foo`s -- legal SQL, and the wrap must
    // preserve them on the outer.  The inner gets a suffixed alias for
    // the second slot to disambiguate the `<wrap_alias>.<inner>` ref.
    let stmt = normalize(
        r#"SELECT s.city AS foo, j.city AS foo, count(*) AS cnt FROM s, j
               GROUP BY s.city, j.city
               HAVING count(*) > (SELECT count(*) FROM p);"#,
    );
    let wrap_alias = wrap_alias_of(&stmt).expect("wrap should have fired");
    let s = rendered(&stmt);

    let outer_first = format!("\"{wrap_alias}\".\"foo\" AS \"foo\"");
    assert!(
        s.contains(&outer_first),
        "outer first projection must be `{wrap_alias}.foo AS foo`: {s}"
    );
    let outer_second = format!("\"{wrap_alias}\".\"foo0\" AS \"foo\"");
    assert!(
        s.contains(&outer_second),
        "explicit `foo` on the second outer projection must be preserved: {s}"
    );
}

// Helpers

/// If `stmt`'s leading FROM item is a derived table, return its inner.
fn first_derived_table_inner(stmt: &SelectStatement) -> Option<&SelectStatement> {
    use readyset_sql::ast::TableExprInner;
    let t = stmt.tables.first()?;
    if let TableExprInner::Subquery(inner) = &t.inner {
        Some(inner.as_ref())
    } else {
        None
    }
}

/// Check whether the outer's leading FROM item is a wrap-produced derived
/// table.  Replaces the pre-refactor `inner.normalize_opaque` check --
/// the wrap alias (prefix `_NSP_W_`) on the outer's leading TableExpr is
/// now the sole signal that a wrap fired at this level.
fn wrap_fired_at(stmt: &SelectStatement) -> bool {
    wrap_alias_of(stmt).is_some()
}

/// Return the wrap alias on the outer's leading TableExpr if the wrap fired
/// at this level.  Enables tests to build specific `<wrap_alias>."col"`
/// substrings without hard-coding a counter value.
fn wrap_alias_of(stmt: &SelectStatement) -> Option<String> {
    stmt.tables
        .first()
        .and_then(|t| t.alias.as_ref())
        .filter(|a| a.as_str().starts_with("_NSP_W_"))
        .map(|a| a.as_str().to_string())
}

// End-to-end via SqlQuery

#[test]
fn sql_query_dispatch_select() {
    let stmt = parse(
        "SELECT a.x, count(*) FROM t AS a GROUP BY a.x \
         HAVING count(*) > (SELECT count(*) FROM ns)",
    );
    let mut query = SqlQuery::Select(stmt);
    query.normalize_subquery_positions().unwrap();

    let SqlQuery::Select(ref result) = query else {
        panic!("expected SqlQuery::Select")
    };
    let s = rendered(result);
    assert!(
        s.contains("\"_NSP_W_"),
        "wrapper absent in SqlQuery dispatch: {s}"
    );
}
