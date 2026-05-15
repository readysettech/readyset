//! Inline Leading Derived Table
//!
//! This pass inlines (or "hoists") the leftmost derived table (subquery in FROM) into
//! the outer query, flattening the query structure when semantically safe.
//!
//! # Motivation
//!
//! Queries are often structured with a "core" subquery that handles filtering, grouping,
//! or pagination, wrapped by an outer query that joins additional data. Inlining the
//! leading derived table can:
//!
//! - Reduce query nesting depth
//! - Expose filter predicates for earlier application
//! - Enable further optimization passes to see the full query structure
//!
//! # Example
//!
//! **Before:**
//! ```sql
//! SELECT s.id, s.total, tags.t
//! FROM (
//!     SELECT o.id, SUM(o.amount) AS total
//!     FROM orders AS o
//!     GROUP BY o.id
//!     ORDER BY o.id
//!     LIMIT 100
//! ) AS s
//! CROSS JOIN LATERAL (
//!     SELECT ARRAY_AGG(t.name) AS t
//!     FROM tags AS t
//!     WHERE t.order_id = s.id
//! ) AS tags
//! ORDER BY s.id
//! ```
//!
//! **After:**
//! ```sql
//! SELECT o.id, SUM(o.amount) AS total, tags.t
//! FROM orders AS o
//! CROSS JOIN LATERAL (
//!     SELECT ARRAY_AGG(t.name) AS t
//!     FROM tags AS t
//!     WHERE t.order_id = o.id
//! ) AS tags
//! GROUP BY o.id, tags.t
//! ORDER BY o.id
//! LIMIT 100
//! ```
//!
//! # Key Transformations
//!
//! - Column rebinding: References to the subquery alias are replaced with the
//!   actual projected expressions from the inner query.
//! - Alias deduplication: Inner FROM aliases that conflict with outer names are
//!   renamed to avoid ambiguity.
//! - WHERE → HAVING migration: When the inner query is aggregated, outer WHERE
//!   predicates that reference only the inner alias move to HAVING.
//! - LIMIT/OFFSET composition: When both inner and outer have numeric limits,
//!   they are combined (e.g., inner LIMIT 50 OFFSET 5 + outer LIMIT 10 OFFSET 3
//!   becomes LIMIT 10 OFFSET 8).
//!
//! # Safety Constraints
//!
//! The pass bails out (leaves the query unchanged) when inlining would alter semantics:
//!
//! - Cardinality preservation: Downstream joins must not change row counts.
//!   CROSS/INNER joins require RHS to produce exactly one row; LEFT joins allow
//!   at most one row.
//! - Window functions: Queries with window functions are not inlined.
//! - ORDER BY conflicts: If inner has LIMIT and outer imposes a different ORDER,
//!   inlining is unsafe.
//! - Nested aggregation: Both inner and outer being aggregated is not supported.
//! - Non-literal limits: LIMIT/OFFSET with placeholders or expressions cannot
//!   be composed.
//!
//! # Invariants
//!
//! This pass is intentionally conservative and relies on these invariants:
//!
//! * **Single-shot hoist**: we recurse only down the left spine once and then attempt at most one hoist per level.
//! * **No semantic reordering under TOP-K**: if the inner has LIMIT/OFFSET, we only hoist when ORDER BY is absent in the outer
//!   or the outer ORDER BY is a prefix-compatible view of the inner ORDER BY after projection rebinding.
//! * **Cardinality preservation**: downstream joins must be 1:1 (ExactlyOne for CROSS/INNER; AtMostOne for LEFT) and must have
//!   non-rejecting join constraints (ON TRUE / empty).
//! * **Guard downstream unnesting**: we bail if hoisting would turn a supported subquery-predicate LHS column into NULL or into a
//!   non-column in a context where later unnesting cannot legally keep it in WHERE.
//! * **GROUP BY compatibility**: when hoisting a grouped LHS into a non-grouped outer, any downstream output referenced by the
//!   outermost expressions must be a bare `rel.col` so it can become a GROUP BY key; mixed expressions over downstream outputs are rejected.
//! * **Engine constraint**: GROUP BY must project at least one aggregate-derived field; we bail if hoisting would produce a GROUP BY
//!   query with no aggregate-derived projection.
use crate::get_local_from_items_iter;
use crate::inline_subquery::{
    apply_inline, can_inline_subquery, normalize_field_reference, normalize_order_by,
    prepare_inline,
};
use crate::rewrite_joins::normalize_comma_separated_lhs;
use crate::rewrite_utils::{
    as_sub_query_with_alias, as_sub_query_with_alias_mut, build_ext_to_int_fields_map,
    expect_sub_query_with_alias_mut, get_from_item_reference_name,
    hoist_parametrizable_join_filters_to_where, is_aggregation_or_grouped,
    make_aliases_distinct_from_base_statement,
};
use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit::{Visitor, walk_select_statement};
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, GroupByClause, JoinRightSide, Relation, SelectStatement, TableExpr,
    TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::HashSet;
use std::iter;
use std::mem;
use tracing::trace;

pub trait InlineLeadingDerivedTable: Sized {
    fn inline_leading_derived_table(&mut self) -> ReadySetResult<&mut Self>;
}

impl InlineLeadingDerivedTable for SelectStatement {
    fn inline_leading_derived_table(&mut self) -> ReadySetResult<&mut Self> {
        let mut rewritten = hoist_lhsmost_derived_table_rewrite_impl(self, true)?;

        if rewritten {
            // If hoisting happened, try to move the parametrizable filters, that
            // existed in the hoistable internal join structure, and after hoisting
            // became available at the main level.
            // **NOTE**: if hoisting actually happened, we had verified all downstream
            // joins have empty join conditions.
            rewritten |= hoist_parametrizable_join_filters_to_where(self)?;
        }

        if rewritten {
            trace!(target: "inline_leading_derived_tables",
                statement = %self.display(Dialect::PostgreSQL),
                ">LHS-most derived table hoisted");
        }

        Ok(self)
    }
}

/// Collect all local FROM-item reference names in `stmt`, descending into **all** nested SELECT
/// statements (including subqueries inside expressions such as EXISTS/IN/scalar subqueries).
fn collect_local_from_item_refs_deep(
    stmt: &SelectStatement,
    out: &mut HashSet<Relation>,
) -> ReadySetResult<()> {
    struct AliasCollector<'a> {
        out: &'a mut HashSet<Relation>,
    }

    impl<'ast> Visitor<'ast> for AliasCollector<'_> {
        type Error = ReadySetError;

        fn visit_select_statement(
            &mut self,
            stmt: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            // Collect aliases/tables bound in this SELECT's FROM/JOIN scope.
            for dt in get_local_from_items_iter!(stmt) {
                self.out.insert(get_from_item_reference_name(dt)?);
            }
            // Recurse into all nested SELECTs (FROM-subqueries and expression subqueries).
            walk_select_statement(self, stmt)
        }
    }

    let mut v = AliasCollector { out };
    v.visit_select_statement(stmt)?;

    Ok(())
}

/// Collect aliases bound *inside* downstream derived-table subqueries.
///
/// Why: After hoisting the LHS-most derived table, its internal FROM aliases become visible at the
/// base level. If a downstream subquery already binds the same alias locally, that alias would be
/// shadowed (name capture) inside the downstream scope. We therefore reserve all aliases that appear
/// inside downstream subquery scopes when deduplicating inner aliases.
///
/// Note: we intentionally descend into **all** nested SELECTs, including expression subqueries.
fn collect_downstream_scoped_aliases(
    base_stmt: &SelectStatement,
) -> ReadySetResult<HashSet<Relation>> {
    let mut out = HashSet::new();
    for dt in get_local_from_items_iter!(base_stmt).skip(1) {
        if let Some((dt_stmt, _dt_alias)) = as_sub_query_with_alias(dt) {
            collect_local_from_item_refs_deep(dt_stmt, &mut out)?;
        }
    }
    Ok(out)
}

fn hoist_lhsmost_from_item_internals(
    base_stmt: &mut SelectStatement,
    lhs_dt: TableExpr,
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    let mut prepared = prepare_inline(lhs_dt)?;

    // Positional insertion: splice tables at position 0, prepend joins.
    base_stmt
        .tables
        .splice(0..=0, mem::take(&mut prepared.stmt.tables));
    if !prepared.stmt.join.is_empty() {
        base_stmt
            .join
            .splice(0..0, mem::take(&mut prepared.stmt.join));
    }

    apply_inline(
        base_stmt,
        prepared,
        is_top_select,
        downstream_group_by_additions,
    )
}

fn normalize_group_and_order_by(stmt: &mut SelectStatement) -> ReadySetResult<()> {
    // Resolve numeric and alias references in ORDER BY
    if let Some(order_by) = &stmt.order {
        stmt.order = Some(normalize_order_by(&stmt.fields, order_by)?);
    }

    // Resolve numeric and alias references in GROUP BY
    if let Some(group_by) = &stmt.group_by {
        stmt.group_by = Some(normalize_group_by(&stmt.fields, group_by)?);
    }

    Ok(())
}

/// Inline a subquery FROM item: rename aliases, hoist filters, substitute columns, and splice in tables/joins.
fn hoist_lhsmost_from_item(
    base_stmt: &mut SelectStatement,
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    // Get the LHS-most derived table
    let Some(lhs_dt) = base_stmt.tables.first_mut() else {
        unsupported!("FROM-less statement found");
    };

    // Take the LHS-most derived table out, replace it with a dummy relation.
    // **IMPORTANT**: keep the original alias.
    let mut lhs_dt = mem::replace(
        lhs_dt,
        TableExpr {
            inner: TableExprInner::Table("$being_inlined$".into()),
            alias: lhs_dt.alias.clone(),
            column_aliases: vec![],
        },
    );

    // Resolve the numeric and alias references in GROUP BY and ORDER BY for the LHS-most statement
    {
        let (lhs_stmt, _) = expect_sub_query_with_alias_mut(&mut lhs_dt);
        normalize_group_and_order_by(lhs_stmt)?;
    }

    // Convert comma separated tables into cross-joined sequence.
    // This will help to preserve Postgres compatible join shape after hoisting the LHS-most derived table.
    normalize_comma_separated_lhs(base_stmt)?;

    // Resolve the numeric and alias references in GROUP BY and ORDER BY for the base statement
    normalize_group_and_order_by(base_stmt)?;

    // Make sure the inner FROM item's aliases of the LHS-most statement do not clash with
    // the existing base statement's FROM items
    let reserved_aliases = collect_downstream_scoped_aliases(base_stmt)?;
    {
        let (inl_stmt, inl_alias) = expect_sub_query_with_alias_mut(&mut lhs_dt);
        make_aliases_distinct_from_base_statement(
            base_stmt,
            &inl_alias,
            inl_stmt,
            &reserved_aliases,
        )?;
    }

    // Embed the LHS-most derived table into the base statement
    hoist_lhsmost_from_item_internals(
        base_stmt,
        lhs_dt,
        is_top_select,
        downstream_group_by_additions,
    )?;

    Ok(())
}

fn normalize_group_by(
    fields: &[FieldDefinitionExpr],
    group_by: &GroupByClause,
) -> ReadySetResult<GroupByClause> {
    let mut norm_group_by = GroupByClause {
        fields: Vec::with_capacity(group_by.fields.len()),
    };
    for gf in group_by.fields.iter() {
        norm_group_by
            .fields
            .push(normalize_field_reference(fields, gf)?);
    }
    Ok(norm_group_by)
}

fn hoist_lhsmost_derived_table(
    stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    // Normalize LATERAL at position 0.  The flag's only expressive
    // capability is correlation to preceding FROM siblings — which do not
    // exist at position 0.  Correlation to outer-enclosing scopes works
    // for any FROM-item subquery without LATERAL (regular correlated-
    // subquery semantics), so clearing the flag here is observationally
    // a no-op for query evaluation.
    //
    // Downstream, `unnest_lateral_subqueries` treats LATERAL bodies via
    // the Flatten/Resolve paths; the Flatten precondition
    // `has_outer_left_join_on` would otherwise admit position-0 LATERAL
    // bodies whose internal LEFT JOIN ON references an outer-enclosing
    // (grandparent) column, applying the algebraic identity
    // `A × (B ⟕_p C) ≡ (A × B) ⟕_p C` with empty `A` — a malformed
    // application of the identity.  Clearing the flag here routes such
    // bodies through the regular FROM-subquery path
    // (`derived_tables_rewrite`), which is the correct handling.
    //
    // This normalization runs at every left-spine level the pass
    // descends to (per `hoist_lhsmost_derived_table_rewrite_impl`'s
    // recursion), so every reachable position-0 subquery gets normalized.
    if let Some(lhs_dt) = stmt.tables.first_mut()
        && let Some((inner_stmt, _)) = as_sub_query_with_alias_mut(lhs_dt)
    {
        inner_stmt.lateral = false;
    }

    let Some(lhs_dt) = stmt.tables.first() else {
        return Ok(false);
    };

    let Some((lhs_stmt, lhs_alias)) = as_sub_query_with_alias(lhs_dt) else {
        return Ok(false);
    };

    // Build a projection-rebinding map for *analysis* (ORDER BY compatibility, unnesting guards).
    // IMPORTANT: we rebuild the map later (after alias de-duplication) for the actual substitution;
    // using a pre-dedup map for rewriting would be unsound because table aliases may change.
    let Ok(outer_to_inner_fields) = build_ext_to_int_fields_map(lhs_stmt, lhs_alias.clone()) else {
        return Ok(false);
    };

    // Common eligibility checks (shared with LATERAL flattening path):
    // window functions, nested aggregation, GROUP BY compat, ORDER/LIMIT,
    // cardinality, self-join, downstream non-trivial output, unnesting guards.
    // Leftmost position — downstream = everything after position 0.
    let ctx = crate::inline_subquery::InliningContext::<
        crate::drop_redundant_join::UniqueColumnsSchemaImpl,
    > {
        inner_stmt: lhs_stmt,
        outer_stmt: stmt,
        inner_alias: &lhs_alias,
        ext_to_int: &outer_to_inner_fields,
        inl_from_item_ord_idx: 0,
        downstream_tables: &stmt.tables[1..],
        downstream_joins: &stmt.join,
        is_top_select,
        skip_unnesting_guard: false,
        inner_rel: lhs_alias.clone().into(),
        is_inner_agg: is_aggregation_or_grouped(lhs_stmt)?,
        is_outer_agg: is_aggregation_or_grouped(stmt)?,
        pre_hoist_lateral_exactly_one: None,
        pre_hoist_lateral_at_most_one: None,
        preceding_flattened_lateral_aliases: None,
        unique_cols_schema: None,
    };

    let Some(downstream_group_by_additions) = can_inline_subquery(&ctx)? else {
        return Ok(false);
    };

    hoist_lhsmost_from_item(stmt, is_top_select, downstream_group_by_additions)?;

    Ok(true)
}

/// Recursively inlines nested FROM subqueries, then inline items at this query level.
fn hoist_lhsmost_derived_table_rewrite_impl(
    stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let mut rewritten = false;

    // Recurse down the left spine exactly once to expose a hoistable core,
    // then attempt a single hoist at this level. We intentionally *do not*
    // call this recursively inside a loop: doing so repeats the same work
    // without changing the AST until a hoist happens.
    if let Some(TableExpr {
        inner: TableExprInner::Subquery(inner_stmt),
        alias: Some(_),
        ..
    }) = stmt.tables.first_mut()
    {
        // Normalize the inner left-spine only; do not repeat per-iteration.
        rewritten |= hoist_lhsmost_derived_table_rewrite_impl(inner_stmt, false)?;
    }

    // Single hoist attempt for this level.
    rewritten |= hoist_lhsmost_derived_table(stmt, is_top_select)?;

    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArrayConstructorRewrite;
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

    fn rewrite_statement(sql_text: &str) -> ReadySetResult<SelectStatement> {
        let mut stmt = parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql_text)?;
        stmt.rewrite_array_constructors()?;
        hoist_lhsmost_derived_table_rewrite_impl(&mut stmt, true)?;
        Ok(stmt)
    }

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        match rewrite_statement(original_text) {
            Ok(rewritten_stmt) => {
                println!(
                    ">>>>>> Hoisted: {}",
                    rewritten_stmt.display(Dialect::PostgreSQL)
                );
                let expected_stmt = match parse_select_with_config(
                    PARSING_CONFIG,
                    Dialect::PostgreSQL,
                    expect_text,
                ) {
                    Ok(stmt) => stmt,
                    Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                };
                assert_eq!(rewritten_stmt, expected_stmt);
            }
            Err(e) => {
                println!("> {test_name}: REWRITE ERROR: {e}");
                assert!(expect_text.is_empty(), "> {test_name}: REWRITE ERROR: {e}")
            }
        }
    }

    // Bail: Window function in the inner subquery
    #[test]
    fn test1() {
        let original = r#"
        SELECT s.x, s.rn FROM (SELECT t.x, row_number() over() AS rn FROM t) AS s"#;
        let expected = original;
        test_it("test1", original, expected);
    }

    // Hoist: non-aggregated inner with downstream join (cardinality check
    // only gates aggregated/LIMIT inners, so this is safe to inline).
    #[test]
    fn test2() {
        let original = r#"SELECT s.x, u.y FROM (SELECT t.x, t.z FROM t) AS s JOIN u ON true"#;
        let expected = r#"SELECT "t"."x", "u"."y" FROM "t" JOIN "u" ON TRUE"#;
        test_it("test2", original, expected);
    }

    // Hoist: Combined LIMIT + OFFSET
    #[test]
    fn test3() {
        let original = r#"
        SELECT s.x
          FROM (
              SELECT t.x FROM t ORDER BY x ASC LIMIT 10 OFFSET 5
          ) AS s
        ORDER BY s.x ASC
        LIMIT 7 OFFSET 2"#;
        let expected =
            r#"SELECT "t"."x" FROM "t" ORDER BY "t"."x" ASC NULLS LAST LIMIT 7 OFFSET 7"#;
        test_it("test3", original, expected);
    }

    // Bail: ORDER BY mismatch
    #[test]
    fn test4() {
        let original = r#"
        SELECT s.x
          FROM (
              SELECT t.x, t.y FROM t ORDER BY y DESC LIMIT 10
          ) AS s
        ORDER BY s.x ASC"#;
        let expected = original;
        test_it("test4", original, expected);
    }

    // Bail: WHERE contains a parametrizable filter that would change after hoisting
    #[test]
    fn test5() {
        let original = r#"
        SELECT s.sumx
          FROM (
            SELECT t.a, SUM(t.x) AS sumx FROM t GROUP BY a
          ) AS s
        WHERE s.sumx > 10"#;
        let expected = original;
        test_it("test5", original, expected);
    }

    // Hoist: LEFT join with LIMIT 1 subquery (AtMostOne)
    #[test]
    fn test6() {
        let original = r#"
        SELECT s.x
          FROM (SELECT t.x FROM t) AS s
        LEFT JOIN LATERAL (SELECT u.y FROM u WHERE u.k = s.x LIMIT 1) AS l ON true"#;
        let expected = r#"SELECT "t"."x" FROM "t" LEFT JOIN LATERAL
        (SELECT "u"."y" FROM "u" WHERE ("u"."k" = "t"."x") LIMIT 1) AS "l" ON TRUE"#;
        test_it("test6", original, expected);
    }

    // Hoist: non-aggregated inner with downstream LATERAL join (cardinality
    // check only gates aggregated/LIMIT inners).
    #[test]
    fn test7() {
        let original = r#"
        SELECT s.x
          FROM (SELECT x FROM t) AS s
        JOIN LATERAL (SELECT y FROM u WHERE u.k = s.x LIMIT 1) AS l ON true"#;
        let expected = r#"SELECT "x" FROM "t" JOIN LATERAL (SELECT "y" FROM "u" WHERE ("u"."k" = "x") LIMIT 1) AS "l" ON TRUE"#;
        test_it("test7", original, expected);
    }

    // Hoist: Aggregated + TOP-K inner: nothing from outer WHERE should be moved to HAVING -> safe
    #[test]
    fn test8() {
        let original = r#"
        SELECT
            "inner".jn, "inner".sum_qty, tags.tags
        FROM (
            SELECT s.jn, SUM(spj.qty) AS sum_qty, s.sn AS _o0, s.jn AS _o1
            FROM qa.s AS s JOIN qa.spj AS spj ON spj.sn = s.sn
            GROUP BY s.sn, s.jn
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 20
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST"#;
        let expected = r#"SELECT "s"."jn", sum("spj"."qty") AS "sum_qty", "tags"."tags" FROM "qa"."s" AS "s"
        JOIN "qa"."spj" AS "spj" ON ("spj"."sn" = "s"."sn") CROSS JOIN LATERAL
        (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags" FROM
        (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
        AS "inner_subq") AS "array_subq") AS "tags"  GROUP BY "s"."sn", "s"."jn", "tags"."tags" ORDER BY "s"."sn" ASC NULLS LAST,
        "s"."jn" ASC NULLS LAST LIMIT 20"#;
        test_it("test8", original, expected);
    }

    // Bail: Aggregated + TOP-K inner: WHERE on inner-only field must be moved to HAVING -> change the inner TOP-K order
    #[test]
    fn test9() {
        let original = r#"
        SELECT
            "inner".jn, "inner".sum_qty, tags.tags
        FROM (
            SELECT s.jn, SUM(spj.qty) AS sum_qty, s.sn AS _o0, s.jn AS _o1
            FROM qa.s AS s JOIN qa.spj AS spj ON spj.sn = s.sn
            GROUP BY s.sn, s.jn
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 20
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags
        WHERE "inner".sum_qty > 10
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "inner"."jn", "inner"."sum_qty", "tags"."tags" FROM
        (SELECT "s"."jn", sum("spj"."qty") AS "sum_qty", "s"."sn" AS "_o0", "s"."jn" AS "_o1" FROM "qa"."s" AS "s"
        JOIN "qa"."spj" AS "spj" ON ("spj"."sn" = "s"."sn") GROUP BY "s"."sn", "s"."jn" ORDER BY "_o0" ASC NULLS LAST,
        "_o1" ASC NULLS LAST LIMIT 20) AS "inner", LATERAL (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags"
        FROM (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "inner"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
        AS "inner_subq") AS "array_subq") AS "tags" WHERE ("inner"."sum_qty" > 10) ORDER BY "inner"."_o0" ASC NULLS LAST,
        "inner"."_o1" ASC NULLS LAST"#;
        test_it("test9", original, expected);
    }

    // Complex QA-shaped hoist: LHS-most subquery with two CROSS LATERAL array aggregations
    // (simulates the early example with tags / concepts lists)
    #[test]
    fn test10() {
        let original = r#"
        SELECT
            "inner".sn, "inner".pn, "inner".jn, tags.tags, concepts.concepts
        FROM (
            SELECT s.sn, s.pn, s.jn, d1.test_varchar AS _o0, s.sn AS _o1
            FROM qa.s AS s
            INNER JOIN qa.datatypes1 AS d1 ON d1.test_integer = s.status
            WHERE s.city != 'PARIS' AND s.pn >= 'P00000'
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 41
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags,
        LATERAL (
            SELECT ARRAY(
                SELECT spj.sn FROM qa.spj AS spj
                WHERE spj.pn = "inner".pn
                ORDER BY spj.sn ASC
            ) AS concepts
        ) AS concepts
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "s"."pn", "s"."jn", "tags"."tags", "concepts"."concepts"
        FROM "qa"."s" AS "s" INNER JOIN "qa"."datatypes1" AS "d1" ON ("d1"."test_integer" = "s"."status")
        CROSS JOIN LATERAL (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags" FROM
        (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn")
        ORDER BY "p"."pn" ASC NULLS LAST) AS "inner_subq") AS "array_subq") AS "tags"  CROSS JOIN LATERAL
        (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "concepts" FROM (SELECT array_agg("inner_subq"."sn"
        ORDER BY "inner_subq"."sn" ASC NULLS LAST) AS "agg_result" FROM (SELECT "spj"."sn" AS "sn" FROM "qa"."spj" AS "spj"
        WHERE ("spj"."pn" = "s"."pn") ORDER BY "spj"."sn" ASC NULLS LAST) AS "inner_subq") AS "array_subq") AS "concepts"
        WHERE (("s"."city" != 'PARIS') AND ("s"."pn" >= 'P00000'))
        ORDER BY "d1"."test_varchar" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 41"#;
        test_it("test10", original, expected);
    }

    // Limit composition: inner LIMIT/OFFSET with matching ORDER BY and outer LIMIT/OFFSET.
    // After hoist, LIMIT/OFFSET must be composed correctly: limit = min(outer.limit, inner.limit - outer.offset);
    // offset = inner.offset + outer.offset.
    #[test]
    fn test11() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, d1.test_varchar AS _o0, s.sn AS _o1
          FROM qa.s AS s
          LEFT JOIN qa.datatypes1 AS d1 ON d1.test_integer = s.status
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 50 OFFSET 5
        ) AS inner
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        LIMIT 10 OFFSET 3
        "#;
        let expected = r#"SELECT "s"."sn" FROM "qa"."s" AS "s" LEFT JOIN "qa"."datatypes1" AS "d1" ON ("d1"."test_integer" = "s"."status") ORDER BY "d1"."test_varchar" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 10 OFFSET 8"#;
        test_it("test11", original, expected);
    }

    // Bail: CROSS LATERAL with an AtMostOne RHS (0 or 1 rows) must *not* hoist.
    // Here the RHS groups by p.jn and will yield 0 rows if no matching p exists.
    #[test]
    fn test12() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, s.jn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 100
        ) AS inner,
        LATERAL (
          SELECT max(p.pn) AS mx
          FROM qa.p AS p
          WHERE p.jn = inner.jn
          GROUP BY p.jn
        ) AS lmt
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test12", original, expected);
    }

    // Hoist: CROSS LATERAL with ExactlyOne RHS (aggregate-only, no GROUP BY)
    #[test]
    fn test13() {
        let original = r#"
        SELECT
            inner.sn, l.cnt
        FROM (
            SELECT s.sn, s.sn AS _o0, s.sn AS _o1
            FROM qa.s AS s
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 10
        ) AS inner,
        LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.j AS j
            WHERE j.sn = inner.sn
        ) AS l
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "l"."cnt" FROM "qa"."s" AS "s" CROSS JOIN LATERAL
        (SELECT count(*) AS "cnt" FROM "qa"."j" AS "j" WHERE ("j"."sn" = "s"."sn")) AS "l"
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test13", original, expected);
    }

    // Hoist: non-aggregated inner with LATERAL + non-trivial ON downstream
    // (cardinality check only gates aggregated/LIMIT inners).
    #[test]
    fn test14() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn FROM qa.s AS s
        ) AS inner
        JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM qa.j AS j WHERE j.sn = inner.sn
        ) AS l ON (l.cnt > 0)
        "#;
        let expected = r#"SELECT "s"."sn" FROM "qa"."s" AS "s" JOIN LATERAL (SELECT count(*) AS "cnt" FROM "qa"."j" AS "j" WHERE ("j"."sn" = "s"."sn")) AS "l" ON ("l"."cnt" > 0)"#;
        test_it("test14", original, expected);
    }

    // Hoist: INNER JOIN with ExactlyOne RHS (aggregate-only) and ON TRUE
    #[test]
    fn test15() {
        let original = r#"
        SELECT inner.sn, c.cnt
        FROM (
          SELECT s.sn FROM qa.s AS s
        ) AS inner
        JOIN (SELECT COUNT(*) AS cnt FROM qa.p AS p) AS c ON TRUE
        "#;
        let expected = r#"SELECT "s"."sn", "c"."cnt" FROM "qa"."s" AS "s" JOIN (SELECT count(*) AS "cnt" FROM "qa"."p" AS "p") AS "c" ON TRUE"#;
        test_it("test15", original, expected);
    }

    // Hoist: additional bare FROM item (CROSS) that is ExactlyOne -> allowed
    #[test]
    fn test16() {
        let original = r#"
        SELECT inner.sn, c.cnt
        FROM (
          SELECT s.sn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 3
        ) AS inner,
        (SELECT COUNT(*) AS cnt FROM qa.p AS p) AS c
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "c"."cnt" FROM "qa"."s" AS "s" CROSS JOIN (SELECT count(*) AS "cnt"
        FROM "qa"."p" AS "p") AS "c"
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 3"#;
        test_it("test16", original, expected);
    }

    // Bail: additional bare FROM item (CROSS) that is AtMostOne (LIMIT 1) -> unsafe
    #[test]
    fn test17() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 5
        ) AS inner,
        (SELECT pn FROM qa.p AS p WHERE p.weight > 100 LIMIT 1) AS c
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test17", original, expected);
    }

    // Hoist: LEFT JOIN LATERAL with AtMostOne RHS (GROUP BY key) -> allowed
    #[test]
    fn test18() {
        let original = r#"
        SELECT inner.sn, l.maxpn
        FROM (
          SELECT s.sn, s.pn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 5
        ) AS inner
        LEFT JOIN LATERAL (
          SELECT MAX(p.pn) AS maxpn
          FROM qa.p AS p
          WHERE p.sn = inner.sn
          GROUP BY p.sn
        ) AS l ON TRUE
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "l"."maxpn" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT max("p"."pn") AS "maxpn" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn") GROUP BY "p"."sn") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 5"#;
        test_it("test18", original, expected);
    }

    // Hoist: ORDER BY alias in outer rebinding to inner expression
    #[test]
    fn test19() {
        let original = r#"
        SELECT s.a
        FROM (
            SELECT t.a, (t.a + 1) AS ax
            FROM t
            ORDER BY ax ASC NULLS LAST
            LIMIT 10
        ) AS s
        ORDER BY s.ax ASC NULLS LAST
        LIMIT 5
        "#;
        let expected = r#"SELECT "t"."a" FROM "t" ORDER BY ("t"."a" + 1) ASC NULLS LAST LIMIT 5"#;
        test_it("test19", original, expected);
    }

    // Bail: Aggregated inner with TOP-K; WHERE has a scalar subquery (cannot push to HAVING)
    #[test]
    fn test20() {
        let original = r#"
        SELECT s.sumx
        FROM (
            SELECT t.a, SUM(t.x) AS sumx
            FROM t
            GROUP BY t.a
            ORDER BY t.a ASC NULLS LAST
            LIMIT 10
        ) AS s
        WHERE s.sumx > (SELECT COUNT(*) FROM u)
        "#;
        let expected = original;
        test_it("test20", original, expected);
    }

    // Hoist: LIMIT/OFFSET composition corner case where resulting LIMIT becomes 0
    #[test]
    fn test21() {
        let original = r#"
        SELECT inner.sn
        FROM (
            SELECT s.sn, s.sn AS _o0, s.sn AS _o1
            FROM qa.s AS s
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 3 OFFSET 10
        ) AS inner
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        LIMIT 5 OFFSET 7
        "#;
        let expected = r#"SELECT "s"."sn" FROM "qa"."s" AS "s" ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 0 OFFSET 17"#;
        test_it("test21", original, expected);
    }

    // Hoist: Outer ORDER is a prefix of inner ORDER (allowed)
    #[test]
    fn test22() {
        let original = r#"
        SELECT s.a
        FROM (
            SELECT t.a, t.b
            FROM t
            ORDER BY t.a ASC NULLS LAST, t.b DESC NULLS LAST
            LIMIT 10
        ) AS s
        ORDER BY s.a ASC NULLS LAST
        LIMIT 4
        "#;
        let expected = r#"SELECT "t"."a" FROM "t" ORDER BY "t"."a" ASC NULLS LAST, "t"."b" DESC NULLS LAST LIMIT 4"#;
        test_it("test22", original, expected);
    }

    // Bail: Inner has LIMIT without ORDER, outer imposes ORDER -> unsafe, must not hoist
    #[test]
    fn test23() {
        let original = r#"
        SELECT s.x
        FROM (
            SELECT t.x
            FROM t
            LIMIT 10
        ) AS s
        ORDER BY s.x ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test23", original, expected);
    }

    // Bail: Aggregated inner with TOP-K; outer WHERE conjunct mixes LHS and other base rel -> must not hoist
    #[test]
    fn test24() {
        let original = r#"
        SELECT s.sumx
        FROM (
            SELECT t.a, SUM(t.x) AS sumx
            FROM t
            GROUP BY t.a
            ORDER BY t.a ASC NULLS LAST
            LIMIT 10
        ) AS s,
        (SELECT COUNT(*) AS c FROM u) AS ctab
        WHERE s.sumx > ctab.c
        "#;
        let expected = original;
        test_it("test24", original, expected);
    }

    // AtMostOne via aggregate-only core wrapped by a single-projecting SELECT (no GROUP BY at wrapper)
    #[test]
    fn test25() {
        let original = r#"
    SELECT inner.sn, l.cnt
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 10
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT a1.cnt
      FROM (
        SELECT COUNT(*) AS cnt
        FROM qa.j AS j
        WHERE j.sn = inner.sn
        GROUP BY j.sn
      ) AS a1
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."cnt" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "a1"."cnt" FROM (SELECT count(*) AS "cnt" FROM "qa"."j" AS "j" WHERE ("j"."sn" = "s"."sn") GROUP BY "j"."sn") AS "a1") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test25", original, expected);
    }

    // AtMostOne via LIMIT 1 deep inside a wrapper (detected by has_limit_one_deep)
    #[test]
    fn test26() {
        let original = r#"
    SELECT inner.sn, l.y
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 10
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."y" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "o"."y" FROM (SELECT "p"."pn" AS "y" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn")
        ORDER BY "p"."pn" ASC NULLS LAST LIMIT 1) AS "o") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test26", original, expected);
    }

    // Bail: INNER JOIN with wrapper whose core is only AtMostOne (LIMIT 1 deep) – not ExactlyOne
    #[test]
    fn test27() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 7
    ) AS inner
    JOIN LATERAL (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test27", original, expected);
    }

    // Bail: CROSS (bare FROM) item that is only AtMostOne (LIMIT 1) – CROSS requires ExactlyOne
    #[test]
    fn test28() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner,
    (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS c
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test28", original, expected);
    }

    // AtMostOne via GROUP BY pinned by correlation, wrapped once (no GROUP BY at wrapper level)
    #[test]
    fn test29() {
        let original = r#"
    SELECT inner.sn, l.mx
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT w.mx
      FROM (
        SELECT MAX(p.pn) AS mx
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        GROUP BY p.sn
      ) AS w
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."mx" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "w"."mx" FROM (SELECT max("p"."pn") AS "mx" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn")
        GROUP BY "p"."sn") AS "w") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 5"#;
        test_it("test29", original, expected);
    }

    // Bail: wrapper introduces unpinned GROUP BY (not fixed by correlation) -> not AtMostOne
    #[test]
    fn test30() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT w.mx
      FROM (
        SELECT MAX(p.pn) AS mx, p.sn
        FROM qa.p AS p
        GROUP BY p.sn
      ) AS w
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test30", original, expected);
    }

    // Bail: wrapper adds a join that can replicate rows -> not AtMostOne even though core is agg-only
    #[test]
    fn test31() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 6
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT a1.cnt, p2.pn
      FROM (
        SELECT COUNT(*) AS cnt
        FROM qa.j AS j
        WHERE j.sn = inner.sn
      ) AS a1
      JOIN qa.p AS p2 ON TRUE
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test31", original, expected);
    }

    // AtMostOne via LIMIT 1 two levels deep under wrappers
    #[test]
    fn test32() {
        let original = r#"
    SELECT inner.sn, l.v
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 9
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT lvl1.v
      FROM (
        SELECT o.y AS v
        FROM (
          SELECT p.pn AS y
          FROM qa.p AS p
          WHERE p.sn = inner.sn
          ORDER BY p.pn ASC NULLS LAST
          LIMIT 1
        ) AS o
      ) AS lvl1
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."v" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "lvl1"."v" FROM (SELECT "o"."y" AS "v" FROM (SELECT "p"."pn" AS "y" FROM "qa"."p" AS "p"
        WHERE ("p"."sn" = "s"."sn") ORDER BY "p"."pn" ASC NULLS LAST LIMIT 1) AS "o") AS "lvl1") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 9"#;
        test_it("test32", original, expected);
    }

    #[test]
    fn test33() {
        let original = r#"
    SELECT s.rownum, s.test_dec, tags.test_dec
    FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
       FROM qa.datatypes AS o
       GROUP BY o.rownum
       ORDER BY o.rownum
       LIMIT 10
    ) AS s
    CROSS JOIN LATERAL (SELECT ARRAY_AGG(o.test_varchar) AS test_dec
       FROM qa.datatypes1 AS o
       WHERE o.rownum = s.rownum
    ) AS tags
    ORDER BY s.rownum;
    "#;
        let expected = r#"SELECT "o1"."rownum", sum("o1"."test_dec") AS "test_dec", "tags"."test_dec" FROM "qa"."datatypes" AS "o1"
        CROSS JOIN LATERAL (SELECT array_agg("o"."test_varchar") AS "test_dec" FROM "qa"."datatypes1" AS "o"
        WHERE ("o"."rownum" = "o1"."rownum")) AS "tags"
        GROUP BY "o1"."rownum", "tags"."test_dec"
        ORDER BY "o1"."rownum" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test33", original, expected);
    }

    // Bail: hoisting will block un-nesting of the `NOT IN`
    #[test]
    fn test34() {
        let original = r#"
    SELECT count(*) AS should_equal_baseline
    FROM (SELECT
        dt.RowNum,
        CONCAT(NULL, NULL) AS x
      FROM DataTypes dt
    ) AS v
    WHERE v.x NOT IN (SELECT d1.test_char
      FROM DataTypes1 d1
      LIMIT 1
    );
    "#;
        let expected = original;
        test_it("test34", original, expected);
    }

    // Hoist: aggregated LHS-most where outer SELECT only references GROUP BY keys
    // (no aggregate column referenced).  After inlining the GROUP BY moves to the
    // outer level; `fix_groupby_without_aggregates` (which runs later in the
    // pipeline) converts it to DISTINCT.  The intermediate state tested here —
    // after hoisting but before that pass — retains the GROUP BY.
    #[test]
    fn test35() {
        let original = r#"
    SELECT
        "inner".sn,
        tags.ts
    FROM (
        SELECT s.sn, SUM(spj.qty) AS sum_qty
        FROM qa.s AS s
        JOIN qa.spj AS spj ON spj.sn = s.sn
        GROUP BY s.sn
    ) AS "inner",
    (
        SELECT min(p.pn) AS ts
        FROM qa.p AS p
    ) AS tags
    ORDER BY "inner".sn;
    "#;
        let expected = r#"SELECT "s"."sn", "tags"."ts"
        FROM "qa"."s" AS "s"
        JOIN "qa"."spj" AS "spj" ON ("spj"."sn" = "s"."sn")
        CROSS JOIN (SELECT min("p"."pn") AS "ts" FROM "qa"."p" AS "p") AS "tags"
        GROUP BY "s"."sn", "tags"."ts"
        ORDER BY "s"."sn" ASC NULLS LAST"#;
        test_it("test35", original, expected);
    }

    // Hoist: **Note**, duplicate alias `o` should be deduplicated.
    #[test]
    fn test36() {
        let original = r#"
      SELECT s.rownum, s.test_dec, tags.test_dec
      FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
           FROM qa.datatypes AS o
           GROUP BY o.rownum
           ORDER BY o.rownum
           LIMIT 10
      ) AS s
      CROSS JOIN LATERAL (SELECT ARRAY_AGG(o.test_varchar) AS test_dec
           FROM qa.datatypes1 AS o
           WHERE o.rownum = s.rownum
      ) AS tags
      ORDER BY s.rownum;
      "#;
        let expected = r#"SELECT "o1"."rownum", sum("o1"."test_dec") AS "test_dec", "tags"."test_dec"
        FROM "qa"."datatypes" AS "o1" CROSS JOIN LATERAL (SELECT array_agg("o"."test_varchar") AS "test_dec"
        FROM "qa"."datatypes1" AS "o" WHERE ("o"."rownum" = "o1"."rownum")) AS "tags"
        GROUP BY "o1"."rownum", "tags"."test_dec"
        ORDER BY "o1"."rownum" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test36", original, expected);
    }

    // Bail: outer WF + inner (agg) with LIMIT.  Absorbing the inner LIMIT to the
    // outer would let the WF compute over the full grouped result (N rows) instead
    // of the bounded 10 rows, producing different ROW_NUMBER() values (§9 — WF
    // input cardinality must be preserved).
    #[test]
    fn test_outer_wf_inner_agg_limit_bails() {
        let original = r#"
    SELECT ROW_NUMBER() OVER (), s.rownum, s.test_dec, tags.test_dec
    FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
       FROM qa.datatypes AS o
       GROUP BY o.rownum
       ORDER BY o.rownum
       LIMIT 10
    ) AS s
    CROSS JOIN LATERAL (SELECT ARRAY_AGG(o.test_varchar) AS test_dec
       FROM qa.datatypes1 AS o
       WHERE o.rownum = s.rownum
    ) AS tags
    ORDER BY s.rownum;
    "#;
        let expected = original;
        test_it("outer_wf_inner_agg_limit_bails", original, expected);
    }

    // Bail: outer DISTINCT + inner LIMIT.  Absorbing the LIMIT would change the
    // dedup scope from the bounded 10 rows to the full row set, producing a
    // different DISTINCT result.
    #[test]
    fn test_outer_distinct_inner_limit_bails() {
        let original = r#"
            SELECT DISTINCT s.x
            FROM (SELECT t.x FROM t LIMIT 10) AS s
        "#;
        let expected = original;
        test_it("outer_distinct_inner_limit_bails", original, expected);
    }

    // Bail: outer aggregate + inner (non-agg) with LIMIT.  Absorbing the LIMIT
    // would cause SUM to aggregate over the full row set instead of the bounded
    // 10 rows (e.g., SUM of all x's instead of the first 10 x's).
    #[test]
    fn test_outer_agg_inner_limit_bails() {
        let original = r#"
            SELECT SUM(s.x) AS total
            FROM (SELECT t.x FROM t LIMIT 10) AS s
        "#;
        let expected = original;
        test_it("outer_agg_inner_limit_bails", original, expected);
    }

    // Bail: outer WF + inner (aggregated, no LIMIT).  Absorbing the inner's
    // GROUP BY into the outer (check-3 path) would produce an outer with
    // GROUP BY + HAVING + WF — an unsupported WF context (§9), and changes the
    // engine's WF evaluation position relative to HAVING even though the
    // standard-SQL semantics happen to align.
    #[test]
    fn test_outer_wf_inner_agg_no_limit_bails() {
        let original = r#"
    SELECT ROW_NUMBER() OVER(),
           "INNER"."rownum",
           "INNER"."test_dec"
    FROM (SELECT "o"."rownum" AS "rownum",
                 sum("o"."test_dec") AS "test_dec",
                 "o"."test_int" AS "__rn"
          FROM qa.datatypes AS o
          GROUP BY "o"."rownum", "o"."test_int") AS "INNER"
    WHERE "INNER"."__rn" <= 10
    ORDER BY "INNER"."rownum" NULLS LAST
    "#;
        let expected = original;
        test_it("outer_wf_inner_agg_no_limit_bails", original, expected);
    }

    // Bail: LHS inlining of a grouped inner into a non-grouped outer when the
    // outer SELECT neither references an aggregate-derived column nor lists
    // all of the inner's GROUP BY keys as standalone projections.  The
    // engine rejects a resulting GROUP BY query that doesn't project either
    // an aggregate or every grouping key (cf. can_inline_from_item lines
    // 303-351 — this check is now uniform across inlining paths after the
    // 2026-04-21 convergence).
    #[test]
    fn test_lhs_grouped_engine_validation_bails() {
        let original = r#"
            SELECT "s"."a"
            FROM (SELECT "t"."a", "t"."b", SUM("t"."x") AS "sx"
                  FROM "t"
                  GROUP BY "t"."a", "t"."b") AS "s"
        "#;
        // Outer SELECT projects s.a only (one of two GROUP BY keys, no
        // aggregate reference).  Engine constraint fails → bail.
        let expected = original;
        test_it("lhs_grouped_engine_validation_bails", original, expected);
    }

    // Self-join bail-out: hoisting would introduce t1 from subquery alongside t1 in JOIN
    #[test]
    fn self_join_bail_out() {
        let original = r#"
            SELECT "sq"."id", "t1"."val"
            FROM (SELECT "t1"."id" FROM "t1") AS "sq"
            JOIN "t1" ON "sq"."id" = "t1"."id"
        "#;
        // Expected: unchanged (hoisting bailed out)
        test_it("self_join_bail_out", original, original);
    }

    // ─── Coverage gap tests ────────────────────────────────────────────────

    // Expression-based GROUP BY with aggregate output — verify hoisting works
    // when GROUP BY uses an expression, not just a simple column. The outer
    // SELECT must reference at least one aggregate-derived output for the
    // engine's "no GROUP BY without aggregates" constraint.
    #[test]
    fn test_expression_group_by() {
        let original = r#"
            SELECT "sq"."bucket", "sq"."total"
            FROM (SELECT ("t1"."x" / 10) AS "bucket", SUM("t1"."val") AS "total"
                  FROM "t1"
                  GROUP BY ("t1"."x" / 10)) AS "sq"
        "#;
        // Inner has GROUP BY expression + aggregate. Outer references
        // sq.total (aggregate-derived). Should hoist.
        let expected = r#"
            SELECT ("t1"."x" / 10) AS "bucket", sum("t1"."val") AS "total"
            FROM "t1"
            GROUP BY ("t1"."x" / 10)
        "#;
        test_it("expression_group_by", original, expected);
    }

    // Downstream INNER JOIN with non-rejecting ON (ON TRUE) + ExactlyOne RHS
    // — should hoist because cardinality is preserved.
    #[test]
    fn test_downstream_inner_exactly_one_on_true() {
        let original = r#"
            SELECT "sq"."a", "sub"."cnt"
            FROM (SELECT "t1"."a" FROM "t1") AS "sq"
            INNER JOIN (SELECT COUNT(*) AS "cnt" FROM "t2") AS "sub" ON TRUE
        "#;
        // sub is ExactlyOne (COUNT no GBY), INNER + ON TRUE → cardinality-preserving.
        let expected = r#"
            SELECT "t1"."a", "sub"."cnt"
            FROM "t1"
            INNER JOIN (SELECT count(*) AS "cnt" FROM "t2") AS "sub" ON TRUE
        "#;
        test_it("downstream_inner_exactly_one_on_true", original, expected);
    }

    // Hoist: non-aggregated inner with downstream INNER JOIN with real ON.
    // Cardinality check only gates aggregated/LIMIT inners — this is safe.
    #[test]
    fn test_downstream_inner_real_on_hoist() {
        let original = r#"
            SELECT "sq"."a", "t2"."b"
            FROM (SELECT "t1"."a" FROM "t1") AS "sq"
            INNER JOIN "t2" ON ("sq"."a" = "t2"."b")
        "#;
        let expected =
            r#"SELECT "t1"."a", "t2"."b" FROM "t1" INNER JOIN "t2" ON ("t1"."a" = "t2"."b")"#;
        test_it("downstream_inner_real_on_hoist", original, expected);
    }

    // Empty-result inner query (WHERE FALSE) — should bail because
    // agg_only_no_gby_cardinality classifies it as not-exactly-one
    // (no aggregates, WHERE FALSE doesn't qualify as ExactlyOne).
    #[test]
    fn test_empty_result_inner_bail() {
        let original = r#"
            SELECT "sq"."a"
            FROM (SELECT "t1"."a" FROM "t1" WHERE FALSE) AS "sq"
        "#;
        // No downstream joins → single FROM item → should still hoist
        // (the WHERE FALSE goes to base WHERE).
        let expected = r#"
            SELECT "t1"."a"
            FROM "t1"
            WHERE FALSE
        "#;
        test_it("empty_result_inner", original, expected);
    }

    /// Step-4 ORDER BY shape: inlinable has GROUP BY; outer ORDER BY contains
    /// `s.rownum + s.rownum` which post-substitution becomes an expression
    /// over a GROUP BY key (not a literal GROUP BY reference).
    /// `normalize_topk_with_aggregate` would reject the post-pipeline result;
    /// we anticipate by rejecting at eligibility time.  The leading derived
    /// table stays untouched (rewrite is a no-op).
    #[test]
    fn outer_order_expr_over_group_by_key_bails() {
        let original_text = r#"
        SELECT s.rownum, s.test_dec + s.test_dec, tags.t
        FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
             FROM qa.datatypes AS o
             GROUP BY o.rownum, o.test_dec
             ORDER BY o.rownum
        ) AS s
        CROSS JOIN LATERAL (SELECT ARRAY_AGG(t.test_varchar) AS t
             FROM qa.datatypes1 AS t
             WHERE t.rownum = s.rownum
        ) AS tags
        ORDER BY s.rownum + s.rownum
        "#;
        // No rewrite: leading derived table preserved as-is (eligibility
        // rejection at step 4's new ORDER-BY-shape sub-check).
        let expected_text = r#"
        SELECT "s"."rownum", ("s"."test_dec" + "s"."test_dec"), "tags"."t"
        FROM (SELECT "o"."rownum", sum("o"."test_dec") AS "test_dec"
              FROM "qa"."datatypes" AS "o"
              GROUP BY "o"."rownum", "o"."test_dec"
              ORDER BY "o"."rownum" NULLS LAST) AS "s"
        CROSS JOIN LATERAL (SELECT array_agg("t"."test_varchar") AS "t"
              FROM "qa"."datatypes1" AS "t"
              WHERE ("t"."rownum" = "s"."rownum")) AS "tags"
        ORDER BY ("s"."rownum" + "s"."rownum") NULLS LAST
        "#;
        test_it(
            "outer_order_expr_over_group_by_key_bails",
            original_text,
            expected_text,
        );
    }

    /// Step-4 ORDER BY shape: outer ORDER BY references only a literal inner
    /// GROUP BY key.  The new sub-check accepts; inlining proceeds.
    #[test]
    fn outer_order_literal_group_by_key_inlines() {
        let original_text = r#"
        SELECT s.rownum, s.test_dec
        FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
             FROM qa.datatypes AS o
             GROUP BY o.rownum, o.test_dec
        ) AS s
        ORDER BY s.rownum
        "#;
        let expected_text = r#"
        SELECT "o"."rownum", sum("o"."test_dec") AS "test_dec"
        FROM "qa"."datatypes" AS "o"
        GROUP BY "o"."rownum", "o"."test_dec"
        ORDER BY "o"."rownum" ASC NULLS LAST
        "#;
        test_it(
            "outer_order_literal_group_by_key_inlines",
            original_text,
            expected_text,
        );
    }
    /// Outer SELECT references a GROUP BY key of the inner grouped DT.
    /// Previously rejected by `is_agg_derived_outputs.all()` because
    /// `s.k` maps to `t.k` (a column reference, not aggregate).  Now
    /// accepted via the membership-only check.
    #[test]
    fn outer_select_references_inner_group_by_key_inlines() {
        let original_text = r#"
        SELECT s.k, s.total
        FROM (SELECT t.k, SUM(t.x) AS total FROM t GROUP BY t.k) AS s
        ORDER BY s.k
        "#;
        let expected_text = r#"
        SELECT "t"."k", sum("t"."x") AS "total"
        FROM "t"
        GROUP BY "t"."k"
        ORDER BY "t"."k" ASC NULLS LAST
        "#;
        test_it(
            "outer_select_references_inner_group_by_key_inlines",
            original_text,
            expected_text,
        );
    }

    /// Outer SELECT references a compound expression mixing a GROUP BY
    /// key column and an aggregate-derived column from a grouped inner
    /// (e.g., `s.k + s.total` where `s.k` maps to a GROUP BY key and
    /// `s.total` maps to `SUM(t.x)`).  Previously rejected by
    /// `is_agg_derived_outputs.all(is_aggregated)` because `s.k` maps
    /// to the non-aggregated `t.k`.  Now accepted via the membership-
    /// only check (both columns are projected by the inner; upstream
    /// `validate_group_by_semantics` already guarantees the post-inline
    /// expression is grouped-query-valid).
    #[test]
    fn outer_select_references_compound_grouped_expression_inlines() {
        let original_text = r#"
        SELECT s.k + s.total AS combined
        FROM (SELECT t.k, SUM(t.x) AS total FROM t GROUP BY t.k) AS s
        "#;
        let expected_text = r#"
        SELECT ("t"."k" + sum("t"."x")) AS "combined"
        FROM "t"
        GROUP BY "t"."k"
        "#;
        test_it(
            "outer_select_references_compound_grouped_expression_inlines",
            original_text,
            expected_text,
        );
    }

    /// Outer SELECT references an inner column not projected by the
    /// inner DT.  Must still bail (the membership check fails).
    #[test]
    fn outer_select_references_unprojected_inner_column_bails() {
        let original_text = r#"
        SELECT s.unprojected
        FROM (SELECT t.k FROM t GROUP BY t.k) AS s
        "#;
        let expected_text = original_text;
        test_it(
            "outer_select_references_unprojected_inner_column_bails",
            original_text,
            expected_text,
        );
    }

    /// Outer ORDER BY references a non-inner-relation column that
    /// becomes a GROUP BY addition post-inline.  Previously rejected
    /// because the ORDER BY check only compared against the inner's
    /// GROUP BY.  Now accepted via the threaded additions.  Shape:
    /// aggregated DT with an ExactlyOne scalar sibling, comma-joined,
    /// outer ORDER BY by the scalar.
    #[test]
    fn outer_order_by_non_inner_relation_in_additions_inlines() {
        let original_text = r#"
        SELECT s.total, sc.x
        FROM (SELECT t.k, SUM(t.x) AS total FROM t GROUP BY t.k) AS s,
             (SELECT MAX(u.y) AS x FROM u) AS sc
        ORDER BY sc.x
        "#;
        let expected_text = r#"
        SELECT sum("t"."x") AS "total", "sc"."x"
        FROM "t"
        CROSS JOIN (SELECT max("u"."y") AS "x" FROM "u") AS "sc"
        GROUP BY "t"."k", "sc"."x"
        ORDER BY "sc"."x" ASC NULLS LAST
        "#;
        test_it(
            "outer_order_by_non_inner_relation_in_additions_inlines",
            original_text,
            expected_text,
        );
    }

    /// Outer ORDER BY references a column outside the post-inline
    /// grouping set — neither in the inner GROUP BY, an addition, an
    /// aggregate, nor a SELECT alias.  Must still bail.  Shape:
    /// aggregated DT with a scalar sibling, outer ORDER BY by a
    /// non-GROUP-BY column of the sibling (not an `Expr::Column` of
    /// either relation matching anything in the post-inline grouping
    /// set).
    #[test]
    fn outer_order_by_unrelated_column_bails() {
        let original_text = r#"
        SELECT s.total, sc.x
        FROM (SELECT t.k, SUM(t.x) AS total FROM t GROUP BY t.k) AS s,
             (SELECT MAX(u.y) AS x, u.z FROM u GROUP BY u.z) AS sc
        ORDER BY sc.z
        "#;
        let expected_text = original_text;
        test_it(
            "outer_order_by_unrelated_column_bails",
            original_text,
            expected_text,
        );
    }

    /// `LATERAL` at position 0 is structurally a no-op: position 0 has
    /// no preceding FROM siblings to correlate with, and correlation to
    /// outer-enclosing scopes does not require the flag.  Verify the
    /// pass clears it.  Body contains a window function so the
    /// eligibility check bails (no hoist); the only AST change between
    /// input and output is the cleared LATERAL flag.
    #[test]
    fn position_0_lateral_flag_is_cleared_independent_of_hoisting() {
        let original = r#"
        SELECT s.x, s.rn
        FROM LATERAL (SELECT t.x, row_number() over() AS rn FROM t) AS s"#;
        let expected = r#"
        SELECT s.x, s.rn
        FROM (SELECT t.x, row_number() over() AS rn FROM t) AS s"#;
        test_it(
            "position_0_lateral_flag_is_cleared_independent_of_hoisting",
            original,
            expected,
        );
    }
}
