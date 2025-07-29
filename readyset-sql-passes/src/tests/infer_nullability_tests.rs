use crate::infer_nullability::infer_select_field_nullability;
use crate::rewrite_utils::expect_field_as_expr;
use crate::unnest_subqueries::NonNullSchema;
use readyset_sql::Dialect;
use readyset_sql::ast::{Column, Relation, SelectStatement};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};
use std::collections::{HashMap, HashSet};

/// ---- Parser shim ---------------------------------------------------------
/// Replace the body of `parse_select` with your project's SELECT parser.
/// It must return a populated `SelectStatement` for the given SQL.
fn parse_select(sql: &str) -> SelectStatement {
    parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
        .unwrap_or_else(|_| panic!("Failed to parse select statement: {sql}"))
}

/// ---- Mock schema with NOT NULL facts (0-FP safe) -------------------------
/// We assume, by schema:
///   spj.qty, spj.sn, spj.pn   are NOT NULL
///   p.pn,  p.weight, p.city   are NOT NULL
///   j.jn                      is NOT NULL
///
/// Add more if your tests need them; this is conservative and safe.
struct TestSchema {
    nn: HashMap<Relation, Vec<&'static str>>,
}

impl TestSchema {
    fn new() -> Self {
        let mut nn = HashMap::new();
        nn.insert(
            "spj".into(),
            vec!["qty", "sn", "pn"], // assumed NOT NULL by schema
        );
        nn.insert(
            "p".into(),
            vec!["pn", "weight", "city"], // assumed NOT NULL by schema
        );
        nn.insert(
            "j".into(),
            vec!["jn"], // assumed NOT NULL by schema
        );
        // add other base tables if needed
        Self { nn }
    }
}

impl NonNullSchema for TestSchema {
    fn not_null_columns_of(&self, rel: &Relation) -> HashSet<Column> {
        let mut out = HashSet::new();
        if let Some(cols) = self.nn.get(rel) {
            for &col in cols {
                out.insert(Column {
                    name: col.into(),
                    table: Some(rel.clone()),
                });
            }
        }
        out
    }
}

/// ---- Tiny assertion helper -----------------------------------------------
fn assert_nullability(sql: &str, expect_non_null: bool, schema: &dyn NonNullSchema) {
    let stmt: SelectStatement = parse_select(sql);
    let (field_expr, _alias) = expect_field_as_expr(
        stmt.fields
            .first()
            .expect("test queries must select their target expression first"),
    );
    let res = infer_select_field_nullability(field_expr, &stmt, schema)
        .expect("inference should succeed");
    assert_eq!(
        res, expect_non_null,
        "expected non-null={}, got {} for SQL:\n{}",
        expect_non_null, res, sql
    );
}

/// ---- Tests ---------------------------------------------------------------

#[test]
fn null_inf_01_where_is_not_null_plus_concat_call() {
    // WHERE proves s.city non-NULL; concat is strict → result non-NULL.
    let sql = r#"
        SELECT concat(s.city, '-ok') AS x
        FROM s
        WHERE s.city IS NOT NULL
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_02_not_is_null_pattern_and_unary_not() {
    // NOT (s.sname IS NULL) is null-rejecting; length is strict.
    let sql = r#"
        SELECT length(s.sname) AS len_sname
        FROM s
        WHERE NOT (s.sname IS NULL)
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_03_between_in_select() {
    // spj.qty is schema-NOT-NULL; literals are non-NULL → BETWEEN result non-NULL.
    let sql = r#"
        SELECT (spj.qty BETWEEN 1 AND 100) AS in_band
        FROM spj
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_04_in_list_in_select() {
    // spj.qty is schema-NOT-NULL; list literals are non-NULL → IN result non-NULL.
    let sql = r#"
        SELECT (spj.qty IN (1, 2, 3)) AS hit
        FROM spj
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_05_in_subquery_in_where_derives_lhs() {
    // Derivation: IN (subquery) proves LHS non-NULL in filters. Here the projected column is spj.qty
    // which is schema-NOT-NULL anyway; this tests the IN-subquery derivation path.
    let sql = r#"
        SELECT spj.qty
        FROM spj
        WHERE spj.qty IN (
          SELECT p.weight
          FROM p
          WHERE p.city = 'LONDON' AND p.pn = spj.pn
        )
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_06_left_join_present_via_where_rhs_filter() {
    // LEFT JOIN p, then WHERE p.city='LONDON' → p becomes present; schema seeds p.city NOT NULL.
    // length(p.city) therefore inferred non-NULL.
    let sql = r#"
        SELECT length(p.city) AS len_city
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        WHERE p.city = 'LONDON'
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_07_left_then_inner_join_promotes_rhs() {
    // p is RHS of LEFT JOIN; INNER JOIN j ON p.jn=j.jn is null-rejecting for p.jn, so p is present.
    // Then p.city is seeded from schema (NOT NULL) → Column inference is non-NULL.
    let sql = r#"
        SELECT p.city
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        JOIN j ON p.jn = j.jn
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_08_derived_sig_with_where_promotes_present() {
    // Derived table dp(pn, c=count(*)) is RHS of LEFT JOIN; WHERE dp.c > 0 is null-rejecting on dp.c
    // → dp present → seed derived signature (c is non-NULL) → Column dp.c is non-NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE dp.c > 0
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_09_derived_sig_without_presence_proof_is_maybe_null() {
    // Same derived table, but no WHERE/INNER evidence → dp may be null-extended → do NOT seed sig.
    // Column dp.c should NOT be inferred as non-NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
    "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_10_case_all_branches_non_null() {
    // Both THEN and ELSE are schema-NOT-NULL (spj.sn, spj.pn) → CASE result non-NULL.
    let sql = r#"
        SELECT CASE WHEN spj.qty > 0 THEN spj.sn ELSE spj.pn END AS c1
        FROM spj
    "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_11_case_without_else_maybe_null() {
    let sql = r#"
            SELECT CASE WHEN spj.qty > 0 THEN spj.sn END AS c2
            FROM spj
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_12_nested_derived_where_promotes_present() {
    // dp is nested 2 levels; outer WHERE dp.c > 0 promotes present → dp.c non-NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, c
          FROM (
            SELECT pn, COUNT(*) AS c
            FROM p
            GROUP BY pn
          ) t
        ) AS dp ON spj.pn = dp.pn
        WHERE dp.c > 0
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_13_two_rhs_left_joins_both_promoted_by_where() {
    // dp and dq both promoted to present by WHERE; addition is strict → non-NULL.
    let sql = r#"
        SELECT (dp.c + dq.d) AS sum_cd
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        LEFT JOIN (
          SELECT pn, COUNT(*) AS d
          FROM spj
          GROUP BY pn
        ) AS dq ON spj.pn = dq.pn
        WHERE dp.c > 0 AND dq.d > 0
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_14_present_via_downstream_inner_join_to_derived() {
    // p is RHS of LEFT; j2 derived enforces INNER `p.jn = j2.jn` → p present → p.city non-NULL.
    let sql = r#"
        SELECT p.city
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        JOIN (
          SELECT jn FROM j
        ) AS j2 ON j2.jn = p.jn
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_15_case_both_branches_non_null_with_nested_derived() {
    // Anti-semi via LEFT; CASE branches are schema non-NULL → CASE non-NULL regardless of match.
    let sql = r#"
        SELECT CASE WHEN d1.minw IS NULL THEN spj.sn ELSE spj.pn END AS c
        FROM spj
        LEFT JOIN (
          SELECT sn, MIN(weight) AS minw
          FROM p
          WHERE city = 'LONDON'
          GROUP BY sn
        ) AS d1 ON d1.sn = spj.sn AND d1.minw = spj.qty
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_16_coalesce_with_literal_masks_null_extended_rhs() {
    // dp may be null-extended; COALESCE(dp.c, 0) ensures result non-NULL.
    let sql = r#"
        SELECT COALESCE(dp.c, 0) AS c0
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_17_cast_preserves_maybe_null_without_presence() {
    // dp remains possibly null-extended; CAST doesn’t change nullability → maybe NULL.
    let sql = r#"
        SELECT CAST(dp.c AS BIGINT) AS c_cast
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_18_min_over_not_null_plus_where_is_not_null() {
    // MIN(city) is non-NULL per non-empty group; WHERE dp.mincity IS NOT NULL promotes presence.
    let sql = r#"
        SELECT length(dp.mincity) AS lmin
        FROM spj
        LEFT JOIN (
          SELECT pn, MIN(city) AS mincity
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE dp.mincity IS NOT NULL
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_19_derived_case_missing_else_guarded_by_where_is_not_null() {
    // Inner CASE can be NULL; outer WHERE `IS NOT NULL` promotes presence and proves non-NULL.
    let sql = r#"
        SELECT dp.flag
        FROM spj
        LEFT JOIN (
          SELECT pn,
                 CASE WHEN COUNT(*) > 0 THEN 1 END AS flag
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE dp.flag IS NOT NULL
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_20_or_does_not_promote_presence() {
    // OR on RHS column doesn’t prove presence; p.city remains maybe NULL.
    let sql = r#"
        SELECT p.city
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        WHERE (p.city = 'LONDON' OR spj.qty > 0)
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_21_and_does_promote_presence() {
    // Conjunct on RHS `p.city = 'LONDON'` is null-rejecting → p present → p.city non-NULL.
    let sql = r#"
        SELECT p.city
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        WHERE p.city = 'LONDON' AND spj.qty > 0
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_22_in_subquery_nested_derived_no_presence_dp_maybe_null() {
    // WHERE uses IN with nested derived (exercises derivation path), but dp not referenced → dp.c maybe NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE spj.qty IN (
          SELECT m.w
          FROM (
            SELECT MIN(weight) AS w, pn
            FROM p
            GROUP BY pn
          ) AS m
          WHERE m.pn = spj.pn
        )
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_23_present_via_inner_join_on_dp_column() {
    // INNER JOIN with predicate on dp.c (in ON) promotes dp present → dp.c non-NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        JOIN (SELECT 1 AS z) AS t ON dp.c > 0
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_24_double_rhs_chain_both_promoted_by_inner_join_on_both() {
    // dp1 and dq2 both promoted present by inner join ON; selecting dq2.c2 → non-NULL.
    let sql = r#"
        SELECT dq2.c2
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c1
          FROM p
          GROUP BY pn
        ) AS dp1 ON spj.pn = dp1.pn
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c2
          FROM spj
          GROUP BY pn
        ) AS dq2 ON spj.pn = dq2.pn
        JOIN (SELECT 1 AS z) AS t ON (dp1.c1 > 0 AND dq2.c2 > 0)
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_25_window_count_in_derived_where_promotes_present() {
    // Window COUNT() is always non-NULL; WHERE on dw.tot promotes presence → dw.tot non-NULL.
    let sql = r#"
        SELECT dw.tot
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) OVER () AS tot
          FROM p
        ) AS dw ON spj.pn = dw.pn
        WHERE dw.tot >= 0
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_26_window_count_without_presence_is_maybe_null() {
    // Same derived, but no presence proof → dw.tot maybe NULL.
    let sql = r#"
        SELECT dw.tot
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) OVER () AS tot
          FROM p
        ) AS dw ON spj.pn = dw.pn
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_27_case_multiple_when_else_all_non_null_no_presence_needed() {
    // All THEN/ELSE expressions are schema non-NULL → CASE result non-NULL regardless of dp.
    let sql = r#"
        SELECT CASE
                 WHEN dp.c > 10 THEN spj.sn
                 WHEN dp.c <= 10 THEN spj.pn
                 ELSE spj.sn
               END AS c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_28_coalesce_across_two_rhs_then_literal() {
    // Neither dp nor dq is proven present, but COALESCE(..., 0) → expression non-NULL.
    let sql = r#"
        SELECT COALESCE(dp.c, dq.d, 0) AS cd0
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        LEFT JOIN (
          SELECT pn, COUNT(*) AS d
          FROM spj
          GROUP BY pn
        ) AS dq ON spj.pn = dq.pn
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_29_where_on_dp_group_key_is_not_null_promotes_presence() {
    // dp.pn is derived from schema-NN p.pn; WHERE dp.pn IS NOT NULL promotes presence → dp.pn non-NULL.
    let sql = r#"
        SELECT dp.pn
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE dp.pn IS NOT NULL
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}

#[test]
fn null_inf_30_where_only_on_outer_base_does_not_promote_dp() {
    // WHERE references only spj; dp remains maybe-null → dp.c maybe NULL.
    let sql = r#"
        SELECT dp.c
        FROM spj
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        WHERE spj.qty > 0
        "#;
    assert_nullability(sql, false, &TestSchema::new());
}

#[test]
fn null_inf_31_concat_of_present_rhs_and_present_derived() {
    // p present via WHERE; dp present via INNER JOIN ON; concat(strict) over both → non-NULL.
    let sql = r#"
        SELECT concat(p.city, (dp.c)::text) AS s
        FROM spj
        LEFT JOIN p ON spj.pn = p.pn
        LEFT JOIN (
          SELECT pn, COUNT(*) AS c
          FROM p
          GROUP BY pn
        ) AS dp ON spj.pn = dp.pn
        JOIN (SELECT 1 AS t) AS t ON dp.c > 0
        WHERE p.city = 'LONDON'
        "#;
    assert_nullability(sql, true, &TestSchema::new());
}
