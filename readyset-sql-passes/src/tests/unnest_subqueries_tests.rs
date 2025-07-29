use crate::unnest_subqueries::{NonNullSchema, unnest_subqueries_main};
use readyset_sql::ast::{Column, Relation};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock, RwLockWriteGuard};

fn mk_col(table: &str, name: &str) -> Column {
    Column {
        name: name.into(),
        table: Some(table.into()),
    }
}

struct SpjNonNullSchema {
    schema: HashMap<Relation, HashSet<Column>>,
}

impl SpjNonNullSchema {
    fn default() -> Self {
        Self {
            schema: HashMap::from([
                ("spj".into(), HashSet::from([mk_col("spj", "qty")])),
                (
                    "p".into(),
                    HashSet::from([mk_col("p", "weight"), mk_col("p", "city")]),
                ),
                (
                    "datatypes".into(),
                    HashSet::from([mk_col("datatypes", "rownum")]),
                ),
                (
                    "datatypes1".into(),
                    HashSet::from([mk_col("datatypes1", "rownum")]),
                ),
            ]),
        }
    }
}

impl NonNullSchema for SpjNonNullSchema {
    fn not_null_columns_of(&self, rel: &Relation) -> HashSet<Column> {
        if let Some(non_null_cols) = self.schema.get(rel) {
            non_null_cols.clone()
        } else {
            HashSet::new()
        }
    }
}

const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

static GLOBAL_SCHEMA: OnceLock<RwLock<SpjNonNullSchema>> = OnceLock::new();

fn global_schema() -> &'static RwLock<SpjNonNullSchema> {
    GLOBAL_SCHEMA.get_or_init(|| RwLock::new(SpjNonNullSchema::default()))
}

fn get_schema_guard() -> RwLockWriteGuard<'static, SpjNonNullSchema> {
    global_schema().write().unwrap()
}

fn rewrite(
    _test_name: &str,
    sql: &str,
    schema_guard: RwLockWriteGuard<SpjNonNullSchema>,
) -> String {
    let mut stmt =
        parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql).expect("parse");

    let _ = unnest_subqueries_main(&mut stmt, &*schema_guard).expect("rewrite");

    let f = stmt.display(Dialect::PostgreSQL);
    f.to_string()
}

fn contains_case(s: &str) -> bool {
    s.contains("CASE") || s.contains("case")
}

fn contains_left_outer_join(s: &str) -> bool {
    s.contains("LEFT OUTER JOIN") || s.contains("left outer join")
}

fn contains_np_3vl(s: &str) -> bool {
    s.contains("NP_3VL")
}

fn contains_ep_3vl(s: &str) -> bool {
    s.contains("EP_3VL")
}

fn make_col_null_free(
    table: &str,
    column: &str,
    schema_guard: &mut RwLockWriteGuard<SpjNonNullSchema>,
) {
    if let Some(cols) = schema_guard.schema.get_mut(&table.into()) {
        cols.insert(mk_col(table, column));
    } else {
        schema_guard
            .schema
            .insert(table.into(), HashSet::from([mk_col(table, column)]));
    }
}

fn make_col_nullable(
    table: &str,
    column: &str,
    schema_guard: &mut RwLockWriteGuard<SpjNonNullSchema>,
) {
    if let Some(cols) = schema_guard.schema.get_mut(&table.into()) {
        cols.remove(&mk_col(table, column));
    }
}

fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
    let rewritten_stmt =
        match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, original_text) {
            Ok(mut stmt) => match unnest_subqueries_main(&mut stmt, &*get_schema_guard()) {
                Ok(_) => {
                    println!(">>>>>> Unnested: {}", stmt.display(Dialect::PostgreSQL));
                    Ok(stmt.clone())
                }
                Err(err) => Err(err),
            },
            Err(e) => panic!("> {test_name}: ORIGINAL STATEMENT PARSE ERROR: {e}"),
        };

    match rewritten_stmt {
        Ok(expect_stmt) => {
            assert_eq!(
                expect_stmt,
                match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, expect_text) {
                    Ok(stmt) => stmt,
                    Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                }
            );
        }
        Err(e) => {
            println!("> {test_name}: REWRITE ERROR: {e}");
            assert!(expect_text.is_empty(), "> {test_name}: REWRITE ERROR: {e}")
        }
    }
}

// WHERE IN; correlated; with GROUP BY; NP/EP omitted when null-safe; RHS deduplicated
// (DISTINCT).
#[test]
fn test1() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    spj.pn in (
        select
            p.pn
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 17
        group by
            p.pn, p.sn
    );"#;
    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT "p"."pn" AS "pn", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 17)) AS "GNL"
        ON (("GNL"."jn" = "spj"."jn") AND ("spj"."pn" = "GNL"."pn"))"#;
    test_it("test1", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test2() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    spj.pn = (
        select
            max(p.pn)
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 17
    )"#;
    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT max("p"."pn") AS "max(pn)", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 17) GROUP BY "p"."jn") AS "GNL"
        ON (("GNL"."jn" = "spj"."jn") AND ("spj"."pn" = "GNL"."max(pn)"))"#;
    test_it("test2", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; RHS deduplicated (DISTINCT).
#[test]
fn test3() {
    let original_text = r#"
select
    s.sn,
    s.sname,
    s.status,
    s.city
from
    s
where
    exists (
        select
            spj.pn,
            max(sn) max_sn
        from
            spj
        where
            s.sn = spj.sn
            and spj.jn = 'J33333'
        group by
            spj.pn
    );"#;
    let expected_text = r#"SELECT "s"."sn", "s"."sname", "s"."status", "s"."city" FROM "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "spj"."sn" AS "sn" FROM "spj" WHERE ("spj"."jn" = 'J33333')) AS "GNL"
        ON ("s"."sn" = "GNL"."sn")"#;
    test_it("test3", original_text, expected_text);
}

// WHERE EXISTS; correlated; rewritten as INNER JOIN; RHS deduplicated (DISTINCT).
#[test]
fn test4() {
    let original_text = r#"
SELECT
    s.sname
FROM
    s
    INNER JOIN p p1 on s.city = p1.city
WHERE
    EXISTS (
        SELECT
            1
        FROM
            spj b
        where
            b.sn = p1.sn and b.pn = s.pn
            and b.jn = (
                SELECT
                    max(j.jn)
                FROM
                    j
                WHERE
                    EXISTS (
                        SELECT
                            1
                        FROM
                            p c
                        where
                            c.pn = s.pn
                    )
            )
    );"#;

    let expected_text = r#"SELECT "s"."sname" FROM "s" INNER JOIN "p" AS "p1" ON ("s"."city" = "p1"."city")
        INNER JOIN (SELECT DISTINCT 1 AS "present_", "GNL"."pn" AS "pn", "b"."pn" AS "pn0", "b"."sn" AS "sn"
        FROM "spj" AS "b" INNER JOIN (SELECT max("j"."jn") AS "max(jn)", "GNL"."pn" AS "pn" FROM "j"
        INNER JOIN (SELECT DISTINCT 1 AS "present_", "c"."pn" AS "pn" FROM "p" AS "c") AS "GNL"  GROUP BY "GNL"."pn") AS "GNL"
        ON ("b"."jn" = "GNL"."max(jn)")) AS "GNL"
        ON ("GNL"."sn" = "p1"."sn") WHERE (("GNL"."pn0" = "s"."pn") AND ("GNL"."pn" = "s"."pn"))"#;
    test_it("test4", original_text, expected_text);
}

// WHERE EXISTS; correlated.
#[test]
fn test5() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    exists (
        select
            max(p.sn)
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 12
    )"#;
    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj""#;
    test_it("test5", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test6() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    spj.pn = (
        select
            max(p.pn)
        from
            p
        where
            p.weight = 17
    )"#;
    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT max("p"."pn") AS "max(pn)" FROM "p" WHERE ("p"."weight" = 17)) AS "GNL"
        ON ("spj"."pn" = "GNL"."max(pn)")"#;
    test_it("test6", original_text, expected_text);
}

// Left out nested, as the subquery is OR-ed
// WHERE EXISTS; correlated; with GROUP BY; RHS deduplicated (DISTINCT).
#[test]
fn test7() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    exists (select
            1
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 12
        group by
            p.pn
    ) and
    (spj.pn = (
        select
            min(p.pn)
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 12
    ) or spj.qty > 100)"#;
    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 12)) AS "GNL"
        ON ("GNL"."jn" = "spj"."jn") WHERE (("spj"."pn" = (SELECT min("p"."pn") FROM "p"
        WHERE (("p"."jn" = "spj"."jn") AND ("p"."weight" = 12)))) OR ("spj"."qty" > 100))"#;
    test_it("test7", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; RHS deduplicated (DISTINCT).
#[test]
fn test8() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.weight = 17
        group by p.pn, p.weight
    )"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "p" WHERE ("p"."weight" = 17)) AS "GNL""#;
    test_it("test8", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; rewritten as INNER JOIN; RHS deduplicated
// (DISTINCT).
#[test]
fn test9() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    (select avg(p.weight) from p where p.pn = spj.pn and p.sn = 'S33333') avg_weight,
    spj.qty
from
    spj
where
    exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.jn = spj.jn and p.weight = 17
        group by p.pn, p.weight
    )"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "GNL1"."avg(weight)" AS "avg_weight", "spj"."qty"
        FROM "spj" INNER JOIN (SELECT DISTINCT 1 AS "present_", "p"."jn" AS "jn" FROM "p"
        WHERE ("p"."weight" = 17)) AS "GNL" ON ("GNL"."jn" = "spj"."jn") LEFT OUTER JOIN
        (SELECT avg("p"."weight") AS "avg(weight)", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."sn" = 'S33333') GROUP BY "p"."pn") AS "GNL1" ON ("GNL1"."pn" = "spj"."pn")"#;

    test_it("test9", original_text, expected_text);
}

// Negative test: The subquery in select list uses ungrouped column from the outer query
// WHERE EXISTS; correlated; with GROUP BY.
#[test]
fn test10() {
    let original_text = r#"
select
    (select avg(p.weight) from p where p.pn = spj.pn and p.sn = 'S33333') avg_weight,
    spj.qty
from
    spj
where
    exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.jn = spj.jn and p.weight = 12
        group by p.pn, p.weight
    )
group by spj.qty"#;

    let expected_text = r#""#;
    test_it("test10", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN.
#[test]
fn test11() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    (select avg(p.weight) from p where p.pn = spj.pn and p.sn = 'S33333') avg_weight,
    spj.qty
from
    spj
where
    not exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.jn = spj.jn and p.weight = 12
        group by p.pn, p.weight
    )"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "GNL1"."avg(weight)" AS "avg_weight", "spj"."qty"
        FROM "spj" LEFT OUTER JOIN (SELECT 1 AS "present_", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 12)) AS "GNL"
        ON ("GNL"."jn" = "spj"."jn") LEFT OUTER JOIN (SELECT avg("p"."weight") AS "avg(weight)", "p"."pn" AS "pn"
        FROM "p" WHERE ("p"."sn" = 'S33333') GROUP BY "p"."pn") AS "GNL1" ON ("GNL1"."pn" = "spj"."pn")
        WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test11", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated.
#[test]
fn test12() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    not exists (
        select
            max(p.sn)
        from
            p
        where
            p.jn = spj.jn
            and p.weight = 17
    )"#;

    let expected_text =
        r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" WHERE FALSE"#;
    test_it("test12", original_text, expected_text);
}

// Test with no rewrite, as any rewrite would cause `problematic self-join` issue
// WHERE IN; correlated; with GROUP BY; NP/EP omitted when null-safe.
#[test]
fn test13() {
    let original_text = r#"
SELECT
    spj.sn,
    spj.pn,
    spj.qty
from
    spj
where
    EXISTS (
        SELECT
            1
        FROM
            p AS p5
        WHERE
            p5.weight = (
                SELECT
                    MIN(p6.weight)
                FROM
                    p AS p6
                WHERE
                    p6.pn in (
                        SELECT
                            s.pn
                        FROM
                            s
                        WHERE
                            s.sn = spj.sn
                        GROUP BY
                            s.pn, s.status
                    )
            )
    );"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."qty" FROM "spj" WHERE
        EXISTS (SELECT 1 FROM "p" AS "p5" WHERE ("p5"."weight" = (SELECT min("p6"."weight") FROM "p" AS "p6"
        WHERE "p6"."pn" IN (SELECT "s"."pn" FROM "s" WHERE ("s"."sn" = "spj"."sn") GROUP BY "s"."pn", "s"."status"))))"#;
    test_it("test13", original_text, expected_text);
}

// Test with partial rewrite, as the full rewrite would cause `problematic self-join` issue
// WHERE IN; correlated; LATERAL subquery flattened; with GROUP BY; rewritten as LEFT OUTER
// JOIN; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test14() {
    let original_text = r#"
SELECT
    spj.sn,
    spj.pn,
    spj.qty,
    L1.total_weight,
    L2.avg_weight_for_status,
    L3.adjusted_sum,
    (
      SELECT COUNT(*)
      FROM p AS p_non
      WHERE p_non.pn = spj.pn
        AND p_non.weight > 100
    ) AS high_weight_count
FROM
    spj
LEFT JOIN LATERAL (
    SELECT
        SUM(p1.weight) AS total_weight
    FROM p AS p1
    WHERE p1.pn = spj.pn
) AS L1
  ON TRUE
INNER JOIN LATERAL (
    SELECT
        AVG(p2.weight) AS avg_weight_for_status
    FROM p AS p2
    WHERE p2.city = (
        SELECT max(s2.city)
        FROM s AS s2
        WHERE s2.sn = spj.sn
    )
    AND p2.weight > 0
) AS L2
  ON TRUE
CROSS JOIN LATERAL (
    SELECT
        L_mid.sum_weight * 1.10 AS adjusted_sum
    FROM (
        SELECT
            SUM(p3.weight) AS sum_weight
        FROM p AS p3
        WHERE p3.jn = spj.jn
        GROUP BY p3.jn
    ) AS L_mid
) AS L3
WHERE
    spj.qty = (
      SELECT MAX(spj2.qty)
      FROM spj AS spj2
      WHERE spj2.pn = spj.pn
    )
    AND spj.pn IN (
      SELECT DISTINCT p4.pn
      FROM p AS p4
      WHERE p4.weight > 50
    );"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."qty", "l1"."total_weight",
        "l2"."avg_weight_for_status", "l3"."adjusted_sum", coalesce("GNL1"."count(*)", 0) AS "high_weight_count" FROM
        "spj" LEFT OUTER JOIN (SELECT sum("p1"."weight") AS "total_weight", "p1"."pn" AS "pn" FROM
        "p" AS "p1" GROUP BY "p1"."pn") AS "l1" ON ("l1"."pn" = "spj"."pn") LEFT OUTER JOIN
        (SELECT avg("p2"."weight") AS "avg_weight_for_status", "GNL"."sn" AS "sn" FROM "p" AS "p2" INNER JOIN
        (SELECT max("s2"."city") AS "max(city)", "s2"."sn" AS "sn" FROM "s" AS "s2" GROUP BY "s2"."sn") AS "GNL" ON
        ("p2"."city" = "GNL"."max(city)") WHERE ("p2"."weight" > 0) GROUP BY "GNL"."sn") AS "l2" ON
        ("l2"."sn" = "spj"."sn") INNER JOIN (SELECT ("l_mid"."sum_weight" * 1.10) AS "adjusted_sum", "l_mid"."jn" AS "jn"
        FROM (SELECT sum("p3"."weight") AS "sum_weight", "p3"."jn" AS "jn" FROM "p" AS "p3"
        GROUP BY "p3"."jn") AS "l_mid") AS "l3" ON ("l3"."jn" = "spj"."jn") INNER JOIN
        (SELECT DISTINCT "p4"."pn" AS "pn" FROM "p" AS "p4" WHERE ("p4"."weight" > 50)) AS "GNL"
        ON ("spj"."pn" = "GNL"."pn") LEFT OUTER JOIN (SELECT count(*) AS "count(*)", "p_non"."pn" AS "pn"
        FROM "p" AS "p_non" WHERE ("p_non"."weight" > 100) GROUP BY "p_non"."pn") AS "GNL1" ON ("GNL1"."pn" = "spj"."pn")
        WHERE ("spj"."qty" = (SELECT max("spj2"."qty") FROM "spj" AS "spj2" WHERE ("spj2"."pn" = "spj"."pn")))"#;
    test_it("test14", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test15() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    (spj.qty / 100) * 10 = (
        select
            max(s.status)
        from
            s
        where
            s.sn = spj.sn
    )"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT max("s"."status") AS "max(status)", "s"."sn" AS "sn" FROM "s" GROUP BY "s"."sn") AS "GNL" ON
        ("GNL"."sn" = "spj"."sn") WHERE ((("spj"."qty" / 100) * 10) = "GNL"."max(status)")"#;
    test_it("test15", original_text, expected_text);
}

// WHERE IN; correlated; with GROUP BY; NP/EP omitted when null-safe; RHS deduplicated
// (DISTINCT).
#[test]
fn test16() {
    let original_text = r#"
select
    spj.sn,
    spj.pn,
    spj.jn,
    spj.qty
from
    spj
where
    (spj.qty / 100) * 10 in (
        select
            max(s.status)
        from
            s
        where
            s.sn = spj.sn
        group by s.pn
    )"#;

    let expected_text = r#"SELECT "spj"."sn", "spj"."pn", "spj"."jn", "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT max("s"."status") AS "max(status)", "s"."sn" AS "sn" FROM "s" GROUP BY "s"."pn", "s"."sn") AS "GNL"
        ON ("GNL"."sn" = "spj"."sn") WHERE ((("spj"."qty" / 100) * 10) = "GNL"."max(status)")"#;
    test_it("test16", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; RHS deduplicated
// (DISTINCT).
#[test]
fn test17() {
    let original_text = r#"
select
    spj.qty = (select avg(p.weight) from p where p.pn = spj.pn and p.sn = 'S33333'),
    spj.qty
from
    spj
where
    exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.jn = spj.jn and p.weight = 12
        group by p.pn, p.weight
    )
group by spj.qty, spj.pn;"#;

    let expected_text = r#"SELECT ("spj"."qty" = "GNL1"."avg(weight)"), "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 12)) AS "GNL"
        ON ("GNL"."jn" = "spj"."jn") LEFT OUTER JOIN (SELECT avg("p"."weight") AS "avg(weight)", "p"."pn" AS "pn"
        FROM "p" WHERE ("p"."sn" = 'S33333') GROUP BY "p"."pn") AS "GNL1" ON ("GNL1"."pn" = "spj"."pn")
        GROUP BY "spj"."qty", "spj"."pn""#;
    test_it("test17", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; RHS deduplicated
// (DISTINCT).
#[test]
fn test18() {
    let original_text = r#"
select
    spj.qty = (select count(*) from p where p.pn = spj.pn and p.sn = 'S33333' group by spj.pn),
    spj.qty
from
    spj
where
    exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.jn = spj.jn and p.weight = 12
        group by p.pn, p.weight
    )
group by spj.qty, spj.pn;"#;

    let expected_text = r#"SELECT ("spj"."qty" = "GNL1"."count(*)"), "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "p"."jn" AS "jn" FROM "p" WHERE ("p"."weight" = 12)) AS "GNL"
        ON ("GNL"."jn" = "spj"."jn") LEFT OUTER JOIN (SELECT count(*) AS "count(*)", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."sn" = 'S33333') GROUP BY "p"."pn") AS "GNL1" ON ("GNL1"."pn" = "spj"."pn")
        GROUP BY "spj"."qty", "spj"."pn""#;
    test_it("test18", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated; boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test19() {
    let original_text = r#"
select
    spj.qty,
    case when not exists (
        select
            count(p.pn)
        from
            p
        where
            p.weight = 12
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT "spj"."qty", CASE WHEN FALSE THEN "spj"."sn" ELSE "spj"."pn" END AS "c" FROM "spj""#;
    test_it("test19", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated; rewritten as LEFT OUTER JOIN; RHS deduplicated (DISTINCT);
// boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test20() {
    let original_text = r#"
select
    case when not exists (
        select
            p.pn
        from
            p
        where
            p.weight = 17
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT CASE WHEN ("GNL"."present_" IS NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT 1 AS "present_" FROM "p" WHERE ("p"."weight" = 17)) AS "GNL""#;
    test_it("test20", original_text, expected_text);
}

// expression NOT IN; correlated; NP/EP omitted when null-safe; boolean SELECT-list shaped via
// CASE for 3-VL.
#[test]
fn test21() {
    let original_text = r#"
select
    case when (spj.qty + 100) / 100  not in (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON'
    ) then spj.sn else spj.pn end as C
from
    spj;"#;

    let expected_text = r#"SELECT CASE WHEN ((("spj"."qty" + 100) / 100) != "GNL"."count(weight)") THEN "spj"."sn"
        ELSE "spj"."pn" END AS "c" FROM "spj" LEFT OUTER JOIN
        (SELECT count("p"."weight") AS "count(weight)" FROM "p" WHERE ("p"."city" = 'LONDON')) AS "GNL""#;
    test_it("test21", original_text, expected_text);
}

// expression NOT IN; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; anti-semi via IS
// NULL guard; NP/EP omitted when null-safe; boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test22() {
    let original_text = r#"
select
    case when spj.qty not in (
        select
            p.weight
        from
            p
        where
            p.city = 'LONDON'
            and p.weight IS NOT NULL
        group by p.weight
    ) then spj.sn else spj.pn end as C
from
    spj
where
    spj.qty is not null"#;

    let expected_text = r#"SELECT CASE WHEN ("GNL"."weight" IS NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT "p"."weight" AS "weight" FROM "p"
        WHERE (("p"."city" = 'LONDON') AND ("p"."weight" IS NOT NULL)) GROUP BY "p"."weight") AS "GNL"
        ON ("spj"."qty" = "GNL"."weight") WHERE ("spj"."qty" IS NOT NULL)"#;
    test_it("test22", original_text, expected_text);
}

// expression subquery; correlated; rewritten as LEFT OUTER JOIN; boolean SELECT-list shaped via
// CASE for 3-VL.
#[test]
fn test23() {
    let original_text = r#"
select
    case when spj.qty > 100
    then (select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON'
    )
    else spj.qty end as C
from
    spj"#;
    let expected_text = r#"SELECT CASE WHEN ("spj"."qty" > 100) THEN "GNL"."min(weight)" ELSE "spj"."qty" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT min("p"."weight") AS "min(weight)" FROM "p" WHERE ("p"."city" = 'LONDON')) AS "GNL""#;
    test_it("test23", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; RHS deduplicated
// (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test24() {
    // Correlated subquery, which might return empty data set
    let original_text = r#"
select
    spj.qty,
    case when exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.weight = 17 and spj.sn = p.sn
        group by p.pn, p.weight
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT "spj"."qty", CASE WHEN ("GNL"."present_" IS NOT NULL) THEN "spj"."sn"
        ELSE "spj"."pn" END AS "c" FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT 1 AS "present_", "p"."sn" AS "sn"
        FROM "p" WHERE ("p"."weight" = 17)) AS "GNL" ON ("spj"."sn" = "GNL"."sn")"#;
    test_it("test24", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; RHS deduplicated
// (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test25() {
    // Correlated subquery, which might return empty data set
    let original_text = r#"
select
    spj.qty,
    case when not exists (
        select
            p.pn, max(p.pn)
        from
            p
        where
            p.weight = 12 and spj.sn = p.sn
        group by p.pn, p.weight
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT "spj"."qty", CASE WHEN ("GNL"."present_" IS NULL) THEN "spj"."sn"
        ELSE "spj"."pn" END AS "c" FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT 1 AS "present_", "p"."sn" AS "sn"
        FROM "p" WHERE ("p"."weight" = 12)) AS "GNL" ON ("spj"."sn" = "GNL"."sn")"#;
    test_it("test25", original_text, expected_text);
}

// expression NOT IN; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; anti-semi via IS
// NULL guard; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT); boolean SELECT-list
// shaped via CASE for 3-VL.
#[test]
fn test26() {
    // Correlated subquery, which might return empty data set
    let original_text = r#"
select
    case when spj.qty not in (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.sn = p.sn
        group by
            p.pn
    ) then spj.sn else spj.pn end as C
from
    spj
where spj.qty is not null
;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."count(weight)" IS NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT count("p"."weight") AS "count(weight)", "p"."sn" AS "sn" FROM "p"
        WHERE ("p"."city" = 'LONDON') GROUP BY "p"."pn", "p"."sn") AS "GNL"
        ON (("spj"."sn" = "GNL"."sn") AND ("spj"."qty" = "GNL"."count(weight)")) WHERE ("spj"."qty" IS NOT NULL)"#;
    test_it("test26", original_text, expected_text);
}

// expression IN; correlated; rewritten as LEFT OUTER JOIN; NP/EP omitted when null-safe; RHS
// deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test27() {
    let original_text = r#"
select
    case when spj.qty in (
        select
            p.weight
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT CASE WHEN ("GNL"."weight" IS NOT NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT "p"."weight" AS "weight", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON')) AS "GNL" ON (("spj"."pn" = "GNL"."pn") AND ("spj"."qty" = "GNL"."weight"))"#;
    test_it("test27", original_text, expected_text);
}

// expression IN; correlated; NP/EP omitted when null-safe; boolean SELECT-list shaped via CASE
// for 3-VL.
#[test]
fn test28() {
    // Correlated subquery, which always returns a single non-`NULL` value
    let original_text = r#"
select
    case when spj.qty in (
        select
            count(*)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
    ) then spj.sn else spj.pn end as C
from
    spj"#;

    let expected_text = r#"SELECT CASE WHEN ("spj"."qty" = coalesce("GNL"."count(*)", 0))
        THEN "spj"."sn" ELSE "spj"."pn" END AS "c" FROM "spj" LEFT OUTER JOIN
        (SELECT count(*) AS "count(*)", "p"."pn" AS "pn" FROM "p" WHERE ("p"."city" = 'LONDON')
        GROUP BY "p"."pn") AS "GNL" ON ("spj"."pn" = "GNL"."pn")"#;
    test_it("test28", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; RHS deduplicated (DISTINCT).
#[test]
fn test29() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    exists (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
        group by
            p.sn
    )"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON')) AS "GNL" ON ("spj"."pn" = "GNL"."pn")"#;
    test_it("test29", original_text, expected_text);
}

// WHERE EXISTS; correlated.
#[test]
fn test30() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    exists (
        select
            count(*)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
    )"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj""#;
    test_it("test30", original_text, expected_text);
}

// WHERE NOT EXISTS; correlated; with GROUP BY.
#[test]
fn test31() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    not exists (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
        group by
            p.sn
    )"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT 1 AS "present_", "p"."pn" AS "pn" FROM "p" WHERE ("p"."city" = 'LONDON')) AS "GNL"
        ON ("spj"."pn" = "GNL"."pn") WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test31", original_text, expected_text);
}

// WHERE IN; correlated; NP/EP omitted when null-safe.
#[test]
fn test32() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty in (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
    );"#;

    let expected_text = r#" SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT count("p"."weight") AS "count(weight)", "p"."pn" AS "pn" FROM "p" WHERE ("p"."city" = 'LONDON')
        GROUP BY "p"."pn") AS "GNL" ON ("spj"."pn" = "GNL"."pn")
        WHERE ("spj"."qty" = coalesce("GNL"."count(weight)", 0))"#;
    test_it("test32", original_text, expected_text);
}

// WHERE NOT IN; correlated; with GROUP BY; NP/EP omitted when null-safe.
#[test]
fn test33() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty not in (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn and p.weight IS NOT NULL
        group by
            p.sn
    )
    and spj.qty is not null;"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT min("p"."weight") AS "min(weight)", "p"."pn" AS "pn" FROM "p" WHERE
        (("p"."city" = 'LONDON') AND ("p"."weight" IS NOT NULL)) GROUP BY "p"."sn", "p"."pn") AS "GNL" ON
        (("spj"."pn" = "GNL"."pn") AND ("spj"."qty" = "GNL"."min(weight)")) WHERE
        (("spj"."qty" IS NOT NULL) AND ("GNL"."min(weight)" IS NULL))"#;
    test_it("test33", original_text, expected_text);
}

// WHERE NOT IN; correlated; NP/EP omitted when null-safe.
#[test]
fn test34() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty not in (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
    )
    and spj.qty is not null;"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT count("p"."weight") AS "count(weight)", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON') GROUP BY "p"."pn") AS "GNL" ON ("spj"."pn" = "GNL"."pn")
        WHERE (("spj"."qty" IS NOT NULL) AND ("spj"."qty" != coalesce("GNL"."count(weight)", 0)))"#;
    test_it("test34", original_text, expected_text);
}

// WHERE NOT IN; correlated; NP/EP omitted when null-safe.
#[test]
fn test35() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty not in (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn and spj.jn = p.jn
    )
    and spj.qty is not null;"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT min("p"."weight") AS "min(weight)", "p"."jn" AS "jn", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON') GROUP BY "p"."jn", "p"."pn") AS "GNL" ON
        (("spj"."pn" = "GNL"."pn") AND ("spj"."jn" = "GNL"."jn"))
        WHERE (("spj"."qty" IS NOT NULL) AND ("spj"."qty" != "GNL"."min(weight)"))"#;
    test_it("test35", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test36() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty = (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn and spj.jn = p.jn
    );"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT min("p"."weight") AS "min(weight)", "p"."jn" AS "jn", "p"."pn" AS "pn" FROM "p" WHERE
        ("p"."city" = 'LONDON') GROUP BY "p"."jn", "p"."pn") AS "GNL" ON
        ((("spj"."pn" = "GNL"."pn") AND ("spj"."jn" = "GNL"."jn")) AND ("spj"."qty" = "GNL"."min(weight)"));"#;
    test_it("test36", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test37() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty = (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn and spj.jn = p.jn
    );"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT count("p"."weight") AS "count(weight)", "p"."jn" AS "jn", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON') GROUP BY "p"."jn", "p"."pn") AS "GNL"
        ON (("spj"."pn" = "GNL"."pn") AND ("spj"."jn" = "GNL"."jn"))
        WHERE ("spj"."qty" = coalesce("GNL"."count(weight)", 0))"#;
    test_it("test37", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test38() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty > (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn and spj.jn = p.jn
    );"#;

    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT min("p"."weight") AS "min(weight)", "p"."jn" AS "jn", "p"."pn" AS "pn" FROM "p"
        WHERE ("p"."city" = 'LONDON') GROUP BY "p"."jn", "p"."pn") AS "GNL"
        ON (("spj"."pn" = "GNL"."pn") AND ("spj"."jn" = "GNL"."jn"))
        WHERE ("spj"."qty" > "GNL"."min(weight)")"#;
    test_it("test38", original_text, expected_text);
}

// WHERE IN; correlated; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test39() {
    let original_text = r#"
SELECT DISTINCT s.sn
FROM s AS s
WHERE s.pn IN (
  SELECT t.pn
  FROM spj AS t
  WHERE t.sn = s.sn      -- correlation (equality)
    AND t.qty > 100      -- extra filter (non-correlated)
);"#;
    let expected_text = r#"SELECT DISTINCT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT "t"."pn" AS "pn", "t"."sn" AS "sn" FROM "spj" AS "t"
        WHERE ("t"."qty" > 100)) AS "GNL" ON (("GNL"."sn" = "s"."sn") AND ("s"."pn" = "GNL"."pn"))"#;
    test_it("test39", original_text, expected_text);
}

// WHERE NOT IN; correlated; NP/EP omitted when null-safe.
#[test]
fn test40() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE p.pn IS NOT NULL
  AND p.pn NOT IN (
    SELECT t.pn
    FROM spj AS t
    WHERE t.jn = p.jn      -- correlation (equality)
      AND t.pn IS NOT NULL -- guarantees RHS first item non-null
);"#;
    let expected_text = r#"SELECT "p"."pn" FROM "p" AS "p" LEFT OUTER JOIN
        (SELECT "t"."pn" AS "pn", "t"."jn" AS "jn" FROM "spj" AS "t" WHERE ("t"."pn" IS NOT NULL)) AS "GNL" ON
        (("GNL"."jn" = "p"."jn") AND ("p"."pn" = "GNL"."pn")) WHERE (("p"."pn" IS NOT NULL) AND ("GNL"."pn" IS NULL));"#;
    test_it("test40", original_text, expected_text);
}

// SELECT-list subquery; correlated; rewritten as LEFT OUTER JOIN.
#[test]
fn test41() {
    let original_text = r#"
SELECT
  s.sn,
  (SELECT COUNT(*) FROM spj AS t WHERE t.sn = s.sn) AS order_cnt
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", coalesce("GNL"."count(*)", 0) AS "order_cnt" FROM "s" AS "s"
        LEFT OUTER JOIN (SELECT count(*) AS "count(*)", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."sn") AS "GNL"
        ON ("GNL"."sn" = "s"."sn");"#;
    test_it("test41", original_text, expected_text);
}

// WHERE scalar; correlated.
#[test]
fn test42() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE 0 < (
  SELECT SUM(t.qty)
  FROM spj AS t
  WHERE t.sn = s.sn
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT sum("t"."qty") AS "sum(qty)", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."sn") AS "GNL"
        ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."sum(qty)" > 0);"#;
    test_it("test42", original_text, expected_text);
}

// WHERE EXISTS; correlated; RHS deduplicated (DISTINCT).
#[test]
fn test43() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM spj AS t
  WHERE t.sn = s.sn
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."sn" AS "sn" FROM "spj" AS "t") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test43", original_text, expected_text);
}

// WHERE IN; correlated; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test44() {
    let original_text = r#"
SELECT j.jn
FROM j AS j
WHERE NOT EXISTS (
  SELECT 1
  FROM spj AS t
  WHERE t.jn = j.jn                                  -- outer correlation
    AND t.sn IN (SELECT s.sn FROM s AS s WHERE s.city = j.city) -- nested, also correlated to j
);"#;
    let expected_text = r#"SELECT "j"."jn" FROM "j" AS "j" LEFT OUTER JOIN
        (SELECT 1 AS "present_", "GNL"."city" AS "city", "t"."jn" AS "jn" FROM "spj" AS "t" INNER JOIN
        (SELECT DISTINCT "s"."sn" AS "sn", "s"."city" AS "city" FROM "s" AS "s") AS "GNL" ON ("t"."sn" = "GNL"."sn")) AS "GNL"
        ON (("GNL"."jn" = "j"."jn") AND ("GNL"."city" = "j"."city")) WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test44", original_text, expected_text);
}

// SELECT-list IN; correlated; rewritten as LEFT OUTER JOIN; 3-VL probes (NP/EP) present; RHS
// deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test45() {
    let original_text = r#"
SELECT
  s.sn,
  s.pn IN (
     SELECT t.pn FROM spj AS t
      WHERE t.sn = s.sn
  ) AS supplies_own_part
FROM
  s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", CASE WHEN ("GNL"."pn" IS NOT NULL) THEN TRUE
        WHEN (("s"."pn" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "supplies_own_part"
        FROM "s" AS "s" LEFT OUTER JOIN (SELECT DISTINCT "t"."pn" AS "pn", "t"."sn" AS "sn" FROM "spj" AS "t") AS "GNL"
        ON (("GNL"."sn" = "s"."sn") AND ("s"."pn" = "GNL"."pn")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."sn" AS "sn" FROM "spj" AS "t") AS "EP_3VL" ON ("EP_3VL"."sn" = "s"."sn")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."sn" AS "sn"
        FROM (SELECT 1 AS "present_", "t"."pn" AS "pn", "t"."sn" AS "sn" FROM "spj" AS "t") AS "NP_3VL"
        WHERE ("NP_3VL"."pn" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."sn" = "s"."sn")"#;
    test_it("test45", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened.
#[test]
fn test46() {
    let original_text = r#"
SELECT s.sn, x.jn, x.qty
FROM s AS s
JOIN LATERAL (
  SELECT t.jn, t.qty
  FROM spj AS t
  WHERE t.sn = s.sn      -- allowed equality correlation
    AND t.qty > 100      -- extra filter is fine
) AS x ON TRUE;"#;
    let expected_text = r#"SELECT "s"."sn", "x"."jn", "x"."qty" FROM "s" AS "s" INNER JOIN
        (SELECT "t"."jn", "t"."qty", "t"."sn" AS "sn" FROM "spj" AS "t"
        WHERE ("t"."qty" > 100)) AS "x" ON ("x"."sn" = "s"."sn");"#;
    test_it("test46", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened.
#[test]
fn test47() {
    let original_text = r#"
SELECT s.sn, x.cnt
FROM s AS s
CROSS JOIN LATERAL (
  SELECT COUNT(*) AS cnt
  FROM spj AS t
  WHERE t.sn = s.sn
) AS x;"#;
    let expected_text = r#"SELECT "s"."sn", coalesce("x"."cnt", 0) FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."sn") AS "x"
        ON ("x"."sn" = "s"."sn");"#;
    test_it("test47", original_text, expected_text);
}

// SELECT-list subquery; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN.
#[test]
fn test48() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT MAX(t.qty)
    FROM spj AS t
    WHERE t.sn = s.sn
    GROUP BY t.sn           -- grouping exactly on local correlated key
  ) AS max_qty_for_supplier
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."max(qty)" AS "max_qty_for_supplier" FROM "s" AS "s"
        LEFT OUTER JOIN (SELECT max("t"."qty") AS "max(qty)", "t"."sn" AS "sn" FROM "spj" AS "t"
        GROUP BY "t"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn");"#;
    test_it("test48", original_text, expected_text);
}

// Test with partial rewrite, as the full rewrite would cause `problematic self-join` issue
// WHERE NOT IN; correlated; LATERAL subquery flattened; rewritten as LEFT OUTER JOIN; anti-semi
// via IS NULL guard; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test49() {
    let original_text = r#"
SELECT
  S.sn,
  S.sname,
  lsp.spj_cnt,
  lmax.max_qty_for_supplier,
  EXISTS (
    SELECT 1
    FROM SPJ sp
    WHERE sp.sn = S.sn
      AND sp.pn IN (SELECT pn FROM P WHERE color = 'RED')
  ) AS supplies_red_parts,
  (S.city IN (SELECT city FROM J WHERE city IS NOT NULL)) AS city_has_job
FROM S
CROSS JOIN LATERAL (
  SELECT COUNT(*) AS spj_cnt
  FROM SPJ sp
  WHERE sp.sn = S.sn
) AS lsp
CROSS JOIN LATERAL (
  SELECT MAX(qty) AS max_qty_for_supplier
  FROM SPJ sp
  WHERE sp.sn = S.sn
) AS lmax
WHERE
  EXISTS (
    SELECT 1
    FROM J
    WHERE J.city = S.city
      AND J.jn IN (
        SELECT sp2.jn
        FROM SPJ sp2
        WHERE sp2.sn = S.sn
          AND sp2.qty > 0
      )
  )
  AND S.status IS NOT NULL
  AND S.status NOT IN (
    SELECT COUNT(*)
    FROM SPJ sp3
    WHERE sp3.sn = S.sn
  );
"#;
    let expected_text = r#"SELECT "s"."sn", "s"."sname", coalesce("lsp"."spj_cnt", 0), "lmax"."max_qty_for_supplier",
        EXISTS (SELECT 1 FROM "spj" AS "sp" WHERE (("sp"."sn" = "s"."sn") AND "sp"."pn" IN
        (SELECT "pn" FROM "p" WHERE ("color" = 'RED')))) AS "supplies_red_parts", "s"."city" IN
        (SELECT "city" FROM "j" WHERE ("city" IS NOT NULL)) AS "city_has_job" FROM "s" LEFT OUTER JOIN
        (SELECT count(*) AS "spj_cnt", "sp"."sn" AS "sn" FROM "spj" AS "sp" GROUP BY "sp"."sn") AS "lsp" ON
        ("lsp"."sn" = "s"."sn") LEFT OUTER JOIN (SELECT max("qty") AS "max_qty_for_supplier", "sp"."sn" AS "sn"
        FROM "spj" AS "sp" GROUP BY "sp"."sn") AS "lmax" ON ("lmax"."sn" = "s"."sn") INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "GNL"."sn" AS "sn", "j"."city" AS "city" FROM "j" INNER JOIN
        (SELECT DISTINCT "sp2"."jn" AS "jn", "sp2"."sn" AS "sn" FROM "spj" AS "sp2" WHERE ("sp2"."qty" > 0)) AS "GNL"
        ON ("j"."jn" = "GNL"."jn")) AS "GNL" ON (("GNL"."city" = "s"."city") AND ("GNL"."sn" = "s"."sn")) LEFT OUTER JOIN
        (SELECT count(*) AS "count(*)", "sp3"."sn" AS "sn" FROM "spj" AS "sp3" GROUP BY "sp3"."sn") AS "GNL1" ON
        ("GNL1"."sn" = "s"."sn") WHERE (("s"."status" IS NOT NULL) AND ("s"."status" != coalesce("GNL1"."count(*)", 0)))"#;
    test_it("test49", original_text, expected_text);
}

// WHERE EXISTS; correlated; with GROUP BY; RHS deduplicated (DISTINCT).
#[test]
fn test50() {
    let original_text = r#"
select spj.qty
FROM spj
where
   exists (
      select 1 from s
      where s.sn = (
         select min(p.sn) from p
         where p.pn = spj.pn and
         exists (
             select 1 from j where j.jn = spj.jn
         )
         group by p.pn, spj.jn
      )
   );"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "GNL"."jn" AS "jn", "GNL"."pn" AS "pn" FROM "s" INNER JOIN
        (SELECT min("p"."sn") AS "min(sn)", "GNL"."jn" AS "jn", "p"."pn" AS "pn" FROM "p" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "j"."jn" AS "jn" FROM "j") AS "GNL"  GROUP BY "p"."pn", "GNL"."jn") AS "GNL"
        ON ("s"."sn" = "GNL"."min(sn)")) AS "GNL" ON (("GNL"."pn" = "spj"."pn") AND ("GNL"."jn" = "spj"."jn"))"#;
    test_it("test50", original_text, expected_text);
}

// No rewrite: bail out due to problematic self-join
#[test]
fn test51() {
    let original_text = r#"
   SELECT t.a, t.b FROM t
   WHERE
      t.a IN (
         SELECT sq.a FROM
            (SELECT t.a, row_number() OVER (ORDER BY t.a) as rn FROM t) sq
         WHERE sq.rn <= 2
     );"#;
    let expected_text = r#"SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a"
        IN (SELECT "sq"."a" FROM (SELECT "t"."a", ROW_NUMBER() OVER(ORDER BY "t"."a" ASC NULLS LAST) AS "rn" FROM "t") AS "sq"
        WHERE ("sq"."rn" <= 2))"#;
    test_it("test51", original_text, expected_text);
}

// WHERE IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; NP/EP
// omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test52() {
    let original_text = r#"
select
   spj.sn
from
   spj
where
   spj.qty in (
      select count(*) over (partition by s.jn) c from s where s.pn = 'P22222' group by s.sn, s.jn
   );"#;
    let expected_text = r#"SELECT "spj"."sn" FROM "spj" INNER JOIN
        (SELECT DISTINCT count(*) OVER(PARTITION BY "s"."jn") AS "c" FROM "s" WHERE ("s"."pn" = 'P22222')
        GROUP BY "s"."sn", "s"."jn") AS "GNL" ON ("spj"."qty" = "GNL"."c")"#;
    test_it("test52", original_text, expected_text);
}

// WHERE IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; NP/EP
// omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test53() {
    let original_text = r#"
select
   spj.sn
from
   spj
where
   spj.qty in (
      select min(s.status) over (partition by s.jn) c from s where s.pn = 'P22222' group by s.sn, s.jn, s.status
   );"#;
    let expected_text = r#"SELECT "spj"."sn" FROM "spj" INNER JOIN
        (SELECT DISTINCT min("s"."status") OVER(PARTITION BY "s"."jn") AS "c" FROM "s" WHERE ("s"."pn" = 'P22222')) AS "GNL"
        ON ("spj"."qty" = "GNL"."c")"#;
    test_it("test53", original_text, expected_text);
}

// WHERE NOT IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT).
#[test]
fn test54() {
    let original_text = r#"
select
   t.rownum
from
   rsdatatypesnull t
where
   t.rs_string not in (
       select s.sn from s where t.rownum = s.status and s.sn is not null
   );"#;
    let expected_text = r#"SELECT "t"."rownum" FROM "rsdatatypesnull" AS "t" LEFT OUTER JOIN
        (SELECT "s"."sn" AS "sn", "s"."status" AS "status" FROM "s" WHERE ("s"."sn" IS NOT NULL)) AS "GNL" ON
        (("t"."rownum" = "GNL"."status") AND ("t"."rs_string" = "GNL"."sn")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "s"."status" AS "status" FROM "s" WHERE ("s"."sn" IS NOT NULL)) AS "EP_3VL" ON
        ("t"."rownum" = "EP_3VL"."status")
        WHERE (("GNL"."sn" IS NULL) AND (("t"."rs_string" IS NOT NULL) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test54", original_text, expected_text);
}

// WHERE NOT IN; correlated; with GROUP BY; NP/EP omitted when null-safe.
#[test]
fn test55() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty not in (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.pn = p.pn
        group by
            p.sn
    );"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT min("p"."weight") AS "min(weight)", "p"."pn" AS "pn" FROM "p" WHERE
        ("p"."city" = 'LONDON') GROUP BY "p"."sn", "p"."pn") AS "GNL" ON
        (("spj"."pn" = "GNL"."pn") AND ("spj"."qty" = "GNL"."min(weight)"))
        WHERE ("GNL"."min(weight)" IS NULL)"#;
    test_it("test55", original_text, expected_text);
}

// WHERE NOT IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT).
#[test]
fn test56() {
    let original_text = r#"
select
   t.rownum
from
   rsdatatypesnull t
where
   t.rs_string not in (
       select s.sn from s where t.rownum = s.status
   );"#;
    let expected_text = r#"SELECT "t"."rownum" FROM "rsdatatypesnull" AS "t" LEFT OUTER JOIN
        (SELECT "s"."sn" AS "sn", "s"."status" AS "status" FROM "s") AS "GNL"
        ON (("t"."rownum" = "GNL"."status") AND ("t"."rs_string" = "GNL"."sn")) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."status" AS "status"
        FROM (SELECT 1 AS "present_", "s"."sn" AS "sn", "s"."status" AS "status" FROM "s") AS "NP_3VL"
        WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL" ON ("t"."rownum" = "NP_3VL"."status") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "s"."status" AS "status" FROM "s") AS "EP_3VL"
        ON ("t"."rownum" = "EP_3VL"."status")
        WHERE (("GNL"."sn" IS NULL) AND ((("t"."rs_string" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL))
        OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test56", original_text, expected_text);
}

// WHERE IN; correlated; with GROUP BY; NP/EP omitted when null-safe.
#[test]
fn test57() {
    let original_text = r#"
select
    spj.qty
from
    spj
where
    spj.qty in (
        select
            count(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.jn = p.jn
        group by
            spj.jn
    );"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT count("p"."weight") AS "count(weight)", "p"."jn" AS "jn" FROM "p" WHERE
        ("p"."city" = 'LONDON') GROUP BY "p"."jn") AS "GNL" ON
        (("spj"."jn" = "GNL"."jn") AND ("spj"."qty" = "GNL"."count(weight)"))"#;
    test_it("test57", original_text, expected_text);
}

// expression NOT IN; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; anti-semi via IS
// NULL guard; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT); boolean SELECT-list
// shaped via CASE for 3-VL.
#[test]
fn test58() {
    let original_text = r#"
select
    case when spj.qty not in (
        select
            min(p.weight)
        from
            p
        where
            p.city = 'LONDON' and spj.sn = p.sn
        group by
            p.pn
    ) then spj.sn else spj.pn end as C
from
    spj;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."min(weight)" IS NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT min("p"."weight") AS "min(weight)", "p"."sn" AS "sn" FROM "p"
         WHERE ("p"."city" = 'LONDON') GROUP BY "p"."pn", "p"."sn") AS "GNL"
         ON (("spj"."sn" = "GNL"."sn") AND ("spj"."qty" = "GNL"."min(weight)"))"#;
    test_it("test58", original_text, expected_text);
}

// expression IN; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; NP/EP omitted when
// null-safe; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test59() {
    let original_text = r#"
select
    case when spj.qty in (
        select
            min(p.weight)
        from
            p join j on p.jn = j.jn and spj.jn = p.jn
        where
            p.city = 'LONDON' and spj.sn = p.sn
        group by
            p.pn
    ) then spj.sn else spj.pn end as C
from
    spj
where
    spj.qty > 100;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."min(weight)" IS NOT NULL) THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT DISTINCT min("p"."weight") AS "min(weight)", "p"."jn" AS "jn", "p"."sn" AS "sn"
        FROM "p" JOIN "j" ON ("p"."jn" = "j"."jn") WHERE ("p"."city" = 'LONDON') GROUP BY "p"."pn", "p"."jn", "p"."sn") AS "GNL"
        ON ((("spj"."sn" = "GNL"."sn") AND ("spj"."jn" = "GNL"."jn")) AND ("spj"."qty" = "GNL"."min(weight)"))
        WHERE ("spj"."qty" > 100)"#;
    test_it("test59", original_text, expected_text);
}

// Expression subquery; correlated; with GROUP BY; rewritten as LEFT OUTER JOIN; boolean SELECT-
// list shaped via CASE for 3-VL.
#[test]
fn test60() {
    let original_text = r#"
select
    case when (
        select
            min(p.weight)
        from
            p join j on p.jn = j.jn and spj.jn = p.jn
        where
            p.city = 'LONDON' and spj.sn = p.sn
        group by
            spj.jn, spj.sn
    ) > 15 then spj.sn else spj.pn end as C
from
    spj
where
    spj.qty > 100;"#;
    let expected = r#"SELECT CASE WHEN (15 < "GNL"."min(weight)") THEN "spj"."sn" ELSE "spj"."pn" END AS "c"
        FROM "spj" LEFT OUTER JOIN (SELECT min("p"."weight") AS "min(weight)", "p"."jn" AS "jn", "p"."sn" AS "sn"
        FROM "p" JOIN "j" ON ("p"."jn" = "j"."jn") WHERE ("p"."city" = 'LONDON') GROUP BY "p"."jn", "p"."sn") AS "GNL"
        ON (("spj"."sn" = "GNL"."sn") AND ("spj"."jn" = "GNL"."jn")) WHERE ("spj"."qty" > 100)"#;
    test_it("test60", original_text, expected);
}

// WHERE NOT IN; correlated; rewritten as INNER JOIN; NP/EP omitted when null-safe.
#[test]
fn test61() {
    let original_text = r#"
SELECT
   s.sname
FROM
   s
WHERE
   s.status NOT IN (
       SELECT
           COUNT(*)
       FROM
           spj
       WHERE
           FALSE
);"#;
    let expected = r#"SELECT "s"."sname" FROM "s" INNER JOIN (SELECT count(*) AS "count(*)"
        FROM "spj" WHERE FALSE) AS "GNL"  WHERE ("s"."status" != "GNL"."count(*)")"#;
    test_it("test61", original_text, expected);
}

// WHERE IN; correlated; rewritten as INNER JOIN; semi-join; NP/EP omitted when null-safe.
#[test]
fn test62() {
    let original_text = r#"
SELECT
   s.sname
FROM
   s
WHERE
   s.status IN (
       SELECT
           COUNT(*)
       FROM
           spj
       WHERE
           FALSE
);"#;
    let expected = r#"SELECT "s"."sname" FROM "s" INNER JOIN (SELECT count(*) AS "count(*)"
        FROM "spj" WHERE FALSE) AS "GNL"  ON ("s"."status" = "GNL"."count(*)")"#;
    test_it("test62", original_text, expected);
}

// Not rewritable test: expression in left outer join condition + `problematic self-join`
// WHERE NOT IN; correlated; NP/EP omitted when null-safe; boolean SELECT-list shaped via CASE
// for 3-VL.
#[test]
fn test63() {
    let original_text = r#"
select
   s.sname
from
   s
where
   case when s.pn = 'P11111'
        then NULL
        else s.pn
   end not in (
      select
         s1.pn
      from
         s s1
      where
         s.sn = s1.sn and s.pn = s1.jn
   );"#;
    let expected = r#"SELECT "s"."sname" FROM "s" WHERE CASE WHEN ("s"."pn" = 'P11111') THEN NULL ELSE "s"."pn" END
        NOT IN (SELECT "s1"."pn" FROM "s" AS "s1" WHERE (("s"."sn" = "s1"."sn") AND ("s"."pn" = "s1"."jn")));"#;
    test_it("test63", original_text, expected);
}

// SELECT-list IN: both sides provably non-NULL
//    - RHS non-NULL due to *local* predicate that remains in RHS after correlation stripping
//    - LHS non-NULL due to base schema (s.sn NOT NULL) in the outer stmt
//    Expectation: we *don’t* emit a 3VL CASE guard; we simplify to `<rhs> IS NOT NULL`
//                 and we still see a LEFT OUTER JOIN for the probe.
#[test]
fn test64() {
    // Make s.sn NULL-free
    let mut schema_guard = get_schema_guard();
    make_col_null_free("s", "sn", &mut schema_guard);

    let q = r#"
        SELECT
            s.sn,
            s.sn IN (
                SELECT spj.sn
                FROM spj
                WHERE spj.sn = s.sn       -- correlation (moved to JOIN ON)
                  AND spj.sn IS NOT NULL  -- local to RHS; ensures RHS first field null-free
            ) AS in_flag
        FROM s
    "#;

    let out = rewrite("test64", q, schema_guard);

    // Should have introduced a LEFT JOIN LATERAL derived table
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN, got:\n{out}"
    );

    // Because both sides are null-free, SELECT-list rewrite should be a simple IS NOT NULL check,
    // not a CASE 3VL guard.
    assert!(
        !contains_case(&out),
        "should NOT produce CASE guard when both sides are non-null:\n{out}"
    );
    assert!(
        out.contains(" IS NOT NULL"),
        "expected `... IS NOT NULL` simplification in:\n{out}"
    );
}

// SELECT-list IN: RHS non-NULL, LHS potentially NULL
//    - RHS non-NULL as above (local predicate remains in RHS after stripping)
//    - LHS nullable because s.sn is nullable in schema
//    Expectation: we DO emit a 3VL CASE guard in the SELECT-list (since LHS may be NULL).
#[test]
fn test65() {
    // Make s.sn NULL-able
    let mut schema_guard = get_schema_guard();
    make_col_nullable("s", "sn", &mut schema_guard);

    let q = r#"
        SELECT
            s.sn,
            s.sn IN (
                SELECT spj.sn
                FROM spj
                WHERE spj.sn = s.sn
                  AND spj.sn IS NOT NULL
            ) AS in_flag
        FROM s
    "#;

    let out = rewrite("test65", q, schema_guard);

    // We should still have a LEFT JOIN LATERAL for the probe.
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN, got:\n{out}"
    );

    // LHS may be NULL -> we must preserve 3VL in SELECT-list via a CASE guard.
    assert!(
        contains_case(&out),
        "expected 3VL CASE guard in SELECT-list when LHS is nullable:\n{out}"
    );
}

// WHERE NOT IN: LHS null-free -> skip EP, keep NP (since RHS may contain NULLs)
#[test]
fn test66() {
    // Mark LHS column t.rs_string as NOT NULL in the schema
    let mut schema_guard = get_schema_guard();
    make_col_null_free("rsdatatypesnull", "rs_string", &mut schema_guard);

    let q = r#"
      SELECT
         t.rownum
      FROM
         rsdatatypesnull t
      WHERE
         t.rs_string NOT IN (
             SELECT s.sn
             FROM s
             WHERE t.rownum = s.status
         );
      "#;

    let out = rewrite("test66", q, schema_guard);

    // We expect a LEFT OUTER JOIN for the anti-join
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN anti-join; got:\n{out}"
    );

    // With LHS provably NON-NULL, EP should be skipped.
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be skipped when LHS is null-free; got:\n{out}"
    );

    // RHS may have NULLs (no RHS-local non-null guarantee), so NP should still be present.
    assert!(
        contains_np_3vl(&out),
        "NP_3VL (null-presence probe) should be present; got:\n{out}"
    );
}

// WHERE NOT IN: LHS null-free and RHS proven null-free -> skip both EP and NP
#[test]
fn test67() {
    // Ensure LHS is NON-NULL
    let mut schema_guard = get_schema_guard();
    make_col_null_free("rsdatatypesnull", "rs_string", &mut schema_guard);

    let q = r#"
      SELECT
         t.rownum
      FROM
         rsdatatypesnull t
      WHERE
         t.rs_string NOT IN (
             SELECT s.sn
             FROM s
             WHERE t.rownum = s.status
               AND s.sn IS NOT NULL    -- RHS-local: guarantees first item is non-NULL
         );
      "#;

    let out = rewrite("test67", q, schema_guard);

    // Expect anti-join shape
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN anti-join; got:\n{out}"
    );

    // No EP or NP should appear when both sides are non-NULL.
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be skipped when LHS is null-free; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be skipped when RHS is proven null-free; got:\n{out}"
    );
}

// WHERE NOT IN: LHS nullable but RHS proven null-free -> EP must remain, NP can be skipped
#[test]
fn test68() {
    // Make sure LHS is nullable by removing the schema non-null assumption.
    let mut schema_guard = get_schema_guard();
    make_col_nullable("rsdatatypesnull", "rs_string", &mut schema_guard);

    // (Do not add any non-null for t.rs_string)

    let q = r#"
      SELECT
         t.rownum
      FROM
         rsdatatypesnull t
      WHERE
         t.rs_string NOT IN (
             SELECT s.sn
             FROM s
             WHERE t.rownum = s.status
               AND s.sn IS NOT NULL    -- RHS-local: guarantees first item is non-NULL
         );
      "#;

    let out = rewrite("test68", q, schema_guard);

    // Expect anti-join shape
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN anti-join; got:\n{out}"
    );

    // Because LHS is nullable, EP must remain to allow TRUE when RHS is empty and LHS is NULL.
    assert!(
        contains_ep_3vl(&out),
        "EP_3VL must be present when LHS is nullable; got:\n{out}"
    );

    // RHS is proven null-free, so NP should not be present.
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted when RHS is proven null-free; got:\n{out}"
    );
}

// Correlated IN with COUNT(*) OVER() on filtered set (tests equality-to-constant via window)
#[test]
fn test69() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE 2 IN (
  SELECT COUNT(*) OVER ()
  FROM spj AS t
  WHERE t.sn = s.sn
);"#;
    let expected = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT count(*) OVER(PARTITION BY "t"."sn") AS "count(*) OVER()", "t"."sn" AS "sn" FROM "spj" AS "t") AS "GNL"
        ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."count(*) OVER()" = 2)"#;
    test_it("test69", original_text, expected);
}

// Correlated scalar subquery in SELECT list; inner uses window, outer aggregates to 1 row
#[test]
fn test70() {
    let original_text = r#"
SELECT
  s.sn,
  (SELECT MAX(x)
     FROM (
       SELECT MAX(t.qty) OVER (PARTITION BY t.pn) AS x
       FROM spj AS t
       WHERE t.sn = s.sn
     ) AS u
  ) AS mx_by_part
FROM
  s AS s;"#;
    let expected = r#"SELECT "s"."sn", "GNL"."max(x)" AS "mx_by_part" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT max("x") AS "max(x)", "u"."sn" AS "sn" FROM (SELECT max("t"."qty") OVER(PARTITION BY "t"."pn", "t"."sn") AS "x",
        "t"."sn" AS "sn" FROM "spj" AS "t") AS "u" GROUP BY "u"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test70", original_text, expected);
}

// Correlated EXISTS; inner derived table computes DENSE_RANK() and filters on it
#[test]
fn test71() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT t.jn,
           DENSE_RANK() OVER (PARTITION BY t.sn ORDER BY t.qty DESC) AS dr
    FROM spj AS t
    WHERE t.sn = s.sn
  ) AS r
  WHERE r.dr = 1
);"#;
    let expected = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "r"."sn" AS "sn" FROM
        (SELECT "t"."jn", DENSE_RANK() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "dr", "t"."sn" AS "sn"
        FROM "spj" AS "t") AS "r" WHERE ("r"."dr" = 1)) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test71", original_text, expected);
}

// Correlated scalar predicate via MAX over COUNT(*) OVER() (aggregate wrapper ensures scalar)
#[test]
fn test72() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE 3 = (
  SELECT MAX(cnt)
  FROM (
    SELECT COUNT(*) OVER () AS cnt
    FROM spj AS t
    WHERE t.pn = p.pn
  ) AS w
);"#;
    let expected = r#"SELECT "p"."pn" FROM "p" AS "p" INNER JOIN
        (SELECT max("cnt") AS "max(cnt)", "w"."pn" AS "pn" FROM
        (SELECT count(*) OVER(PARTITION BY "t"."pn") AS "cnt", "t"."pn" AS "pn" FROM "spj" AS "t") AS "w" GROUP BY "w"."pn") AS "GNL"
        ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."max(cnt)" = 3)"#;
    test_it("test72", original_text, expected);
}

// Non-correlated `IN` in SELECT list using RANK() window; tests non-correlated predicate path
#[test]
fn test73() {
    let original_text = r#"
SELECT
  j.jn,
  (j.jn IN (
     SELECT u.jn
     FROM (
       SELECT t.jn, RANK() OVER (PARTITION BY t.sn ORDER BY t.qty DESC) AS rk
       FROM spj AS t
     ) AS u
     WHERE u.rk = 1
  )) AS top_for_some_supplier
FROM j AS j;"#;
    let expected = r#"SELECT "j"."jn", CASE WHEN ("GNL"."jn" IS NOT NULL) THEN TRUE
        WHEN (("j"."jn" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "top_for_some_supplier"
        FROM "j" AS "j" LEFT OUTER JOIN (SELECT DISTINCT "u"."jn" AS "jn" FROM
        (SELECT "t"."jn", RANK() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "rk" FROM "spj" AS "t") AS "u"
        WHERE ("u"."rk" = 1)) AS "GNL" ON ("j"."jn" = "GNL"."jn") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM (SELECT "t"."jn", RANK() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "rk"
         FROM "spj" AS "t") AS "u" WHERE ("u"."rk" = 1)) AS "EP_3VL"  LEFT OUTER JOIN
         (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT 1 AS "present_", "u"."jn" AS "jn" FROM
         (SELECT "t"."jn", RANK() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "rk" FROM "spj" AS "t") AS "u"
         WHERE ("u"."rk" = 1)) AS "NP_3VL" WHERE ("NP_3VL"."jn" IS NULL)) AS "NP_3VL""#;
    test_it("test73", original_text, expected);
}

// Mixed: correlated scalar on LHS vs non-correlated scalar on RHS; both sides use windows
#[test]
fn test74() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE (
  SELECT MAX(m)
  FROM (
    SELECT MAX(t.qty) OVER (PARTITION BY t.pn) AS m
    FROM spj AS t
    WHERE t.sn = s.sn
  ) AS q
) > (
  SELECT MAX(mm)
  FROM (
    SELECT MAX(t2.qty) OVER () AS mm
    FROM spj AS t2
  ) AS q2
);"#;
    let expected = r#"SELECT "s"."sn" FROM "s" AS "s" WHERE
        ((SELECT max("m") FROM (SELECT max("t"."qty") OVER(PARTITION BY "t"."pn") AS "m" FROM "spj" AS "t"
        WHERE ("t"."sn" = "s"."sn")) AS "q") >
        (SELECT max("mm") FROM (SELECT max("t2"."qty") OVER() AS "mm" FROM "spj" AS "t2") AS "q2"))"#;
    test_it("test74", original_text, expected);
}

// Correlated scalar in SELECT list using MIN window wrapped by MIN aggregate
#[test]
fn test75() {
    let original_text = r#"
SELECT
  sp.sn,
  (SELECT MIN(mm)
     FROM (
       SELECT MIN(t.weight) OVER (PARTITION BY t.pn) AS mm
       FROM p AS t
       WHERE t.sn = sp.sn
     ) AS z
  ) AS min_win
FROM spj AS sp;"#;
    let expected = r#"SELECT "sp"."sn", "GNL"."min(mm)" AS "min_win" FROM "spj" AS "sp" LEFT OUTER JOIN
        (SELECT min("mm") AS "min(mm)", "z"."sn" AS "sn" FROM
        (SELECT min("t"."weight") OVER(PARTITION BY "t"."pn", "t"."sn") AS "mm", "t"."sn" AS "sn" FROM "p" AS "t") AS "z"
        GROUP BY "z"."sn") AS "GNL" ON ("GNL"."sn" = "sp"."sn")"#;
    test_it("test75", original_text, expected);
}

// Non-correlated EXISTS with aggregate-only window (COUNT(*) OVER()); tests trivial true path
#[test]
fn test76() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT COUNT(*) OVER () AS c
    FROM j
  ) AS x
  WHERE x.c > 0
);"#;
    let expected = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM (SELECT count(*) OVER() AS "c" FROM "j") AS "x" WHERE ("x"."c" > 0)) AS "GNL""#;
    test_it("test76", original_text, expected);
}

// Correlated NOT EXISTS with DENSE_RANK() over jn; tests negative window predicate
#[test]
fn test77() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT t.pn,
           DENSE_RANK() OVER (PARTITION BY t.jn ORDER BY t.qty DESC) AS r
    FROM spj AS t
    WHERE t.pn = p.pn
  ) AS y
  WHERE y.r > 5
);"#;
    let expected = r#"SELECT "p"."pn" FROM "p" AS "p" LEFT OUTER JOIN
        (SELECT 1 AS "present_", "y"."pn" AS "pn" FROM (SELECT "t"."pn", DENSE_RANK() OVER(PARTITION BY "t"."jn", "t"."pn"
        ORDER BY "t"."qty" DESC NULLS FIRST) AS "r" FROM "spj" AS "t") AS "y" WHERE ("y"."r" > 5)) AS "GNL"
        ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test77", original_text, expected);
}

// Both SELECT-list boolean (IN with window) and correlated numeric WHERE using COUNT(*) OVER()
#[test]
fn test78() {
    let original_text = r#"
SELECT
  s.sn,
  (s.sn IN (
     SELECT v.sn
     FROM (
       SELECT t.sn, MAX(t.qty) OVER (PARTITION BY t.pn) AS m
       FROM spj AS t
     ) AS v
     WHERE v.m >= 100
  )) AS has_big_part
FROM s AS s
WHERE 2 <= (
  SELECT MAX(c)
  FROM (
    SELECT COUNT(*) OVER (PARTITION BY t.sn) AS c
    FROM spj AS t
    WHERE t.sn = s.sn
  ) AS w
);"#;
    let expected = r#"SELECT "s"."sn", CASE WHEN ("GNL1"."sn" IS NOT NULL) THEN TRUE
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "has_big_part"
        FROM "s" AS "s" INNER JOIN (SELECT max("c") AS "max(c)", "w"."sn" AS "sn"
        FROM (SELECT count(*) OVER(PARTITION BY "t"."sn") AS "c", "t"."sn" AS "sn" FROM "spj" AS "t") AS "w" GROUP BY "w"."sn") AS "GNL"
        ON ("GNL"."sn" = "s"."sn") LEFT OUTER JOIN (SELECT DISTINCT "v"."sn" AS "sn"
        FROM (SELECT "t"."sn", max("t"."qty") OVER(PARTITION BY "t"."pn") AS "m" FROM "spj" AS "t") AS "v"
        WHERE ("v"."m" >= 100)) AS "GNL1" ON ("s"."sn" = "GNL1"."sn") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT 1 AS "present_", "v"."sn" AS "sn" FROM
        (SELECT "t"."sn", max("t"."qty") OVER(PARTITION BY "t"."pn") AS "m" FROM "spj" AS "t") AS "v"
        WHERE ("v"."m" >= 100)) AS "NP_3VL" WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL"
        WHERE ("GNL"."max(c)" >= 2)"#;
    test_it("test78", original_text, expected);
}

// EXISTS with WF over grouped rows: SUM(...) grouped, then MAX(SUM(...)) OVER (PARTITION BY ...)
#[test]
fn test79() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT
      q.sn,
      q.jn,
      q.sm,
      MAX(q.sm) OVER (PARTITION BY q.sn) AS sm_pmax
    FROM (
      SELECT t.sn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.jn
    ) AS q
  ) AS u
  WHERE u.sm = u.sm_pmax
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "u"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."jn", "q"."sm", max("q"."sm") OVER(PARTITION BY "q"."sn") AS "sm_pmax" FROM
        (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "u"
        WHERE ("u"."sm" = "u"."sm_pmax")) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test79", original_text, expected_text);
}

// SELECT-list scalar: AVG of the top-3 grouped SUM(...) per supplier using ROW_NUMBER over grouped rows
#[test]
fn test80() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT AVG(z.sm)
    FROM (
      SELECT
        q.sn,
        q.sm,
        ROW_NUMBER() OVER (PARTITION BY q.sn ORDER BY q.jn ASC) AS rn
      FROM (
        SELECT t.sn, t.jn, SUM(t.qty) AS sm
        FROM spj AS t
        WHERE t.sn = s.sn
        GROUP BY t.sn, t.jn
      ) AS q
    ) AS z
    WHERE z.rn <= 3
  ) AS avg_top3_sum
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."avg(sm)" AS "avg_top3_sum" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT avg("z"."sm") AS "avg(sm)", "z"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."sm", ROW_NUMBER() OVER(PARTITION BY "q"."sn" ORDER BY "q"."jn" ASC NULLS LAST) AS "rn"
        FROM (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "z"
        WHERE ("z"."rn" <= 3) GROUP BY "z"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test80", original_text, expected_text);
}

// WHERE scalar >=: take MAX(sm) among top-2 (by sm) per part; WF over aggregated rows via two-level derived
#[test]
fn test81() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE (
  SELECT MAX(w.sm)
  FROM (
    SELECT
      q.pn,
      q.jn,
      q.sm,
      DENSE_RANK() OVER (PARTITION BY q.pn ORDER BY q.sm DESC) AS rk
    FROM (
      SELECT t.pn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.pn = p.pn
      GROUP BY t.pn, t.jn
    ) AS q
  ) AS w
  WHERE w.pn = p.pn AND w.rk <= 2
) >= 100;"#;
    let expected_text = r#"SELECT "p"."pn" FROM "p" AS "p" INNER JOIN
        (SELECT max("w"."sm") AS "max(sm)", "w"."pn" AS "pn" FROM
        (SELECT "q"."pn", "q"."jn", "q"."sm", DENSE_RANK() OVER(PARTITION BY "q"."pn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rk"
        FROM (SELECT "t"."pn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."pn", "t"."jn") AS "q") AS "w"
        WHERE ("w"."rk" <= 2) GROUP BY "w"."pn") AS "GNL" ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."max(sm)" >= 100)"#;
    test_it("test81", original_text, expected_text);
}

// EXISTS with comparison between grouped SUM and partition AVG over those grouped sums
#[test]
fn test82() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT
      q.sn,
      q.pn,
      q.sm,
      AVG(q.sm) OVER (PARTITION BY q.sn) AS avg_sm
    FROM (
      SELECT t.sn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.pn
    ) AS q
  ) AS w
  WHERE w.sm > w.avg_sm
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "w"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."pn", "q"."sm", avg("q"."sm") OVER(PARTITION BY "q"."sn") AS "avg_sm" FROM
        (SELECT "t"."sn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."pn") AS "q") AS "w"
        WHERE ("w"."sm" > "w"."avg_sm")) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test82", original_text, expected_text);
}

// SELECT-list scalar over grouped rows with WF: MIN of DENSE_RANK per job
#[test]
fn test83() {
    let original_text = r#"
SELECT
  j.jn,
  (
    SELECT MIN(r.dr)
    FROM (
      SELECT
        q.jn,
        q.pn,
        q.sm,
        DENSE_RANK() OVER (PARTITION BY q.jn ORDER BY q.sm DESC) AS dr
      FROM (
        SELECT t.jn, t.pn, SUM(t.qty) AS sm
        FROM spj AS t
        WHERE t.jn = j.jn
        GROUP BY t.jn, t.pn
      ) AS q
    ) AS r
  ) AS min_rank
FROM j AS j;"#;
    let expected_text = r#"SELECT "j"."jn", "GNL"."min(dr)" AS "min_rank" FROM "j" AS "j" LEFT OUTER JOIN
        (SELECT min("r"."dr") AS "min(dr)", "r"."jn" AS "jn" FROM
        (SELECT "q"."jn", "q"."pn", "q"."sm", DENSE_RANK() OVER(PARTITION BY "q"."jn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "dr"
        FROM (SELECT "t"."jn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."pn") AS "q") AS "r"
        GROUP BY "r"."jn") AS "GNL" ON ("GNL"."jn" = "j"."jn")"#;
    test_it("test83", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test84() {
    let original_text = r#"
SELECT
  s.sn,
  x.avg_top2_sm
FROM s AS s
CROSS JOIN LATERAL (
  SELECT AVG(z.sm) AS avg_top2_sm
  FROM (
    SELECT
      q.sn,
      q.sm,
      ROW_NUMBER() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rn
    FROM (
      SELECT t.sn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.pn
    ) AS q
  ) AS z
  WHERE z.rn <= 2
  GROUP BY z.sn
) AS x;"#;
    let expected_text = r#"SELECT "s"."sn", "x"."avg_top2_sm" FROM "s" AS "s" INNER JOIN
        (SELECT avg("z"."sm") AS "avg_top2_sm", "z"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."sm", ROW_NUMBER() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rn"
        FROM (SELECT "t"."sn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."pn") AS "q") AS "z"
        WHERE ("z"."rn" <= 2) GROUP BY "z"."sn") AS "x" ON ("x"."sn" = "s"."sn")"#;
    test_it("test84", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test85() {
    let original_text = r#"
SELECT
  s.sn,
  x.has_top_job
FROM s AS s
JOIN LATERAL (
  SELECT 1 AS has_top_job
  FROM (
    SELECT
      q.sn,
      q.jn,
      q.sm,
      RANK() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rk
    FROM (
      SELECT t.sn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.jn
    ) AS q
  ) AS r
  WHERE r.rk = 1
  GROUP BY r.sn
) AS x ON TRUE;
"#;
    let expected_text = r#"SELECT "s"."sn", "x"."has_top_job" FROM "s" AS "s" INNER JOIN
        (SELECT 1 AS "has_top_job", "r"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."jn", "q"."sm", RANK() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rk"
        FROM (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "r"
        WHERE ("r"."rk" = 1) GROUP BY "r"."sn") AS "x" ON ("x"."sn" = "s"."sn")"#;
    test_it("test85", original_text, expected_text);
}

// SELECT-list subquery; non-correlated; LATERAL subquery flattened; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test86() {
    let original_text = r#"
SELECT
  s.sn,
  x.max_of_jobmax
FROM s AS s
CROSS JOIN LATERAL (
  SELECT MAX(u.sm_pmax) AS max_of_jobmax
  FROM (
    SELECT
      q.sn,
      q.jn,
      q.sm,
      MAX(q.sm) OVER (PARTITION BY q.sn) AS sm_pmax
    FROM (
      SELECT t.sn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.jn
    ) AS q
  ) AS u
  GROUP BY u.sn
) AS x;
"#;
    let expected_text = r#"SELECT "s"."sn", "x"."max_of_jobmax" FROM "s" AS "s" INNER JOIN
        (SELECT max("u"."sm_pmax") AS "max_of_jobmax", "u"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."jn", "q"."sm", max("q"."sm") OVER(PARTITION BY "q"."sn") AS "sm_pmax" FROM
        (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "u"
        GROUP BY "u"."sn") AS "x" ON ("x"."sn" = "s"."sn")"#;
    test_it("test86", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test87() {
    let original_text = r#"
SELECT
  s.sn,
  x.max_rn
FROM s AS s
LEFT JOIN LATERAL (
  SELECT MAX(z.rn) AS max_rn
  FROM (
    SELECT
      q.sn,
      q.sm,
      ROW_NUMBER() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rn
    FROM (
      SELECT t.sn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.pn
    ) AS q
  ) AS z
  WHERE z.rn <= 3
  GROUP BY z.sn
) AS x ON TRUE;
"#;
    let expected_text = r#"SELECT "s"."sn", "x"."max_rn" FROM "s" AS "s" LEFT JOIN
        (SELECT max("z"."rn") AS "max_rn", "z"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."sm", ROW_NUMBER() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rn"
        FROM (SELECT "t"."sn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."pn") AS "q") AS "z"
        WHERE ("z"."rn" <= 3) GROUP BY "z"."sn") AS "x" ON ("x"."sn" = "s"."sn")"#;
    test_it("test87", original_text, expected_text);
}

// SELECT-list subquery; correlated; LATERAL subquery flattened; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test88() {
    let original_text = r#"
SELECT
  j.jn,
  x.min_rank
FROM j AS j
JOIN LATERAL (
  SELECT MIN(r.dr) AS min_rank
  FROM (
    SELECT
      q.jn,
      q.pn,
      q.sm,
      DENSE_RANK() OVER (PARTITION BY q.jn ORDER BY q.sm DESC) AS dr
    FROM (
      SELECT t.jn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.jn = j.jn
      GROUP BY t.jn, t.pn
    ) AS q
  ) AS r
  GROUP BY r.jn
) AS x ON TRUE;
"#;
    let expected_text = r#"SELECT "j"."jn", "x"."min_rank" FROM "j" AS "j" INNER JOIN
        (SELECT min("r"."dr") AS "min_rank", "r"."jn" AS "jn" FROM
        (SELECT "q"."jn", "q"."pn", "q"."sm", DENSE_RANK() OVER(PARTITION BY "q"."jn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "dr"
        FROM (SELECT "t"."jn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."pn") AS "q") AS "r"
        GROUP BY "r"."jn") AS "x" ON ("x"."jn" = "j"."jn")"#;
    test_it("test88", original_text, expected_text);
}

// WHERE EXISTS; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; RHS
// deduplicated (DISTINCT).
#[test]
fn test89() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT
      q.sn,
      q.pn,
      q.sm,
      RANK() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rk
    FROM (
      SELECT t.sn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.pn
    ) AS q
  ) AS r
  WHERE r.rk = 1
);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "r"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."pn", "q"."sm", RANK() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rk"
        FROM (SELECT "t"."sn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."pn") AS "q") AS "r"
        WHERE ("r"."rk" = 1)) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test89", original_text, expected_text);
}

// WHERE IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; rewritten
// as INNER JOIN; semi-join; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test90() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE s.sn IN (
  SELECT v.sn
  FROM (
    SELECT
      q.sn,
      q.sm,
      ROW_NUMBER() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rn
    FROM (
      SELECT t.sn, SUM(t.qty) AS sm
      FROM spj AS t
      GROUP BY t.sn
    ) AS q
  ) AS v
  WHERE v.rn = 1
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT DISTINCT "v"."sn" AS "sn" FROM
        (SELECT "q"."sn", "q"."sm", ROW_NUMBER() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rn"
        FROM (SELECT "t"."sn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn") AS "q") AS "v"
        WHERE ("v"."rn" = 1)) AS "GNL" ON ("s"."sn" = "GNL"."sn")"#;
    test_it("test90", original_text, expected_text);
}

// SELECT-list, correlated: AVG over top-3 job sums per supplier
#[test]
fn test91() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT AVG(z.sm)
    FROM (
      SELECT SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.jn
      ORDER BY sm DESC
      LIMIT 3
    ) AS z
  ) AS avg_top3_job_sums
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."avg(sm)" AS "avg_top3_job_sums" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT avg("z"."sm") AS "avg(sm)", "z"."sn" AS "sn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM
        (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn", ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT sum("t"."qty") AS "sm", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "z" GROUP BY "z"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test91", original_text, expected_text);
}

// WHERE scalar; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test92() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE 2 <= (
  SELECT MAX(c)
  FROM (
    SELECT
      q.jn,
      COUNT(*) OVER (PARTITION BY q.jn) AS c
    FROM (
      SELECT t.sn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.sn, t.jn
    ) AS q
  ) AS w
);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT max("c") AS "max(c)", "w"."sn" AS "sn" FROM
        (SELECT "q"."jn", count(*) OVER(PARTITION BY "q"."jn", "q"."sn") AS "c", "q"."sn" AS "sn" FROM
        (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "w"
        GROUP BY "w"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."max(c)" >= 2)"#;
    test_it("test92", original_text, expected_text);
}

// WHERE scalar; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test93() {
    let original_text = r#"
SELECT j.jn
FROM j AS j
WHERE 1 = (
  SELECT MIN(dr)
  FROM (
    SELECT
      q.jn,
      q.pn,
      q.sm,
      DENSE_RANK() OVER (PARTITION BY q.jn ORDER BY q.sm DESC) AS dr
    FROM (
      SELECT t.jn, t.pn, SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.jn = j.jn
      GROUP BY t.jn, t.pn
    ) AS q
  ) AS r
);
"#;
    let expected_text = r#"SELECT "j"."jn" FROM "j" AS "j" INNER JOIN
        (SELECT min("dr") AS "min(dr)", "r"."jn" AS "jn" FROM
        (SELECT "q"."jn", "q"."pn", "q"."sm", DENSE_RANK() OVER(PARTITION BY "q"."jn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "dr"
        FROM (SELECT "t"."jn", "t"."pn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."pn") AS "q") AS "r"
        GROUP BY "r"."jn") AS "GNL" ON ("GNL"."jn" = "j"."jn") WHERE ("GNL"."min(dr)" = 1)"#;
    test_it("test93", original_text, expected_text);
}

// WHERE IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; NP/EP
// omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test94() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE p.pn IN (
  SELECT y.pn
  FROM (
    SELECT
      q.pn,
      q.sm,
      MAX(q.sm) OVER (PARTITION BY q.pn) AS mx
    FROM (
      SELECT t.pn, t.jn, SUM(t.qty) AS sm
      FROM spj AS t
      GROUP BY t.pn, t.jn
    ) AS q
  ) AS y
  WHERE y.mx >= 100
);
"#;
    let expected_text = r#"SELECT "p"."pn" FROM "p" AS "p" INNER JOIN
        (SELECT DISTINCT "y"."pn" AS "pn" FROM (SELECT "q"."pn", "q"."sm", max("q"."sm") OVER(PARTITION BY "q"."pn") AS "mx"
        FROM (SELECT "t"."pn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."pn", "t"."jn") AS "q") AS "y"
        WHERE ("y"."mx" >= 100)) AS "GNL" ON ("p"."pn" = "GNL"."pn")"#;
    test_it("test94", original_text, expected_text);
}

// WHERE NOT IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; NP/EP omitted when
// null-safe.
#[test]
fn test95() {
    let original_text = r#"
SELECT
    DataTypes.RowNum
FROM
    DataTypes
WHERE
    DataTypes.RowNum NOT IN (
        SELECT
            AVG(DataTypes1.RowNum) OVER () avg_w
        FROM
            DataTypes1
        WHERE
            DataTypes1.RowNum = DataTypes.RowNum
    );"#;
    let expected_text = r#"SELECT "datatypes"."rownum" FROM "datatypes" LEFT OUTER JOIN
        (SELECT avg("datatypes1"."rownum") OVER(PARTITION BY "datatypes1"."rownum") AS "avg_w",
        "datatypes1"."rownum" AS "rownum" FROM "datatypes1") AS "GNL" ON
        (("GNL"."rownum" = "datatypes"."rownum") AND ("datatypes"."rownum" = "GNL"."avg_w"))
        WHERE ("GNL"."avg_w" IS NULL)"#;
    test_it("test95", original_text, expected_text);
}

// WHERE NOT IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY; NP/EP
// omitted when null-safe.
#[test]
fn test96() {
    let original_text = r#"
SELECT
    DataTypes.RowNum
FROM
    DataTypes
WHERE
    DataTypes.RowNum NOT IN (
        SELECT
            AVG(DataTypes1.RowNum) OVER () avg_w
        FROM
            DataTypes1
        WHERE
            DataTypes1.RowNum = DataTypes.RowNum
        GROUP BY
            DataTypes1.test_int, DataTypes1.RowNum
    );"#;
    let expected_text = r#"SELECT "datatypes"."rownum" FROM "datatypes" LEFT OUTER JOIN
        (SELECT avg("datatypes1"."rownum") OVER(PARTITION BY "datatypes1"."rownum") AS "avg_w", "datatypes1"."rownum" AS "rownum"
        FROM "datatypes1" GROUP BY "datatypes1"."test_int", "datatypes1"."rownum") AS "GNL" ON
        (("GNL"."rownum" = "datatypes"."rownum") AND ("datatypes"."rownum" = "GNL"."avg_w"))
        WHERE ("GNL"."avg_w" IS NULL)"#;
    test_it("test96", original_text, expected_text);
}

// expression NOT IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP BY;
// NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test97() {
    let original_text = r#"
SELECT
    DataTypes.RowNum,
    DataTypes.RowNum NOT IN (
        SELECT
            AVG(DataTypes1.RowNum) OVER () avg_w
        FROM
            DataTypes1
        WHERE
            DataTypes1.RowNum = DataTypes.RowNum
        GROUP BY
            DataTypes1.test_int, DataTypes1.RowNum
    )
FROM
    DataTypes
WHERE
    DataTypes.RowNum = 10;"#;
    let expected_text = r#"SELECT "datatypes"."rownum", ("GNL"."avg_w" IS NULL) FROM "datatypes" LEFT OUTER JOIN
        (SELECT DISTINCT avg("datatypes1"."rownum") OVER(PARTITION BY "datatypes1"."rownum") AS "avg_w", "datatypes1"."rownum" AS "rownum"
        FROM "datatypes1" GROUP BY "datatypes1"."test_int", "datatypes1"."rownum") AS "GNL" ON
         (("GNL"."rownum" = "datatypes"."rownum") AND ("datatypes"."rownum" = "GNL"."avg_w"))
         WHERE ("datatypes"."rownum" = 10)"#;
    test_it("test97", original_text, expected_text);
}

// SELECT-list subquery; correlated; materialized via window ROW_NUMBER/DENSE_RANK; with GROUP
// BY.
#[test]
fn test98() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT AVG(z.sm)
    FROM (
      SELECT q.sn, q.jn, q.sm,
             ROW_NUMBER() OVER (PARTITION BY q.sn ORDER BY q.sm DESC) AS rn
      FROM (
        SELECT t.sn, t.jn, SUM(t.qty) AS sm
        FROM spj AS t
        WHERE t.sn = s.sn
        GROUP BY t.sn, t.jn
      ) AS q
    ) AS z
    WHERE z.rn <= 3
  ) AS avg_top3_job_sums
FROM s AS s;"#;

    let expected_text = r#"SELECT "s"."sn", "GNL"."avg(sm)" AS "avg_top3_job_sums" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT avg("z"."sm") AS "avg(sm)", "z"."sn" AS "sn" FROM (SELECT "q"."sn", "q"."jn", "q"."sm",
        ROW_NUMBER() OVER(PARTITION BY "q"."sn" ORDER BY "q"."sm" DESC NULLS FIRST) AS "rn" FROM
        (SELECT "t"."sn", "t"."jn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn", "t"."jn") AS "q") AS "z"
        WHERE ("z"."rn" <= 3) GROUP BY "z"."sn") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test98", original_text, expected_text);
}

// WHERE EXISTS; correlated; with ORDER BY/LIMIT; materialized via window ROW_NUMBER/DENSE_RANK;
// RHS deduplicated (DISTINCT).
#[test]
fn test99() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT t.jn
    FROM spj AS t
    WHERE t.sn = s.sn
    ORDER BY t.qty DESC
    LIMIT 1
  ) AS r
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "r"."sn" AS "sn" FROM (SELECT "INNER"."jn" AS "jn", "INNER"."sn" AS "sn" FROM (
        SELECT "t"."jn" AS "jn", "t"."sn" AS "sn", ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn"
        FROM "spj" AS "t") AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "r") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test99", original_text, expected_text);
}

// WHERE subquery; correlated; with ORDER BY/LIMIT; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY.
#[test]
fn test100() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT AVG(z.sm)
    FROM (
      SELECT SUM(t.qty) AS sm
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.jn
      ORDER BY sm DESC
      LIMIT 3
    ) AS z
    join (
      SELECT SUM(t.weight) AS sm
      FROM p AS t
      WHERE t.sn = s.sn
      GROUP BY t.jn
      ORDER BY sm DESC
      LIMIT 5
    ) as y
    on
      z.sm = y.sm
  ) AS avg_top3_job_sums
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."avg(sm)" AS "avg_top3_job_sums" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT avg("z"."sm") AS "avg(sm)", "y"."sn" AS "sn", "z"."sn" AS "sn0" FROM
        (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn" FROM
        (SELECT sum("t"."qty") AS "sm", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "z" JOIN (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM
        (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn", ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT sum("t"."weight") AS "sm", "t"."sn" AS "sn" FROM "p" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 5)) AS "y" ON ("z"."sm" = "y"."sm")
        GROUP BY "y"."sn", "z"."sn") AS "GNL" ON (("GNL"."sn0" = "s"."sn") AND ("GNL"."sn" = "s"."sn"))"#;
    test_it("test100", original_text, expected_text);
}

// WHERE, correlated: MAX among top-2 job sums ≥ 100
#[test]
fn test101() {
    let original_text = r#"
SELECT p.pn
FROM p AS p
WHERE (
  SELECT MAX(w.sm)
  FROM (
    SELECT SUM(t.qty) AS sm
    FROM spj AS t
    WHERE t.pn = p.pn
    GROUP BY t.jn
    ORDER BY sm DESC
    LIMIT 2
  ) AS w
) >= 100;"#;
    let expected_text = r#"SELECT "p"."pn" FROM "p" AS "p" INNER JOIN (SELECT max("w"."sm") AS "max(sm)", "w"."pn" AS "pn"
        FROM (SELECT "INNER"."sm" AS "sm", "INNER"."pn" AS "pn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."pn" AS "pn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."pn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn" FROM
        (SELECT sum("t"."qty") AS "sm", "t"."pn" AS "pn" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."pn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 2)) AS "w" GROUP BY "w"."pn") AS "GNL" ON ("GNL"."pn" = "p"."pn")
        WHERE ("GNL"."max(sm)" >= 100)"#;
    test_it("test101", original_text, expected_text);
}

// WHERE IN, non-correlated: jobs in global top-5 by qty (via spj)
#[test]
fn test102() {
    let original_text = r#"
SELECT j.jn
FROM j AS j
WHERE j.jn IN (
  SELECT t.jn
  FROM spj AS t
  ORDER BY t.qty DESC
  LIMIT 5
);"#;
    let expected_text = r#"SELECT "j"."jn" FROM "j" AS "j" INNER JOIN
        (SELECT DISTINCT "INNER"."jn" AS "jn" FROM (SELECT "t"."jn" AS "jn", ROW_NUMBER() OVER(ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn"
        FROM "spj" AS "t") AS "INNER" WHERE ("INNER"."__rn" <= 5)) AS "GNL" ON ("j"."jn" = "GNL"."jn")"#;
    test_it("test102", original_text, expected_text);
}

// SELECT-list boolean, non-correlated: “is supplier in top-5 by total qty”
#[test]
fn test103() {
    let original_text = r#"
SELECT
  s.sn,
  (s.sn IN (
     SELECT x.sn
     FROM (
       SELECT t.sn, SUM(t.qty) AS sm
       FROM spj AS t
       GROUP BY t.sn
       ORDER BY sm DESC
       LIMIT 5
     ) AS x
  )) AS top5_supplier
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."sn", CASE WHEN ("GNL"."sn" IS NOT NULL) THEN TRUE
        WHEN (("s"."sn" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "top5_supplier"
        FROM "s" AS "s" LEFT OUTER JOIN (SELECT DISTINCT "x"."sn" AS "sn" FROM
        (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm" FROM
        (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm", ROW_NUMBER() OVER(ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT "t"."sn" AS "sn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 5)) AS "x") AS "GNL" ON ("s"."sn" = "GNL"."sn") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm" FROM
        (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm", ROW_NUMBER() OVER(ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT "t"."sn" AS "sn", sum("t"."qty") AS "sm" FROM "spj" AS "t" GROUP BY "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 5)) AS "x") AS "EP_3VL"  LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT 1 AS "present_", "x"."sn" AS "sn" FROM
        (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm" FROM (SELECT "INNER"."sn" AS "sn", "INNER"."sm" AS "sm",
        ROW_NUMBER() OVER(ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn" FROM (SELECT "t"."sn" AS "sn", sum("t"."qty") AS "sm"
        FROM "spj" AS "t" GROUP BY "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 5)) AS "x") AS "NP_3VL" WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL""#;
    test_it("test103", original_text, expected_text);
}

// LATERAL, correlated: return top-1 job (jn, sm) per supplier
#[test]
fn test104() {
    let original_text = r#"
SELECT
  s.sn,
  x.top_jn,
  x.top_sm
FROM s AS s
CROSS JOIN LATERAL (
  SELECT q.jn AS top_jn, q.sm AS top_sm
  FROM (
    SELECT t.jn, SUM(t.qty) AS sm
    FROM spj AS t
    WHERE t.sn = s.sn
    GROUP BY t.jn
    ORDER BY sm DESC
    LIMIT 1
  ) AS q
) AS x;"#;
    let expected_text = r#"SELECT "s"."sn", "x"."top_jn", "x"."top_sm" FROM "s" AS "s" INNER JOIN
        (SELECT "q"."jn" AS "top_jn", "q"."sm" AS "top_sm", "q"."sn" AS "sn" FROM
        (SELECT "INNER"."jn" AS "jn", "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM
        (SELECT "INNER"."jn" AS "jn", "INNER"."sm" AS "sm", "INNER"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT "t"."jn" AS "jn", sum("t"."qty") AS "sm", "t"."sn" AS "sn"
        FROM "spj" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "q") AS "x" ON ("x"."sn" = "s"."sn")"#;
    test_it("test104", original_text, expected_text);
}

// WHERE NOT IN, correlated: exclude top-3 sums per part (3VL-safe)
#[test]
fn test105() {
    let original_text = r#"
SELECT
  spj.qty
FROM
  spj
WHERE
  spj.qty NOT IN (
    SELECT z.sm
    FROM (
      SELECT SUM(t.weight) AS sm
      FROM p AS t
      WHERE t.pn = spj.pn
      GROUP BY t.jn
      ORDER BY sm DESC
      LIMIT 3
    ) AS z
  );"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT "z"."sm" AS "sm", "z"."pn" AS "pn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."pn" AS "pn" FROM
        (SELECT "INNER"."sm" AS "sm", "INNER"."pn" AS "pn", ROW_NUMBER() OVER(PARTITION BY "INNER"."pn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn"
        FROM (SELECT sum("t"."weight") AS "sm", "t"."pn" AS "pn" FROM "p" AS "t" GROUP BY "t"."jn", "t"."pn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "z") AS "GNL" ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."sm"))
        WHERE ("GNL"."sm" IS NULL)"#;
    test_it("test105", original_text, expected_text);
}

// WHERE IN + OFFSET, correlated: job in positions 3–7 by qty per supplier
#[test]
fn test106() {
    let original_text = r#"
SELECT spj.jn
FROM spj
WHERE spj.jn IN (
  SELECT t.jn
  FROM p AS t
  WHERE t.sn = spj.sn
  ORDER BY t.weight DESC
  LIMIT 5 OFFSET 2   -- top 5 after skipping first 2
);"#;
    let expected_text = r#"SELECT "spj"."jn" FROM "spj" INNER JOIN
        (SELECT DISTINCT "INNER"."jn" AS "jn", "INNER"."sn" AS "sn" FROM
        (SELECT "t"."jn" AS "jn", "t"."sn" AS "sn", ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."weight" DESC NULLS FIRST) AS "__rn"
        FROM "p" AS "t") AS "INNER" WHERE (("INNER"."__rn" > 2) AND ("INNER"."__rn" <= 7))) AS "GNL"
        ON (("GNL"."sn" = "spj"."sn") AND ("spj"."jn" = "GNL"."jn"))"#;
    test_it("test106", original_text, expected_text);
}

// SELECT-list, correlated per color: top-1 weight (no aggregate wrapper needed)
#[test]
fn test107() {
    let original_text = r#"
SELECT
  s.city,
  (
    SELECT MAX(w)
    FROM (
      SELECT i.weight AS w
      FROM p AS i
      WHERE i.city = s.city
      ORDER BY i.weight DESC
      LIMIT 1
    ) AS top1
  ) AS max_weight_in_color
FROM s AS s;"#;
    let expected_text = r#"SELECT "s"."city", "GNL"."max(w)" AS "max_weight_in_color" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT max("w") AS "max(w)", "top1"."city" AS "city" FROM (SELECT "INNER"."w" AS "w", "INNER"."city" AS "city"
        FROM (SELECT "i"."weight" AS "w", "i"."city" AS "city",
        ROW_NUMBER() OVER(PARTITION BY "i"."city" ORDER BY "i"."weight" DESC NULLS FIRST) AS "__rn" FROM "p" AS "i") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "top1" GROUP BY "top1"."city") AS "GNL" ON ("GNL"."city" = "s"."city")"#;
    test_it("test107", original_text, expected_text);
}

// WHERE EXISTS, correlated with filter on the top-1 row
#[test]
fn test108() {
    let original_text = r#"
SELECT p.pn
FROM spj AS p
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT i.city, i.weight
    FROM p AS i
    WHERE i.pn = p.pn
    ORDER BY i.weight DESC
    LIMIT 1
  ) AS top1
  WHERE top1.weight >= 50 AND top1.city = 'LONDON'
);"#;
    let expected_text = r#"SELECT "p"."pn" FROM "spj" AS "p" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "top1"."pn" AS "pn" FROM
        (SELECT "INNER"."city" AS "city", "INNER"."weight" AS "weight", "INNER"."pn" AS "pn" FROM
        (SELECT "i"."city" AS "city", "i"."weight" AS "weight", "i"."pn" AS "pn",
        ROW_NUMBER() OVER(PARTITION BY "i"."pn" ORDER BY "i"."weight" DESC NULLS FIRST) AS "__rn" FROM "p" AS "i") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "top1" WHERE (("top1"."weight" >= 50) AND ("top1"."city" = 'LONDON'))) AS "GNL"
        ON ("GNL"."pn" = "p"."pn")"#;
    test_it("test108", original_text, expected_text);
}

// WHERE IN; correlated; NP/EP omitted when null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test109() {
    let original_text = r#"
select
   spj.jn
from
   spj
where
   spj.qty in (
       select
           s.status
       from (
          select
             t1.weight,
             t1.pn
          from
             p t1
          where
             t1.sn = spj.sn
       ) t1
       join s on s.pn = t1.pn
   );"#;
    let expected_text = r#"SELECT "spj"."jn" FROM "spj" INNER JOIN
        (SELECT DISTINCT "s"."status" AS "status", "t1"."sn" AS "sn" FROM
        (SELECT "t1"."weight", "t1"."pn", "t1"."sn" AS "sn" FROM "p" AS "t1") AS "t1" JOIN "s" ON ("s"."pn" = "t1"."pn")) AS "GNL"
        ON (("GNL"."sn" = "spj"."sn") AND ("spj"."qty" = "GNL"."status"))"#;
    test_it("test109", original_text, expected_text);
}

// Reuse NP/EP across two SELECT-list IN predicates sharing the same RHS (same ON)
// WHERE IN; non-correlated; rewritten as LEFT OUTER JOIN; 3-VL probes (NP/EP) present; RHS
// deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test110() {
    let original_text = r#"
    SELECT
      (t1.i   IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t)) AS in1,
      (t1.b   IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t)) AS in2
    FROM test1 t1;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in1",
        CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE WHEN (("t1"."b" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in2" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL"
        ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."t" AS "t" FROM "test2" AS "t2") AS "EP_3VL"
        ON ("EP_3VL"."t" = "t1"."t") LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t"
        FROM (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "NP_3VL"
         WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
         (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL1"
         ON (("GNL1"."t" = "t1"."t") AND ("t1"."b" = "GNL1"."i"))"#;
    test_it("test110", original_text, expected_text);
}

// Reuse NP/EP across WHERE NOT IN and SELECT-list IN sharing the same RHS
// WHERE NOT IN; non-correlated; rewritten as LEFT OUTER JOIN; anti-semi via IS NULL guard; 3-VL
// probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for
// 3-VL.
#[test]
fn test111() {
    let original_text = r#"
    SELECT
      (t1.i IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t)) AS in1
    FROM test1 t1
    WHERE t1.i NOT IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t);"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in1" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL" ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i"))
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "NP_3VL"
        WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."t" AS "t" FROM "test2" AS "t2") AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t")
        LEFT OUTER JOIN (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL1"
        ON (("GNL1"."t" = "t1"."t") AND ("t1"."i" = "GNL1"."i"))
        WHERE (("GNL"."i" IS NULL) AND ((("t1"."i" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test111", original_text, expected_text);
}

// RHS first field is provably NULL-free (COUNT(*)) and always return result; NP and EP must be omitted.
// SELECT-list IN; non-correlated; rewritten as LEFT OUTER JOIN; NP/EP omitted when null-safe.
#[test]
fn test112() {
    let original_text = r#"
    SELECT
      (t1.i IN (SELECT COUNT(*) FROM test2 t2 WHERE t2.t = t1.t)) AS in_cnt
    FROM test1 t1"#;
    let expected_text = r#"SELECT ("t1"."i" = coalesce("GNL"."count(*)", 0)) AS "in_cnt" FROM "test1" AS "t1"
        LEFT OUTER JOIN (SELECT count(*) AS "count(*)", "t2"."t" AS "t" FROM "test2" AS "t2" GROUP BY "t2"."t") AS "GNL"
        ON ("GNL"."t" = "t1"."t")"#;
    test_it("test112", original_text, expected_text);
}

// WF-based NP: require NP to be built via a window function wrapper, with WHERE RHS_WF IS NULL
// SELECT-list IN; non-correlated; materialized via window ROW_NUMBER/DENSE_RANK; rewritten as
// LEFT OUTER JOIN; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-
// list shaped via CASE for 3-VL.
#[test]
fn test113() {
    let original_text = r#"
    SELECT
      (t1.i IN (
        SELECT x.i
        FROM (
          SELECT t2.i, ROW_NUMBER() OVER (PARTITION BY t2.t ORDER BY t2.i) AS rn, t2.t
          FROM test2 t2
        ) x
        WHERE x.rn = 1 AND x.t = t1.t
      )) AS in_wf
    FROM test1 t1"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_wf"
        FROM "test1" AS "t1" LEFT OUTER JOIN (SELECT DISTINCT "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t"
        FROM "test2" AS "t2") AS "x" WHERE ("x"."rn" = 1)) AS "GNL" ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i"))
        LEFT OUTER JOIN (SELECT DISTINCT 1 AS "present_", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t"
        FROM "test2" AS "t2") AS "x" WHERE ("x"."rn" = 1)) AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t"
        FROM "test2" AS "t2") AS "x" WHERE ("x"."rn" = 1)) AS "NP_3VL"
        WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t")"#;
    test_it("test113", original_text, expected_text);
}

// Different correlation ON => different keys; expect two NP/EP pairs (with suffixed aliases)
#[test]
fn test114() {
    let original_text = r#"
    SELECT
      (t1.i IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t)) AS in_t,
      (t1.i IN (SELECT t2.i FROM test2 t2 WHERE t2.b = t1.b)) AS in_b
    FROM test1 t1"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_t",
        CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL1"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL1"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_b"
        FROM "test1" AS "t1" LEFT OUTER JOIN (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL"
        ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."t" AS "t" FROM "test2" AS "t2") AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "NP_3VL" WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL"
        ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN (SELECT DISTINCT "t2"."i" AS "i", "t2"."b" AS "b" FROM "test2" AS "t2") AS "GNL1"
        ON (("GNL1"."b" = "t1"."b") AND ("t1"."i" = "GNL1"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."b" AS "b" FROM "test2" AS "t2") AS "EP_3VL1" ON ("EP_3VL1"."b" = "t1"."b")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL1"."present_" AS "present_", "NP_3VL1"."b" AS "b" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."b" AS "b" FROM "test2" AS "t2") AS "NP_3VL1"
        WHERE ("NP_3VL1"."i" IS NULL)) AS "NP_3VL1" ON ("NP_3VL1"."b" = "t1"."b")"#;
    test_it("test114", original_text, expected_text);
}

// WHERE IN; correlated; 3-VL probes (NP/EP) present; asserts helper(s): contains_np_3vl,
// contains_ep_3vl.
#[test]
fn test115() {
    // No reuse across DIFFERENT ON: one correlated and one non-correlated RHS with the same body
    // should yield TWO distinct probe pairs (EP_3VL/NP_3VL and EP_3VL1/NP_3VL1).
    let q = r#"
        SELECT
          (t1.i IN (SELECT t2.i FROM test2 AS t2 WHERE t2.t = t1.t)) AS corr,
          (t1.b IN (SELECT t3.i FROM test2 AS t3))                    AS uncorr
        FROM test1 AS t1;
        "#;

    let schema_guard = get_schema_guard();
    let out = rewrite("test116", q, schema_guard);

    assert!(
        contains_np_3vl(&out) && contains_ep_3vl(&out),
        "expected base probe pair; got:\n{out}"
    );
    // Second, distinct pair must be suffixed by the alias generator.
    assert!(
        out.contains("NP_3VL1") && out.contains("EP_3VL1"),
        "expected a second probe pair for the non-correlated RHS; got:\n{out}"
    );
}

// WHERE IN; non-correlated; materialized via window ROW_NUMBER/DENSE_RANK; 3-VL probes (NP/EP)
// present; asserts helper(s): contains_np_3vl, contains_ep_3vl.
#[test]
fn test116() {
    // Different TOP-K (window function) shapes must not be reused: rn=1 vs rn<=2.
    // Expect TWO distinct probe pairs because the NP variant (window expr) differs.
    let q = r#"
        SELECT
          (t1.i IN (
            SELECT x.i
            FROM (
              SELECT t2.i, ROW_NUMBER() OVER (PARTITION BY t2.t ORDER BY t2.i) AS rn, t2.t
              FROM test2 AS t2
            ) AS x
            WHERE x.rn = 1 AND x.t = t1.t
          )) AS in_top1,
          (t1.i IN (
            SELECT y.i
            FROM (
              SELECT t3.i, ROW_NUMBER() OVER (PARTITION BY t3.t ORDER BY t3.i) AS rn, t3.t
              FROM test2 AS t3
            ) AS y
            WHERE y.rn <= 2 AND y.t = t1.t
          )) AS in_top2
        FROM test1 AS t1;
        "#;

    let schema_guard = get_schema_guard();
    let out = rewrite("test94", q, schema_guard);

    // Must have at least one NP/EP pair…
    assert!(
        contains_np_3vl(&out) && contains_ep_3vl(&out),
        "expected first probe pair; got:\n{out}"
    );
    // …and a second suffixed pair because the NP variant hash differs (rn=1 vs rn<=2).
    assert!(
        out.contains("NP_3VL1") && out.contains("EP_3VL1"),
        "expected a second probe pair for distinct TOP-K variant; got:\n{out}"
    );
}

// WHERE NOT IN: LHS spj.qty NON-NULL (by schema), RHS p.weight NON-NULL (by schema)
// => No NP/EP; anti-semi via LEFT JOIN + IS NULL
#[test]
fn test117() {
    let schema_guard = get_schema_guard();

    let q = r#"
    SELECT spj.qty
    FROM spj
    WHERE spj.qty NOT IN (
      SELECT p.weight
      FROM p
      WHERE p.pn = spj.pn
    );
    "#;

    let out = rewrite("test117", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free); got:\n{out}"
    );
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be omitted (LHS non-null); got:\n{out}"
    );
    assert!(
        out.contains(" IS NULL"),
        "expected anti-join IS NULL guard; got:\n{out}"
    );
}

// WHERE NOT IN with extra RHS filter, but RHS first field still NON-NULL by schema
// => No NP/EP; same anti-semi shape
#[test]
fn test118() {
    let schema_guard = get_schema_guard();

    let q = r#"
    SELECT spj.qty
    FROM spj
    WHERE spj.qty NOT IN (
      SELECT p.weight
      FROM p
      WHERE p.pn = spj.pn
        AND p.weight > 0
    );
    "#;

    let out = rewrite("test118", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free); got:\n{out}"
    );
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be omitted (LHS non-null); got:\n{out}"
    );
    assert!(
        out.contains(" IS NULL"),
        "expected anti-join IS NULL guard; got:\n{out}"
    );
}

// SELECT-list IN: LHS spj.qty NON-NULL, RHS p.weight NON-NULL
// => No CASE, no NP/EP; simplify to `… IS NOT NULL`
#[test]
fn test119() {
    let schema_guard = get_schema_guard();

    let q = r#"
    SELECT
      spj.sn,
      (spj.qty IN (
         SELECT p.weight
         FROM p
         WHERE p.pn = spj.pn
      )) AS in_flag
    FROM spj;
    "#;

    let out = rewrite("test95", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN; got:\n{out}"
    );
    assert!(
        !contains_case(&out),
        "3VL CASE should be simplified away; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted; got:\n{out}"
    );
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be omitted; got:\n{out}"
    );
    assert!(
        out.contains(" IS NOT NULL"),
        "expected IS NOT NULL simplification; got:\n{out}"
    );
}

// SELECT-list IN with TOP-1 via ROW_NUMBER: both sides NON-NULL
// => No NP (WF-based probe unnecessary), no EP, no CASE
#[test]
fn test120() {
    let schema_guard = get_schema_guard();

    let q = r#"
    SELECT
      spj.sn,
      (spj.qty IN (
        SELECT z.weight
        FROM (
          SELECT p.weight,
                 ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.weight DESC) AS rn,
                 p.pn
          FROM p
        ) AS z
        WHERE z.rn = 1 AND z.pn = spj.pn
      )) AS in_top1
    FROM spj;
    "#;

    let out = rewrite("test120", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN; got:\n{out}"
    );
    assert!(
        out.contains("ROW_NUMBER"),
        "expected WF inlined for TOP-1; got:\n{out}"
    );
    assert!(
        !contains_case(&out),
        "CASE should be simplified when both sides non-null; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free); got:\n{out}"
    );
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be omitted (LHS non-null); got:\n{out}"
    );
}

// WHERE NOT IN with TOP-1 via ROW_NUMBER: both sides NON-NULL
// => No NP/EP; anti-semi via LEFT JOIN + IS NULL
#[test]
fn test121() {
    let schema_guard = get_schema_guard();

    let q = r#"
    SELECT spj.qty
    FROM spj
    WHERE spj.qty NOT IN (
      SELECT z.weight
      FROM (
        SELECT p.weight,
               ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.weight DESC) AS rn,
               p.pn
        FROM p
      ) AS z
      WHERE z.rn = 1 AND z.pn = spj.pn
    );
    "#;

    let out = rewrite("test121", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN; got:\n{out}"
    );
    assert!(
        out.contains("ROW_NUMBER"),
        "expected WF inlined for TOP-1; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free); got:\n{out}"
    );
    assert!(
        !contains_ep_3vl(&out),
        "EP_3VL should be omitted (LHS non-null); got:\n{out}"
    );
    assert!(
        out.contains(" IS NULL"),
        "expected anti-join IS NULL guard; got:\n{out}"
    );
}

// SELECT-list IN: LHS s.city NULLABLE, RHS p.city NULL-free by schema -> EP present, NP omitted
#[test]
fn test122() {
    let mut schema_guard = get_schema_guard();
    // Make LHS nullable explicitly to exercise the EP-only path
    make_col_nullable("s", "city", &mut schema_guard);

    let q = r#"
    SELECT
      s.sn,
      (s.city IN (
         SELECT p.city
         FROM p
         WHERE p.sn = s.sn
      )) AS same_city
    FROM s;
    "#;

    let out = rewrite("test96", q, schema_guard);
    assert!(
        contains_case(&out),
        "expected 3VL CASE for nullable LHS; got:\n{out}"
    );
    assert!(
        contains_ep_3vl(&out),
        "EP_3VL should be present (LHS nullable); got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free by schema); got:\n{out}"
    );
}

// WHERE NOT IN on cities: LHS s.city may be NULL, RHS p.city NON-NULL by schema
// => EP present (for is_empty branch), NP omitted
#[test]
fn test123() {
    let mut schema_guard = get_schema_guard();
    // Ensure LHS can be NULL
    make_col_nullable("s", "city", &mut schema_guard);

    let q = r#"
    SELECT s.sn
    FROM s
    WHERE s.city NOT IN (
      SELECT p.city
      FROM p
      WHERE p.sn = s.sn
    );
    "#;

    let out = rewrite("test123", q, schema_guard);
    assert!(
        contains_left_outer_join(&out),
        "expected LEFT OUTER JOIN anti-join; got:\n{out}"
    );
    assert!(
        contains_ep_3vl(&out),
        "EP_3VL must be present when LHS is nullable; got:\n{out}"
    );
    assert!(
        !contains_np_3vl(&out),
        "NP_3VL should be omitted (RHS null-free by schema); got:\n{out}"
    );
}

// WHERE IN; correlated; rewritten as LEFT OUTER JOIN; 3-VL probes (NP/EP) present; RHS
// deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test124() {
    // Alias-insensitive reuse across two SELECT-list IN predicates (same RHS semantics)
    let original_text = r#"
SELECT
  (t1.i IN (SELECT u.i FROM test2 AS u WHERE u.t = t1.t)) AS in_u,
  (t1.b IN (SELECT v.i FROM test2 AS v WHERE v.t = t1.t)) AS in_v
FROM test1 AS t1;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_u",
        CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE WHEN (("t1"."b" IS NULL) AND ("EP_3VL1"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL1"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_v" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT DISTINCT "u"."i" AS "i", "u"."t" AS "t" FROM "test2" AS "u") AS "GNL"
        ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "u"."t" AS "t" FROM "test2" AS "u") AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "u"."i" AS "i", "u"."t" AS "t" FROM "test2" AS "u") AS "NP_3VL"
        WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT "v"."i" AS "i", "v"."t" AS "t" FROM "test2" AS "v") AS "GNL1"
        ON (("GNL1"."t" = "t1"."t") AND ("t1"."b" = "GNL1"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "v"."t" AS "t" FROM "test2" AS "v") AS "EP_3VL1" ON ("EP_3VL1"."t" = "t1"."t")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL1"."present_" AS "present_", "NP_3VL1"."t" AS "t" FROM
        (SELECT 1 AS "present_", "v"."i" AS "i", "v"."t" AS "t" FROM "test2" AS "v") AS "NP_3VL1"
        WHERE ("NP_3VL1"."i" IS NULL)) AS "NP_3VL1" ON ("NP_3VL1"."t" = "t1"."t")"#;
    test_it("test124", original_text, expected_text);
}

// WHERE IN; correlated; rewritten as LEFT OUTER JOIN; 3-VL probes (NP/EP) present; RHS
// deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test125() {
    // Different correlation ON => different keys; expect two probe pairs (suffixed aliases)
    let original_text = r#"
SELECT
  (t1.i IN (SELECT t2.i FROM test2 AS t2 WHERE t2.t = t1.t)) AS in_t,
  (t1.i IN (SELECT t2.i FROM test2 AS t2 WHERE t2.b = t1.b)) AS in_b
FROM test1 AS t1;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_t",
        CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE WHEN (("t1"."i" IS NULL) AND ("EP_3VL1"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL1"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_b" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL"
        ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."t" AS "t" FROM "test2" AS "t2") AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "NP_3VL" WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL"
        ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN (SELECT DISTINCT "t2"."i" AS "i", "t2"."b" AS "b" FROM "test2" AS "t2") AS "GNL1"
        ON (("GNL1"."b" = "t1"."b") AND ("t1"."i" = "GNL1"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."b" AS "b" FROM "test2" AS "t2") AS "EP_3VL1" ON ("EP_3VL1"."b" = "t1"."b")
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL1"."present_" AS "present_", "NP_3VL1"."b" AS "b" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."b" AS "b" FROM "test2" AS "t2") AS "NP_3VL1"
        WHERE ("NP_3VL1"."i" IS NULL)) AS "NP_3VL1" ON ("NP_3VL1"."b" = "t1"."b")"#;
    test_it("test125", original_text, expected_text);
}

// WHERE NOT IN; non-correlated; rewritten as LEFT OUTER JOIN; anti-semi via IS NULL guard; 3-VL
// probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for
// 3-VL.
#[test]
fn test126() {
    // Reuse NP/EP across WHERE NOT IN and SELECT-list IN sharing the same RHS
    let original_text = r#"
SELECT
  (t1.i IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t)) AS in1
FROM test1 t1
WHERE t1.i NOT IN (SELECT t2.i FROM test2 t2 WHERE t2.t = t1.t);"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL1"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in1" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL"
        ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "NP_3VL"
        WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t2"."t" AS "t" FROM "test2" AS "t2") AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t")
        LEFT OUTER JOIN (SELECT DISTINCT "t2"."i" AS "i", "t2"."t" AS "t" FROM "test2" AS "t2") AS "GNL1"
        ON (("GNL1"."t" = "t1"."t") AND ("t1"."i" = "GNL1"."i"))
        WHERE (("GNL"."i" IS NULL) AND ((("t1"."i" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test126", original_text, expected_text);
}

// SELECT-list IN; non-correlated; materialized via window ROW_NUMBER/DENSE_RANK; 3-VL probes
// (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test127() {
    // WF-based NP on SELECT-list IN (TOP-1 via ROW_NUMBER); expect EP and NP present
    let original_text = r#"
SELECT
  (t1.i IN (
    SELECT x.i
    FROM (
      SELECT t2.i, ROW_NUMBER() OVER (PARTITION BY t2.t ORDER BY t2.i) AS rn, t2.t
      FROM test2 t2
    ) x
    WHERE x.rn = 1 AND x.t = t1.t
  )) AS in_wf
FROM test1 t1;"#;
    let expected_text = r#"SELECT CASE WHEN ("GNL"."i" IS NOT NULL) THEN TRUE
        WHEN (("t1"."i" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_wf" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT DISTINCT "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t" FROM "test2" AS "t2") AS "x"
        WHERE ("x"."rn" = 1)) AS "GNL" ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i")) LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t" FROM "test2" AS "t2") AS "x"
        WHERE ("x"."rn" = 1)) AS "EP_3VL" ON ("EP_3VL"."t" = "t1"."t") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t" FROM
        (SELECT 1 AS "present_", "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t" FROM "test2" AS "t2") AS "x"
        WHERE ("x"."rn" = 1)) AS "NP_3VL" WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."t" = "t1"."t")"#;
    test_it("test127", original_text, expected_text);
}

// WHERE NOT IN; correlated; NP/EP omitted when null-safe.
#[test]
fn test128() {
    // WHERE NOT IN: both sides non-NULL (spj.qty, p.weight) => no NP/EP, anti-join via IS NULL
    let original_text = r#"
SELECT spj.qty
FROM spj
WHERE spj.qty NOT IN (
  SELECT p.weight
  FROM p
  WHERE p.pn = spj.pn
);"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT "p"."weight" AS "weight", "p"."pn" AS "pn" FROM "p") AS "GNL" ON
        (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."weight"))
        WHERE ("GNL"."weight" IS NULL)"#;
    test_it("test128", original_text, expected_text);
}

// SELECT-list IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; NP/EP omitted when
// null-safe; RHS deduplicated (DISTINCT).
#[test]
fn test129() {
    // SELECT-list IN with LIMIT/OFFSET on non-NULL field (weight) => no NP/EP, IS NOT NULL
    let original_text = r#"
SELECT
  spj.sn,
  (spj.qty IN (
     SELECT i.weight
     FROM (
       SELECT p.weight, ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.weight DESC) AS rn, p.pn
       FROM p AS p
     ) AS i
     WHERE i.pn = spj.pn AND i.rn BETWEEN 3 AND 7
  )) AS in_range
FROM spj;"#;
    let expected_text = r#"SELECT "spj"."sn", ("GNL"."weight" IS NOT NULL) AS "in_range" FROM "spj" LEFT OUTER JOIN
        (SELECT DISTINCT "i"."weight" AS "weight", "i"."pn" AS "pn" FROM
        (SELECT "p"."weight", ROW_NUMBER() OVER(PARTITION BY "p"."pn" ORDER BY "p"."weight" DESC NULLS FIRST) AS "rn", "p"."pn" FROM "p" AS "p") AS "i"
        WHERE "i"."rn" BETWEEN 3 AND 7) AS "GNL" ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."weight"))"#;
    test_it("test129", original_text, expected_text);
}

// WHERE IN; correlated; with ORDER BY/LIMIT; materialized via window ROW_NUMBER/DENSE_RANK;
// rewritten as INNER JOIN; semi-join; NP/EP omitted when null-safe; RHS deduplicated
// (DISTINCT).
#[test]
fn test130() {
    // Non-correlated IN with LIMIT/OFFSET by qty top-K
    let original_text = r#"
SELECT j.jn
FROM j AS j
WHERE j.jn IN (
  SELECT t.jn
  FROM spj AS t
  ORDER BY t.qty DESC
  LIMIT 5 OFFSET 2
);"#;
    let expected_text = r#"SELECT "j"."jn" FROM "j" AS "j" INNER JOIN (SELECT DISTINCT "INNER"."jn" AS "jn" FROM
        (SELECT "t"."jn" AS "jn", ROW_NUMBER() OVER(ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn" FROM "spj" AS "t") AS "INNER"
         WHERE (("INNER"."__rn" > 2) AND ("INNER"."__rn" <= 7))) AS "GNL" ON ("j"."jn" = "GNL"."jn")"#;
    test_it("test130", original_text, expected_text);
}

// SELECT-list IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test131() {
    // SELECT-list IN with nested join inside RHS
    let original_text = r#"
SELECT
  spj.sn,
  (spj.qty IN (
     SELECT s.status
     FROM (
        SELECT t1.weight, t1.pn, t1.sn
        FROM p t1
        WHERE t1.sn = spj.sn
     ) t1
     JOIN s ON s.pn = t1.pn
  )) AS in_status
FROM spj;"#;
    let expected_text = r#"SELECT "spj"."sn", CASE WHEN ("GNL"."status" IS NOT NULL) THEN TRUE
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_status" FROM "spj" LEFT OUTER JOIN
        (SELECT DISTINCT "s"."status" AS "status", "t1"."sn" AS "sn" FROM
        (SELECT "t1"."weight", "t1"."pn", "t1"."sn" FROM "p" AS "t1") AS "t1" JOIN "s"
        ON ("s"."pn" = "t1"."pn")) AS "GNL" ON (("GNL"."sn" = "spj"."sn") AND ("spj"."qty" = "GNL"."status")) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."sn" AS "sn" FROM
        (SELECT 1 AS "present_", "s"."status" AS "status", "t1"."sn" AS "sn" FROM
        (SELECT "t1"."weight", "t1"."pn", "t1"."sn" FROM "p" AS "t1") AS "t1" JOIN "s" ON ("s"."pn" = "t1"."pn")) AS "NP_3VL"
        WHERE ("NP_3VL"."status" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."sn" = "spj"."sn")"#;
    test_it("test131", original_text, expected_text);
}

// WHERE NOT IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; rewritten as LEFT
// OUTER JOIN; anti-semi via IS NULL guard; 3-VL probes (NP/EP) present; RHS deduplicated
// (DISTINCT).
#[test]
fn test132() {
    // Ensure LHS is NULL-free
    make_col_null_free("test1", "i", &mut get_schema_guard());

    // WHERE NOT IN with WF-based RHS first item potentially NULL: NP present, EP omitted (LHS non-NULL)
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1
WHERE t1.i NOT IN (
  SELECT x.i
  FROM (
    SELECT t2.i, ROW_NUMBER() OVER (PARTITION BY t2.t ORDER BY t2.i) AS rn, t2.t
    FROM test2 AS t2
  ) AS x
  WHERE x.rn = 1 AND x.t = t1.t
);"#;
    let expected_text = r#"SELECT "t1"."i" FROM "test1" AS "t1" LEFT OUTER JOIN
        (SELECT "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t"
        FROM "test2" AS "t2") AS "x" WHERE ("x"."rn" = 1)) AS "GNL" ON (("GNL"."t" = "t1"."t") AND ("t1"."i" = "GNL"."i"))
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."t" AS "t"
        FROM (SELECT 1 AS "present_", "x"."i" AS "i", "x"."t" AS "t" FROM
        (SELECT "t2"."i", ROW_NUMBER() OVER(PARTITION BY "t2"."t" ORDER BY "t2"."i" ASC NULLS LAST) AS "rn", "t2"."t"
        FROM "test2" AS "t2") AS "x" WHERE ("x"."rn" = 1)) AS "NP_3VL" WHERE ("NP_3VL"."i" IS NULL)) AS "NP_3VL"
        ON ("NP_3VL"."t" = "t1"."t") WHERE (("GNL"."i" IS NULL) AND ("NP_3VL"."present_" IS NULL))"#;
    test_it("test132", original_text, expected_text);
}

// SELECT-list IN; non-correlated; with ORDER BY/LIMIT; materialized via window
// ROW_NUMBER/DENSE_RANK; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test133() {
    // SELECT-list IN, non-correlated with LIMIT; boolean result via CASE because LHS may be NULL
    let original_text = r#"
SELECT
  j.jn,
  (j.jn IN (
     SELECT t.jn
     FROM spj AS t
     ORDER BY t.qty DESC
     LIMIT 3
  )) AS in_top3
FROM j AS j;"#;
    let expected_text = r#"SELECT "j"."jn", CASE WHEN ("GNL"."jn" IS NOT NULL) THEN TRUE
        WHEN (("j"."jn" IS NULL) AND ("EP_3VL"."present_" IS NOT NULL)) THEN NULL
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_top3" FROM "j" AS "j" LEFT OUTER JOIN
        (SELECT DISTINCT "INNER"."jn" AS "jn" FROM (SELECT "t"."jn" AS "jn", ROW_NUMBER() OVER(ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn"
        FROM "spj" AS "t") AS "INNER" WHERE ("INNER"."__rn" <= 3)) AS "GNL" ON ("j"."jn" = "GNL"."jn") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj" AS "t") AS "EP_3VL"  LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT "INNER"."present_" AS "present_", "INNER"."jn" AS "jn" FROM
        (SELECT 1 AS "present_", "t"."jn" AS "jn", ROW_NUMBER() OVER(ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn" FROM "spj" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "NP_3VL" WHERE ("NP_3VL"."jn" IS NULL)) AS "NP_3VL""#;
    test_it("test133", original_text, expected_text);
}

// WHERE NOT IN; correlated; rewritten as LEFT OUTER JOIN; anti-semi via IS NULL guard; 3-VL
// probes (NP/EP) present; RHS deduplicated (DISTINCT).
#[test]
fn test134() {
    let original_text = r#"
SELECT
   COUNT(*)
FROM
   s JOIN spj ON s.sn = spj.sn
WHERE
   s.sn NOT IN (SELECT spj.sn FROM spj WHERE s.pn = spj.pn) AND
   spj.pn NOT IN (SELECT spj.sn FROM spj WHERE s.pn = spj.pn);
"#;
    let expected_text = r#"SELECT count(*) FROM "s" JOIN "spj" ON ("s"."sn" = "spj"."sn") LEFT OUTER JOIN
        (SELECT "spj"."sn" AS "sn", "spj"."pn" AS "pn" FROM "spj") AS "GNL" ON (("s"."pn" = "GNL"."pn") AND ("s"."sn" = "GNL"."sn"))
        LEFT OUTER JOIN (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn"
        FROM (SELECT 1 AS "present_", "spj"."sn" AS "sn", "spj"."pn" AS "pn" FROM "spj") AS "NP_3VL"
        WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL" ON ("s"."pn" = "NP_3VL"."pn")
        WHERE ((("GNL"."sn" IS NULL) AND ("NP_3VL"."present_" IS NULL))
        AND "spj"."pn" NOT IN (SELECT "spj"."sn" FROM "spj" WHERE ("s"."pn" = "spj"."pn")))"#;
    test_it("test134", original_text, expected_text);
}

// WHERE subquery; correlated; with ORDER BY/LIMIT; materialized via window
// ROW_NUMBER/DENSE_RANK; with GROUP BY; rewritten as LEFT OUTER JOIN; 3-VL probes (NP/EP)
// present; RHS deduplicated (DISTINCT).
#[test]
fn test135() {
    let original_text = r#"
SELECT
   s.sname
FROM
   s
WHERE
   s.pn NOT IN
   (
       SELECT
           max(spj.pn) mx
       FROM
           spj
       WHERE
           spj.sn = 'spj.sn' and spj.sn = s.sn
       group by
           spj.jn
       order by
           max(spj.pn)
       limit
           1
   )
ORDER BY
   1, 2, 3, 4, 5, 6;"#;
    let expected_text = r#"SELECT "s"."sname" FROM "s" LEFT OUTER JOIN (SELECT "INNER"."mx" AS "mx", "INNER"."sn" AS "sn" FROM
        (SELECT "INNER"."mx" AS "mx", "INNER"."sn" AS "sn", ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."mx" ASC NULLS LAST) AS "__rn"
        FROM (SELECT max("spj"."pn") AS "mx", "spj"."sn" AS "sn" FROM "spj" WHERE ("spj"."sn" = 'spj.sn') GROUP BY "spj"."jn", "spj"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "GNL" ON (("GNL"."sn" = "s"."sn") AND ("s"."pn" = "GNL"."mx")) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."sn" AS "sn" FROM
        (SELECT "INNER"."present_" AS "present_", "INNER"."mx" AS "mx", "INNER"."sn" AS "sn" FROM
        (SELECT "INNER"."present_" AS "present_", "INNER"."mx" AS "mx", "INNER"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."mx" ASC NULLS LAST) AS "__rn" FROM
        (SELECT 1 AS "present_", max("spj"."pn") AS "mx", "spj"."sn" AS "sn" FROM "spj" WHERE ("spj"."sn" = 'spj.sn')
        GROUP BY "spj"."jn", "spj"."sn") AS "INNER") AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "NP_3VL"
        WHERE ("NP_3VL"."mx" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."sn" = "s"."sn") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "spj"."sn" AS "sn" FROM "spj" WHERE ("spj"."sn" = 'spj.sn')) AS "EP_3VL" ON ("EP_3VL"."sn" = "s"."sn")
        WHERE (("GNL"."mx" IS NULL) AND ((("s"."pn" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))
        ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST, 6 NULLS LAST"#;
    test_it("test135", original_text, expected_text);
}

// WHERE NOT IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test136() {
    // WHERE NOT IN; LHS const; RHS first field forced NULL via CASE -> NP present, EP omitted
    let original_text = r#"
SELECT spj.sn
FROM spj
WHERE 'X' NOT IN (
  SELECT CASE WHEN 1 = 1 THEN NULL ELSE p.city END AS v
  FROM p
  WHERE p.pn = spj.pn
);"#;
    let expected_text = r#"SELECT "spj"."sn" FROM "spj" LEFT OUTER JOIN
        (SELECT CASE WHEN (1 = 1) THEN NULL ELSE "p"."city" END AS "v", "p"."pn" AS "pn" FROM "p") AS "GNL"
        ON (("GNL"."pn" = "spj"."pn") AND ("GNL"."v" = 'X')) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn"
        FROM (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "p"."city" END AS "v", "p"."pn" AS "pn" FROM "p") AS "NP_3VL"
        WHERE ("NP_3VL"."v" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "spj"."pn")
        WHERE (("GNL"."v" IS NULL) AND ("NP_3VL"."present_" IS NULL))"#;
    test_it("test136", original_text, expected_text);
}

// SELECT-list IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; 3-VL probes
// (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test137() {
    // SELECT-list IN; RHS first field is window; forced NULL via CASE -> NP present; LHS const -> no EP
    let original_text = r#"
SELECT
  s.sn,
  (100 IN (
     SELECT CASE WHEN 1 = 1 THEN NULL ELSE w.mx END as w_mx
     FROM (
       SELECT MAX(t.qty) OVER (PARTITION BY t.pn) AS mx, t.pn
       FROM spj AS t
     ) AS w
     WHERE w.pn = s.pn
  )) AS has_const_in_win
FROM s;"#;
    let expected_text = r#"SELECT "s"."sn", CASE WHEN ("GNL"."w_mx" IS NOT NULL) THEN TRUE
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "has_const_in_win" FROM "s" LEFT OUTER JOIN
        (SELECT DISTINCT CASE WHEN (1 = 1) THEN NULL ELSE "w"."mx" END AS "w_mx", "w"."pn" AS "pn" FROM
        (SELECT max("t"."qty") OVER(PARTITION BY "t"."pn") AS "mx", "t"."pn" FROM "spj" AS "t") AS "w") AS "GNL"
        ON (("GNL"."pn" = "s"."pn") AND ("GNL"."w_mx" = 100)) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn" FROM
        (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "w"."mx" END AS "w_mx", "w"."pn" AS "pn" FROM
        (SELECT max("t"."qty") OVER(PARTITION BY "t"."pn") AS "mx", "t"."pn" FROM "spj" AS "t") AS "w") AS "NP_3VL"
        WHERE ("NP_3VL"."w_mx" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "s"."pn")"#;
    test_it("test137", original_text, expected_text);
}

// SELECT-list IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test138() {
    // SELECT-list IN; RHS join of nested subqueries, first field forced NULL via CASE -> NP present
    let original_text = r#"
SELECT
  s.sn,
  ('X' IN (
     SELECT CASE WHEN 1 = 1 THEN NULL ELSE t.city END as city
     FROM (
       SELECT p.city, p.pn FROM p
     ) AS t
     JOIN (
       SELECT spj.pn FROM spj
     ) AS u ON u.pn = t.pn
     WHERE t.pn = s.pn
  )) AS in_joined
FROM s;"#;
    let expected_text = r#"SELECT "s"."sn", CASE WHEN ("GNL"."city" IS NOT NULL) THEN TRUE
        WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_joined" FROM "s" LEFT OUTER JOIN
        (SELECT DISTINCT CASE WHEN (1 = 1) THEN NULL ELSE "t"."city" END AS "city", "t"."pn" AS "pn" FROM
        (SELECT "p"."city", "p"."pn" FROM "p") AS "t" JOIN (SELECT "spj"."pn" FROM "spj") AS "u" ON ("u"."pn" = "t"."pn")) AS "GNL"
        ON (("GNL"."pn" = "s"."pn") AND ("GNL"."city" = 'X')) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn" FROM
        (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "t"."city" END AS "city", "t"."pn" AS "pn" FROM
        (SELECT "p"."city", "p"."pn" FROM "p") AS "t" JOIN (SELECT "spj"."pn" FROM "spj") AS "u" ON ("u"."pn" = "t"."pn")) AS "NP_3VL"
        WHERE ("NP_3VL"."city" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "s"."pn")"#;
    test_it("test138", original_text, expected_text);
}

// WHERE NOT IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; rewritten as LEFT
// OUTER JOIN; anti-semi via IS NULL guard; 3-VL probes (NP/EP) present; RHS deduplicated
// (DISTINCT).
#[test]
fn test139() {
    // WHERE NOT IN; RHS TOP-1 over p.weight, first field p.city is null-free -> no NP, EP only
    let original_text = r#"
SELECT s.sn
FROM s
WHERE s.city NOT IN (
  SELECT z.city
  FROM (
    SELECT p.city, ROW_NUMBER() OVER (PARTITION BY p.city ORDER BY p.weight DESC) AS rn
    FROM p
  ) AS z
  WHERE z.rn = 1 AND z.city = s.city
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN (SELECT "z"."city" AS "city" FROM
        (SELECT "p"."city", ROW_NUMBER() OVER(PARTITION BY "p"."city" ORDER BY "p"."weight" DESC NULLS FIRST) AS "rn" FROM "p") AS "z"
        WHERE ("z"."rn" = 1)) AS "GNL" ON ("GNL"."city" = "s"."city") LEFT OUTER JOIN (SELECT DISTINCT 1 AS "present_", "z"."city" AS "city"
        FROM (SELECT "p"."city", ROW_NUMBER() OVER(PARTITION BY "p"."city" ORDER BY "p"."weight" DESC NULLS FIRST) AS "rn" FROM "p") AS "z"
        WHERE ("z"."rn" = 1)) AS "EP_3VL" ON ("EP_3VL"."city" = "s"."city")
        WHERE (("GNL"."city" IS NULL) AND (("s"."city" IS NOT NULL) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test139", original_text, expected_text);
}

// SELECT-list IN; correlated; materialized via window ROW_NUMBER/DENSE_RANK; 3-VL probes
// (NP/EP) present; RHS deduplicated (DISTINCT); boolean SELECT-list shaped via CASE for 3-VL.
#[test]
fn test140() {
    // SELECT-list IN; TOP-1 with forced NULL first field via CASE -> NP present; LHS const -> no EP
    let original_text = r#"
SELECT
  s.sn,
  ('CONST' IN (
     SELECT CASE WHEN 1 = 1 THEN NULL ELSE i.city END AS city
     FROM (
       SELECT p.city, ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.weight DESC) AS rn, p.pn
       FROM p
     ) AS i
     WHERE i.rn = 1 AND i.pn = s.pn
  )) AS in_nullified_top1
FROM s;"#;
    let expected_text = r#"SELECT "s"."sn", CASE WHEN ("GNL"."city" IS NOT NULL) THEN TRUE
         WHEN ("NP_3VL"."present_" IS NOT NULL) THEN NULL ELSE FALSE END AS "in_nullified_top1" FROM "s" LEFT OUTER JOIN
         (SELECT DISTINCT CASE WHEN (1 = 1) THEN NULL ELSE "i"."city" END AS "city", "i"."pn" AS "pn" FROM
         (SELECT "p"."city", ROW_NUMBER() OVER(PARTITION BY "p"."pn" ORDER BY "p"."weight" DESC NULLS FIRST) AS "rn", "p"."pn" FROM "p") AS "i"
         WHERE ("i"."rn" = 1)) AS "GNL" ON (("GNL"."pn" = "s"."pn") AND ("GNL"."city" = 'CONST')) LEFT OUTER JOIN
         (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn" FROM
         (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "i"."city" END AS "city", "i"."pn" AS "pn" FROM
         (SELECT "p"."city", ROW_NUMBER() OVER(PARTITION BY "p"."pn" ORDER BY "p"."weight" DESC NULLS FIRST) AS "rn", "p"."pn" FROM "p") AS "i"
         WHERE ("i"."rn" = 1)) AS "NP_3VL" WHERE ("NP_3VL"."city" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "s"."pn")"#;
    test_it("test140", original_text, expected_text);
}

// WHERE NOT IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test141() {
    // WHERE NOT IN; LHS const; RHS single-table forced NULL via CASE -> NP present
    let original_text = r#"
SELECT spj.sn
FROM spj
WHERE 'Y' NOT IN (
  SELECT CASE WHEN 1 = 1 THEN NULL ELSE p.city END AS city
  FROM p
  WHERE p.pn = spj.pn
);"#;
    let expected_text = r#"SELECT "spj"."sn" FROM "spj" LEFT OUTER JOIN
        (SELECT CASE WHEN (1 = 1) THEN NULL ELSE "p"."city" END AS "city", "p"."pn" AS "pn" FROM "p") AS "GNL"
        ON (("GNL"."pn" = "spj"."pn") AND ("GNL"."city" = 'Y')) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn" FROM
        (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "p"."city" END AS "city", "p"."pn" AS "pn" FROM "p") AS "NP_3VL"
        WHERE ("NP_3VL"."city" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "spj"."pn")
        WHERE (("GNL"."city" IS NULL) AND ("NP_3VL"."present_" IS NULL))"#;
    test_it("test141", original_text, expected_text);
}

// WHERE NOT IN; correlated; 3-VL probes (NP/EP) present; RHS deduplicated (DISTINCT); boolean
// SELECT-list shaped via CASE for 3-VL.
#[test]
fn test142() {
    // WHERE NOT IN; RHS join with subqueries; force NULL via CASE inside -> NP present
    let original_text = r#"
SELECT s.sn
FROM s
WHERE s.city NOT IN (
  SELECT CASE WHEN 1 = 1 THEN NULL ELSE t.city END as city
  FROM (
    SELECT p.city, p.pn FROM p
  ) AS t
  JOIN (
    SELECT spj.pn FROM spj
  ) AS u ON u.pn = t.pn
  WHERE t.pn = s.pn
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
        (SELECT CASE WHEN (1 = 1) THEN NULL ELSE "t"."city" END AS "city", "t"."pn" AS "pn" FROM
        (SELECT "p"."city", "p"."pn" FROM "p") AS "t" JOIN (SELECT "spj"."pn" FROM "spj") AS "u" ON ("u"."pn" = "t"."pn")) AS "GNL"
        ON (("GNL"."pn" = "s"."pn") AND ("s"."city" = "GNL"."city")) LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_", "NP_3VL"."pn" AS "pn" FROM
        (SELECT 1 AS "present_", CASE WHEN (1 = 1) THEN NULL ELSE "t"."city" END AS "city", "t"."pn" AS "pn" FROM
        (SELECT "p"."city", "p"."pn" FROM "p") AS "t" JOIN (SELECT "spj"."pn" FROM "spj") AS "u" ON ("u"."pn" = "t"."pn")) AS "NP_3VL"
        WHERE ("NP_3VL"."city" IS NULL)) AS "NP_3VL" ON ("NP_3VL"."pn" = "s"."pn") LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."pn" AS "pn" FROM (SELECT "p"."city", "p"."pn" FROM "p") AS "t" JOIN
        (SELECT "spj"."pn" FROM "spj") AS "u" ON ("u"."pn" = "t"."pn")) AS "EP_3VL" ON ("EP_3VL"."pn" = "s"."pn")
        WHERE (("GNL"."city" IS NULL) AND ((("s"."city" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test142", original_text, expected_text);
}

// Empty RHS + LEFT join w/ EXISTS mirror:
// Expect: all s.sn (RHS empty).
#[test]
fn test143() {
    let original_text = r#"
        SELECT s.sn
        FROM s LEFT JOIN LATERAL (
          SELECT 1 AS present_ FROM spj WHERE spj.sn = s.sn AND false
        ) gnl ON true
        WHERE gnl.present_ IS NULL;"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT JOIN (SELECT 1 AS "present_", "spj"."sn" AS "sn"
        FROM "spj" WHERE FALSE) AS "gnl" ON ("gnl"."sn" = "s"."sn") WHERE ("gnl"."present_" IS NULL)"#;
    test_it("test143", original_text, expected_text);
}

// NOT IN with NULLs inside subquery:
// Expect: PostgreSQL 3-VL behavior — if any spj.qty IS NULL and s.sn overlaps, result may be empty for those keys.
#[test]
fn test144() {
    let original_text = r#"
        SELECT s.sn
        FROM s
        WHERE s.sn NOT IN (SELECT spj.sn FROM spj WHERE spj.qty IS NULL);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN (SELECT "spj"."sn" AS "sn" FROM "spj"
        WHERE ("spj"."qty" IS NULL)) AS "GNL" ON ("s"."sn" = "GNL"."sn") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT 1 AS "present_", "spj"."sn" AS "sn" FROM "spj"
        WHERE ("spj"."qty" IS NULL)) AS "NP_3VL" WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL"  LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj" WHERE ("spj"."qty" IS NULL)) AS "EP_3VL"  WHERE
        (("GNL"."sn" IS NULL)
        AND ((("s"."sn" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test144", original_text, expected_text);
}

// Multiple LHS candidates; closest wins:
// Correlate c.k to b.k (closest preceding) in the rewritten ON.
// expression subquery; correlated; LATERAL subquery flattened.
#[test]
fn test145() {
    let original_text = r#"
        SELECT a.l FROM a
        JOIN b ON a.k = b.k
        JOIN LATERAL (SELECT 1 FROM c WHERE c.k = a.k and c.m = b.k) g ON true;"#;
    let expected_text = r#"SELECT "a"."l" FROM "a" JOIN "b" ON ("a"."k" = "b"."k") INNER JOIN
        (SELECT 1, "c"."m" AS "m", "c"."k" AS "k" FROM "c") AS "g" ON ("g"."m" = "b"."k")
        WHERE ("g"."k" = "a"."k")"#;
    test_it("test145", original_text, expected_text);
}

// Top-K with correlation:
// Ensure the rewrite partitions by city and binds correctly.
#[test]
fn test146() {
    let original_text = r#"
        SELECT p.city
        FROM p
        WHERE p.pn IN (
          SELECT t.pn
          FROM s t
          WHERE t.city = p.city
          ORDER BY t.weight DESC
          LIMIT 3
        );"#;
    let expected_text = r#" SELECT "p"."city" FROM "p" INNER JOIN
        (SELECT DISTINCT "INNER"."pn" AS "pn", "INNER"."city" AS "city" FROM
        (SELECT "t"."pn" AS "pn", "t"."city" AS "city",
        ROW_NUMBER() OVER(PARTITION BY "t"."city" ORDER BY "t"."weight" DESC NULLS FIRST) AS "__rn" FROM "s" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "GNL" ON (("GNL"."city" = "p"."city") AND ("p"."pn" = "GNL"."pn"))"#;
    test_it("test146", original_text, expected_text);
}

// Expected: ON keeps GNL.pn = p.pn; WHERE has GNL.sn = s.sn;
// Crucially, the derived table must project present_, pn, sn.
#[test]
fn test147() {
    let original = r#"
select s.sn
from s, p
where exists (
  select 1
  from spj t
  where t.pn = p.pn and t.sn = s.sn
);"#;
    let expected = r#"SELECT "s"."sn" FROM "s", "p" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."sn" AS "sn", "t"."pn" AS "pn" FROM "spj" AS "t") AS "GNL"
        ON ("GNL"."pn" = "p"."pn")
        WHERE ("GNL"."sn" = "s"."sn")"#;
    test_it("test147", original, expected);
}

// Closest-LHS selection is deterministic.
// ON must bind RHS to the closest LHS: here that's `p`.
// So ON keeps `t.pn = p.pn` and moves `t.sn = s.sn` to WHERE.
#[test]
fn test148() {
    let original_text = r#"
select s.sname, p.pname
from s
join p on true
where exists (select 1 from spj t where t.sn = s.sn and t.pn = p.pn);
"#;
    let expected_text = r#"SELECT "s"."sname", "p"."pname" FROM "s" JOIN "p" ON TRUE INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."pn" AS "pn", "t"."sn" AS "sn" FROM "spj" AS "t") AS "GNL"
        ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."sn" = "s"."sn")"#;
    test_it("test148", original_text, expected_text);
}

// Expect EP/NP 3-VL shaping with probes and a guard;
// WHERE NOT IN; correlated; rewritten as LEFT OUTER JOIN; anti-semi via IS NULL guard; 3-VL
// probes (NP/EP) present; RHS deduplicated (DISTINCT).
#[test]
fn test149() {
    let original_text = r#"
select s.sn
from s
where s.sn not in (select t.sn from spj t where t.qty is null or t.qty > 0);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN (SELECT "t"."sn" AS "sn" FROM "spj" AS "t"
        WHERE (("t"."qty" IS NULL) OR ("t"."qty" > 0))) AS "GNL" ON ("s"."sn" = "GNL"."sn") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM (SELECT 1 AS "present_", "t"."sn" AS "sn" FROM "spj" AS "t"
        WHERE (("t"."qty" IS NULL) OR ("t"."qty" > 0))) AS "NP_3VL" WHERE ("NP_3VL"."sn" IS NULL)) AS "NP_3VL"  LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj" AS "t" WHERE (("t"."qty" IS NULL) OR ("t"."qty" > 0))) AS "EP_3VL" WHERE
        (("GNL"."sn" IS NULL) AND ((("s"."sn" IS NOT NULL) AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test149", original_text, expected_text);
}

// Expect partitioned TOP-K rewrite by t.sn before join.
// expression subquery; correlated; LATERAL subquery flattened; with ORDER BY/LIMIT;
// materialized via window ROW_NUMBER/DENSE_RANK; rewritten as INNER JOIN.
#[test]
fn test150() {
    let original_text = r#"
select s.sn
from s
join lateral (
  select t.pn
  from p t
  where t.sn = s.sn
  order by t.weight desc
  limit 3
) dt on true;
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN (SELECT "INNER"."pn" AS "pn", "INNER"."sn" AS "sn"
        FROM (SELECT "t"."pn" AS "pn", ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."weight" DESC NULLS FIRST) AS "__rn",
        "t"."sn" AS "sn" FROM "p" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 3)) AS "dt" ON ("dt"."sn" = "s"."sn")"#;
    test_it("test150", original_text, expected_text);
}

// WHERE EXISTS; correlated; rewritten as INNER JOIN; RHS deduplicated (DISTINCT).
#[test]
fn test151() {
    let original_text = r#"
select s.sn
from s
where exists (select 1 from spj t where t.sn = s.sn and t.qty > 10);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN (SELECT DISTINCT 1 AS "present_", "t"."sn" AS "sn"
         FROM "spj" AS "t" WHERE ("t"."qty" > 10)) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test151", original_text, expected_text);
}

// WHERE, correlated scalar with ORDER BY … LIMIT 1 (non-aggregated):
// Expect RN-partitioned TOP-1 materialized and compared in outer WHERE.
#[test]
fn test152() {
    let original_text = r#"
SELECT p.pn
FROM s AS p
WHERE (
  SELECT i.weight
  FROM p AS i
  WHERE i.pn = p.pn
  ORDER BY i.weight DESC
  LIMIT 1
) >= 50;"#;

    let expected_text = r#"SELECT "p"."pn" FROM "s" AS "p" INNER JOIN
    (SELECT "INNER"."weight" AS "weight", "INNER"."pn" AS "pn" FROM
    (SELECT "i"."weight" AS "weight", "i"."pn" AS "pn",
    ROW_NUMBER() OVER(PARTITION BY "i"."pn" ORDER BY "i"."weight" DESC NULLS FIRST) AS "__rn"
    FROM "p" AS "i") AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "GNL"
    ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."weight" >= 50)"#;

    test_it("test152", original_text, expected_text);
}

// SELECT-list, correlated scalar with ORDER BY … LIMIT 1 (non-aggregated):
// Expect LEFT OUTER JOIN of RN-partitioned TOP-1 and projection of the scalar.
#[test]
fn test153() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT t.qty
    FROM spj AS t
    WHERE t.sn = s.sn
    ORDER BY t.qty DESC
    LIMIT 1
  ) AS top_qty
FROM s AS s;
"#;

    let expected_text = r#"SELECT "s"."sn", "GNL"."qty" AS "top_qty" FROM "s" AS "s" LEFT OUTER JOIN
        (SELECT "INNER"."qty" AS "qty", "INNER"."sn" AS "sn" FROM (SELECT "t"."qty" AS "qty", "t"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn" FROM "spj" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "GNL" ON ("GNL"."sn" = "s"."sn");"#;
    test_it("test153", original_text, expected_text);
}

// WHERE, correlated scalar, LIMIT 1 buried under two projecting wrappers,
// with GROUP BY in the core and a window in the outer wrapper.
#[test]
fn test154() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE (
  SELECT z1.sm
  FROM (
    SELECT z0.sm, z0.sn, SUM(z0.sm) OVER() AS tot
    FROM (
      SELECT SUM(t.qty) AS sm, t.sn AS sn
      FROM spj AS t
      WHERE t.sn = s.sn
      GROUP BY t.jn, t.sn
      ORDER BY sm DESC
      LIMIT 1
    ) AS z0
  ) AS z1
) >= 100;"#;

    // EXPECT: materialize TOP-1 per sn via RN<=1 (partition by sn, order by sm DESC),
    // then inner-join that per-sn top row and apply sm >= 100.
    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "z1"."sm" AS "sm", "z1"."sn" AS "sn" FROM
        (SELECT "z0"."sm", "z0"."sn", sum("z0"."sm") OVER(PARTITION BY "z0"."sn") AS "tot" FROM
        (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn" FROM
        (SELECT sum("t"."qty") AS "sm", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "z0") AS "z1") AS "GNL" ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."sm" >= 100)"#;

    test_it("test154", original_text, expected_text);
}

// SELECT-list, correlated scalar, LIMIT 1 under a projecting wrapper that also
// adds a window function; expect LEFT OUTER JOIN with RN<=1 partitioned by jn.
#[test]
fn test155() {
    let original_text = r#"
SELECT
  j.jn,
  (
    SELECT u.mx
    FROM (
      SELECT t.jn, MAX(t.qty) AS mx
      FROM spj AS t
      WHERE t.jn = j.jn
      GROUP BY t.jn
      ORDER BY mx DESC
      LIMIT 1
    ) AS u
  ) AS top_qty
FROM j AS j;"#;

    let expected_text = r#"SELECT "j"."jn", "GNL"."mx" AS "top_qty" FROM "j" AS "j" LEFT OUTER JOIN
        (SELECT "u"."mx" AS "mx", "u"."jn" AS "jn" FROM (SELECT "INNER"."jn" AS "jn", "INNER"."mx" AS "mx" FROM
        (SELECT "INNER"."jn" AS "jn", "INNER"."mx" AS "mx",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."jn" ORDER BY "INNER"."mx" DESC NULLS FIRST) AS "__rn" FROM
        (SELECT "t"."jn" AS "jn", max("t"."qty") AS "mx" FROM "spj" AS "t" GROUP BY "t"."jn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "u") AS "GNL" ON ("GNL"."jn" = "j"."jn")"#;

    test_it("test155", original_text, expected_text);
}

// WHERE, uncorrelated scalar, LIMIT 1 under two wrappers with a window;
// expect global RN<=1 (no partition) and filter by the scalar comparison.
#[test]
fn test156() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE (
  SELECT w.weight
  FROM (
    SELECT i.weight, ROW_NUMBER() OVER(ORDER BY i.weight DESC) AS r
    FROM (
      SELECT p.weight
      FROM p AS p
      ORDER BY p.weight DESC
      LIMIT 1
    ) AS i
  ) AS w
) >= 75;"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "w"."weight" AS "weight" FROM
        (SELECT "i"."weight", ROW_NUMBER() OVER(ORDER BY "i"."weight" DESC NULLS FIRST) AS "r" FROM
        (SELECT "INNER"."weight" AS "weight" FROM (SELECT "p"."weight" AS "weight",
        ROW_NUMBER() OVER(ORDER BY "p"."weight" DESC NULLS FIRST) AS "__rn" FROM "p" AS "p") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "i") AS "w") AS "GNL"  WHERE ("GNL"."weight" >= 75)"#;

    test_it("test156", original_text, expected_text);
}

// WHERE, IN ... LIMIT 1 wrapped once; uncorrelated.
// With the new relaxation, IN (… LIMIT 1) is treated as scalar "=" in WHERE,
// then TOP-1 is materialized via global RN<=1.
#[test]
fn test157() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE s.pn IN (
  SELECT x.pn
  FROM (
    SELECT p.pn
    FROM p AS p
    ORDER BY p.weight DESC
    LIMIT 1
  ) AS x
);"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "x"."pn" AS "pn" FROM
        (SELECT "INNER"."pn" AS "pn" FROM (SELECT "p"."pn" AS "pn",
        ROW_NUMBER() OVER(ORDER BY "p"."weight" DESC NULLS FIRST) AS "__rn" FROM "p" AS "p") AS "INNER" WHERE
        ("INNER"."__rn" <= 1)) AS "x") AS "GNL" ON ("s"."pn" = "GNL"."pn")"#;

    test_it("test157", original_text, expected_text);
}

// WHERE, correlated IN ... LIMIT 1 → scalar "=" (correlated variant).
// Expect RN-partitioned TOP-1 by sn and INNER JOIN on equality.
#[test]
fn test158() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE s.sn IN (
  SELECT x.sn
  FROM (
    SELECT t.sn
    FROM spj AS t
    WHERE t.sn = s.sn
    ORDER BY t.qty DESC
    LIMIT 1
  ) AS x
);"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "x"."sn" AS "sn" FROM
        (SELECT "INNER"."sn" AS "sn" FROM (SELECT "t"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn" FROM "spj" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "x") AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;

    test_it("test158", original_text, expected_text);
}

// WHERE, correlated scalar with DISTINCT + ORDER BY ... LIMIT 1.
// DISTINCT must be dropped (redundant under RN <= 1).
#[test]
fn test159() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE (
  SELECT DISTINCT t.qty
  FROM spj AS t
  WHERE t.sn = s.sn
  ORDER BY t.qty DESC
  LIMIT 1
) >= 10;"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN
        (SELECT "INNER"."qty" AS "qty", "INNER"."sn" AS "sn" FROM (SELECT "t"."qty" AS "qty", "t"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "t"."sn" ORDER BY "t"."qty" DESC NULLS FIRST) AS "__rn" FROM "spj" AS "t") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "GNL" ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."qty" >= 10)"#;

    test_it("test159", original_text, expected_text);
}

// WHERE, correlated scalar over grouped inner with ORDER BY … LIMIT 1:
// Inner groups by job and orders by SUM; LIMIT 1 is materialized via RN.
#[test]
fn test160() {
    let original_text = r#"
SELECT s.sn
FROM s AS s
WHERE (
  SELECT z.sm
  FROM (
    SELECT SUM(t.qty) AS sm
    FROM spj AS t
    WHERE t.sn = s.sn
    GROUP BY t.jn
    ORDER BY sm DESC
    LIMIT 1
  ) AS z
) >= 100;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "z"."sm" AS "sm", "z"."sn" AS "sn"
        FROM (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn" FROM (SELECT "INNER"."sm" AS "sm", "INNER"."sn" AS "sn",
        ROW_NUMBER() OVER(PARTITION BY "INNER"."sn" ORDER BY "INNER"."sm" DESC NULLS FIRST) AS "__rn" FROM
        (SELECT sum("t"."qty") AS "sm", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."jn", "t"."sn") AS "INNER") AS "INNER"
        WHERE ("INNER"."__rn" <= 1)) AS "z") AS "GNL" ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."sm" >= 100)"#;

    test_it("test160", original_text, expected_text);
}

// WHERE, correlated scalar with ORDER BY … LIMIT 1 OFFSET 0 (explicit offset accepted):
// Expect identical RN-partitioned TOP-1 materialization (OFFSET 0 treated as single-row).
#[test]
fn test161() {
    let original_text = r#"
SELECT p.pn
FROM s AS p
WHERE (
  SELECT i.weight
  FROM p AS i
  WHERE i.pn = p.pn
  ORDER BY i.weight DESC
  LIMIT 1 OFFSET 0
) >= 75;"#;

    let expected_text = r#"SELECT "p"."pn" FROM "s" AS "p" INNER JOIN
    (SELECT "INNER"."weight" AS "weight", "INNER"."pn" AS "pn" FROM
    (SELECT "i"."weight" AS "weight", "i"."pn" AS "pn",
    ROW_NUMBER() OVER(PARTITION BY "i"."pn" ORDER BY "i"."weight" DESC NULLS FIRST) AS "__rn"
    FROM "p" AS "i") AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "GNL"
    ON ("GNL"."pn" = "p"."pn") WHERE ("GNL"."weight" >= 75)"#;

    test_it("test161", original_text, expected_text);
}

// LIMIT 0 — WHERE EXISTS → FALSE
#[test]
fn test162() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE EXISTS (
            SELECT 1 FROM "spj" AS "t"
            LIMIT 0
        )
    "#;

    // Early short-circuit: EXISTS(empty) ⇒ FALSE
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE FALSE
    "#;

    test_it("test162", original_text, expected_text);
}

// LIMIT 0 — WHERE NOT EXISTS → TRUE (drop conjunct)
#[test]
fn test163() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE NOT EXISTS (
            SELECT 1 FROM "spj" AS "t"
            LIMIT 0
        )
    "#;

    // Early short-circuit: NOT EXISTS(empty) ⇒ TRUE ⇒ WHERE disappears
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
    "#;

    test_it("test163", original_text, expected_text);
}

// LIMIT 0 — WHERE IN (… LIMIT 0) → FALSE
#[test]
fn test164() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE "s"."pn" IN (
            SELECT "t"."pn" FROM "spj" AS "t"
            LIMIT 0
        )
    "#;

    // Early short-circuit: IN(empty) ⇒ FALSE
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE FALSE
    "#;

    test_it("test164", original_text, expected_text);
}

// LIMIT 0 — WHERE NOT IN (… LIMIT 0) → TRUE (drop conjunct)
#[test]
fn test165() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE "s"."pn" NOT IN (
            SELECT "t"."pn" FROM "spj" AS "t"
            LIMIT 0
        )
    "#;

    // Early short-circuit: NOT IN(empty) ⇒ TRUE ⇒ WHERE disappears
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
    "#;

    test_it("test165", original_text, expected_text);
}

// LIMIT 0 under a simple projecting wrapper — WHERE IN → FALSE
#[test]
fn test166() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE "s"."pn" IN (
            SELECT "u"."pn" FROM (
                SELECT "t"."pn"
                FROM "spj" AS "t"
                LIMIT 0
            ) AS "u"
        )
    "#;

    // has_limit_zero_deep descends through the wrapper; still empty ⇒ FALSE
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE FALSE
    "#;

    test_it("test166", original_text, expected_text);
}

// LIMIT 0 — SELECT-list IN (… LIMIT 0) → literal FALSE
#[test]
fn test167() {
    let original_text = r#"
        SELECT
          "s"."sn",
          ("s"."pn" IN (SELECT "t"."pn" FROM "spj" AS "t" LIMIT 0)) AS "has_part"
        FROM "s" AS "s"
    "#;

    // Early short-circuit in SELECT list: IN(empty) ⇒ FALSE
    let expected_text = r#"
        SELECT
          "s"."sn",
          FALSE AS "has_part"
        FROM "s" AS "s"
    "#;

    test_it("test167", original_text, expected_text);
}

// LIMIT 0 — SELECT-list NOT IN (… LIMIT 0) → literal TRUE
#[test]
fn test168() {
    let original_text = r#"
        SELECT
          "s"."sn",
          ("s"."pn" NOT IN (SELECT "t"."pn" FROM "spj" AS "t" LIMIT 0)) AS "none"
        FROM "s" AS "s"
    "#;

    // Early short-circuit in SELECT list: NOT IN(empty) ⇒ TRUE
    let expected_text = r#"
        SELECT
          "s"."sn",
          TRUE AS "none"
        FROM "s" AS "s"
    "#;

    test_it("test168", original_text, expected_text);
}

// LIMIT 0 — SELECT-list EXISTS / NOT EXISTS → FALSE / TRUE
#[test]
fn test169() {
    let original_text = r#"
        SELECT
          EXISTS (SELECT 1 FROM "spj" AS "t" LIMIT 0) AS "e",
          NOT EXISTS (SELECT 1 FROM "spj" AS "t" LIMIT 0) AS "ne"
          FROM s
    "#;

    // Early short-circuit in SELECT list: EXISTS(empty) ⇒ FALSE; NOT EXISTS(empty) ⇒ TRUE
    let expected_text = r#"
        SELECT
          FALSE AS "e",
          TRUE AS "ne"
          FROM s
    "#;

    test_it("test169", original_text, expected_text);
}

// LIMIT 0 — Scalar subquery in WHERE: yields NULL ⇒ WHERE filters out (FALSE)
#[test]
fn test170() {
    let original_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE (
          SELECT "t"."qty" FROM "spj" AS "t" LIMIT 0
        ) >= 10
    "#;

    // Early short-circuit: scalar with zero rows in WHERE ⇒ NULL ⇒ treated as FALSE
    let expected_text = r#"
        SELECT "s"."sn"
        FROM "s" AS "s"
        WHERE FALSE
    "#;

    test_it("test170", original_text, expected_text);
}

// LIMIT 0 — Scalar subquery in SELECT list: yields NULL literal
#[test]
fn test171() {
    let original_text = r#"
        SELECT
          (SELECT "t"."qty" FROM "spj" AS "t" LIMIT 0) AS "mx"
        from s
    "#;

    // Early short-circuit: scalar with zero rows in SELECT list ⇒ NULL
    let expected_text = r#"
        SELECT
          NULL AS "mx"
        FROM s
    "#;

    test_it("test171", original_text, expected_text);
}

// LATERAL + LIMIT 0 — uncorrelated; INNER join ⇒ empty result set (RHS forced empty via WHERE FALSE).
#[test]
fn test172() {
    let original_text = r#"
SELECT s.sn
FROM s
INNER JOIN LATERAL (
  SELECT 1 AS k
  FROM p
  LIMIT 0
) AS x ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN (SELECT 1 AS "k" FROM "p" WHERE FALSE) AS "x" ON TRUE"#;

    test_it("test172", original_text, expected_text);
}

// LATERAL + LIMIT 0 — uncorrelated; LEFT join preserves LHS rows; RHS columns become NULLs.
#[test]
fn test173() {
    let original_text = r#"
SELECT s.sn, x.k
FROM s
LEFT JOIN LATERAL (
  SELECT 1 AS k
  FROM p
  LIMIT 0
) AS x ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn", "x"."k" FROM "s" LEFT JOIN
    (SELECT 1 AS "k" FROM "p" WHERE FALSE) AS "x"
    ON TRUE"#;

    test_it("test173", original_text, expected_text);
}

// LATERAL + LIMIT 0 — correlated to two preceding items; “closest LHS wins” still reflected in ON,
// the other equality remains in WHERE (INNER-safe) even though RHS is empty.
#[test]
fn test174() {
    let original_text = r#"
SELECT a.l
FROM a
JOIN b ON a.k = b.k
INNER JOIN LATERAL (
  SELECT 1, c.k, c.m
  FROM c
  WHERE c.k = a.k AND c.m = b.k
  LIMIT 0
) AS g ON TRUE;
"#;

    let expected_text = r#"SELECT "a"."l" FROM "a" JOIN "b" ON ("a"."k" = "b"."k") INNER JOIN
        (SELECT 1, "c"."k", "c"."m" FROM "c" WHERE FALSE) AS "g" ON TRUE"#;

    test_it("test174", original_text, expected_text);
}

// LATERAL + LIMIT 0 — correlated to the immediately preceding item inside a larger join tree;
// LEFT LATERAL keeps all (a ⋈ b) rows with NULL-extended RHS.
#[test]
fn test175() {
    let original_text = r#"
SELECT a.l
FROM a
JOIN b ON a.k = b.k
LEFT JOIN LATERAL (
  SELECT t.pn
  FROM p AS t
  WHERE t.k = b.k
  LIMIT 0
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "a"."l" FROM "a" JOIN "b" ON ("a"."k" = "b"."k") LEFT JOIN
        (SELECT "t"."pn" FROM "p" AS "t" WHERE FALSE) AS "dt" ON TRUE"#;

    test_it("test175", original_text, expected_text);
}

// LATERAL + LIMIT 0 — uncorrelated with ORDER BY present; ORDER/OFFSET cleared and WHERE FALSE injected;
// LEFT join with EXISTS-like sentinel; WHERE keeps the IS NULL filter on the sentinel.
#[test]
fn test176() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN LATERAL (
  SELECT 1 AS present_
  FROM p AS t
  ORDER BY t.weight DESC
  LIMIT 0
) AS gnl ON TRUE
WHERE gnl.present_ IS NULL;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT JOIN
    (SELECT 1 AS "present_" FROM "p" AS "t" WHERE FALSE) AS "gnl"
    ON TRUE WHERE ("gnl"."present_" IS NULL)"#;

    test_it("test176", original_text, expected_text);
}

// LATERAL + LIMIT 0 under a projecting wrapper — UNCORRELATED; INNER join ⇒ empty RHS.
#[test]
fn test177() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN LATERAL (
  SELECT y.pn
  FROM (
    SELECT p.pn
    FROM p AS p
    LIMIT 0
  ) AS y
) AS dt ON TRUE;
"#;

    // Expect: WHERE FALSE injected into innermost child; ON TRUE remains (uncorrelated).
    let expected_text = r#"SELECT "s"."sn" FROM "s" JOIN
        (SELECT "y"."pn" FROM (SELECT "p"."pn" FROM "p" AS "p" WHERE FALSE) AS "y") AS "dt" ON TRUE"#;

    test_it("test177", original_text, expected_text);
}

// LATERAL + LIMIT 0 under a projecting wrapper — CORRELATED; LEFT join preserves LHS with NULL RHS.
#[test]
fn test178() {
    let original_text = r#"
SELECT s.sn, dt.pn
FROM s
LEFT JOIN LATERAL (
  SELECT y.pn, y.sn
  FROM (
    SELECT t.pn, t.sn
    FROM p AS t
    LIMIT 0
  ) AS y
  WHERE y.sn = s.sn
) AS dt ON TRUE;
"#;

    // Expect: WHERE FALSE in the innermost child; ON binds dt.sn = s.sn.
    let expected_text = r#"SELECT "s"."sn", "dt"."pn" FROM "s" LEFT JOIN (SELECT "y"."pn", "y"."sn" FROM
        (SELECT "t"."pn", "t"."sn" FROM "p" AS "t" WHERE FALSE) AS "y") AS "dt" ON ("dt"."sn" = "s"."sn")"#;

    test_it("test178", original_text, expected_text);
}

// LATERAL nested under another derived table — CORRELATED; INNER path collapses overall to empty.
#[test]
fn test179() {
    let original_text = r#"
SELECT s2.sn
FROM s AS s2
JOIN (
  SELECT s1.sn
  FROM s AS s1
  JOIN LATERAL (
    SELECT 1 AS present_
    FROM spj AS t
    WHERE t.sn = s1.sn
    LIMIT 0
  ) AS g ON TRUE
) AS sub ON sub.sn = s2.sn;
"#;

    // Expect: inner LATERAL forced empty via WHERE FALSE; ON keeps g.sn = s1.sn; top JOIN matches style.
    let expected_text = r#"SELECT "s2"."sn" FROM "s" AS "s2" JOIN (SELECT "s1"."sn" FROM "s" AS "s1" JOIN
        (SELECT 1 AS "present_" FROM "spj" AS "t" WHERE FALSE) AS "g" ON TRUE) AS "sub" ON ("sub"."sn" = "s2"."sn")"#;

    test_it("test179", original_text, expected_text);
}

// LATERAL nested under another derived table — UNCORRELATED; LEFT path preserves LHS rows.
#[test]
fn test180() {
    let original_text = r#"
SELECT s2.sn
FROM s AS s2
LEFT JOIN (
  SELECT s1.sn
  FROM s AS s1
  LEFT JOIN LATERAL (
    SELECT 1 AS present_
    FROM p
    LIMIT 0
  ) AS g ON TRUE
) AS sub ON sub.sn = s2.sn;
"#;

    // Expect: uncorrelated LATERAL forced empty; LEFTs preserved; outer rows kept, RHS NULL-extended.
    let expected_text = r#"SELECT "s2"."sn" FROM "s" AS "s2" LEFT JOIN
    (SELECT "s1"."sn" FROM "s" AS "s1" LEFT JOIN
      (SELECT 1 AS "present_" FROM "p" WHERE FALSE) AS "g" ON TRUE
    ) AS "sub"
    ON ("sub"."sn" = "s2"."sn")"#;

    test_it("test180", original_text, expected_text);
}

// LATERAL + ORDER BY … LIMIT 0 — CORRELATED Top-K form;
// Expect ORDER removed and WHERE FALSE injected; INNER join yields no rows.
#[test]
fn test181() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN LATERAL (
  SELECT t.pn
  FROM p AS t
  WHERE t.sn = s.sn
  ORDER BY t.weight DESC
  LIMIT 0
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" JOIN (SELECT "t"."pn" FROM "p" AS "t" WHERE FALSE) AS "dt" ON TRUE"#;

    test_it("test181", original_text, expected_text);
}

// LATERAL + ORDER BY … OFFSET … LIMIT 0 — UNCORRELATED;
// Expect ORDER/OFFSET cleared, WHERE FALSE injected; LEFT keeps all LHS rows; sentinel used in WHERE.
#[test]
fn test182() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN LATERAL (
  SELECT 1 AS present_
  FROM p AS t
  ORDER BY t.weight DESC
  OFFSET 5
  LIMIT 0
) AS gnl ON TRUE
WHERE gnl.present_ IS NULL;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT JOIN
    (SELECT 1 AS "present_" FROM "p" AS "t" WHERE FALSE) AS "gnl"
    ON TRUE WHERE ("gnl"."present_" IS NULL)"#;

    test_it("test182", original_text, expected_text);
}

// LATERAL + LIMIT 0 — MINIMAL PROJECTION (extra columns in body are dropped);
// Expect only join key(s) and sentinel to be projected from RHS.
#[test]
fn test183() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN LATERAL (
  SELECT t.sn, t.pn, t.qty, 1 AS present_
  FROM spj AS t
  WHERE t.sn = s.sn
  LIMIT 0
) AS gnl ON TRUE
WHERE gnl.present_ IS NULL;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT JOIN
        (SELECT "t"."sn", "t"."pn", "t"."qty", 1 AS "present_" FROM "spj" AS "t" WHERE FALSE) AS "gnl" ON TRUE
        WHERE ("gnl"."present_" IS NULL)"#;

    test_it("test183", original_text, expected_text);
}

// LATERAL nested inside another derived table — MINIMAL PROJECTION + LIMIT 0;
// INNER outer join; inner LEFT LATERAL is empty, filtered by sentinel IS NULL.
#[test]
fn test184() {
    let original_text = r#"
SELECT s2.sn
FROM s AS s2
JOIN (
  SELECT s1.sn
  FROM s AS s1
  LEFT JOIN LATERAL (
    SELECT t.sn, t.qty, 1 AS present_
    FROM spj AS t
    WHERE t.sn = s1.sn
    LIMIT 0
  ) AS g ON TRUE
  WHERE g.present_ IS NULL
) AS sub ON sub.sn = s2.sn;
"#;

    let expected_text = r#"SELECT "s2"."sn" FROM "s" AS "s2" JOIN (SELECT "s1"."sn" FROM "s" AS "s1" LEFT JOIN
        (SELECT "t"."sn", "t"."qty", 1 AS "present_" FROM "spj" AS "t" WHERE FALSE) AS "g" ON TRUE
        WHERE ("g"."present_" IS NULL)) AS "sub" ON ("sub"."sn" = "s2"."sn")"#;

    test_it("test184", original_text, expected_text);
}

// LATERAL + nested LIMIT 0 under TWO projecting wrappers — CORRELATED (LEFT):
// Expect WHERE FALSE at the *innermost* level; wrapper correlation survives and
// is used to build ON ("dt"."sn" = "s"."sn").
#[test]
fn test185() {
    let original_text = r#"
SELECT s.sn, dt.pn
FROM s
LEFT JOIN LATERAL (
  SELECT z.pn, z.sn
  FROM (
    SELECT y.pn, y.sn
    FROM (
      SELECT t.pn, t.sn
      FROM p AS t
      LIMIT 0
    ) AS y
  ) AS z
  WHERE z.sn = s.sn
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn", "dt"."pn" FROM "s" LEFT JOIN
    (SELECT "z"."pn", "z"."sn" FROM
      (SELECT "y"."pn", "y"."sn" FROM
        (SELECT "t"."pn", "t"."sn" FROM "p" AS "t" WHERE FALSE) AS "y"
      ) AS "z"
    ) AS "dt"
    ON ("dt"."sn" = "s"."sn")"#;

    test_it("test185", original_text, expected_text);
}

// LATERAL + nested LIMIT 0 under TWO projecting wrappers — UNCORRELATED (INNER):
// Expect WHERE FALSE at the *innermost* level; ON TRUE remains (no correlation).
#[test]
fn test186() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN LATERAL (
  SELECT z.pn
  FROM (
    SELECT y.pn
    FROM (
      SELECT p.pn
      FROM p
      LIMIT 0
    ) AS y
  ) AS z
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" JOIN
    (SELECT "z"."pn" FROM
      (SELECT "y"."pn" FROM
        (SELECT "p"."pn" FROM "p" WHERE FALSE) AS "y"
      ) AS "z"
    ) AS "dt" ON TRUE"#;

    test_it("test186", original_text, expected_text);
}

// LATERAL + LIMIT 0 nested one level; inner had ORDER BY … LIMIT 0 — CORRELATED (INNER):
// Expect ORDER removed and WHERE FALSE at the *inner* level; ON equality retained.
#[test]
fn test187() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN LATERAL (
  SELECT y.pn, y.sn
  FROM (
    SELECT t.pn, t.sn
    FROM p AS t
    WHERE t.sn = s.sn
    ORDER BY t.weight DESC
    LIMIT 0
  ) AS y
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn" FROM "s" JOIN (SELECT "y"."pn", "y"."sn" FROM
        (SELECT "t"."pn", "t"."sn" FROM "p" AS "t" WHERE FALSE) AS "y") AS "dt" ON TRUE"#;

    test_it("test187", original_text, expected_text);
}

// LATERAL + LIMIT 0 nested TWO levels with correlation at the outer wrapper — LEFT:
// Expect deepest SELECT gets WHERE FALSE; outer wrapper still exposes the join key so
// ON ("dt"."sn" = "s"."sn") is constructed, preserving NULL extension on RHS.
#[test]
fn test188() {
    let original_text = r#"
SELECT s.sn, dt.pn
FROM s
LEFT JOIN LATERAL (
  SELECT u.pn, u.sn
  FROM (
    SELECT z.pn, z.sn
    FROM (
      SELECT t.pn, t.sn
      FROM p AS t
      LIMIT 0
    ) AS z
  ) AS u
  WHERE u.sn = s.sn
) AS dt ON TRUE;
"#;

    let expected_text = r#"SELECT "s"."sn", "dt"."pn" FROM "s" LEFT JOIN
    (SELECT "u"."pn", "u"."sn" FROM
      (SELECT "z"."pn", "z"."sn" FROM
        (SELECT "t"."pn", "t"."sn" FROM "p" AS "t" WHERE FALSE) AS "z"
      ) AS "u"
    ) AS "dt"
    ON ("dt"."sn" = "s"."sn")"#;

    test_it("test188", original_text, expected_text);
}

#[test]
fn test189() {
    let original_text = r#"
SELECT (
    SELECT T1.Test_INTEGER2 FROM DataTypes,
      LATERAL (SELECT DT.Test_INTEGER2 FROM  DataTypes3 DT WHERE DataTypes.Test_INTEGER = DT.Test_INTEGER2 LIMIT 1) T1 LIMIT 1
    ) T2
FROM s;"#;
    let expected_text = r#"SELECT "GNL"."test_integer2" AS "t2" FROM "s" LEFT OUTER JOIN
        (SELECT "INNER"."test_integer2" AS "test_integer2" FROM (SELECT "t1"."test_integer2" AS "test_integer2",
        ROW_NUMBER() OVER() AS "__rn" FROM "datatypes" INNER JOIN (SELECT "INNER"."test_integer2" AS "test_integer2" FROM
        (SELECT "dt"."test_integer2" AS "test_integer2", ROW_NUMBER() OVER(PARTITION BY "dt"."test_integer2") AS "__rn"
        FROM "datatypes3" AS "dt") AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "t1" ON
        ("datatypes"."test_integer" = "t1"."test_integer2")) AS "INNER" WHERE ("INNER"."__rn" <= 1)) AS "GNL""#;
    test_it("test189", original_text, expected_text);
}

#[test]
fn test190() {
    let original_text = r#"
SELECT s.sname FROM s WHERE s.sn NOT IN (SELECT MAX(spj.sn) OVER() FROM spj)"#;
    let expected_text = r#"SELECT "s"."sname" FROM "s" LEFT OUTER JOIN
        (SELECT max("spj"."sn") OVER() AS "max(""spj"".""sn"") OVER()" FROM "spj") AS "GNL" ON
        ("s"."sn" = "GNL"."max(""spj"".""sn"") OVER()") LEFT OUTER JOIN
        (SELECT DISTINCT "NP_3VL"."present_" AS "present_" FROM
        (SELECT 1 AS "present_", max("spj"."sn") OVER() AS "max(""spj"".""sn"") OVER()" FROM "spj") AS "NP_3VL"
        WHERE ("NP_3VL"."max(""spj"".""sn"") OVER()" IS NULL)) AS "NP_3VL"  LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj") AS "EP_3VL"
        WHERE (("GNL"."max(""spj"".""sn"") OVER()" IS NULL) AND ((("s"."sn" IS NOT NULL)
        AND ("NP_3VL"."present_" IS NULL)) OR ("EP_3VL"."present_" IS NULL)))"#;
    test_it("test190", original_text, expected_text);
}

#[test]
fn test191() {
    let original_text = r#"
SELECT a.RowNum, a.Test_VARCHAR, a.Test_INT
FROM qa.DataTypes a
WHERE EXISTS (SELECT MAX(b.Test_INT) FROM qa.DataTypesNull b
  WHERE b.RowNum = a.RowNum HAVING MAX(b.Test_INT) > 5);
"#;
    let expected_text = r#"SELECT "a"."rownum", "a"."test_varchar", "a"."test_int" FROM "qa"."datatypes" AS "a"
        INNER JOIN (SELECT DISTINCT 1 AS "present_", "b"."rownum" AS "rownum" FROM "qa"."datatypesnull" AS "b"
        GROUP BY "b"."rownum"
        HAVING (max("b"."test_int") > 5)) AS "GNL"
        ON ("GNL"."rownum" = "a"."rownum")"#;
    test_it("test191", original_text, expected_text);
}

#[test]
fn test192() {
    let original_text = r#"
SELECT a.RowNum, a.Test_VARCHAR, a.Test_INT
FROM qa.DataTypes a
WHERE NOT EXISTS (SELECT MAX(b.Test_INT) FROM qa.DataTypesNull b
  WHERE b.RowNum = a.RowNum HAVING MAX(b.Test_INT) > 5);
"#;
    let expected_text = r#"SELECT "a"."rownum", "a"."test_varchar", "a"."test_int" FROM "qa"."datatypes" AS "a"
        LEFT OUTER JOIN (SELECT 1 AS "present_", "b"."rownum" AS "rownum" FROM "qa"."datatypesnull" AS "b" GROUP BY "b"."rownum"
        HAVING (max("b"."test_int") > 5)) AS "GNL"
        ON ("GNL"."rownum" = "a"."rownum")
        WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test192", original_text, expected_text);
}

#[test]
fn test193() {
    let original_text = r#"
SELECT t.RowNum, t.Test_DEC
FROM (SELECT DataTypes.RowNum, SUM(DataTypes.Test_DEC) AS Test_DEC FROM DataTypes GROUP BY DataTypes.RowNum) t
WHERE t.Test_DEC > 100000.00
ORDER BY t.Test_DEC;
"#;
    test_it("test193", original_text, original_text);
}

#[test]
fn test194() {
    let original_text = r#"
SELECT spj.qty
FROM spj
WHERE spj.qty IN (
  SELECT MAX(p.weight)
  FROM p
  WHERE p.pn = spj.pn
  HAVING MAX(p.weight) > 0
);
"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" INNER JOIN
        (SELECT max("p"."weight") AS "max(weight)", "p"."pn" AS "pn" FROM "p" GROUP BY "p"."pn"
        HAVING (max("p"."weight") > 0)) AS "GNL"
        ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."max(weight)"))"#;
    test_it("test194", original_text, expected_text);
}

#[test]
fn test195() {
    let original_text = r#"
SELECT spj.qty
FROM spj
WHERE spj.qty NOT IN (
  SELECT MAX(p.weight)
  FROM p
  WHERE p.pn = spj.pn
  HAVING MAX(p.weight) > 0
);
"#;
    let expected_text = r#"SELECT "spj"."qty" FROM "spj" LEFT OUTER JOIN
        (SELECT max("p"."weight") AS "max(weight)", "p"."pn" AS "pn"
        FROM "p" GROUP BY "p"."pn" HAVING (max("p"."weight") > 0)) AS "GNL"
        ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."max(weight)")) WHERE ("GNL"."max(weight)" IS NULL)"#;
    test_it("test195", original_text, expected_text);
}

#[test]
fn test196() {
    let original_text = r#"
SELECT
  spj.sn,
  (spj.qty IN (
     SELECT MAX(p.weight)
     FROM p
     WHERE p.pn = spj.pn
     HAVING MAX(p.weight) > 0
  )) AS in_mx
FROM spj;"#;
    let expected_text = r#"SELECT "spj"."sn", ("GNL"."max(weight)" IS NOT NULL) AS "in_mx" FROM "spj"
        LEFT OUTER JOIN (SELECT max("p"."weight") AS "max(weight)", "p"."pn" AS "pn" FROM "p" GROUP BY "p"."pn"
        HAVING (max("p"."weight") > 0)) AS "GNL" ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."max(weight)"))"#;
    test_it("test196", original_text, expected_text);
}

#[test]
fn test197() {
    let original_text = r#"
SELECT
  spj.sn,
  (spj.qty NOT IN (
     SELECT MAX(p.weight)
     FROM p
     WHERE p.pn = spj.pn
     HAVING MAX(p.weight) > 0
  )) AS not_in_mx
FROM spj;"#;
    let expected_text = r#"SELECT "spj"."sn", ("GNL"."max(weight)" IS NULL) AS "not_in_mx" FROM "spj"
        LEFT OUTER JOIN (SELECT max("p"."weight") AS "max(weight)", "p"."pn" AS "pn" FROM "p" GROUP BY "p"."pn"
        HAVING (max("p"."weight") > 0)) AS "GNL" ON (("GNL"."pn" = "spj"."pn") AND ("spj"."qty" = "GNL"."max(weight)"))"#;
    test_it("test197", original_text, expected_text);
}

#[test]
fn test198() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE (
  SELECT SUM(t.qty)
  FROM spj AS t
  WHERE t.sn = s.sn
  HAVING SUM(t.qty) > 0
) >= 100;"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN (SELECT sum("t"."qty") AS "sum(qty)",
        "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."sn" HAVING (sum("t"."qty") > 0)) AS "GNL"
        ON ("GNL"."sn" = "s"."sn") WHERE ("GNL"."sum(qty)" >= 100)"#;
    test_it("test198", original_text, expected_text);
}

#[test]
fn test199() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT SUM(t.qty)
    FROM spj AS t
    WHERE t.sn = s.sn
    HAVING SUM(t.qty) > 0
  ) AS total_qty
FROM s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."sum(qty)" AS "total_qty" FROM "s" LEFT OUTER JOIN
        (SELECT sum("t"."qty") AS "sum(qty)", "t"."sn" AS "sn" FROM "spj" AS "t" GROUP BY "t"."sn"
        HAVING (sum("t"."qty") > 0)) AS "GNL" ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test199", original_text, expected_text);
}

#[test]
fn test200() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE EXISTS (
  SELECT COUNT(*)
  FROM spj AS t
  WHERE t.sn = s.sn
  HAVING COUNT(*) > 2
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "t"."sn" AS "sn" FROM "spj" AS "t"
         GROUP BY "t"."sn" HAVING (count(*) > 2)) AS "GNL"
        ON ("GNL"."sn" = "s"."sn")"#;
    test_it("test200", original_text, expected_text);
}

#[test]
fn test201() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE NOT EXISTS (
  SELECT COUNT(*)
  FROM spj AS t
  WHERE t.sn = s.sn
  HAVING COUNT(*) > 2
);"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
        (SELECT 1 AS "present_", "t"."sn" AS "sn" FROM "spj" AS "t"
         GROUP BY "t"."sn" HAVING (count(*) > 2)) AS "GNL"
        ON ("GNL"."sn" = "s"."sn")
        WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test201", original_text, expected_text);
}

// WHERE EXISTS; non-correlated; aggregate-only w/o GROUP BY + HAVING (COUNT(*));
// implement via INNER JOIN to a single-row (or zero-row) derived table; ON TRUE.
#[test]
fn test202() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE EXISTS (
  SELECT COUNT(*)
  FROM spj AS t
  HAVING COUNT(*) > 2
);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj" AS "t" HAVING (count(*) > 2)) AS "GNL""#;
    test_it("test202", original_text, expected_text);
}

// WHERE NOT EXISTS; non-correlated; aggregate-only w/o GROUP BY + HAVING (COUNT(*));
// implement via LEFT OUTER JOIN to a single-row (or zero-row) derived table; guard by IS NULL.
#[test]
fn test203() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE NOT EXISTS (
  SELECT COUNT(*)
  FROM spj AS t
  HAVING COUNT(*) > 1000000
);
"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
        (SELECT DISTINCT 1 AS "present_" FROM "spj" AS "t" HAVING (count(*) > 1000000)) AS "GNL"
        WHERE ("GNL"."present_" IS NULL)"#;
    test_it("test203", original_text, expected_text);
}

// WHERE IN; non-correlated; aggregate-only w/o GROUP BY + HAVING; semi via INNER JOIN on equality.
#[test]
fn test204() {
    let original_text = r#"
SELECT spj.sn
FROM spj
WHERE spj.qty IN (
  SELECT MAX(p.weight)
  FROM p
  HAVING MAX(p.weight) > 0
);
"#;
    let expected_text = r#"SELECT "spj"."sn" FROM "spj" INNER JOIN
        (SELECT max("p"."weight") AS "max(weight)" FROM "p" HAVING (max("p"."weight") > 0)) AS "GNL"
        ON ("spj"."qty" = "GNL"."max(weight)"); "#;
    test_it("test204", original_text, expected_text);
}

// SELECT-list IN; non-correlated; aggregate-only w/o GROUP BY + HAVING; boolean simplifies to IS NOT NULL.
#[test]
fn test205() {
    let original_text = r#"
SELECT
  spj.sn,
  (spj.qty IN (
     SELECT MAX(p.weight)
     FROM p
     HAVING MAX(p.weight) > 0
  )) AS in_mx
FROM spj;"#;
    let expected_text = r#"SELECT "spj"."sn", ("GNL"."max(weight)" IS NOT NULL) AS "in_mx" FROM "spj"
        LEFT OUTER JOIN (SELECT max("p"."weight") AS "max(weight)" FROM "p" HAVING (max("p"."weight") > 0)) AS "GNL"
        ON ("spj"."qty" = "GNL"."max(weight)")"#;
    test_it("test205", original_text, expected_text);
}

// WHERE scalar compare; non-correlated; aggregate-only w/o GROUP BY + HAVING; INNER JOIN on TRUE then filter.
#[test]
fn test206() {
    let original_text = r#"
SELECT s.sn
FROM s
WHERE (
  SELECT SUM(p.weight)
  FROM p
  HAVING SUM(p.weight) > 0
) >= 100;"#;
    let expected_text = r#"SELECT "s"."sn" FROM "s" INNER JOIN
        (SELECT sum("p"."weight") AS "sum(weight)" FROM "p" HAVING (sum("p"."weight") > 0)) AS "GNL"
        WHERE ("GNL"."sum(weight)" >= 100)"#;
    test_it("test206", original_text, expected_text);
}

// SELECT-list scalar; non-correlated; aggregate-only w/o GROUP BY + HAVING; LEFT JOIN on TRUE to project scalar.
#[test]
fn test207() {
    let original_text = r#"
SELECT
  s.sn,
  (
    SELECT SUM(p.weight)
    FROM p
    HAVING SUM(p.weight) > 0
  ) AS total_w
FROM s;"#;
    let expected_text = r#"SELECT "s"."sn", "GNL"."sum(weight)" AS "total_w" FROM "s" LEFT OUTER JOIN
        (SELECT sum("p"."weight") AS "sum(weight)" FROM "p" HAVING (sum("p"."weight") > 0)) AS "GNL""#;
    test_it("test207", original_text, expected_text);
}
