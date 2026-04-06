use crate::derived_tables_rewrite::derived_tables_rewrite_main;
use crate::hoist_parametrizable_filters::hoist_parametrizable_filters;
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
    let rewritten_stmt =
        match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, original_text) {
            Ok(mut stmt) => match derived_tables_rewrite_main(&mut stmt, Dialect::PostgreSQL) {
                Ok(has_dt_rw) => match hoist_parametrizable_filters(&mut stmt) {
                    Ok(has_opt_rw) => {
                        if has_dt_rw || has_opt_rw {
                            println!(">>>>>> Rewritten: {}", stmt.display(Dialect::PostgreSQL));
                        }
                        Ok(stmt)
                    }
                    Err(err) => Err(err),
                },
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

#[test]
fn test1() {
    let original_text = r#"SELECT "t1"."i", "t2"."b"
                FROM "test1" AS "t1"
                INNER JOIN (
                    SELECT "t3"."i", "t3"."b" FROM "test2" AS "t3") AS "t2"
                ON ("t2"."b" = "t1"."b")
                WHERE ("t1"."t" = 'aa')"#;

    test_it(
        "test1",
        original_text,
        r#"SELECT "t1"."i", "t3"."b" FROM "test1" AS "t1" INNER JOIN
            "test2" AS "t3" ON ("t1"."b" = "t3"."b") WHERE ("t1"."t" = 'aa')"#,
    );
}

#[test]
fn test2() {
    let original_text = r#"SELECT max("t1"."i"), "t2"."b"
                FROM "test1" AS "t1"
                INNER JOIN (
                    SELECT "t3"."i", "t3"."b" FROM "test2" AS "t3") AS "t2"
                ON ("t2"."b" = "t1"."b")
                WHERE ("t1"."t" = 'aa')
                GROUP BY t2.b"#;

    test_it(
        "test2",
        original_text,
        r#"SELECT max("t1"."i"), "t3"."b"
            FROM "test1" AS "t1" INNER JOIN "test2" AS "t3" ON ("t1"."b" = "t3"."b")
            WHERE ("t1"."t" = 'aa')
            GROUP BY "t3"."b""#,
    );
}

#[test]
fn test3() {
    let original_text = r#"SELECT max(("t1"."i" - t2.i) / 100), "t2"."b"
                FROM "test1" AS "t1"
                INNER JOIN (
                    SELECT "t2"."i", "t3"."b" FROM "test2" t2, test3 "t3" where t2.t = t3.t and t3.t = 'ba') AS "t2"
                ON ("t2"."i" = "t1"."b")
                WHERE ("t1"."t" = 'aa') and t2.i = t1.i
                GROUP BY t2.b"#;

    test_it(
        "test3",
        original_text,
        r#"SELECT max((("t1"."i" - "t2"."i") / 100)), "t3"."b"
            FROM "test1" AS "t1" INNER JOIN "test2" AS "t2" ON (("t1"."b" = "t2"."i") AND ("t1"."i" = "t2"."i"))
            INNER JOIN "test3" AS "t3" ON ("t2"."t" = "t3"."t")
            WHERE (("t1"."t" = 'aa') AND ("t3"."t" = 'ba'))
             GROUP BY "t3"."b""#,
    );
}

#[test]
fn test4() {
    let original_text = r#"SELECT max(("t1"."i" - t2.i) / 100), "t2"."b"
                FROM "test1" AS "t1"
                INNER JOIN (
                    SELECT ("t2"."i" + t3.b) * 0.001 as i, concat("t3"."b", 'abc') as b
                    FROM "test2" t2 left outer join test3 "t3" on t2.t = t3.t
                    where t3.t = 'ba'
                ) AS "t2"
                ON ("t2"."i" = "t1"."b")
                WHERE ("t1"."t" = 'aa') and t2.i = t1.i
                GROUP BY t2.b"#;

    test_it(
        "test4",
        original_text,
        r#"SELECT max((("t1"."i" - (("t2"."i" + "t3"."b") * 0.001)) / 100)), concat("t3"."b", 'abc') AS "b"
            FROM "test2" AS "t2" INNER JOIN "test3" AS "t3" ON ("t2"."t" = "t3"."t") CROSS JOIN "test1" AS "t1"
            WHERE (((("t1"."t" = 'aa') AND ((("t2"."i" + "t3"."b") * 0.001) = "t1"."i")) AND
            ((("t2"."i" + "t3"."b") * 0.001) = "t1"."b")) AND ("t3"."t" = 'ba'))
            GROUP BY "b""#,
    );
}

#[test]
fn test5() {
    let original_text = r#"SELECT max(("t1"."i" - t2.i) / 100), "t2"."b"
                FROM (
                    SELECT ("t2"."i" + t3.b) * 0.001 as i, concat("t3"."b", 'abc') as b
                    FROM "test2" t2 left outer join test3 "t3" on t2.t = t3.t
                    where t3.t = 'ba'
                ) AS "t2"
                INNER JOIN "test1" AS "t1"
                ON ("t2"."i" = "t1"."b")
                WHERE ("t1"."t" = 'aa') and t2.i = t1.i
                GROUP BY t2.b"#;

    test_it(
        "test5",
        original_text,
        r#"SELECT max((("t1"."i" - (("t2"."i" + "t3"."b") * 0.001)) / 100)), concat("t3"."b", 'abc') AS "b"
            FROM "test2" AS "t2" INNER JOIN "test3" AS "t3" ON ("t2"."t" = "t3"."t") CROSS JOIN "test1" AS "t1"
            WHERE (((("t1"."t" = 'aa') AND ((("t2"."i" + "t3"."b") * 0.001) = "t1"."i")) AND
            ((("t2"."i" + "t3"."b") * 0.001) = "t1"."b")) AND ("t3"."t" = 'ba'))
            GROUP BY "b""#,
    );
}

#[test]
fn test6() {
    let original_text = r#"SELECT max(("t1"."i" - t2.i) / 100), "t2"."b"
               FROM
                  (SELECT ("t2"."i" + t3.b) * 0.001 as i, concat("t3"."b", 'abc') as b
                   FROM "test2" t2 left outer join test3 "t3" on t2.t = t3.t
                   where t3.t = 'ba'
                  ) AS "t2",
                  "test1" AS "t1"
               WHERE "t1"."t" = 'aa' and t2.i = t1.i and "t2"."i" = "t1"."b"
               GROUP BY t2.b"#;

    test_it(
        "test6",
        original_text,
        r#"SELECT max((("t1"."i" - (("t2"."i" + "t3"."b") * 0.001)) / 100)), concat("t3"."b", 'abc') AS "b"
            FROM  "test2" AS "t2" INNER JOIN "test3" AS "t3" ON ("t2"."t" = "t3"."t") CROSS JOIN "test1" AS "t1"
            WHERE (((("t1"."t" = 'aa') AND ((("t2"."i" + "t3"."b") * 0.001) = "t1"."i")) AND
            ((("t2"."i" + "t3"."b") * 0.001) = "t1"."b")) AND ("t3"."t" = 'ba'))
            GROUP BY "b""#,
    );
}

#[test]
fn test7() {
    let original_text = r#"SELECT x.sn, x.qty
               FROM (
                  SELECT level2.sn, level2.qty
                     FROM (
                        SELECT level1.sn, level1.qty
                           FROM (
                              SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj
                           ) AS level1
                     ) AS level2
               ) AS x"#;

    test_it(
        "test7",
        original_text,
        r#"SELECT "spj"."sn", "spj"."qty" FROM "spj""#,
    );
}

#[test]
fn test8() {
    let original_text = r#"SELECT job_info.jn, job_info.city
               FROM (
                  SELECT job_base.jn, job_base.city
                     FROM (
                        SELECT j.jn, j.city FROM j
                     ) AS job_base
               ) AS job_info
               WHERE job_info.city IN ('LONDON', 'ATHENS');"#;
    test_it(
        "test8",
        original_text,
        r#"SELECT "j"."jn", "j"."city" FROM "j" WHERE "j"."city" IN ('LONDON', 'ATHENS')"#,
    );
}

#[test]
fn test9() {
    let original_text = r#"SELECT pj_details.pname, pj_details.jname
               FROM (
                  SELECT p.pname, j.jname
                     FROM (SELECT p.pn, p.pname, p.color, p.weight, p.city, p.sn, p.jn FROM p) AS p
                     JOIN (SELECT j.jn, j.jname, j.city, j.sn, j.pn FROM j) AS j
                     ON p.jn = j.jn
               ) AS pj_details;"#;
    test_it(
        "test9",
        original_text,
        r#"SELECT "p"."pname", "j"."jname" FROM "j" INNER JOIN "p" ON ("j"."jn" = "p"."jn")"#,
    );
}

#[test]
fn test10() {
    let original_text = r#"SELECT double_nested.sname, double_nested.city
               FROM (
                  SELECT status_filtered.sn, status_filtered.sname, status_filtered.status,
                         status_filtered.city, status_filtered.pn, status_filtered.jn
                     FROM (SELECT s.sn, s.sname, s.status, s.city, s.pn, s.jn
                           FROM s WHERE s.status >= 20) AS status_filtered
               ) AS double_nested
               WHERE double_nested.city = 'LONDON';"#;
    test_it(
        "test9",
        original_text,
        r#"SELECT "s"."sname", "s"."city" FROM "s"
            WHERE (("s"."city" = 'LONDON') AND ("s"."status" >= 20))"#,
    );
}

#[test]
fn test11() {
    let original_text = r#"SELECT final_nested.sn, final_nested.pn, final_nested.pname
               FROM (
                  SELECT sp.sn, sp.pn, pj.pname
                     FROM (
                        SELECT inner_spj.sn, inner_spj.pn, inner_spj.jn, inner_spj.qty
                           FROM (SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj) AS inner_spj
                     ) AS sp
                     JOIN (
                        SELECT inner_p.pn, inner_p.pname
                           FROM (SELECT p.pn, p.pname, p.color, p.weight, p.city, p.sn, p.jn FROM p) AS inner_p
                     ) AS pj
                     ON sp.pn = pj.pn
               ) AS final_nested;"#;

    test_it(
        "test11",
        original_text,
        r#"SELECT "spj"."sn", "spj"."pn", "p"."pname"
        FROM "p" INNER JOIN "spj" ON ("p"."pn" = "spj"."pn")"#,
    );
}

#[test]
fn test12() {
    let original_text = r#"SELECT avg_qty.jn, avg_qty.avg_qty
               FROM (
                  SELECT nested_spj.jn, AVG(nested_spj.qty) AS avg_qty
                  FROM (SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj) AS nested_spj
                  GROUP BY nested_spj.jn
               ) AS avg_qty;"#;

    test_it(
        "test12",
        original_text,
        r#"SELECT "spj"."jn", avg("spj"."qty") AS "avg_qty" FROM "spj" GROUP BY "spj"."jn""#,
    );
}

#[test]
fn test13() {
    let original_text = r#"SELECT dist_suppliers.sn
               FROM (
                  SELECT DISTINCT nested_spj.sn FROM (SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj) AS nested_spj
               ) AS dist_suppliers;"#;

    test_it(
        "test13",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj""#,
    );
}

#[test]
fn test14() {
    let original_text = r#"
SELECT
    final_data.sn,
    final_data.total_qty,
    supplier_info.sname
FROM
    (
        SELECT
            spj_agg.sn,
            SUM(spj_agg.qty) AS total_qty
        FROM
            (
                SELECT
                    filtered_spj.sn,
                    filtered_spj.pn,
                    filtered_spj.jn,
                    filtered_spj.qty
                FROM
                    (
                        SELECT
                            spj.sn,
                            spj.pn,
                            spj.jn,
                            spj.qty
                        FROM
                            spj
                        WHERE
                            spj.pn = 'P33333'
                    ) AS filtered_spj
            ) AS spj_agg
        WHERE spj_agg.qty = 100
        GROUP BY
            spj_agg.sn
        HAVING
            (COUNT(spj_agg.jn) between 3 and 10) and spj_agg.sn in ('S11111', 'S33333', 'S555555')
    ) AS final_data
    JOIN (
        SELECT
            base_suppliers.sn,
            base_suppliers.sname
        FROM
            (
                SELECT
                    s.sn,
                    s.sname
                FROM
                    s
            ) AS base_suppliers
    ) AS supplier_info ON final_data.sn = supplier_info.sn and supplier_info.sname = 'JONES' and final_data.total_qty = 500
WHERE
    final_data.total_qty > 300;"#;

    test_it(
        "test14",
        original_text,
        r#"SELECT "final_data"."sn", "final_data"."total_qty", "s"."sname" FROM
            (SELECT "spj"."sn" AS "sn", sum("spj"."qty") AS "total_qty", "spj"."qty" AS "qty", "spj"."pn" AS "pn",
            count("spj"."jn") AS "count(jn)" FROM "spj" GROUP BY "spj"."sn", "spj"."qty", "spj"."pn") AS "final_data"
            INNER JOIN "s" ON ("final_data"."sn" = "s"."sn") WHERE ((("final_data"."total_qty" > 300) AND (("s"."sname" = 'JONES')
            AND ("final_data"."total_qty" = 500))) AND (((("final_data"."qty" = 100) AND ("final_data"."pn" = 'P33333'))
            AND "final_data"."count(jn)" BETWEEN 3 AND 10) AND "final_data"."sn" IN ('S11111', 'S33333', 'S555555')))"#,
    );
}

#[test]
fn test15() {
    let original_text = r#"SELECT joined_data.jn AS job_no, joined_data.part_name, joined_data.supplier_city
               FROM (
                  SELECT s_sub.sn, j_sub.jn, p_sub.pname AS part_name, s_sub.city AS supplier_city
                     FROM (
                        SELECT s_filtered.sn, s_filtered.sname, s_filtered.status, s_filtered.city, s_filtered.pn, s_filtered.jn
                        FROM (
                           SELECT s.sn, s.sname, s.status, s.city, s.pn, s.jn FROM s WHERE s.city != 'ROME'
                        ) AS s_filtered
                     ) AS s_sub
                     JOIN (
                        SELECT spj_mid.sn, spj_mid.pn, spj_mid.jn, spj_mid.qty
                           FROM (
                              SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj WHERE spj.qty BETWEEN 100 AND 500
                           ) AS spj_mid
                     ) AS spj_sub
                     ON s_sub.sn = spj_sub.sn
                     JOIN (
                        SELECT p_filtered.pn, p_filtered.pname
                           FROM (
                              SELECT p.pn, p.pname FROM p WHERE p.color IN ('RED', 'BLUE')
                           ) AS p_filtered
                     ) AS p_sub
                     ON spj_sub.pn = p_sub.pn
                     JOIN (
                        SELECT j_base.jn
                           FROM (
                              SELECT j.jn FROM j
                           ) AS j_base
                     ) AS j_sub
                     ON spj_sub.jn = j_sub.jn
               ) AS joined_data
               ORDER BY joined_data.jn, joined_data.part_name;"#;

    test_it(
        "test15",
        original_text,
        r#"SELECT "j"."jn" AS "job_no", "p"."pname" AS "part_name", "s"."city" AS "supplier_city"
            FROM "j" INNER JOIN "spj" ON ("j"."jn" = "spj"."jn") INNER JOIN "p" ON ("spj"."pn" = "p"."pn")
            INNER JOIN "s" ON ("spj"."sn" = "s"."sn") WHERE (("s"."city" != 'ROME') AND
            ("spj"."qty" BETWEEN 100 AND 500 AND "p"."color" IN ('RED', 'BLUE')))
            ORDER BY "j"."jn" NULLS LAST, "p"."pname" NULLS LAST"#,
    );
}

#[test]
fn test16() {
    let original_text = r#"SELECT avg_qty.jn, avg_qty.avg_qty
               FROM (
                  SELECT nested_spj.jn, AVG(nested_spj.qty) AS avg_qty
                  FROM (SELECT spj.sn, spj.pn, spj.jn, spj.qty FROM spj) AS nested_spj
                  GROUP BY nested_spj.jn
                  ORDER BY AVG(nested_spj.qty)
               ) AS avg_qty;"#;

    test_it(
        "test16",
        original_text,
        r#"SELECT "spj"."jn", avg("spj"."qty") AS "avg_qty" FROM "spj" GROUP BY "spj"."jn" ORDER BY "avg_qty""#,
    );
}

#[test]
fn test17() {
    let original_text = r#"
select
    t3.ssn the_ssn,
    t3.jsn || '_' || t3.jsn sum_jsn
from
    (
        select
            min(t2.jsn) jsn,
            t1.ssn
        from
            (
                select
                    spj.sn,
                    spj.pn,
                    s.sn ssn
                from
                    spj
                    join s on spj.sn = s.sn
                    and spj.pn = 'P11111'
                    and s.jn = 'J11111'
            ) t1
            join (
                select
                    p.sn psn,
                    p.jn,
                    j.sn jsn
                from
                    p
                    join j on p.sn = j.sn
                    and p.pn = 'P11111'
            ) t2 on t1.ssn = t2.jsn
            and t1.sn > 'S33333'
            and t2.jsn < 'S55555'
        where
            t1.sn < 'S44444'
            and t2.psn < 'P55555'
        group by t1.ssn
        having
            max(t1.pn) > 'P33333'
    ) t3
where
    t3.ssn in ('S44444', 'S55555');"#;

    test_it(
        "test17",
        original_text,
        r#"SELECT "s"."sn" AS "the_ssn", ((min("j"."sn") || '_') || min("j"."sn")) AS "sum_jsn"
        FROM "p"
        INNER JOIN "j" ON ("p"."sn" = "j"."sn")
        INNER JOIN "s" ON ("j"."sn" = "s"."sn")
        INNER JOIN "spj" ON ("s"."sn" = "spj"."sn")
        WHERE (((((("spj"."sn" < 'S44444') AND ("p"."sn" < 'P55555')) AND ("spj"."sn" > 'S33333')) AND ("j"."sn" < 'S55555'))
        AND ((("spj"."pn" = 'P11111') AND ("s"."jn" = 'J11111')) AND ("p"."pn" = 'P11111'))) AND "s"."sn" IN ('S44444', 'S55555'))
        GROUP BY "s"."sn"
        HAVING (max("spj"."pn") > 'P33333')"#,
    );
}

#[test]
fn test18() {
    let original_text = r#"
SELECT
    dt1.s_sn AS supplier_sn,
    dt2.pn_color AS part_color,
    dt3.j_city AS job_city
FROM
(
    SELECT lvl5.s_sn
    FROM (
        SELECT lvl4.s_sn
        FROM (
            SELECT lvl3.s_sn
            FROM (
                SELECT MAX(lvl2.sn) AS s_sn
                FROM (
                    SELECT s.sn, s.status
                    FROM qa.s AS s
                    WHERE s.status >= 20
                ) AS lvl2
            ) AS lvl3
        ) AS lvl4
    ) AS lvl5
) AS dt1
JOIN
(
    SELECT lvl5.pn_color
    FROM (
        SELECT lvl4.pn_color
        FROM (
            SELECT lvl3.pn_color
            FROM (
                SELECT lvl2.pn_color
                FROM (
                    SELECT MAX(p.color) AS pn_color
                    FROM qa.p AS p
                    WHERE p.weight > 10
                ) AS lvl2
            ) AS lvl3
        ) AS lvl4
    ) AS lvl5
) AS dt2
ON dt1.s_sn = dt2.pn_color
JOIN
(
    SELECT lvl5.j_city
    FROM (
        SELECT lvl4.j_city
        FROM (
            SELECT lvl3.j_city
            FROM (
                SELECT MIN(lvl2.city) AS j_city
                FROM (
                    SELECT j.jn, j.city
                    FROM qa.j AS j
                    WHERE j.city != 'ROME'
                ) AS lvl2
            ) AS lvl3
        ) AS lvl4
    ) AS lvl5
) AS dt3
ON dt2.pn_color = dt3.j_city
LIMIT 10;"#;

    test_it(
        "test18",
        original_text,
        r#"SELECT "dt1"."s_sn" AS "supplier_sn", "dt2"."pn_color" AS "part_color", "dt3"."j_city" AS "job_city"
            FROM (SELECT max("s"."sn") AS "s_sn" FROM "qa"."s" AS "s" WHERE ("s"."status" >= 20)) AS "dt1" INNER JOIN
            (SELECT max("p"."color") AS "pn_color" FROM "qa"."p" AS "p" WHERE ("p"."weight" > 10)) AS "dt2"
            ON ("dt1"."s_sn" = "dt2"."pn_color") INNER JOIN
            (SELECT min("j"."city") AS "j_city" FROM "qa"."j" AS "j" WHERE ("j"."city" != 'ROME')) AS "dt3"
            ON ("dt2"."pn_color" = "dt3"."j_city") LIMIT 10"#,
    );
}

#[test]
fn test19() {
    let original_text = r#"
select
    spj.sn,
    p.city,
    t1.pn
from
    p join
    spj on p.sn = spj.sn
    join (
        select
            j.jn,
            s.pn
        from
            s
            join j on s.sn = j.sn and s.sn = 'S33333' and j.jn > 'J55555'
    ) t1 on spj.jn = t1.jn;"#;

    test_it(
        "test19",
        original_text,
        r#"SELECT "spj"."sn", "p"."city", "s"."pn" FROM "p" INNER JOIN "spj" ON ("p"."sn" = "spj"."sn")
        INNER JOIN "j" ON ("spj"."jn" = "j"."jn") INNER JOIN "s" ON ("j"."sn" = "s"."sn")
        WHERE (("s"."sn" = 'S33333') AND ("j"."jn" > 'J55555'))"#,
    );
}

#[test]
fn test20() {
    let original_text = r#"
SELECT
    ps.weight / 100,
    ps.sn,
    spj.jn
FROM
    spj
    INNER JOIN (
        SELECT
            p.weight * 100 as weight,
            p.city,
            p.pname,
            p.pn,
            s.sn
        FROM
            p
            left outer join s on p.pn = s.pn
    ) AS ps ON spj.pn = ps.pn
    join j on ps.city = j.city;"#;

    test_it(
        "test20",
        original_text,
        r#"SELECT (("p"."weight" * 100) / 100), "s"."sn", "spj"."jn" FROM "p" INNER JOIN "spj"
            ON ("p"."pn" = "spj"."pn") LEFT OUTER JOIN "s" ON ("p"."pn" = "s"."pn") INNER JOIN "j"
            ON ("p"."city" = "j"."city");"#,
    );
}

#[test]
fn test21() {
    let original_text = r#"
SELECT result.sn, result.total
FROM (
    SELECT spj.sn, SUM(spj.qty) AS total
    FROM spj
    GROUP BY spj.sn
    HAVING SUM(spj.qty) > 500
) AS result
WHERE result.total < 1000"#;

    test_it(
        "test21",
        original_text,
        r#"SELECT "spj"."sn", SUM("spj"."qty") AS "total"
        FROM "spj"
        GROUP BY "spj"."sn"
        HAVING (("total" > 500) AND ("total" < 1000))"#,
    );
}

#[test]
fn test22() {
    let original_text = r#"
SELECT s.sname, jp.weight
FROM s
JOIN (
    SELECT j.city, p.weight
    FROM j
    JOIN p ON j.city = p.city AND p.color = 'RED'
) AS jp ON s.city = jp.city"#;

    test_it(
        "test22",
        original_text,
        r#"SELECT "s"."sname", "p"."weight" FROM "p" INNER JOIN "j" ON ("p"."city" = "j"."city")
        INNER JOIN "s" ON ("j"."city" = "s"."city") WHERE ("p"."color" = 'RED')"#,
    );
}

#[test]
fn test23() {
    let original_text = r#"
SELECT x.sn
FROM (
    SELECT sn, ROW_NUMBER() OVER (PARTITION BY pn ORDER BY qty DESC) AS rank
    FROM spj
) AS x
WHERE x.rank <= 3"#;

    test_it(
        "test23",
        original_text,
        r#"SELECT x.sn FROM
        (SELECT sn, ROW_NUMBER() OVER (PARTITION BY pn ORDER BY qty DESC) AS rank FROM spj) AS x WHERE x.rank <= 3"#,
    );
}

#[test]
fn test24() {
    let original_text = r#"
SELECT spj.sn, pj.pname
FROM (
    SELECT p.pn, p.pname FROM p
) AS pj
JOIN spj ON pj.pn = spj.pn
WHERE pj.pn = 'P44444'"#;

    test_it(
        "test24",
        original_text,
        r#"SELECT "spj"."sn", "p"."pname" FROM "p"
        INNER JOIN "spj" ON ("p"."pn" = "spj"."pn")
        WHERE "p"."pn" = 'P44444'"#,
    );
}

#[test]
fn test25() {
    let original_text = r#"
SELECT pj.pn
FROM (
    SELECT p.pn, p.color FROM p
    WHERE p.color IS NULL
) AS pj"#;

    test_it(
        "test25",
        original_text,
        r#"SELECT "p"."pn" FROM "p" WHERE "p"."color" IS NULL"#,
    );
}

#[test]
fn test26() {
    let original_text = r#"
SELECT avg_table.city, avg_table.avg_w
FROM (
    SELECT p.city, AVG(p.weight) AS avg_w FROM p GROUP BY p.city
) AS avg_table
WHERE avg_table.avg_w > 10"#;

    test_it(
        "test26",
        original_text,
        r#"SELECT "p"."city", avg("p"."weight") AS "avg_w"
        FROM "p"
        GROUP BY "p"."city"
        HAVING "avg_w" > 10"#,
    );
}

#[test]
fn test27() {
    let original_text = r#"
SELECT outer1.sn
FROM (
    SELECT s.sn FROM s WHERE s.city = 'ROME'
) AS outer1
WHERE outer1.sn = 'S11111'"#;

    test_it(
        "test27",
        original_text,
        r#"SELECT s.sn FROM s WHERE (s.sn = 'S11111' AND s.city = 'ROME')"#,
    );
}

#[test]
fn test28() {
    let original_text = r#"
SELECT x.sn
FROM (
    SELECT spj.sn, spj.qty FROM spj WHERE spj.qty = 200
) AS x
WHERE x.sn = 'S22222'"#;

    test_it(
        "test28",
        original_text,
        r#"SELECT spj.sn FROM spj WHERE (spj.sn = 'S22222' AND spj.qty = 200)"#,
    );
}

#[test]
fn test29() {
    let original_text = r#"
SELECT z.sn z_sn
FROM (
    SELECT y.sn FROM (
        SELECT x.sn FROM spj x WHERE x.qty = 300
    ) AS y
) AS z
WHERE z.sn = 'S33333'"#;

    test_it(
        "test29",
        original_text,
        r#"SELECT "x"."sn" AS "z_sn" FROM "spj" AS "x" WHERE (("x"."sn" = 'S33333') AND ("x"."qty" = 300))"#,
    );
}

#[test]
fn test30() {
    let original_text = r#"
SELECT outer2.sn
FROM (
    SELECT s.sn FROM s WHERE s.status > 10
) AS outer2
JOIN j ON outer2.sn = j.sn
WHERE j.city = 'LONDON'"#;

    test_it(
        "test30",
        original_text,
        r#"SELECT s.sn FROM j INNER JOIN s ON j.sn = s.sn WHERE (j.city = 'LONDON' AND s.status > 10)"#,
    );
}

#[test]
fn test31() {
    let original_text = r#"
SELECT result.sn
FROM (
    SELECT sn FROM (
        SELECT spj.sn FROM spj WHERE spj.qty = 500
    ) AS inner1
    WHERE inner1.sn > 'S10000'
) AS result
WHERE result.sn != 'S99999'"#;

    test_it(
        "test31",
        original_text,
        r#"SELECT "sn" FROM "spj" WHERE (("sn" != 'S99999') AND (("spj"."sn" > 'S10000') AND ("spj"."qty" = 500)))"#,
    );
}

#[test]
fn test32() {
    let original_text = r#"
SELECT t.sn
FROM (
    SELECT spj.sn, spj.pn FROM spj WHERE spj.qty = 700
) AS t
WHERE t.pn = 'P44444'"#;

    test_it(
        "test32",
        original_text,
        r#"SELECT spj.sn FROM spj WHERE (spj.pn = 'P44444' AND spj.qty = 700)"#,
    );
}

#[test]
fn test33() {
    let original_text = r#"
SELECT t1.sn t1_sn
FROM (
    SELECT t2.sn FROM (
        SELECT t3.sn FROM s t3 WHERE t3.status = 30
    ) AS t2
) AS t1
WHERE t1.sn IN ('S1', 'S2')"#;

    test_it(
        "test33",
        original_text,
        r#"SELECT "t3"."sn" AS "t1_sn" FROM "s" AS "t3" WHERE ("t3"."sn" IN ('S1', 'S2') AND ("t3"."status" = 30))"#,
    );
}

#[test]
fn test34() {
    let original_text = r#"
SELECT nested.sn, nested.city
FROM (
    SELECT s.sn, s.city FROM s WHERE s.status = 50
) AS nested
WHERE nested.city = 'ATHENS'"#;

    test_it(
        "test34",
        original_text,
        r#"SELECT s.sn, s.city FROM s WHERE (s.city = 'ATHENS' AND s.status = 50)"#,
    );
}

#[test]
fn test35() {
    let original_text = r#"
SELECT r.jn
FROM (
    SELECT j.jn, j.city FROM j WHERE j.city = 'ROME'
) AS r
WHERE r.jn > 'J20000'"#;

    test_it(
        "test35",
        original_text,
        r#"SELECT j.jn FROM j WHERE (j.jn > 'J20000' AND j.city = 'ROME')"#,
    );
}

#[test]
fn test36() {
    let original_text = r#"
SELECT final.sn
FROM (
    SELECT inner1.sn FROM (
        SELECT spj.sn FROM spj WHERE spj.qty = 400
    ) AS inner1
) AS final
WHERE final.sn < 'S88888'"#;

    test_it(
        "test36",
        original_text,
        r#"SELECT spj.sn FROM spj WHERE (spj.sn < 'S88888' AND spj.qty = 400)"#,
    );
}

#[test]
fn test37() {
    let original_text = r#"
SELECT outer.sn
FROM (
    SELECT spj.sn, COUNT(*) as cnt FROM spj GROUP BY spj.sn
) AS outer
WHERE outer.sn = 'S1'"#;

    test_it(
        "test37",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE ("spj"."sn" = 'S1')"#,
    );
}

#[test]
fn test38() {
    let original_text = r#"
SELECT p.pname, filtered.qty
FROM p
JOIN (
    SELECT spj.pn, spj.qty FROM spj WHERE spj.qty > 100
) AS filtered ON p.pn = filtered.pn
WHERE filtered.qty < 500"#;

    test_it(
        "test38",
        original_text,
        r#"SELECT p.pname, spj.qty FROM p INNER JOIN spj ON p.pn = spj.pn WHERE (spj.qty < 500 AND spj.qty > 100)"#,
    );
}

#[test]
fn test39() {
    let original_text = r#"
SELECT final.jjn
FROM (
    SELECT j.jn jjn, COUNT(*) AS cnt FROM j GROUP BY j.jn
) AS final
WHERE final.jjn LIKE 'J%' AND final.cnt > 3"#;

    test_it(
        "test39",
        original_text,
        r#"SELECT "j"."jn" AS "jjn" FROM "j" WHERE ("j"."jn" LIKE 'J%') GROUP BY "j"."jn" HAVING (count(*) > 3)"#,
    );
}

#[test]
fn test40() {
    let original_text = r#"
SELECT result.sn
FROM (
    SELECT nested.sn, nested.qty FROM (
        SELECT spj.sn, spj.qty FROM spj WHERE spj.qty > 50
    ) AS nested
) AS result
WHERE result.qty < 200"#;

    test_it(
        "test40",
        original_text,
        r#"SELECT spj.sn FROM spj WHERE (spj.qty < 200 AND spj.qty > 50)"#,
    );
}

#[test]
fn test41() {
    let original_text = r#"
SELECT final.sn
FROM (
    SELECT agg.sn, SUM(agg.qty) AS total FROM (
        SELECT spj.sn, spj.qty FROM spj WHERE spj.qty >= 100
    ) AS agg
    GROUP BY agg.sn
) AS final
WHERE final.total <= 500"#;

    test_it(
        "test41",
        original_text,
        r#"SELECT "spj"."sn" FROM "spj" WHERE ("spj"."qty" >= 100) GROUP BY "spj"."sn" HAVING (sum("spj"."qty") <= 500)"#,
    );
}

#[test]
fn test42() {
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1
JOIN (
  SELECT t2.i, t2.b FROM test2 AS t2
) AS d
ON (t1.i = d.i + 1)"#;

    test_it(
        "test42",
        original_text,
        r#"SELECT "t1"."i" FROM "test1" AS "t1" CROSS JOIN "test2" AS "t2"  WHERE ("t1"."i" = ("t2"."i" + 1))"#,
    );
}

// Aggregate-only derived (no GROUP BY) may only be cross-joined; do not flatten into an equi-join
// The rewrite should just normalize the order of the joining tables
#[test]
fn test43() {
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1 INNER JOIN (
  SELECT SUM(spj.qty) AS total FROM spj
) AS a
ON (t1.i = a.total)"#;

    test_it(
        "test43",
        original_text,
        r#"SELECT "t1"."i" FROM (SELECT sum("spj"."qty") AS "total" FROM "spj") AS "a"
        INNER JOIN "test1" AS "t1" ON ("a"."total" = "t1"."i")"#,
    );
}

#[test]
fn test44() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN LATERAL (
  SELECT SUM(spj.qty) AS total
  FROM spj
  WHERE spj.sn = s.sn
) AS a ON TRUE"#;

    // Correlated LATERAL may not be part of an OUTER join; do not rewrite LATERAL, just normalize `JOIN ON TRUE`
    test_it(
        "test44",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN LATERAL (SELECT sum("spj"."qty") AS "total" FROM "spj"
        WHERE ("spj"."sn" = "s"."sn")) AS "a""#,
    );
}

#[test]
fn test45() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN LATERAL (
  SELECT spj.jn
  FROM spj
  WHERE spj.sn = s.sn AND spj.qty > s.status
  LIMIT 1
) AS x ON TRUE"#;

    // LATERAL correlations must be equality constraints; non-equality (> ) must not be introduced by flattening
    test_it(
        "test45",
        original_text,
        r#"SELECT "s"."sn" FROM "s" CROSS JOIN LATERAL (SELECT "spj"."jn" FROM "spj"
                      WHERE (("spj"."sn" = "s"."sn") AND ("spj"."qty" > "s"."status")) LIMIT 1) AS "x""#,
    );
}

// Flattening would yield column = window-expression (not column=column);
// keep as-is, just normalize the join
#[test]
fn test46() {
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1 INNER JOIN (
  SELECT t2.i, ROW_NUMBER() OVER (ORDER BY t2.i) AS rn
  FROM test2 AS t2
) AS d
ON (t1.i = d.rn)"#;

    test_it(
        "test46",
        original_text,
        r#"SELECT "t1"."i" FROM (SELECT "t2"."i", ROW_NUMBER() OVER(ORDER BY "t2"."i" ASC NULLS LAST) AS "rn"
        FROM "test2" AS "t2") AS "d" INNER JOIN "test1" AS "t1" ON ("d"."rn" = "t1"."i")"#,
    );
}

#[test]
fn test47() {
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1
JOIN (
  SELECT t2.i FROM test2 AS t2
) AS d
ON (t1.i <> d.i)"#;

    test_it(
        "test47",
        original_text,
        r#"SELECT "t1"."i" FROM "test1" AS "t1" CROSS JOIN "test2" AS "t2"  WHERE ("t1"."i" != "t2"."i")"#,
    );
}

#[test]
fn test48() {
    let original_text = r#"
SELECT t1.i
FROM test1 AS t1
JOIN (
  SELECT 42 AS cst, t2.t
  FROM test2 AS t2
) AS d
ON (t1.i = d.cst AND t1.t = d.t)"#;

    test_it(
        "test48",
        original_text,
        r#"SELECT "t1"."i" FROM "test1" AS "t1" INNER JOIN "test2" AS "t2" ON ("t1"."t" = "t2"."t")
            WHERE ("t1"."i" = 42)"#,
    );
}

// Aggregate-only LATERAL subquery must be cross-joined; joining it via equality would violate invariants
// Just normalize the join
#[test]
fn test49() {
    let original_text = r#"
SELECT s.sn
FROM s INNER JOIN LATERAL (
  SELECT COUNT(*) AS c
  FROM spj
  WHERE spj.sn = s.sn
) AS c ON (s.status = c.c)"#;

    test_it(
        "test49",
        original_text,
        r#"SELECT "s"."sn" FROM LATERAL (SELECT count(*) AS "c" FROM "spj" WHERE ("spj"."sn" = "s"."sn")) AS "c"
        INNER JOIN "s" ON ("c"."c" = "s"."status")"#,
    );
}

// Multi-level WHERE hoisting across joins
#[test]
fn test50() {
    let original_text = r#"
SELECT outer1.sn, outer1.city
FROM (
    SELECT inner1.sn, inner1.city
    FROM (
        SELECT s.sn, s.city FROM s WHERE s.status = 20
    ) AS inner1
    WHERE inner1.city = 'ATHENS'
) AS outer1
JOIN (
    SELECT j.jn, j.city FROM j
) AS j ON outer1.city = j.city"#;

    test_it(
        "test50",
        original_text,
        r#"SELECT s.sn, s.city FROM j INNER JOIN s ON j.city = s.city WHERE (s.city = 'ATHENS' AND s.status = 20)"#,
    );
}

// Hoist simple filter from nested derived under inner join
#[test]
fn test51() {
    let original_text = r#"
SELECT pj.sn, pj.jn
FROM s
JOIN (
    SELECT spj.sn, spj.jn FROM (
        SELECT spj.sn, spj.jn, spj.qty FROM spj WHERE spj.qty = 500
    ) AS inner_spj
) AS pj ON s.sn = pj.sn
WHERE pj.jn = 'J44444'"#;

    test_it(
        "test51",
        original_text,
        r#"SELECT spj.sn, spj.jn FROM s INNER JOIN spj ON s.sn = spj.sn WHERE (("spj"."jn" = 'J44444') AND ("spj"."qty" = 500))"#,
    );
}

// Deep nesting with join and post-join filter
#[test]
fn test52() {
    let original_text = r#"
SELECT result.sn
FROM (
    SELECT spj_data.sn
    FROM (
        SELECT spj.sn, spj.pn FROM spj WHERE spj.qty >= 300
    ) AS spj_data
    JOIN (
        SELECT p.pn, p.color FROM p WHERE p.color = 'RED'
    ) AS p_data ON spj_data.pn = p_data.pn
) AS result
WHERE result.sn LIKE 'S%'"#;

    test_it(
        "test52",
        original_text,
        r#"SELECT spj.sn FROM p  INNER JOIN spj ON p.pn = spj.pn
            WHERE ((("spj"."sn" LIKE 'S%') AND ("spj"."qty" >= 300)) AND ("p"."color" = 'RED'))"#,
    );
}

// Join tree with hoisted and join-normalized filters
#[test]
fn test53() {
    let original_text = r#"
SELECT final.sn
FROM (
    SELECT spj.sn, spj.pn FROM spj WHERE spj.qty > 200
) AS final
JOIN (
    SELECT p.pn FROM p WHERE p.weight < 50
) AS p_sub ON final.pn = p_sub.pn
WHERE final.sn = 'S12345'"#;

    test_it(
        "test53",
        original_text,
        r#"SELECT spj.sn FROM p INNER JOIN spj ON p.pn = spj.pn
            WHERE ((("spj"."sn" = 'S12345') AND ("spj"."qty" > 200)) AND ("p"."weight" < 50))"#,
    );
}

// Multi-nested joins, with conjunct hoisting
#[test]
fn test54() {
    let original_text = r#"
SELECT sname_info.sname
FROM (
    SELECT s.sn, s.sname, s.city FROM s WHERE s.city = 'LONDON'
) AS sname_info
JOIN (
    SELECT pj.jn, pj.city FROM (
        SELECT j.jn, j.city FROM j WHERE j.city != 'ROME'
    ) AS pj
) AS j_info ON sname_info.city = j_info.city
WHERE sname_info.sname = 'JONES'"#;

    test_it(
        "test54",
        original_text,
        r#"SELECT s.sname FROM j INNER JOIN s ON j.city = s.city
            WHERE ((("s"."sname" = 'JONES') AND ("s"."city" = 'LONDON')) AND ("j"."city" != 'ROME'))"#,
    );
}

// Grouped base + grouped inlinable with HAVING pushdown
#[test]
fn test55() {
    let original_text = r#"
SELECT grouped.sn
FROM (
    SELECT spj.sn, COUNT(*) as cnt FROM spj GROUP BY spj.sn HAVING COUNT(*) > 1
) AS grouped
GROUP BY grouped.sn
HAVING grouped.sn LIKE 'S%'"#;

    test_it(
        "test55",
        original_text,
        r#"SELECT DISTINCT "grouped"."sn" FROM (SELECT "spj"."sn", count(*) AS "cnt" FROM "spj" GROUP BY "spj"."sn")
        AS "grouped" WHERE (("grouped"."sn" LIKE 'S%') AND ("grouped"."cnt" > 1))"#,
    );
}

// Inlinable has GROUP BY, base does not
#[test]
fn test56() {
    let original_text = r#"
SELECT result.sn
FROM (
    SELECT spj.sn, COUNT(*) AS cnt FROM spj GROUP BY spj.sn
) AS result
WHERE result.cnt > 10"#;

    test_it(
        "test56",
        original_text,
        r#"SELECT spj.sn FROM spj GROUP BY spj.sn HAVING COUNT(*) > 10"#,
    );
}

// Inlinable has WF, base does not
#[test]
fn test57() {
    let original_text = r#"
SELECT ranked.sn
FROM (
    SELECT spj.sn, ROW_NUMBER() OVER (PARTITION BY spj.pn ORDER BY spj.qty DESC) AS rn
    FROM spj
) AS ranked
WHERE ranked.rn <= 2"#;

    test_it("test57", original_text, original_text);
}

// Base has GROUP BY and aggregates, inlinable is grouped
#[test]
fn test58() {
    let original_text = r#"
SELECT agg.sn, COUNT(*) AS total
FROM (
    SELECT spj.sn, spj.qty FROM spj WHERE spj.qty >= 100
) AS agg
GROUP BY agg.sn
HAVING COUNT(*) > 3"#;

    test_it(
        "test58",
        original_text,
        r#"SELECT spj.sn, COUNT(*) AS total FROM spj WHERE spj.qty >= 100 GROUP BY spj.sn HAVING total > 3"#,
    );
}

// Inlinable has window function + group by, base is flat
#[test]
fn test59() {
    let original_text = r#"
SELECT derived.sn
FROM (
    SELECT spj.sn, ROW_NUMBER() OVER (PARTITION BY spj.jn ORDER BY spj.qty DESC) AS rn
    FROM spj
    WHERE spj.qty > 100
) AS derived
WHERE derived.rn = 1"#;

    test_it(
        "test59",
        original_text,
        original_text, //r#"SELECT derived.sn FROM (SELECT spj.sn, ROW_NUMBER() OVER (PARTITION BY spj.jn ORDER BY spj.qty DESC) AS rn FROM spj WHERE spj.qty > 100) AS derived WHERE derived.rn = 1"#,
    );
}

// HAVING on grouping key (equality) → moved to WHERE
#[test]
fn test60() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, SUM(spj.qty) AS total
  FROM spj
  GROUP BY spj.sn
  HAVING spj.sn = 'S11111'
) AS r"#;

    test_it(
        "test60",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE ("spj"."sn" = 'S11111')"#,
    );
}

// HAVING on grouping key (range) → moved to WHERE
#[test]
fn test61() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, SUM(spj.qty) AS total
  FROM spj
  GROUP BY spj.sn
  HAVING spj.sn > 'S20000'
) AS r"#;

    test_it(
        "test61",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE ("spj"."sn" > 'S20000')"#,
    );
}

// HAVING on grouping key (BETWEEN) with positional GROUP BY → moved to WHERE
#[test]
fn test62() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, SUM(spj.qty) AS total
  FROM spj
  GROUP BY 1
  HAVING spj.sn BETWEEN 'S10000' AND 'S99999'
) AS r"#;

    test_it(
        "test62",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE "spj"."sn" BETWEEN 'S10000' AND 'S99999'"#,
    );
}

// HAVING on grouping key (LIKE) with ORDER BY aggregate → key filter moved to WHERE, ORDER BY preserved
#[test]
fn test63() {
    let original_text = r#"
SELECT r.spj_sn
FROM (
  SELECT spj.sn spj_sn, SUM(spj.qty) AS total
  FROM spj
  GROUP BY spj.sn
  HAVING spj_sn LIKE 'S%'
  ORDER BY SUM(spj.qty)
) AS r"#;

    test_it(
        "test63",
        original_text,
        r#"SELECT "spj"."sn" AS "spj_sn" FROM "spj" WHERE ("spj"."sn" LIKE 'S%') GROUP BY "spj"."sn"
        ORDER BY sum("spj"."qty") NULLS LAST"#,
    );
}

// HAVING on grouping key (IN list, including NULL) → moved to WHERE (3VL preserved)
#[test]
fn test64() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, COUNT(*) AS c
  FROM spj
  GROUP BY spj.sn
  HAVING spj.sn IN ('S1', NULL)
) AS r"#;

    test_it(
        "test64",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE ("spj"."sn" IN ('S1', NULL))"#,
    );
}

// NEGATIVE: HAVING with aggregate (SUM) must NOT move; stays in HAVING
#[test]
fn test65() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, SUM(spj.qty) AS total
  FROM spj
  GROUP BY spj.sn
  HAVING SUM(spj.qty) > 0
) AS r"#;

    test_it(
        "test65",
        original_text,
        r#"SELECT "spj"."sn" FROM "spj"
        GROUP BY "spj"."sn"
        HAVING SUM("spj"."qty") > 0"#,
    );
}

// NEGATIVE: HAVING contains subquery (not parametrizable) → must NOT move
#[test]
fn test66() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn
  FROM spj
  GROUP BY spj.sn
  HAVING spj.sn IN (SELECT s.sn FROM s)
) AS r"#;

    // The inner GROUP BY without aggregates becomes DISTINCT, HAVING moves
    // to WHERE.  The outer wrapper `r` is then a simple projection over a
    // single FROM subquery — it gets inlined (the inner has no expression
    // subqueries at its OWN level after GROUP BY→DISTINCT conversion).
    let expected = r#"SELECT DISTINCT "spj"."sn" FROM "spj"
        WHERE "spj"."sn" IN (SELECT "s"."sn" FROM "s")"#;

    test_it("test66", original_text, expected);
}

// Combination: existing WHERE + HAVING on grouping key → both end up in WHERE
#[test]
fn test67() {
    let original_text = r#"
SELECT r.sn
FROM (
  SELECT spj.sn, SUM(spj.qty) AS total
  FROM spj
  WHERE spj.pn = 'P1'
  GROUP BY spj.sn
  HAVING spj.sn >= 'S00001'
) AS r"#;

    test_it(
        "test67",
        original_text,
        r#"SELECT DISTINCT "spj"."sn" FROM "spj" WHERE (("spj"."pn" = 'P1') AND ("spj"."sn" >= 'S00001'))"#,
    );
}

#[test]
fn test68() {
    let original_text = r#"
SELECT a.Test_INT, b.Test_DEC2
FROM DataTypes a
LEFT OUTER JOIN DataTypes2 b
  ON b.Test_INT2 = a.Test_INT AND (b.Test_DEC2 IS NULL OR b.Test_DEC2 >= 8);
"#;
    test_it(
        "test68",
        original_text,
        r#"SELECT "a"."test_int", "b"."test_dec2" FROM "datatypes" AS "a" LEFT OUTER JOIN "datatypes2" AS "b"
        ON (("a"."test_int" = "b"."test_int2") AND (("b"."test_dec2" IS NULL) OR ("b"."test_dec2" >= 8)))"#,
    );
}

// Must preserve join ON as-is — i.e., avoid pushing j.city = 'A' to WHERE.
#[test]
fn test69() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN (
  SELECT j.jn, j.sn
  FROM j
  WHERE j.city = 'A'
) AS jsub
  ON s.sn = jsub.sn AND jsub.jn = 'J1';
"#;
    test_it(
        "test69",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN "j"
        ON ((("s"."sn" = "j"."sn") AND ("j"."jn" = 'J1')) AND ("j"."city" = 'A'))"#,
    );
}

// Nested LEFT JOIN + filter on subquery
#[test]
fn test70() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN (
  SELECT inner_j.jn, inner_j.sn
  FROM (
    SELECT j.jn, j.sn FROM j WHERE j.city = 'A'
  ) AS inner_j
) AS outer_j
  ON s.sn = outer_j.sn;
"#;
    test_it(
        "test70",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN "j" ON (("s"."sn" = "j"."sn") AND ("j"."city" = 'A'))"#,
    );
}

// Mixed JOINs with safe inner filter hoisting
#[test]
fn test71() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN (
  SELECT spj.sn FROM spj WHERE spj.pn = 'P1'
) AS sub1
  ON s.sn = sub1.sn
LEFT JOIN (
  SELECT j.jn, j.sn FROM j WHERE j.city = 'X'
) AS sub2
  ON s.sn = sub2.sn;"#;
    test_it(
        "test71",
        original_text,
        r#"SELECT "s"."sn" FROM "s" INNER JOIN "spj" ON ("s"."sn" = "spj"."sn") LEFT OUTER JOIN "j"
            ON (("s"."sn" = "j"."sn") AND ("j"."city" = 'X')) WHERE ("spj"."pn" = 'P1')"#,
    );
}

// Verify WHERE predicate not injected from outer join
#[test]
fn test72() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN (
  SELECT j.sn FROM j WHERE j.city = 'X' AND j.jn = 'J2'
) AS sub
  ON s.sn = sub.sn;
"#;
    test_it(
        "test72",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN "j" ON (("s"."sn" = "j"."sn")
            AND (("j"."city" = 'X') AND ("j"."jn" = 'J2')))"#,
    );
}

// Aggregated LEFT JOIN with filters
#[test]
fn test73() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (
  SELECT j.sn, COUNT(*) AS cnt
  FROM j
  WHERE j.city = 'X'
  GROUP BY j.sn
) AS sub
  ON s.sn = sub.sn;
"#;
    test_it("test73", original_text, original_text);
}

#[test]
fn test74() {
    let original_text = r#"
SELECT
    count(*)
FROM
    "s"
    LEFT OUTER JOIN (
        SELECT
            "spj"."sn" AS "sn",
            "spj"."pn" AS "pn"
        FROM
            "spj"
    ) AS "GNL1" ON (
        ("s"."pn" = "GNL1"."pn") AND ("s"."jn" = "GNL1"."sn")
    )
    LEFT OUTER JOIN (
        SELECT
            "spj"."sn" AS "sn",
            "spj"."pn" AS "pn"
        FROM
            "spj"
    ) AS "GNL2" ON (
        ("s"."pn" = "GNL2"."pn") AND ("s"."pn" = "GNL2"."sn")
    );
"#;
    // GNL1 inlines (no table overlap with outer "s"), but GNL2 bails out because "spj"
    // already exists after GNL1 was inlined — inlining would create a self-join.
    test_it(
        "test74",
        original_text,
        r#"SELECT count(*) FROM "s" LEFT OUTER JOIN "spj" ON (("s"."pn" = "spj"."pn") AND ("s"."jn" = "spj"."sn"))
            LEFT OUTER JOIN (SELECT "spj"."sn" AS "sn", "spj"."pn" AS "pn" FROM "spj") AS "GNL2"
            ON (("s"."pn" = "GNL2"."pn") AND ("s"."pn" = "GNL2"."sn"))"#,
    );
}

#[test]
fn test75() {
    let original_text = r#"
    SELECT MAX(sub.x) OVER (), sub.y
    FROM (
      SELECT MAX(t.a) AS x, t.y FROM t GROUP BY t.y
    ) AS sub;
"#;
    test_it("test75", original_text, original_text);
}

#[test]
fn test76() {
    let original_text = r#"
    SELECT MAX(sub.z) OVER (), sub.y
    FROM (
      SELECT t.a AS z, t.y FROM t
    ) AS sub;
    "#;
    test_it(
        "test76",
        original_text,
        r#"SELECT max("t"."a") OVER(), "t"."y" FROM "t""#,
    );
}

#[test]
fn test77() {
    let original_text = r#"
    SELECT MAX(b) OVER ()
    FROM r, (SELECT x FROM s) AS sub;
    "#;
    test_it(
        "test77",
        original_text,
        r#"SELECT max("b") OVER() FROM "r" CROSS JOIN "s""#,
    );
}

#[test]
fn test78() {
    let original_text = r#"
    SELECT MAX(sub.x) OVER ()
    FROM (
      SELECT MAX(t.a) AS x, t.b FROM t GROUP BY t.b
    ) AS sub;
    "#;
    test_it("test78", original_text, original_text);
}

#[test]
fn test79() {
    let original_text = r#"
SELECT
    "spj"."sn"
FROM
    "spj"
    INNER JOIN (
        SELECT
            "s"."sn"
        FROM
            "s"
    ) AS "tab1" ON ("tab1"."sn" = "spj"."sn")
    INNER JOIN (
        SELECT
            "s"."sn"
        FROM
            "s"
    ) AS "tab2" ON ("tab2"."sn" = "tab1"."sn");
    "#;
    // tab1 inlines (no overlap with outer "spj"), but tab2 bails out because "s" already
    // exists after tab1 was inlined — inlining would create a self-join.
    test_it(
        "test79",
        original_text,
        r#"SELECT "spj"."sn" FROM "s" INNER JOIN "spj" ON ("s"."sn" = "spj"."sn")
        CROSS JOIN (SELECT "s"."sn" FROM "s") AS "tab2"
        WHERE ("tab2"."sn" = "s"."sn")"#,
    );
}

// Test80: Should NOT flatten when base contains WF over aggregated inlinable field
#[test]
fn test80() {
    let original_text = r#"
SELECT MAX(sub.x) OVER (), sub.y
FROM (
  SELECT MAX(t.a) AS x, t.y FROM t GROUP BY t.y
) AS sub;"#;

    test_it("test80", original_text, original_text);
}

// Test81: Flattening allowed: left join of single-table derived with simple filter
#[test]
fn test81() {
    let original_text = r#"
SELECT s.sn, x.qty
FROM s
LEFT JOIN (
  SELECT spj.sn, spj.qty FROM spj WHERE spj.qty > 100
) AS x ON s.sn = x.sn;"#;

    test_it(
        "test81",
        original_text,
        r#"SELECT "s"."sn", "spj"."qty" FROM "s" LEFT OUTER JOIN "spj" ON
            (("s"."sn" = "spj"."sn") AND ("spj"."qty" > 100))"#,
    );
}

// Test82: Flattening NOT allowed: ON condition would reference >2 tables after WHERE merge
#[test]
fn test82() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (
  SELECT t1.sn, t2.pn FROM t1 INNER JOIN t2 ON t1.sn = t2.sn WHERE t2.pn = 'P1'
) AS x ON s.sn = x.sn;"#;

    test_it("test82", original_text, original_text);
}

// Test83: Self-join bail-out — t inlines but t1 bails because "spj" already exists
#[test]
fn test83() {
    let original_text = r#"
SELECT t.sn
FROM (
  SELECT spj.sn FROM spj
) AS t
JOIN (
  SELECT spj.sn FROM spj
) AS t1 ON t.sn = t1.sn;"#;

    test_it(
        "test83",
        original_text,
        r#"SELECT "spj"."sn" FROM "spj"
        CROSS JOIN (SELECT "spj"."sn" FROM "spj") AS "t1"
        WHERE ("spj"."sn" = "t1"."sn")"#,
    );
}

// Test84: Flattening avoids breaking outer WF dependency on inlinable columns
#[test]
fn test84() {
    let original_text = r#"
SELECT MAX(t.col1) OVER (), t.col2
FROM (
  SELECT s.col1, s.col2 FROM s
) AS t;"#;

    test_it(
        "test84",
        original_text,
        r#"SELECT max("s"."col1") OVER (), "s"."col2" FROM "s""#,
    );
}

#[test]
fn test85() {
    let original_text = r#"
select spj.qty
from spj inner join
(select s.sn, p.pn, j.jn from spj, s t, s, p inner join j on p.city = j.city where spj.sn = 'S33333' and j.city = 'PARIS') t on spj.pn = t.jn;
        "#;
    // Self-join bail-out: subquery contains "spj" which already exists in outer FROM.
    // The hoist pass extracts parametrizable filters (spj.sn = 'S33333', j.city = 'PARIS')
    // from the inner WHERE, projects the involved columns (spj.sn → t.sn0, j.city → t.city),
    // and adds the rebound filters to the outer WHERE for cache key exposure.
    test_it(
        "test85",
        original_text,
        r#"SELECT "spj"."qty" FROM "spj"
        INNER JOIN (SELECT "s"."sn", "p"."pn", "j"."jn", "spj"."sn" AS "sn0",
            "j"."city" AS "city"
            FROM "j" INNER JOIN "p" ON ("j"."city" = "p"."city")
            CROSS JOIN "s" CROSS JOIN "spj" CROSS JOIN "s" AS "t") AS "t"
        ON ("spj"."pn" = "t"."jn")
        WHERE (("t"."sn0" = 'S33333') AND ("t"."city" = 'PARIS'))"#,
    );
}

// Base has WF, inlinable is non-aggregated — safe to inline
#[test]
fn test86() {
    let original_text = r#"
SELECT MAX(sub.col1) OVER (), sub.col2
FROM (
  SELECT s.col1, s.col2 FROM s
) AS sub;"#;

    test_it(
        "test86",
        original_text,
        r#"SELECT max("s"."col1") OVER (), "s"."col2" FROM "s""#,
    );
}

// Cross join between base and inlinable — safe flattening
#[test]
fn test87() {
    let original_text = r#"
SELECT s.sn, t.city
FROM s, (
  SELECT j.city FROM j WHERE j.jn = 'J1'
) AS t;"#;

    test_it(
        "test87",
        original_text,
        r#"SELECT "s"."sn", "j"."city" FROM "j" CROSS JOIN "s" WHERE "j"."jn" = 'J1'"#,
    );
}

// Inlinable joins 3 tables, outer-joined to base — must not flatten
#[test]
fn test88() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (
  SELECT j.sn, j.jn FROM p INNER JOIN j ON j.city = p.city INNER JOIN spj ON j.jn = spj.jn
) AS x ON s.sn = x.sn;"#;

    test_it(
        "test88",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
    (SELECT "j"."sn", "j"."jn" FROM "p" INNER JOIN "j" ON ("p"."city" = "j"."city")
    INNER JOIN "spj" ON ("j"."jn" = "spj"."jn")) AS "x" ON ("s"."sn" = "x"."sn")"#,
    );
}

// Alias-only projection from inlinable — ensure column survives
#[test]
fn test89() {
    let original_text = r#"
SELECT outer.sn
FROM (
  SELECT spj.sn AS sn FROM spj
) AS outer;"#;

    test_it("test89", original_text, r#"SELECT "spj"."sn" FROM "spj""#);
}

// Inlinable WHERE contains constant-only conjunct — should not block flattening
#[test]
fn test90() {
    let original_text = r#"
SELECT s.sn
FROM s
JOIN (
  SELECT spj.sn FROM spj WHERE spj.pn = 'P1' AND 1 = 1
) AS x ON s.sn = x.sn;"#;

    test_it(
        "test90",
        original_text,
        r#"SELECT "s"."sn" FROM "s" INNER JOIN "spj" ON ("s"."sn" = "spj"."sn") WHERE ((1 = 1) AND ("spj"."pn" = 'P1'))"#,
    );
}

// Test92: LEFT JOIN with null-rejecting filter in WHERE becomes INNER JOIN
#[test]
fn test91() {
    let original_text = r#"
SELECT s.sn, p.city
FROM s
LEFT JOIN p ON s.sn = p.sn
WHERE p.city IS NOT NULL"#;

    test_it(
        "test91",
        original_text,
        r#"SELECT "s"."sn", "p"."city" FROM "p" INNER JOIN "s" ON "p"."sn" = "s"."sn" WHERE "p"."city" IS NOT NULL"#,
    );
}

// Test93: LEFT JOIN not rewritten due to opaque filter in HAVING
#[test]
fn test92() {
    let original_text = r#"
SELECT s.city, COUNT(p.pn)
FROM s LEFT OUTER JOIN p ON s.sn = p.sn
GROUP BY s.city
HAVING MIN(p.pn) > 'P33333'"#;

    test_it("test92", original_text, original_text);
}

// Test94: LEFT JOIN null-rejected by later inner join
#[test]
fn test93() {
    let original_text = r#"
SELECT s.sname, j.jname
FROM s
LEFT JOIN p ON s.sn = p.sn
INNER JOIN j ON p.pn = j.pn"#;

    test_it(
        "test93",
        original_text,
        r#"SELECT s.sname, j.jname FROM "j" INNER JOIN "p" ON ("j"."pn" = "p"."pn")
        INNER JOIN "s" ON ("p"."sn" = "s"."sn")"#,
    );
}

// Test95: Multi-join, both null-rejected
#[test]
fn test94() {
    let original_text = r#"
SELECT s.sn, p.pn, j.jn
FROM s
LEFT JOIN p ON s.sn = p.sn
LEFT JOIN j ON p.pn = j.pn
WHERE j.city IS NOT NULL"#;

    test_it(
        "test94",
        original_text,
        r#"SELECT s.sn, p.pn, j.jn FROM "j" INNER JOIN "p" ON ("j"."pn" = "p"."pn")
        INNER JOIN "s" ON ("p"."sn" = "s"."sn") WHERE "j"."city" IS NOT NULL"#,
    );
}

// Test96: LEFT JOIN not rewritten due to missing null-rejecting filter
#[test]
fn test95() {
    let original_text = r#"
SELECT s.sname, p.pname
FROM s LEFT OUTER JOIN p ON s.sn = p.sn"#;

    test_it("test95", original_text, original_text);
}

// Skip inlining into left join RHS, even it's a bare bone select, but projecting a literal.
#[test]
fn test96() {
    let original_text = r#"
SELECT "a"."rownum", "b"."test_int2"
FROM "datatypes" AS "a"
LEFT OUTER JOIN "datatypes2" AS "b" ON ("b"."rownum" = "a"."rownum")
LEFT OUTER JOIN (
  SELECT 1 AS "present_", "c"."test_int" AS "test_int" FROM "datatypes1" AS "c"
) AS "GNL" ON ("GNL"."test_int" = "b"."test_int2")
WHERE ("GNL"."present_" IS NULL);
"#;
    test_it(
        "test96",
        original_text,
        r#"SELECT "a"."rownum", "b"."test_int2" FROM "datatypes" AS "a"
    LEFT OUTER JOIN "datatypes2" AS "b" ON ("a"."rownum" = "b"."rownum")
    LEFT OUTER JOIN (SELECT 1 AS "present_", "c"."test_int" AS "test_int" FROM "datatypes1" AS "c") AS "GNL"
    ON ("b"."test_int2" = "GNL"."test_int")
    WHERE ("GNL"."present_" IS NULL)"#,
    );
}

#[test]
fn test97() {
    let original_text = r#"
    SELECT s.city
    FROM s
    LEFT JOIN (
        SELECT spj.sn, spj.qty + 0 AS qty FROM spj
    ) AS dt ON s.sn = dt.sn
    WHERE dt.qty IS NULL"#;
    test_it(
        "test97",
        original_text,
        r#"SELECT "s"."city" FROM "s" LEFT OUTER JOIN "spj" ON ("s"."sn" = "spj"."sn")
            WHERE (("spj"."qty" + 0) IS NULL)"#,
    );
}

#[test]
fn test98() {
    let original_text = r#"
    SELECT s.city
    FROM s
    LEFT OUTER JOIN (
        SELECT spj.sn, 1 AS present_ FROM spj
    ) AS dt ON s.sn = dt.sn
    WHERE dt.present_ IS NULL"#;
    test_it("test98", original_text, original_text);
}

#[test]
fn test99() {
    let original_text = r#"
    SELECT s.city
    FROM s
    LEFT JOIN (
        SELECT spj.sn, CASE WHEN spj.qty > 0 THEN spj.qty ELSE NULL END AS q FROM spj
    ) AS dt ON s.sn = dt.sn
    WHERE dt.q IS NULL"#;
    test_it(
        "test99",
        original_text,
        r#"SELECT "s"."city" FROM "s" LEFT OUTER JOIN "spj" ON ("s"."sn" = "spj"."sn")
        WHERE (CASE WHEN ("spj"."qty" > 0) THEN "spj"."qty" ELSE NULL END IS NULL)"#,
    );
}

#[test]
fn test100() {
    let original_text = r#"
    SELECT s.city
    FROM s
    LEFT OUTER JOIN (
        SELECT spj.sn, CASE WHEN spj.qty IS NULL THEN 1 ELSE spj.qty END AS q FROM spj
    ) AS dt ON s.sn = dt.sn
    WHERE dt.q IS NULL"#;
    test_it("test100", original_text, original_text);
}

#[test]
fn test101() {
    let original_text = r#"
    SELECT s.city
    FROM s
    LEFT JOIN (
        SELECT spj.sn, spj.qty FROM spj
    ) AS dt ON s.sn = dt.sn
    WHERE dt.qty IS NULL"#;
    test_it(
        "test101",
        original_text,
        r#"SELECT "s"."city" FROM "s" LEFT OUTER JOIN "spj" ON ("s"."sn" = "spj"."sn")
            WHERE ("spj"."qty" IS NULL)"#,
    );
}

// Test103: HAVING refers to expression from projection → must normalize to the expression's alias
#[test]
fn test102() {
    let original_text = r#"
        SELECT spj.sn, SUM(spj.qty) AS total
        FROM spj
        GROUP BY spj.sn
        HAVING sum("spj"."qty") > 100"#;

    test_it(
        "test102",
        original_text,
        r#"SELECT "spj"."sn", sum("spj"."qty") AS "total" FROM "spj" GROUP BY "spj"."sn" HAVING (total > 100)"#,
    );
}

#[test]
fn test103() {
    let original_text = r#"
SELECT
	shipping.supp_nation,
	shipping.cust_nation,
	shipping.l_year,
	sum(shipping.volume) as revenue
from
	(SELECT
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from lineitem.l_shipdate) as l_year,
			lineitem.l_extendedprice * (1 - lineitem.l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			supplier.s_suppkey = lineitem.l_suppkey
			and orders.o_orderkey = lineitem.l_orderkey
			and customer.c_custkey = orders.o_custkey
			and supplier.s_nationkey = n1.n_nationkey
			and customer.c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
			)
			and lineitem.l_shipdate between '1990-01-01' and '1996-12-31'
			-- and lineitem.l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	shipping.supp_nation,
	shipping.cust_nation,
	shipping.l_year
order by
	shipping.supp_nation,
	shipping.cust_nation,
	shipping.l_year;
"#;

    test_it(
        "test103",
        original_text,
        r#"SELECT "n1"."n_name" AS "supp_nation", "n2"."n_name" AS "cust_nation", extract(YEAR FROM "lineitem"."l_shipdate") AS "l_year",
        sum(("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount"))) AS "revenue"
        FROM "nation" AS "n1"
        INNER JOIN "supplier" ON ("n1"."n_nationkey" = "supplier"."s_nationkey")
        INNER JOIN "lineitem" ON ("supplier"."s_suppkey" = "lineitem"."l_suppkey")
        INNER JOIN "orders" ON ("lineitem"."l_orderkey" = "orders"."o_orderkey")
        INNER JOIN "customer" ON ("orders"."o_custkey" = "customer"."c_custkey")
        INNER JOIN "nation" AS "n2" ON ("customer"."c_nationkey" = "n2"."n_nationkey")
        WHERE (((("n1"."n_name" = 'FRANCE') AND ("n2"."n_name" = 'GERMANY')) OR (("n1"."n_name" = 'GERMANY')
        AND ("n2"."n_name" = 'FRANCE'))) AND "lineitem"."l_shipdate" BETWEEN '1990-01-01' AND '1996-12-31')
        GROUP BY "n1"."n_name", "n2"."n_name", "l_year"
        ORDER BY "n1"."n_name" NULLS LAST, "n2"."n_name" NULLS LAST, "l_year" NULLS LAST"#,
    );
}

// Basic: GROUP BY == SELECT (no HAVING/ORDER BY) → DISTINCT
#[test]
fn test104() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
GROUP BY s.sn
"#;
    test_it(
        "test104",
        original_text,
        r#"SELECT DISTINCT "s"."sn" FROM "qa"."s" AS "s""#,
    );
}

// Aliased projection; GROUP BY via alias → DISTINCT keeps alias as a projected column
#[test]
fn test105() {
    let original_text = r#"
SELECT s.sn AS x
FROM qa.s AS s
GROUP BY x
"#;
    test_it(
        "test105",
        original_text,
        r#"SELECT DISTINCT "s"."sn" AS "x" FROM "qa"."s" AS "s""#,
    );
}

// ORDER BY expression not in SELECT (and not an ordinal) → must NOT rewrite
#[test]
fn test106() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
GROUP BY s.sn
ORDER BY s.sn || ''
"#;
    test_it("test106", original_text, original_text);
}

// ORDER BY ordinal is compatible with DISTINCT → rewrite, keep ORDER BY 2
#[test]
fn test107() {
    let original_text = r#"
SELECT s.sn, s.city
FROM qa.s AS s
GROUP BY s.sn, s.city
ORDER BY 2
"#;

    test_it(
        "test107",
        original_text,
        r#"SELECT DISTINCT "s"."sn", "s"."city" FROM "qa"."s" AS "s" ORDER BY 2"#,
    );
}

// Window function present in ORDER BY → conservatively skip rewrite
#[test]
fn test108() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
GROUP BY s.sn
ORDER BY ROW_NUMBER() OVER (ORDER BY s.sn)
"#;
    test_it("test108", original_text, original_text);
}

// GROUP BY ordinal pointing to an aliased SELECT item → resolve to alias and rewrite
#[test]
fn test109() {
    let original_text = r#"
SELECT s.sn AS x
FROM qa.s AS s
GROUP BY 1
"#;
    test_it(
        "test109",
        original_text,
        r#"SELECT DISTINCT "s"."sn" AS "x" FROM "qa"."s" AS "s""#,
    );
}

#[test]
fn test110() {
    let original_text = r#"
SELECT coalesce("array_subq"."agg_result", ARRAY[]::TEXT[])
FROM "dogs" AS "d"
LEFT OUTER JOIN (
   SELECT array_agg("inner_subq"."tag" ORDER BY "inner_subq"."tag" DESC NULLS FIRST) AS "agg_result", "inner_subq"."d_id" AS "d_id"
   FROM (
      SELECT "tags"."tag", "tags"."d_id" AS "d_id" FROM "tags" ORDER BY "tags"."tag" DESC NULLS FIRST
   ) AS "inner_subq"
   GROUP BY "inner_subq"."d_id"
) AS "array_subq" ON ("array_subq"."d_id" = "d"."id")
WHERE ("d"."id" = 1)"#;
    test_it(
        "test110",
        original_text,
        r#"SELECT coalesce("array_subq"."agg_result", (ARRAY[]::TEXT[])) FROM "dogs" AS "d"
        LEFT OUTER JOIN (SELECT array_agg("tags"."tag" ORDER BY "tags"."tag" DESC NULLS FIRST) AS "agg_result", "tags"."d_id" AS "d_id"
        FROM "tags" GROUP BY "tags"."d_id") AS "array_subq" ON ("d"."id" = "array_subq"."d_id")
        WHERE ("d"."id" = 1)"#,
    );
}

// Should not promote LEFT -> INNER join, as WHERE uses CONCAT that is not null-rejecting
#[test]
fn test111() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE concat(s.cnt::text, '') = '';
"#;
    test_it(
        "test111",
        original_text,
        r#"SELECT "t"."id" FROM "t" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "u") AS "s" ON ("t"."id" = "s"."cnt") WHERE (concat(("s"."cnt"::TEXT), '') = '')"#,
    );
}

// Should promote LEFT -> INNER join: LOWER is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test112() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE lower(s.cnt::text) = '1';
"#;
    test_it(
        "test112",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (lower(("s"."cnt"::TEXT)) = '1')"#,
    );
}

// Should promote LEFT -> INNER join: SUBSTRING is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test113() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE substring(s.cnt::text, 1, 1) = '1';
"#;
    test_it(
        "test113",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (substring(("s"."cnt"::TEXT), 1, 1) = '1')"#,
    );
}

// Should promote LEFT -> INNER join: SPLIT_PART is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test114() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE split_part(s.cnt::text, '1', 1) = '';
"#;
    test_it(
        "test114",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (split_part(("s"."cnt"::TEXT), '1', 1) = '')"#,
    );
}

// Should promote LEFT -> INNER join: LENGTH is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test115() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE length(s.cnt::text) > 0;
"#;
    test_it(
        "test115",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (length(("s"."cnt"::TEXT)) > 0)"#,
    );
}

// Should promote LEFT -> INNER join: ROUND is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test116() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE round(s.cnt::numeric) = 1;
"#;
    test_it(
        "test116",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (round(("s"."cnt"::NUMERIC)) = 1)"#,
    );
}

// Should promote LEFT -> INNER join: ASCII is null-rejecting (strict) w.r.t. NULL-extended RHS
#[test]
fn test117() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE ascii(s.cnt::text) > 0;
"#;
    test_it(
        "test117",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (ascii(("s"."cnt"::TEXT)) > 0)"#,
    );
}

// Should not promote LEFT -> INNER join: COALESCE is null-tolerant (not null-rejecting)
#[test]
fn test118() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE coalesce(s.cnt::text, '') = '';
"#;
    test_it(
        "test118",
        original_text,
        r#"SELECT "t"."id" FROM "t" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "u") AS "s" ON ("t"."id" = "s"."cnt")
        WHERE (coalesce(("s"."cnt"::TEXT), '') = '')"#,
    );
}

// Should not promote LEFT -> INNER join: CASE expression is null-tolerant (not null-rejecting)
#[test]
fn test119() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE (CASE WHEN s.cnt IS NULL THEN '' ELSE s.cnt::text END) = '';
"#;
    test_it(
        "test119",
        original_text,
        r#"SELECT "t"."id" FROM "t" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "u") AS "s" ON ("t"."id" = "s"."cnt")
        WHERE (CASE WHEN ("s"."cnt" IS NULL) THEN '' ELSE ("s"."cnt"::TEXT) END = '')"#,
    );
}

// Should not promote LEFT -> INNER join: CONCAT_WS ignores NULL arguments (null-tolerant)
#[test]
fn test120() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE concat_ws('', s.cnt::text, '') = '';
"#;
    test_it(
        "test120",
        original_text,
        r#"SELECT "t"."id" FROM "t" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "u") AS "s" ON ("t"."id" = "s"."cnt")
        WHERE (concat_ws('', ("s"."cnt"::TEXT), '') = '')"#,
    );
}

// Should not promote LEFT -> INNER join: OR makes predicate null-tolerant (not null-rejecting)
#[test]
fn test121() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE (s.cnt IS NULL OR s.cnt = 1);
"#;
    test_it(
        "test121",
        original_text,
        r#"SELECT "t"."id" FROM "t" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "u") AS "s" ON ("t"."id" = "s"."cnt")
        WHERE (("s"."cnt" IS NULL) OR ("s"."cnt" = 1))"#,
    );
}

// Should promote LEFT -> INNER join: DATE_TRUNC is strict (null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test122() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE date_trunc('day', CAST(s.cnt AS TIMESTAMP)) = CURRENT_TIMESTAMP;
"#;
    test_it(
        "test122",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (date_trunc('day', CAST("s"."cnt" as TIMESTAMP)) = current_timestamp)"#,
    );
}

// Should promote LEFT -> INNER join: EXTRACT is strict (null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test123() {
    let original_text = r#"
SELECT t.id
FROM t LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM u) AS s ON t.id = s.cnt
WHERE extract(YEAR FROM CAST(s.cnt AS TIMESTAMP)) = 1970;
"#;
    test_it(
        "test123",
        original_text,
        r#"SELECT "t"."id" FROM (SELECT count(*) AS "cnt" FROM "u") AS "s" INNER JOIN "t"
        ON ("s"."cnt" = "t"."id") WHERE (extract(YEAR FROM CAST("s"."cnt" as TIMESTAMP)) = 1970)"#,
    );
}

// Should not promote LEFT -> INNER join: CONCAT is not strict (not null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test124() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE concat(d.cnt::text, '') = '';
"#;

    test_it(
        "test124",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "p") AS "d" ON ("s"."status" = "d"."cnt")
        WHERE (concat(("d"."cnt"::TEXT), '') = '')"#,
    );
}

// Should promote LEFT -> INNER join: LOWER is strict (null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test125() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE lower(d.cnt::text) = '1';
"#;

    test_it(
        "test125",
        original_text,
        r#"SELECT "s"."sn" FROM (SELECT count(*) AS "cnt" FROM "p") AS "d" INNER JOIN "s"
        ON ("d"."cnt" = "s"."status") WHERE (lower(("d"."cnt"::TEXT)) = '1')"#,
    );
}

// Should promote LEFT -> INNER join: SUBSTRING is strict (null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test126() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE substring(d.cnt::text, 1, 1) = '1';
"#;

    test_it(
        "test126",
        original_text,
        r#"SELECT "s"."sn" FROM (SELECT count(*) AS "cnt" FROM "p") AS "d" INNER JOIN "s"
        ON ("d"."cnt" = "s"."status") WHERE (substring(("d"."cnt"::TEXT), 1, 1) = '1')"#,
    );
}

// Should not promote LEFT -> INNER join: COALESCE is not strict (not null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test127() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE coalesce(d.cnt::text, '') = '';
"#;

    test_it(
        "test127",
        original_text,
        r#"SELECT "s"."sn" FROM "s" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt" FROM "p") AS "d" ON ("s"."status" = "d"."cnt")
        WHERE (coalesce(("d"."cnt"::TEXT), '') = '')"#,
    );
}

// Should promote LEFT -> INNER join: EXTRACT is strict (null-rejecting) w.r.t. NULL-extended RHS
#[test]
fn test128() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE extract(YEAR FROM CAST(d.cnt AS TIMESTAMP)) = 1970;
"#;

    test_it(
        "test128",
        original_text,
        r#"SELECT "s"."sn" FROM (SELECT count(*) AS "cnt" FROM "p") AS "d" INNER JOIN "s"
        ON ("d"."cnt" = "s"."status") WHERE (extract(YEAR FROM CAST("d"."cnt" as TIMESTAMP)) = 1970)"#,
    );
}

// The key property: no reference to "v" remains after inlining.
#[test]
fn test129() {
    let original_text = r#"
SELECT s.sn
FROM (
  SELECT s.sn AS sn, lower(s.city) AS lc
  FROM s
) AS v
INNER JOIN p ON v.lc = p.city;
"#;

    test_it(
        "test129",
        original_text,
        r#"SELECT "s"."sn" FROM "p" CROSS JOIN "s"  WHERE (lower("s"."city") = "p"."city")"#,
    );
}

// If someone incorrectly marks NULLIF as null-rejecting in arg1, this might promote LOJ->IJ (wrong).
#[test]
fn test130() {
    let original_text = r#"
SELECT s.sn
FROM s LEFT OUTER JOIN (SELECT COUNT(*) AS cnt FROM p) AS d ON s.status = d.cnt
WHERE nullif(d.cnt::text, 'x') = 'y';
"#;

    test_it("test130", original_text, original_text);
}

// HAVING should count as null-rejecting evidence for LOJ->IJ promotion
#[test]
fn test131() {
    let original_text = r#"
SELECT s.sn
FROM s
LEFT JOIN (
  SELECT spj.sn, COUNT(*) AS c
  FROM spj
  GROUP BY spj.sn
) AS d ON d.sn = s.sn
GROUP BY s.sn, d.c
HAVING d.c > 0;
"#;

    // Expect INNER JOIN after normalization because HAVING references d.c in a strict predicate
    test_it(
        "test131",
        original_text,
        r#"SELECT "s"."sn" FROM (SELECT "spj"."sn", count(*) AS "c" FROM "spj" GROUP BY "spj"."sn") AS "d"
        INNER JOIN "s" ON ("d"."sn" = "s"."sn") WHERE ("d"."c" > 0) GROUP BY "s"."sn", "d"."c""#,
    );
}

// Nontrivial ON gets moved to WHERE before inlining; substitution must rewrite it correctly.
#[test]
fn test132() {
    let original_text = r#"
SELECT s.sn
FROM s
INNER JOIN (
  SELECT concat(spj.sn, '') AS x, spj.qty
  FROM spj
) AS d ON d.x = s.sn
WHERE s.sn = 'S1';
"#;

    // Expected: d is inlined, ON becomes empty/TRUE (INNER join), and the moved predicate
    // appears in WHERE with d.x substituted to concat(spj.sn,'')
    test_it(
        "test132",
        original_text,
        r#"SELECT "s"."sn" FROM "s" CROSS JOIN "spj"
        WHERE (("s"."sn" = 'S1') AND (concat("spj"."sn", '') = "s"."sn"))"#,
    );
}

// If the path to the very top WHERE is clear, hoist *all the way*.
#[test]
fn test133() {
    let original_text = r#"
SELECT outer_q.sn
FROM (
    SELECT mid0.sn
    FROM (
        SELECT s.sn, ROW_NUMBER() OVER (ORDER BY s.sn) AS rn
        FROM qa.s AS s
        WHERE s.city = 'ATHENS'
    ) AS mid0
    WHERE mid0.sn > 'S20000' and mid0.rn > 3
) AS outer_q
JOIN qa.spj AS spj ON outer_q.sn = spj.sn
WHERE spj.qty = 100;
"#;

    test_it(
        "test133",
        original_text,
        r#"SELECT "mid0"."sn" FROM (SELECT "s"."sn", ROW_NUMBER() OVER(ORDER BY "s"."sn" ASC NULLS LAST) AS "rn"
        FROM "qa"."s" AS "s" WHERE ("s"."city" = 'ATHENS')) AS "mid0" INNER JOIN "qa"."spj" AS "spj" ON ("mid0"."sn" = "spj"."sn")
        WHERE (("spj"."qty" = 100) AND (("mid0"."sn" > 'S20000') AND ("mid0"."rn" > 3)))"#,
    );
}

// If the path to the very top WHERE is *not* clear (LEFT JOIN barrier),
// do not hoist anything at all (no partial hoist within the RHS subtree either).
// The RHS is made non-trivially inlinable by including an unused window function.
#[test]
fn test134() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
LEFT JOIN (
    SELECT rhs0.sn, max(rhs0.rn) rn
    FROM (
        SELECT spj.sn, ROW_NUMBER() OVER (ORDER BY spj.sn) AS rn
        FROM qa.spj AS spj
        JOIN qa.p AS p ON spj.pn = p.pn
        WHERE p.color = 'RED'
    ) AS rhs0
    GROUP BY rhs0.sn
) AS rhs ON s.sn = rhs.sn and s.status = rhs.rn;
"#;

    // The filter p.color='RED' must remain inside the deepest RHS subquery.
    test_it(
        "test134",
        original_text,
        r#"SELECT "s"."sn" FROM "qa"."s" AS "s" LEFT OUTER JOIN (SELECT "rhs0"."sn", max("rhs0"."rn") AS "rn"
        FROM (SELECT "spj"."sn", ROW_NUMBER() OVER(ORDER BY "spj"."sn" ASC NULLS LAST) AS "rn" FROM "qa"."p" AS "p"
        INNER JOIN "qa"."spj" AS "spj" ON ("p"."pn" = "spj"."pn") WHERE ("p"."color" = 'RED')) AS "rhs0"
        GROUP BY "rhs0"."sn") AS "rhs" ON (("s"."sn" = "rhs"."sn") AND ("s"."status" = "rhs"."rn"))"#,
    );
}

// If the path to the very top WHERE is *not* clear (LEFT JOIN barrier),
// do not hoist anything at all (no partial hoist inside the RHS subtree).
// 6 layers: every ROW_NUMBER() output is referenced upstream, so it cannot be dropped.
#[test]
fn test135() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
LEFT JOIN (
    SELECT l6.sn, max(l6.rn6) AS rn6
    FROM (
        SELECT l5.sn, l5.rn5,
               ROW_NUMBER() OVER (ORDER BY l5.sn) AS rn6
        FROM (
            SELECT l4.sn, l4.rn4,
                   ROW_NUMBER() OVER (ORDER BY l4.sn) AS rn5
            FROM (
                SELECT l3.sn, l3.rn3,
                       ROW_NUMBER() OVER (ORDER BY l3.sn) AS rn4
                FROM (
                    SELECT l2.sn, l2.rn2,
                           ROW_NUMBER() OVER (ORDER BY l2.sn) AS rn3
                    FROM (
                        SELECT l1.sn, l1.rn1,
                               ROW_NUMBER() OVER (ORDER BY l1.sn) AS rn2
                        FROM (
                            SELECT spj.sn,
                                   ROW_NUMBER() OVER (ORDER BY spj.sn) AS rn1
                            FROM qa.spj AS spj
                            JOIN qa.p AS p ON spj.pn = p.pn
                            WHERE p.color = 'RED' AND spj.qty >= 100
                        ) AS l1
                        WHERE l1.rn1 > 10
                    ) AS l2
                    WHERE l2.rn2 > 20
                ) AS l3
                WHERE l3.rn3 > 30
            ) AS l4
            WHERE l4.rn4 > 40
        ) AS l5
        WHERE l5.rn5 > 50
    ) AS l6
    GROUP BY l6.sn
) AS rhs
  ON (s.sn = rhs.sn AND s.status = rhs.rn6)
WHERE s.city = 'ATHENS';
"#;

    // The critical property: all RHS filters stay in their original layers.
    // In particular: (p.color='RED' AND spj.qty>=100) must remain at the deepest layer.
    test_it(
        "test135",
        original_text,
        r#"SELECT "s"."sn" FROM "qa"."s" AS "s" LEFT OUTER JOIN
        (SELECT "l6"."sn", max("l6"."rn6") AS "rn6" FROM
            (SELECT "l5"."sn", "l5"."rn5", ROW_NUMBER() OVER(ORDER BY "l5"."sn" ASC NULLS LAST) AS "rn6" FROM
                (SELECT "l4"."sn", "l4"."rn4", ROW_NUMBER() OVER(ORDER BY "l4"."sn" ASC NULLS LAST) AS "rn5" FROM
                    (SELECT "l3"."sn", "l3"."rn3", ROW_NUMBER() OVER(ORDER BY "l3"."sn" ASC NULLS LAST) AS "rn4" FROM
                        (SELECT "l2"."sn", "l2"."rn2", ROW_NUMBER() OVER(ORDER BY "l2"."sn" ASC NULLS LAST) AS "rn3" FROM
                            (SELECT "l1"."sn", "l1"."rn1", ROW_NUMBER() OVER(ORDER BY "l1"."sn" ASC NULLS LAST) AS "rn2" FROM
                                (SELECT "spj"."sn", ROW_NUMBER() OVER(ORDER BY "spj"."sn" ASC NULLS LAST) AS "rn1"
                                 FROM "qa"."p" AS "p" INNER JOIN "qa"."spj" AS "spj" ON ("p"."pn" = "spj"."pn")
                                 WHERE (("p"."color" = 'RED') AND ("spj"."qty" >= 100))) AS "l1"
                             WHERE ("l1"."rn1" > 10)) AS "l2"
                         WHERE ("l2"."rn2" > 20)) AS "l3"
                     WHERE ("l3"."rn3" > 30)) AS "l4"
                 WHERE ("l4"."rn4" > 40)) AS "l5"
             WHERE ("l5"."rn5" > 50)) AS "l6"
         GROUP BY "l6"."sn") AS "rhs"
        ON ((("s"."sn" = "rhs"."sn") AND ("s"."status" = "rhs"."rn6")))
        WHERE ("s"."city" = 'ATHENS')"#,
    );
}

// Two independent hoist chains inside RHS: one based on spj.qty, one based on rn.
// This catches partial-hoist behavior that "selectively" hoists some conjuncts.
#[test]
fn test136() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
LEFT JOIN (
    SELECT l5.sn, l5.qty, l5.rn5
    FROM (
        SELECT l4.sn, l4.qty, l4.rn4,
               ROW_NUMBER() OVER (ORDER BY l4.sn) AS rn5
        FROM (
            SELECT l3.sn, l3.qty, l3.rn3,
                   ROW_NUMBER() OVER (ORDER BY l3.sn) AS rn4
            FROM (
                SELECT l2.sn, l2.qty, l2.rn2,
                       ROW_NUMBER() OVER (ORDER BY l2.sn) AS rn3
                FROM (
                    SELECT l1.sn, l1.qty, l1.rn1,
                           ROW_NUMBER() OVER (ORDER BY l1.sn) AS rn2
                    FROM (
                        SELECT spj.sn, spj.qty,
                               ROW_NUMBER() OVER (ORDER BY spj.sn) AS rn1
                        FROM qa.spj AS spj
                        JOIN qa.p AS p ON spj.pn = p.pn
                        WHERE p.color = 'RED'
                    ) AS l1
                    WHERE l1.qty >= 100 AND l1.rn1 > 10
                ) AS l2
                WHERE l2.qty >= 200 AND l2.rn2 > 20
            ) AS l3
            WHERE l3.qty >= 300 AND l3.rn3 > 30
        ) AS l4
        WHERE l4.qty >= 400 AND l4.rn4 > 40
    ) AS l5
    WHERE l5.rn5 > 50
) AS rhs
  ON (s.sn = rhs.sn)
WHERE s.city = 'ATHENS';
"#;

    test_it(
        "test136",
        original_text,
        r#"SELECT "s"."sn" FROM "qa"."s" AS "s" LEFT OUTER JOIN (SELECT "l4"."sn", "l4"."qty", "l4"."rn4",
        ROW_NUMBER() OVER(ORDER BY "l4"."sn" ASC NULLS LAST) AS "rn5" FROM (SELECT "l3"."sn", "l3"."qty", "l3"."rn3",
        ROW_NUMBER() OVER(ORDER BY "l3"."sn" ASC NULLS LAST) AS "rn4" FROM (SELECT "l2"."sn", "l2"."qty", "l2"."rn2",
        ROW_NUMBER() OVER(ORDER BY "l2"."sn" ASC NULLS LAST) AS "rn3" FROM (SELECT "l1"."sn", "l1"."qty", "l1"."rn1",
        ROW_NUMBER() OVER(ORDER BY "l1"."sn" ASC NULLS LAST) AS "rn2" FROM (SELECT "spj"."sn", "spj"."qty",
        ROW_NUMBER() OVER(ORDER BY "spj"."sn" ASC NULLS LAST) AS "rn1" FROM "qa"."p" AS "p" INNER JOIN "qa"."spj" AS "spj"
        ON ("p"."pn" = "spj"."pn") WHERE ("p"."color" = 'RED')) AS "l1" WHERE (("l1"."qty" >= 100) AND ("l1"."rn1" > 10))) AS "l2"
        WHERE (("l2"."qty" >= 200) AND ("l2"."rn2" > 20))) AS "l3" WHERE (("l3"."qty" >= 300) AND ("l3"."rn3" > 30))) AS "l4"
        WHERE (("l4"."qty" >= 400) AND ("l4"."rn4" > 40))) AS "l5" ON (("s"."sn" = "l5"."sn") AND ("l5"."rn5" > 50))
        WHERE ("s"."city" = 'ATHENS')"#,
    );
}

// LEFT JOIN barrier blocks hoisting to top WHERE, even if there is an INNER JOIN above.
// Goal 1: hoist-all-the-way OR hoist-nothing. This should be hoist-nothing.
#[test]
fn test137() {
    let original_text = r#"
SELECT s.sn
FROM qa.s AS s
INNER JOIN qa.j AS j ON s.jn = j.jn
LEFT JOIN (
    SELECT l5.sn, l5.rn5
    FROM (
        SELECT l4.sn, l4.rn4,
               ROW_NUMBER() OVER (ORDER BY l4.sn) AS rn5
        FROM (
            SELECT l3.sn, l3.rn3,
                   ROW_NUMBER() OVER (ORDER BY l3.sn) AS rn4
            FROM (
                SELECT l2.sn, l2.rn2,
                       ROW_NUMBER() OVER (ORDER BY l2.sn) AS rn3
                FROM (
                    SELECT l1.sn, l1.rn1,
                           ROW_NUMBER() OVER (ORDER BY l1.sn) AS rn2
                    FROM (
                        SELECT spj.sn,
                               ROW_NUMBER() OVER (ORDER BY spj.sn) AS rn1
                        FROM qa.spj AS spj
                        JOIN qa.p AS p ON spj.pn = p.pn
                        WHERE p.color = 'RED' AND spj.qty >= 100
                    ) AS l1
                    WHERE l1.rn1 > 10
                ) AS l2
                WHERE l2.rn2 > 20
            ) AS l3
            WHERE l3.rn3 > 30
        ) AS l4
        WHERE l4.rn4 > 40
    ) AS l5
    WHERE l5.rn5 > 50
) AS rhs
  ON (s.sn = rhs.sn)
WHERE j.city = 'ATHENS';
"#;

    test_it(
        "test137",
        original_text,
        r#"SELECT "s"."sn" FROM "qa"."j" AS "j" INNER JOIN "qa"."s" AS "s" ON ("j"."jn" = "s"."jn") LEFT OUTER JOIN
        (SELECT "l4"."sn", "l4"."rn4", ROW_NUMBER() OVER(ORDER BY "l4"."sn" ASC NULLS LAST) AS "rn5" FROM (SELECT "l3"."sn", "l3"."rn3",
        ROW_NUMBER() OVER(ORDER BY "l3"."sn" ASC NULLS LAST) AS "rn4" FROM (SELECT "l2"."sn", "l2"."rn2",
        ROW_NUMBER() OVER(ORDER BY "l2"."sn" ASC NULLS LAST) AS "rn3" FROM (SELECT "l1"."sn", "l1"."rn1",
        ROW_NUMBER() OVER(ORDER BY "l1"."sn" ASC NULLS LAST) AS "rn2" FROM (SELECT "spj"."sn",
        ROW_NUMBER() OVER(ORDER BY "spj"."sn" ASC NULLS LAST) AS "rn1" FROM "qa"."p" AS "p" INNER JOIN "qa"."spj" AS "spj"
        ON ("p"."pn" = "spj"."pn") WHERE (("p"."color" = 'RED') AND ("spj"."qty" >= 100))) AS "l1"
        WHERE ("l1"."rn1" > 10)) AS "l2"
        WHERE ("l2"."rn2" > 20)) AS "l3"
        WHERE ("l3"."rn3" > 30)) AS "l4"
        WHERE ("l4"."rn4" > 40)) AS "l5"
        ON (("s"."sn" = "l5"."sn") AND ("l5"."rn5" > 50))
        WHERE ("j"."city" = 'ATHENS')"#,
    );
}

// Tests for enhanced HAVING → WHERE hoisting (Case 2: non-aggregated GROUP BY column filters)
#[test]
fn test138() {
    // Test Case 2: Complex expression (a + b) using only GROUP BY columns
    // Should move to WHERE because both 'a' and 'b' are GROUP BY keys
    let original_text = r#"
        SELECT a, b, COUNT(*) as cnt
        FROM t
        GROUP BY a, b
        HAVING a + b > 10
    "#;

    test_it(
        "test138",
        original_text,
        r#"SELECT "a", "b", COUNT(*) AS "cnt"
           FROM "t"
           WHERE (("a" + "b") > 10)
           GROUP BY "a", "b""#,
    );
}

#[test]
fn test139() {
    // Test Case 2: OR condition using only GROUP BY columns
    // Should move to WHERE because all columns (dept, status) are GROUP BY keys
    let original_text = r#"
        SELECT dept, status, COUNT(*) as cnt
        FROM employees
        GROUP BY dept, status
        HAVING dept > 100 OR status = 'active'
    "#;

    test_it(
        "test139",
        original_text,
        r#"SELECT "dept", "status", COUNT(*) AS "cnt"
           FROM "employees"
           WHERE (("dept" > 100) OR ("status" = 'active'))
           GROUP BY "dept", "status""#,
    );
}

#[test]
fn test140() {
    // Test mixed: aggregated filter stays in HAVING, non-aggregated moves to WHERE
    let original_text = r#"
        SELECT dept, COUNT(*) as cnt
        FROM employees
        GROUP BY dept
        HAVING COUNT(*) > 10 AND dept IN (1, 2, 3)
    "#;

    test_it(
        "test140",
        original_text,
        r#"SELECT "dept", COUNT(*) AS "cnt"
           FROM "employees"
           WHERE ("dept" IN (1, 2, 3))
           GROUP BY "dept"
           HAVING ("cnt" > 10)"#,
    );
}

#[test]
fn test141() {
    // Test that purely aggregated expressions are NOT moved
    let original_text = r#"
        SELECT dept, SUM(salary) as total_salary
        FROM employees
        GROUP BY dept
        HAVING SUM(salary) > 100000
    "#;

    test_it(
        "test141",
        original_text,
        r#"SELECT "dept", SUM("salary") AS "total_salary"
           FROM "employees"
           GROUP BY "dept"
           HAVING ("total_salary" > 100000)"#,
    );
}

#[test]
fn test142() {
    // Test Case 1: Parametrizable-like filter with literal (col = literal)
    let original_text = r#"
        SELECT dept_id, COUNT(*) as cnt
        FROM employees
        GROUP BY dept_id
        HAVING dept_id = 42
    "#;

    test_it(
        "test142",
        original_text,
        r#"SELECT "dept_id", COUNT(*) AS "cnt"
           FROM "employees"
           WHERE ("dept_id" = 42)
           GROUP BY "dept_id""#,
    );
}

#[test]
fn test143() {
    // Test Case 2: Compound AND condition with multiple GROUP BY columns
    let original_text = r#"
        SELECT region, dept, status, COUNT(*) as cnt
        FROM employees
        GROUP BY region, dept, status
        HAVING region = 'US' AND dept > 100 AND status IN ('active', 'pending')
    "#;

    test_it(
        "test143",
        original_text,
        r#"SELECT "region", "dept", "status", COUNT(*) AS "cnt"
           FROM "employees"
           WHERE ((("region" = 'US') AND ("dept" > 100)) AND ("status" IN ('active', 'pending')))
           GROUP BY "region", "dept", "status""#,
    );
}

#[test]
fn test144() {
    // Test that filters referencing non-GROUP BY columns are NOT moved
    let original_text = r#"
        SELECT dept, COUNT(*) as cnt
        FROM employees
        GROUP BY dept
        HAVING salary > 50000
    "#;

    // Should keep in HAVING because 'salary' is not a GROUP BY column
    test_it(
        "test144",
        original_text,
        r#"SELECT "dept", COUNT(*) AS "cnt"
           FROM "employees"
           GROUP BY "dept"
           HAVING ("salary" > 50000)"#,
    );
}

// Self-join bail-out: inlining sq would put t1 twice in FROM/JOINs.
// Inlining is blocked, but normalize_joins_shape still reshapes the query.
#[test]
fn self_join_bail_out() {
    let original = r#"
        SELECT "t1"."id", "sq"."val"
        FROM "t1"
        JOIN (SELECT "t1"."id", "t1"."val" FROM "t1" WHERE "t1"."active") AS "sq"
        ON "t1"."id" = "sq"."id"
    "#;
    test_it(
        "self_join_bail_out",
        original,
        r#"SELECT "t1"."id", "sq"."val"
        FROM (SELECT "t1"."id", "t1"."val" FROM "t1" WHERE "t1"."active") AS "sq"
        CROSS JOIN "t1"
        WHERE ("t1"."id" = "sq"."id")"#,
    );
}

/// Duplicate alias deduplication with window functions across nested subqueries.
///
/// The input has three nesting levels, both aliased "INNER":
///   - Level 3 (innermost): projects a *partitioned* ROW_NUMBER(__rn) from base table "dt"
///   - Level 2 (middle): joins level 3 with another table and filters on __rn <= 1;
///     also projects its own *non-partitioned* ROW_NUMBER(__rn)
///   - Level 1 (outermost): selects from level 2 with a CROSS JOIN
///
/// Key behaviors verified:
///   1. Level 3 cannot be inlined into level 2 because level 2's WHERE
///      references INNER.__rn which maps to a window function (WF guard).
///   2. Level 2 IS inlined into level 1 (level 1 doesn't reference __rn).
///   3. During level 2's inlining, `make_aliases_distinct_from_base_statement`
///      detects that level 2's internal subquery reuses the external alias
///      "INNER" and renames it to "INNER1", preventing the absorbed WHERE
///      (INNER.__rn <= 1) from being incorrectly substituted by
///      `replace_columns_with_inlinable_expr`.
///   4. The partitioned ROW_NUMBER from level 3 is preserved inside the
///      derived table — no window function leaks into WHERE.
#[test]
fn test146() {
    let original_text = r#"
SELECT
           "INNER"."test_integer2" AS "t2"
       FROM
           (SELECT
                   "INNER"."test_integer2" AS "test_integer2",
                   ROW_NUMBER() OVER(
                       ORDER BY
                           "INNER"."test_integer2" ASC NULLS LAST
                   ) AS "__rn"
               FROM
                   (SELECT
                           "dt"."test_integer2" AS "test_integer2",
                           "dt"."rownum" AS "rownum",
                           ROW_NUMBER() OVER(
                               PARTITION BY "dt"."test_integer2"
                               ORDER BY
                                   "dt"."rownum" ASC NULLS LAST
                           ) AS "__rn"
                       FROM
                           "rea6496c_dt3" AS "dt"
                   ) AS "INNER"
                   INNER JOIN "rea6496c_dt" ON (
                       "INNER"."test_integer2" = "rea6496c_dt"."test_integer"
                   )
               WHERE
                   ("INNER"."__rn" <= 1)
           ) AS "INNER"
           CROSS JOIN "rea6496c_s"
       ORDER BY
           1 NULLS LAST;
"#;
    test_it(
        "test146",
        original_text,
        r#"SELECT "INNER"."test_integer2" AS "t2"
        FROM (SELECT "dt"."test_integer2" AS "test_integer2",
                     "dt"."rownum" AS "rownum",
                     ROW_NUMBER() OVER(PARTITION BY "dt"."test_integer2"
                                       ORDER BY "dt"."rownum" ASC NULLS LAST) AS "__rn"
              FROM "rea6496c_dt3" AS "dt") AS "INNER"
        INNER JOIN "rea6496c_dt" ON ("INNER"."test_integer2" = "rea6496c_dt"."test_integer")
        CROSS JOIN "rea6496c_s"
        WHERE ("INNER"."__rn" <= 1)
        ORDER BY 1 NULLS LAST"#,
    );
}

/// Risk A: base-table alias collision during derived-table inlining.
///
/// When a subquery is aliased the same as its internal base table AND the
/// subquery projects a renamed column, the absorbed WHERE can be incorrectly
/// substituted.
///
/// `(SELECT t.y AS z, t.z AS z2 FROM t WHERE t.z > 5) AS t`
///
/// After inlining:
///   - `ext_to_int_fields` maps `t.z → t.y` (from the projection `t.y AS z`
///     under external alias `t`) and `t.z2 → t.z`.
///   - The absorbed WHERE `t.z > 5` has column `t.z` which matches the
///     ext_to_int key — but this `t.z` refers to the *base table* column `z`,
///     not the external projection alias `z`.
///   - Incorrect substitution: `t.z > 5` → `t.y > 5` (wrong column).
///   - Correct result: `SELECT t.y AS z FROM t WHERE t.z > 5`.
///
/// This test documents the known issue.  If it starts passing, the underlying
/// alias-collision bug for base tables has been fixed.
#[test]
fn test_risk_a_base_table_alias_collision() {
    test_it(
        "risk_a_simple",
        r#"SELECT "t"."z"
            FROM (SELECT "t"."y" AS "z", "t"."z" AS "z2"
                  FROM "t"
                  WHERE "t"."z" > 5) AS "t"
            INNER JOIN "s" ON "t"."z" = "s"."a""#,
        r#"SELECT "t"."y" AS "z"
            FROM "s"
            INNER JOIN "t" ON "s"."a" = "t"."y"
            WHERE ("t"."z" > 5)"#,
    );
}

/// Risk A variant: the renamed projection collides with the WHERE column name.
///
/// `(SELECT t.y AS z FROM t WHERE t.z > 5) AS t` — the external projection
/// aliases column `y` to `z`, while the WHERE references the *actual* column
/// `z` on the same base table.  After ext_to_int mapping, `t.z` in the
/// absorbed WHERE incorrectly resolves to `t.y`.
#[test]
fn test_risk_a_renamed_projection_collides_with_where() {
    test_it(
        "risk_a_renamed",
        r#"SELECT "t"."z"
            FROM (SELECT "t"."y" AS "z"
                  FROM "t"
                  WHERE "t"."z" > 5) AS "t"
            INNER JOIN "s" ON "t"."z" = "s"."a""#,
        r#"SELECT "t"."y" AS "z"
            FROM "s"
            INNER JOIN "t" ON "s"."a" = "t"."y"
            WHERE ("t"."z" > 5)"#,
    );
}

// ── contains_subquery_predicates_deep guard ──

/// When a subquery remains in an expression position (e.g., WHERE), the
/// `contains_subquery_predicates_deep` guard at the top of `derived_tables_rewrite_main`
/// silently skips all rewrite passes — the statement is returned unchanged.
#[test]
fn test_subquery_in_where_skips_rewrite() {
    // Query with an un-nested subquery in WHERE — passes through unchanged.
    let sql = r#"SELECT "t"."x" FROM "t" WHERE "t"."x" IN (SELECT "s"."x" FROM "s")"#;
    test_it("subquery_guard", sql, sql);
}

/// Subquery in JOIN ON also triggers the guard (defense-in-depth for §13).
#[test]
fn test_subquery_in_join_on_skips_rewrite() {
    let sql = r#"SELECT "t"."x" FROM "t"
        INNER JOIN "s" ON "t"."x" = (SELECT "u"."x" FROM "u" LIMIT 1)"#;
    // Inlining skipped (JOIN ON has subquery), but comma normalization and
    // join shape normalization still run.
    let expected = r#"SELECT "t"."x" FROM "s"
        CROSS JOIN "t"
        WHERE ("t"."x" = (SELECT "u"."x" FROM "u" LIMIT 1))"#;
    test_it("subquery_guard_join_on", sql, expected);
}

// ── inline_leading_derived_table bail-out coverage ──

/// Non-literal LIMIT in inner subquery should prevent LIMIT composition and bail.
/// The inner has `LIMIT $1` (a placeholder) — cannot compose with outer LIMIT.
/// The subquery stays as-is.
#[test]
fn test_ildt_nonliteral_limit_bail() {
    let sql = r#"SELECT "sq"."x" FROM (SELECT "t"."x" FROM "t" LIMIT $1) AS "sq" LIMIT 5"#;
    // Should bail — subquery stays unchanged (LIMIT $1 cannot be composed).
    test_it("ildt_nonliteral_limit", sql, sql);
}

/// Downstream subquery references inner alias column that maps to an aggregate
/// expression — should bail because the downstream LATERAL would get a non-column
/// expression after substitution.
#[test]
fn test_ildt_downstream_non_trivial_ref_bail() {
    let sql = r#"SELECT "sq"."s", "l"."cnt"
        FROM (SELECT SUM("t"."x") AS "s", "t"."a" FROM "t" GROUP BY "t"."a") AS "sq"
        LEFT JOIN LATERAL (SELECT COUNT(*) AS "cnt" FROM "u"
            WHERE "u"."val" = "sq"."s") AS "l" ON TRUE"#;
    // Should bail — sq.s maps to SUM(t.x), which is non-trivial for downstream LATERAL ref.
    // ON TRUE is normalized to empty constraint; LEFT JOIN → LEFT OUTER JOIN.
    let expected = r#"SELECT "sq"."s", "l"."cnt"
        FROM (SELECT sum("t"."x") AS "s", "t"."a" FROM "t" GROUP BY "t"."a") AS "sq"
        LEFT OUTER JOIN LATERAL (SELECT count(*) AS "cnt" FROM "u"
            WHERE ("u"."val" = "sq"."s")) AS "l""#;
    test_it("ildt_downstream_non_trivial", sql, expected);
}

// ─── LHS subquery with WHERE into LEFT JOIN ────────────────────────────────

/// Single-table LHS subquery with WHERE, joined via LEFT JOIN.
/// Previously blocked because the WHERE made the inlinable "complex."
/// Safe because the WHERE only references local (LHS) columns, and LEFT JOIN
/// never NULL-extends LHS columns.
#[test]
fn test_lhs_subquery_where_left_join() {
    let sql = r#"SELECT "sq"."a", "t2"."val"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1" WHERE "t1"."a" > 5) AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")"#;
    let expected = r#"SELECT "t1"."a", "t2"."val"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON ("t1"."id" = "t2"."id")
        WHERE ("t1"."a" > 5)"#;
    test_it("lhs_subquery_where_left_join", sql, expected);
}

/// LHS subquery with WHERE, base has multiple LEFT JOINs.
#[test]
fn test_lhs_subquery_where_multiple_left_joins() {
    let sql = r#"SELECT "sq"."a", "t2"."val", "t3"."z"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1" WHERE "t1"."a" > 5) AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")
        LEFT JOIN "t3" ON ("sq"."a" = "t3"."a")"#;
    let expected = r#"SELECT "t1"."a", "t2"."val", "t3"."z"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON ("t1"."id" = "t2"."id")
        LEFT OUTER JOIN "t3" ON ("t1"."a" = "t3"."a")
        WHERE ("t1"."a" > 5)"#;
    test_it("lhs_subquery_where_multiple_left_joins", sql, expected);
}

/// Multi-table LHS subquery with LEFT JOIN — should still be blocked.
/// The multi-table check (tables.len() > 1 || joins non-empty) prevents
/// inlining regardless of WHERE.
#[test]
fn test_lhs_multi_table_subquery_left_join_bail() {
    let sql = r#"SELECT "sq"."a", "t3"."z"
        FROM (SELECT "t1"."a" FROM "t1" INNER JOIN "t2" ON ("t1"."id" = "t2"."id")) AS "sq"
        LEFT JOIN "t3" ON ("sq"."a" = "t3"."a")"#;
    // Should bail — multi-table LHS with LEFT JOIN base.
    let expected = r#"SELECT "sq"."a", "t3"."z"
        FROM (SELECT "t1"."a" FROM "t1" INNER JOIN "t2" ON ("t1"."id" = "t2"."id")) AS "sq"
        LEFT OUTER JOIN "t3" ON ("sq"."a" = "t3"."a")"#;
    test_it("lhs_multi_table_subquery_left_join_bail", sql, expected);
}

// ─── Redundant DISTINCT stripping (ExactlyOne / AtMostOne) ─────────────────

/// DISTINCT + aggregate (SUM) in subquery without GROUP BY — DISTINCT is
/// stripped (ExactlyOne: at most 1 row, DISTINCT is a no-op).  The subquery
/// is still blocked from inlining by the strict aggregation check.
#[test]
fn test_distinct_with_aggregate_stripped() {
    let sql = r#"SELECT "sq"."s", "t2"."val"
        FROM (SELECT DISTINCT SUM("t1"."a") AS "s" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."s" = "t2"."id")"#;
    // DISTINCT stripped (ExactlyOne), but aggregation still blocks inlining.
    let expected = r#"SELECT "sq"."s", "t2"."val"
        FROM (SELECT sum("t1"."a") AS "s" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."s" = "t2"."id")"#;
    test_it("distinct_with_aggregate_stripped", sql, expected);
}

// ─── Regression guards for LHS+WHERE relaxation ────────────────────────────

/// LHS subquery with complex WHERE (OR) + LEFT JOIN — should inline,
/// the OR is single-table and stays in base WHERE.
#[test]
fn test_lhs_subquery_complex_where_left_join() {
    let sql = r#"SELECT "sq"."a", "t2"."val"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1"
              WHERE "t1"."a" > 5 OR "t1"."a" < 2) AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")"#;
    let expected = r#"SELECT "t1"."a", "t2"."val"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON ("t1"."id" = "t2"."id")
        WHERE (("t1"."a" > 5) OR ("t1"."a" < 2))"#;
    test_it("lhs_subquery_complex_where_left_join", sql, expected);
}

/// LHS subquery with WHERE + first join INNER, second LEFT — should
/// inline because the first join is INNER (the check only examines
/// the first join).
#[test]
fn test_lhs_subquery_where_first_inner_second_left() {
    let sql = r#"SELECT "sq"."a", "t2"."val", "t3"."z"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1" WHERE "t1"."a" > 5) AS "sq"
        INNER JOIN "t2" ON ("sq"."id" = "t2"."id")
        LEFT JOIN "t3" ON ("sq"."a" = "t3"."a")"#;
    let expected = r#"SELECT "t1"."a", "t2"."val", "t3"."z"
        FROM "t1"
        INNER JOIN "t2" ON ("t1"."id" = "t2"."id")
        LEFT OUTER JOIN "t3" ON ("t1"."a" = "t3"."a")
        WHERE ("t1"."a" > 5)"#;
    test_it("lhs_subquery_where_first_inner_second_left", sql, expected);
}

/// LHS simple subquery (NO WHERE) + LEFT JOIN — pre-existing behavior,
/// must still work after the relaxation.
#[test]
fn test_lhs_simple_subquery_left_join_still_works() {
    let sql = r#"SELECT "sq"."a", "t2"."val"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1") AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")"#;
    let expected = r#"SELECT "t1"."a", "t2"."val"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON ("t1"."id" = "t2"."id")"#;
    test_it("lhs_simple_subquery_left_join_still_works", sql, expected);
}

/// LHS subquery with WHERE + LEFT JOIN with empty ON — should bail
/// (defense-in-depth guard: ON must have supported join condition).
#[test]
fn test_lhs_subquery_where_left_join_empty_on_bail() {
    let sql = r#"SELECT "sq"."a", "t2"."val"
        FROM (SELECT "t1"."a" FROM "t1" WHERE "t1"."a" > 5) AS "sq"
        LEFT JOIN "t2" ON TRUE"#;
    // Inlining bails (ON TRUE not supported), but hoist_parametrizable_filters
    // extracts the WHERE from the subquery to outer WHERE.
    let expected = r#"SELECT "sq"."a", "t2"."val"
        FROM (SELECT "t1"."a" FROM "t1") AS "sq"
        LEFT OUTER JOIN "t2"
        WHERE ("sq"."a" > 5)"#;
    test_it("lhs_subquery_where_left_join_empty_on_bail", sql, expected);
}

// ─── Deeply disguised ExactlyOne DISTINCT stripping ────────────────────────

/// Wrapper around ExactlyOne: SELECT DISTINCT * FROM (SELECT COUNT(*) ...) sq.
/// The inner is ExactlyOne (count-no-GBY). The outer is a projection-only
/// wrapper with DISTINCT. agg_only_no_gby_cardinality should detect the
/// wrapper shape and classify as ExactlyOne → DISTINCT stripped.
#[test]
fn test_distinct_strip_wrapper_around_count() {
    let sql = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT DISTINCT "inner"."c"
              FROM (SELECT COUNT(*) AS "c" FROM "t1") AS "inner") AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")"#;
    // The inner-inner is ExactlyOne (COUNT no GBY).
    // The wrapper DISTINCT is redundant → stripped.
    // The wrapper subquery (now a simple projection) is inlined into the base.
    // The inner aggregate subquery survives with alias "sq" (from the wrapper).
    let expected = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT count(*) AS "c" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")"#;
    test_it("distinct_strip_wrapper_around_count", sql, expected);
}

/// Wrapper with non-trivial WHERE around ExactlyOne: the wrapper's WHERE
/// degrades ExactlyOne to AtMostOne, but DISTINCT is still redundant on
/// ≤1 row.
#[test]
fn test_distinct_strip_wrapper_with_where() {
    let sql = r#"SELECT "sq"."s", "t2"."val"
        FROM (SELECT DISTINCT "inner"."s"
              FROM (SELECT SUM("t1"."a") AS "s" FROM "t1") AS "inner"
              WHERE "inner"."s" > 0) AS "sq"
        INNER JOIN "t2" ON ("sq"."s" = "t2"."id")"#;
    // Inner is ExactlyOne (SUM no GBY). Wrapper WHERE degrades to AtMostOne.
    // DISTINCT on AtMostOne → stripped. Wrapper inlined, WHERE hoisted.
    let expected = r#"SELECT "sq"."s", "t2"."val"
        FROM (SELECT sum("t1"."a") AS "s" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."s" = "t2"."id")
        WHERE ("sq"."s" > 0)"#;
    test_it("distinct_strip_wrapper_with_where", sql, expected);
}

/// HAVING FALSE on ExactlyOne → ExactlyZero. DISTINCT on 0 rows is redundant.
#[test]
fn test_distinct_strip_having_false() {
    let sql = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT DISTINCT COUNT(*) AS "c" FROM "t1" HAVING FALSE) AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")"#;
    // ExactlyZero (HAVING FALSE). DISTINCT stripped.
    let expected = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT count(*) AS "c" FROM "t1" HAVING FALSE) AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")"#;
    test_it("distinct_strip_having_false", sql, expected);
}

/// DISTINCT with GROUP BY is NOT ExactlyOne — should NOT be stripped.
/// agg_only_no_gby_cardinality returns None when GROUP BY is present.
#[test]
fn test_distinct_not_stripped_with_group_by() {
    let sql = r#"SELECT "sq"."a", "sq"."c"
        FROM (SELECT DISTINCT "t1"."a", COUNT(*) AS "c"
              FROM "t1" GROUP BY "t1"."a") AS "sq""#;
    // GROUP BY → not ExactlyOne → DISTINCT preserved.
    let expected = r#"SELECT DISTINCT "t1"."a", count(*) AS "c"
        FROM "t1" GROUP BY "t1"."a""#;
    test_it("distinct_not_stripped_with_group_by", sql, expected);
}

/// Direct aggregate-only + HAVING (non-trivial) → AtMostOne.
/// DISTINCT still redundant on ≤1 row.
#[test]
fn test_distinct_strip_agg_with_having() {
    let sql = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT DISTINCT COUNT(*) AS "c" FROM "t1"
              HAVING COUNT(*) > 0) AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")"#;
    // AtMostOne (HAVING non-trivial). DISTINCT stripped.
    // hoist_parametrizable_filters converts HAVING to WHERE.
    let expected = r#"SELECT "sq"."c", "t2"."val"
        FROM (SELECT count(*) AS "c" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."c" = "t2"."id")
        WHERE ("sq"."c" > 0)"#;
    test_it("distinct_strip_agg_with_having", sql, expected);
}

// ─── Per-level subquery guard regression tests ─────────────────────────────

/// Expression subquery is deep inside a non-inlined FROM subquery.
/// Another FROM subquery at the SAME level is a simple projection →
/// it should be inlined (the deep subquery doesn't block outer-level inlining).
#[test]
fn test_per_level_guard_deep_subquery_doesnt_block_outer() {
    let sql = r#"SELECT "sq1"."a", "sq2"."b"
        FROM (SELECT "t1"."a" FROM "t1") AS "sq1"
        INNER JOIN (SELECT "t2"."b" FROM "t2"
                    WHERE "t2"."b" IN (SELECT "t3"."c" FROM "t3")) AS "sq2"
        ON ("sq1"."a" = "sq2"."b")"#;
    // Both sq1 and sq2 are inlined: the outer level has no expression
    // subqueries (they're inside sq2's FROM scope), so the per-level
    // guard passes and try_inline_from_items inlines both.  sq2's WHERE
    // (with the IN subquery) is absorbed into base WHERE.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        INNER JOIN "t2" ON ("t1"."a" = "t2"."b")
        WHERE "t2"."b" IN (SELECT "t3"."c" FROM "t3")"#;
    test_it("per_level_deep_subquery_doesnt_block", sql, expected);
}

/// Expression subquery in outer WHERE — inlining at this level is skipped,
/// but join normalization still runs (comma-sep → CROSS JOIN, canonicalization).
#[test]
fn test_per_level_guard_where_subquery_normalization_runs() {
    let sql = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1", "t2"
        WHERE "t1"."a" = "t2"."b"
          AND "t1"."a" IN (SELECT "t3"."c" FROM "t3")"#;
    // Comma-sep normalized to CROSS JOIN, equality moved to ON.
    // Subquery stays in WHERE.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        INNER JOIN "t2" ON ("t1"."a" = "t2"."b")
        WHERE "t1"."a" IN (SELECT "t3"."c" FROM "t3")"#;
    test_it("per_level_where_subquery_normalization", sql, expected);
}

/// Subquery-in-ON via substitution: inlinable SELECT projects a scalar
/// subquery, base INNER JOIN ON references that column → bail
/// (contains_select rejects subquery in ON for all join types).
#[test]
fn test_subquery_in_on_via_substitution_bail() {
    let sql = r#"SELECT "sq"."x", "t2"."val"
        FROM (SELECT (SELECT MAX("u"."y") FROM "u") AS "x", "t1"."id" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."x" = "t2"."val")"#;
    // sq.x maps to a scalar subquery. After substitution, ON would have
    // (SELECT ...) = t2.val → contains_select fires → bail.
    let expected = r#"SELECT "sq"."x", "t2"."val"
        FROM (SELECT (SELECT max("u"."y") FROM "u") AS "x", "t1"."id" FROM "t1") AS "sq"
        INNER JOIN "t2" ON ("sq"."x" = "t2"."val")"#;
    test_it("subquery_in_on_via_substitution_bail", sql, expected);
}

/// Same as above but the column is referenced in WHERE (not ON) → safe,
/// should inline.
#[test]
fn test_subquery_select_referenced_in_where_inlines() {
    let sql = r#"SELECT "sq"."x"
        FROM (SELECT (SELECT MAX("u"."y") FROM "u") AS "x", "t1"."id" FROM "t1") AS "sq"
        WHERE "sq"."x" > 5"#;
    // sq.x maps to a scalar subquery but it's in WHERE, not ON → safe.
    let expected = r#"SELECT (SELECT max("u"."y") FROM "u") AS "x"
        FROM "t1"
        WHERE ((SELECT max("u"."y") FROM "u") > 5)"#;
    test_it("subquery_select_in_where_inlines", sql, expected);
}

// ─── Correlated subqueries inside inlinables ───────────────────────────────

/// Inlinable sq2 has a correlated subquery in WHERE that correlates with
/// sq2's OWN table (t2.id = t2.b).  After inlining, the absorbed WHERE
/// uses internal table names → safe.  sq1 should also inline.
#[test]
fn test_inlinable_with_self_correlated_subquery() {
    let sql = r#"SELECT "sq1"."a", "sq2"."b"
        FROM (SELECT "t1"."a" FROM "t1") AS "sq1"
        INNER JOIN (SELECT "t2"."b" FROM "t2"
                    WHERE "t2"."b" = (SELECT MAX("t4"."x") FROM "t4"
                                      WHERE "t4"."id" = "t2"."id")) AS "sq2"
        ON ("sq1"."a" = "sq2"."b")"#;
    // Both inlined: sq2's correlated subquery references t2 (its own table),
    // which survives inlining.  The correlation t4.id = t2.id stays valid.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        INNER JOIN "t2" ON ("t1"."a" = "t2"."b")
        WHERE ("t2"."b" = (SELECT max("t4"."x") FROM "t4"
                            WHERE ("t4"."id" = "t2"."id")))"#;
    test_it("inlinable_self_correlated_subquery", sql, expected);
}

/// Inlinable sq has a correlated subquery in SELECT that correlates with
/// sq's own table.  After inlining, the substituted expression preserves
/// the internal correlation.
#[test]
fn test_inlinable_select_self_correlated_subquery() {
    let sql = r#"SELECT "sq"."x"
        FROM (SELECT (SELECT MAX("t4"."y") FROM "t4"
                      WHERE "t4"."id" = "t1"."id") AS "x"
              FROM "t1") AS "sq"
        WHERE "sq"."x" > 5"#;
    // sq.x maps to a scalar subquery correlated with t1.  After inlining,
    // t1 is in the base FROM, so t4.id = t1.id still resolves.
    let expected = r#"SELECT (SELECT max("t4"."y") FROM "t4"
                      WHERE ("t4"."id" = "t1"."id")) AS "x"
        FROM "t1"
        WHERE ((SELECT max("t4"."y") FROM "t4"
                WHERE ("t4"."id" = "t1"."id")) > 5)"#;
    test_it("inlinable_select_self_correlated", sql, expected);
}

/// Inlinable sq2 has a correlated subquery that references the BASE table
/// (t1, which is sq1 after inlining).  The correlation crosses scopes:
/// the expression subquery inside sq2 references t1, which is from the
/// outer FROM clause.  After inlining sq1 first (t1 in base FROM), then
/// sq2, the absorbed WHERE has t2.b = (SELECT ... WHERE t4.id = t1.id) —
/// t1 is now in the base FROM, so the reference resolves.
#[test]
fn test_inlinable_with_base_correlated_subquery() {
    let sql = r#"SELECT "sq1"."a", "sq2"."b"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1") AS "sq1"
        INNER JOIN (SELECT "t2"."b" FROM "t2"
                    WHERE "t2"."b" = (SELECT MAX("t4"."x") FROM "t4"
                                      WHERE "t4"."id" = "sq1"."id")) AS "sq2"
        ON ("sq1"."a" = "sq2"."b")"#;
    // sq2's WHERE references sq1.id — a lateral/correlated reference from
    // sq2 to the outer FROM.  Non-LATERAL derived tables can't do this in
    // standard SQL, so this query is invalid / unreachable in practice.
    //
    // If it somehow reaches here: sq1 is inlined (sq1.id → t1.id in base
    // expressions), then sq2 is inlined (its WHERE absorbed into base WHERE).
    // The expression subquery inside sq2's WHERE still has sq1.id — the
    // substitution didn't enter it (visitor stops at boundary).  Result
    // has a dangling sq1.id reference.  This is a known limitation for
    // an unreachable query shape.
    //
    // NOTE: the rewrite "succeeds" with a dangling reference — downstream
    // passes (QueryGraph, MIR) would reject it.  We document the actual
    // output rather than asserting an error from the rewrite pass itself.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        INNER JOIN "t2" ON ("t1"."a" = "t2"."b")
        WHERE ("t2"."b" = (SELECT max("t4"."x") FROM "t4"
                            WHERE ("t4"."id" = "sq1"."id")))"#;
    test_it("inlinable_base_correlated_subquery", sql, expected);
}

// ─── Item 6: RHS LEFT JOIN inlining with WHERE → ON absorption ─────────────

/// RHS single-table subquery with WHERE, inlined into LEFT JOIN.
/// The WHERE filter is absorbed into the LEFT JOIN ON clause.
/// Semantically correct: pre-filtering the RHS before LEFT JOIN is
/// equivalent to having the filter in ON (non-matching → NULL extension).
#[test]
fn test_rhs_left_join_where_absorbed_to_on() {
    let sql = r#"SELECT "t1"."a", "sq"."b"
        FROM "t1"
        LEFT JOIN (SELECT "t2"."id", "t2"."b" FROM "t2"
                   WHERE "t2"."b" > 5) AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    // RHS WHERE (t2.b > 5) is a SingleRelFilter → absorbed into ON.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON (("t1"."id" = "t2"."id") AND ("t2"."b" > 5))"#;
    test_it("rhs_left_join_where_absorbed_to_on", sql, expected);
}

/// RHS LEFT JOIN with WHERE that is NOT a SingleRelFilter → bail.
/// Cross-table predicates in the RHS WHERE can't be pushed to ON.
#[test]
fn test_rhs_left_join_cross_table_where_bail() {
    let sql = r#"SELECT "t1"."a", "sq"."b"
        FROM "t1"
        LEFT JOIN (SELECT "t2"."id", "t2"."b" FROM "t2"
                   WHERE "t2"."b" = "t1"."a") AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    // RHS WHERE references t1 (outer scope) — not a SingleRelFilter.
    // can_inline_left_join_rhs_safe bails.
    let expected = r#"SELECT "t1"."a", "sq"."b"
        FROM "t1"
        LEFT OUTER JOIN (SELECT "t2"."id", "t2"."b" FROM "t2"
                         WHERE ("t2"."b" = "t1"."a")) AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    test_it("rhs_left_join_cross_table_where_bail", sql, expected);
}

/// RHS LEFT JOIN without WHERE — simple case, should inline.
#[test]
fn test_rhs_left_join_no_where_inlines() {
    let sql = r#"SELECT "t1"."a", "sq"."b"
        FROM "t1"
        LEFT JOIN (SELECT "t2"."id", "t2"."b" FROM "t2") AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t1"
        LEFT OUTER JOIN "t2" ON ("t1"."id" = "t2"."id")"#;
    test_it("rhs_left_join_no_where_inlines", sql, expected);
}

/// RHS LEFT JOIN with non-null-preserving projection → bail.
/// COALESCE(t2.a, 0) changes NULL to 0, altering LEFT JOIN semantics.
#[test]
fn test_rhs_left_join_non_null_preserving_bail() {
    let sql = r#"SELECT "t1"."a", "sq"."safe_b"
        FROM "t1"
        LEFT JOIN (SELECT COALESCE("t2"."b", 0) AS "safe_b", "t2"."id"
                   FROM "t2") AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    // sq.safe_b maps to COALESCE(t2.b, 0) — NOT null-preserving.
    // After inlining, NULL-extended rows would produce 0 instead of NULL.
    let expected = r#"SELECT "t1"."a", "sq"."safe_b"
        FROM "t1"
        LEFT OUTER JOIN (SELECT coalesce("t2"."b", 0) AS "safe_b", "t2"."id"
                         FROM "t2") AS "sq"
        ON ("t1"."id" = "sq"."id")"#;
    test_it("rhs_left_join_non_null_preserving_bail", sql, expected);
}

// ─── Item 7: NULL promotion → inlining interaction ─────────────────────────

/// LEFT JOIN promoted to INNER by null-rejecting WHERE, then the LHS
/// subquery becomes inlinable (first join is now INNER).
#[test]
fn test_null_promotion_enables_inlining() {
    let sql = r#"SELECT "sq"."a", "t2"."b"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1"
              INNER JOIN "t3" ON ("t1"."id" = "t3"."id")) AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")
        WHERE "t2"."b" > 0"#;
    // Step 1: NULL promotion — WHERE t2.b > 0 null-rejects t2 → LEFT → INNER
    // Step 2: LHS subquery with multi-table (t1 JOIN t3): first join is now
    //         INNER → inlining allowed.
    // Join reorder may change table order.
    let expected = r#"SELECT "t1"."a", "t2"."b"
        FROM "t2"
        INNER JOIN "t1" ON ("t2"."id" = "t1"."id")
        INNER JOIN "t3" ON ("t1"."id" = "t3"."id")
        WHERE ("t2"."b" > 0)"#;
    test_it("null_promotion_enables_inlining", sql, expected);
}

/// LEFT JOIN NOT promoted (no null-rejecting evidence) → LHS multi-table
/// subquery stays as derived table (first join is LEFT → bail for complex LHS).
#[test]
fn test_no_null_promotion_blocks_complex_lhs() {
    let sql = r#"SELECT "sq"."a", "t2"."b"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1"
              INNER JOIN "t3" ON ("t1"."id" = "t3"."id")) AS "sq"
        LEFT JOIN "t2" ON ("sq"."id" = "t2"."id")"#;
    // No null-rejecting WHERE on t2 → LEFT stays LEFT.
    // LHS is multi-table (t1 JOIN t3), first base join is LEFT → bail.
    let expected = r#"SELECT "sq"."a", "t2"."b"
        FROM (SELECT "t1"."a", "t1"."id" FROM "t1"
              INNER JOIN "t3" ON ("t1"."id" = "t3"."id")) AS "sq"
        LEFT OUTER JOIN "t2" ON ("sq"."id" = "t2"."id")"#;
    test_it("no_null_promotion_blocks_complex_lhs", sql, expected);
}

// ─── Item 8: Aggregated subquery inlining path ─────────────────────────────

/// Aggregated subquery into single-FROM base: WHERE→HAVING conversion.
/// Base WHERE (sq.total > 100) becomes HAVING after inlining because
/// it applies post-aggregation.
#[test]
fn test_aggregated_subquery_where_to_having() {
    let sql = r#"SELECT "sq"."total"
        FROM (SELECT SUM("t1"."x") AS "total", "t1"."a"
              FROM "t1" GROUP BY "t1"."a") AS "sq"
        WHERE "sq"."total" > 100"#;
    // Base WHERE (sq.total > 100) → HAVING. normalize_having_and_group_by
    // replaces the expression with the alias reference per §12.1 canonical form.
    let expected = r#"SELECT sum("t1"."x") AS "total"
        FROM "t1"
        GROUP BY "t1"."a"
        HAVING ("total" > 100)"#;
    test_it("aggregated_subquery_where_to_having", sql, expected);
}

/// Aggregated subquery with its own WHERE — the inlinable's WHERE
/// replaces the base WHERE (which was converted to HAVING).
#[test]
fn test_aggregated_subquery_both_wheres() {
    let sql = r#"SELECT "sq"."total"
        FROM (SELECT SUM("t1"."x") AS "total", "t1"."a"
              FROM "t1"
              WHERE "t1"."x" > 0
              GROUP BY "t1"."a") AS "sq"
        WHERE "sq"."total" > 100"#;
    // Base WHERE → HAVING (alias form per §12.1).
    // Inlinable WHERE → base WHERE.
    let expected = r#"SELECT sum("t1"."x") AS "total"
        FROM "t1"
        WHERE ("t1"."x" > 0)
        GROUP BY "t1"."a"
        HAVING ("total" > 100)"#;
    test_it("aggregated_subquery_both_wheres", sql, expected);
}

/// Aggregated subquery with DISTINCT — DISTINCT is adopted by base.
#[test]
fn test_aggregated_subquery_distinct_adopted() {
    let sql = r#"SELECT "sq"."total"
        FROM (SELECT DISTINCT SUM("t1"."x") AS "total", "t1"."a"
              FROM "t1" GROUP BY "t1"."a") AS "sq""#;
    let expected = r#"SELECT DISTINCT sum("t1"."x") AS "total"
        FROM "t1"
        GROUP BY "t1"."a""#;
    test_it("aggregated_subquery_distinct_adopted", sql, expected);
}

/// Aggregated subquery into base WITH JOINS — blocked.
/// Cannot inline aggregated subquery when base has joins.
#[test]
fn test_aggregated_subquery_with_joins_blocked() {
    let sql = r#"SELECT "sq"."total", "t2"."b"
        FROM (SELECT SUM("t1"."x") AS "total", "t1"."a"
              FROM "t1" GROUP BY "t1"."a") AS "sq"
        INNER JOIN "t2" ON ("sq"."a" = "t2"."a")"#;
    // Blocked — base has joins, can't inline aggregated subquery.
    let expected = r#"SELECT "sq"."total", "t2"."b"
        FROM (SELECT sum("t1"."x") AS "total", "t1"."a"
              FROM "t1" GROUP BY "t1"."a") AS "sq"
        INNER JOIN "t2" ON ("sq"."a" = "t2"."a")"#;
    test_it("aggregated_subquery_with_joins_blocked", sql, expected);
}

// ─── Numeric GROUP BY resolution during inlining ─────────────────────────────

/// QA regression: window function over grouped subquery.
/// Inlining is blocked because the outer SELECT has a window function —
/// splicing GROUP BY into the outer statement would produce an invalid
/// "GROUP BY without aggregates" shape. The subquery survives with
/// GROUP BY rewritten to DISTINCT by fix_groupby_without_aggregates.
#[test]
fn numeric_group_by_resolved_before_inline_window() {
    let sql = r#"
        SELECT COUNT(*) OVER ()
        FROM (SELECT "t1"."a", "t1"."b" FROM "t1" GROUP BY 1, 2) AS "t0"
    "#;
    let expected = r#"
        SELECT count(*) OVER() FROM (SELECT DISTINCT "t1"."a", "t1"."b" FROM "t1") AS "t0"
    "#;
    test_it(
        "numeric_group_by_resolved_before_inline_window",
        sql,
        expected,
    );
}

/// Numeric GROUP BY in inlineable aggregated subquery (no window function).
/// GROUP BY 1 references the subquery's first field, not the outer's.
#[test]
fn numeric_group_by_resolved_before_inline_agg() {
    let sql = r#"
        SELECT "t0"."a"
        FROM (SELECT "t1"."a", count(*) AS "cnt" FROM "t1" GROUP BY 1) AS "t0"
    "#;
    let expected = r#"
        SELECT DISTINCT "t1"."a" FROM "t1"
    "#;
    test_it("numeric_group_by_resolved_before_inline_agg", sql, expected);
}

/// Mixed numeric and expression GROUP BY in non-aggregated subquery with
/// unreferenced GROUP BY key — inlining blocked, subquery survives with
/// GROUP BY rewritten to DISTINCT.
#[test]
fn numeric_group_by_mixed_resolved_before_inline() {
    let sql = r#"
        SELECT "t0"."x"
        FROM (SELECT "t1"."a" AS "x", "t1"."b" FROM "t1" GROUP BY 1, "t1"."b") AS "t0"
    "#;
    let expected = r#"
        SELECT "t0"."x"
        FROM (SELECT DISTINCT "t1"."a" AS "x", "t1"."b" FROM "t1") AS "t0"
    "#;
    test_it(
        "numeric_group_by_mixed_resolved_before_inline",
        sql,
        expected,
    );
}

/// Numeric GROUP BY with WHERE filter on aggregated column.
#[test]
fn numeric_group_by_with_having_after_inline() {
    let sql = r#"
        SELECT "t0"."cnt"
        FROM (SELECT "t1"."a", count(*) AS "cnt" FROM "t1" GROUP BY 1) AS "t0"
        WHERE "t0"."cnt" > 1
    "#;
    let expected = r#"
        SELECT count(*) AS "cnt" FROM "t1" GROUP BY "t1"."a" HAVING ("cnt" > 1)
    "#;
    test_it("numeric_group_by_with_having_after_inline", sql, expected);
}

/// Numeric ORDER BY in inlineable subquery — same class of bug as GROUP BY.
#[test]
fn numeric_order_by_resolved_before_inline() {
    let sql = r#"
        SELECT "t0"."a"
        FROM (SELECT "t1"."a", "t1"."b" FROM "t1" ORDER BY 2) AS "t0"
    "#;
    let expected = r#"
        SELECT "t1"."a" FROM "t1" ORDER BY "t1"."b" NULLS LAST
    "#;
    test_it("numeric_order_by_resolved_before_inline", sql, expected);
}

/// Alias ORDER BY in inlineable subquery — alias must be resolved to
/// the underlying expression before transfer, since the alias disappears.
#[test]
fn alias_order_by_resolved_before_inline() {
    let sql = r#"
        SELECT "t0"."x"
        FROM (SELECT "t1"."a" AS "x", "t1"."b" AS "y" FROM "t1" ORDER BY "y") AS "t0"
    "#;
    let expected = r#"
        SELECT "t1"."a" AS "x" FROM "t1" ORDER BY "t1"."b" NULLS LAST
    "#;
    test_it("alias_order_by_resolved_before_inline", sql, expected);
}
