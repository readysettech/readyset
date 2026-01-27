use readyset_sql::{Dialect, DialectDisplay, ast::SqlQuery};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
use test_strategy::proptest;
use test_utils::tags;

mod utils;

#[tags(slow)]
#[proptest]
#[ignore = "WIP REA-5456"]
fn arbitrary_query_mysql(#[any(Some(Dialect::MySQL))] q: SqlQuery) {
    let sql = q.display(Dialect::MySQL).to_string();
    check_parse_mysql!(&sql);
}

#[tags(slow)]
#[proptest]
#[ignore = "WIP REA-5456"]
fn arbitrary_query_postgres(#[any(Some(Dialect::PostgreSQL))] q: SqlQuery) {
    let sql = q.display(Dialect::PostgreSQL).to_string();
    check_parse_postgres!(&sql);
}

#[test]
fn bare_functions() {
    // XXX: This list differs from the one in `nom_sql::common::function_call_without_parens`,
    // because sqlparser-rs doesn't cover them all; but we test to match them all.
    for func in [
        "user",
        "current_user",
        "session_user",
        "system_user",
        "current_schema",
        "current_role",
        "current_catalog",
        "now", // Not actually a bare function despite nom-sql previously treating it that way, will parse as a column.
        "current_date",
        "current_timestamp",
        "current_time",
        "localtimestamp",
        "localtime",
    ] {
        check_rt_both!(format!("SELECT * FROM myfuncs WHERE {func}name = {func}"));
        check_rt_mysql!(format!("SELECT * FROM myfuncs WHERE `{func}` = {func}"));
        check_rt_postgres!(format!(r#"SELECT * FROM myfuncs WHERE "{func}" = {func}"#));
    }
}

#[test]
fn collation_name() {
    check_rt_mysql!("CREATE TABLE t (x TEXT COLLATE latin1_swedish_ci)");
    check_rt_mysql!("CREATE TABLE t (x TEXT COLLATE `latin1_swedish_ci`)");
    check_rt_mysql!("CREATE TABLE t (x TEXT COLLATE 'latin1_swedish_ci')");
    check_rt_mysql!(r#"CREATE TABLE t (x TEXT COLLATE "latin1_swedish_ci")"#);
    check_rt_postgres!(r#"CREATE TABLE t (x TEXT COLLATE "latin1_swedish_ci")"#);
    check_rt_postgres!("CREATE TABLE t (x TEXT COLLATE latin1_swedish_ci)");
}

#[test]
fn create_index() {
    check_rt_mysql_sqlparser!("CREATE INDEX idx_name ON t (c1);");
    check_rt_mysql_sqlparser!("CREATE INDEX `idx_name` ON `t` (`c1`);");

    check_rt_mysql_sqlparser!("CREATE UNIQUE INDEX idx_name ON t (c1);");
    check_rt_mysql_sqlparser!("CREATE UNIQUE INDEX `idx_name` ON `t` (`c1`, `c2`);");
    check_rt_mysql_sqlparser!("CREATE UNIQUE INDEX `idx_name` ON `t` (`c1`, `c2`)  LOCK=NONE;");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_multi ON t (c1, c2, c3);");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_prefix ON t (c1(10));");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_prefix_multi ON t (c1(10), c2(20));");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_mixed ON t (c1, c2(15), c3);");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_asc ON t (c1 ASC);");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_desc ON t (c1 DESC);");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_mixed_order ON t (c1 ASC, c2 DESC);");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_btree ON t (c1) USING BTREE;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_hash ON t (c1) USING HASH;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_combo ON t (c1) USING BTREE COMMENT 'Test index';");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_comment ON t (c1) COMMENT 'Index comment';");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_lock_default ON t (c1) LOCK=DEFAULT;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_lock_none ON t (c1) LOCK=NONE;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_lock_shared ON t (c1) LOCK=SHARED;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_lock_exclusive ON t (c1) LOCK=EXCLUSIVE;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_lock_space ON t (c1) LOCK = NONE;");

    check_rt_mysql_sqlparser!("CREATE INDEX idx_algo_default ON t (c1) ALGORITHM=DEFAULT;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_algo_inplace ON t (c1) ALGORITHM=INPLACE;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_algo_copy ON t (c1) ALGORITHM=COPY;");
    check_rt_mysql_sqlparser!("CREATE INDEX idx_algo_space ON t (c1) ALGORITHM = INPLACE;");

    check_rt_mysql_sqlparser!(
        "CREATE UNIQUE INDEX idx_complex ON t (c1(10), c2 DESC) COMMENT 'Complex index' ALGORITHM=INPLACE LOCK=NONE;"
    );

    check_rt_postgres_sqlparser!("CREATE INDEX \"idx_double_quotes\" ON \"table\" (\"column\");");

    // FULLTEXT/SPATIAL indexes: not supported by sqlparser-rs yet
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE FULLTEXT INDEX idx_name ON t (c1);",
        "found: FULLTEXT"
    );

    check_parse_fails!(
        Dialect::MySQL,
        "CREATE FULLTEXT INDEX idx_fulltext ON articles (title, content);",
        "found: FULLTEXT"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE SPATIAL INDEX idx_spatial ON locations (coordinates);",
        "found: SPATIAL"
    );
}

#[test]
fn test_alter_table_rename() {
    check_rt_mysql_sqlparser!("ALTER TABLE tb1 RENAME TO tb2");
    check_rt_postgres_sqlparser!("ALTER TABLE tb1 RENAME TO tb2");
    check_rt_mysql_sqlparser!("ALTER TABLE tb1 RENAME AS tb2");
}

/// Roundtrip tests for MySQL column position (FIRST, AFTER) in ALTER TABLE
#[test]
fn test_alter_table_add_column_position() {
    // ADD COLUMN with FIRST
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT FIRST");

    // ADD COLUMN with AFTER
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER other_col");

    // ADD COLUMN without position (should still work)
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT");
}

#[test]
fn test_alter_table_change_column_position() {
    // CHANGE COLUMN with FIRST
    check_rt_mysql_sqlparser!("ALTER TABLE t CHANGE COLUMN old_col new_col INT FIRST");

    // CHANGE COLUMN with AFTER
    check_rt_mysql_sqlparser!("ALTER TABLE t CHANGE COLUMN old_col new_col INT AFTER other_col");

    // CHANGE COLUMN without position
    check_rt_mysql_sqlparser!("ALTER TABLE t CHANGE COLUMN old_col new_col INT");
}

#[test]
fn test_alter_table_column_position_reserved_words() {
    // AFTER with reserved word column name (requires backticks in MySQL)
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `select`");
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `from`");
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `table`");

    // CHANGE COLUMN with reserved word in AFTER
    check_rt_mysql_sqlparser!("ALTER TABLE t CHANGE COLUMN old_col new_col INT AFTER `order`");
}

#[test]
fn test_alter_table_column_position_quoted_identifiers() {
    // Identifiers with special characters
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `my-column`");
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `column with spaces`");

    // Mixed case identifiers
    check_rt_mysql_sqlparser!("ALTER TABLE t ADD COLUMN c INT AFTER `CamelCase`");
}

#[test]
fn limit_placeholders() {
    check_rt_mysql!("select * from users limit ?");
    check_rt_mysql!("select * from users limit $1");
    check_rt_postgres!("select * from users limit $1");
    check_rt_postgres!("select * from users limit :1");
}

// Roundtrip fails for CREATE CACHE statements due to metadata differences (token casing, spans,
// unparsed_create_cache_statement), not placeholder parsing. Parity test confirms placeholder
// parsing works correctly. This is a known limitation of CREATE CACHE roundtrip testing.
#[test]
#[should_panic = "assertion failed: `(left == right)`"]
fn limit_placeholders_create_cache() {
    check_rt_mysql!("create cache from select * from users limit ?");
    check_rt_mysql!("create cache from select * from users limit $1");
    check_rt_postgres!("create cache from select * from users limit $1");
    check_rt_postgres!("create cache from select * from users limit :1");
}

#[test]
fn create_table_like() {
    check_rt_mysql!("CREATE TABLE a LIKE b");
    check_rt_mysql!("CREATE TABLE IF NOT EXISTS a LIKE b");
}

// TODO: Fix sqlparser upstream REA-6164
#[test]
#[should_panic = "nom-sql AST differs from sqlparser-rs AST"]
fn create_table_like_parenthesized() {
    check_rt_mysql!("CREATE TABLE a(LIKE b)");
    check_rt_mysql!("CREATE TABLE a (LIKE b)");
    check_rt_mysql!("CREATE TABLE IF NOT EXISTS a (LIKE b)");
}

#[test]
fn time_without_time_zone_type() {
    check_rt_postgres!("CREATE TABLE t_time (c TIME)");
    check_rt_postgres!("CREATE TABLE t_time (c TIME WITHOUT TIME ZONE)");
}

#[test]
fn user_defined_function_expr_qualified() {
    check_rt_mysql_sqlparser!(
        r#"
        SELECT DISTINCT con.cco_conta,
                        tmp.pa_codigo,
                        cco.cli_cpfcnpj,
                        cco.end_uf,
                        modal.mod_modalbacen,
                        agencia_0009.saldoparcelacontabiladitivo (emp.cco_conta, emp.con_ndoc, emp.con_seq, emp.emp_parcela, "2025-06-30") AS Saldo,
                        agencia_0009.saldoparcelacontabiladitivo (emp.cco_conta, emp.con_ndoc, emp.con_seq, emp.emp_parcela, "2025-05-31") AS SaldoAnt,
                        con.con_ndoc,
                        con.mod_codigo,
                        con.prd_codigo,
                        con.con_parcelas,
                        con.con_juros,
                        con.con_vlrini AS ValorInicial,
                        emp.emp_parcela,
                        con.con_emis AS emissao,
                        con.con_tipooper,
                        "" AS con_caracespecial,
                        emp.emp_vcto AS vencimento,
                        prd.prd_nivelinicial,
                        1 AS prd_provisiona,
                        con.con_regime_caixa,
                        con.con_taxacdi,
                        con.con_seq,
                        con.emprestimo_proporcional,
                        con.prd_tipocalcpos,
                        agencia_0009.DiasAtrasoEmprestimoaditivo (con.cco_conta, con.con_ndoc, con.con_seq, "2025-06-30") AS DiasAtraso,
                        con.con_data_inclusao_regime,
                        con.con_finan_hh,
                        con.con_valor,
                        modal.ORI_CODIGO,
                        atp.atp_entrada,
                        con.con_tac,
                        if(con.con_tac > 0, tjeoaefetivar(con.cco_conta, con.con_ndoc, con.con_seq, "2025-06-30"), 0) AS RendasTjeo,

          (SELECT MAX(EMP_VCTO)
           FROM agencia_0009.ep_parcela
           WHERE cco_conta = con.cco_conta
             AND con_ndoc = con.con_ndoc
             AND con_seq = con.con_seq
             AND (emp_pgto IS NULL
                  OR emp_pgto > "2025-06-30")
           LIMIT 1) AS VctoUltimaParcela,
                        con.con_vcto
        FROM agencia_0009.ep_saldocontrato AS sco
        LEFT JOIN agencia_0009.ep_contrato AS con ON sco.cco_conta = con.cco_conta
        AND sco.sco_ndoc = con.con_ndoc
        AND sco.sco_seq = con.con_seq
        LEFT JOIN agencia_0009.ep_parcela AS emp ON con.cco_conta = emp.cco_conta
        AND con.con_ndoc = emp.con_ndoc
        AND con.con_seq = emp.con_seq
        LEFT JOIN agencia_0009.view_dadoscontas AS cco ON con.cco_conta = cco.cco_conta
        LEFT JOIN agencia_0009.cc_saldo AS tmp ON con.cco_conta = tmp.cco_conta
        AND tmp.sld_data = "2025-06-30"
        LEFT JOIN unico.ep_produto AS prd ON con.prd_codigo = prd.prd_codigo
        LEFT JOIN unico.ep_modalidade AS modal ON con.mod_codigo = modal.mod_codigo
        LEFT JOIN agencia_0009.4966_atp_contrato AS atp ON atp.cco_conta = con.cco_conta
        AND atp.atp_ndoc = con.con_ndoc
        AND atp.atp_tipooper = 5
        AND atp.atp_entrada <= "2025-06-30"
        AND (atp.atp_saida IS NULL
             OR atp.atp_saida > "2025-06-30")
        WHERE (sco.sco_data = "2025-06-30"
               OR sco.sco_data = "2025-05-30")
          AND sco_tipo = 5
          AND IF(con.con_taxacdi > 0, (emp.emp_pagpos IS NULL
                                       OR emp.emp_pagpos > "2025-05-31"), (emp.emp_pgto IS NULL
                                                                           OR emp.emp_pgto > "2025-05-31"))
        HAVING Saldo > 0
        OR SaldoAnt > 0
    "#
    );
}

#[test]
fn schema_qualified_function_simple_mysql() {
    // Basic schema-qualified function call
    check_rt_mysql_sqlparser!("SELECT myschema.myfunc()");
    check_rt_mysql_sqlparser!("SELECT myschema.myfunc(1)");
    check_rt_mysql_sqlparser!("SELECT myschema.myfunc(1, 2, 3)");
    check_rt_mysql_sqlparser!("SELECT myschema.myfunc(x, y) FROM t");
}

#[test]
fn schema_qualified_function_simple_postgres() {
    // Basic schema-qualified function call for PostgreSQL
    check_rt_postgres_sqlparser!("SELECT myschema.myfunc()");
    check_rt_postgres_sqlparser!("SELECT myschema.myfunc(1)");
    check_rt_postgres_sqlparser!("SELECT myschema.myfunc(1, 2, 3)");
    check_rt_postgres_sqlparser!("SELECT myschema.myfunc(x, y) FROM t");
}

#[test]
fn schema_qualified_function_quoted_mysql() {
    // Schema-qualified function with quoted identifiers (MySQL uses backticks)
    check_rt_mysql_sqlparser!("SELECT `my-schema`.`my-func`()");
    check_rt_mysql_sqlparser!("SELECT `my schema`.`my func`(1, 2)");
}

#[test]
fn schema_qualified_function_quoted_postgres() {
    // Schema-qualified function with quoted identifiers (PostgreSQL uses double quotes)
    check_rt_postgres_sqlparser!(r#"SELECT "my-schema"."my-func"()"#);
    check_rt_postgres_sqlparser!(r#"SELECT "my schema"."my func"(1, 2)"#);
}

#[test]
fn pg_catalog_qualified_builtin_function() {
    // pg_catalog-qualified built-in functions should be recognized as built-ins
    check_rt_postgres_sqlparser!("SELECT pg_catalog.count(*) FROM t");
    check_rt_postgres_sqlparser!("SELECT pg_catalog.sum(x) FROM t");
    check_rt_postgres_sqlparser!("SELECT pg_catalog.max(x) FROM t");
}

#[test]
fn unqualified_function_still_works() {
    // Ensure unqualified function calls still work as before
    check_rt_mysql_sqlparser!("SELECT myfunc()");
    check_rt_mysql_sqlparser!("SELECT myfunc(1, 2, 3)");
    check_rt_postgres_sqlparser!("SELECT myfunc()");
    check_rt_postgres_sqlparser!("SELECT myfunc(1, 2, 3)");
}
