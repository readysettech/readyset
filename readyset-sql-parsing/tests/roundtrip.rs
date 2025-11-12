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

#[test]
fn limit_placeholders() {
    check_rt_mysql!("select * from users limit ?");
    check_rt_mysql!("select * from users limit $1");
    check_rt_postgres!("select * from users limit $1");
    check_rt_postgres!("select * from users limit :1");
}

// TODO: Fix REA-6179
#[test]
#[should_panic = "nom-sql AST differs from sqlparser-rs AST"]
fn limit_placeholders_create_cache() {
    check_rt_mysql!("create cache from select * from users limit ?");
    check_rt_mysql!("create cache from select * from users limit $1");
    check_rt_postgres!("create cache from select * from users limit $1");
    check_rt_postgres!("create cache from select * from users limit :1");
}
