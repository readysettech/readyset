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
