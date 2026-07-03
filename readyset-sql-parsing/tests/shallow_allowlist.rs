//! Parse tests for the shallow-cache function allowlist statements. These are
//! implemented in the sqlparser path only (nom-sql does not parse them), so the
//! tests run with `ParsingPreset::OnlySqlparser` rather than the both-parser
//! check macros.

use readyset_sql::Dialect;
use readyset_sql::ast::{
    AlterReadysetStatement, ShallowCacheAllowedFunctions, ShowStatement, SqlQuery,
};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

fn parse(dialect: Dialect, sql: &str) -> SqlQuery {
    parse_query_with_config(ParsingPreset::OnlySqlparser.into_config(), dialect, sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
}

#[test]
fn alter_add_single_function() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(dialect, "ALTER READYSET ADD SHALLOW CACHE ALLOWED FUNCTION now");
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowedFunctions(
                ShallowCacheAllowedFunctions {
                    add: true,
                    functions: vec!["now".into()],
                }
            ))
        );
    }
}

#[test]
fn alter_drop_multiple_functions() {
    let q = parse(
        Dialect::PostgreSQL,
        "ALTER READYSET DROP SHALLOW CACHE ALLOWED FUNCTION now, current_timestamp, my_udf",
    );
    assert_eq!(
        q,
        SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowedFunctions(
            ShallowCacheAllowedFunctions {
                add: false,
                functions: vec!["now".into(), "current_timestamp".into(), "my_udf".into()],
            }
        ))
    );
}

#[test]
fn show_allowed_functions() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(dialect, "SHOW SHALLOW CACHE ALLOWED FUNCTIONS");
        assert_eq!(
            q,
            SqlQuery::Show(ShowStatement::ShallowCacheAllowedFunctions)
        );
    }
}
