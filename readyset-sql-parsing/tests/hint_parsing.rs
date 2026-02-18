use std::time::Duration;

use readyset_sql::ast::{CacheType, EvictionPolicy, ReadysetHintDirective};
use readyset_sql::Dialect;
use readyset_sql_parsing::{parse_hint_directive, parse_shallow_query};

#[test]
fn parse_hint_extracts_create_cache_directive() {
    let (_, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ * FROM users WHERE id = 1",
    )
    .expect("should parse");
    assert!(matches!(
        directive,
        Some(ReadysetHintDirective::CreateCache(_))
    ));
}

#[test]
fn parse_hint_returns_none_for_no_hint() {
    let (_, directive) =
        parse_shallow_query(Dialect::MySQL, "SELECT * FROM users WHERE id = 1")
            .expect("should parse");
    assert!(directive.is_none());
}

#[test]
fn query_id_stable_with_and_without_hint() {
    let (with_hint, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ id, name FROM users WHERE id = 1",
    )
    .expect("should parse hinted query");
    assert!(directive.is_some());

    let (without_hint, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT id, name FROM users WHERE id = 1",
    )
    .expect("should parse non-hinted query");
    assert!(directive.is_none());

    // The hint-stripped query should be identical to the non-hinted query.
    assert_eq!(
        format!("{with_hint}"),
        format!("{without_hint}"),
        "Display output should be identical with and without hint"
    );

    // Hash/PartialEq should be the same (which means QueryId is the same).
    assert_eq!(
        with_hint, without_hint,
        "ShallowCacheQuery should be equal with and without hint"
    );
}

#[test]
fn parse_hint_with_ttl_options() {
    let (_, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE POLICY TTL 300 SECONDS REFRESH 60 SECONDS */ id FROM orders WHERE user_id = 1",
    )
    .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = directive else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(opts.cache_type, Some(readyset_sql::ast::CacheType::Shallow));
    assert!(opts.policy.is_some());
}

#[test]
fn parse_hint_postgresql_dialect() {
    let (with_hint, _) = parse_shallow_query(
        Dialect::PostgreSQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ id FROM users WHERE id = $1",
    )
    .expect("should parse hinted postgres query");

    let (without_hint, _) = parse_shallow_query(
        Dialect::PostgreSQL,
        "SELECT id FROM users WHERE id = $1",
    )
    .expect("should parse non-hinted postgres query");

    assert_eq!(with_hint, without_hint);
}

#[test]
fn malformed_hint_still_returns_valid_shallow_query() {
    // "POLICY TT" is invalid (should be "POLICY TTL"), but the SQL query itself is valid.
    // parse_shallow_query should succeed with directive = None, not propagate the hint error.
    let (query, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE POLICY TT 300 SECONDS */ RAND()",
    )
    .expect("should parse despite malformed hint");
    assert!(
        directive.is_none(),
        "Malformed hint should produce None directive"
    );

    // The returned query should match the hint-stripped version.
    let (plain_query, _) = parse_shallow_query(Dialect::MySQL, "SELECT RAND()")
        .expect("should parse plain query");
    assert_eq!(
        query, plain_query,
        "Hint-stripped query should equal the plain query"
    );
}

// --- parse_hint_directive unit tests ---

#[test]
fn parse_create_cache_hint_basic() {
    let result =
        parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE").expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(opts.cache_type, Some(CacheType::Shallow));
    assert!(!opts.always);
    assert!(opts.policy.is_none());
}

#[test]
fn parse_create_cache_hint_ttl() {
    let result =
        parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE POLICY TTL 300 SECONDS")
            .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(
        opts.policy,
        Some(EvictionPolicy::Ttl {
            ttl: Duration::from_secs(300)
        })
    );
}

#[test]
fn parse_create_cache_hint_ttl_refresh() {
    let result =
        parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE POLICY TTL 300 SECONDS REFRESH 60 SECONDS")
            .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(
        opts.policy,
        Some(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(300),
            refresh: Duration::from_secs(60),
            schedule: false,
        })
    );
}

#[test]
fn parse_create_cache_hint_always() {
    let result = parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE ALWAYS").expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert!(opts.always);
}

#[test]
fn parse_create_cache_hint_case_insensitive() {
    let result =
        parse_hint_directive(Dialect::MySQL, "create shallow cache policy ttl 100 seconds")
            .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(
        opts.policy,
        Some(EvictionPolicy::Ttl {
            ttl: Duration::from_secs(100)
        })
    );
}

#[test]
fn parse_unknown_directive() {
    let result = parse_hint_directive(Dialect::MySQL, "SKIP CACHE").expect("should parse");
    assert!(result.is_none());
}

#[test]
fn parse_empty_hint() {
    let result = parse_hint_directive(Dialect::MySQL, "").expect("should parse");
    assert!(result.is_none());
}

#[test]
fn parse_malformed_ttl() {
    let result = parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE POLICY TTL abc SECONDS");
    assert!(result.is_err());
}

#[test]
fn parse_refresh_without_ttl() {
    // REFRESH without TTL — the parser sees REFRESH as an unexpected token after CREATE CACHE
    let result = parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE REFRESH 60");
    assert!(result.is_err());
}

#[test]
fn parse_hint_with_extra_spaces() {
    let result =
        parse_hint_directive(Dialect::MySQL, "  CREATE   SHALLOW   CACHE   POLICY   TTL   200   SECONDS  ")
            .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(
        opts.policy,
        Some(EvictionPolicy::Ttl {
            ttl: Duration::from_secs(200)
        })
    );
}

#[test]
fn parse_hint_ddl_syntax_also_works() {
    // Full DDL syntax should also work in hints for consistency
    let result =
        parse_hint_directive(Dialect::MySQL, "CREATE SHALLOW CACHE POLICY TTL 300 SECONDS REFRESH 60 SECONDS")
            .expect("should parse");
    let Some(ReadysetHintDirective::CreateCache(opts)) = result else {
        panic!("Expected CreateCache directive");
    };
    assert_eq!(
        opts.policy,
        Some(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(300),
            refresh: Duration::from_secs(60),
            schedule: false,
        })
    );
}
