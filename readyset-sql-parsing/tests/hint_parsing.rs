use std::time::Duration;

use readyset_sql::ast::{CacheInner, CacheType, EvictionPolicy, ReadysetHintDirective, SqlQuery};
use readyset_sql::Dialect;
use readyset_sql_parsing::{parse_hint_directive, parse_query, parse_shallow_query};

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

#[test]
fn multiple_hints_all_stripped() {
    // A query with two /*rs+ ... */ hints: one valid, one invalid.
    let (with_hints, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ /*rs+ INVALID */ RAND()",
    )
    .expect("should parse query with multiple hints");

    // The first (valid) hint should be returned as the directive.
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "First valid hint should produce CreateCache directive"
    );

    // The plain query (no hints) should be identical after stripping.
    let (plain, _) =
        parse_shallow_query(Dialect::MySQL, "SELECT RAND()").expect("should parse plain query");
    assert_eq!(
        with_hints, plain,
        "Query with multiple hints should equal the plain query after stripping"
    );
    assert_eq!(
        format!("{with_hints}"),
        format!("{plain}"),
        "Display output should match the plain query (no leftover hints)"
    );
}

#[test]
fn uppercase_hint_prefix_recognized() {
    let (with_hint, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*RS+ CREATE SHALLOW CACHE */ id FROM users WHERE id = 1",
    )
    .expect("should parse uppercase hint");
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "Uppercase RS prefix should be recognized"
    );

    let (plain, _) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT id FROM users WHERE id = 1",
    )
    .expect("should parse plain query");
    assert_eq!(
        with_hint, plain,
        "Uppercase-hinted query should equal the plain query"
    );
}

#[test]
fn non_rs_hint_also_stripped() {
    // A non-rs hint (e.g. /*mysql+ ... */) should be stripped from the query
    // so it doesn't affect the hash, but no directive is returned.
    let (query, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*mysql+ SET_VAR(sort_buffer_size=16M) */ id FROM users WHERE id = 1",
    )
    .expect("should parse query with non-rs hint");
    assert!(
        directive.is_none(),
        "Non-rs hint should not produce a directive"
    );

    let (plain, _) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT id FROM users WHERE id = 1",
    )
    .expect("should parse plain query");
    assert_eq!(
        query, plain,
        "Non-rs hint should be stripped, matching the plain query"
    );
    assert_eq!(
        format!("{query}"),
        format!("{plain}"),
        "Display output should not contain the non-rs hint"
    );
}

#[test]
fn mixed_rs_and_non_rs_hints_all_stripped() {
    // A query with both an rs hint and a non-rs hint — both should be stripped,
    // and only the rs hint is returned as the directive.
    let (query, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*mysql+ NO_INDEX_MERGE() */ /*rs+ CREATE SHALLOW CACHE */ RAND()",
    )
    .expect("should parse query with mixed hints");
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "rs hint should produce CreateCache directive"
    );

    let (plain, _) =
        parse_shallow_query(Dialect::MySQL, "SELECT RAND()").expect("should parse plain query");
    assert_eq!(
        query, plain,
        "Both rs and non-rs hints should be stripped"
    );
    assert_eq!(
        format!("{query}"),
        format!("{plain}"),
        "Display output should contain no hints at all"
    );
}

// --- UNION / SetOperation hint tests ---

#[test]
fn union_query_hint_extracted_and_stripped() {
    let (with_hint, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ RAND() UNION SELECT RAND()",
    )
    .expect("should parse hinted UNION query");
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "Hint directive should be extracted from UNION query"
    );

    let (without_hint, _) =
        parse_shallow_query(Dialect::MySQL, "SELECT RAND() UNION SELECT RAND()")
            .expect("should parse plain UNION query");
    assert_eq!(
        with_hint, without_hint,
        "Hinted UNION query should equal the plain UNION query after stripping"
    );
    assert_eq!(
        format!("{with_hint}"),
        format!("{without_hint}"),
        "Display output should be identical with and without hint on UNION"
    );
}

#[test]
fn nested_union_hint_stripped() {
    let (with_hint, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ 1 UNION SELECT 2 UNION SELECT 3",
    )
    .expect("should parse nested UNION with hint");
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "Hint directive should be extracted from nested UNION"
    );

    let (without_hint, _) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT 1 UNION SELECT 2 UNION SELECT 3",
    )
    .expect("should parse plain nested UNION");
    assert_eq!(
        with_hint, without_hint,
        "Nested UNION with hint should equal the plain version"
    );
}

#[test]
fn non_rs_hint_on_union_stripped() {
    let (query, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*mysql+ SET_VAR(sort_buffer_size=16M) */ 1 UNION SELECT 2",
    )
    .expect("should parse UNION with non-rs hint");
    assert!(
        directive.is_none(),
        "Non-rs hint on UNION should not produce a directive"
    );

    let (plain, _) = parse_shallow_query(Dialect::MySQL, "SELECT 1 UNION SELECT 2")
        .expect("should parse plain UNION");
    assert_eq!(
        query, plain,
        "Non-rs hint on UNION should be stripped, matching the plain query"
    );
    assert_eq!(
        format!("{query}"),
        format!("{plain}"),
        "Display output should not contain the non-rs hint on UNION"
    );
}

#[test]
fn hint_on_right_side_of_union_stripped() {
    // Hint on the right branch of a UNION should still be drained.
    let (query, directive) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT 1 UNION SELECT /*rs+ CREATE SHALLOW CACHE */ 2",
    )
    .expect("should parse UNION with hint on right");
    assert!(
        matches!(directive, Some(ReadysetHintDirective::CreateCache(_))),
        "Hint on right side of UNION should be extracted"
    );

    let (plain, _) = parse_shallow_query(Dialect::MySQL, "SELECT 1 UNION SELECT 2")
        .expect("should parse plain UNION");
    assert_eq!(
        query, plain,
        "Hint on right side of UNION should be stripped"
    );
}

#[test]
fn create_cache_select_hints_stripped() {
    let create_sql =
        "CREATE SHALLOW CACHE FROM SELECT /*rs+ CREATE SHALLOW CACHE */ id FROM t WHERE id = ?";
    let query = parse_query(Dialect::MySQL, create_sql).expect("parse create cache");
    let SqlQuery::CreateCache(stmt) = query else {
        panic!("expected CreateCache")
    };
    let CacheInner::Statement {
        shallow: Ok(shallow),
        ..
    } = stmt.inner
    else {
        panic!("expected shallow Ok")
    };

    let (via_parse_shallow, _) = parse_shallow_query(
        Dialect::MySQL,
        "SELECT /*rs+ CREATE SHALLOW CACHE */ id FROM t WHERE id = ?",
    )
    .expect("parse shallow");

    assert_eq!(
        *shallow, via_parse_shallow,
        "CREATE CACHE path must produce the same hint-stripped ShallowCacheQuery as the query path"
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
