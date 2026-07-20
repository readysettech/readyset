//! Parse tests for the shallow-cache allowlist statements. These are
//! implemented in the sqlparser path only (nom-sql does not parse them), so the
//! tests run with `ParsingPreset::OnlySqlparser` rather than the both-parser
//! check macros.

use readyset_sql::ast::{
    AlterReadysetStatement, ShallowCacheAllowlistChange, ShallowCacheAllowlistKind, ShowStatement,
    SqlQuery,
};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

fn parse(dialect: Dialect, sql: &str) -> SqlQuery {
    parse_query_with_config(ParsingPreset::OnlySqlparser.into_config(), dialect, sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
}

/// Whether `sql` fails to parse (used for the negative cases below).
fn rejects(dialect: Dialect, sql: &str) -> bool {
    parse_query_with_config(ParsingPreset::OnlySqlparser.into_config(), dialect, sql).is_err()
}

/// The three kinds paired with their ALTER (singular) and SHOW (plural)
/// keywords, so the tests below cover all of them uniformly.
const KINDS: &[(ShallowCacheAllowlistKind, &str, &str)] = &[
    (ShallowCacheAllowlistKind::Function, "FUNCTION", "FUNCTIONS"),
    (ShallowCacheAllowlistKind::Variable, "VARIABLE", "VARIABLES"),
    (ShallowCacheAllowlistKind::Schema, "SCHEMA", "SCHEMAS"),
];

#[test]
fn alter_add_single_function() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(dialect, "ALTER READYSET ADD SHALLOW CACHE ALLOWED FUNCTION now");
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
                ShallowCacheAllowlistChange {
                    kind: ShallowCacheAllowlistKind::Function,
                    add: true,
                    names: vec!["now".into()],
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
        SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
            ShallowCacheAllowlistChange {
                kind: ShallowCacheAllowlistKind::Function,
                add: false,
                names: vec!["now".into(), "current_timestamp".into(), "my_udf".into()],
            }
        ))
    );
}

#[test]
fn alter_variable_and_schema() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(
            dialect,
            "ALTER READYSET ADD SHALLOW CACHE ALLOWED VARIABLE version_comment, sql_mode",
        );
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
                ShallowCacheAllowlistChange {
                    kind: ShallowCacheAllowlistKind::Variable,
                    add: true,
                    names: vec!["version_comment".into(), "sql_mode".into()],
                }
            ))
        );

        let q = parse(
            dialect,
            "ALTER READYSET DROP SHALLOW CACHE ALLOWED SCHEMA information_schema",
        );
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
                ShallowCacheAllowlistChange {
                    kind: ShallowCacheAllowlistKind::Schema,
                    add: false,
                    names: vec!["information_schema".into()],
                }
            ))
        );
    }
}

#[test]
fn show_allowed_kinds() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        assert_eq!(
            parse(dialect, "SHOW SHALLOW CACHE ALLOWED FUNCTIONS"),
            SqlQuery::Show(ShowStatement::ShallowCacheAllowlist(
                ShallowCacheAllowlistKind::Function
            ))
        );
        assert_eq!(
            parse(dialect, "SHOW SHALLOW CACHE ALLOWED VARIABLES"),
            SqlQuery::Show(ShowStatement::ShallowCacheAllowlist(
                ShallowCacheAllowlistKind::Variable
            ))
        );
        assert_eq!(
            parse(dialect, "SHOW SHALLOW CACHE ALLOWED SCHEMAS"),
            SqlQuery::Show(ShowStatement::ShallowCacheAllowlist(
                ShallowCacheAllowlistKind::Schema
            ))
        );
    }
}

#[test]
fn alter_add_and_drop_every_kind() {
    // Every (ADD|DROP) x (FUNCTION|VARIABLE|SCHEMA) combination parses to the
    // matching kind and direction, on both dialects.
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        for &(kind, singular, _) in KINDS {
            for add in [true, false] {
                let verb = if add { "ADD" } else { "DROP" };
                let sql =
                    format!("ALTER READYSET {verb} SHALLOW CACHE ALLOWED {singular} a, b, c");
                assert_eq!(
                    parse(dialect, &sql),
                    SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
                        ShallowCacheAllowlistChange {
                            kind,
                            add,
                            names: vec!["a".into(), "b".into(), "c".into()],
                        }
                    )),
                    "failed on {sql:?}"
                );
            }
        }
    }
}

#[test]
fn keywords_are_case_insensitive() {
    // The Readyset-specific keywords and the kind keyword are matched
    // case-insensitively, like the rest of SQL.
    assert_eq!(
        parse(
            Dialect::MySQL,
            "alter readyset add shallow cache allowed variable version_comment"
        ),
        SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(
            ShallowCacheAllowlistChange {
                kind: ShallowCacheAllowlistKind::Variable,
                add: true,
                names: vec!["version_comment".into()],
            }
        ))
    );
    assert_eq!(
        parse(Dialect::PostgreSQL, "show shallow cache allowed schemas"),
        SqlQuery::Show(ShowStatement::ShallowCacheAllowlist(
            ShallowCacheAllowlistKind::Schema
        ))
    );
}

#[test]
fn roundtrips_through_display() {
    // Rendering a parsed statement back to SQL and reparsing it yields the same
    // AST, which exercises the DialectDisplay arms (the singular/plural keyword
    // rendering and identifier quoting) on both dialects.
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        for &(_, singular, plural) in KINDS {
            // `SqlQuery` renders the AlterReadyset *body* without the leading
            // `ALTER READYSET` keyword (shared by the whole AlterReadyset family),
            // so prepend it before reparsing.
            for body in [
                format!("ADD SHALLOW CACHE ALLOWED {singular} a, b"),
                format!("DROP SHALLOW CACHE ALLOWED {singular} a"),
            ] {
                let parsed = parse(dialect, &format!("ALTER READYSET {body}"));
                let rendered = parsed.display(dialect).to_string();
                assert_eq!(
                    parse(dialect, &format!("ALTER READYSET {rendered}")),
                    parsed,
                    "round-trip changed the AST: {body:?} rendered as {rendered:?}"
                );
            }
            // SHOW renders a complete statement (it keeps its `SHOW` keyword), and
            // carries no identifiers, so its canonical rendering is exact.
            let show = format!("SHOW SHALLOW CACHE ALLOWED {plural}");
            let parsed = parse(dialect, &show);
            assert_eq!(parsed.display(dialect).to_string(), show);
            assert_eq!(parse(dialect, &parsed.display(dialect).to_string()), parsed);
        }
    }
}

#[test]
fn rejects_malformed_statements() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        // Unknown target kind after ALLOWED.
        assert!(rejects(
            dialect,
            "ALTER READYSET ADD SHALLOW CACHE ALLOWED WIDGET foo"
        ));
        // ALTER takes the singular kind keyword; the plural is rejected.
        assert!(rejects(
            dialect,
            "ALTER READYSET ADD SHALLOW CACHE ALLOWED FUNCTIONS foo"
        ));
        // A change with no names is not a valid statement.
        assert!(rejects(
            dialect,
            "ALTER READYSET ADD SHALLOW CACHE ALLOWED VARIABLE"
        ));
        // SHOW takes the plural kind keyword; the singular is rejected.
        assert!(rejects(dialect, "SHOW SHALLOW CACHE ALLOWED VARIABLE"));
    }
}
