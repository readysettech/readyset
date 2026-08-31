//! Parse tests for the dual-password `ALTER READYSET MODIFY USER` forms.

use readyset_sql::ast::{
    AlterReadysetStatement, ModifyUserAction, ModifyUserStatement, SqlQuery,
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

#[test]
fn modify_user_retain_current_password() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(
            dialect,
            "ALTER READYSET MODIFY USER 'alice' PASSWORD 'newsecret' RETAIN CURRENT PASSWORD",
        );
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ModifyUser(ModifyUserStatement {
                user: "alice".into(),
                action: ModifyUserAction::SetPassword {
                    password: "newsecret".to_string().into(),
                    retain_current: true,
                },
            }))
        );
    }
}

#[test]
fn modify_user_discard_old_password() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(dialect, "ALTER READYSET MODIFY USER 'alice' DISCARD OLD PASSWORD");
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ModifyUser(ModifyUserStatement {
                user: "alice".into(),
                action: ModifyUserAction::DiscardOldPassword,
            }))
        );
    }
}

#[test]
fn modify_user_plain_password() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        let q = parse(dialect, "ALTER READYSET MODIFY USER 'alice' PASSWORD 'newsecret'");
        assert_eq!(
            q,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ModifyUser(ModifyUserStatement {
                user: "alice".into(),
                action: ModifyUserAction::SetPassword {
                    password: "newsecret".to_string().into(),
                    retain_current: false,
                },
            }))
        );
    }
}

#[test]
fn roundtrips_through_display() {
    // `SqlQuery` renders the AlterReadyset body without the leading `ALTER READYSET`
    // keywords, so prepend them before reparsing.
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        for body in [
            "MODIFY USER 'alice' PASSWORD 'newsecret' RETAIN CURRENT PASSWORD",
            "MODIFY USER 'alice' DISCARD OLD PASSWORD",
        ] {
            let parsed = parse(dialect, &format!("ALTER READYSET {body}"));
            let rendered = parsed.display(dialect).to_string();
            assert_eq!(
                parse(dialect, &format!("ALTER READYSET {rendered}")),
                parsed,
                "round-trip changed the AST: {body:?} rendered as {rendered:?}"
            );
        }
    }
}

#[test]
fn rejects_malformed_statements() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        // DISCARD requires both OLD and PASSWORD.
        assert!(rejects(dialect, "ALTER READYSET MODIFY USER 'alice' DISCARD PASSWORD"));
        assert!(rejects(dialect, "ALTER READYSET MODIFY USER 'alice' DISCARD OLD"));
        // A partial RETAIN clause leaves unconsumed tokens.
        assert!(rejects(
            dialect,
            "ALTER READYSET MODIFY USER 'alice' PASSWORD 'x' RETAIN PASSWORD"
        ));
        assert!(rejects(
            dialect,
            "ALTER READYSET MODIFY USER 'alice' PASSWORD 'x' RETAIN CURRENT"
        ));
    }
}
