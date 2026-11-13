//! Parse tests for `FOR CHANNEL` on the replication `ALTER READYSET` statements.

use readyset_sql::ast::{AlterReadysetStatement, SetReplicationPositionStatement, SqlQuery};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

const DIALECTS: [Dialect; 2] = [Dialect::MySQL, Dialect::PostgreSQL];

fn parse(dialect: Dialect, sql: &str) -> AlterReadysetStatement {
    match parse_query_with_config(ParsingPreset::OnlySqlparser.into_config(), dialect, sql) {
        Ok(SqlQuery::AlterReadySet(stmt)) => stmt,
        other => panic!("{sql:?} parsed as {other:?}"),
    }
}

fn rejects(dialect: Dialect, sql: &str) -> bool {
    parse_query_with_config(ParsingPreset::OnlySqlparser.into_config(), dialect, sql).is_err()
}

fn set_position(position: &str, channel: Option<&str>) -> AlterReadysetStatement {
    AlterReadysetStatement::SetReplicationPosition(SetReplicationPositionStatement {
        position: position.to_string(),
        channel: channel.map(str::to_string),
    })
}

#[test]
fn stop_and_start_replication() {
    for dialect in DIALECTS {
        assert_eq!(
            parse(dialect, "ALTER READYSET STOP REPLICATION"),
            AlterReadysetStatement::StopReplication { channel: None }
        );
        assert_eq!(
            parse(dialect, "ALTER READYSET STOP REPLICATION FOR CHANNEL 'db2'"),
            AlterReadysetStatement::StopReplication {
                channel: Some("db2".to_string())
            }
        );
        assert_eq!(
            parse(dialect, "ALTER READYSET START REPLICATION"),
            AlterReadysetStatement::StartReplication { channel: None }
        );
        assert_eq!(
            parse(dialect, "alter readyset start replication for channel ''"),
            AlterReadysetStatement::StartReplication {
                channel: Some(String::new())
            }
        );
    }
}

#[test]
fn set_replication_position() {
    for dialect in DIALECTS {
        assert_eq!(
            parse(
                dialect,
                "ALTER READYSET SET REPLICATION POSITION '0/16B3748'"
            ),
            set_position("0/16B3748", None)
        );
        assert_eq!(
            parse(
                dialect,
                "ALTER READYSET SET REPLICATION POSITION '0/16B3748' FOR CHANNEL 'db2'"
            ),
            set_position("0/16B3748", Some("db2"))
        );
    }
}

#[test]
fn roundtrips_through_display() {
    // `SqlQuery` renders the AlterReadyset body without the leading `ALTER READYSET`
    // keywords, so prepend them before reparsing.
    for dialect in DIALECTS {
        for body in [
            "STOP REPLICATION",
            "STOP REPLICATION FOR CHANNEL 'db2'",
            "START REPLICATION FOR CHANNEL ''",
            "START REPLICATION FOR CHANNEL 'it''s'",
            "SET REPLICATION POSITION '0/16B3748'",
            "SET REPLICATION POSITION '0/16B3748' FOR CHANNEL 'db2'",
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
fn rejects_malformed_clauses() {
    for dialect in DIALECTS {
        assert!(rejects(
            dialect,
            "ALTER READYSET STOP REPLICATION FOR CHANNEL"
        ));
        assert!(rejects(
            dialect,
            "ALTER READYSET START REPLICATION CHANNEL 'db2'"
        ));
        assert!(rejects(
            dialect,
            "ALTER READYSET START REPLICATION FOR 'db2'"
        ));
        assert!(rejects(
            dialect,
            "ALTER READYSET SET REPLICATION POSITION FOR CHANNEL 'db2'"
        ));
    }
}
