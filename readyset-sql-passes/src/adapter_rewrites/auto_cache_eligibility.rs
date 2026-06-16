//! Eligibility filter for in-request-path shallow cache auto-creation.
//!
//! When the adapter runs with `--query-caching=inrequestpath` and
//! `--cache-mode=shallow`, every previously-unseen SELECT becomes a candidate
//! for a new shallow cache.  That is too aggressive: client/driver bootstrap
//! traffic (`information_schema` introspection, `@@version_comment`,
//! `SELECT NOW()`, etc.) is structurally a SELECT but should never be cached.
//! This module classifies a parsed [`ShallowCacheQuery`] and returns a short
//! reason string when the query should be excluded from auto-creation.
//!
//! The filter only applies to the implicit in-request-path trigger; explicit
//! `/*rs+ CREATE SHALLOW CACHE */` hints are user-initiated and bypass it.
//!
//! The walk is performed directly over the sqlparser AST (rather than
//! converting to Readyset's `SelectStatement`) because shallow caches
//! deliberately accept queries whose Readyset AST conversion would fail —
//! that is the entire point of carrying the sqlparser tree forward.  Any
//! pre-conversion would defeat the feature for the very queries that need
//! shallow caching.

use std::ops::ControlFlow;

use readyset_sql::Dialect;
use readyset_sql::ast::ShallowCacheQuery;
use sqlparser::ast::{Expr, Function, ObjectName, Value, ValueWithSpan, Visit, Visitor};

const REASON_NON_DETERMINISTIC: &str = "non-deterministic function";
const REASON_SYSTEM_SCHEMA: &str = "system schema reference";
const REASON_USER_VARIABLE: &str = "user or session variable";

/// MySQL system databases.  Mirrors `replicators::MYSQL_INTERNAL_DBS`; kept
/// as a local copy so this crate does not depend on `replicators`.
const MYSQL_SYSTEM_SCHEMAS: &[&str] = &["mysql", "information_schema", "performance_schema", "sys"];

/// PostgreSQL schemas excluded from snapshot replication, mirrored from
/// `replicators::postgres_connector::snapshot`.  Any identifier beginning
/// with `pg_` is also treated as a system reference by [`is_system_schema`]
/// (covers `pg_catalog`, `pg_toast*`, `pg_temp*`, and unqualified catalog
/// tables such as `pg_class`, `pg_type`, `pg_proc`, ...).
const PG_SYSTEM_SCHEMAS: &[&str] = &["information_schema", "readyset"];

/// Functions whose result depends on session/clock/random state and therefore
/// must not be auto-cached.  Compared case-insensitively against the *last*
/// component of the function's name (so `pg_catalog.now()` and `now()` both
/// match).
const NON_DETERMINISTIC_FUNCTIONS: &[&str] = &[
    // Time / clock
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "localtime",
    "localtimestamp",
    "sysdate",
    "clock_timestamp",
    "statement_timestamp",
    "transaction_timestamp",
    "timeofday",
    "unix_timestamp",
    "curdate",
    "curtime",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    // Identity / session
    "user",
    "current_user",
    "session_user",
    "system_user",
    "current_role",
    "current_database",
    "current_schema",
    "current_schemas",
    "current_catalog",
    "database",
    "schema",
    "version",
    "connection_id",
    "pg_backend_pid",
    "ps_current_thread_id",
    "ps_thread_id",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "txid_current",
    "last_insert_id",
    "found_rows",
    "row_count",
    // Sequence state (PostgreSQL); setval writes the sequence
    "lastval",
    "nextval",
    "currval",
    "setval",
    // Randomness
    "rand",
    "random",
    "random_bytes",
    "uuid",
    "uuid_short",
    "gen_random_uuid",
    // Side-effecting / blocking (MySQL)
    "sleep",
    "load_file",
    // Side-effecting / blocking (PostgreSQL)
    "set_config",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_notify",
    "pg_logical_emit_message",
    "dblink",
    "dblink_exec",
    "lo_import",
    "lo_export",
    "lo_create",
    "lo_unlink",
    // Advisory locks (MySQL)
    "get_lock",
    "release_lock",
    "release_all_locks",
    "is_free_lock",
    "is_used_lock",
    // Advisory locks (PostgreSQL)
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_unlock",
    "pg_advisory_unlock_shared",
    "pg_advisory_unlock_all",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
    // Replication waits (MySQL)
    "master_pos_wait",
    "source_pos_wait",
    "wait_for_executed_gtid_set",
    // Misc
    "benchmark",
];

/// Bare identifiers (no parentheses) some dialects parse as session/clock
/// references rather than function calls.  These are matched only when the
/// identifier appears alone (`Expr::Identifier`), to avoid colliding with
/// columns of the same name when used as part of a compound reference.
const NON_DETERMINISTIC_BARE_IDENTIFIERS: &[&str] = &[
    "current_user",
    "current_role",
    "session_user",
    "system_user",
    "current_database",
    "current_schema",
    "current_catalog",
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
];

/// Return `Some(reason)` when this shallow query should be excluded from
/// in-request-path auto-creation, or `None` when it is eligible.
pub fn auto_cache_skip_reason(query: &ShallowCacheQuery, dialect: Dialect) -> Option<&'static str> {
    let mut visitor = AutoCacheVisitor {
        dialect,
        system_schemas: match dialect {
            Dialect::MySQL => MYSQL_SYSTEM_SCHEMAS,
            Dialect::PostgreSQL => PG_SYSTEM_SCHEMAS,
        },
        reason: None,
    };
    let _ = query.visit(&mut visitor);
    visitor.reason
}

struct AutoCacheVisitor {
    dialect: Dialect,
    system_schemas: &'static [&'static str],
    reason: Option<&'static str>,
}

impl Visitor for AutoCacheVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        for part in &name.0 {
            if let Some(ident) = part.as_ident()
                && is_system_schema(&ident.value, self.dialect, self.system_schemas)
            {
                self.reason = Some(REASON_SYSTEM_SCHEMA);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let reason = match expr {
            Expr::Identifier(id) if id.value.starts_with('@') => REASON_USER_VARIABLE,
            Expr::Identifier(id)
                if NON_DETERMINISTIC_BARE_IDENTIFIERS
                    .iter()
                    .any(|s| id.value.eq_ignore_ascii_case(s)) =>
            {
                REASON_NON_DETERMINISTIC
            }
            Expr::CompoundIdentifier(parts)
                if parts.first().is_some_and(|p| p.value.starts_with('@')) =>
            {
                REASON_USER_VARIABLE
            }
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(s),
                ..
            }) if s.starts_with('@') => REASON_USER_VARIABLE,
            Expr::Function(Function { name, .. })
                if name
                    .0
                    .last()
                    .and_then(|p| p.as_ident())
                    .is_some_and(|last| {
                        NON_DETERMINISTIC_FUNCTIONS
                            .iter()
                            .any(|s| last.value.eq_ignore_ascii_case(s))
                    }) =>
            {
                REASON_NON_DETERMINISTIC
            }
            _ => return ControlFlow::Continue(()),
        };
        self.reason = Some(reason);
        ControlFlow::Break(())
    }
}

fn is_system_schema(name: &str, dialect: Dialect, system_schemas: &[&str]) -> bool {
    if system_schemas.iter().any(|s| name.eq_ignore_ascii_case(s)) {
        return true;
    }
    matches!(dialect, Dialect::PostgreSQL)
        && name
            .as_bytes()
            .get(..3)
            .is_some_and(|p| p.eq_ignore_ascii_case(b"pg_"))
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_shallow_query;

    use super::*;

    fn skip_reason(dialect: Dialect, q: &str) -> Option<&'static str> {
        let (parsed, _hint) = parse_shallow_query(dialect, q).expect("shallow parse");
        auto_cache_skip_reason(&parsed, dialect)
    }

    #[track_caller]
    fn assert_eligible(dialect: Dialect, q: &str) {
        assert!(
            skip_reason(dialect, q).is_none(),
            "expected {q:?} to be eligible"
        );
    }

    #[track_caller]
    fn assert_rejected(dialect: Dialect, q: &str, expected_reason: &str) {
        let msg = skip_reason(dialect, q).unwrap_or_else(|| panic!("expected {q:?} to be skipped"));
        assert!(
            msg.contains(expected_reason),
            "query {q:?} skipped as {msg:?}, expected mention of {expected_reason:?}"
        );
    }

    #[test]
    fn plain_select_is_eligible() {
        assert_eligible(Dialect::MySQL, "SELECT id, name FROM users WHERE id = 1");
        assert_eligible(
            Dialect::PostgreSQL,
            "SELECT id, name FROM users WHERE id = 1",
        );
    }

    #[test]
    fn join_with_user_table_is_eligible() {
        assert_eligible(
            Dialect::MySQL,
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id",
        );
    }

    #[test]
    fn information_schema_is_blocked() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT * FROM information_schema.tables WHERE table_schema = 'app'",
            REASON_SYSTEM_SCHEMA,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT table_name FROM information_schema.tables",
            REASON_SYSTEM_SCHEMA,
        );
    }

    #[test]
    fn mysql_system_schemas_are_blocked() {
        for q in [
            "SELECT * FROM mysql.user",
            "SELECT * FROM performance_schema.session_variables",
            "SELECT * FROM sys.processlist",
        ] {
            assert_rejected(Dialect::MySQL, q, REASON_SYSTEM_SCHEMA);
        }
    }

    #[test]
    fn postgres_system_schemas_are_blocked() {
        for q in [
            "SELECT * FROM pg_catalog.pg_class",
            "SELECT * FROM pg_toast.pg_toast_2619",
            "SELECT * FROM readyset.queries",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_SYSTEM_SCHEMA);
        }
    }

    #[test]
    fn postgres_unqualified_catalog_tables_are_blocked() {
        for q in [
            "SELECT oid, typname FROM pg_type",
            "SELECT * FROM pg_class",
            "SELECT * FROM pg_proc",
            "SELECT * FROM pg_attribute",
            "SELECT name FROM pg_timezone_names",
            "SELECT * FROM PG_Class",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_SYSTEM_SCHEMA);
        }
    }

    #[test]
    fn mysql_pg_prefixed_table_is_eligible() {
        assert_eligible(Dialect::MySQL, "SELECT * FROM pg_my_user_table");
    }

    #[test]
    fn user_and_session_variables_are_blocked() {
        assert_rejected(Dialect::MySQL, "SELECT @@version", REASON_USER_VARIABLE);
        assert_rejected(
            Dialect::MySQL,
            "SELECT @@global.max_connections",
            REASON_USER_VARIABLE,
        );
        assert_rejected(Dialect::MySQL, "SELECT @my_user_var", REASON_USER_VARIABLE);
        assert_rejected(
            Dialect::MySQL,
            "SELECT id FROM users WHERE id = @target_id",
            REASON_USER_VARIABLE,
        );
    }

    #[test]
    fn non_deterministic_calls_are_blocked() {
        for q in [
            "SELECT NOW()",
            "SELECT current_timestamp()",
            "SELECT current_user()",
            "SELECT RAND()",
            "SELECT uuid()",
            "SELECT VERSION()",
            "SELECT DATABASE()",
            "SELECT connection_id()",
            "SELECT last_insert_id()",
            "SELECT CURDATE()",
            "SELECT CURTIME()",
            "SELECT UTC_TIMESTAMP()",
            "SELECT SLEEP(0)",
            "SELECT LOAD_FILE('/etc/passwd')",
            "SELECT GET_LOCK('a', 0)",
            "SELECT RELEASE_LOCK('a')",
            "SELECT IS_FREE_LOCK('a')",
            "SELECT MASTER_POS_WAIT('f', 0)",
            "SELECT SOURCE_POS_WAIT('f', 0)",
            "SELECT WAIT_FOR_EXECUTED_GTID_SET('a:1')",
            "SELECT RANDOM_BYTES(16)",
        ] {
            assert_rejected(Dialect::MySQL, q, REASON_NON_DETERMINISTIC);
        }
        for q in [
            "SELECT random()",
            "SELECT gen_random_uuid()",
            "SELECT clock_timestamp()",
            "SELECT current_database()",
            "SELECT pg_backend_pid()",
            "SELECT lastval()",
            "SELECT nextval('s')",
            "SELECT currval('s')",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_NON_DETERMINISTIC);
        }
    }

    #[test]
    fn bare_session_keywords_are_blocked() {
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_user",
            REASON_NON_DETERMINISTIC,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_timestamp",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn deterministic_function_calls_are_eligible() {
        assert_eligible(
            Dialect::MySQL,
            "SELECT LOWER(name), CONCAT(a, b) FROM t WHERE LENGTH(x) > 3",
        );
    }

    // The visitor inspects the whole query tree, so a non-deterministic or
    // side-effecting call outside the projection and WHERE clause is caught
    // too. The prior hand-rolled walk did not descend into these positions.
    #[test]
    fn non_deterministic_function_in_order_by_is_blocked() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT id FROM users ORDER BY RAND()",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn non_deterministic_function_in_group_by_is_blocked() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT count(*) FROM users GROUP BY RAND()",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn non_deterministic_function_in_join_condition_is_blocked() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id AND o.created > NOW()",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn non_deterministic_function_in_window_spec_is_blocked() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT ROW_NUMBER() OVER (ORDER BY RAND()) FROM users",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn nested_subquery_propagates_block() {
        assert_rejected(
            Dialect::MySQL,
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM information_schema.tables)",
            REASON_SYSTEM_SCHEMA,
        );
        // The EXISTS subquery contains a non-deterministic call and a user
        // variable; the walk reaches NOW() first, but either way the query must
        // be flagged.
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT id FROM users WHERE EXISTS (SELECT 1 WHERE NOW() < @cutoff)"
            ),
            Some(REASON_NON_DETERMINISTIC)
        );
    }

    #[test]
    fn set_config_calls_are_blocked() {
        // set_config writes a session/transaction GUC; serving from cache would
        // silently drop the side effect.  Block regardless of arg shape, batch
        // shape, surrounding query, or schema qualification.
        for q in [
            "SELECT set_config('role', 'admin', true)",
            "SELECT set_config($1, $2, $3)",
            "SELECT set_config($1, $2, $3), set_config($4, $5, $6)",
            "SELECT pg_catalog.set_config('role', 'admin', true)",
            "SELECT SET_CONFIG($1, $2, $3)",
            "SELECT set_config('role', 'admin', true), 1",
            "SELECT set_config('role', 'admin', true) FROM users",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_NON_DETERMINISTIC);
        }
    }

    #[test]
    fn cte_with_system_schema_is_blocked() {
        assert_rejected(
            Dialect::PostgreSQL,
            "WITH t AS (SELECT * FROM pg_catalog.pg_class) SELECT * FROM t",
            REASON_SYSTEM_SCHEMA,
        );
    }
}
