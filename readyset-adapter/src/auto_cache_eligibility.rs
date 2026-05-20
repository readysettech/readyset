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

use readyset_sql::Dialect;
use readyset_sql::ast::ShallowCacheQuery;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query, Select,
    SelectItem, SetExpr, TableFactor, TableWithJoins, Value, ValueWithSpan,
};

pub const REASON_SYSTEM_SCHEMA: &str = "system schema reference";
pub const REASON_USER_VARIABLE: &str = "user or session variable";
pub const REASON_NON_DETERMINISTIC: &str = "non-deterministic function";

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
    // Sequence state (PostgreSQL)
    "lastval",
    "nextval",
    "currval",
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
    // Advisory locks (MySQL)
    "get_lock",
    "release_lock",
    "release_all_locks",
    "is_free_lock",
    "is_used_lock",
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
    let ctx = Ctx {
        dialect,
        system_schemas: match dialect {
            Dialect::MySQL => MYSQL_SYSTEM_SCHEMAS,
            Dialect::PostgreSQL => PG_SYSTEM_SCHEMAS,
        },
    };
    walk_query(query, &ctx)
}

#[derive(Clone, Copy)]
struct Ctx {
    dialect: Dialect,
    system_schemas: &'static [&'static str],
}

fn walk_query(q: &Query, ctx: &Ctx) -> Option<&'static str> {
    if let Some(with) = &q.with {
        for cte in &with.cte_tables {
            if let Some(r) = walk_query(&cte.query, ctx) {
                return Some(r);
            }
        }
    }
    walk_set_expr(&q.body, ctx)
}

fn walk_set_expr(s: &SetExpr, ctx: &Ctx) -> Option<&'static str> {
    match s {
        SetExpr::Select(sel) => walk_select(sel, ctx),
        SetExpr::Query(q) => walk_query(q, ctx),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left, ctx).or_else(|| walk_set_expr(right, ctx))
        }
        // VALUES / INSERT / UPDATE / DELETE / MERGE / TABLE never reach this
        // path: `parse_shallow_query` rejects them at parse time.
        _ => None,
    }
}

fn walk_select(s: &Select, ctx: &Ctx) -> Option<&'static str> {
    for twj in &s.from {
        if let Some(r) = walk_table_with_joins(twj, ctx) {
            return Some(r);
        }
    }
    for item in &s.projection {
        if let Some(r) = walk_select_item(item, ctx) {
            return Some(r);
        }
    }
    if let Some(e) = &s.selection
        && let Some(r) = walk_expr(e, ctx)
    {
        return Some(r);
    }
    if let Some(e) = &s.having
        && let Some(r) = walk_expr(e, ctx)
    {
        return Some(r);
    }
    None
}

fn walk_select_item(item: &SelectItem, ctx: &Ctx) -> Option<&'static str> {
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => walk_expr(e, ctx),
        // Wildcards (`*`, `t.*`) carry no expressions to inspect.
        _ => None,
    }
}

fn walk_table_with_joins(twj: &TableWithJoins, ctx: &Ctx) -> Option<&'static str> {
    if let Some(r) = walk_table_factor(&twj.relation, ctx) {
        return Some(r);
    }
    for j in &twj.joins {
        if let Some(r) = walk_table_factor(&j.relation, ctx) {
            return Some(r);
        }
    }
    None
}

fn walk_table_factor(t: &TableFactor, ctx: &Ctx) -> Option<&'static str> {
    match t {
        TableFactor::Table { name, .. } => check_object_name_for_system_schema(name, ctx),
        TableFactor::Derived { subquery, .. } => walk_query(subquery, ctx),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => walk_table_with_joins(table_with_joins, ctx),
        // Less-common factors (UNNEST, JsonTable, Pivot, etc.) are not
        // inspected; if they reference a system schema we will fall back to
        // existing behaviour (auto-create may proceed).  We still recurse via
        // the surrounding query for nested SELECTs.
        _ => None,
    }
}

fn check_object_name_for_system_schema(name: &ObjectName, ctx: &Ctx) -> Option<&'static str> {
    for part in &name.0 {
        if let Some(ident) = part.as_ident()
            && is_system_schema(&ident.value, ctx)
        {
            return Some(REASON_SYSTEM_SCHEMA);
        }
    }
    None
}

fn is_system_schema(name: &str, ctx: &Ctx) -> bool {
    if ctx
        .system_schemas
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s))
    {
        return true;
    }
    if matches!(ctx.dialect, Dialect::PostgreSQL)
        && name
            .as_bytes()
            .get(..3)
            .is_some_and(|p| p.eq_ignore_ascii_case(b"pg_"))
    {
        return true;
    }
    false
}

fn walk_expr(e: &Expr, ctx: &Ctx) -> Option<&'static str> {
    if let Some(r) = check_expr_self(e) {
        return Some(r);
    }
    walk_expr_children(e, ctx)
}

fn check_expr_self(e: &Expr) -> Option<&'static str> {
    match e {
        Expr::Identifier(id) if id.value.starts_with('@') => Some(REASON_USER_VARIABLE),
        Expr::Identifier(id)
            if NON_DETERMINISTIC_BARE_IDENTIFIERS
                .iter()
                .any(|s| id.value.eq_ignore_ascii_case(s)) =>
        {
            Some(REASON_NON_DETERMINISTIC)
        }
        Expr::CompoundIdentifier(parts)
            if parts.first().is_some_and(|p| p.value.starts_with('@')) =>
        {
            Some(REASON_USER_VARIABLE)
        }
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(s),
            ..
        }) if s.starts_with('@') => Some(REASON_USER_VARIABLE),
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
            Some(REASON_NON_DETERMINISTIC)
        }
        _ => None,
    }
}

fn walk_expr_children(e: &Expr, ctx: &Ctx) -> Option<&'static str> {
    match e {
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, ctx).or_else(|| walk_expr(right, ctx))
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. } => walk_expr(expr, ctx),
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => walk_query(q, ctx),
        Expr::InList { expr, list, .. } => {
            walk_expr(expr, ctx).or_else(|| list.iter().find_map(|x| walk_expr(x, ctx)))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            walk_expr(expr, ctx).or_else(|| walk_query(subquery, ctx))
        }
        Expr::Between {
            expr, low, high, ..
        } => walk_expr(expr, ctx)
            .or_else(|| walk_expr(low, ctx))
            .or_else(|| walk_expr(high, ctx)),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            walk_expr(expr, ctx).or_else(|| walk_expr(pattern, ctx))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand
                && let Some(r) = walk_expr(op, ctx)
            {
                return Some(r);
            }
            for cw in conditions {
                if let Some(r) = walk_expr(&cw.condition, ctx) {
                    return Some(r);
                }
                if let Some(r) = walk_expr(&cw.result, ctx) {
                    return Some(r);
                }
            }
            else_result.as_deref().and_then(|er| walk_expr(er, ctx))
        }
        Expr::Function(f) => walk_function_args(&f.args, ctx)
            .or_else(|| walk_function_args(&f.parameters, ctx))
            .or_else(|| {
                f.filter
                    .as_deref()
                    .and_then(|filter| walk_expr(filter, ctx))
            }),
        Expr::Tuple(items) => items.iter().find_map(|x| walk_expr(x, ctx)),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => walk_expr(timestamp, ctx).or_else(|| walk_expr(time_zone, ctx)),
        // Variants we deliberately do not recurse into (literals, wildcards,
        // typed values, dialect-specific exotica).  Any user-variable or
        // non-deterministic call that hides inside one of these will just
        // fall through to the existing auto-create behaviour.
        _ => None,
    }
}

fn walk_function_args(args: &FunctionArguments, ctx: &Ctx) -> Option<&'static str> {
    match args {
        FunctionArguments::None => None,
        FunctionArguments::Subquery(q) => walk_query(q, ctx),
        FunctionArguments::List(list) => {
            for arg in &list.args {
                if let Some(r) = walk_function_arg(arg, ctx) {
                    return Some(r);
                }
            }
            None
        }
    }
}

fn walk_function_arg(arg: &FunctionArg, ctx: &Ctx) -> Option<&'static str> {
    let arg_expr = match arg {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => arg,
    };
    match arg_expr {
        FunctionArgExpr::Expr(e) => walk_expr(e, ctx),
        FunctionArgExpr::QualifiedWildcard(_)
        | FunctionArgExpr::Wildcard
        | FunctionArgExpr::WildcardWithOptions(_) => None,
    }
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

    #[test]
    fn plain_select_is_eligible() {
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT id, name FROM users WHERE id = 1"),
            None
        );
        assert_eq!(
            skip_reason(
                Dialect::PostgreSQL,
                "SELECT id, name FROM users WHERE id = 1"
            ),
            None
        );
    }

    #[test]
    fn join_with_user_table_is_eligible() {
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id"
            ),
            None
        );
    }

    #[test]
    fn information_schema_is_blocked() {
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables WHERE table_schema = 'app'"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
        assert_eq!(
            skip_reason(
                Dialect::PostgreSQL,
                "SELECT table_name FROM information_schema.tables"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
    }

    #[test]
    fn mysql_system_schemas_are_blocked() {
        for q in [
            "SELECT * FROM mysql.user",
            "SELECT * FROM performance_schema.session_variables",
            "SELECT * FROM sys.processlist",
        ] {
            assert_eq!(
                skip_reason(Dialect::MySQL, q),
                Some(REASON_SYSTEM_SCHEMA),
                "query was {q}"
            );
        }
    }

    #[test]
    fn postgres_system_schemas_are_blocked() {
        for q in [
            "SELECT * FROM pg_catalog.pg_class",
            "SELECT * FROM pg_toast.pg_toast_2619",
            "SELECT * FROM readyset.queries",
        ] {
            assert_eq!(
                skip_reason(Dialect::PostgreSQL, q),
                Some(REASON_SYSTEM_SCHEMA),
                "query was {q}"
            );
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
            assert_eq!(
                skip_reason(Dialect::PostgreSQL, q),
                Some(REASON_SYSTEM_SCHEMA),
                "query was {q}"
            );
        }
    }

    #[test]
    fn mysql_pg_prefixed_table_is_eligible() {
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT * FROM pg_my_user_table"),
            None
        );
    }

    #[test]
    fn user_and_session_variables_are_blocked() {
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT @@version"),
            Some(REASON_USER_VARIABLE)
        );
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT @@global.max_connections"),
            Some(REASON_USER_VARIABLE)
        );
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT @my_user_var"),
            Some(REASON_USER_VARIABLE)
        );
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT id FROM users WHERE id = @target_id"),
            Some(REASON_USER_VARIABLE)
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
            assert_eq!(
                skip_reason(Dialect::MySQL, q),
                Some(REASON_NON_DETERMINISTIC),
                "query was {q}"
            );
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
            assert_eq!(
                skip_reason(Dialect::PostgreSQL, q),
                Some(REASON_NON_DETERMINISTIC),
                "query was {q}"
            );
        }
    }

    #[test]
    fn bare_session_keywords_are_blocked() {
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT current_user"),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT current_timestamp"),
            Some(REASON_NON_DETERMINISTIC)
        );
    }

    #[test]
    fn deterministic_function_calls_are_eligible() {
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT LOWER(name), CONCAT(a, b) FROM t WHERE LENGTH(x) > 3"
            ),
            None
        );
    }

    #[test]
    fn nested_subquery_propagates_block() {
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT id FROM users WHERE id IN (SELECT user_id FROM information_schema.tables)"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
        assert_eq!(
            skip_reason(
                Dialect::MySQL,
                "SELECT id FROM users WHERE EXISTS (SELECT 1 WHERE NOW() < @cutoff)"
            ),
            // Outer expr walk reaches the EXISTS first; the inner reason
            // depends on traversal order, but the query must be flagged.
            Some(REASON_NON_DETERMINISTIC)
        );
    }

    #[test]
    fn cte_with_system_schema_is_blocked() {
        assert_eq!(
            skip_reason(
                Dialect::PostgreSQL,
                "WITH t AS (SELECT * FROM pg_catalog.pg_class) SELECT * FROM t"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
    }
}
