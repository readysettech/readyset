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
//! Calls to user-defined functions are excluded too, since a UDF may have side
//! effects or be non-deterministic.  For PostgreSQL the pass is default-deny: a
//! call is eligible only if it is an IMMUTABLE builtin (identified by
//! `pg_proc.provolatile` and generated into `pg_builtins`), so any session-,
//! clock-, or table-state-dependent builtin is treated as non-deterministic
//! without a hand-curated list.  MySQL is default-deny too, but exposes no
//! volatility signal, so its immutable set is the hand-curated
//! `MYSQL_IMMUTABLE_FUNCTIONS`: only those builtins are cacheable and every
//! other builtin is denied.  A call whose name is in neither dialect's builtin
//! set is treated as user-defined.
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

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use readyset_sql::Dialect;
use readyset_sql::ast::{ShallowCacheAllowlistKind, ShallowCacheQuery};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Expr, Function, ObjectName, Query, SetExpr, TableFactor, TableSampleKind, Value, ValueWithSpan,
    Visit, Visitor,
};

// Generated builtin-function allowlists, one sorted `BUILTINS: &[&str]` each.
// Regenerate with the gen_*_builtins.py scripts in this directory.
mod mysql_builtins;
mod pg_builtins;

const REASON_NON_DETERMINISTIC: &str = "non-deterministic function";
const REASON_SIDE_EFFECT: &str = "side-effecting function";
const REASON_WRITE_IN_CTE: &str = "write statement inside a CTE body";
const REASON_SYSTEM_SCHEMA: &str = "system schema reference";
const REASON_SESSION_SPECIFIC: &str = "session-specific function";
const REASON_USER_VARIABLE: &str = "user or session variable";
const REASON_UDF: &str = "user-defined function";
// Default-deny catch-all: a recognized builtin not proven cacheable and not on
// any relaxable list. Distinct from the specific hazard reasons above.
const REASON_BUILTIN_NOT_ALLOWED: &str = "builtin not allowed for caching";
const REASON_UNSEEDED_TABLESAMPLE: &str = "unseeded TABLESAMPLE";

/// MySQL system databases.  Mirrors `replicators::MYSQL_INTERNAL_DBS`; kept
/// as a local copy so this crate does not depend on `replicators`.
const MYSQL_SYSTEM_SCHEMAS: &[&str] = &["mysql", "information_schema", "performance_schema", "sys"];

/// PostgreSQL schemas excluded from snapshot replication, mirrored from
/// `replicators::postgres_connector::snapshot`.  Any identifier beginning
/// with `pg_` is also treated as a system reference by [`is_system_schema`]
/// (covers `pg_catalog`, `pg_toast*`, `pg_temp*`, and unqualified catalog
/// tables such as `pg_class`, `pg_type`, `pg_proc`, ...).
const PG_SYSTEM_SCHEMAS: &[&str] = &["information_schema", "readyset"];

/// MySQL builtins whose result depends only on their arguments: deterministic,
/// side-effect-free, and independent of session, connection, locale, time zone,
/// clock, and table state. This is the MySQL analogue of PostgreSQL's
/// `provolatile = 'i'` set, but hand-curated, since MySQL exposes no volatility
/// signal to generate it from. Under default-deny this is the base allow-list --
/// a builtin absent from it (and from the opt-in lists below) is denied -- so it
/// errs toward omission: a missing deterministic function only under-caches,
/// whereas a wrongly-included non-deterministic one would serve stale results.
/// Deliberately excluded are functions that vary with `lc_time_names` or
/// `lc_numeric` (`date_format`, `str_to_date`, `dayname`, `format`), the session
/// time zone (`from_unixtime`, `convert_tz`), or a session variable
/// (`week`/`yearweek` honor `default_week_format`). Standard-SQL keyword
/// functions (`cast`, `coalesce`, `greatest`, ...) are handled by
/// [`is_keyword_function`]. Not exhaustive by design.
const MYSQL_IMMUTABLE_FUNCTIONS: &[&str] = &[
    // Numeric and mathematical.
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "bit_count",
    "ceil",
    "ceiling",
    "conv",
    "cos",
    "cot",
    "crc32",
    "degrees",
    "exp",
    "floor",
    "ln",
    "log",
    "log10",
    "log2",
    "mod",
    "pi",
    "pow",
    "power",
    "radians",
    "round",
    "sign",
    "sin",
    "sqrt",
    "tan",
    "truncate",
    // String.
    "ascii",
    "bin",
    "bit_length",
    "char",
    "char_length",
    "character_length",
    "concat",
    "concat_ws",
    "elt",
    "export_set",
    "field",
    "find_in_set",
    "from_base64",
    "hex",
    "insert",
    "instr",
    "lcase",
    "left",
    "length",
    "locate",
    "lower",
    "lpad",
    "ltrim",
    "make_set",
    "mid",
    "oct",
    "octet_length",
    "ord",
    "quote",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rpad",
    "rtrim",
    "soundex",
    "space",
    "strcmp",
    "substring_index",
    "to_base64",
    "ucase",
    "unhex",
    "upper",
    // Hashing.
    "md5",
    "sha",
    "sha1",
    "sha2",
    // Conditional and comparison (non-keyword).
    "if",
    "ifnull",
    "interval",
    "isnull",
    // Date/time arithmetic and extraction (no clock, locale, or time zone).
    "adddate",
    "addtime",
    "date",
    "date_add",
    "date_sub",
    "datediff",
    "day",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "from_days",
    "hour",
    "last_day",
    "makedate",
    "maketime",
    "microsecond",
    "minute",
    "month",
    "period_add",
    "period_diff",
    "quarter",
    "sec_to_time",
    "second",
    "subdate",
    "subtime",
    "time",
    "time_to_sec",
    "timediff",
    "timestamp",
    "timestampadd",
    "timestampdiff",
    "to_days",
    "to_seconds",
    "weekday",
    "weekofyear",
    "year",
    // Network-address and UUID conversion.
    "bin_to_uuid",
    "inet6_aton",
    "inet6_ntoa",
    "inet_aton",
    "inet_ntoa",
    "is_ipv4",
    "is_ipv4_compat",
    "is_ipv4_mapped",
    "is_ipv6",
    "is_uuid",
    "uuid_to_bin",
    // JSON (pure functions of their arguments).
    "json_array",
    "json_array_append",
    "json_array_insert",
    "json_contains",
    "json_contains_path",
    "json_depth",
    "json_extract",
    "json_insert",
    "json_keys",
    "json_length",
    "json_merge_patch",
    "json_merge_preserve",
    "json_object",
    "json_overlaps",
    "json_pretty",
    "json_quote",
    "json_remove",
    "json_replace",
    "json_search",
    "json_set",
    "json_storage_size",
    "json_type",
    "json_unquote",
    "json_valid",
    "json_value",
];

/// MySQL non-deterministic builtins: clock and randomness whose result varies
/// across statements. Under default-deny these are already excluded (absent from
/// [`MYSQL_IMMUTABLE_FUNCTIONS`]); enumerating them here routes them to
/// `--shallow-cache-allow-nondeterministic` rather than the broad
/// `--shallow-cache-allow-all-functions`. Session-dependent and side-effecting
/// builtins have their own opt-in lists below. Not exhaustive: it only needs to
/// cover functions worth exposing a narrow opt-in for.
const MYSQL_NONDETERMINISTIC_FUNCTIONS: &[&str] = &[
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "localtime",
    "localtimestamp",
    "sysdate",
    "unix_timestamp",
    "curdate",
    "curtime",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    "version",
    "rand",
    "uuid",
    "uuid_short",
    "random_bytes",
    "is_free_lock",
    "is_used_lock",
    "icu_version",
    "group_replication_get_communication_protocol",
    "group_replication_get_write_concurrency",
    // Charset/collation introspection and password-strength checks vary with
    // connection or server state.
    "charset",
    "collation",
    "coercibility",
    "validate_password_strength",
    // information_schema internal storage-stat readers reflect live table state.
    "internal_auto_increment",
    "internal_avg_row_length",
    "internal_check_time",
    "internal_data_free",
    "internal_data_length",
    "internal_index_length",
    "internal_max_data_length",
    "internal_update_time",
];

/// MySQL builtins whose result depends on session identity (current user, role,
/// database, schema) or privileges. Caching one would serve another session's
/// value; `--shallow-cache-allow-session-specific` opts into caching these.
///
/// Connection- and statement-state reads (`last_insert_id`, `connection_id`,
/// `found_rows`, `row_count`, the `ps_*` thread ids) are deliberately absent: a
/// cached value is always some other connection's, so they are left to the
/// builtin default-deny that no per-category flag relaxes. Only
/// `--shallow-cache-allow-all-functions` or the runtime allowlist caches one.
const MYSQL_SESSION_SPECIFIC_FUNCTIONS: &[&str] = &[
    "user",
    "current_user",
    "session_user",
    "system_user",
    "current_role",
    "database",
    "schema",
    "can_access_database",
    "can_access_column",
    "can_access_table",
    "can_access_user",
    "can_access_view",
];

/// MySQL side-effecting or blocking builtins. Under default-deny these are
/// already denied (absent from [`MYSQL_IMMUTABLE_FUNCTIONS`]); this list is not
/// a guard but a reason refinement -- it reports the accurate
/// [`REASON_SIDE_EFFECT`] instead of the generic default-deny reason for
/// functions that are side-effecting (a blocking `sleep`, a lock acquisition).
/// Reachable only via
/// `--shallow-cache-allow-all-functions` or the runtime allowlist; there is no
/// dedicated side-effect opt-in. Not exhaustive; a missing entry only yields the
/// generic reason, never a caching hole.
const MYSQL_SIDE_EFFECTING_FUNCTIONS: &[&str] = &[
    "load_file",
    "get_lock",
    "release_lock",
    "release_all_locks",
    "sleep",
    "benchmark",
    "master_pos_wait",
    "source_pos_wait",
    "wait_for_executed_gtid_set",
    "wait_until_sql_thread_after_gtids",
    "asynchronous_connection_failover_add_managed",
    "asynchronous_connection_failover_add_source",
    "asynchronous_connection_failover_delete_managed",
    "asynchronous_connection_failover_delete_source",
    "asynchronous_connection_failover_reset",
    "group_replication_disable_member_action",
    "group_replication_enable_member_action",
    "group_replication_reset_member_actions",
    "group_replication_set_as_primary",
    "group_replication_set_communication_protocol",
    "group_replication_set_write_concurrency",
    "group_replication_switch_to_multi_primary_mode",
    "group_replication_switch_to_single_primary_mode",
];

/// PostgreSQL builtins whose result depends on session/connection state. These
/// are non-IMMUTABLE, so they are denied by default; naming them here routes
/// them to `--shallow-cache-allow-session-specific` rather than the broader
/// `--shallow-cache-allow-nondeterministic`. The bare-identifier session
/// specials (`current_user`, ...) are handled by [`SESSION_SPECIFIC_BARE_IDENTIFIERS`].
///
/// Connection-identity reads (`pg_backend_pid`, `inet_client_addr`,
/// `inet_client_port`) are deliberately absent: a cached value is always some
/// other connection's, so they fall to the builtin default-deny that no
/// per-category flag relaxes. `inet_server_*` stay -- the server address is the
/// same for every session.
const PG_SESSION_SPECIFIC_FUNCTIONS: &[&str] = &[
    // Identity/context specials, which sqlparser may surface as a function call
    // (`current_user`) as well as a bare identifier (see
    // [`SESSION_SPECIFIC_BARE_IDENTIFIERS`]).
    "current_user",
    "current_role",
    "session_user",
    "system_user",
    "current_database",
    "current_schema",
    "current_catalog",
    // Function-only session reads.
    "current_setting",
    "current_query",
    "inet_server_addr",
    "inet_server_port",
];

/// PostgreSQL non-deterministic builtins worth an explicit opt-in: clock and
/// randomness whose result merely varies (staleness), not the whole
/// non-IMMUTABLE set. `--shallow-cache-allow-nondeterministic` is a curated
/// allow-list, so it never sweeps in side-effecting builtins (which stay
/// denied, reachable only via `--shallow-cache-allow-all-functions`). Any other
/// non-IMMUTABLE builtin not named here is denied by default and cacheable only
/// via `--shallow-cache-allow-all-functions`. Not exhaustive.
const PG_NONDETERMINISTIC_FUNCTIONS: &[&str] = &[
    "now",
    "transaction_timestamp",
    "statement_timestamp",
    "clock_timestamp",
    "timeofday",
    "random",
    "random_normal",
    "gen_random_uuid",
];

/// PostgreSQL side-effecting builtins. Like the MySQL list above, under
/// default-deny these are already denied (non-IMMUTABLE, absent from the
/// generated `IMMUTABLE_BUILTINS`); this list is a reason refinement, not a
/// guard -- it reports the accurate [`REASON_SIDE_EFFECT`] rather than the
/// generic default-deny reason. Side-effecting is orthogonal to
/// non-determinism: `setval('s', 42)` returns 42 deterministically yet mutates
/// the sequence, and `pg_sleep(1)` always returns void yet blocks. Reachable
/// only via `--shallow-cache-allow-all-functions` or the runtime allowlist;
/// there is no dedicated side-effect opt-in. Not exhaustive; a missing entry
/// only yields the generic reason, never a caching hole.
const PG_SIDE_EFFECTING_FUNCTIONS: &[&str] = &[
    // Sequences.
    "nextval",
    "setval",
    // Delay / blocking.
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    // Configuration and server control.
    "set_config",
    "pg_reload_conf",
    "pg_switch_wal",
    "pg_create_restore_point",
    "pg_promote",
    "pg_terminate_backend",
    "pg_cancel_backend",
    // Notification and advisory locks.
    "pg_notify",
    "pg_advisory_lock",
    "pg_advisory_unlock",
    "pg_advisory_unlock_all",
    "pg_advisory_xact_lock",
    "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock",
    // Large objects.
    "lo_import",
    "lo_export",
    "lo_create",
    "lo_creat",
    "lo_unlink",
    "lo_put",
    "lo_get",
    // Filesystem reads.
    "pg_read_file",
    "pg_read_binary_file",
    // Random-seed mutation.
    "setseed",
];

/// Bare identifiers (no parentheses) that some dialects parse as session
/// references rather than function calls: session identity and current
/// database/schema. Gated by `--shallow-cache-allow-session-specific`. Matched
/// only when the identifier appears alone (`Expr::Identifier`), to avoid
/// colliding with columns of the same name in a compound reference.
const SESSION_SPECIFIC_BARE_IDENTIFIERS: &[&str] = &[
    "current_user",
    "current_role",
    "session_user",
    "system_user",
    "current_database",
    "current_schema",
    "current_catalog",
];

/// Bare identifiers parsed as clock references rather than function calls.
/// Gated by `--shallow-cache-allow-nondeterministic`. Matched only when the
/// identifier appears alone, as with [`SESSION_SPECIFIC_BARE_IDENTIFIERS`].
const NON_DETERMINISTIC_BARE_IDENTIFIERS: &[&str] = &[
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
];

/// Per-category opt-ins that let the in-request-path auto-cache pass treat an
/// otherwise-ineligible query as cacheable. Each flag, when `true`, stops the
/// pass from skipping the corresponding class of query. All off by default.
///
/// `allow_all_functions` is the broad escape hatch: it makes any function call
/// eligible (non-deterministic, side-effecting, session-specific, or
/// user-defined), while still leaving the non-function guards (system schema,
/// session variables, unseeded `TABLESAMPLE`) in force. `allow_nondeterministic`
/// and `allow_session_specific` are narrower: each opens a curated list (clock
/// and random builtins, and session-dependent builtins, respectively). Anything
/// else non-IMMUTABLE -- notably side-effecting builtins like `nextval` and
/// `pg_sleep`, where caching drops the effect rather than merely returning
/// stale data -- stays denied and is reachable only through `allow_all_functions`
/// or the runtime allowlist. Side-effecting functions are therefore not
/// enumerated on PostgreSQL (they are implicitly denied by not being IMMUTABLE);
/// MySQL, lacking a volatility signal, must list them in order to deny them.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ShallowCacheEligibility {
    pub allow_nondeterministic: bool,
    pub allow_udf: bool,
    pub allow_all_functions: bool,
    pub allow_system_schema: bool,
    pub allow_session_specific: bool,
}

/// One class of reason a shallow query was declined, with the specific offending
/// function names grouped under it. `functions` lists the builtins or UDFs that
/// triggered this reason (e.g. `now`, `rand` for a non-deterministic skip), in
/// first-encountered order and deduplicated; it is empty for reasons that are
/// not function calls (a system-schema reference, a user variable, an unseeded
/// TABLESAMPLE). `reason` is the low-cardinality category used as the skip-metric
/// label; `functions` is for logs and explainability, never a metric label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipReason {
    pub reason: &'static str,
    pub functions: Vec<String>,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.functions.is_empty() {
            write!(f, "{}", self.reason)
        } else {
            write!(f, "{}: {}", self.reason, self.functions.join(", "))
        }
    }
}

/// Operator-managed set of function names that are eligible for shallow-cache
/// auto-creation even when they appear on a built-in deny-list. This is the
/// fine-grained counterpart to the blanket [`ShallowCacheEligibility`] flags:
/// with the flags off (the deny-lists active), allowlisting `now` makes
/// `now()` eligible while every other non-deterministic function stays blocked.
///
/// The handle is cheap to clone (an `Arc`) and shared by every connection on
/// the adapter, so a runtime `ALTER READYSET ... SHALLOW CACHE ALLOWED FUNCTION`
/// is visible to all of them at once. Names are stored lowercased; lookups
/// lowercase the candidate.
///
/// The set is per-adapter, not deployment-global. An adapter seeds it from the
/// authority at startup, and a runtime `ALTER` updates both this in-memory set
/// and the authority; other adapters in the same deployment pick the change up
/// only when they next re-seed at startup, not immediately. A deployment that
/// needs every adapter to honor an `ALTER` at once must restart the others.
///
/// Only unqualified function names are matched. A schema-qualified call such as
/// `myschema.now()` is treated as user-defined and stays ineligible even when
/// `now` is allowlisted, because the allowlist keys on the bare name.
#[derive(Debug, Clone, Default)]
pub struct ShallowCacheAllowlist(Arc<AllowlistInner>);

#[derive(Debug, Default)]
struct AllowlistInner {
    names: parking_lot::RwLock<HashSet<String>>,
    /// Serializes runtime `ALTER READYSET ... SHALLOW CACHE ALLOWED FUNCTION`
    /// updates. The adapter holds it across the authority write and the
    /// in-memory mutation so a concurrent add and drop cannot interleave and
    /// leave the persisted set and the shared in-memory set disagreeing.
    update: tokio::sync::Mutex<()>,
}

impl ShallowCacheAllowlist {
    /// Build an allowlist seeded with `names` (lowercased on insert).
    pub fn new(names: impl IntoIterator<Item = String>) -> Self {
        Self(Arc::new(AllowlistInner {
            names: parking_lot::RwLock::new(
                names.into_iter().map(|n| n.to_ascii_lowercase()).collect(),
            ),
            update: tokio::sync::Mutex::new(()),
        }))
    }

    fn read(&self) -> parking_lot::RwLockReadGuard<'_, HashSet<String>> {
        self.0.names.read()
    }

    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, HashSet<String>> {
        self.0.names.write()
    }

    /// Serialize a runtime allowlist update against concurrent ones. Hold the
    /// returned guard across the authority write and the in-memory mutation so
    /// the persisted set and the shared set cannot diverge.
    pub async fn lock_for_update(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.update.lock().await
    }

    /// Whether `name` is allowlisted (case-insensitive).
    pub fn contains(&self, name: &str) -> bool {
        self.read().contains(&name.to_ascii_lowercase())
    }

    /// Add a function name, returning `true` if it was newly inserted.
    pub fn insert(&self, name: &str) -> bool {
        self.write().insert(name.to_ascii_lowercase())
    }

    /// Remove a function name, returning `true` if it was present.
    pub fn remove(&self, name: &str) -> bool {
        self.write().remove(&name.to_ascii_lowercase())
    }

    /// The current set of allowlisted names, sorted, for display.
    pub fn snapshot(&self) -> Vec<String> {
        let mut names: Vec<String> = self.read().iter().cloned().collect();
        names.sort_unstable();
        names
    }
}

/// The three dedicated shallow-cache auto-creation allowlists, one per
/// [`ShallowCacheAllowlistKind`]: `functions` bypasses the builtin function
/// deny-lists, `variables` bypasses the session/user-variable guard, and
/// `schemas` bypasses the system-schema guard. Each is an independent
/// [`ShallowCacheAllowlist`] handle (a cheap `Arc` clone) shared across every
/// connection on the adapter, so a runtime `ALTER READYSET ... SHALLOW CACHE
/// ALLOWED ...` is visible to all of them at once.
#[derive(Debug, Clone, Default)]
pub struct ShallowCacheAllowlists {
    pub functions: ShallowCacheAllowlist,
    pub variables: ShallowCacheAllowlist,
    pub schemas: ShallowCacheAllowlist,
}

impl ShallowCacheAllowlists {
    /// The allowlist for `kind`.
    pub fn for_kind(&self, kind: ShallowCacheAllowlistKind) -> &ShallowCacheAllowlist {
        match kind {
            ShallowCacheAllowlistKind::Function => &self.functions,
            ShallowCacheAllowlistKind::Variable => &self.variables,
            ShallowCacheAllowlistKind::Schema => &self.schemas,
        }
    }
}

/// The bare name of a `@`/`@@`-prefixed variable reference, as matched against
/// the variables allowlist: leading `@` sigils and any `session.`/`global.`
/// scope qualifier are stripped, so `@@version_comment`,
/// `@@session.version_comment`, and a plain `version_comment` allowlist entry
/// all refer to the same variable. Matched case-insensitively by the caller.
fn variable_name(raw: &str) -> &str {
    let stripped = raw.trim_start_matches('@');
    stripped.rsplit('.').next().unwrap_or(stripped)
}

/// Case-insensitive membership test against an allowlist set. The sets are
/// small and already read-locked for the walk, so this scans without
/// allocating a lowercased copy of the candidate.
fn contains_ci(set: &HashSet<String>, name: &str) -> bool {
    set.iter().any(|allowed| name.eq_ignore_ascii_case(allowed))
}

/// Return every reason this shallow query should be excluded from
/// in-request-path auto-creation, grouped by reason class in first-encountered
/// order, with the offending function names collected under each. An empty
/// vector means the query is eligible. Collecting all reasons (rather than
/// stopping at the first) lets the caller bump a per-reason metric and log which
/// functions were at fault. `eligibility` carries the per-category opt-ins that
/// relax the check.
pub fn auto_cache_skip_reasons(
    query: &ShallowCacheQuery,
    dialect: Dialect,
    eligibility: &ShallowCacheEligibility,
    allowlists: &ShallowCacheAllowlists,
) -> Vec<SkipReason> {
    // Read-lock each allowlist once for the whole walk and compare names without
    // allocating. This runs once per previously-unseen query (the verdict is
    // memoized in the skip set), so it is not a per-row path, but there is still
    // no reason to lock and allocate a lowercased copy on every node.
    let functions = allowlists.functions.read();
    let variables = allowlists.variables.read();
    let schemas = allowlists.schemas.read();
    let mut visitor = AutoCacheVisitor {
        dialect,
        allow_nondeterministic: eligibility.allow_nondeterministic,
        allow_udf: eligibility.allow_udf,
        allow_all_functions: eligibility.allow_all_functions,
        allow_system_schema: eligibility.allow_system_schema,
        allow_session_specific: eligibility.allow_session_specific,
        fn_allowlist: &functions,
        var_allowlist: &variables,
        schema_allowlist: &schemas,
        system_schemas: match dialect {
            Dialect::MySQL => MYSQL_SYSTEM_SCHEMAS,
            Dialect::PostgreSQL => PG_SYSTEM_SCHEMAS,
        },
        reasons: Vec::new(),
    };
    let _ = query.visit(&mut visitor);
    visitor.reasons
}

/// The first reason [`auto_cache_skip_reasons`] reports, or `None` when the
/// query is eligible. Convenience for callers that only need to know whether
/// (and roughly why) a query is skipped.
pub fn auto_cache_skip_reason(
    query: &ShallowCacheQuery,
    dialect: Dialect,
    eligibility: &ShallowCacheEligibility,
    allowlists: &ShallowCacheAllowlists,
) -> Option<&'static str> {
    auto_cache_skip_reasons(query, dialect, eligibility, allowlists)
        .into_iter()
        .next()
        .map(|r| r.reason)
}

struct AutoCacheVisitor<'a> {
    dialect: Dialect,
    allow_nondeterministic: bool,
    allow_udf: bool,
    allow_all_functions: bool,
    allow_system_schema: bool,
    allow_session_specific: bool,
    fn_allowlist: &'a HashSet<String>,
    var_allowlist: &'a HashSet<String>,
    schema_allowlist: &'a HashSet<String>,
    system_schemas: &'static [&'static str],
    reasons: Vec<SkipReason>,
}

impl Visitor for AutoCacheVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        // A CTE body can be a data-modifying statement (`WITH x AS (INSERT/UPDATE/DELETE
        // ... RETURNING) SELECT ... FROM x`, the shape PostgREST emits for every mutation).
        // Top-level write rejection does not catch these, and caching one would re-run the
        // write on every hit. Unconditional: no opt-in flag relaxes it, because caching
        // drops the mutation rather than merely returning stale rows.
        if matches!(
            &*query.body,
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
        ) {
            self.add_reason(REASON_WRITE_IN_CTE, None);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        if self.allow_system_schema {
            return ControlFlow::Continue(());
        }
        for part in &name.0 {
            if let Some(ident) = part.as_ident()
                && is_system_schema(&ident.value, self.dialect, self.system_schemas)
                && !self.schema_allowlisted(&ident.value)
            {
                self.add_reason(REASON_SYSTEM_SCHEMA, None);
                break;
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        // A TABLESAMPLE draws a fresh random sample on every execution unless a
        // REPEATABLE seed pins it, so the unseeded form is non-deterministic.
        if !self.allow_nondeterministic
            && let TableFactor::Table {
                sample: Some(kind), ..
            } = table_factor
        {
            let sample = match kind {
                TableSampleKind::BeforeTableAlias(s) | TableSampleKind::AfterTableAlias(s) => s,
            };
            if sample.seed.is_none() {
                self.add_reason(REASON_UNSEEDED_TABLESAMPLE, None);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let (reason, function): (&'static str, Option<&str>) = match expr {
            Expr::Identifier(id)
                if !self.allow_session_specific
                    && id.value.starts_with('@')
                    && !self.variable_allowlisted(&id.value) =>
            {
                (REASON_USER_VARIABLE, None)
            }
            Expr::Identifier(id)
                if !self.allow_session_specific
                    && !self.allowlisted(&id.value)
                    && SESSION_SPECIFIC_BARE_IDENTIFIERS
                        .iter()
                        .any(|s| id.value.eq_ignore_ascii_case(s)) =>
            {
                (REASON_SESSION_SPECIFIC, Some(id.value.as_str()))
            }
            Expr::Identifier(id)
                if !self.allow_nondeterministic
                    && !self.allowlisted(&id.value)
                    && NON_DETERMINISTIC_BARE_IDENTIFIERS
                        .iter()
                        .any(|s| id.value.eq_ignore_ascii_case(s)) =>
            {
                (REASON_NON_DETERMINISTIC, Some(id.value.as_str()))
            }
            Expr::CompoundIdentifier(parts)
                if !self.allow_session_specific
                    && parts.first().is_some_and(|p| p.value.starts_with('@'))
                    && !parts
                        .last()
                        .is_some_and(|p| self.variable_allowlisted(&p.value)) =>
            {
                (REASON_USER_VARIABLE, None)
            }
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(s),
                ..
            }) if !self.allow_session_specific
                && s.starts_with('@')
                && !self.variable_allowlisted(s) =>
            {
                (REASON_USER_VARIABLE, None)
            }
            Expr::Function(func) => match self.function_reason(func) {
                Some(reason) => (
                    reason,
                    func.name
                        .0
                        .last()
                        .and_then(|p| p.as_ident())
                        .map(|i| i.value.as_str()),
                ),
                None => return ControlFlow::Continue(()),
            },
            _ => return ControlFlow::Continue(()),
        };
        self.add_reason(reason, function);
        ControlFlow::Continue(())
    }
}

impl AutoCacheVisitor<'_> {
    /// Record a skip `reason`, optionally attributing the offending `function`
    /// name to it. Reasons keep first-encountered order; the functions under a
    /// reason are deduplicated and also kept in first-encountered order.
    fn add_reason(&mut self, reason: &'static str, function: Option<&str>) {
        if let Some(entry) = self.reasons.iter_mut().find(|r| r.reason == reason) {
            if let Some(f) = function
                && !entry.functions.iter().any(|e| e == f)
            {
                entry.functions.push(f.to_string());
            }
        } else {
            self.reasons.push(SkipReason {
                reason,
                functions: function.map(str::to_string).into_iter().collect(),
            });
        }
    }

    /// Whether `name` is on the operator function allowlist (case-insensitive).
    fn allowlisted(&self, name: &str) -> bool {
        contains_ci(self.fn_allowlist, name)
    }

    /// Whether the variable referenced by `raw` (a `@`/`@@`-prefixed token) is
    /// on the operator variable allowlist. Matches on the bare variable name
    /// (see [`variable_name`]), so an allowlist entry `version_comment` covers
    /// `@@version_comment` and `@@session.version_comment` alike.
    fn variable_allowlisted(&self, raw: &str) -> bool {
        contains_ci(self.var_allowlist, variable_name(raw))
    }

    /// Whether the system schema `name` is on the operator schema allowlist.
    /// Allowlisting `pg_catalog` additionally covers any `pg_`-prefixed catalog
    /// reference, which PostgreSQL treats as a system reference wholesale.
    fn schema_allowlisted(&self, name: &str) -> bool {
        contains_ci(self.schema_allowlist, name)
            || (matches!(self.dialect, Dialect::PostgreSQL)
                && name
                    .as_bytes()
                    .get(..3)
                    .is_some_and(|p| p.eq_ignore_ascii_case(b"pg_"))
                && contains_ci(self.schema_allowlist, "pg_catalog"))
    }

    /// Why a function call makes the query ineligible, or `None` if it is a
    /// deterministic builtin that is fine to auto-cache.
    fn function_reason(&self, func: &Function) -> Option<&'static str> {
        // The broad escape hatch: allow every function call regardless of its
        // category (non-deterministic, side-effecting, or user-defined).
        if self.allow_all_functions {
            return None;
        }
        // A call qualified by a non-system, non-public schema is user-defined
        // regardless of its bare name (it may shadow a builtin).
        if self.is_user_schema_qualified(&func.name) {
            return (!self.allow_udf).then_some(REASON_UDF);
        }
        // A name that does not resolve to a plain identifier cannot be matched
        // against the builtin list; treat it as user-defined (fail-safe).
        let Some(last) = func.name.0.last().and_then(|p| p.as_ident()) else {
            return (!self.allow_udf).then_some(REASON_UDF);
        };
        let name = last.value.as_str();
        // An operator-allowlisted function is eligible regardless of the
        // deny-lists below -- this is what makes "block all X except now()" work.
        if self.allowlisted(name) {
            return None;
        }
        let on = |list: &[&str]| list.iter().any(|s| name.eq_ignore_ascii_case(s));

        match self.dialect {
            // PostgreSQL: default-deny. Only IMMUTABLE builtins and SQL keyword
            // functions are cacheable outright; `pg_proc.provolatile` gives the
            // IMMUTABLE signal. The side-effecting, session, and non-deterministic
            // lists are curated (volatility cannot express those axes), but they
            // only refine the skip reason: a builtin absent from all of them is
            // still denied by the is_builtin fallthrough, so a forgotten entry
            // never opens a caching hole. Every non-immutable call is reachable
            // only via allow_all_functions (checked above) or the allowlist.
            Dialect::PostgreSQL => {
                if is_immutable_builtin(name, self.dialect) || is_keyword_function(name) {
                    None
                } else if on(PG_SIDE_EFFECTING_FUNCTIONS) {
                    Some(REASON_SIDE_EFFECT)
                } else if on(PG_SESSION_SPECIFIC_FUNCTIONS) {
                    (!self.allow_session_specific).then_some(REASON_SESSION_SPECIFIC)
                } else if on(PG_NONDETERMINISTIC_FUNCTIONS) || is_nondeterministic_special(name) {
                    (!self.allow_nondeterministic).then_some(REASON_NON_DETERMINISTIC)
                } else if is_builtin(name, self.dialect) {
                    Some(REASON_BUILTIN_NOT_ALLOWED)
                } else {
                    (!self.allow_udf).then_some(REASON_UDF)
                }
            }
            // MySQL: default-deny, exactly like PostgreSQL. MySQL has no
            // volatility signal, so its IMMUTABLE set is the hand-curated
            // MYSQL_IMMUTABLE_FUNCTIONS rather than a catalog lookup. Only those
            // and SQL keyword functions are cacheable outright. As on PostgreSQL,
            // the side-effecting/session/non-deterministic lists only refine the
            // skip reason; anything not proven immutable is denied by the
            // is_builtin fallthrough regardless, reachable only via
            // allow_all_functions (checked above) or the allowlist.
            Dialect::MySQL => {
                if is_immutable_builtin(name, self.dialect) || is_keyword_function(name) {
                    None
                } else if on(MYSQL_SIDE_EFFECTING_FUNCTIONS) {
                    Some(REASON_SIDE_EFFECT)
                } else if on(MYSQL_SESSION_SPECIFIC_FUNCTIONS) {
                    (!self.allow_session_specific).then_some(REASON_SESSION_SPECIFIC)
                } else if on(MYSQL_NONDETERMINISTIC_FUNCTIONS) {
                    (!self.allow_nondeterministic).then_some(REASON_NON_DETERMINISTIC)
                } else if is_builtin(name, self.dialect) {
                    Some(REASON_BUILTIN_NOT_ALLOWED)
                } else {
                    (!self.allow_udf).then_some(REASON_UDF)
                }
            }
        }
    }

    /// Whether the function name carries a schema qualifier that is neither a
    /// system schema nor `public` -- i.e. a user schema, implying a UDF.
    fn is_user_schema_qualified(&self, name: &ObjectName) -> bool {
        let parts = &name.0;
        if parts.len() < 2 {
            return false;
        }
        let Some(qualifier) = parts[parts.len() - 2].as_ident() else {
            return false;
        };
        let q = qualifier.value.as_str();
        !is_system_schema(q, self.dialect, self.system_schemas) && !q.eq_ignore_ascii_case("public")
    }
}

/// SQL grammar-construct functions that every engine treats as builtin but that
/// the generated allowlists can miss: they are spelled like keywords and are not
/// always catalog rows under their bare name (e.g. PostgreSQL has no `coalesce`
/// in `pg_proc`).  sqlparser surfaces some of these as a plain `Expr::Function`,
/// so they must be recognized here to avoid mistaking them for UDFs.  Dialect-
/// agnostic: all are standard SQL.
const SQL_KEYWORD_FUNCTIONS: &[&str] = &[
    "cast",
    "coalesce",
    "convert",
    "extract",
    "greatest",
    "least",
    "nullif",
    "overlay",
    "position",
    "substr",
    "substring",
    "trim",
];

/// Whether `name` is one of the standard-SQL keyword functions -- grammar
/// constructs sqlparser surfaces as `Expr::Function` but that are not catalog
/// functions. They are deterministic and always safe to cache.
fn is_keyword_function(name: &str) -> bool {
    SQL_KEYWORD_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str())
}

/// Whether `name` is a known builtin function of `dialect` (case-insensitively).
/// The generated allowlist slices are interned into a set on first use.
fn is_builtin(name: &str, dialect: Dialect) -> bool {
    if is_keyword_function(name) {
        return true;
    }
    let lower = name.to_ascii_lowercase();
    static MYSQL: OnceLock<HashSet<&'static str>> = OnceLock::new();
    static PG: OnceLock<HashSet<&'static str>> = OnceLock::new();
    let builtins = match dialect {
        Dialect::MySQL => MYSQL.get_or_init(|| mysql_builtins::BUILTINS.iter().copied().collect()),
        Dialect::PostgreSQL => PG.get_or_init(|| pg_builtins::BUILTINS.iter().copied().collect()),
    };
    builtins.contains(lower.as_str())
}

/// Whether `name` is a builtin whose result depends only on its arguments --
/// the definitive safe-to-cache set. PostgreSQL derives this from
/// `pg_proc.provolatile = 'i'` (generated into `pg_builtins`); MySQL has no
/// volatility signal, so it is the hand-curated [`MYSQL_IMMUTABLE_FUNCTIONS`].
/// Matched case-insensitively.
fn is_immutable_builtin(name: &str, dialect: Dialect) -> bool {
    let lower = name.to_ascii_lowercase();
    match dialect {
        Dialect::PostgreSQL => {
            static PG_IMMUTABLE: OnceLock<HashSet<&'static str>> = OnceLock::new();
            PG_IMMUTABLE
                .get_or_init(|| pg_builtins::IMMUTABLE_BUILTINS.iter().copied().collect())
                .contains(lower.as_str())
        }
        Dialect::MySQL => {
            static MYSQL_IMMUTABLE: OnceLock<HashSet<&'static str>> = OnceLock::new();
            MYSQL_IMMUTABLE
                .get_or_init(|| MYSQL_IMMUTABLE_FUNCTIONS.iter().copied().collect())
                .contains(lower.as_str())
        }
    }
}

/// SQL special functions such as `CURRENT_TIMESTAMP` that are non-deterministic
/// but are spelled as keywords rather than `pg_catalog` entries, so they are
/// absent from the generated builtin list. Shared with the bare-identifier
/// check in [`AutoCacheVisitor::pre_visit_expr`]. Matched case-insensitively.
fn is_nondeterministic_special(name: &str) -> bool {
    NON_DETERMINISTIC_BARE_IDENTIFIERS
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s))
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

    fn run(
        dialect: Dialect,
        q: &str,
        allow_nd: bool,
        allow_udf: bool,
        allow_system_schema: bool,
        allow_session_specific: bool,
        allow_all_functions: bool,
    ) -> Option<&'static str> {
        run_allowlisting(
            dialect,
            q,
            allow_nd,
            allow_udf,
            allow_system_schema,
            allow_session_specific,
            allow_all_functions,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_allowlisting(
        dialect: Dialect,
        q: &str,
        allow_nd: bool,
        allow_udf: bool,
        allow_system_schema: bool,
        allow_session_specific: bool,
        allow_all_functions: bool,
        allowlist: &[&str],
    ) -> Option<&'static str> {
        let (parsed, _hint) = parse_shallow_query(dialect, q).expect("shallow parse");
        let eligibility = ShallowCacheEligibility {
            allow_nondeterministic: allow_nd,
            allow_udf,
            allow_all_functions,
            allow_system_schema,
            allow_session_specific,
        };
        let allowlists = ShallowCacheAllowlists {
            functions: ShallowCacheAllowlist::new(allowlist.iter().map(|s| s.to_string())),
            ..Default::default()
        };
        auto_cache_skip_reason(&parsed, dialect, &eligibility, &allowlists)
    }

    /// Skip reason with only the given function names allowlisted (all opt-in
    /// flags off).
    fn skip_reason_allowlisting(
        dialect: Dialect,
        q: &str,
        allowlist: &[&str],
    ) -> Option<&'static str> {
        run_allowlisting(dialect, q, false, false, false, false, false, allowlist)
    }

    fn skip_reason(dialect: Dialect, q: &str) -> Option<&'static str> {
        run(dialect, q, false, false, false, false, false)
    }

    /// Skip reason with the given per-kind allowlists populated and every opt-in
    /// flag off, exercising the variables and schemas allowlists.
    fn skip_reason_with_allowlists(
        dialect: Dialect,
        q: &str,
        functions: &[&str],
        variables: &[&str],
        schemas: &[&str],
    ) -> Option<&'static str> {
        let (parsed, _hint) = parse_shallow_query(dialect, q).expect("shallow parse");
        let allowlists = ShallowCacheAllowlists {
            functions: ShallowCacheAllowlist::new(functions.iter().map(|s| s.to_string())),
            variables: ShallowCacheAllowlist::new(variables.iter().map(|s| s.to_string())),
            schemas: ShallowCacheAllowlist::new(schemas.iter().map(|s| s.to_string())),
        };
        auto_cache_skip_reason(
            &parsed,
            dialect,
            &ShallowCacheEligibility::default(),
            &allowlists,
        )
    }

    fn skip_reason_with(
        dialect: Dialect,
        q: &str,
        allow_nondeterministic: bool,
    ) -> Option<&'static str> {
        run(
            dialect,
            q,
            allow_nondeterministic,
            false,
            false,
            false,
            false,
        )
    }

    fn skip_reason_allow_udf(dialect: Dialect, q: &str) -> Option<&'static str> {
        run(dialect, q, false, true, false, false, false)
    }

    fn skip_reason_allow_system_schema(dialect: Dialect, q: &str) -> Option<&'static str> {
        run(dialect, q, false, false, true, false, false)
    }

    fn skip_reason_allow_session_specific(dialect: Dialect, q: &str) -> Option<&'static str> {
        run(dialect, q, false, false, false, true, false)
    }

    fn skip_reason_allow_all_functions(dialect: Dialect, q: &str) -> Option<&'static str> {
        run(dialect, q, false, false, false, false, true)
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
    fn write_bearing_cte_is_blocked() {
        // PostgREST emits every mutation as a data-modifying CTE wrapped in a SELECT.
        for q in [
            "WITH t AS (INSERT INTO users (name) VALUES ('a') RETURNING id) SELECT id FROM t",
            "WITH t AS (UPDATE users SET name = 'a' WHERE id = 1 RETURNING id) SELECT id FROM t",
            "WITH t AS (DELETE FROM users WHERE id = 1 RETURNING id) SELECT id FROM t",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_WRITE_IN_CTE);
        }
    }

    #[test]
    fn write_bearing_cte_is_not_relaxed_by_allow_all_functions() {
        // Caching a write drops the mutation, so no opt-in flag makes it eligible.
        let q = "WITH t AS (INSERT INTO users (name) VALUES ('a') RETURNING id) SELECT id FROM t";
        assert_eq!(
            skip_reason_allow_all_functions(Dialect::PostgreSQL, q),
            Some(REASON_WRITE_IN_CTE),
        );
    }

    #[test]
    fn read_only_cte_is_eligible() {
        assert_eligible(
            Dialect::PostgreSQL,
            "WITH t AS (SELECT id FROM users WHERE id = 1) SELECT id FROM t",
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
            "SELECT RAND()",
            "SELECT uuid()",
            "SELECT VERSION()",
            "SELECT CURDATE()",
            "SELECT CURTIME()",
            "SELECT UTC_TIMESTAMP()",
            "SELECT IS_FREE_LOCK('a')",
            "SELECT RANDOM_BYTES(16)",
            "SELECT ICU_VERSION()",
            "SELECT GROUP_REPLICATION_GET_WRITE_CONCURRENCY()",
        ] {
            assert_rejected(Dialect::MySQL, q, REASON_NON_DETERMINISTIC);
        }
        for q in [
            "SELECT random()",
            "SELECT random_normal()",
            "SELECT gen_random_uuid()",
            "SELECT clock_timestamp()",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_NON_DETERMINISTIC);
        }
        // Recognized builtins absent from the curated non-deterministic list are
        // still denied, but by the generic builtin default-deny -- the pass does
        // not claim they are non-deterministic, only that they were not allowed.
        for q in [
            "SELECT txid_status(1)",
            "SELECT lastval()",
            "SELECT currval('s')",
            "SELECT pg_current_wal_lsn()",
            "SELECT pg_database_size(1)",
            "SELECT pg_relation_size(1)",
            "SELECT pg_is_in_recovery()",
            "SELECT pg_current_xact_id()",
            "SELECT pg_listening_channels()",
            "SELECT pg_my_temp_schema()",
            // Read-only replication-origin reads: non-IMMUTABLE, so denied.
            "SELECT pg_replication_origin_progress('o', true)",
            "SELECT pg_replication_origin_session_is_setup()",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_BUILTIN_NOT_ALLOWED);
        }
        // pgcrypto lives outside pg_catalog, so under default-deny its functions
        // are user-defined (still blocked, just via the UDF path).
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT gen_random_bytes(8)",
            REASON_UDF,
        );
    }

    #[test]
    fn overload_collapse_functions_are_not_immutable() {
        // Regression for the IMMUTABLE overload-collapse bug: these pg_catalog
        // functions have a mix of immutable and non-immutable overloads (e.g.
        // date_trunc is immutable for (text, timestamp) but stable for
        // (text, timestamptz)), so the name must NOT count as immutable. They
        // are still recognized builtins, denied by the builtin default-deny
        // rather than wrongly cached.
        for q in [
            "SELECT date_trunc('day', c) FROM t",
            "SELECT age(c) FROM t",
            "SELECT to_timestamp(0)",
            "SELECT to_tsvector('x')",
            "SELECT ts_rewrite(c, 'a', 'b') FROM t",
            "SELECT generate_series(1, 10)",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_BUILTIN_NOT_ALLOWED);
        }
    }

    #[test]
    fn side_effecting_calls_are_blocked() {
        // Side-effecting builtins are denied outright and NOT reachable via
        // allow-nondeterministic (only allow-all-functions or the runtime
        // allowlist); caching one would drop its effect. They are non-immutable,
        // so default-deny blocks them regardless; the per-dialect side-effecting
        // lists refine the skip reason to the accurate REASON_SIDE_EFFECT (side
        // effects are orthogonal to non-determinism -- e.g. setval is
        // deterministic yet mutating).
        for q in [
            "SELECT SLEEP(0)",
            "SELECT LOAD_FILE('/etc/passwd')",
            "SELECT GET_LOCK('a', 0)",
            "SELECT RELEASE_LOCK('a')",
            "SELECT MASTER_POS_WAIT('f', 0)",
            "SELECT SOURCE_POS_WAIT('f', 0)",
            "SELECT WAIT_FOR_EXECUTED_GTID_SET('a:1')",
            "SELECT BENCHMARK(1000, MD5('x'))",
            "SELECT GROUP_REPLICATION_SET_AS_PRIMARY('uuid')",
            "SELECT ASYNCHRONOUS_CONNECTION_FAILOVER_RESET()",
            "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('a:1')",
        ] {
            assert_rejected(Dialect::MySQL, q, REASON_SIDE_EFFECT);
        }
        for q in [
            "SELECT nextval('s')",
            "SELECT setval('s', 1)",
            "SELECT set_config('role', 'admin', true)",
            "SELECT pg_notify('chan', 'msg')",
            "SELECT pg_advisory_lock(1)",
            "SELECT pg_try_advisory_xact_lock(1)",
            "SELECT lo_import('/etc/passwd')",
            "SELECT pg_sleep(0)",
            "SELECT setseed(0.5)",
            "SELECT pg_terminate_backend(1)",
            "SELECT pg_reload_conf()",
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT lo_get(1)",
            "SELECT pg_switch_wal()",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_SIDE_EFFECT);
        }
        // On both dialects, side-effecting calls stay denied even with
        // allow-nondeterministic; only allow-all-functions reaches them.
        assert!(skip_reason_with(Dialect::PostgreSQL, "SELECT nextval('s')", true).is_some());
        assert!(skip_reason_with(Dialect::MySQL, "SELECT GET_LOCK('a', 0)", true).is_some());
        assert!(
            skip_reason_allow_all_functions(Dialect::PostgreSQL, "SELECT nextval('s')").is_none()
        );
        assert!(
            skip_reason_allow_all_functions(Dialect::MySQL, "SELECT GET_LOCK('a', 0)").is_none()
        );
    }

    #[test]
    fn unseeded_tablesample_is_blocked() {
        // TABLESAMPLE draws a fresh random sample on each execution, so the
        // unseeded form is non-deterministic and must not be auto-cached.
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT a FROM t TABLESAMPLE BERNOULLI (50)",
            REASON_UNSEEDED_TABLESAMPLE,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT a FROM t TABLESAMPLE SYSTEM (10)",
            REASON_UNSEEDED_TABLESAMPLE,
        );
    }

    #[test]
    fn seeded_tablesample_is_eligible() {
        // A REPEATABLE seed pins the sample, so it is reproducible across
        // executions and may be auto-cached.
        assert_eligible(
            Dialect::PostgreSQL,
            "SELECT a FROM t TABLESAMPLE BERNOULLI (50) REPEATABLE (42)",
        );
        assert_eligible(
            Dialect::PostgreSQL,
            "SELECT a FROM t TABLESAMPLE SYSTEM (10) REPEATABLE (7)",
        );
    }

    #[test]
    fn mysql_nondeterministic_list_has_no_duplicates() {
        // Matching is case-insensitive, so uniqueness is over lowercased names.
        let lowered: Vec<String> = MYSQL_NONDETERMINISTIC_FUNCTIONS
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .collect();
        let set: HashSet<&String> = lowered.iter().collect();
        assert_eq!(
            set.len(),
            lowered.len(),
            "duplicate entry in MYSQL_NONDETERMINISTIC_FUNCTIONS"
        );
    }

    #[test]
    fn deny_list_entries_are_known_builtins() {
        // Every denied function should be a real builtin, so the deny-list check
        // wins over the allowlist for it. Anything not in a generated allowlist
        // must be a SQL-keyword function; otherwise it is a typo that would
        // silently match nothing and fail to deny. PostgreSQL keeps no curated
        // denylist (default-deny on IMMUTABLE), so only MySQL's list applies.
        let mut known: HashSet<&str> = pg_builtins::BUILTINS.iter().copied().collect();
        known.extend(mysql_builtins::BUILTINS.iter().copied());
        known.extend(SQL_KEYWORD_FUNCTIONS.iter().copied());
        for &f in MYSQL_NONDETERMINISTIC_FUNCTIONS {
            assert!(
                known.contains(f),
                "deny-list entry {f:?} is not a known builtin or keyword (typo?)"
            );
        }
    }

    #[test]
    fn bare_session_keywords_are_blocked() {
        // Bare identity keywords route to session-specific; bare clock keywords
        // to non-deterministic.
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_user",
            REASON_SESSION_SPECIFIC,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT session_user",
            REASON_SESSION_SPECIFIC,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_timestamp",
            REASON_NON_DETERMINISTIC,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_date",
            REASON_NON_DETERMINISTIC,
        );
    }

    #[test]
    fn collects_all_reasons() {
        // A query can be ineligible for several reasons at once; the pass reports
        // all of them (deduplicated) so the caller can profile per reason.
        let (parsed, _) =
            parse_shallow_query(Dialect::MySQL, "SELECT RAND() FROM mysql.user").expect("parse");
        let reasons = auto_cache_skip_reasons(
            &parsed,
            Dialect::MySQL,
            &ShallowCacheEligibility::default(),
            &ShallowCacheAllowlists::default(),
        );
        assert!(
            reasons.iter().any(|r| r.reason == REASON_NON_DETERMINISTIC)
                && reasons.iter().any(|r| r.reason == REASON_SYSTEM_SCHEMA),
            "expected both non-deterministic and system-schema reasons, got {reasons:?}"
        );
    }

    #[test]
    fn allowlist_permits_only_listed_nondeterministic_functions() {
        // The canonical use: "block all non-deterministic functions except now()".
        for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
            // now() is denied by default ...
            assert_eq!(
                skip_reason(dialect, "SELECT now()"),
                Some(REASON_NON_DETERMINISTIC)
            );
            // ... and eligible once allowlisted.
            assert_eq!(
                skip_reason_allowlisting(dialect, "SELECT now()", &["now"]),
                None
            );
        }
        // Every other non-deterministic function stays blocked: allowlisting `now`
        // does not open the whole category.
        assert_eq!(
            skip_reason_allowlisting(Dialect::MySQL, "SELECT now(), rand()", &["now"]),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert_eq!(
            skip_reason_allowlisting(Dialect::PostgreSQL, "SELECT now(), random()", &["now"]),
            Some(REASON_NON_DETERMINISTIC)
        );
    }

    #[test]
    fn allowlist_is_case_insensitive() {
        assert_eq!(
            skip_reason_allowlisting(Dialect::PostgreSQL, "SELECT NOW()", &["NoW"]),
            None
        );
    }

    #[test]
    fn allowlist_permits_bare_identifier_specials() {
        // CURRENT_TIMESTAMP arrives as a bare identifier, not a function call;
        // allowlisting its name still makes it eligible.
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT current_timestamp"),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert_eq!(
            skip_reason_allowlisting(
                Dialect::PostgreSQL,
                "SELECT current_timestamp",
                &["current_timestamp"]
            ),
            None
        );
    }

    #[test]
    fn allowlist_permits_session_specific_bare_identifiers() {
        // `current_user` arrives as a bare identifier on the session-specific
        // guard (not a function call); allowlisting its name makes it eligible,
        // mirroring the non-deterministic bare-identifier case.
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT current_user"),
            Some(REASON_SESSION_SPECIFIC)
        );
        assert_eq!(
            skip_reason_allowlisting(
                Dialect::PostgreSQL,
                "SELECT current_user",
                &["current_user"]
            ),
            None
        );
    }

    #[test]
    fn allowlist_overrides_side_effecting_and_udf() {
        // A side-effecting builtin and a UDF are both denied by default, and
        // both become eligible when explicitly allowlisted.
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT nextval('s')"),
            Some(REASON_SIDE_EFFECT)
        );
        assert_eq!(
            skip_reason_allowlisting(Dialect::PostgreSQL, "SELECT nextval('s')", &["nextval"]),
            None
        );
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT my_udf(a) FROM t"),
            Some(REASON_UDF)
        );
        assert_eq!(
            skip_reason_allowlisting(Dialect::PostgreSQL, "SELECT my_udf(a) FROM t", &["my_udf"]),
            None
        );
    }

    #[test]
    fn allowlist_does_not_override_user_schema_qualified_calls() {
        // A user-schema-qualified call is a UDF regardless of its bare name, so
        // allowlisting the bare name must not make `app.now()` eligible.
        assert_eq!(
            skip_reason_allowlisting(Dialect::PostgreSQL, "SELECT app.now() FROM t", &["now"]),
            Some(REASON_UDF)
        );
    }

    #[test]
    fn variables_allowlist_overrides_user_variable() {
        // A `@@`-system-variable read is denied by default (session-specific).
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT @@version_comment"),
            Some(REASON_USER_VARIABLE)
        );
        // Allowlisting the bare variable name makes that one variable eligible,
        // by name or with the `@@` sigil, while others stay blocked.
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT @@version_comment",
                &[],
                &["version_comment"],
                &[]
            ),
            None
        );
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT @@sql_mode",
                &[],
                &["version_comment"],
                &[]
            ),
            Some(REASON_USER_VARIABLE)
        );
        // The variables allowlist is keyed on the bare name, so a `session.`
        // scope qualifier is transparent.
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT @@session.version_comment",
                &[],
                &["version_comment"],
                &[]
            ),
            None
        );
        // The functions allowlist must not stand in for the variables one.
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT @@version_comment",
                &["version_comment"],
                &[],
                &[]
            ),
            Some(REASON_USER_VARIABLE)
        );
    }

    #[test]
    fn schemas_allowlist_overrides_system_schema() {
        // A system-schema reference is denied by default.
        assert_eq!(
            skip_reason(Dialect::MySQL, "SELECT * FROM information_schema.tables"),
            Some(REASON_SYSTEM_SCHEMA)
        );
        // Allowlisting that schema makes it eligible; other system schemas stay
        // blocked.
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables",
                &[],
                &[],
                &["information_schema"]
            ),
            None
        );
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::MySQL,
                "SELECT * FROM mysql.user",
                &[],
                &[],
                &["information_schema"]
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
    }

    #[test]
    fn schemas_allowlist_pg_catalog_covers_pg_prefix() {
        // PostgreSQL treats any `pg_`-prefixed reference as a system reference;
        // allowlisting `pg_catalog` covers the whole family, including an
        // unqualified catalog table.
        assert_eq!(
            skip_reason(Dialect::PostgreSQL, "SELECT * FROM pg_class"),
            Some(REASON_SYSTEM_SCHEMA)
        );
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::PostgreSQL,
                "SELECT * FROM pg_catalog.pg_class",
                &[],
                &[],
                &["pg_catalog"]
            ),
            None
        );
        assert_eq!(
            skip_reason_with_allowlists(
                Dialect::PostgreSQL,
                "SELECT * FROM pg_class",
                &[],
                &[],
                &["pg_catalog"]
            ),
            None
        );
    }

    #[test]
    fn allowlist_mutation_is_observable() {
        let allowlist = ShallowCacheAllowlist::default();
        assert!(!allowlist.contains("now"));
        assert!(allowlist.insert("NOW"));
        assert!(allowlist.contains("now"));
        assert!(!allowlist.insert("now")); // idempotent (case-insensitive)
        assert_eq!(allowlist.snapshot(), vec!["now".to_string()]);
        assert!(allowlist.remove("now"));
        assert!(!allowlist.contains("now"));
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
        // The visitor must descend into GROUP BY: RAND() there contributes the
        // non-deterministic reason (alongside the aggregate's own default-deny).
        assert!(
            skip_reasons(Dialect::MySQL, "SELECT count(*) FROM users GROUP BY RAND()")
                .contains(&REASON_NON_DETERMINISTIC)
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
        // The visitor must descend into the window ORDER BY: RAND() there
        // contributes the non-deterministic reason (alongside the window
        // function's own default-deny).
        assert!(
            skip_reasons(
                Dialect::MySQL,
                "SELECT ROW_NUMBER() OVER (ORDER BY RAND()) FROM users"
            )
            .contains(&REASON_NON_DETERMINISTIC)
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
            assert_rejected(Dialect::PostgreSQL, q, REASON_SIDE_EFFECT);
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

    #[test]
    fn allow_nondeterministic_permits_functions_only() {
        // The opt-in opens the curated clock/random list (and the bare clock
        // keywords) only.
        assert!(skip_reason_with(Dialect::MySQL, "SELECT NOW()", true).is_none());
        assert!(skip_reason_with(Dialect::PostgreSQL, "SELECT random()", true).is_none());
        assert!(skip_reason_with(Dialect::PostgreSQL, "SELECT clock_timestamp()", true).is_none());
        assert!(
            skip_reason_with(Dialect::MySQL, "SELECT id FROM users ORDER BY RAND()", true)
                .is_none()
        );
        assert!(skip_reason_with(Dialect::PostgreSQL, "SELECT current_date", true).is_none());
        // It does NOT open side-effecting builtins (only allow-all-functions
        // does), session-dependent functions/identifiers, system schemas, or
        // session variables.
        assert_eq!(
            skip_reason_with(Dialect::PostgreSQL, "SELECT nextval('s')", true),
            Some(REASON_SIDE_EFFECT)
        );
        assert_eq!(
            skip_reason_with(Dialect::PostgreSQL, "SELECT current_user", true),
            Some(REASON_SESSION_SPECIFIC)
        );
        assert_eq!(
            skip_reason_with(Dialect::MySQL, "SELECT DATABASE()", true),
            Some(REASON_SESSION_SPECIFIC)
        );
        assert!(
            skip_reason_with(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables",
                true
            )
            .is_some()
        );
        assert!(skip_reason_with(Dialect::MySQL, "SELECT @@version", true).is_some());
    }

    #[test]
    fn user_defined_functions_are_blocked() {
        // A function not in the engine's builtin allowlist is a UDF.
        assert_rejected(Dialect::MySQL, "SELECT my_udf(id) FROM t", REASON_UDF);
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT compute_score(id) FROM t",
            REASON_UDF,
        );
    }

    #[test]
    fn allow_udf_permits_user_defined_functions_only() {
        // With the opt-in, UDFs (including schema-qualified ones) are eligible.
        assert!(skip_reason_allow_udf(Dialect::MySQL, "SELECT my_udf(id) FROM t").is_none());
        assert!(
            skip_reason_allow_udf(Dialect::PostgreSQL, "SELECT app.score(id) FROM t").is_none()
        );
        // Non-deterministic functions and system schemas still cause a skip.
        assert_eq!(
            skip_reason_allow_udf(Dialect::MySQL, "SELECT RAND()"),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert!(
            skip_reason_allow_udf(Dialect::PostgreSQL, "SELECT * FROM pg_catalog.pg_class")
                .is_some()
        );
    }

    #[test]
    fn allow_system_schema_permits_system_schemas_only() {
        // With the opt-in, system-schema references are eligible.
        assert!(
            skip_reason_allow_system_schema(
                Dialect::PostgreSQL,
                "SELECT * FROM pg_catalog.pg_class"
            )
            .is_none()
        );
        assert!(
            skip_reason_allow_system_schema(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables"
            )
            .is_none()
        );
        // Unqualified `pg_`-prefixed catalog tables and the other MySQL system
        // schemas take different branches of `is_system_schema`; all eligible.
        assert!(
            skip_reason_allow_system_schema(Dialect::PostgreSQL, "SELECT * FROM pg_class")
                .is_none()
        );
        assert!(
            skip_reason_allow_system_schema(Dialect::MySQL, "SELECT * FROM mysql.user").is_none()
        );
        // Session/user variables, non-deterministic functions, and UDFs still
        // cause a skip.
        assert_eq!(
            skip_reason_allow_system_schema(Dialect::MySQL, "SELECT @@version"),
            Some(REASON_USER_VARIABLE)
        );
        assert_eq!(
            skip_reason_allow_system_schema(Dialect::MySQL, "SELECT @my_var"),
            Some(REASON_USER_VARIABLE)
        );
        assert_eq!(
            skip_reason_allow_system_schema(Dialect::MySQL, "SELECT RAND()"),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert_eq!(
            skip_reason_allow_system_schema(Dialect::MySQL, "SELECT my_udf(id) FROM t"),
            Some(REASON_UDF)
        );
    }

    #[test]
    fn session_specific_functions_are_blocked() {
        // Session-dependent functions are denied by default and routed to
        // allow-session-specific, not the broader allow-nondeterministic.
        for q in ["SELECT current_user()", "SELECT DATABASE()"] {
            assert_rejected(Dialect::MySQL, q, REASON_SESSION_SPECIFIC);
        }
        for q in [
            "SELECT current_setting('search_path')",
            "SELECT current_database()",
        ] {
            assert_rejected(Dialect::PostgreSQL, q, REASON_SESSION_SPECIFIC);
        }
        // Bare-identifier session specials take the same category.
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT current_user",
            REASON_SESSION_SPECIFIC,
        );
        assert_rejected(
            Dialect::PostgreSQL,
            "SELECT session_user",
            REASON_SESSION_SPECIFIC,
        );
    }

    #[test]
    fn connection_and_statement_state_functions_deny_by_default() {
        // last_insert_id / connection_id / found_rows / row_count (MySQL) and
        // pg_backend_pid / inet_client_* (PG) report values meaningful only to the
        // calling connection, so a cached value is always some other connection's.
        // They are hardcoded in no relaxable list: denied by the builtin
        // default-deny, which no per-category flag opens.
        for q in [
            "SELECT last_insert_id()",
            "SELECT connection_id()",
            "SELECT found_rows()",
            "SELECT row_count()",
        ] {
            // Denied by default...
            assert_eq!(
                skip_reason(Dialect::MySQL, q),
                Some(REASON_BUILTIN_NOT_ALLOWED),
                "{q}"
            );
            // ...not relaxed by allow-session-specific (they left that class)...
            assert_eq!(
                run(Dialect::MySQL, q, false, false, false, true, false),
                Some(REASON_BUILTIN_NOT_ALLOWED),
                "{q} under allow-session-specific"
            );
            // ...nor by allow-nondeterministic (the builtin fallthrough is ungated)...
            assert_eq!(
                run(Dialect::MySQL, q, true, false, false, false, false),
                Some(REASON_BUILTIN_NOT_ALLOWED),
                "{q} under allow-nondeterministic"
            );
            // ...but allow-all-functions DOES cache them: not being hardcoded in
            // any deny list, the broad escape hatch reaches them.
            assert_eq!(
                run(Dialect::MySQL, q, false, false, false, false, true),
                None,
                "{q} under allow-all-functions"
            );
        }
        // The runtime function allowlist reaches them too.
        assert_eq!(
            skip_reason_allowlisting(
                Dialect::MySQL,
                "SELECT last_insert_id()",
                &["last_insert_id"]
            ),
            None
        );
        // Same for the PostgreSQL connection-identity reads.
        for q in [
            "SELECT pg_backend_pid()",
            "SELECT inet_client_addr()",
            "SELECT inet_client_port()",
        ] {
            assert_eq!(
                skip_reason(Dialect::PostgreSQL, q),
                Some(REASON_BUILTIN_NOT_ALLOWED),
                "{q}"
            );
            assert_eq!(
                run(Dialect::PostgreSQL, q, false, false, false, true, false),
                Some(REASON_BUILTIN_NOT_ALLOWED),
                "{q} under allow-session-specific"
            );
            assert_eq!(
                run(Dialect::PostgreSQL, q, false, false, false, false, true),
                None,
                "{q} under allow-all-functions"
            );
        }
    }

    #[test]
    fn allow_session_specific_permits_session_refs() {
        // With the opt-in, session variables, session identifiers, and
        // session-dependent functions are all eligible. Cover the three @-arms
        // (bare `@`/`@@`, compound `@@scope.name`, `@`-placeholder), a bare
        // identifier special, and function-form session reads on both dialects.
        assert!(skip_reason_allow_session_specific(Dialect::MySQL, "SELECT @@version").is_none());
        assert!(skip_reason_allow_session_specific(Dialect::MySQL, "SELECT @my_var").is_none());
        assert!(
            skip_reason_allow_session_specific(Dialect::MySQL, "SELECT @@global.max_connections")
                .is_none()
        );
        assert!(
            skip_reason_allow_session_specific(
                Dialect::MySQL,
                "SELECT id FROM users WHERE id = @target_id"
            )
            .is_none()
        );
        assert!(skip_reason_allow_session_specific(Dialect::MySQL, "SELECT DATABASE()").is_none());
        assert!(
            skip_reason_allow_session_specific(Dialect::PostgreSQL, "SELECT current_user")
                .is_none()
        );
        assert!(
            skip_reason_allow_session_specific(
                Dialect::PostgreSQL,
                "SELECT current_setting('search_path')"
            )
            .is_none()
        );
        // System schemas, non-deterministic and side-effecting functions, and
        // UDFs still cause a skip.
        assert_eq!(
            skip_reason_allow_session_specific(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
        assert_eq!(
            skip_reason_allow_session_specific(Dialect::MySQL, "SELECT RAND()"),
            Some(REASON_NON_DETERMINISTIC)
        );
        assert_eq!(
            skip_reason_allow_session_specific(Dialect::PostgreSQL, "SELECT nextval('s')"),
            Some(REASON_SIDE_EFFECT)
        );
        assert_eq!(
            skip_reason_allow_session_specific(Dialect::MySQL, "SELECT my_udf(id) FROM t"),
            Some(REASON_UDF)
        );
    }

    #[test]
    fn allow_all_functions_permits_every_function_call() {
        // The escape hatch makes any function eligible regardless of category:
        // side-effecting/blocking, non-deterministic, and user-defined.
        assert!(skip_reason_allow_all_functions(Dialect::MySQL, "SELECT SLEEP(0)").is_none());
        assert!(
            skip_reason_allow_all_functions(Dialect::PostgreSQL, "SELECT nextval('s')").is_none()
        );
        assert!(
            skip_reason_allow_all_functions(Dialect::PostgreSQL, "SELECT pg_advisory_lock(1)")
                .is_none()
        );
        assert!(skip_reason_allow_all_functions(Dialect::MySQL, "SELECT NOW()").is_none());
        assert!(
            skip_reason_allow_all_functions(Dialect::MySQL, "SELECT my_udf(id) FROM t").is_none()
        );
        // The non-function guards still apply: system schemas and session
        // variables are not functions, so allow-all-functions does not cover them.
        assert_eq!(
            skip_reason_allow_all_functions(
                Dialect::MySQL,
                "SELECT * FROM information_schema.tables"
            ),
            Some(REASON_SYSTEM_SCHEMA)
        );
        assert_eq!(
            skip_reason_allow_all_functions(Dialect::MySQL, "SELECT @@version"),
            Some(REASON_USER_VARIABLE)
        );
    }

    #[test]
    fn unimplemented_builtins_are_eligible() {
        // Builtins Readyset does not itself implement must still be recognized as
        // builtins (the whole reason for a catalog/doc-derived allowlist).
        assert_eligible(Dialect::MySQL, "SELECT abs(x), md5(y), sqrt(z) FROM t");
        assert_eligible(Dialect::PostgreSQL, "SELECT abs(x), md5(y), sqrt(z) FROM t");
    }

    #[test]
    fn mysql_immutable_builtins_are_eligible() {
        // The hand-curated MySQL immutable allow-list spans math, string,
        // hashing, deterministic date arithmetic, and JSON.
        for q in [
            "SELECT abs(a), round(b, 2), sqrt(c) FROM t",
            "SELECT concat(a, b), length(s), lower(s), substring_index(s, ',', 1) FROM t",
            "SELECT md5(a), sha2(b, 256), crc32(c) FROM t",
            "SELECT datediff(a, b), to_days(c), dayofweek(d) FROM t",
            "SELECT json_extract(doc, '$.k'), json_length(doc) FROM t",
            "SELECT inet_aton(ip), hex(x) FROM t",
        ] {
            assert_eligible(Dialect::MySQL, q);
        }
    }

    #[test]
    fn mysql_non_immutable_builtins_are_denied() {
        // Under default-deny, a recognized MySQL builtin absent from the
        // immutable allow-list is denied even when it is on no curated denylist.
        // Each of these varies with the session locale, time zone, or a session
        // variable -- a denylist would have silently cached them, serving stale
        // or cross-session results.
        for q in [
            "SELECT from_unixtime(t) FROM foo", // session time zone
            "SELECT convert_tz(t, '+00:00', '+09:00') FROM foo", // time-zone tables
            "SELECT date_format(d, '%W') FROM foo", // lc_time_names
            "SELECT week(d) FROM foo",          // default_week_format
        ] {
            assert_rejected(Dialect::MySQL, q, REASON_BUILTIN_NOT_ALLOWED);
        }
    }

    #[test]
    fn schema_qualified_user_function_is_blocked() {
        // A call qualified by a user schema is a UDF even when its bare name
        // matches a builtin (it shadows one).
        assert_rejected(Dialect::PostgreSQL, "SELECT app.now() FROM t", REASON_UDF);
        assert_rejected(Dialect::MySQL, "SELECT mydb.my_fn(x) FROM t", REASON_UDF);
        // A builtin qualified by a system schema is still recognized.
        assert_eligible(Dialect::PostgreSQL, "SELECT pg_catalog.abs(x) FROM t");
    }

    #[test]
    fn sql_keyword_constructs_are_eligible() {
        // Grammar constructs spelled like calls must not be mistaken for UDFs.
        for q in [
            "SELECT coalesce(a, b) FROM t",
            "SELECT nullif(a, b) FROM t",
            "SELECT greatest(a, b) FROM t",
            "SELECT least(a, b) FROM t",
            "SELECT cast(a AS int) FROM t",
        ] {
            assert_eligible(Dialect::PostgreSQL, q);
            assert_eligible(Dialect::MySQL, q);
        }
    }

    /// The full deduplicated label list feeding the per-reason skip metric: the
    /// adapter increments `SHALLOW_AUTO_CREATE_SKIPPED{reason=...}` once per
    /// element of this vector, so its exact contents are what the metric records.
    fn skip_reasons(dialect: Dialect, q: &str) -> Vec<&'static str> {
        let (parsed, _hint) = parse_shallow_query(dialect, q).expect("shallow parse");
        auto_cache_skip_reasons(
            &parsed,
            dialect,
            &ShallowCacheEligibility::default(),
            &ShallowCacheAllowlists::default(),
        )
        .into_iter()
        .map(|r| r.reason)
        .collect()
    }

    #[test]
    fn skip_reasons_report_each_distinct_cause_as_its_own_metric_label() {
        // A query that trips several guards surfaces each distinct reason once,
        // since every element becomes one metric increment. Order is irrelevant
        // to the metric, so compare as a set.
        let reasons = skip_reasons(
            Dialect::PostgreSQL,
            "SELECT now(), current_user FROM pg_catalog.pg_class",
        );
        let labels: std::collections::BTreeSet<_> = reasons.iter().copied().collect();
        assert_eq!(
            labels,
            std::collections::BTreeSet::from([
                REASON_NON_DETERMINISTIC,
                REASON_SESSION_SPECIFIC,
                REASON_SYSTEM_SCHEMA,
            ]),
            "expected one label per distinct cause, got {reasons:?}"
        );
        assert_eq!(
            reasons.len(),
            labels.len(),
            "reasons must be deduplicated for the metric: {reasons:?}"
        );
    }

    #[test]
    fn skip_reasons_collapse_a_repeated_cause_to_one_label() {
        // Two non-deterministic calls are one metric label, not two.
        assert_eq!(
            skip_reasons(Dialect::MySQL, "SELECT RAND(), NOW()"),
            vec![REASON_NON_DETERMINISTIC]
        );
    }

    #[test]
    fn skip_reasons_are_empty_for_an_eligible_query() {
        assert!(skip_reasons(Dialect::MySQL, "SELECT a FROM foo").is_empty());
        assert!(skip_reasons(Dialect::PostgreSQL, "SELECT a FROM foo").is_empty());
    }

    #[test]
    fn skip_reason_groups_offending_function_names_under_one_cause() {
        // Several functions tripping the same guard collapse to a single reason
        // whose `functions` lists every offender, so operators see exactly which
        // calls made the query ineligible.
        let (parsed, _) =
            parse_shallow_query(Dialect::MySQL, "SELECT RAND(), NOW()").expect("shallow parse");
        let reasons = auto_cache_skip_reasons(
            &parsed,
            Dialect::MySQL,
            &ShallowCacheEligibility::default(),
            &ShallowCacheAllowlists::default(),
        );
        assert_eq!(reasons.len(), 1, "one cause, got {reasons:?}");
        assert_eq!(reasons[0].reason, REASON_NON_DETERMINISTIC);
        let names: std::collections::BTreeSet<_> = reasons[0]
            .functions
            .iter()
            .map(|f| f.to_ascii_lowercase())
            .collect();
        assert_eq!(
            names,
            std::collections::BTreeSet::from(["rand".to_string(), "now".to_string()]),
            "both offenders grouped under the one reason, got {reasons:?}"
        );
    }

    #[test]
    fn skip_reason_display_lists_the_functions_after_the_cause() {
        let grouped = SkipReason {
            reason: REASON_NON_DETERMINISTIC,
            functions: vec!["rand".to_string(), "now".to_string()],
        };
        assert_eq!(
            grouped.to_string(),
            format!("{REASON_NON_DETERMINISTIC}: rand, now")
        );
        let bare = SkipReason {
            reason: REASON_SYSTEM_SCHEMA,
            functions: vec![],
        };
        assert_eq!(bare.to_string(), REASON_SYSTEM_SCHEMA);
    }
}
