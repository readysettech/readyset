use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lazy_static::lazy_static;
use nom_sql::{
    Literal, PostgresParameterValue, PostgresParameterValueInner, SetNames, SetPostgresParameter,
    SetPostgresParameterValue, SetStatement, SqlQuery,
};
use noria::results::Results;
use noria::ReadySetResult;
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;

enum AllowedParameterValue {
    Literal(PostgresParameterValue),
    OneOf(HashSet<PostgresParameterValue>),
    Predicate(fn(&PostgresParameterValue) -> bool),
}

impl AllowedParameterValue {
    fn literal<L>(lit: L) -> Self
    where
        Literal: From<L>,
    {
        Self::Literal(PostgresParameterValue::literal(lit))
    }

    fn one_of<I, T>(allowed: I) -> Self
    where
        I: IntoIterator<Item = T>,
        PostgresParameterValue: From<T>,
    {
        Self::OneOf(allowed.into_iter().map(|v| v.into()).collect())
    }

    fn set_value_is_allowed(&self, value: &SetPostgresParameterValue) -> bool {
        match value {
            SetPostgresParameterValue::Default => true,
            SetPostgresParameterValue::Value(val) => match self {
                AllowedParameterValue::Literal(l) => val == l,
                AllowedParameterValue::OneOf(m) => m.contains(val),
                AllowedParameterValue::Predicate(p) => p(val),
            },
        }
    }
}

fn search_path_includes_public(val: &PostgresParameterValue) -> bool {
    match val {
        PostgresParameterValue::Single(PostgresParameterValueInner::Literal(Literal::String(
            s,
        ))) => s == "public",
        PostgresParameterValue::List(vals) => vals.first().iter().all(|v| {
            matches!(
                v,
                PostgresParameterValueInner::Literal(Literal::String(s)) if s == "public"
            )
        }),
        _ => false,
    }
}

lazy_static! {
    /// The set of parameters that we can safely proxy upstream with *any* value, as we've
    /// determined that they don't change the semantics of queries in a way that would matter for us
    static ref ALLOWED_PARAMETERS_ANY_VALUE: HashSet<&'static str> =
        HashSet::from([
            "autovacuum",
            "autovacuum_analyze_scale_factor",
            "autovacuum_analyze_threshold",
            "autovacuum_freeze_max_age",
            "autovacuum_max_workers",
            "autovacuum_multixact_freeze_max_age",
            "autovacuum_naptime",
            "autovacuum_vacuum_cost_delay",
            "autovacuum_vacuum_cost_limit",
            "autovacuum_vacuum_scale_factor",
            "autovacuum_vacuum_threshold",
            "default_text_search_config",
            "server_encoding",
            "timezone_abbreviations",
            "dynamic_library_path",
            "gin_fuzzy_search_limit",
            "tcp_keepalives_count",
            "tcp_keepalives_idle",
            "tcp_keepalives_interval",
            "local_preload_libraries",
            "session_preload_libraries",
            "shared_preload_libraries",
            "check_function_bodies",
            "default_tablespace",
            "default_transaction_deferrable",
            "default_transaction_isolation",
            "default_transaction_read_only",
            "gin_pending_list_limit",
            "idle_in_transaction_session_timeout",
            "lock_timeout",
            "session_replication_role",
            "statement_timeout",
            "temp_tablespaces",
            "transaction_deferrable",
            "transaction_isolation",
            "transaction_read_only",
            "vacuum_freeze_min_age",
            "vacuum_freeze_table_age",
            "vacuum_multixact_freeze_min_age",
            "vacuum_multixact_freeze_table_age",
            "xmlbinary",
            "xmloption",
            "bonjour",
            "bonjour_name",
            "listen_addresses",
            "max_connections",
            "port",
            "superuser_reserved_connections",
            "unix_socket_directories",
            "unix_socket_group",
            "unix_socket_permissions",
            "authentication_timeout",
            "db_user_namespace",
            "krb_caseins_users",
            "krb_server_keyfile",
            "password_encryption",
            "row_security",
            "ssl",
            "ssl_ca_file",
            "ssl_cert_file",
            "ssl_ciphers",
            "ssl_crl_file",
            "ssl_dh_params_file",
            "ssl_ecdh_curve",
            "ssl_key_file",
            "ssl_prefer_server_ciphers",
            "allow_system_table_mods",
            "ignore_checksum_failure",
            "ignore_system_indexes",
            "post_auth_delay",
            "pre_auth_delay",
            "trace_notify",
            "trace_recovery_messages",
            "trace_sort",
            "wal_consistency_checking",
            "zero_damaged_pages",
            "exit_on_error",
            "restart_after_crash",
            "config_file",
            "data_directory",
            "external_pid_file",
            "hba_file",
            "ident_file",
            "deadlock_timeout",
            "max_locks_per_transaction",
            "max_pred_locks_per_page",
            "max_pred_locks_per_relation",
            "max_pred_locks_per_transaction",
            "block_size",
            "data_checksums",
            "debug_assertions",
            "integer_datetimes",
            "max_function_args",
            "max_identifier_length",
            "max_index_keys",
            "segment_size",
            "server_version",
            "server_version_num",
            "wal_block_size",
            "wal_segment_size",
            "cluster_name",
            "update_process_title",
            "geqo",
            "geqo_effort",
            "geqo_generations",
            "geqo_pool_size",
            "geqo_seed",
            "geqo_selection_bias",
            "geqo_threshold",
            "constraint_exclusion",
            "cursor_tuple_fraction",
            "default_statistics_target",
            "force_parallel_mode",
            "from_collapse_limit",
            "join_collapse_limit",
            "cpu_index_tuple_cost",
            "cpu_operator_cost",
            "cpu_tuple_cost",
            "effective_cache_size",
            "min_parallel_index_scan_size",
            "min_parallel_table_scan_size",
            "parallel_setup_cost",
            "parallel_tuple_cost",
            "random_page_cost",
            "seq_page_cost",
            "enable_bitmapscan",
            "enable_gathermerge",
            "enable_hashagg",
            "enable_hashjoin",
            "enable_indexonlyscan",
            "enable_indexscan",
            "enable_material",
            "enable_mergejoin",
            "enable_nestloop",
            "enable_seqscan",
            "enable_sort",
            "enable_tidscan",
            "track_commit_timestamp",
            "synchronous_standby_names",
            "vacuum_defer_cleanup_age",
            "max_replication_slots",
            "max_wal_senders",
            "wal_keep_segments",
            "wal_sender_timeout",
            "hot_standby",
            "hot_standby_feedback",
            "max_standby_archive_delay",
            "max_standby_streaming_delay",
            "wal_receiver_status_interval",
            "wal_receiver_timeout",
            "wal_retrieve_retry_interval",
            "max_logical_replication_workers",
            "max_sync_workers_per_subscription",
            "application_name",
            "debug_pretty_print",
            "debug_print_parse",
            "debug_print_plan",
            "debug_print_rewritten",
            "log_autovacuum_min_duration",
            "log_checkpoints",
            "log_connections",
            "log_disconnections",
            "log_duration",
            "log_error_verbosity",
            "log_hostname",
            "log_line_prefix",
            "log_lock_waits",
            "log_replication_commands",
            "log_statement",
            "log_temp_files",
            "log_timezone",
            "client_min_messages",
            "log_min_duration_statement",
            "log_min_error_statement",
            "log_min_messages",
            "event_source",
            "log_destination",
            "log_directory",
            "log_file_mode",
            "log_filename",
            "log_rotation_age",
            "log_rotation_size",
            "log_truncate_on_rotation",
            "logging_collector",
            "syslog_facility",
            "syslog_ident",
            "syslog_sequence_numbers",
            "syslog_split_messages",
            "backend_flush_after",
            "effective_io_concurrency",
            "max_parallel_workers",
            "max_parallel_workers_per_gather",
            "max_worker_processes",
            "old_snapshot_threshold",
            "bgwriter_delay",
            "bgwriter_flush_after",
            "bgwriter_lru_maxpages",
            "bgwriter_lru_multiplier",
            "vacuum_cost_delay",
            "vacuum_cost_limit",
            "vacuum_cost_page_dirty",
            "vacuum_cost_page_hit",
            "vacuum_cost_page_miss",
            "temp_file_limit",
            "max_files_per_process",
            "autovacuum_work_mem",
            "dynamic_shared_memory_type",
            "huge_pages",
            "maintenance_work_mem",
            "max_prepared_transactions",
            "max_stack_depth",
            "replacement_sort_tuples",
            "shared_buffers",
            "temp_buffers",
            "track_activity_query_size",
            "work_mem",
            "log_executor_stats",
            "log_parser_stats",
            "log_planner_stats",
            "log_statement_stats",
            "stats_temp_directory",
            "track_activities",
            "track_counts",
            "track_functions",
            "track_io_timing",
            "default_with_oids",
            "escape_string_warning",
            "lo_compat_privileges",
            "operator_precedence_warning",
            "quote_all_identifiers",
            "synchronize_seqscans",
            "archive_command",
            "archive_mode",
            "archive_timeout",
            "checkpoint_completion_target",
            "checkpoint_flush_after",
            "checkpoint_timeout",
            "checkpoint_warning",
            "max_wal_size",
            "min_wal_size",
            "commit_delay",
            "commit_siblings",
            "fsync",
            "full_page_writes",
            "synchronous_commit",
            "wal_buffers",
            "wal_compression",
            "wal_level",
            "wal_log_hints",
            "wal_sync_method",
            "wal_writer_delay",
            "wal_writer_flush_after",

            // This parameter *would* matter, if we supported intervals - once we do we should move
            // this to WITH_VALUE and specify the value for the interval style we actually use
            "intervalstyle",

            // Similarly this parameter *would* matter if we supported arrays, and once we do we
            // should move this to WITH_VALUE
            "array_nulls",
        ]);

    static ref ALLOWED_PARAMETERS_WITH_VALUE: HashMap<&'static str, AllowedParameterValue> =
        HashMap::from([
            ("client_encoding", AllowedParameterValue::one_of([
                PostgresParameterValue::literal("utf-8"),
                PostgresParameterValue::literal("UTF8"),
            ])),
            ("timezone", AllowedParameterValue::literal("UTC")),
            ("datestyle", AllowedParameterValue::literal("ISO")),
            ("extra_float_digits", AllowedParameterValue::literal(1)),
            ("TimeZone",  AllowedParameterValue::literal("Etc/UTC")),
            ("bytea_output",  AllowedParameterValue::literal("hex")),
            ("search_path", AllowedParameterValue::Predicate(search_path_includes_public)),
            ("transform_null_equals", AllowedParameterValue::literal(false)),
            ("backslash_quote", AllowedParameterValue::one_of([
                PostgresParameterValue::identifier("safe_encoding"),
                PostgresParameterValue::literal("safe_encoding"),
            ])),
            ("standard_conforming_strings", AllowedParameterValue::one_of([
                PostgresParameterValue::literal(true),
                PostgresParameterValue::identifier("on"),
            ])),
        ]);
}

/// PostgreSQL flavor of [`QueryHandler`].
pub struct PostgreSqlQueryHandler;

impl QueryHandler for PostgreSqlQueryHandler {
    fn requires_fallback(_: &SqlQuery) -> bool {
        false
    }

    fn default_response(_: &SqlQuery) -> ReadySetResult<QueryResult<'static>> {
        Ok(noria_connector::QueryResult::Select {
            data: vec![Results::new(vec![vec![]], Arc::new([]))],
            select_schema: SelectSchema {
                use_bogo: false,
                schema: Cow::Owned(vec![]),
                columns: Cow::Owned(vec![]),
            },
        })
    }

    fn is_set_allowed(stmt: &SetStatement) -> bool {
        match stmt {
            SetStatement::PostgresParameter(SetPostgresParameter { name, .. })
                if ALLOWED_PARAMETERS_ANY_VALUE.contains(name.to_ascii_lowercase().as_str()) =>
            {
                true
            }
            SetStatement::PostgresParameter(SetPostgresParameter { name, value, .. }) => {
                if let Some(allowed_value) = ALLOWED_PARAMETERS_WITH_VALUE.get(name.as_str()) {
                    allowed_value.set_value_is_allowed(value)
                } else {
                    false
                }
            }
            SetStatement::Names(SetNames { charset, .. }) => charset.to_lowercase() == "utf8",
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Dialect};

    use super::*;

    fn parse_set_statement(statement: &str) -> SetStatement {
        match parse_query(Dialect::PostgreSQL, statement).unwrap() {
            SqlQuery::Set(stmt) => stmt,
            _ => panic!("Wrong query type"),
        }
    }

    fn is_allowed(statement: &str) {
        assert!(PostgreSqlQueryHandler::is_set_allowed(
            &parse_set_statement(statement)
        ))
    }

    fn is_forbidden(statement: &str) {
        assert!(!PostgreSqlQueryHandler::is_set_allowed(
            &parse_set_statement(statement)
        ))
    }

    #[test]
    fn search_path_with_public_is_allowed() {
        is_allowed("SET search_path = 'public', 'other'");
    }

    #[test]
    fn search_path_not_starting_with_public_isnt_allowed() {
        is_forbidden("SET search_path = 'other', 'public'");
    }

    #[test]
    fn standard_conforming_strings_on_allowed() {
        is_allowed("SET standard_conforming_strings = on");
    }

    #[test]
    fn client_encoding_utf8_allowed() {
        is_allowed("SET client_encoding = 'UTF8'");
    }
}
