use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use lazy_static::lazy_static;
use readyset_adapter::backend::noria_connector;
use readyset_adapter::backend::noria_connector::QueryResult;
use readyset_adapter::{QueryHandler, SetBehavior, parse_timezone};
use readyset_client::schema::SelectSchema;
use readyset_errors::ReadySetResult;
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{
    Literal, PostgresParameterValue, PostgresParameterValueInner, SetNames, SetPostgresParameter,
    SetPostgresParameterValue, SetStatement, SqlQuery,
};

/// Parse PG's numeric `SET TimeZone = N` shorthand. PG documents this as
/// "N hours behind UTC" — POSIX-style, sign-inverted relative to ISO 8601 —
/// so `SET TimeZone = -8` resolves to ISO offset `+08:00`.
fn parse_posix_hours_offset(n: f64) -> Option<readyset_adapter::SessionTimezone> {
    if !n.is_finite() {
        return None;
    }
    // `f64 as i32` saturates; `from_fixed_offset_secs` rejects out-of-range.
    let east_secs = (-n * 3600.0).round() as i32;
    readyset_adapter::SessionTimezone::from_fixed_offset_secs(east_secs)
}

enum AllowedParameterValue {
    Literal(PostgresParameterValue),
    OneOf(HashSet<PostgresParameterValue>),
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
            },
        }
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
                PostgresParameterValue::literal("utf8"),
                PostgresParameterValue::literal("utf-8"),
                PostgresParameterValue::literal("UTF8"),
                PostgresParameterValue::literal("unicode"),
            ])),
            ("datestyle", AllowedParameterValue::one_of([
                PostgresParameterValue::literal("ISO"),
                PostgresParameterValue::identifier("iso"),
            ])),
            // extra_float_digits accepts a range of -15..3, and as of
            // pg 12 any value greater than zero is treated the same.
            // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT,
            // "shortest-precise format".
            ("extra_float_digits", AllowedParameterValue::one_of([
                PostgresParameterValue::literal(1),
                PostgresParameterValue::literal(2),
                PostgresParameterValue::literal(3),
            ])),
            ("bytea_output",  AllowedParameterValue::literal("hex")),
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

    fn return_default_response(_: &SqlQuery) -> bool {
        false
    }

    fn default_response(_: &SqlQuery) -> ReadySetResult<QueryResult<'static>> {
        Ok(noria_connector::QueryResult::empty(SelectSchema {
            schema: Cow::Owned(vec![]),
            columns: Cow::Owned(vec![]),
        }))
    }

    fn handle_set_statement(stmt: &SetStatement) -> SetBehavior {
        let behavior = SetBehavior::default();
        match stmt {
            SetStatement::PostgresParameter(SetPostgresParameter { name, value, .. }) => {
                // PG preserves case in double-quoted identifiers, so match
                // against the lowercased name to converge `SET "TimeZone"`
                // with `SET timezone`.
                let name_lower = name.to_ascii_lowercase();
                if ALLOWED_PARAMETERS_ANY_VALUE.contains(name_lower.as_str()) {
                    return behavior.unsupported(false);
                }
                match name_lower.as_str() {
                    "autocommit" => {
                        let enabled = match value {
                            SetPostgresParameterValue::Default => true,
                            SetPostgresParameterValue::Value(val) => ![
                                PostgresParameterValue::literal(0),
                                PostgresParameterValue::literal(false),
                                PostgresParameterValue::identifier("off"),
                            ]
                            .contains(val),
                        };
                        behavior.set_autocommit(enabled)
                    }
                    // Non-UTC zones are flagged unsupported (cache results are
                    // UTC-wallclock); the parsed value is still recorded so
                    // future eval-side support can read it unchanged.
                    "timezone" => {
                        let parsed = match value {
                            // `TO DEFAULT` resolves to the upstream config
                            // default, which we can't inspect at SET time and
                            // is frequently non-UTC.
                            SetPostgresParameterValue::Default => None,
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                PostgresParameterValueInner::Literal(Literal::String(s)),
                            )) => parse_timezone(s),
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                PostgresParameterValueInner::Identifier(id),
                            )) => parse_timezone(id.as_str()),
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                PostgresParameterValueInner::Literal(Literal::Integer(n)),
                            )) => parse_posix_hours_offset(*n as f64),
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                PostgresParameterValueInner::Literal(Literal::UnsignedInteger(n)),
                            )) => parse_posix_hours_offset(*n as f64),
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                PostgresParameterValueInner::Literal(Literal::Number(s)),
                            )) => s.parse::<f64>().ok().and_then(parse_posix_hours_offset),
                            _ => None,
                        };
                        match parsed {
                            Some(tz) if tz.is_utc() => behavior.set_timezone(tz),
                            Some(tz) => behavior.set_timezone(tz).unsupported(true),
                            None => behavior.unsupported(true),
                        }
                    }
                    "search_path" => {
                        let value_to_string = |value: &PostgresParameterValueInner| match value {
                            PostgresParameterValueInner::Identifier(id) => id.clone(),
                            PostgresParameterValueInner::Literal(Literal::String(s)) => s.into(),
                            PostgresParameterValueInner::Literal(lit) => lit
                                .display(readyset_sql::Dialect::PostgreSQL)
                                .to_string()
                                .into(),
                        };

                        let search_path = match value {
                            SetPostgresParameterValue::Default => vec!["public".into()],
                            SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                                val,
                            )) => vec![value_to_string(val)],
                            SetPostgresParameterValue::Value(PostgresParameterValue::List(
                                vals,
                            )) => vals.iter().map(value_to_string).collect(),
                        };

                        behavior.set_search_path(search_path)
                    }
                    other => {
                        if let Some(allowed_value) = ALLOWED_PARAMETERS_WITH_VALUE.get(other) {
                            behavior.unsupported(!allowed_value.set_value_is_allowed(value))
                        } else {
                            behavior.unsupported(true)
                        }
                    }
                }
            }
            SetStatement::Names(SetNames { charset, .. }) => {
                let charset = charset.to_ascii_lowercase();
                behavior.unsupported(!["utf8", "utf-8"].contains(&charset.as_str()))
            }
            _ => behavior.unsupported(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_query;

    use super::*;

    fn parse_set_statement(statement: &str) -> SetStatement {
        match parse_query(Dialect::PostgreSQL, statement).unwrap() {
            SqlQuery::Set(stmt) => stmt,
            _ => panic!("Wrong query type"),
        }
    }

    fn is_proxy(statement: &str) {
        assert_eq!(
            PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(statement)),
            SetBehavior::default()
        )
    }

    #[test]
    fn standard_conforming_strings_on_allowed() {
        is_proxy("SET standard_conforming_strings = on");
    }

    #[test]
    fn client_encoding_utf8_allowed() {
        is_proxy("SET client_encoding = 'UTF8'");
    }

    #[test]
    fn set_time_zone_utc_equivalents_are_supported() {
        for stmt in [
            "SET timezone = 'UTC'",
            "SET timezone = 'Etc/UTC'",
            "SET timezone = 'utc'",
            "SET timezone = 'etc/utc'",
            "SET timezone = '+00:00'",
            "SET timezone = 'Universal'",
            "SET timezone = 'Zulu'",
            "SET timezone = 'GMT'",
            "SET timezone = 'Etc/GMT'",
            "SET TimeZone = 'Etc/UTC'",
            "SET TIMEZONE = 'UTC'",
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                !beh.unsupported,
                "{stmt:?} should be supported (UTC-equivalent), got {beh:?}"
            );
            let tz = beh
                .set_timezone
                .unwrap_or_else(|| panic!("{stmt:?} should record a parsed timezone, got {beh:?}"));
            assert!(
                tz.is_utc(),
                "{stmt:?} should record a UTC-equivalent timezone, got {tz:?}"
            );
        }
    }

    /// `TO DEFAULT` is unsupported because the upstream default is frequently
    /// non-UTC and we can't read it at SET time.
    #[test]
    fn set_time_zone_default_is_unsupported() {
        let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(
            "SET timezone TO DEFAULT",
        ));
        assert!(
            beh.unsupported,
            "SET timezone TO DEFAULT must be unsupported (upstream default may not be UTC), got {beh:?}"
        );
        assert!(
            beh.set_timezone.is_none(),
            "DEFAULT carries no parsed value, set_timezone should stay None"
        );
    }

    /// PG preserves case in double-quoted identifiers; `SET "TimeZone"` must
    /// still dispatch to the `timezone` arm.
    #[test]
    fn set_time_zone_quoted_identifier_dispatches() {
        let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(
            "SET \"TimeZone\" = 'UTC'",
        ));
        assert!(
            !beh.unsupported,
            "quoted-identifier UTC SET should be supported, got {beh:?}"
        );
        let tz = beh
            .set_timezone
            .expect("quoted-identifier SET should record the parsed timezone");
        assert!(tz.is_utc(), "expected UTC, got {tz:?}");

        let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(
            "SET \"TimeZone\" = 'America/New_York'",
        ));
        assert!(
            beh.unsupported,
            "quoted-identifier non-UTC SET should be unsupported, got {beh:?}"
        );
        assert!(
            beh.set_timezone.is_some(),
            "quoted-identifier non-UTC SET should still record the parsed timezone, got {beh:?}"
        );
    }

    #[test]
    fn set_time_zone_numeric_zero_is_utc() {
        for stmt in ["SET TimeZone = 0", "SET TimeZone TO 0"] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                !beh.unsupported,
                "{stmt:?} should be supported (UTC-equivalent), got {beh:?}"
            );
            let tz = beh
                .set_timezone
                .unwrap_or_else(|| panic!("{stmt:?} should record a parsed timezone"));
            assert!(tz.is_utc(), "{stmt:?} should record UTC, got {tz:?}");
        }
    }

    #[test]
    fn set_time_zone_numeric_non_zero_is_unsupported_but_recorded() {
        // POSIX-inverted: `SET TimeZone = -8` means UTC+8, `= 8` means UTC-8.
        for (stmt, expected_offset_secs) in [
            ("SET TimeZone = -8", 8 * 3600),
            ("SET TimeZone = 8", -8 * 3600),
            ("SET TimeZone = 5", -5 * 3600),
            ("SET TimeZone TO -3", 3 * 3600),
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                beh.unsupported,
                "{stmt:?} should be unsupported (non-UTC), got {beh:?}"
            );
            let expected = readyset_adapter::SessionTimezone::FixedOffset(
                chrono::FixedOffset::east_opt(expected_offset_secs).unwrap(),
            );
            assert_eq!(
                beh.set_timezone,
                Some(expected),
                "{stmt:?} should record FixedOffset({expected_offset_secs}s), got {beh:?}"
            );
        }
    }

    #[test]
    fn set_time_zone_numeric_out_of_range_is_unsupported() {
        for stmt in [
            "SET TimeZone = 99",
            "SET TimeZone = -99",
            "SET TimeZone = 15",
            "SET TimeZone = -15",
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(beh.unsupported, "{stmt:?} should be unsupported");
            assert!(
                beh.set_timezone.is_none(),
                "{stmt:?} parsed nothing, set_timezone should stay None, got {beh:?}"
            );
        }
    }

    /// Fractional numerics arrive as `Literal::Number` rather than `Literal::Integer`.
    #[test]
    fn set_time_zone_numeric_fractional_zero_is_utc() {
        for stmt in [
            "SET TimeZone = 0.0",
            "SET TimeZone = -0.0",
            "SET TimeZone TO 0.0",
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                !beh.unsupported,
                "{stmt:?} should be supported (UTC-equivalent), got {beh:?}"
            );
            let tz = beh
                .set_timezone
                .unwrap_or_else(|| panic!("{stmt:?} should record a parsed timezone"));
            assert!(tz.is_utc(), "{stmt:?} should record UTC, got {tz:?}");
        }
    }

    #[test]
    fn set_time_zone_numeric_fractional_non_zero_is_unsupported_but_recorded() {
        for (stmt, expected_offset_secs) in [
            ("SET TimeZone = 0.5", -1800),
            ("SET TimeZone = -0.5", 1800),
            ("SET TimeZone = -7.5", 27000),
            ("SET TimeZone TO 5.5", -19800),
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                beh.unsupported,
                "{stmt:?} should be unsupported (non-UTC), got {beh:?}"
            );
            let expected = readyset_adapter::SessionTimezone::FixedOffset(
                chrono::FixedOffset::east_opt(expected_offset_secs).unwrap(),
            );
            assert_eq!(
                beh.set_timezone,
                Some(expected),
                "{stmt:?} should record FixedOffset({expected_offset_secs}s), got {beh:?}"
            );
        }
    }

    #[test]
    fn set_time_zone_numeric_fractional_out_of_range_is_unsupported() {
        for stmt in ["SET TimeZone = 99.5", "SET TimeZone = -99.5"] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(beh.unsupported, "{stmt:?} should be unsupported");
            assert!(
                beh.set_timezone.is_none(),
                "{stmt:?} parsed nothing, set_timezone should stay None, got {beh:?}"
            );
        }
    }

    #[test]
    fn set_time_zone_non_utc_is_unsupported() {
        for stmt in [
            "SET timezone = 'America/New_York'",
            "SET timezone = 'Europe/London'",
            "SET timezone = '+05:30'",
            "SET TimeZone = 'us/eastern'",
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(
                beh.unsupported,
                "{stmt:?} should be unsupported (non-UTC), got {beh:?}"
            );
            assert!(
                beh.set_timezone.is_some(),
                "{stmt:?} should still record the parsed timezone, got {beh:?}"
            );
        }
    }

    #[test]
    fn set_time_zone_unparseable_is_unsupported() {
        for stmt in [
            "SET timezone = 'Narnia/Cair_Paravel'",
            "SET timezone = 'not a zone'",
            "SET timezone = ''",
        ] {
            let beh = PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt));
            assert!(beh.unsupported, "{stmt:?} should be unsupported");
            assert!(
                beh.set_timezone.is_none(),
                "{stmt:?} parsed nothing, set_timezone should stay None"
            );
        }
    }

    #[test]
    fn set_name_utf8_variations() {
        // The Postgres documentation lists "UTF8" as a supported Character Set. However, the
        // Postgres server also accepts "UTF-8" and "utf-8" as correct Character Set names.
        is_proxy("SET names 'UTF8'");
        is_proxy("SET names 'UTF-8'");
        is_proxy("SET names 'utf8'");
        is_proxy("SET names 'utf-8'");
    }

    #[test]
    fn autcommit_state() {
        assert_eq!(
            PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(
                "SET autocommit = off"
            )),
            SetBehavior::default().set_autocommit(false),
        );

        assert_eq!(
            PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(
                "SET autocommit = on"
            )),
            SetBehavior::default().set_autocommit(true),
        );
    }

    mod search_path {
        use super::*;

        fn sets_search_path(stmt: &str, search_path: Vec<&str>) {
            assert_eq!(
                PostgreSqlQueryHandler::handle_set_statement(&parse_set_statement(stmt)),
                SetBehavior::default()
                    .set_search_path(search_path.into_iter().map(Into::into).collect())
            );
        }

        #[test]
        fn single_identifier() {
            sets_search_path("SET search_path = s7", vec!["s7"])
        }

        #[test]
        fn single_string() {
            sets_search_path("SET search_path = 's7'", vec!["s7"])
        }

        #[test]
        fn identifiers() {
            sets_search_path(
                "SET search_path = s1, s2,   public",
                vec!["s1", "s2", "public"],
            );
        }

        #[test]
        fn identifiers_and_strings() {
            sets_search_path("SET search_path = s1, 's three'", vec!["s1", "s three"]);
        }

        #[test]
        fn default() {
            sets_search_path("SET search_path to DEFAULT", vec!["public"]);
        }
    }
}
