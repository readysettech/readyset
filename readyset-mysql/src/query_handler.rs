use std::borrow::Cow;
use std::collections::HashSet;
use std::str::FromStr;

use lazy_static::lazy_static;
use nom_sql::{Column, Expr, FieldDefinitionExpr, Literal, SqlIdentifier, SqlQuery, VariableScope};
use readyset_adapter::backend::noria_connector::QueryResult;
use readyset_adapter::backend::SelectSchema;
use readyset_adapter::{QueryHandler, SetBehavior};
use readyset_client::results::Results;
use readyset_client::ColumnSchema;
use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetError, ReadySetResult};
use tracing::warn;

const MAX_ALLOWED_PACKET_VARIABLE_NAME: &str = "max_allowed_packet";
const MAX_ALLOWED_PACKET_DEFAULT: DfValue = DfValue::UnsignedInt(67108864);

/// The list of mysql `SQL_MODE`s that *must* be set by a client
const REQUIRED_SQL_MODES: [SqlMode; 3] = [
    SqlMode::NoZeroDate,
    SqlMode::NoZeroInDate,
    SqlMode::OnlyFullGroupBy,
];

/// The list of mysql `SQL_MODE`s that *may* be set by a client (because they don't affect query
/// semantics)
const ALLOWED_SQL_MODES: [SqlMode; 11] = [
    SqlMode::ErrorForDivisionByZero, // deprecated
    SqlMode::IgnoreSpace,            // TODO: I think this is fine, but I'm not 100% sure
    SqlMode::NoAutoValueOnZero,
    SqlMode::NoDirInCreate,
    SqlMode::NoEngineSubstitution,
    SqlMode::NoZeroDate,
    SqlMode::NoZeroInDate,
    SqlMode::OnlyFullGroupBy,
    SqlMode::StrictAllTables,
    SqlMode::StrictTransTables,
    SqlMode::TimeTruncateFractional,
];

/// Enum representing the various flags that can be set as part of the MySQL `SQL_MODE` parameter.
/// See [the official mysql documentation][mysql-docs] for more information.
///
/// Note that this enum only includes the SQL mode flags that are present as of mysql 8.0 - any SQL
/// modes that have been removed since earlier versions are omitted here, as they're unsupported
/// regardless.
///
/// [mysql-docs]: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html
#[derive(PartialEq, Eq, Hash)]
enum SqlMode {
    AllowInvalidDates,
    AnsiQuotes,
    ErrorForDivisionByZero,
    HighNotPrecedence,
    IgnoreSpace,
    NoAutoCreateUser,
    NoAutoValueOnZero,
    NoBackslashEscapes,
    NoDirInCreate,
    NoEngineSubstitution,
    NoUnsignedSubtraction,
    NoZeroDate,
    NoZeroInDate,
    OnlyFullGroupBy,
    PadCharToFullLength,
    PipesAsConcat,
    RealAsFloat,
    StrictAllTables,
    StrictTransTables,
    TimeTruncateFractional,
}

impl FromStr for SqlMode {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.trim().to_ascii_lowercase()[..] {
            "allow_invalid_dates" => Ok(SqlMode::AllowInvalidDates),
            "ansi_quotes" => Ok(SqlMode::AnsiQuotes),
            "error_for_division_by_zero" => Ok(SqlMode::ErrorForDivisionByZero),
            "high_not_precedence" => Ok(SqlMode::HighNotPrecedence),
            "ignore_space" => Ok(SqlMode::IgnoreSpace),
            "no_auto_create_user" => Ok(SqlMode::NoAutoCreateUser),
            "no_auto_value_on_zero" => Ok(SqlMode::NoAutoValueOnZero),
            "no_backslash_escapes" => Ok(SqlMode::NoBackslashEscapes),
            "no_dir_in_create" => Ok(SqlMode::NoDirInCreate),
            "no_engine_substitution" => Ok(SqlMode::NoEngineSubstitution),
            "no_unsigned_subtraction" => Ok(SqlMode::NoUnsignedSubtraction),
            "no_zero_date" => Ok(SqlMode::NoZeroDate),
            "no_zero_in_date" => Ok(SqlMode::NoZeroInDate),
            "only_full_group_by" => Ok(SqlMode::OnlyFullGroupBy),
            "pad_char_to_full_length" => Ok(SqlMode::PadCharToFullLength),
            "pipes_as_concat" => Ok(SqlMode::PipesAsConcat),
            "real_as_float" => Ok(SqlMode::RealAsFloat),
            "strict_all_tables" => Ok(SqlMode::StrictAllTables),
            "strict_trans_tables" => Ok(SqlMode::StrictTransTables),
            "time_truncate_fractional" => Ok(SqlMode::TimeTruncateFractional),
            _ => Err(ReadySetError::SqlModeParseFailed(s.to_string())),
        }
    }
}

fn raw_sql_modes_to_list(sql_modes: &str) -> Result<Vec<SqlMode>, ReadySetError> {
    sql_modes
        .split(',')
        .map(SqlMode::from_str)
        .collect::<Result<Vec<SqlMode>, ReadySetError>>()
}

lazy_static! {
    /// The set of parameters that we can safely proxy upstream with *any* value, as we've
    /// determined that they don't change the semantics of queries in a way that would matter for us
    static ref ALLOWED_PARAMETERS_ANY_VALUE: HashSet<&'static str> = HashSet::from([
        "activate_all_roles_on_login",
        "admin_ssl_ca",
        "admin_ssl_capath",
        "admin_ssl_cert",
        "admin_ssl_cipher",
        "admin_ssl_crl",
        "admin_ssl_crlpath",
        "admin_ssl_key",
        "admin_tls_ciphersuites",
        "admin_tls_version",
        "audit_log_connection_policy",
        "audit_log_disable",
        "audit_log_exclude_accounts",
        "audit_log_flush",
        "audit_log_format_unix_timestamp",
        "audit_log_include_accounts",
        "audit_log_max_size",
        "audit_log_password_history_keep_days",
        "audit_log_prune_seconds",
        "audit_log_read_buffer_size",
        "audit_log_rotate_on_size",
        "audit_log_statement_policy",
        "authentication_fido_rp_id",
        "authentication_kerberos_service_principal",
        "authentication_ldap_sasl_auth_method_name",
        "authentication_ldap_sasl_bind_base_dn",
        "authentication_ldap_sasl_bind_root_dn",
        "authentication_ldap_sasl_bind_root_pwd",
        "authentication_ldap_sasl_ca_path",
        "authentication_ldap_sasl_group_search_attr",
        "authentication_ldap_sasl_group_search_filter",
        "authentication_ldap_sasl_init_pool_size",
        "authentication_ldap_sasl_log_status",
        "authentication_ldap_sasl_max_pool_size",
        "authentication_ldap_sasl_referral",
        "authentication_ldap_sasl_server_host",
        "authentication_ldap_sasl_server_port",
        "authentication_ldap_sasl_tls",
        "authentication_ldap_sasl_user_search_attr",
        "authentication_ldap_simple_auth_method_name",
        "authentication_ldap_simple_bind_base_dn",
        "authentication_ldap_simple_bind_root_dn",
        "authentication_ldap_simple_bind_root_pwd",
        "authentication_ldap_simple_ca_path",
        "authentication_ldap_simple_group_search_attr",
        "authentication_ldap_simple_group_search_filter",
        "authentication_ldap_simple_init_pool_size",
        "authentication_ldap_simple_log_status",
        "authentication_ldap_simple_max_pool_size",
        "authentication_ldap_simple_referral",
        "authentication_ldap_simple_server_host",
        "authentication_ldap_simple_server_port",
        "authentication_ldap_simple_tls",
        "authentication_ldap_simple_user_search_attr",
        "authentication_policy",
        "auto_increment_increment",
        "auto_increment_offset",
        "autocommit",
        "automatic_sp_privileges",
        "avoid_temporal_upgrade",
        "big_tables",
        "binlog_cache_size",
        "binlog_checksum",
        "binlog_direct_non_transactional_updates",
        "binlog_encryption",
        "binlog_error_action",
        "binlog_expire_logs_auto_purge",
        "binlog_expire_logs_seconds",
        "binlog_format",
        "binlog_group_commit_sync_delay",
        "binlog_group_commit_sync_no_delay_count",
        "binlog_max_flush_queue_time",
        "binlog_order_commits",
        "binlog_row_image",
        "binlog_row_metadata",
        "binlog_row_value_options",
        "binlog_rows_query_log_events",
        "binlog_stmt_cache_size",
        "binlog_transaction_compression",
        "binlog_transaction_compression_level_zstd",
        "binlog_transaction_dependency_history_size",
        "binlog_transaction_dependency_tracking",
        "block_encryption_mode",
        "bulk_insert_buffer_size",
        "character_set_client",
        "character_set_connection",
        "character_set_database",
        "character_set_filesystem",
        "character_set_results",
        "character_set_server",
        "check_proxy_users",
        "clone_autotune_concurrency",
        "clone_block_ddl",
        "clone_buffer_size",
        "clone_ddl_timeout",
        "clone_delay_after_data_drop",
        "clone_donor_timeout_after_network_failure",
        "clone_enable_compression",
        "clone_max_concurrency",
        "clone_max_data_bandwidth",
        "clone_max_network_bandwidth",
        "clone_ssl_ca",
        "clone_ssl_cert",
        "clone_ssl_key",
        "clone_valid_donor_list",
        "collation_connection",
        "collation_database",
        "collation_server",
        "completion_type",
        "concurrent_insert",
        "connect_timeout",
        "connection_control_failed_connections_threshold",
        "connection_control_max_connection_delay",
        "connection_control_min_connection_delay",
        "connection_memory_chunk_size",
        "connection_memory_limit",
        "cte_max_recursion_depth",
        "debug",
        "debug_sync",
        "default_collation_for_utf8mb4",
        "default_password_lifetime",
        "default_storage_engine",
        "default_table_encryption",
        "default_tmp_storage_engine",
        "default_week_format",
        "delay_key_write",
        "delayed_insert_limit",
        "delayed_insert_timeout",
        "delayed_queue_size",
        "div_precision_increment",
        "dragnet.log_error_filter_rules",
        "end_markers_in_json",
        "enforce_gtid_consistency",
        "eq_range_index_dive_limit",
        "event_scheduler",
        "expire_logs_days",
        "explicit_defaults_for_timestamp",
        "flush",
        "flush_time",
        "foreign_key_checks",
        "ft_boolean_syntax",
        "general_log",
        "general_log_file",
        "generated_random_password_length",
        "global_connection_memory_limit",
        "global_connection_memory_tracking",
        "group_concat_max_len",
        "group_replication_advertise_recovery_endpoints",
        "group_replication_allow_local_lower_version_join",
        "group_replication_auto_increment_increment",
        "group_replication_autorejoin_tries",
        "group_replication_bootstrap_group",
        "group_replication_clone_threshold",
        "group_replication_communication_debug_options",
        "group_replication_communication_max_message_size",
        "group_replication_components_stop_timeout",
        "group_replication_compression_threshold",
        "group_replication_consistency",
        "group_replication_enforce_update_everywhere_checks",
        "group_replication_exit_state_action",
        "group_replication_flow_control_applier_threshold",
        "group_replication_flow_control_certifier_threshold",
        "group_replication_flow_control_hold_percent",
        "group_replication_flow_control_max_commit_quota",
        "group_replication_flow_control_member_quota_percent",
        "group_replication_flow_control_min_quota",
        "group_replication_flow_control_min_recovery_quota",
        "group_replication_flow_control_mode",
        "group_replication_flow_control_period",
        "group_replication_flow_control_release_percent",
        "group_replication_force_members",
        "group_replication_group_name",
        "group_replication_group_seeds",
        "group_replication_gtid_assignment_block_size",
        "group_replication_ip_allowlist",
        "group_replication_ip_whitelist",
        "group_replication_local_address",
        "group_replication_member_expel_timeout",
        "group_replication_member_weight",
        "group_replication_message_cache_size",
        "group_replication_paxos_single_leader",
        "group_replication_poll_spin_loops",
        "group_replication_recovery_complete_at",
        "group_replication_recovery_compression_algorithms",
        "group_replication_recovery_get_public_key",
        "group_replication_recovery_public_key_path",
        "group_replication_recovery_reconnect_interval",
        "group_replication_recovery_retry_count",
        "group_replication_recovery_ssl_ca",
        "group_replication_recovery_ssl_capath",
        "group_replication_recovery_ssl_cert",
        "group_replication_recovery_ssl_cipher",
        "group_replication_recovery_ssl_crl",
        "group_replication_recovery_ssl_crlpath",
        "group_replication_recovery_ssl_key",
        "group_replication_recovery_ssl_verify_server_cert",
        "group_replication_recovery_tls_ciphersuites",
        "group_replication_recovery_tls_version",
        "group_replication_recovery_use_ssl",
        "group_replication_recovery_zstd_compression_level",
        "group_replication_single_primary_mode",
        "group_replication_ssl_mode",
        "group_replication_start_on_boot",
        "group_replication_tls_source",
        "group_replication_transaction_size_limit",
        "group_replication_unreachable_majority_timeout",
        "group_replication_view_change_uuid",
        "gtid_executed_compression_period",
        "gtid_mode",
        "gtid_next",
        "gtid_purged",
        "histogram_generation_max_mem_size",
        "host_cache_size",
        "identity",
        "immediate_server_version",
        "information_schema_stats_expiry",
        "init_connect",
        "init_replica",
        "init_slave",
        "innodb_adaptive_flushing",
        "innodb_adaptive_flushing_lwm",
        "innodb_adaptive_hash_index",
        "innodb_adaptive_max_sleep_delay",
        "innodb_api_bk_commit_interval",
        "innodb_api_trx_level",
        "innodb_autoextend_increment",
        "innodb_background_drop_list_empty",
        "innodb_buffer_pool_dump_at_shutdown",
        "innodb_buffer_pool_dump_now",
        "innodb_buffer_pool_dump_pct",
        "innodb_buffer_pool_filename",
        "innodb_buffer_pool_in_core_file",
        "innodb_buffer_pool_load_abort",
        "innodb_buffer_pool_load_now",
        "innodb_buffer_pool_size",
        "innodb_change_buffer_max_size",
        "innodb_change_buffering",
        "innodb_change_buffering_debug",
        "innodb_checkpoint_disabled",
        "innodb_checksum_algorithm",
        "innodb_cmp_per_index_enabled",
        "innodb_commit_concurrency",
        "innodb_compress_debug",
        "innodb_compression_failure_threshold_pct",
        "innodb_compression_level",
        "innodb_compression_pad_pct_max",
        "innodb_concurrency_tickets",
        "innodb_ddl_buffer_size",
        "innodb_ddl_log_crash_reset_debug",
        "innodb_ddl_threads",
        "innodb_deadlock_detect",
        "innodb_default_row_format",
        "innodb_disable_sort_file_cache",
        "innodb_extend_and_initialize",
        "innodb_fast_shutdown",
        "innodb_fil_make_page_dirty_debug",
        "innodb_file_per_table",
        "innodb_fill_factor",
        "innodb_flush_log_at_timeout",
        "innodb_flush_log_at_trx_commit",
        "innodb_flush_neighbors",
        "innodb_flush_sync",
        "innodb_flushing_avg_loops",
        "innodb_fsync_threshold",
        "innodb_ft_aux_table",
        "innodb_ft_enable_diag_print",
        "innodb_ft_enable_stopword",
        "innodb_ft_num_word_optimize",
        "innodb_ft_result_cache_limit",
        "innodb_ft_server_stopword_table",
        "innodb_ft_user_stopword_table",
        "innodb_idle_flush_pct",
        "innodb_io_capacity",
        "innodb_io_capacity_max",
        "innodb_limit_optimistic_insert_debug",
        "innodb_lock_wait_timeout",
        "innodb_log_buffer_size",
        "innodb_log_checkpoint_fuzzy_now",
        "innodb_log_checkpoint_now",
        "innodb_log_checksums",
        "innodb_log_compressed_pages",
        "innodb_log_spin_cpu_abs_lwm",
        "innodb_log_spin_cpu_pct_hwm",
        "innodb_log_wait_for_flush_spin_hwm",
        "innodb_log_write_ahead_size",
        "innodb_log_writer_threads",
        "innodb_lru_scan_depth",
        "innodb_max_dirty_pages_pct",
        "innodb_max_dirty_pages_pct_lwm",
        "innodb_max_purge_lag",
        "innodb_max_purge_lag_delay",
        "innodb_max_undo_log_size",
        "innodb_merge_threshold_set_all_debug",
        "innodb_monitor_disable",
        "innodb_monitor_enable",
        "innodb_monitor_reset",
        "innodb_monitor_reset_all",
        "innodb_old_blocks_pct",
        "innodb_old_blocks_time",
        "innodb_online_alter_log_max_size",
        "innodb_open_files",
        "innodb_optimize_fulltext_only",
        "innodb_parallel_read_threads",
        "innodb_print_all_deadlocks",
        "innodb_print_ddl_logs",
        "innodb_purge_batch_size",
        "innodb_purge_rseg_truncate_frequency",
        "innodb_random_read_ahead",
        "innodb_read_ahead_threshold",
        "innodb_redo_log_archive_dirs",
        "innodb_redo_log_encrypt",
        "innodb_replication_delay",
        "innodb_rollback_segments",
        "innodb_saved_page_number_debug",
        "innodb_segment_reserve_factor",
        "innodb_spin_wait_delay",
        "innodb_spin_wait_pause_multiplier",
        "innodb_stats_auto_recalc",
        "innodb_stats_include_delete_marked",
        "innodb_stats_method",
        "innodb_stats_on_metadata",
        "innodb_stats_persistent",
        "innodb_stats_persistent_sample_pages",
        "innodb_stats_transient_sample_pages",
        "innodb_status_output",
        "innodb_status_output_locks",
        "innodb_strict_mode",
        "innodb_sync_spin_loops",
        "innodb_table_locks",
        "innodb_thread_concurrency",
        "innodb_thread_sleep_delay",
        "innodb_tmpdir",
        "innodb_trx_purge_view_update_only_debug",
        "innodb_trx_rseg_n_slots_debug",
        "innodb_undo_log_encrypt",
        "innodb_undo_log_truncate",
        "innodb_undo_tablespaces",
        "innodb_use_fdatasync",
        "insert_id",
        "interactive_timeout",
        "internal_tmp_disk_storage_engine",
        "internal_tmp_mem_storage_engine",
        "join_buffer_size",
        "keep_files_on_create",
        "key_buffer_size",
        "key_cache_age_threshold",
        "key_cache_block_size",
        "key_cache_division_limit",
        "keyring_aws_cmk_id",
        "keyring_aws_region",
        "keyring_encrypted_file_data",
        "keyring_encrypted_file_password",
        "keyring_file_data",
        "keyring_hashicorp_auth_path",
        "keyring_hashicorp_ca_path",
        "keyring_hashicorp_caching",
        "keyring_hashicorp_role_id",
        "keyring_hashicorp_secret_id",
        "keyring_hashicorp_server_url",
        "keyring_hashicorp_store_path",
        "keyring_okv_conf_dir",
        "keyring_operations",
        "last_insert_id",
        "lc_messages",
        "lc_time_names",
        "local_infile",
        "lock_wait_timeout",
        "log_bin_trust_function_creators",
        "log_bin_use_v1_row_events",
        "log_error_services",
        "log_error_suppression_list",
        "log_error_verbosity",
        "log_output",
        "log_queries_not_using_indexes",
        "log_raw",
        "log_slow_admin_statements",
        "log_slow_extra",
        "log_slow_replica_statements",
        "log_slow_slave_statements",
        "log_statements_unsafe_for_binlog",
        "log_syslog",
        "log_syslog_facility",
        "log_syslog_include_pid",
        "log_syslog_tag",
        "log_throttle_queries_not_using_indexes",
        "log_timestamps",
        "long_query_time",
        "low_priority_updates",
        "mandatory_roles",
        "master_info_repository",
        "master_verify_checksum",
        "max_allowed_packet",
        "max_binlog_cache_size",
        "max_binlog_size",
        "max_binlog_stmt_cache_size",
        "max_connect_errors",
        "max_connections",
        "max_delayed_threads",
        "max_error_count",
        "max_execution_time",
        "max_heap_table_size",
        "max_insert_delayed_threads",
        "max_join_size",
        "max_length_for_sort_data",
        "max_points_in_geometry",
        "max_prepared_stmt_count",
        "max_relay_log_size",
        "max_seeks_for_key",
        "max_sort_length",
        "max_sp_recursion_depth",
        "max_user_connections",
        "max_write_lock_count",
        "min_examined_row_limit",
        "myisam_data_pointer_size",
        "myisam_max_sort_file_size",
        "myisam_repair_threads",
        "myisam_sort_buffer_size",
        "myisam_stats_method",
        "myisam_use_mmap",
        "mysql_firewall_mode",
        "mysql_firewall_trace",
        "mysql_native_password_proxy_users",
        "mysqlx_compression_algorithms",
        "mysqlx_connect_timeout",
        "mysqlx_deflate_default_compression_level",
        "mysqlx_deflate_max_client_compression_level",
        "mysqlx_document_id_unique_prefix",
        "mysqlx_enable_hello_notice",
        "mysqlx_idle_worker_thread_timeout",
        "mysqlx_interactive_timeout",
        "mysqlx_lz4_default_compression_level",
        "mysqlx_lz4_max_client_compression_level",
        "mysqlx_max_allowed_packet",
        "mysqlx_max_connections",
        "mysqlx_min_worker_threads",
        "mysqlx_read_timeout",
        "mysqlx_wait_timeout",
        "mysqlx_write_timeout",
        "mysqlx_zstd_default_compression_level",
        "mysqlx_zstd_max_client_compression_level",
        "ndb_allow_copying_alter_table",
        "ndb_autoincrement_prefetch_sz",
        "ndb_blob_read_batch_bytes",
        "ndb_blob_write_batch_bytes",
        "ndb_cache_check_time",
        "ndb_clear_apply_status",
        "ndb_conflict_role",
        "ndb_data_node_neighbour",
        "ndb_dbg_check_shares",
        "ndb_default_column_format",
        "ndb_default_column_format",
        "ndb_deferred_constraints",
        "ndb_deferred_constraints",
        "ndb_distribution",
        "ndb_distribution",
        "ndb_eventbuffer_free_percent",
        "ndb_eventbuffer_max_alloc",
        "ndb_extra_logging",
        "ndb_force_send",
        "ndb_fully_replicated",
        "ndb_index_stat_enable",
        "ndb_index_stat_option",
        "ndb_join_pushdown",
        "ndb_log_binlog_index",
        "ndb_log_empty_epochs",
        "ndb_log_empty_epochs",
        "ndb_log_empty_update",
        "ndb_log_empty_update",
        "ndb_log_exclusive_reads",
        "ndb_log_exclusive_reads",
        "ndb_log_update_as_write",
        "ndb_log_update_minimal",
        "ndb_log_updated_only",
        "ndb_metadata_check",
        "ndb_metadata_check_interval",
        "ndb_metadata_sync",
        "ndb_optimization_delay",
        "ndb_read_backup",
        "ndb_recv_thread_activation_threshold",
        "ndb_recv_thread_cpu_mask",
        "ndb_replica_batch_size",
        "ndb_replica_blob_write_batch_bytes",
        "ndb_report_thresh_binlog_epoch_slip",
        "ndb_report_thresh_binlog_mem_usage",
        "ndb_row_checksum",
        "ndb_schema_dist_lock_wait_timeout",
        "ndb_show_foreign_key_mock_tables",
        "ndb_slave_conflict_role",
        "ndb_table_no_logging",
        "ndb_table_temporary",
        "ndb_use_exact_count",
        "ndb_use_transactions",
        "ndbinfo_max_bytes",
        "ndbinfo_max_rows",
        "ndbinfo_offline",
        "ndbinfo_show_hidden",
        "net_buffer_length",
        "net_read_timeout",
        "net_retry_count",
        "net_write_timeout",
        "new",
        "offline_mode",
        "old_alter_table",
        "optimizer_prune_level",
        "optimizer_search_depth",
        "optimizer_switch",
        "optimizer_trace",
        "optimizer_trace_features",
        "optimizer_trace_limit",
        "optimizer_trace_max_mem_size",
        "optimizer_trace_offset",
        "original_commit_timestamp",
        "original_server_version",
        "parser_max_mem_size",
        "partial_revokes",
        "password_history",
        "password_require_current",
        "password_reuse_interval",
        "performance_schema_max_digest_sample_age",
        "performance_schema_show_processlist",
        "preload_buffer_size",
        "print_identified_with_as_hex",
        "profiling",
        "profiling_history_size",
        "protocol_compression_algorithms",
        "pseudo_replica_mode",
        "pseudo_slave_mode",
        "pseudo_thread_id",
        "query_alloc_block_size",
        "query_prealloc_size",
        "rand_seed1",
        "rand_seed2",
        "range_alloc_block_size",
        "range_optimizer_max_mem_size",
        "rbr_exec_mode",
        "read_buffer_size",
        "read_only",
        "read_rnd_buffer_size",
        "regexp_stack_limit",
        "regexp_time_limit",
        "relay_log_info_repository",
        "relay_log_purge",
        "replica_allow_batching",
        "replica_checkpoint_group",
        "replica_checkpoint_period",
        "replica_compressed_protocol",
        "replica_exec_mode",
        "replica_max_allowed_packet",
        "replica_net_timeout",
        "replica_parallel_type",
        "replica_parallel_workers",
        "replica_pending_jobs_size_max",
        "replica_preserve_commit_order",
        "replica_sql_verify_checksum",
        "replica_transaction_retries",
        "replica_type_conversions",
        "replication_optimize_for_static_plugin_config",
        "replication_sender_observe_commit_only",
        "require_row_format",
        "require_secure_transport",
        "resultset_metadata",
        "rewriter_enabled",
        "rewriter_verbose",
        "rpl_read_size",
        "rpl_semi_sync_master_enabled",
        "rpl_semi_sync_master_timeout",
        "rpl_semi_sync_master_trace_level",
        "rpl_semi_sync_master_wait_for_slave_count",
        "rpl_semi_sync_master_wait_no_slave",
        "rpl_semi_sync_master_wait_point",
        "rpl_semi_sync_replica_enabled",
        "rpl_semi_sync_replica_trace_level",
        "rpl_semi_sync_slave_enabled",
        "rpl_semi_sync_slave_trace_level",
        "rpl_semi_sync_source_enabled",
        "rpl_semi_sync_source_timeout",
        "rpl_semi_sync_source_trace_level",
        "rpl_semi_sync_source_wait_for_replica_count",
        "rpl_semi_sync_source_wait_no_replica",
        "rpl_semi_sync_source_wait_point",
        "rpl_stop_replica_timeout",
        "rpl_stop_slave_timeout",
        "schema_definition_cache",
        "secondary_engine_cost_threshold",
        "select_into_buffer_size",
        "select_into_disk_sync",
        "select_into_disk_sync_delay",
        "server_id",
        "session_track_gtids",
        "session_track_schema",
        "session_track_state_change",
        "session_track_system_variables",
        "session_track_transaction_info",
        "sha256_password_proxy_users",
        "show_create_table_skip_secondary_engine",
        "show_create_table_verbosity",
        "show_gipk_in_create_table_and_information_schema",
        "show_old_temporals",
        "slave_allow_batching",
        "slave_checkpoint_group",
        "slave_checkpoint_period",
        "slave_compressed_protocol",
        "slave_exec_mode",
        "slave_max_allowed_packet",
        "slave_net_timeout",
        "slave_parallel_type",
        "slave_parallel_workers",
        "slave_pending_jobs_size_max",
        "slave_preserve_commit_order",
        "slave_rows_search_algorithms",
        "slave_sql_verify_checksum",
        "slave_transaction_retries",
        "slave_type_conversions",
        "slow_launch_time",
        "slow_query_log",
        "slow_query_log_file",
        "sort_buffer_size",
        "source_verify_checksum",
        "sql_auto_is_null",
        "sql_big_selects",
        "sql_buffer_result",
        "sql_generate_invisible_primary_key",
        "sql_log_bin",
        "sql_log_off",
        "sql_notes",
        "sql_quote_show_create",
        "sql_replica_skip_counter",
        "sql_require_primary_key",
        "sql_safe_updates",
        "sql_select_limit",
        "sql_slave_skip_counter",
        "sql_warnings",
        "ssl_ca",
        "ssl_capath",
        "ssl_cert",
        "ssl_cipher",
        "ssl_crl",
        "ssl_crlpath",
        "ssl_fips_mode",
        "ssl_key",
        "ssl_session_cache_mode",
        "ssl_session_cache_timeout",
        "stored_program_cache",
        "stored_program_definition_cache",
        "super_read_only",
        "sync_binlog",
        "sync_master_info",
        "sync_relay_log",
        "sync_relay_log_info",
        "sync_source_info",
        "syseventlog.facility",
        "syseventlog.include_pid",
        "syseventlog.tag",
        "table_definition_cache",
        "table_encryption_privilege_check",
        "table_open_cache",
        "tablespace_definition_cache",
        "temptable_max_mmap",
        "temptable_max_ram",
        "temptable_use_mmap",
        "terminology_use_previous",
        "thread_cache_size",
        "thread_pool_high_priority_connection",
        "thread_pool_max_active_query_threads",
        "thread_pool_max_unused_threads",
        "thread_pool_prio_kickup_timer",
        "thread_pool_stall_limit",
        "timestamp",
        "tls_ciphersuites",
        "tls_version",
        "tmp_table_size",
        "transaction_alloc_block_size",
        "transaction_allow_batching",
        "transaction_isolation",
        "transaction_prealloc_size",
        "transaction_read_only",
        "transaction_write_set_extraction",
        "unique_checks",
        "updatable_views_with_limit",
        "use_secondary_engine",
        "validate_password.check_user_name",
        "validate_password.dictionary_file",
        "validate_password.length",
        "validate_password.mixed_case_count",
        "validate_password.number_count",
        "validate_password.policy",
        "validate_password.special_char_count",
        "validate_password_check_user_name",
        "validate_password_dictionary_file",
        "validate_password_length",
        "validate_password_mixed_case_count",
        "validate_password_number_count",
        "validate_password_policy",
        "validate_password_special_char_count",
        "version_tokens_session",
        "wait_timeout",
        "windowing_use_high_precision",
        "xa_detach_on_prepare"
    ]);
}

/// MySQL flavor of [`QueryHandler`].
pub struct MySqlQueryHandler;

impl QueryHandler for MySqlQueryHandler {
    fn requires_fallback(query: &SqlQuery) -> bool {
        // Currently any query with variables requires a fallback
        match query {
            SqlQuery::Select(stmt) => stmt.fields.iter().any(|field| match field {
                FieldDefinitionExpr::Expr { expr, .. } => expr.contains_vars(),
                _ => false,
            }),
            _ => false,
        }
    }

    fn default_response(query: &SqlQuery) -> ReadySetResult<QueryResult<'static>> {
        // For now we only care if we are querying for the `@@max_allowed_packet`
        // (ignoring any other field), in which case we return a hardcoded result.
        // This hardcoded result is needed because some libraries expect it when
        // creating a new MySQL connection (i.e., when using `[mysql::Conn::new]`).
        // No matter how many fields appeared in the query, we only return the mentioned
        // hardcoded value.
        // If `@@max_allowed_packet` was not present in the fields, we return an empty set
        // of rows.
        match query {
            SqlQuery::Select(stmt)
                if stmt.fields.iter().any(|field| {
                    matches!(field, FieldDefinitionExpr::Expr {
                        expr: Expr::Variable(var),
                        ..
                    } if var.as_non_user_var() == Some(MAX_ALLOWED_PACKET_VARIABLE_NAME))
                }) =>
            {
                let field_name: SqlIdentifier =
                    format!("@@{}", MAX_ALLOWED_PACKET_VARIABLE_NAME).into();
                Ok(QueryResult::from_owned(
                    SelectSchema {
                        schema: Cow::Owned(vec![ColumnSchema {
                            column: Column {
                                name: field_name.clone(),
                                table: None,
                            },
                            column_type: DfType::UnsignedInt,
                            base: None,
                        }]),
                        columns: Cow::Owned(vec![field_name]),
                    },
                    vec![Results::new(vec![vec![MAX_ALLOWED_PACKET_DEFAULT]])],
                ))
            }
            _ => Ok(QueryResult::empty(SelectSchema {
                schema: Cow::Owned(vec![]),
                columns: Cow::Owned(vec![]),
            })),
        }
    }

    fn handle_set_statement(stmt: &nom_sql::SetStatement) -> SetBehavior {
        use SetBehavior::*;

        match stmt {
            nom_sql::SetStatement::Variable(set) => {
                if let Some(val) = set.variables.iter().find_map(|(var, val)| {
                    if var.name.as_str().eq_ignore_ascii_case("autocommit") {
                        Some(val)
                    } else {
                        None
                    }
                }) {
                    return SetAutocommit(
                        matches!(val, Expr::Literal(Literal::Integer(i)) if *i == 1),
                    );
                }

                SetBehavior::proxy_if(set.variables.iter().all(|(variable, value)| {
                    if variable.scope == VariableScope::User {
                        return false;
                    }
                    match variable.name.to_ascii_lowercase().as_str() {
                        "time_zone" => {
                            matches!(value, Expr::Literal(Literal::String(ref s)) if s == "+00:00")
                        }
                        "sql_mode" => {
                            if let Expr::Literal(Literal::String(ref s)) = value {
                                match raw_sql_modes_to_list(&s[..]) {
                                    Ok(sql_modes) => {
                                        REQUIRED_SQL_MODES.iter().all(|m| sql_modes.contains(m))
                                            && sql_modes.iter().all(|sql_mode| {
                                                ALLOWED_SQL_MODES.contains(sql_mode)
                                            })
                                    }
                                    Err(e) => {
                                        warn!(%e, "unknown sql modes in set");
                                        false
                                    }
                                }
                            } else {
                                false
                            }
                        }
                        "names" => {
                            if let Expr::Literal(Literal::String(ref s)) = value {
                                matches!(&s[..], "latin1" | "utf8" | "utf8mb4")
                            } else {
                                false
                            }
                        }
                        p => ALLOWED_PARAMETERS_ANY_VALUE.contains(p),
                    }
                }))
            }
            nom_sql::SetStatement::Names(names) => SetBehavior::proxy_if(
                names.collation.is_none()
                    && matches!(&names.charset[..], "latin1" | "utf8" | "utf8mb4"),
            ),
            nom_sql::SetStatement::PostgresParameter(_) => Unsupported,
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{SetStatement, SetVariables, Variable};

    use super::*;

    #[test]
    fn supported_sql_mode() {
        let m = "NO_ZERO_DATE,STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE";
        let stmt = SetStatement::Variable(SetVariables {
            variables: vec![(
                Variable {
                    scope: VariableScope::Session,
                    name: "sql_mode".into(),
                },
                Expr::Literal(Literal::from(m)),
            )],
        });
        assert_eq!(
            MySqlQueryHandler::handle_set_statement(&stmt),
            SetBehavior::Proxy
        );
    }

    #[test]
    fn unsupported_sql_mode() {
        let m = "NO_ZERO_IN_DATE,STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,ANSI_QUOTES";
        let stmt = SetStatement::Variable(SetVariables {
            variables: vec![(
                Variable {
                    scope: VariableScope::Session,
                    name: "sql_mode".into(),
                },
                Expr::Literal(Literal::from(m)),
            )],
        });
        assert_eq!(
            MySqlQueryHandler::handle_set_statement(&stmt),
            SetBehavior::Unsupported
        );
    }

    #[test]
    fn all_required_sql_modes_are_allowed() {
        for mode in REQUIRED_SQL_MODES {
            assert!(ALLOWED_SQL_MODES.contains(&mode))
        }
    }
}
