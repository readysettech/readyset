//! Registry of all Readyset metrics.
//!
//! Each constant is the string passed to a metrics macro (e.g., counter!, gauge!, histogram!).
//!
//! Definitions are grouped into modules by category, e.g., subsystem or concept, and every module
//! is re-exported flat, so callers reference a constant as `metric::METRIC_NAME` regardless of
//! which module defines it.

macro_rules! export_metrics {
    ($module:ident) => {
        mod $module;
        pub use $module::*;
    };
}

export_metrics!(allocator);
export_metrics!(connections);
export_metrics!(controller);
export_metrics!(dataflow);
export_metrics!(instance);
export_metrics!(migration_handler);
export_metrics!(query_log);
export_metrics!(query_sampler);
export_metrics!(query_status_cache);
export_metrics!(reader_map);
export_metrics!(replication);
export_metrics!(rocksdb);
export_metrics!(schema_catalog);
export_metrics!(session);
export_metrics!(shallow);
export_metrics!(views_synchronizer);
