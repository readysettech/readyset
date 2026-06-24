//! Registry of all Readyset metrics.
//!
//! Each constant is the string passed to a metrics macro (e.g., counter!, gauge!, histogram!).
//! Definitions are grouped into modules by the subsystem that emits them, and every module is
//! re-exported flat, so callers reference a constant as `metric::METRIC_NAME` regardless of which
//! module defines it.

mod allocator;
mod noria;
mod reader_map;
mod readyset;
mod rocksdb;
mod schema_catalog;

pub use allocator::*;
pub use noria::*;
pub use reader_map::*;
pub use readyset::*;
pub use rocksdb::*;
pub use schema_catalog::*;
