//! Shared AVG display-scale policy for the dataflow grouped/window
//! operators and the adapter's post-lookup aggregation path.

use serde::{Deserialize, Serialize};

/// Controls how AVG display scale is computed after division.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AvgScaleMode {
    /// MySQL: round to a fixed number of decimal places (determined at plan time).
    Fixed(i64),
    /// PostgreSQL: dynamically compute display scale targeting ~16 significant digits,
    /// dependent on the magnitude of sum and count.
    PostgresComputed,
}
