//! Client-side types for replay paths
//!
//! These are simplified, flattened representations of replay paths suitable for RPC
//! communication and display, without requiring complex nested types.

use serde::{Deserialize, Serialize};

/// Flattened representation of a replay path suitable for display and RPC
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayPathInfo {
    /// Domain index
    pub domain: u32,
    /// Tag identifying this replay path
    pub tag: u32,
    /// Source node (if any)
    pub source: Option<u32>,
    /// Destination index formatted as "type[cols]" (e.g., "HashMap[1,2,3]")
    pub destination_index: Option<String>,
    /// Target index formatted as "type[cols]"
    pub target_index: Option<String>,
    /// Path segments, each formatted as a string
    pub path_segments: Vec<String>,
    /// Trigger type: "Start", "End", "Local", or "None"
    pub trigger_type: String,
    /// Trigger index (if applicable)
    pub trigger_index: Option<String>,
    /// Trigger source/options (e.g., "AllShards(2), options: [Domain 1]")
    pub trigger_source_options: String,
}
