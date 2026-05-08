//! Compatibility shims required to deserialize older persisted `ControllerState` blobs.
//!
//! The types in this module are no longer consulted at runtime; they exist only so that
//! `rmp_serde` can decode the array slots they used to occupy in `DfState` and `Config`.

#![allow(deprecated, dead_code)]

use serde::{Deserialize, Serialize};

/// Historical replication strategy configuration.
#[doc(hidden)]
#[deprecated(note = "kept only for persisted state compat; do not consult at runtime")]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    #[default]
    Never,
    ReaderDomains(usize),
    NonBaseDomains(usize),
}
