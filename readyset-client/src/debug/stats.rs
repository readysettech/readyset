use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use readyset_sql::ast::SqlIdentifier;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::internal::*;
use crate::MaterializationStatus;

type DomainMap = HashMap<DomainIndex, Option<(DomainStats, HashMap<NodeIndex, NodeStats>)>>;

/// Statistics about a domain.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStats {
    /// Total wall-clock time elapsed while processing in this domain.
    pub total_time: u64,
    /// Total thread time elapsed while processing in this domain.
    pub total_ptime: u64,
    /// Total wall-clock time spent processing replays in this domain.
    pub total_replay_time: u64,
    /// Total wall-clock time spent processing forward updates in this domain.
    pub total_forward_time: u64,
    /// Total wall-clock time spent waiting for work in this domain.
    pub wait_time: u64,
}

/// Statistics about a node.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStats {
    /// A textual description of this node.
    pub desc: String,
    /// Total wall-clock time elapsed while processing in this node.
    pub process_time: u64,
    /// Total thread time elapsed while processing in this node.
    pub process_ptime: u64,
    /// Total memory size of this node's state.
    pub mem_size: u64,
    /// The materialization type of this node's state.
    pub materialized: MaterializationStatus,
    /// The value returned from Ingredient::probe.
    pub probe_result: HashMap<String, String>,
}

/// Status that we persist in the Authority to make it available across restarts.
#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistentStats {
    /// Time in millis when the controller last started up.
    pub last_controller_startup: Option<u64>,
    /// Time in millis when the last snapshot was completed.
    pub last_completed_snapshot: Option<u64>,
    /// Time in millis when we last started the main replication loop.
    pub last_started_replication: Option<u64>,
    /// Last error reported by the replicator of the `None` catalog that caused it to restart. This
    /// message is cleared when we enter the main replication loop, because it is primarily
    /// intended to help debug issues with starting replication and holding onto errors forever
    /// can be confusing.
    pub last_replicator_error: Option<String>,
    /// Last error of the replicator of each `Some` catalog, kept as `last_replicator_error` is.
    /// The errors are serialized as a list of pairs because JSON map keys must be strings, and
    /// [`SqlIdentifier`] serializes as bytes.
    #[serde_as(as = "Vec<(_, _)>")]
    #[serde(default)]
    pub catalog_replicator_errors: HashMap<SqlIdentifier, String>,
}

impl PersistentStats {
    /// Set the last error of the replicator of `catalog`, or clear it if `error` is `None`.
    pub fn set_replicator_error(&mut self, catalog: Option<SqlIdentifier>, error: Option<String>) {
        match (catalog, error) {
            (None, error) => self.last_replicator_error = error,
            (Some(catalog), Some(error)) => {
                self.catalog_replicator_errors.insert(catalog, error);
            }
            (Some(catalog), None) => {
                self.catalog_replicator_errors.remove(&catalog);
            }
        }
    }
}

/// Statistics about the dataflow graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphStats {
    pub domains: DomainMap,
}

use std::ops::Deref;
impl Deref for GraphStats {
    type Target = DomainMap;
    fn deref(&self) -> &Self::Target {
        &self.domains
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`PersistentStats`] as stored before it had `catalog_replicator_errors`.
    #[derive(Serialize)]
    struct LegacyPersistentStats {
        last_controller_startup: Option<u64>,
        last_completed_snapshot: Option<u64>,
        last_started_replication: Option<u64>,
        last_replicator_error: Option<String>,
    }

    const LEGACY: LegacyPersistentStats = LegacyPersistentStats {
        last_controller_startup: Some(1),
        last_completed_snapshot: Some(2),
        last_started_replication: Some(3),
        last_replicator_error: None,
    };

    #[test]
    fn replicator_errors_are_per_catalog() {
        let db2 = SqlIdentifier::from("db2");
        let mut stats = PersistentStats::default();
        stats.set_replicator_error(None, Some("url".into()));
        stats.set_replicator_error(Some(db2.clone()), Some("first".into()));
        stats.set_replicator_error(Some(db2.clone()), Some("second".into()));
        assert_eq!(stats.last_replicator_error.as_deref(), Some("url"));
        assert_eq!(
            stats.catalog_replicator_errors,
            HashMap::from([(db2.clone(), "second".into())])
        );

        stats.set_replicator_error(Some(db2.clone()), None);
        assert_eq!(stats.last_replicator_error.as_deref(), Some("url"));
        assert!(stats.catalog_replicator_errors.is_empty());

        stats.set_replicator_error(Some(db2.clone()), Some("db2".into()));
        stats.set_replicator_error(None, None);
        assert_eq!(stats.last_replicator_error, None);
        assert_eq!(
            stats.catalog_replicator_errors,
            HashMap::from([(db2, "db2".into())])
        );
    }

    #[test]
    fn catalog_replicator_errors_round_trip() {
        let mut stats = PersistentStats::default();
        stats.set_replicator_error(Some("db2".into()), Some("error".into()));

        let json = serde_json::to_string(&stats).unwrap();
        let decoded: PersistentStats = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded.catalog_replicator_errors,
            stats.catalog_replicator_errors
        );

        let bytes = rmp_serde::to_vec(&stats).unwrap();
        let decoded: PersistentStats = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(
            decoded.catalog_replicator_errors,
            stats.catalog_replicator_errors
        );
    }

    #[test]
    fn legacy_json_decodes() {
        let json = serde_json::to_string(&LEGACY).unwrap();
        let stats: PersistentStats = serde_json::from_str(&json).unwrap();
        assert_eq!(stats.last_started_replication, Some(3));
        assert!(stats.catalog_replicator_errors.is_empty());
    }

    #[test]
    fn legacy_msgpack_decodes() {
        let bytes = rmp_serde::to_vec(&LEGACY).unwrap();
        let stats: PersistentStats = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(stats.last_started_replication, Some(3));
        assert!(stats.catalog_replicator_errors.is_empty());
    }
}
