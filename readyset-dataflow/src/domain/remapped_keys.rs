//! Mapping, for nodes which [generate columns][], from *source(upstream)* keys, to *destination(downstream)* keys which
//! have remapped to those upstream keys.
//!
//! Written when we miss on downstream so we can use when we receive an eviction for those upstream keys, to rewrite that eviction into an
//! eviction for the downstream keys.
//!
//! # Internals
//!
//! The internals of this type are reasonably complicated, and best illustrated by an example.
//!
//! Consider we have some join, `n3` *(destination(downstream))*, with two parents `n1` partially materialized and `n2` fully materialized
//!  *(source(upstream))*, and which is indexed on columns `[1, 2]`, where column `1` maps to column `1` in `n1`, and column `2` maps to column `1`
//! in `n2` (a [straddled join][]).
//!
//! Dataflow Graph Structure
//! =======================
//!```notrust
//! ┌─────────┐    ┌─────────┐
//! │   n1 ◕  │    │   n2 ●  │
//! | (table) │    │ (table) │
//! | col[1]  │    │ col[1]  │
//! └─────────┘    └─────────┘
//!  │              │
//!  │              │
//!  ▼              ▼
//! ┌─────────────────────────┐
//! |          n3             │
//! |      (straddled         │
//! |       join)             │
//! |   indexed on [1,2]      │
//! |   col[1] → n1.col[1]    │
//! |   col[2] → n2.col[1]    │
//! └─────────────────────────┘
//!```
//!
//! Upquery Flow:
//! ─────────────
//! Now let's say we get some upquery on `[1, 2] = ["a", "b"]` for that join.
//! That upquery would get remapped to a pair of upqueries on the parents, `[1] = ["a"]`
//! on `n1` and `[1] = ["b"]` on `n2`. We then perform both of those upqueries (either in parallel or via state lookup depending on the SJ algorithm used),
//! and then once both have finished we perform the join and return the results.
//! Original upquery: n3[1,2] = ["a", "b"]
//!
//! Remapped to parents:
//! ├─ n1[1] = ["a"]  (from n3's col[1])
//! └─ n2[1] = ["b"]  (from n3's col[2])
//!
//! Eviction Flow:
//! ──────────────
//! Node `n2` is fuly materialized, but `n1` is partial. Now let's say `n1` gets an eviction for `[1] = ["a"]` it propagates to `n3` and `n3` gets an eviction
//! for `[1] = ["a"]` from `n1`. We don't have an index on only column `[1]` in `n3`, so we need to find a way of *mapping* that eviction into an eviction on
//! `[1, 2] = ["a", "b"]`. In that case, this data structure would look something like:
//!
//! RemappingInfo Structure:
//! =============================
//!
//! ```notrust
//! RemappedKeys {
//!     remappings: {
//!         RemappingKey {
//!         destination_node: n3,                                       // The destination/downstream node (n3)
//!         destination_columns: [1, 2],                                // The columns in n3 that were remapped
//!         source_node: n1,                                            // The source/upstream node (n1)
//!         source_columns: [1],                                        // The columns in n1 that were missed on
//!         source_keys: [KeyComparison::Equal(["a"])]                  // The source/upstream keys that were missed on n1
//!         } -> RemappingInfo {
//!             destination_keys: [KeyComparison::Equal(["a", "b"])],  // The destination/downstream keys that were remapped to the source/upstream keys
//!             tag: Tag(some_tag)                                     // The tag that triggered the remapping (the miss on n3)
//!         }
//!     },
//!     len: 1,
//! }
//! ```
//!
//! Which is sufficient information to remap our upstream eviction to a downstream one
//!
//! [generate columns]: http://docs/dataflow/replay_paths.html#generated-columns
//! [straddled join]: http://docs/dataflow/replay_paths.html#straddled-joins
//! Information about a single remapping from downstream keys to upstream keys.
//! This replaces the complex nested HashMap structure with a more readable flat structure.
use crate::processing::ColumnMiss;
use common::{Len, Tag};
use readyset_client::{internal::LocalNodeIndex, KeyComparison};
use std::collections::HashMap;

/// Composite key for identifying a unique remapping
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RemappingKey {
    destination_node: LocalNodeIndex,
    destination_columns: Vec<usize>,
    source_node: LocalNodeIndex,
    source_columns: Vec<usize>,
    source_keys: Vec<KeyComparison>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RemappingInfo {
    /// The downstream keys that have been remapped to the upstream keys
    destination_keys: Vec<KeyComparison>,
    /// The tag associated with this remapping
    tag: Tag,
}

impl RemappingInfo {
    /// Create a new RemappingInfo with the given downstream keys and tag
    fn new(destination_keys: Vec<KeyComparison>, tag: Tag) -> Self {
        Self {
            destination_keys,
            tag,
        }
    }

    /// Add a destination key
    fn add_destination_key(&mut self, destination_key: KeyComparison) {
        self.destination_keys.push(destination_key);
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemappedKeys {
    remappings: HashMap<RemappingKey, RemappingInfo>,
    len: usize,
}

impl RemappedKeys {
    /// Record that some downstream key was rewritten by `miss_in` into an `upstream_miss`.
    pub fn insert(
        &mut self,
        destination_node: LocalNodeIndex,
        destination_columns: Vec<usize>,
        destination_key: KeyComparison,
        source_miss: ColumnMiss,
        tag: Tag,
    ) {
        let key = RemappingKey {
            destination_node,
            destination_columns,
            source_node: source_miss.node,
            source_columns: source_miss.column_indices,
            source_keys: source_miss.missed_keys.into_vec(),
        };

        match self.remappings.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                debug_assert_eq!(entry.get().tag, tag);
                entry.get_mut().add_destination_key(destination_key);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let new_remapping = RemappingInfo::new(vec![destination_key], tag);
                entry.insert(new_remapping);
            }
        }
        self.len += 1;
    }

    /// If `node` rewrites some of its downstream keys into upstream upqueries to `column` in a
    /// `target` node, look up the set of downstream tags and keys which have been rewritten to
    /// `keys`.
    pub fn remove(
        &mut self,
        destination_node: LocalNodeIndex,
        destination_columns: &[usize],
        source_node: LocalNodeIndex,
        source_columns: &[usize],
        source_keys: &[KeyComparison],
    ) -> Option<impl ExactSizeIterator<Item = (Tag, Vec<KeyComparison>)>> {
        let lookup_key = RemappingKey {
            destination_node,
            destination_columns: destination_columns.into(),
            source_node,
            source_columns: source_columns.into(),
            source_keys: source_keys.into(),
        };

        if let Some(remapping_info) = self.remappings.remove(&lookup_key) {
            let tag = remapping_info.tag;
            let destination_keys = remapping_info.destination_keys;
            self.len -= destination_keys.len();
            return Some(std::iter::once((tag, destination_keys)));
        }
        None
    }
}

impl Len for RemappedKeys {
    fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_data::DfValue;
    use vec1::Vec1;

    fn create_test_column_miss(
        node: LocalNodeIndex,
        column_indices: Vec<usize>,
        missed_keys: Vec<KeyComparison>,
    ) -> ColumnMiss {
        ColumnMiss {
            node,
            column_indices,
            missed_keys: Vec1::try_from_vec(missed_keys).unwrap(),
        }
    }

    fn create_test_key_comparison(values: Vec<DfValue>) -> KeyComparison {
        KeyComparison::Equal(Vec1::try_from_vec(values).unwrap())
    }

    fn create_test_tag(id: u32) -> Tag {
        Tag::new(id)
    }

    #[test]
    fn test_insert_single_entry() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0, 1],
            vec![create_test_key_comparison(vec![
                DfValue::Int(1),
                DfValue::Int(2),
            ])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(3), DfValue::Int(4)]);
        let downstream_tag = create_test_tag(1);

        remapped_keys.insert(
            miss_in,
            vec![0, 1],
            downstream_key.clone(),
            upstream_miss,
            downstream_tag,
        );

        assert_eq!(remapped_keys.len(), 1);
    }

    #[test]
    fn test_insert_multiple_entries() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss1 = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key1 = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag1 = create_test_tag(1);

        let upstream_miss2 = create_test_column_miss(
            LocalNodeIndex::make(3),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(3)])],
        );
        let downstream_key2 = create_test_key_comparison(vec![DfValue::Int(4)]);
        let downstream_tag2 = create_test_tag(2);

        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key1,
            upstream_miss1,
            downstream_tag1,
        );
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key2,
            upstream_miss2,
            downstream_tag2,
        );

        assert_eq!(remapped_keys.len(), 2);
    }

    #[test]
    fn test_insert_duplicate_keys_same_remapping() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key1 = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_key2 = create_test_key_comparison(vec![DfValue::Int(3)]);
        let downstream_tag = create_test_tag(1);

        // Insert first downstream key
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key1,
            upstream_miss.clone(),
            downstream_tag,
        );

        // Insert second downstream key for the same remapping
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key2,
            upstream_miss,
            downstream_tag,
        );

        assert_eq!(remapped_keys.len(), 2);
    }

    #[test]
    fn test_insert_duplicate_downstream_key() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag = create_test_tag(1);

        // Insert the same downstream key twice
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key.clone(),
            upstream_miss.clone(),
            downstream_tag,
        );
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key,
            upstream_miss,
            downstream_tag,
        );

        assert_eq!(remapped_keys.len(), 2);
    }

    #[test]
    fn test_remove_existing_entry() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag = create_test_tag(1);

        // Insert entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key.clone(),
            upstream_miss,
            downstream_tag,
        );
        assert_eq!(remapped_keys.len(), 1);

        // Remove entry
        let result = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );

        assert!(result.is_some());
        let mut result = result.unwrap();
        let (tag, keys) = result.next().unwrap();
        assert_eq!(tag, create_test_tag(1));
        assert_eq!(keys, vec![downstream_key]);
        assert_eq!(remapped_keys.len(), 0);
    }

    #[test]
    fn test_remove_non_existing_entry() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag = create_test_tag(1);

        // Insert entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key,
            upstream_miss,
            downstream_tag,
        );
        assert_eq!(remapped_keys.len(), 1);

        // Try to remove non-existing entry
        let result = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(3), // Different node
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );

        assert!(result.is_none());
        assert_eq!(remapped_keys.len(), 1); // Should still have the original entry
    }

    #[test]
    fn test_remove_with_different_columns() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0, 1],
            vec![create_test_key_comparison(vec![
                DfValue::Int(1),
                DfValue::Int(2),
            ])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(3)]);
        let downstream_tag = create_test_tag(1);

        // Insert entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key,
            upstream_miss,
            downstream_tag,
        );
        assert_eq!(remapped_keys.len(), 1);

        // Try to remove with different columns
        let result = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0], // Only one column instead of two
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );

        assert!(result.is_none());
        assert_eq!(remapped_keys.len(), 1);
    }

    #[test]
    fn test_remove_with_different_keys() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag = create_test_tag(1);

        // Insert entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key,
            upstream_miss,
            downstream_tag,
        );
        assert_eq!(remapped_keys.len(), 1);

        // Try to remove with different keys
        let result = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(5)])], // Different key
        );

        assert!(result.is_none());
        assert_eq!(remapped_keys.len(), 1);
    }

    #[test]
    fn test_multiple_downstream_keys_same_remapping() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key1 = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_key2 = create_test_key_comparison(vec![DfValue::Int(3)]);
        let downstream_tag = create_test_tag(1);

        // Insert multiple downstream keys for the same remapping
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key1.clone(),
            upstream_miss.clone(),
            downstream_tag,
        );
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key2.clone(),
            upstream_miss,
            downstream_tag,
        );

        assert_eq!(remapped_keys.len(), 2);

        // Remove should return both downstream keys
        let result = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );

        assert!(result.is_some());
        let mut result = result.unwrap();
        let (tag, keys) = result.next().unwrap();
        assert_eq!(tag, create_test_tag(1));
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&downstream_key1));
        assert!(keys.contains(&downstream_key2));
        assert_eq!(remapped_keys.len(), 0);
    }

    #[test]
    fn test_len_tracking() {
        let mut remapped_keys = RemappedKeys::default();
        assert_eq!(remapped_keys.len(), 0);

        let miss_in = LocalNodeIndex::make(1);
        let upstream_miss1 = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key1 = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag1 = create_test_tag(1);

        let upstream_miss2 = create_test_column_miss(
            LocalNodeIndex::make(3),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(3)])],
        );
        let downstream_key2 = create_test_key_comparison(vec![DfValue::Int(4)]);
        let downstream_tag2 = create_test_tag(2);

        // Insert first entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key1,
            upstream_miss1,
            downstream_tag1,
        );
        assert_eq!(remapped_keys.len(), 1);

        // Insert second entry
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key2,
            upstream_miss2,
            downstream_tag2,
        );
        assert_eq!(remapped_keys.len(), 2);

        // Remove first entry
        remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        assert_eq!(remapped_keys.len(), 1); // Should be 1 because we still have the second entry
    }

    #[test]
    fn test_complex_scenario() {
        let mut remapped_keys = RemappedKeys::default();

        let miss_in = LocalNodeIndex::make(1);

        // Create multiple remappings
        let upstream_miss1 = create_test_column_miss(
            LocalNodeIndex::make(2),
            vec![0],
            vec![create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        let downstream_key1 = create_test_key_comparison(vec![DfValue::Int(2)]);
        let downstream_tag1 = create_test_tag(1);

        let upstream_miss2 = create_test_column_miss(
            LocalNodeIndex::make(3),
            vec![0, 1],
            vec![create_test_key_comparison(vec![
                DfValue::Int(3),
                DfValue::Int(4),
            ])],
        );
        let downstream_key2 = create_test_key_comparison(vec![DfValue::Int(5)]);
        let downstream_tag2 = create_test_tag(2);

        // Insert both remappings
        remapped_keys.insert(
            miss_in,
            vec![0],
            downstream_key1,
            upstream_miss1,
            downstream_tag1,
        );
        remapped_keys.insert(
            miss_in,
            vec![0, 1],
            downstream_key2,
            upstream_miss2,
            downstream_tag2,
        );

        assert_eq!(remapped_keys.len(), 2);

        // Remove first remapping
        let result1 = remapped_keys.remove(
            miss_in,
            &[0],
            LocalNodeIndex::make(2),
            &[0],
            &[create_test_key_comparison(vec![DfValue::Int(1)])],
        );
        assert!(result1.is_some());
        assert_eq!(remapped_keys.len(), 1); // Should be 1 because we still have the second remapping

        // Second remapping should still exist
        let result2 = remapped_keys.remove(
            miss_in,
            &[0, 1],
            LocalNodeIndex::make(3),
            &[0, 1],
            &[create_test_key_comparison(vec![
                DfValue::Int(3),
                DfValue::Int(4),
            ])],
        );
        assert!(result2.is_some());
        assert_eq!(remapped_keys.len(), 0);
    }
}
