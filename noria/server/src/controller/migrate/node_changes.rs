//! This module holds all the structures necessary to represent the node changes that have to be
//! made to the Noria state (addition, removal, modification) during a migration.

use std::collections::HashSet;
use std::vec::IntoIter;

use petgraph::graph::NodeIndex;

/// Represents a set of node changes that need to be made to the Noria state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(in crate::controller) enum NodeChanges {
    /// The nodes that have to be added to the Noria state.
    Add(HashSet<NodeIndex>),
    /// The nodes that have to be removed from the Noria state.
    Drop(HashSet<NodeIndex>),
}

/// Represent the list of changes that need to be made to the Noria state, in the context of a
/// [`Migration`].
/// This structure is a simple wrapper of a `Vec<NodeChanges>`, and the primary intention of that is
/// to maintain "groups" of changes, by clustering as many changes of the same kind as possible
/// before creating a new `NodeChange` group. For example, if the changes to be made were:
/// 1. Add node 1
/// 2. Add node 2
/// 3. Drop node 1
/// 4. Add node 3
/// Then the resulting list of changes would be
/// `[NodeChanges::Add(1, 2), NodeChanges::Drop(1), NodeChanges::Add(3)]`.
///
/// [`Migration`]: noria_server::controller::migrate::Migration
#[derive(Default, Debug)]
pub(in crate::controller) struct MigrationNodeChanges(Vec<NodeChanges>);

impl MigrationNodeChanges {
    /// Registers a node addition in the list of changes.
    /// If the last change registered is a `NodeChanges::Drop` change, then a new `NodeChanges::Add`
    /// change is created, and the node is added to it. Otherwise, the node gets added to the last
    /// `NodeChanges::Add` change.
    pub(in crate::controller) fn add_node(&mut self, node: NodeIndex) {
        match self.0.last_mut() {
            Some(NodeChanges::Add(nodes)) => {
                nodes.insert(node);
            }
            _ => {
                let mut nodes_set = HashSet::new();
                nodes_set.insert(node);
                self.0.push(NodeChanges::Add(nodes_set));
            }
        }
    }

    /// Registers a node deletion in the list of changes.
    /// If the last change registered is a `NodeChanges::Add` change, then a new `NodeChanges::Drop`
    /// change is created, and the node is added to it. Otherwise, the node gets added to the last
    /// `NodeChanges::Drop` change.
    pub(in crate::controller) fn drop_node(&mut self, node: NodeIndex) {
        match self.0.last_mut() {
            Some(NodeChanges::Drop(nodes)) => {
                nodes.insert(node);
            }
            _ => {
                let mut nodes_set = HashSet::new();
                nodes_set.insert(node);
                self.0.push(NodeChanges::Drop(nodes_set));
            }
        }
    }

    /// Whether or not the given node is part of any of the nodes being added.
    pub(in crate::controller) fn contains_new(&self, ni: &NodeIndex) -> bool {
        let mut found = false;
        for nc in self.0.iter() {
            match nc {
                // If it's present as part of the nodes being added, then it's
                // part of the new nodes.
                NodeChanges::Add(nodes) => found |= nodes.contains(ni),
                // If it's present as part of the nodes being dropped, then it's
                // not part of the new nodes.
                NodeChanges::Drop(nodes) => found &= !nodes.contains(ni),
            }
        }
        found
    }
}

impl IntoIterator for MigrationNodeChanges {
    type Item = NodeChanges;
    type IntoIter = std::vec::IntoIter<NodeChanges>;

    fn into_iter(self) -> IntoIter<NodeChanges> {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_drop() {
        let mut changes = MigrationNodeChanges::default();
        changes.add_node(NodeIndex::new(1));
        changes.add_node(NodeIndex::new(2));
        changes.add_node(NodeIndex::new(3));
        changes.drop_node(NodeIndex::new(1));
        changes.add_node(NodeIndex::new(4));

        let changes = changes.0;
        assert_eq!(changes.len(), 3);
        assert_eq!(
            changes[0],
            NodeChanges::Add(
                vec![NodeIndex::new(1), NodeIndex::new(2), NodeIndex::new(3)]
                    .into_iter()
                    .collect()
            )
        );
        assert_eq!(
            changes[1],
            NodeChanges::Drop(vec![NodeIndex::new(1)].into_iter().collect())
        );
        assert_eq!(
            changes[2],
            NodeChanges::Add(vec![NodeIndex::new(4)].into_iter().collect())
        );
    }

    #[test]
    fn contains_new() {
        let mut changes = MigrationNodeChanges::default();
        // We'll check for node 1 and node 5.
        changes.add_node(NodeIndex::new(1)); // Node 1 - Present
        changes.add_node(NodeIndex::new(2));
        changes.add_node(NodeIndex::new(3));
        changes.drop_node(NodeIndex::new(2));
        changes.drop_node(NodeIndex::new(3));
        changes.add_node(NodeIndex::new(4));
        changes.add_node(NodeIndex::new(5)); // Node 5 - Present
        changes.drop_node(NodeIndex::new(1)); // Node 1 - Not present
        changes.add_node(NodeIndex::new(6));
        changes.add_node(NodeIndex::new(7));
        changes.drop_node(NodeIndex::new(5)); // Node 5 - Not present
        changes.add_node(NodeIndex::new(1)); // Node 1 - Present again

        assert!(changes.contains_new(&NodeIndex::new(1)));
        assert!(!changes.contains_new(&NodeIndex::new(5)));
    }
}
