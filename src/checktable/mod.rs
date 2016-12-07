//! # Check Tables
//!
//! Check Tables are used by the transaction subsystem to ensure that multiple conflicting
//! transactions do not all commit. They work by tracking the last timestamp that a write occurred
//! to a specific region.

use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::cmp;

#[derive(Clone, Eq, PartialEq, Hash)]
enum Conflict {
    BaseTable(NodeIndex),
}

#[derive(Clone)]
pub struct Token {
    conflicts: HashMap<Conflict, i64>,
}

impl Token {
    /// Reduce the size of the token, potentially increasing the amount of things it conflicts with.
    fn compact(&mut self) {

    }

    /// Combine two tokens into a single token conflicting with everything in either token's
    /// conflict set.
    pub fn merge(&mut self, other: Token) {
        for (conflict, ts) in other.conflicts.into_iter() {
            match self.conflicts.entry(conflict) {
                Entry::Occupied(mut entry) => {
                    let v = cmp::max(ts, entry.get().clone());
                    entry.insert(v);
                }
                Entry::Vacant(entry) => {
                    entry.insert(ts);
                }
            }
        }
        self.compact();
    }
}

#[derive(Clone)]
pub struct TokenGenerator {
    conflicts: Vec<Conflict>,
}

impl TokenGenerator {
    pub fn new(base_parents: Vec<NodeIndex>) -> Self {
        TokenGenerator {
            conflicts: base_parents.into_iter().map(|p| Conflict::BaseTable(p)).collect(),
        }
    }

    pub fn generate(&self, ts: i64) -> Token {
        Token {
            conflicts: self.conflicts.iter().map(|c| (c.clone(), ts)).collect(),
        }
    }
}

pub enum TransactionResult {
    Committed(i64),
    Aborted,
}

pub struct CheckTableSet {
    next_timestamp: i64,

    // Holds the last time each base node was written to.
    toplevel: HashMap<NodeIndex, i64>,
}

impl CheckTableSet {
    pub fn new(base_nodes: Vec<NodeIndex>) -> CheckTableSet {
        CheckTableSet {
            next_timestamp: 0,
            toplevel: base_nodes.into_iter().map(|n| (n, -1)).collect(),
        }
    }

    pub fn add_base_node(&mut self, index: NodeIndex) {
        self.toplevel.insert(index, self.next_timestamp - 1);
    }

    /// Return whether a transaction with this Token should commit.
    fn validate_token(&self, token: &Token) -> bool{
        return token.conflicts.iter().all(|(conflict,ts)| match conflict {
            &Conflict::BaseTable(node) => ts >= self.toplevel.get(&node).unwrap(),
        });
    }

    pub fn claim_timestamp(&mut self, token: &Token) -> Option<i64> {
        if self.validate_token(token) {
            self.next_timestamp += 1;
            Some(self.next_timestamp - 1)
        } else {
            None
        }
    }
}
