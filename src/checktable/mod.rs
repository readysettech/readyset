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

/// TODO: add docs
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

/// Represents the result of a transaction
pub enum TransactionResult {
    /// The transaction committed at a given timestamp
    Committed(i64),
    /// The transaction aborted
    Aborted,
}

impl TransactionResult {
    /// Checks if a transaction committed.
    pub fn ok(&self) -> bool {
        if let &TransactionResult::Committed(_) = self {
            true
        } else {
            false
        }
    }
}

pub struct CheckTable {
    next_timestamp: i64,

    // Holds the last time each base node was written to.
    toplevel: HashMap<NodeIndex, i64>,
}

impl CheckTable {
    pub fn new() -> Self {
        CheckTable {
            next_timestamp: 0,
            toplevel: HashMap::new(),
        }
    }

    /// Return whether a transaction with this Token should commit.
    fn validate_token(&self, token: &Token) -> bool{
        return token.conflicts.iter().all(|(conflict,ts)| match conflict {
            &Conflict::BaseTable(node) => ts >= self.toplevel.get(&node).unwrap_or(&-1),
        });
    }

    pub fn claim_timestamp(&mut self, token: &Token, base: NodeIndex) -> TransactionResult {
        if self.validate_token(token) {
            let ts = self.next_timestamp;
            self.next_timestamp += 1;
            self.toplevel.insert(base, ts);
            TransactionResult::Committed(ts)
        } else {
            TransactionResult::Aborted
        }
    }
}
