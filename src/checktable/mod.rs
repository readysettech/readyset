//! # Check Tables
//!
//! Check Tables are used by the transaction subsystem to ensure that multiple conflicting
//! transactions do not all commit. They work by tracking the last timestamp that a write occurred
//! to a specific region.

use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;

use query::DataType;
use flow::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum Conflict {
    /// This conflict should trigger an abort if the given base table has seen a write after the given time.
    BaseTable(NodeIndex),
    /// This conflict should trigger an abort if the given base table has seen a write to the
    /// specified column that had a given value after the time indicated.
    BaseColumn(NodeIndex, usize),
}

/// Tokens are used to perform transactions. Any transactional write will include a token indicating
/// the universe of other writes that it could conflict with. A transaction's token is considered
/// invalid (and will therefore cause it to abort) if any of the contained conflicts are triggered.
#[derive(Clone)]
pub struct Token {
    conflicts: Vec<(i64, DataType, Vec<Conflict>)>,
}

impl Token {
    /// Reduce the size of the token, potentially increasing the amount of things it conflicts with.
    fn compact(&mut self) {}

    /// Combine two tokens into a single token conflicting with everything in either token's
    /// conflict set.
    pub fn merge(&mut self, other: Token) {
        let mut other_conflicts = other.conflicts;
        self.conflicts.append(&mut other_conflicts);
        self.compact();
    }

    /// Generate an empty token that conflicts with nothing. Such a token can be used to do a
    /// transaction that has no read set.
    pub fn empty() -> Self {
        Token { conflicts: Vec::new() }
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for &(ts, ref key, ref c) in self.conflicts.iter() {
            match write!(f, "{:?} @ ts={}, key={:?}", c, ts, key) {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct TokenGenerator {
    conflicts: Vec<Conflict>,
}

impl TokenGenerator {
    pub fn new(base_table_conflicts: Vec<NodeIndex>,
               base_column_conflicts: Vec<(NodeIndex, usize)>)
               -> Self {
        TokenGenerator {
            conflicts: base_table_conflicts.into_iter()
                .map(|n| Conflict::BaseTable(n))
                .chain(base_column_conflicts.into_iter()
                    .map(|(n, c)| Conflict::BaseColumn(n, c)))
                .collect(),
        }
    }

    // Generate a token that conflicts with any write that could modify a row with the given key.
    pub fn generate(&self, ts: i64, key: DataType) -> Token {
        Token { conflicts: vec![(ts, key, self.conflicts.clone())] }
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

    // For each base node, holds a hash map from column number to a map from value to the last time
    // that a row of that value was written.
    granular: HashMap<NodeIndex, HashMap<usize, Option<HashMap<DataType, i64>>>>,
}

impl CheckTable {
    pub fn new() -> Self {
        CheckTable {
            next_timestamp: 0,
            toplevel: HashMap::new(),
            granular: HashMap::new(),
        }
    }

    // Return whether the conflict should trigger, causing the associated transaction to abort.
    fn check_conflict(&self, ts: i64, key: &DataType, conflict: &Conflict) -> bool {
        match conflict {
            &Conflict::BaseTable(node) => ts < *self.toplevel.get(&node).unwrap_or(&-1),
            &Conflict::BaseColumn(node, column) => {
                let t = self.granular.get(&node);
                if t.is_none() {
                    // If the base node has never seen a write, then don't trigger.
                    return false;
                }

                let r = None;
                let t = t.unwrap().get(&column).unwrap_or(&r);
                if let &Some(ref t) = t {
                    // Column is being tracked.
                    let t = t.get(key);
                    if t.is_none() {
                        // If the column is being tracked, and a token has been generated for a
                        // given value, but no time is present in the checktable, then there could
                        // never have been a write with that key.
                        return false;
                    }

                    ts < *t.unwrap()
                } else {
                    // If this column is not being tracked, then trigger only if there has been any
                    // write to the base node.
                    ts < *self.toplevel.get(&node).unwrap_or(&-1)
                }
            }
        }
    }

    /// Return whether a transaction with this Token should commit.
    pub fn validate_token(&self, token: &Token) -> bool {
        !token.conflicts.iter().any(|&(ts, ref key, ref conflicts)| {
            conflicts.iter().any(|ref c| self.check_conflict(ts, &key, c))
        })
    }

    pub fn claim_timestamp(&mut self,
                           token: &Token,
                           base: NodeIndex,
                           rs: &Records)
                           -> TransactionResult {
        if self.validate_token(token) {
            let ts = self.next_timestamp;
            self.next_timestamp += 1;
            self.toplevel.insert(base, ts);

            let ref mut t = self.granular.entry(base).or_insert_with(HashMap::new);
            for record in rs.iter() {
                for (i, value) in record.iter().enumerate() {
                    if let &mut Some(ref mut m) = t.entry(i).or_insert(Some(HashMap::new())) {
                        *m.entry(value.clone()).or_insert(0) = ts;
                    }
                    // TODO(jonathan): fall back to coarse checktable if granular table has gotten too large.
                }
            }

            TransactionResult::Committed(ts)
        } else {
            TransactionResult::Aborted
        }
    }

    /// Claim a pair of successive timestamps. Used by migration code to ensure
    /// that no transactions happen while a migration is in progress.
    pub fn claim_timestamp_pair(&mut self) -> (i64, i64){
        let ts = self.next_timestamp;
        self.next_timestamp += 2;
        (ts, ts+1)
    }
}
