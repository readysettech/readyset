//! # Check Tables
//!
//! Check Tables are used by the transaction subsystem to ensure that multiple conflicting
//! transactions do not all commit. They work by tracking the last timestamp that a write occurred
//! to a specific region.

use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;

use flow::domain;
use flow::prelude::*;
use flow::migrate::materialization::Tag as ReplayPath;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum Conflict {
    /// This conflict should trigger an abort if the given base table has seen a write after the
    /// given time.
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

    /// Get the latest timestamp associated with this Token.
    pub fn get_timestamp(&self) -> i64 {
        self.conflicts.iter().map(|c| c.0).max().unwrap_or(-1)
    }

    /// Convert to a token that only relies on coarse checktables.
    pub fn make_coarse(&mut self) {
        for &mut (_, _, ref mut vc) in self.conflicts.iter_mut() {
            for c in vc.iter_mut() {
                match *c {
                    Conflict::BaseTable(..) => {}
                    Conflict::BaseColumn(n, _) => *c = Conflict::BaseTable(n),
                }
            }
        }
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for &(ts, ref key, ref c) in &self.conflicts {
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
            conflicts: base_table_conflicts
                .into_iter()
                .map(Conflict::BaseTable)
                .chain(base_column_conflicts
                           .into_iter()
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
    Committed(i64, Option<HashMap<domain::Index, i64>>),
    /// The transaction aborted
    Aborted,
}

impl TransactionResult {
    /// Checks if a transaction committed.
    pub fn ok(&self) -> bool {
        if let TransactionResult::Committed(..) = *self {
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

    // For each base node, holds a hash map from column number to a tuple. First element is a map
    // from value to the last time that a row of that value was written. Second element is the time
    // the column started being tracked.
    granular: HashMap<NodeIndex, HashMap<usize, (HashMap<DataType, i64>, i64)>>,

    // For each domain, stores the set of base nodes that it receives updates from.
    domain_dependencies: HashMap<domain::Index, Vec<NodeIndex>>,

    // Domains impacted by each replay path.
    replay_paths: HashMap<ReplayPath, Vec<domain::Index>>,

    last_replay: HashMap<domain::Index, i64>,
    last_migration: Option<i64>,
    last_base: Option<NodeIndex>,
}

impl CheckTable {
    pub fn new() -> Self {
        CheckTable {
            next_timestamp: 0,
            toplevel: HashMap::new(),
            granular: HashMap::new(),
            domain_dependencies: HashMap::new(),
            replay_paths: HashMap::new(),
            last_replay: HashMap::new(),
            last_migration: None,
            last_base: None,
        }
    }

    // Return whether the conflict should trigger, causing the associated transaction to abort.
    fn check_conflict(&self, ts: i64, key: &DataType, conflict: &Conflict) -> bool {
        match *conflict {
            Conflict::BaseTable(node) => ts < *self.toplevel.get(&node).unwrap_or(&-1),
            Conflict::BaseColumn(node, column) => {
                let t = self.granular.get(&node);
                if t.is_none() {
                    // If the base node has never seen a write, then don't trigger.
                    return false;
                }

                if let Some(&(ref t, ref start_time)) = (*t.unwrap()).get(&column) {
                    // Column is being tracked.
                    match t.get(key) {
                        None => ts < *start_time,
                        Some(update_time) => ts < *update_time,
                    }
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
        !token
             .conflicts
             .iter()
             .any(|&(ts, ref key, ref conflicts)| {
                      conflicts.iter().any(|c| self.check_conflict(ts, key, c))
                  })
    }

    fn compute_previous_timestamps(&self,
                                   base: Option<NodeIndex>)
                                   -> Option<HashMap<domain::Index, i64>> {
        if self.last_base.is_some() && self.last_base == base {
            return None;
        }

        Some(self.domain_dependencies
                 .iter()
                 .map(|(d, v)| {
            let earliest: i64 = v.iter()
                .filter_map(|b| self.toplevel.get(b))
                .chain(self.last_migration.iter())
                .chain(self.last_replay.get(d).iter().map(|t| *t))
                .max()
                .cloned()
                .unwrap_or(0);
            (*d, earliest)
        })
                 .collect())
    }

    pub fn attempt_claim_timestamp(&mut self,
                                   token: &Token,
                                   base: NodeIndex,
                                   rs: &Records)
                                   -> TransactionResult {
        if self.validate_token(token) {
            let (ts, prevs) = self.claim_timestamp(base, rs);
            TransactionResult::Committed(ts, prevs)
        } else {
            TransactionResult::Aborted
        }
    }

    pub fn claim_timestamp(&mut self,
                           base: NodeIndex,
                           rs: &Records)
                           -> (i64, Option<HashMap<domain::Index, i64>>) {
        // Take timestamp
        let ts = self.next_timestamp;
        self.next_timestamp += 1;

        // Compute the previous timestamp that each domain will see before getting this one
        let prev_times = self.compute_previous_timestamps(Some(base));

        // Update checktables
        self.last_base = Some(base);
        self.toplevel.insert(base, ts);
        let t = &mut self.granular.entry(base).or_insert_with(HashMap::new);
        for record in rs.iter() {
            for (i, value) in record.iter().enumerate() {
                let mut delete = false;
                if let Some(&mut (ref mut m, _)) = t.get_mut(&i) {
                    if m.len() > 10000000 {
                        delete = true;
                    } else {
                        *m.entry(value.clone()).or_insert(0) = ts;
                    }
                }
                if delete {
                    t.remove(&i);
                }
            }
        }
        (ts, prev_times)
    }

    pub fn claim_replay_timestamp(&mut self,
                                  path: &ReplayPath)
                                  -> (i64, Option<HashMap<domain::Index, i64>>) {
        // Take timestamp
        let ts = self.next_timestamp;
        self.next_timestamp += 1;

        // Compute the previous timestamp that each domain will see before getting this one
        let prevs = self.compute_previous_timestamps(None);

        // Update checktable state
        self.last_base = None;
        for d in self.replay_paths.get(path).unwrap() {
            self.last_replay.insert(d.clone(), ts);
        }

        (ts, prevs)
    }

    /// Transition to using `new_domain_dependencies`, and reserve a pair of
    /// timestamps for the migration to happen between.
    pub fn perform_migration(&mut self,
                             ingresses_from_base: &HashMap<domain::Index,
                                                           HashMap<NodeIndex, usize>>)
                             -> (i64, i64, Option<HashMap<domain::Index, i64>>) {
        let ts = self.next_timestamp;
        let prevs = self.compute_previous_timestamps(None);

        self.last_base = None;
        self.next_timestamp += 2;
        self.last_migration = Some(ts + 1);
        self.domain_dependencies = ingresses_from_base
            .iter()
            .map(|(domain, ingress_from_base)| {
                     (*domain,
                      ingress_from_base
                          .iter()
                          .filter(|&(_, n)| *n > 0)
                          .map(|(k, _)| *k)
                          .collect())
                 })
            .collect();

        (ts, ts + 1, prevs)
    }

    pub fn add_replay_paths(&mut self,
                            additional_replay_paths: HashMap<ReplayPath, Vec<domain::Index>>) {
        self.replay_paths
            .extend(additional_replay_paths.into_iter());
    }

    pub fn track(&mut self, gen: &TokenGenerator) {
        for conflict in &gen.conflicts {
            match *conflict {
                Conflict::BaseTable(..) => {}
                Conflict::BaseColumn(base, col) => {
                    let t = &mut self.granular.entry(base).or_insert_with(HashMap::new);
                    t.entry(col)
                        .or_insert((HashMap::new(), self.next_timestamp - 1));
                }
            }
        }
    }
}
