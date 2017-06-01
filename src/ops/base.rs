use std::collections::HashMap;
use vec_map::VecMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base {
    durability: Option<BaseDurabilityLevel>,
    primary_key: Option<Vec<usize>>,

    us: Option<NodeAddress>,

    defaults: Vec<DataType>,
    dropped: Vec<usize>,
    unmodified: bool,
}

/// Specifies the level of durability that this base node should offer. Stronger guarantees imply a
/// reduced write performance.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BaseDurabilityLevel {
    /// Buffered writes: records are accumulated in an in-memory buffer and occasionally flushed to
    /// the durable log, which may itself buffer in the file system. Results in large batched
    /// writes, but offers no durability guarantees on crashes.
    Buffered,
    /// Synchronous writes: forces every record to be written to disk before it is emitted further
    /// into the data-flow graph. Strong guarantees (writes are never lost), but high performance
    /// penalty.
    SyncImmediately,
}

impl Base {
    /// Create a non-durable base node operator.
    pub fn new(defaults: Vec<DataType>) -> Self {
        let mut base = Base::default();
        base.defaults = defaults;
        base
    }

    /// Builder with a known primary key.
    pub fn with_key(mut self, primary_key: Vec<usize>) -> Base {
        self.primary_key = Some(primary_key);
        self
    }

    /// Builder with a durability level.
    pub fn with_durability(mut self, durability: BaseDurabilityLevel) -> Base {
        self.durability = Some(durability);
        self
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DataType) -> usize {
        assert!(!self.defaults.is_empty(),
                "cannot add columns to base nodes without\
                setting default values for initial columns");
        self.defaults.push(default);
        self.unmodified = false;
        self.defaults.len() - 1
    }

    /// Drop a column from this base node.
    pub fn drop_column(&mut self, column: usize) {
        assert!(!self.defaults.is_empty(),
                "cannot add columns to base nodes without\
                setting default values for initial columns");
        assert!(column < self.defaults.len());
        self.unmodified = false;

        // note that we don't need to *do* anything for dropped columns when we receive records.
        // the only thing that matters is that new Mutators remember to inject default values for
        // dropped columns.
        self.dropped.push(column);
    }

    pub(crate) fn get_dropped(&self) -> VecMap<DataType> {
        self.dropped
            .iter()
            .map(|&col| (col, self.defaults[col].clone()))
            .collect()
    }

    pub(crate) fn is_unmodified(&self) -> bool {
        self.unmodified
    }

    /// Whether this base node should delete its durable log on drop.  Used when durable log is not
    /// intended to be used for future recovery, e.g., on tests.
    pub fn delete_log_on_drop(mut self) -> Base {
        self
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            durability: self.durability,
            primary_key: self.primary_key.clone(),
            us: self.us,

            defaults: self.defaults.clone(),
            dropped: self.dropped.clone(),
            unmodified: self.unmodified,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            durability: None,
            primary_key: None,
            us: None,

            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
        }
    }
}

use flow::prelude::*;

impl Ingredient for Base {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized && self.primary_key.is_some()
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeAddress, _: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
    }

    fn on_input(&mut self,
                _: NodeAddress,
                rs: Records,
                _: &mut Tracer,
                _: &DomainNodes,
                state: &StateMap)
                -> ProcessingResult {
        let results = rs.into_iter().map(|r| {
            //rustfmt
            match r {
                Record::Positive(u) => Record::Positive(u),
                Record::Negative(u) => Record::Negative(u),
                Record::DeleteRequest(key) => {
                    let cols = self.primary_key
                        .as_ref()
                        .expect("base must have a primary key to support deletions");
                    let db =
                    state.get(self.us
                                  .as_ref()
                                  .unwrap()
                                  .as_local())
                        .expect("base must have its own state materialized to support deletions");

                    match db.lookup(cols.as_slice(), &KeyType::from(&key[..])) {
                        LookupResult::Some(rows) => {
                            assert_eq!(rows.len(), 1);
                            Record::Negative(rows[0].clone())
                        }
                        LookupResult::Missing => unreachable!(),
                    }
                }
            }
        });

        let rs = if self.unmodified {
            results.collect()
        } else {
            results
                .map(|r| {
                    //rustfmt
                    if r.len() != self.defaults.len() {
                        let rlen = r.len();
                        let (mut v, pos) = r.extract();

                        use std::sync::Arc;
                        if let Some(mut v) = Arc::get_mut(&mut v) {
                            v.extend(self.defaults.iter().skip(rlen).cloned());
                        }

                        // the trick above failed, probably because we're doing a replay
                        if v.len() == rlen {
                            let newv = v.iter()
                                .cloned()
                                .chain(self.defaults.iter().skip(rlen).cloned())
                                .collect();
                            v = Arc::new(newv)
                        }

                        (v, pos).into()
                    } else {
                        r
                    }
                })
                .collect()
        };

        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, n: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        if self.primary_key.is_some() {
            Some((n, self.primary_key.as_ref().unwrap().clone()))
                .into_iter()
                .collect()
        } else {
            HashMap::new()
        }
    }

    fn resolve(&self, _: usize) -> Option<Vec<(NodeAddress, usize)>> {
        None
    }

    fn get_base(&self) -> Option<&Base> {
        Some(self)
    }

    fn get_base_mut(&mut self) -> Option<&mut Base> {
        Some(self)
    }

    fn description(&self) -> String {
        "B".into()
    }

    fn parent_columns(&self, _: usize) -> Vec<(NodeAddress, Option<usize>)> {
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();

        assert!(b.durability.is_none());
        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![]);

        assert!(b.durability.is_none());
        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_durability_buffered() {
        let b = Base::new(vec![]).with_durability(BaseDurabilityLevel::Buffered);

        assert_eq!(b.durability, Some(BaseDurabilityLevel::Buffered));
        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_durability_sync_immediately() {
        let b = Base::new(vec![]).with_durability(BaseDurabilityLevel::SyncImmediately);

        assert_eq!(b.durability, Some(BaseDurabilityLevel::SyncImmediately));
        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }
}
