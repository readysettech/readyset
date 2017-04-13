use std::collections::HashMap;
use vec_map::VecMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Clone)]
pub struct Base {
    primary_key: Option<Vec<usize>>,
    defaults: Vec<DataType>,
    dropped: Vec<usize>,
    us: Option<NodeAddress>,
    unmodified: bool,
}

impl Base {
    /// Create a base node operator.
    pub fn new(defaults: Vec<DataType>) -> Self {
        Base {
            primary_key: None,
            us: None,

            defaults: defaults,
            dropped: Vec::new(),
            unmodified: true,
        }
    }

    /// Create a base node operator with a known primary key.
    pub fn with_key(primary_key: Vec<usize>, defaults: Vec<DataType>) -> Self {
        Base {
            primary_key: Some(primary_key),
            us: None,

            defaults: defaults,
            dropped: Vec::new(),
            unmodified: true,
        }
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
                "cannot add columns to base nodes without setting default values for initial columns");
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
}

impl Default for Base {
    fn default() -> Self {
        Base::new(vec![])
    }
}

#[cfg(test)]
impl Drop for Base {
    fn drop(&mut self) {
        //println!("Dropping Base!");
    }
}

use flow::prelude::*;

impl Ingredient for Base {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
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
                _: &DomainNodes,
                state: &StateMap)
                -> ProcessingResult {
        let rs = rs.into_iter()
            .map(|r| {
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
            rs.collect()
        } else {
            rs.map(|r| {
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
