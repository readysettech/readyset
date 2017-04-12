use std::collections::HashMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Clone)]
pub struct Base {
    primary_key: Option<Vec<usize>>,
    defaults: Vec<DataType>,
    us: Option<NodeAddress>,
    unmodified: bool,
}

impl Base {
    /// Create a base node operator.
    pub fn new(defaults: Vec<DataType>) -> Self {
        Base {
            primary_key: None,
            defaults: defaults,
            us: None,
            unmodified: true,
        }
    }

    /// Create a base node operator with a known primary key.
    pub fn with_key(primary_key: Vec<usize>, defaults: Vec<DataType>) -> Self {
        Base {
            primary_key: Some(primary_key),
            defaults: defaults,
            us: None,
            unmodified: true,
        }
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DataType) -> usize {
        self.defaults.push(default);
        self.unmodified = false;
        self.defaults.len() - 1
    }

    pub(crate) fn is_unmodified(&self) -> bool {
        self.unmodified
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
        let rs = rs.into_iter().map(|r| {
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
                        let (v, pos) = r.extract();

                        use std::sync::Arc;
                        let mut v =
                            Arc::try_unwrap(v).expect("base nodes should be only initial owner");
                        v.extend(self.defaults
                                     .iter()
                                     .skip(rlen)
                                     .cloned());
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
            Some((n,
                  self.primary_key
                      .as_ref()
                      .unwrap()
                      .clone()))
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
