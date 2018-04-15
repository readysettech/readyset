use std::collections::HashMap;

use vec_map::VecMap;

use prelude::*;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Serialize, Deserialize)]
pub struct Base {
    primary_key: Option<Vec<usize>>,
    us: Option<IndexPair>,

    defaults: Vec<DataType>,
    dropped: Vec<usize>,
    unmodified: bool,
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

    pub fn key(&self) -> Option<&[usize]> {
        self.primary_key.as_ref().map(|cols| &cols[..])
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DataType) -> usize {
        assert!(
            !self.defaults.is_empty(),
            "cannot add columns to base nodes without\
             setting default values for initial columns"
        );
        self.defaults.push(default);
        self.unmodified = false;
        self.defaults.len() - 1
    }

    /// Drop a column from this base node.
    pub fn drop_column(&mut self, column: usize) {
        assert!(
            !self.defaults.is_empty(),
            "cannot add columns to base nodes without\
             setting default values for initial columns"
        );
        assert!(column < self.defaults.len());
        self.unmodified = false;

        // note that we don't need to *do* anything for dropped columns when we receive records.
        // the only thing that matters is that new Mutators remember to inject default values for
        // dropped columns.
        self.dropped.push(column);
    }

    pub fn get_dropped(&self) -> VecMap<DataType> {
        self.dropped
            .iter()
            .map(|&col| (col, self.defaults[col].clone()))
            .collect()
    }

    pub fn fix(&self, row: &mut Record) {
        if self.unmodified {
            return;
        }

        if row.len() != self.defaults.len() {
            let rlen = row.len();
            row.extend(self.defaults.iter().skip(rlen).cloned());
        }
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
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
            primary_key: None,
            us: None,

            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
        }
    }
}

impl Ingredient for Base {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        _: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        let mut results = Vec::with_capacity(rs.len());
        for r in rs {
            match r {
                Record::Positive(u) => {
                    if let Some(ref key) = self.primary_key {
                        let cols = self.primary_key.as_ref().unwrap();
                        let db = state
                            .get(&*self.us.unwrap())
                            .expect("base with primary key must be materialized");

                        let keyval: Vec<DataType> = key.iter().map(|kc| u[*kc].clone()).collect();
                        match db.lookup(cols.as_slice(), &KeyType::from(&keyval[..])) {
                            LookupResult::Some(rows) => {
                                if rows.is_empty() {
                                    results.push(Record::Positive(u));
                                }
                            }
                            LookupResult::Missing => unreachable!(), // base can't be partial
                        }
                    } else {
                        results.push(Record::Positive(u));
                    }
                }
                Record::Negative(u) => results.push(Record::Negative(u)),
                Record::BaseOperation(op) => {
                    let cols = self.primary_key
                        .as_ref()
                        .expect("base must have a primary key to support deletions");
                    let db = state
                        .get(&*self.us.unwrap())
                        .expect("base must have its own state materialized to support deletions");

                    let current = {
                        let key = match op {
                            BaseOperation::Delete { ref key } => KeyType::from(&key[..]),
                            BaseOperation::Update { ref key, .. } => KeyType::from(&key[..]),
                            BaseOperation::InsertOrUpdate { ref row, .. } => {
                                KeyType::from(cols.iter().map(|&c| &row[c]))
                            }
                        };

                        match db.lookup(cols.as_slice(), &key) {
                            LookupResult::Some(rows) => {
                                match rows.len() {
                                    0 => None,
                                    1 => Some((*rows[0]).clone()),
                                    n => {
                                        // primary key, so better be unique!
                                        assert_eq!(n, 1, "key {:?} not unique!", key);
                                        unreachable!();
                                    }
                                }
                            }
                            LookupResult::Missing => unreachable!(),
                        }
                    };

                    let update = match op {
                        BaseOperation::Delete { .. } => {
                            if let Some(r) = current {
                                results.push(Record::Negative(r));
                            } else {
                                // TODO: warn?
                            }
                            continue;
                        }
                        BaseOperation::Update { set, .. } => set,
                        BaseOperation::InsertOrUpdate { row, update } => {
                            if current.is_none() {
                                results.push(Record::Positive(row));
                                continue;
                            }
                            update
                        }
                    };

                    if current.is_none() {
                        // TODO: also warn here?
                        continue;
                    }
                    let mut current = current.unwrap();
                    results.push(Record::Negative(current.clone()));

                    for (col, op) in update.into_iter().enumerate() {
                        // XXX: make sure user doesn't update primary key?
                        match op {
                            Modification::Set(v) => current[col] = v,
                            Modification::Apply(op, v) => {
                                let old: i64 = current[col].clone().into();
                                let delta: i64 = v.into();
                                current[col] = match op {
                                    Operation::Add => (old + delta).into(),
                                    Operation::Sub => (old - delta).into(),
                                };
                            }
                            Modification::None => {}
                        }
                    }
                    results.push(Record::Positive(current));
                }
            }
        }

        for r in &mut results {
            self.fix(r);
        }

        ProcessingResult {
            results: results.into(),
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        if self.primary_key.is_some() {
            Some((n, (self.primary_key.as_ref().unwrap().clone(), true)))
                .into_iter()
                .collect()
        } else {
            HashMap::new()
        }
    }

    fn resolve(&self, _: usize) -> Option<Vec<(NodeIndex, usize)>> {
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

    fn parent_columns(&self, _: usize) -> Vec<(NodeIndex, Option<usize>)> {
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();

        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![]);

        assert!(b.primary_key.is_none());
        assert!(b.us.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }
}
