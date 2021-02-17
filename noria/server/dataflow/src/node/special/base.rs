use crate::prelude::*;
use maplit::hashmap;
use noria::{Modification, Operation, TableOperation};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use vec_map::VecMap;

/// Base is used to represent the root nodes of the Noria data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Serialize, Deserialize)]
pub struct Base {
    primary_key: Option<Vec<usize>>,

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

    pub(crate) fn fix(&self, row: &mut Vec<DataType>) {
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

            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
        }
    }
}

fn key_val(i: usize, col: usize, r: &TableOperation) -> &DataType {
    match *r {
        TableOperation::Insert(ref row) => &row[col],
        TableOperation::Delete { ref key } => &key[i],
        TableOperation::Update { ref key, .. } => &key[i],
        TableOperation::InsertOrUpdate { ref row, .. } => &row[col],
    }
}

fn key_of<'a>(key_cols: &'a [usize], r: &'a TableOperation) -> impl Iterator<Item = &'a DataType> {
    key_cols
        .iter()
        .enumerate()
        .map(move |(i, col)| key_val(i, *col, r))
}

impl Base {
    pub(in crate::node) fn take(&mut self) -> Self {
        Clone::clone(self)
    }

    pub(in crate::node) fn process(
        &mut self,
        us: LocalNodeIndex,
        mut ops: Vec<TableOperation>,
        state: &StateMap,
    ) -> Records {
        if self.primary_key.is_none() || ops.is_empty() {
            return ops
                .into_iter()
                .map(|r| {
                    if let TableOperation::Insert(mut r) = r {
                        self.fix(&mut r);
                        Record::Positive(r)
                    } else {
                        unreachable!("unkeyed base got non-insert operation {:?}", r);
                    }
                })
                .collect();
        }

        let key_cols = &self.primary_key.as_ref().unwrap()[..];
        ops.sort_by(|a, b| key_of(key_cols, a).cmp(key_of(key_cols, b)));

        // starting key
        let mut this_key: Vec<_> = key_of(key_cols, &ops[0]).cloned().collect();

        // starting record state
        let db = state
            .get(us)
            .expect("base with primary key must be materialized");

        let get_current = |current_key: &'_ _| {
            match db.lookup(key_cols, &KeyType::from(current_key)) {
                LookupResult::Some(rows) => {
                    match rows.len() {
                        0 => None,
                        1 => rows.into_iter().next(),
                        n => {
                            // primary key, so better be unique!
                            assert_eq!(n, 1, "key {:?} not unique (n = {})!", current_key, n);
                            unreachable!();
                        }
                    }
                }
                LookupResult::Missing => unreachable!(),
            }
        };
        let mut current = get_current(&this_key);
        let mut was = current.clone();

        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            if this_key.iter().cmp(key_of(key_cols, &op)) != Ordering::Equal {
                if current != was {
                    if let Some(was) = was {
                        results.push(Record::Negative(was.into_owned()));
                    }
                    if let Some(current) = current {
                        results.push(Record::Positive(current.into_owned()));
                    }
                }

                this_key = key_of(key_cols, &op).cloned().collect();
                current = get_current(&this_key);
                was = current.clone();
            }

            let update = match op {
                TableOperation::Insert(row) => {
                    if let Some(ref was) = was {
                        eprintln!("base ignoring {:?} since it already has {:?}", row, was);
                    } else {
                        //assert!(was.is_none());
                        current = Some(Cow::Owned(row));
                    }
                    continue;
                }
                TableOperation::Delete { .. } => {
                    if current.is_some() {
                        current = None;
                    } else {
                        // supposed to delete a non-existing row?
                        // TODO: warn?
                    }
                    continue;
                }
                TableOperation::Update { set, .. } => set,
                TableOperation::InsertOrUpdate { row, update } => {
                    if current.is_none() {
                        current = Some(Cow::Owned(row));
                        continue;
                    }
                    update
                }
            };

            if current.is_none() {
                // supposed to update a non-existing row?
                // TODO: also warn here?
                continue;
            }

            let mut future = current.unwrap().into_owned();
            for (col, op) in update.into_iter().enumerate() {
                // XXX: make sure user doesn't update primary key?
                match op {
                    Modification::Set(v) => future[col] = v,
                    Modification::Apply(op, v) => {
                        let old: i128 = future[col].clone().into();
                        let delta: i128 = v.into();
                        future[col] = match op {
                            Operation::Add => (old + delta).into(),
                            Operation::Sub => (old - delta).into(),
                        };
                    }
                    Modification::None => {}
                }
            }
            current = Some(Cow::Owned(future));
        }

        // we may have changed things in the last iteration of the loop above
        if current != was {
            if let Some(was) = was {
                results.push(Record::Negative(was.into_owned()));
            }
            if let Some(current) = current {
                results.push(Record::Positive(current.into_owned()));
            }
        }

        for r in &mut results {
            self.fix(r);
        }

        results.into()
    }

    pub(in crate::node) fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, Index> {
        if let Some(ref pk) = self.primary_key {
            hashmap! {
                n => Index::hash_map(pk.clone())
            }
        } else {
            HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();

        assert!(b.primary_key.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![]);

        assert!(b.primary_key.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    fn test_lots_of_changes_in_same_batch(mut state: Box<dyn State>) {
        use crate::node;
        use crate::prelude::*;

        // most of this is from MockGraph
        let mut graph = Graph::new();
        let source = graph.add_node(Node::new(
            "source",
            &["because-type-inference"],
            node::NodeType::Source,
        ));

        let b = Base::new(vec![]).with_key(vec![0, 2]);
        let global = graph.add_node(Node::new("b", &["x", "y", "z"], b));
        graph.add_edge(source, global, ());
        let local = unsafe { LocalNodeIndex::make(0 as u32) };
        let mut ip: IndexPair = global.into();
        ip.set_local(local);
        graph
            .node_weight_mut(global)
            .unwrap()
            .set_finalized_addr(ip);

        let mut remap = HashMap::new();
        remap.insert(global, ip);
        graph.node_weight_mut(global).unwrap().on_commit(&remap);
        graph.node_weight_mut(global).unwrap().add_to(0.into());

        for (_, index) in graph[global].suggest_indexes(global) {
            state.add_key(&index, None);
        }

        let mut states = StateMap::new();
        states.insert(local, state);
        let n = graph[global].take();
        let mut n = n.finalize(&graph);

        let mut one = move |u: Vec<TableOperation>| {
            let mut m = n.get_base_mut().unwrap().process(local, u, &states);
            node::materialize(&mut m, None, states.get_mut(local));
            m
        };

        assert_eq!(
            one(vec![
                TableOperation::Insert(vec![1.into(), "a".into(), 1.into()]),
                TableOperation::Insert(vec![2.into(), "2a".into(), 1.into()]),
                TableOperation::Delete {
                    key: vec![1.into(), 1.into()],
                },
                TableOperation::Insert(vec![1.into(), "b".into(), 1.into()]),
                TableOperation::InsertOrUpdate {
                    row: vec![1.into(), "c".into(), 1.into()],
                    update: vec![
                        Modification::None,
                        Modification::Set("never".into()),
                        Modification::None,
                    ],
                },
                TableOperation::InsertOrUpdate {
                    row: vec![1.into(), "also never".into(), 1.into()],
                    update: vec![
                        Modification::None,
                        Modification::Set("d".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Update {
                    key: vec![1.into(), 1.into()],
                    set: vec![
                        Modification::None,
                        Modification::Set("e".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Update {
                    key: vec![2.into(), 1.into()],
                    set: vec![
                        Modification::None,
                        Modification::Set("2x".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Delete {
                    key: vec![1.into(), 1.into()],
                },
                TableOperation::Delete {
                    key: vec![2.into(), 1.into()],
                },
            ]),
            Records::default()
        );
    }

    #[test]
    fn lots_of_changes_in_same_batch() {
        let state = MemoryState::default();
        test_lots_of_changes_in_same_batch(Box::new(state));
    }

    #[test]
    fn lots_of_changes_in_same_batch_persistent() {
        let state = PersistentState::new(
            String::from("lots_of_changes_in_same_batch_persistent"),
            None,
            &PersistenceParameters::default(),
        );

        test_lots_of_changes_in_same_batch(Box::new(state));
    }
}
