use maplit::hashmap;

use crate::prelude::*;
use crate::processing::ColumnSource;
use crate::processing::SuggestedIndex;
use noria::errors::{internal_err, ReadySetResult};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use std::convert::TryInto;

/// A Trigger data-flow operator.
///
/// This node triggers an event in the dataflow graph whenever a
/// new `key` arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    us: Option<IndexPair>,
    src: IndexPair,
    trigger: TriggerEvent,
    key: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerEvent {
    /// Triggers the creation of a new group universe.
    GroupCreation { group: String },
}

impl Trigger {
    /// Construct a new Trigger operator.
    ///
    /// `src` is the parent node from which this node receives records.
    /// Whenever this node receives a record with a new value for `key`,
    /// it triggers the event specified by `trigger`
    pub fn new(src: NodeIndex, trigger: TriggerEvent, key: usize) -> Trigger {
        Trigger {
            us: None,
            src: src.into(),
            trigger,
            key,
        }
    }

    fn create_universes<I>(&self, executor: &mut dyn Executor, requests: I)
    where
        I: IntoIterator<Item = HashMap<String, DataType>>,
    {
        for req in requests {
            executor.create_universe(req);
        }
    }

    fn trigger(&self, executor: &mut dyn Executor, ids: Vec<DataType>) {
        if ids.is_empty() {
            return;
        }

        match self.trigger {
            TriggerEvent::GroupCreation { ref group } => {
                let mut requests = Vec::new();
                for gid in ids.iter() {
                    let mut group_context: HashMap<String, DataType> = HashMap::new();
                    group_context.insert(String::from("id"), gid.clone());
                    group_context.insert(String::from("group"), group.clone().into());
                    requests.push(group_context);
                }

                self.create_universes(executor, requests);
            }
        }
    }
}

impl Ingredient for Trigger {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        executor: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        let us = self.us.unwrap();
        let db = state
            .get(*us)
            .ok_or_else(|| internal_err("trigger must have its own state materialized"))?;

        let mut trigger_keys: Vec<DataType> = rs.iter().map(|r| r[self.key].clone()).collect();

        // sort and dedup to trigger just once for each key
        trigger_keys.sort();
        trigger_keys.dedup();

        let keys = trigger_keys
            .iter()
            // FIXME(eta): should return internal!() if Missing
            .filter(|k| {
                matches!(db.lookup(&[self.key], &KeyType::Single(k)),
                    LookupResult::Some(rs) if rs.is_empty()
                )
            })
            .cloned()
            .collect();

        self.trigger(executor, keys);

        Ok(ProcessingResult {
            results: rs,
            ..Default::default()
        })
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, SuggestedIndex> {
        // index all key columns
        hashmap! {
            this => SuggestedIndex::Strict(Index::hash_map(vec![self.key]))
        }
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        ColumnSource::exact_copy(self.src.as_global(), cols.try_into().unwrap())
    }

    fn description(&self, _: bool) -> String {
        "T".into()
    }

    // Trigger nodes require full materialization because we want group universes
    // to be long lived and to exist even if no user makes use of it.
    // We do this for two reasons: 1) to make user universe creation faster and
    // 2) so we don't have to order group and user universe migrations.
    fn requires_full_materialization(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        let trigger_type = TriggerEvent::GroupCreation {
            group: String::from("group"),
        };
        g.set_op(
            "trigger",
            &["x", "y", "z"],
            Trigger::new(s.as_global(), trigger_type, 0),
            materialized,
        );
        g
    }

    #[test]
    #[ignore]
    fn it_forwards() {
        let mut g = setup(true);

        let left: Vec<DataType> = vec![1.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }
}
