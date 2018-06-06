use std::collections::HashMap;

use prelude::*;

use hyper::Client;
use serde::Serialize;
use serde_json;
use std::thread;
use tokio;

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
    GroupCreation {
        controller_url: String,
        group: String,
    },
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
            trigger: trigger,
            key: key,
        }
    }

    fn rpc<Q: Serialize>(&self, path: &str, requests: Vec<Q>, url: &String)
    where
        Q: Send,
    {
        use hyper;
        let url = format!("{}/{}", url, path);
        let requests: Vec<_> = requests
            .iter()
            .map(|req| {
                hyper::Request::post(&url)
                    .body(hyper::Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap()
            })
            .collect();

        // TODO: instead of spawing a thred for each request, we could have a
        // long running thread that we just send requests to.
        thread::spawn(move || {
            let client = Client::new();
            let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
            for r in requests {
                rt.block_on(client.request(r)).unwrap();
            }
        });
    }

    fn trigger(&self, ids: Vec<DataType>) {
        if ids.is_empty() {
            return;
        }

        match self.trigger {
            TriggerEvent::GroupCreation {
                ref controller_url,
                ref group,
            } => {
                let contexts = ids
                    .iter()
                    .map(|gid| {
                        let mut group_context: HashMap<String, DataType> = HashMap::new();
                        group_context.insert(String::from("id"), gid.clone());
                        group_context.insert(String::from("group"), group.clone().into());
                        group_context
                    })
                    .collect();

                self.rpc("create_universe", contexts, controller_url);
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
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        let us = self.us.unwrap();
        let db = state
            .get(&*us)
            .expect("trigger must have its own state materialized");

        let mut trigger_keys: Vec<DataType> = rs.iter().map(|r| r[self.key].clone()).collect();

        // sort and dedup to trigger just once for each key
        trigger_keys.sort();
        trigger_keys.dedup();

        let keys = trigger_keys
            .iter()
            .filter_map(|k| match db.lookup(&[self.key], &KeyType::Single(&k)) {
                LookupResult::Some(rs) => {
                    if rs.len() == 0 {
                        Some(k)
                    } else {
                        None
                    }
                }
                LookupResult::Missing => unimplemented!(),
            })
            .cloned()
            .collect();

        self.trigger(keys);

        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index all key columns
        Some((this, (vec![self.key], true))).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self) -> String {
        "T".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
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

    use ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        let trigger_type = TriggerEvent::GroupCreation {
            controller_url: String::from("localhost"),
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

        let left: Vec<DataType> = vec![1.into(), "a".into()];
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
