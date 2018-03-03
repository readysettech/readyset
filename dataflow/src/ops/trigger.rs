use std::collections::HashMap;

use prelude::*;

use hyper::Client;
use tokio_core::reactor::Core;
use serde::Serialize;
use serde_json;
use std::thread;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    us: Option<IndexPair>,
    src: IndexPair,
    trigger: TriggerType,
    key: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    // url to the blender, group name
    GroupCreation{ url: String, group: String },
}

impl Trigger {
    /// Construct a new Trigger operator.
    pub fn new(src: NodeIndex, trigger: TriggerType, key: Vec<usize>) -> Trigger {
        assert_eq!(key.len(), 1);
        Trigger {
            us: None,
            src: src.into(),
            trigger: trigger,
            key: key,
        }
    }

    fn rpc<Q: Serialize>(
        &self,
        path: &str,
        requests: Vec<Q>,
        url: &String,
    ) where Q: Send {
        use hyper;
        let url = format!("{}/{}", url, path);
        let requests: Vec<hyper::Request> = requests.iter().map(|req| {
            let mut r = hyper::Request::new(hyper::Method::Post, url.clone().parse().unwrap());
            r.set_body(serde_json::to_string(&req).unwrap());
            r
        }).collect();

        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let client = Client::new(&core.handle());
            for r in requests {
                let work = client.request(r);
                core.run(work).unwrap();
            }
        });
    }

    fn trigger(&self, gids: Vec<DataType>) {
        if gids.is_empty() {
            return
        }

        match self.trigger {
            TriggerType::GroupCreation{ ref url, ref group } => {
                let contexts = gids.iter().map(|gid| {
                    let mut group_context: HashMap<String, DataType> = HashMap::new();
                    group_context.insert(String::from("id"), gid.clone());
                    group_context.insert(String::from("group"), group.clone().into());
                    group_context
                }).collect();

                self.rpc("create_universe", contexts, url);
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
        _: Option<usize>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        let us = self.us.unwrap();
        let db = state
            .get(&*us)
            .expect("trigger must have its own state materialized");

        let mut trigger_keys: Vec<DataType> = rs
            .iter()
            .map(|r| r[self.key[0]].clone())
            .collect();

        // sort and dedup to trigger just once for each key
        trigger_keys.sort();
        trigger_keys.dedup();

        let keys = trigger_keys.iter().filter_map(|k| {
            match db.lookup(&[self.key[0]], &KeyType::Single(&k)) {
                LookupResult::Some(rs) => {
                    if rs.len() == 0 {
                        Some(k)
                    } else {
                        None
                    }
                }
                LookupResult::Missing => unimplemented!(),
            }
        }).cloned().collect();

        self.trigger(keys);

        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index all key columns
        Some((this, (self.key.clone(), true))).into_iter().collect()
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

    fn must_materialize(&self) -> bool {
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
        let trigger_type = TriggerType::GroupCreation {
            url: String::from("url"),
            group: String::from("group"),
        };
        g.set_op(
            "trigger",
            &["x", "y", "z"],

            Trigger::new(s.as_global(), trigger_type, vec![0]),
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
