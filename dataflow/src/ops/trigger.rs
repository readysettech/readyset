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
    src: IndexPair,
    trigger: TriggerType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    // url to the blender, group name
    GroupCreation{ url: String, group: String },
}

impl Trigger {
    /// Construct a new Trigger operator.
    pub fn new(src: NodeIndex, trigger: TriggerType) -> Trigger {
        Trigger { src: src.into(), trigger: trigger }
    }

    fn rpc<Q: Serialize>(
        &self,
        path: &str,
        request: Q,
        url: &String,
    ) where Q: Send {
        use hyper;
        let url = format!("{}/{}", url, path);
        let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
        r.set_body(serde_json::to_string(&request).unwrap());

        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let client = Client::new(&core.handle());
            let work = client.request(r);

            core.run(work).unwrap();
        });
    }

    fn trigger(&self, row: &Record) {
        match self.trigger {
            TriggerType::GroupCreation{ ref url, ref group } => {
                let gid = match *row {
                    Record::Positive(ref v) => v[1].clone(),
                    _ => return,
                };

                let mut group_context: HashMap<String, DataType> = HashMap::new();
                group_context.insert(String::from("id"), gid);
                group_context.insert(String::from("group"), group.clone().into());

                self.rpc("create_universe", group_context, url);
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

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        for r in rs.iter() {
            self.trigger(r);
        }

        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        HashMap::new()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "trigger",
            &["x", "y", "z"],
            Trigger::new(s.as_global()),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false);

        let left: Vec<DataType> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
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
