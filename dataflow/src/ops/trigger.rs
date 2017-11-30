use std::collections::HashMap;

use prelude::*;

use hyper::Client;
use tokio_core::reactor::Core;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use std::error::Error;
use futures::{Future, Stream};

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    src: IndexPair,
    url: String,
}

impl Trigger {
    /// Construct a new Trigger operator.
    pub fn new(src: NodeIndex, url: String) -> Trigger {
        Trigger { src: src.into(), url: url }
    }

    fn rpc<Q: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        request: &Q,
    ) -> Result<R, Box<Error>> {
        use hyper;

        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let url = format!("{}/{}", self.url, path);

        let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
        r.set_body(serde_json::to_string(request).unwrap());

        let work = client.request(r).and_then(|res| {
            res.body().concat2().and_then(move |body| {
                let reply: R = serde_json::from_slice(&body)
                    .map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::Other, e))
                    .unwrap();
                Ok(reply)
            })
        });
        Ok(core.run(work).unwrap())
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
        _: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
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
        "≡".into()
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
