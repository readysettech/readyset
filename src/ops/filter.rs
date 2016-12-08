use std::collections::HashMap;

use flow::prelude::*;
use query::DataType;

/// Filters incoming records according to some filter.
#[derive(Debug)]
pub struct Filter {
    src: NodeAddress,
    filter: Vec<Option<DataType>>,
}

impl Filter {
    /// Construct a new identity operator.
    pub fn new(src: NodeAddress, filter: &[Option<DataType>]) -> Filter {
        Filter {
            src: src,
            filter: Vec::from(filter),
        }
    }
}

impl Ingredient for Filter {
    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[*self.src.as_global()];
        assert_eq!(self.filter.len(), srcn.fields().len());
    }

    fn on_commit(&mut self, _: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        self.src = remap[&self.src];
    }

    fn on_input(&mut self, mut input: Message, _: &DomainNodes, _: &StateMap) -> Option<Update> {
        let mut f = self.filter.iter();
        let any;

        match input.data {
            Update::Records(ref mut rs) => {
                rs.retain(|r| {
                    r.iter().all(|d| {
                        // check if this filter matches
                        let fi = f.next()
                            .expect("should have as many filters as there are columns in ancestor");
                        if let Some(ref f) = *fi {
                            f == d
                        } else {
                            // everything matches no condition
                            true
                        }
                    })
                });
                any = !rs.is_empty();
            }
        }

        if any { Some(input.data) } else { None }
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, usize> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        "σ".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query::DataType;

    fn setup(materialized: bool, filters: Option<&[Option<DataType>]>) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op("filter",
                 &["x", "y"],
                 Filter::new(s, filters.unwrap_or(&[None, Some("a".into())])),
                 materialized);
        g
    }

    #[test]
    fn it_forwards_nofilter() {
        use query;
        let mut g = setup(false, Some(&[None, None]));

        let mut left: Vec<query::DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));
    }

    #[test]
    fn it_forwards() {
        use query;
        let mut g = setup(false, None);

        let mut left: Vec<query::DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one(left.clone(), false), None);

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));
    }

    #[test]
    fn it_forwards_mfilter() {
        use query;
        let mut g = setup(false, Some(&[Some(1.into()), Some("a".into())]));

        let mut left: Vec<query::DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), Some(left.into()));

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one(left.clone(), false), None);

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one(left.clone(), false), None);

        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one(left.clone(), false), None);
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false, None);
        let me = NodeAddress::mock_global(1.into());
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false, None);
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
    }
}
