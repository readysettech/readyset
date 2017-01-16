use std::collections::HashMap;
use std::sync;

use flow::prelude::*;
use query::DataType;

/// Filters incoming records according to some filter.
#[derive(Debug, Clone)]
pub struct Filter {
    src: NodeAddress,
    filter: sync::Arc<Vec<Option<DataType>>>,
}

impl Filter {
    /// Construct a new identity operator.
    pub fn new(src: NodeAddress, filter: &[Option<DataType>]) -> Filter {
        Filter {
            src: src,
            filter: sync::Arc::new(Vec::from(filter)),
        }
    }
}

impl Ingredient for Filter {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

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

    fn on_input(&mut self,
                _: NodeAddress,
                mut rs: Records,
                _: &DomainNodes,
                _: &StateMap)
                -> Records {

        let mut f = self.filter.iter();
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

        rs
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

    fn can_query_through(&self) -> bool {
        true
    }

    fn query_through<'a>(&self,
                         column: usize,
                         value: &'a DataType,
                         states: &'a StateMap)
                         -> Option<Box<Iterator<Item = &'a sync::Arc<Vec<DataType>>> + 'a>> {
        states.get(self.src.as_local()).map(|state| {
            let f = self.filter.clone();
            Box::new(state.lookup(column, value).iter().filter(move |r| {
                r.iter().enumerate().all(|(i, d)| {
                    // check if this filter matches
                    if let Some(ref f) = f[i] {
                        f == d
                    } else {
                        // everything matches no condition
                        true
                    }
                })
            })) as Box<_>
        })
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
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        use query;
        let mut g = setup(false, None);

        let mut left: Vec<query::DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_mfilter() {
        use query;
        let mut g = setup(false, Some(&[Some(1.into()), Some("a".into())]));

        let mut left: Vec<query::DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());
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
