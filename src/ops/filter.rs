use std::collections::HashMap;
use std::sync;

pub use nom_sql::Operator;
use flow::prelude::*;

/// Filters incoming records according to some filter.
#[derive(Debug, Clone)]
pub struct Filter {
    src: NodeAddress,
    filter: sync::Arc<Vec<Option<(Operator, DataType)>>>,
}

impl Filter {
    /// Construct a new filter operator. The `filter` vector must have as many elements as the
    /// `src` node has columns. Each column that is set to `None` matches any value, while columns
    /// in the filter that have values set will check for equality on that column.
    pub fn new(src: NodeAddress, filter: &[Option<(Operator, DataType)>]) -> Filter {
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

        rs.retain(|r| {
            let mut f = self.filter.iter();
            r.iter().all(|d| {
                // check if this filter matches
                let fi = f.next()
                    .expect("should have as many filters as there are columns in ancestor");
                if let Some((ref op, ref f)) = *fi {
                    match *op {
                        Operator::Equal => d == f,
                        Operator::NotEqual => d != f,
                        Operator::Greater => d > f,
                        Operator::GreaterOrEqual => d >= f,
                        Operator::Less => d < f,
                        Operator::LessOrEqual => d <= f,
                        _ => unimplemented!(),
                    }
                } else {
                    // everything matches no condition
                    true
                }
            })
        });

        rs
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        use regex::Regex;

        let escape = |s: &str| Regex::new("([<>])").unwrap().replace_all(s, "\\$1");
        format!("σ[{}]",
                self.filter
                    .iter()
                    .enumerate()
                    .filter_map(|(i, ref e)| match e.as_ref() {
                        Some(&(ref op, ref x)) => {
                            Some(format!("f{} {} {}", i, escape(&format!("{}", op)), x))
                        }
                        None => None,
                    })
                    .collect::<Vec<_>>()
                    .as_slice()
                    .join(", "))
            .into()
    }

    fn can_query_through(&self) -> bool {
        true
    }

    fn query_through<'a>(&self,
                         columns: &[usize],
                         key: &KeyType<DataType>,
                         states: &'a StateMap)
                         -> Option<Box<Iterator<Item = &'a sync::Arc<Vec<DataType>>> + 'a>> {
        states.get(self.src.as_local()).map(|state| {
            let f = self.filter.clone();
            Box::new(state.lookup(columns, key).iter().filter(move |r| {
                r.iter().enumerate().all(|(i, d)| {
                    // check if this filter matches
                    if let Some((ref op, ref f)) = f[i] {
                        match *op {
                            Operator::Equal => d == f,
                            Operator::NotEqual => d != f,
                            Operator::Greater => d > f,
                            Operator::GreaterOrEqual => d >= f,
                            Operator::Less => d < f,
                            Operator::LessOrEqual => d <= f,
                            _ => unimplemented!(),
                        }
                    } else {
                        // everything matches no condition
                        true
                    }
                })
            })) as Box<_>
        })
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool,
             filters: Option<&[Option<(Operator, DataType)>]>)
             -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op("filter",
                 &["x", "y"],
                 Filter::new(s,
                             filters.unwrap_or(&[None, Some((Operator::Equal, "a".into()))])),
                 materialized);
        g
    }

    #[test]
    fn it_forwards_nofilter() {
        let mut g = setup(false, Some(&[None, None]));

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false, None);

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_mfilter() {
        let mut g = setup(false,
                          Some(&[Some((Operator::Equal, 1.into())),
                                 Some((Operator::Equal, "a".into()))]));

        let mut left: Vec<DataType>;

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

    #[test]
    fn it_works_with_many() {
        let mut g = setup(false, None);

        let mut many = Vec::new();

        for i in 0..10 {
            many.push(vec![i.into(), "a".into()]);
        }

        assert_eq!(g.narrow_one(many.clone(), false), many.into());
    }

    #[test]
    fn it_works_with_inequalities() {
        let mut g = setup(false,
                          Some(&[Some((Operator::LessOrEqual, 2.into())),
                                 Some((Operator::NotEqual, "a".into()))]));

        let mut left: Vec<DataType>;

        // both conditions match (2 <= 2, "b" != "a")
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" != "a")
        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 <= 2)
        left = vec![3.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (1 <= 2, "b" != "a")
        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }
}
