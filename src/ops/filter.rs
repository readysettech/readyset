use std::collections::HashMap;
use std::sync;

pub use nom_sql::Operator;
use flow::prelude::*;

/// Filters incoming records according to some filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    src: IndexPair,
    filter: sync::Arc<Vec<Option<(Operator, DataType)>>>,
}

impl Filter {
    /// Construct a new filter operator. The `filter` vector must have as many elements as the
    /// `src` node has columns. Each column that is set to `None` matches any value, while columns
    /// in the filter that have values set will check for equality on that column.
    pub fn new(src: NodeIndex, filter: &[Option<(Operator, DataType)>]) -> Filter {
        Filter {
            src: src.into(),
            filter: sync::Arc::new(Vec::from(filter)),
        }
    }
}

impl Ingredient for Filter {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];
        // N.B.: <= because the adjacent node might be a base with a suffix of removed columns.
        // It's okay to just ignore those.
        assert!(self.filter.len() <= srcn.fields().len());
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: LocalNodeIndex,
        mut rs: Records,
        _: &mut Tracer,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        rs.retain(|r| {
            self.filter.iter().enumerate().all(|(i, fi)| {
                // check if this filter matches
                let d = &r[i];
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
        use regex::Regex;

        let escape = |s: &str| {
            Regex::new("([<>])")
                .unwrap()
                .replace_all(s, "\\$1")
                .to_string()
        };
        format!(
            "σ[{}]",
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
                .join(", ")
        ).into()
    }

    fn can_query_through(&self) -> bool {
        true
    }

    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType<DataType>,
        states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = &'a [DataType]> + 'a>>> {
        states.get(&*self.src).and_then(|state| {
            let f = self.filter.clone();
            match state.lookup(columns, key) {
                LookupResult::Some(rs) => {
                    let r = Box::new(
                        rs.iter()
                            .filter(move |r| {
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
                            })
                            .map(|r| &r[..]),
                    ) as Box<_>;
                    Some(Some(r))
                }
                LookupResult::Missing => Some(None),
            }
        })
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(
        materialized: bool,
        filters: Option<&[Option<(Operator, DataType)>]>,
    ) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "filter",
            &["x", "y"],
            Filter::new(
                s.as_global(),
                filters.unwrap_or(&[None, Some((Operator::Equal, "a".into()))]),
            ),
            materialized,
        );
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
        let mut g = setup(
            false,
            Some(&[
                Some((Operator::Equal, 1.into())),
                Some((Operator::Equal, "a".into())),
            ]),
        );

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
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false, None);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
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
        let mut g = setup(
            false,
            Some(&[
                Some((Operator::LessOrEqual, 2.into())),
                Some((Operator::NotEqual, "a".into())),
            ]),
        );

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
