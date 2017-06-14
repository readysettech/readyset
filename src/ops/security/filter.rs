use std::collections::HashMap;
use ops::security::*;
pub use nom_sql::Operator;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterType {
	DataType(DataType),
	Key(String)
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityFilter {
	filter: Vec<Option<(Operator, FilterType)>>,
	context: HashMap<String, DataType>,
}

impl SecurityFilter {
	pub fn new(src: NodeIndex, filter: Vec<Option<(Operator, FilterType)>>, context: HashMap<String, DataType>) -> SecurityOperator<SecurityFilter> {
		SecurityOperator::new(
			src, 
			SecurityFilter {
				filter: filter,
				context: context,
			})
	}
}

impl SecurityOperation for SecurityFilter {
	fn description(&self) -> String {
		format!("Ω[{}]", "filter").into()
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
				let d = &r[i];
				if let Some((ref op, ref ft)) = *fi {
					let f = match *ft {
						FilterType::DataType(ref dt) => dt,
						FilterType::Key(ref key) => &self.context[key],
					};

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
					true
				}
			})
		});

		ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
	use super::*;
	use ops;

    fn setup(
        materialized: bool,
        filters: Option<Vec<Option<(Operator, FilterType)>>>,
    ) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
    	let a: DataType = "a".into();
    	let id: DataType = 1.into();
    	let default_filter = vec![Some((Operator::Equal, FilterType::Key("userid".into()))), 
						  		  Some((Operator::Equal, FilterType::DataType("a".into()))),
						  		  None];
    	let mut context = HashMap::new();
    	context.insert("userid".into(), id);

        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "security_filter",
            &["x", "y", "z"],
            SecurityFilter::new(
                s.as_global(),
                filters.unwrap_or(default_filter),
                context,
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_nofilter() {
        let mut g = setup(false, Some(vec![None, None, None]));

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
    	let a: DataType = "a".into();
        let mut g = setup(false, Some(vec![None, Some((Operator::Equal, FilterType::DataType("a".into()))), None]));

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into(), 0.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_withkeys() {
    	let mut g = setup(false, None);

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into(), 0.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into(), 0.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());
    }

	#[test]
    fn it_suggests_indices() {
    	let mut g = setup(false, None);

        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
		let mut g = setup(false, None);

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
            many.push(vec![1.into(), "a".into(), i.into()]);
        }

        assert_eq!(g.narrow_one(many.clone(), false), many.into());
    }

        #[test]
    fn it_works_with_inequalities() {
        let mut g = setup(
            false,
            Some(
                vec![
                    Some((Operator::LessOrEqual, FilterType::Key("userid".into()))),
                    Some((Operator::NotEqual, FilterType::DataType("a".into()))),
                ],
            ),
        );

        let mut left: Vec<DataType>;

        // both conditions match (1 <= 1, "b" != "a")
        left = vec![1.into(), "b".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" != "a")
        left = vec![1.into(), "a".into(), 0.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 <= 2)
        left = vec![2.into(), "b".into(), 0.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (1 <= 2, "b" != "a")
        left = vec![0.into(), "b".into(), 0.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }
}