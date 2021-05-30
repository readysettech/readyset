use derive_more::{Deref, From, Into};

use regex::Regex;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync;

use crate::prelude::*;
use crate::processing::ColumnSource;
pub use nom_sql::BinaryOperator;
use noria::errors::ReadySetResult;
use std::convert::TryInto;

/// FilterVec represents set of constraints on columns against which a record can be checked
/// Used in Filter operators and aggregations with case statements
#[derive(Debug, Clone, Serialize, Deserialize, From, Into, Deref)]
pub struct FilterVec(Vec<(usize, FilterCondition)>);

impl FilterVec {
    /// Checks if record `r` passes the filter
    pub fn matches(&self, r: &[DataType]) -> bool {
        self.iter().all(|(i, cond)| {
            let d = &r[*i];
            match cond {
                FilterCondition::Comparison(ref op, ref f) => {
                    let v = match f {
                        Value::Constant(dt) => dt,
                        Value::Column(c) => &r[*c],
                    };

                    if *op == BinaryOperator::Is {
                        return d == v;
                    } else if d.is_none() || v.is_none() {
                        return false;
                    }

                    match *op {
                        // FIXME(ENG-209): Make NULL = NULL not true, but NULL IS NULL true
                        BinaryOperator::Equal | BinaryOperator::Is => d == v,
                        BinaryOperator::NotEqual => d != v,
                        BinaryOperator::Greater => d > v,
                        BinaryOperator::GreaterOrEqual => d >= v,
                        BinaryOperator::Less => d < v,
                        BinaryOperator::LessOrEqual => d <= v,
                        _ => unimplemented!(),
                    }
                }
                FilterCondition::In(ref fs) => fs.contains(d),
            }
        })
    }

    pub fn describe(&self) -> String {
        lazy_static! {
            static ref ESC_RE: Regex = Regex::new("([<>])").unwrap();
        }
        self.iter()
            .map(|(i, ref cond)| match *cond {
                FilterCondition::Comparison(ref op, ref x) => format!(
                    "f{} {} {}",
                    i,
                    ESC_RE.replace_all(&format!("{}", op), "\\$1").to_string(),
                    x
                ),
                FilterCondition::In(ref xs) => format!(
                    "f{} IN ({})",
                    i,
                    xs.iter()
                        .map(|d| format!("{}", d))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            })
            .collect::<Vec<_>>()
            .as_slice()
            .join(", ")
    }
}

/// The actual Filter Operator
/// Filters incoming records according to some FilterVec
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    src: IndexPair,
    filter: sync::Arc<FilterVec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Constant(DataType),
    Column(usize),
}

impl From<DataType> for Value {
    fn from(dt: DataType) -> Self {
        Value::Constant(dt)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Constant(ref c) => write!(f, "{}", c),
            Value::Column(ref ci) => write!(f, "col: {}", ci),
        }
    }
}

// TODO: expect that this does not properly capture all possible filter conditions?
// e.g. nested conditions?
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterCondition {
    Comparison(BinaryOperator, Value),
    In(Vec<DataType>),
}

impl Filter {
    /// Construct a new filter operator. The `filter` vector must have as many elements as the
    /// `src` node has columns. Each column that is set to `None` matches any value, while columns
    /// in the filter that have values set will check for equality on that column.
    pub fn new(src: NodeIndex, filter: &[(usize, FilterCondition)]) -> Filter {
        Filter {
            src: src.into(),
            filter: sync::Arc::new(FilterVec(Vec::from(filter))),
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
        _: &mut dyn Executor,
        _: LocalNodeIndex,
        mut rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        rs.retain(|r| self.filter.matches(r));

        Ok(ProcessingResult {
            results: rs,
            ..Default::default()
        })
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Index> {
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ReadySetResult<ColumnSource> {
        Ok(ColumnSource::exact_copy(
            self.src.as_global(),
            cols.try_into().unwrap(),
        ))
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            String::from("σ")
        } else {
            format!("σ[{}]", self.filter.describe())
        }
    }

    fn can_query_through(&self) -> bool {
        true
    }

    #[allow(clippy::type_complexity)]
    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType,
        nodes: &DomainNodes,
        states: &'a StateMap,
    ) -> Option<Option<Box<dyn Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        self.lookup(*self.src, columns, key, nodes, states)
            .map(|result| {
                let f = self.filter.clone();
                let filter = move |r: &[DataType]| f.matches(r);

                result.map(|rs| Box::new(rs.filter(move |r| filter(r))) as Box<_>)
            })
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(
        materialized: bool,
        filters: Option<&[(usize, FilterCondition)]>,
    ) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "filter",
            &["x", "y"],
            Filter::new(
                s.as_global(),
                filters.unwrap_or(&[(
                    1,
                    FilterCondition::Comparison(BinaryOperator::Equal, Value::Constant("a".into())),
                )]),
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_nofilter() {
        let mut g = setup(false, Some(&[]));

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
                (
                    0,
                    FilterCondition::Comparison(BinaryOperator::Equal, Value::Constant(1.into())),
                ),
                (
                    1,
                    FilterCondition::Comparison(BinaryOperator::Equal, Value::Constant("a".into())),
                ),
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
        assert!(g.narrow_one_row(left, false).is_empty());
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
            g.node().resolve(0).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1).unwrap(),
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
                (
                    0,
                    FilterCondition::Comparison(
                        BinaryOperator::LessOrEqual,
                        Value::Constant(2.into()),
                    ),
                ),
                (
                    1,
                    FilterCondition::Comparison(
                        BinaryOperator::NotEqual,
                        Value::Constant("a".into()),
                    ),
                ),
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

    #[test]
    fn it_works_with_columns() {
        let mut g = setup(
            false,
            Some(&[(
                0,
                FilterCondition::Comparison(BinaryOperator::Equal, Value::Column(1)),
            )]),
        );

        let mut left: Vec<DataType>;
        left = vec![2.into(), 2.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left, false), Records::default());
    }

    #[test]
    fn it_works_with_in_list() {
        let mut g = setup(
            false,
            Some(&[
                (0, FilterCondition::In(vec![2.into(), 42.into()])),
                (1, FilterCondition::In(vec!["b".into()])),
            ]),
        );

        let mut left: Vec<DataType>;

        // both conditions match (2 IN (2, 42), "b" IN ("b"))
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" NOT IN ("b"))
        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 NOT IN (2, 42))
        left = vec![3.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (42 IN (2, 42), "b" IN ("b"))
        left = vec![42.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn null_equal_null() {
        let fv = FilterVec(vec![(
            0,
            FilterCondition::Comparison(BinaryOperator::Equal, Value::Constant(DataType::None)),
        )]);
        assert!(!fv.matches(&[DataType::None]));
    }

    #[test]
    fn value_not_equal_null() {
        let fv = FilterVec(vec![(
            0,
            FilterCondition::Comparison(BinaryOperator::NotEqual, Value::Constant(DataType::None)),
        )]);
        assert!(!fv.matches(&[DataType::Int(1)]));
    }
}
