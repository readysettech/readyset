use shortcut;
use flow;

#[cfg(feature="web")]
use rustc_serialize::json::{ToJson, Json};
use std::sync;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
#[derive(Eq, PartialOrd, Hash, Debug, Clone)]
#[cfg_attr(feature="b_binsoup", derive(Serialize, Deserialize))]
pub enum DataType {
    /// A placeholder value -- is considered equal to every other `DataType` value.
    None,
    /// A string-like value.
    Text(sync::Arc<String>),
    /// A numeric value.
    Number(i64),
}

impl DataType {
    /// Detect if this `DataType` is none.
    ///
    /// Since we re-implement `PartialEq` for `DataType` to always return `true` for `None`, it can
    /// actually be somewhat hard to do this right for users.
    pub fn is_none(&self) -> bool {
        if let DataType::None = *self {
            true
        } else {
            false
        }
    }
}

#[cfg(feature="web")]
impl ToJson for DataType {
    fn to_json(&self) -> Json {
        use std::ops::Deref;
        match *self {
            DataType::None => Json::Null,
            DataType::Number(n) => Json::I64(n),
            DataType::Text(ref s) => Json::String(s.deref().clone()),
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        if let DataType::None = *self {
            return true;
        }
        if let DataType::None = *other {
            return true;
        }

        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::Number(ref a), &DataType::Number(ref b)) => a == b,
            _ => false,
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::Number(s)
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Number(s as i64)
    }
}

impl Into<i64> for DataType {
    fn into(self) -> i64 {
        if let DataType::Number(s) = self {
            s
        } else {
            unreachable!();
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType::Text(sync::Arc::new(s))
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::Text(sync::Arc::new(s.to_owned()))
    }
}

/// `Query` provides a mechanism for filtering and projecting records.
#[derive(Clone, Debug)]
pub struct Query {
    select: Vec<bool>,
    selects: usize,
    /// The filtering conditions set for this query.
    pub having: Vec<shortcut::Condition<DataType>>,
}

impl Query {
    /// Filter and project the given record `r` through this query.
    pub fn feed(&self, r: &[DataType]) -> Option<Vec<DataType>> {
        if self.having.iter().all(|c| c.matches(r)) {
            // Data matches -- project and return
            Some(self.project(r))
        } else {
            // Data did not match
            None
        }
    }

    /// Project the given record `r` through the selection of this query.
    pub fn project(&self, r: &[DataType]) -> Vec<DataType> {
        assert_eq!(r.len(), self.select.len());
        let mut into = Vec::with_capacity(self.selects);
        for (i, f) in r.iter().enumerate() {
            if self.select[i] {
                into.push(f.clone())
            }
        }
        into
    }

    /// Construct a new `Query` that selects the fields marked `true` in `s` for all rows that
    /// match the set of conditions in `h`.
    pub fn new(s: &[bool], h: Vec<shortcut::Condition<DataType>>) -> Query {
        Query {
            select: s.iter().cloned().collect(),
            selects: s.len(),
            having: h,
        }
    }
}

impl flow::FillableQuery for Query {
    type Params = Vec<shortcut::Value<DataType>>;

    fn fill(&mut self, mut p: Self::Params) {
        // insert all the query arguments
        p.reverse(); // so we can pop below
        for c in self.having.iter_mut() {
            match c.cmp {
                shortcut::Comparison::Equal(ref mut v @ shortcut::Value::Const(DataType::None)) => {
                    *v = p.pop().expect("not enough query parameters were given");
                }
                _ => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shortcut;

    #[test]
    fn query() {
        let t = |s: &str| -> shortcut::Value<DataType> { shortcut::Value::Const(s.into()) };
        let d = vec!["a".into(), "b".into(), "c".into()];
        let c_good = shortcut::Condition {
            cmp: shortcut::Comparison::Equal(t("a")),
            column: 0,
        };
        let c_bad = shortcut::Condition {
            cmp: shortcut::Comparison::Equal(t("a")),
            column: 1,
        };
        assert_eq!(c_good.matches(&d), true);
        assert_eq!(c_bad.matches(&d), false);
        let q_good = Query::new(&[true, false, true], vec![c_good.clone()]);
        let q_bad = Query::new(&[true, false, true], vec![c_bad.clone()]);
        let q_both = Query::new(&[true, false, true], vec![c_good, c_bad]);
        assert_eq!(q_good.feed(&d), Some(vec!["a".into(), "c".into()]));
        assert_eq!(q_bad.feed(&d), None);
        assert_eq!(q_both.feed(&d), None);
    }

    // fn it_works() {
    //    let mut g = FlowGraph::new();
    //    let a = g.incorporate(ops::Base::new(&["id", "title"]), &[]);
    //    let v = g.incorporate(ops::Base::new(&["id", "user"]), &[]);
    //    let vc = g.incorporate(ops::Aggregate::new(1, &[0], ops::Aggregate::COUNT),
    //                           &[(Query::new(&[true, true], vec![]), v)]);
    //    let awv = g.incorporate(ops::Join::new(&[&[Some(0)], &[Some(0)]]),
    //                            &[(Query::new(&[true, true], vec![]), a),
    //                              (Query::new(&[true, true], vec![]), vc)]);
    //    let (insert, augment) = g.run();
    // }
}
