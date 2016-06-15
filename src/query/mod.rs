use shortcut;

#[derive(PartialEq, PartialOrd, Debug, Clone)]
pub enum DataType {
    None,
    Text(String),
    Number(i64),
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
        DataType::Text(s)
    }
}

impl Into<String> for DataType {
    fn into(self) -> String {
        if let DataType::Text(s) = self {
            s
        } else {
            unreachable!();
        }
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::Text(s.to_owned())
    }
}

#[derive(Clone)]
pub struct Query {
    pub select: Vec<bool>,
    pub having: Vec<shortcut::Condition<DataType>>,
}

impl Query {
    pub fn feed(&self, r: &[DataType]) -> Option<Vec<DataType>> {
        if self.having.iter().all(|c| c.matches(r)) {
            // Data matches -- project and return
            Some(self.project(r))
        } else {
            // Data did not match
            None
        }
    }

    pub fn project(&self, r: &[DataType]) -> Vec<DataType> {
        r.iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if self.select[i] {
                    Some(f.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

pub fn new(s: &[bool], h: Vec<shortcut::Condition<DataType>>) -> Query {
    Query {
        select: s.iter().cloned().collect(),
        having: h,
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
        let q_good = Query {
            select: vec![true, false, true],
            having: vec![c_good.clone()],
        };
        let q_bad = Query {
            select: vec![true, false, true],
            having: vec![c_bad.clone()],
        };
        let q_both = Query {
            select: vec![true, false, true],
            having: vec![c_good, c_bad],
        };
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
