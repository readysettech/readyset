use std::ops::Index;

#[derive(PartialEq, Debug)]
pub struct Data(Vec<String>);

pub struct Condition {
    val: String,
    field: usize,
}

impl Condition {
    pub fn matches(&self, r: &Data) -> bool {
        r.0[self.field] == self.val
    }
}

pub struct Query {
    select: Vec<bool>,
    having: Vec<Vec<Condition>>,
}

impl Query {
    pub fn matches(&self, r: &Data) -> bool {
        // having is an OR of ANDs
        self.having.iter().any(|ands| ands.iter().all(|c| c.matches(r)))
    }

    pub fn feed(&self, r: &Data) -> Option<Data> {
        if self.matches(r) {
            // Data matches -- project and return
            Some(Data(r.0
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if self.select[i] {
                        Some(f.clone())
                    } else {
                        None
                    }
                })
                .collect()))
        } else {
            // Data did not match
            None
        }
    }
}

pub fn new(s: &[bool], h: Vec<Vec<Condition>>) -> Query {
    Query {
        select: s.iter().cloned().collect(),
        having: h,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query() {
        let d = Data(vec!["a".into(), "b".into(), "c".into()]);
        let c_good = Condition {
            val: "a".into(),
            field: 0,
        };
        let c_bad = Condition {
            val: "a".into(),
            field: 1,
        };
        assert_eq!(c_good.matches(&d), true);
        assert_eq!(c_bad.matches(&d), false);
        let q = Query {
            select: vec![true, false, true],
            having: vec![vec![c_good], vec![c_bad]],
        };
        assert_eq!(q.matches(&d), true);
        assert_eq!(q.feed(&d), Some(Data(vec!["a".into(), "c".into()])));
    }

    // fn it_works() {
    //    let mut g = FlowGraph::new();
    //    let a = g.incorporate(ops::Atom::new(&["id", "title"]), &[]);
    //    let v = g.incorporate(ops::Atom::new(&["id", "user"]), &[]);
    //    let vc = g.incorporate(ops::Aggregate::new(1, &[0], ops::Aggregate::COUNT),
    //                           &[(Query::new(&[true, true], vec![]), v)]);
    //    let awv = g.incorporate(ops::Join::new(&[&[Some(0)], &[Some(0)]]),
    //                            &[(Query::new(&[true, true], vec![]), a),
    //                              (Query::new(&[true, true], vec![]), vc)]);
    //    let (insert, augment) = g.run();
    // }
}
