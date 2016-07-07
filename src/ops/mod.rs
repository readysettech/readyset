pub mod base;
pub mod aggregate;
pub mod join;
pub mod union;

use flow;
use query;
use backlog;
use shortcut;

use std::sync;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<query::DataType>),
    Negative(Vec<query::DataType>),
}

impl Record {
    pub fn rec(&self) -> &[query::DataType] {
        match *self {
            Record::Positive(ref v) => &v[..],
            Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (Vec<query::DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }
}

#[derive(Clone)]
pub enum Update {
    Records(Vec<Record>),
}

pub type Params = Vec<shortcut::Value<query::DataType>>;
pub type AQ = HashMap<flow::NodeIndex,
                      Box<Fn(Params, i64) -> Vec<Vec<query::DataType>> + Send + Sync>>;
pub type Datas = Vec<Vec<query::DataType>>;

/// `NodeOp` represents the internal operations performed by a node. This trait is very similar to
/// `flow::View`, and for good reason. This is effectively the behavior of a node when there is no
/// materialization, and no multithreading. Those features are both added on by Node to expose a
/// `flow::View`. A NodeOp should not have, nor need, any mutable state (which is why all receivers
/// are `&`, not `&mut`). Instead, `self` should hold the node's internal configuration (e.g., what
/// fields to join on, how to aggregate).
///
/// It *might* be possible to merge forward and query (after all, they do very similar things), but
/// I haven't found a nice interface for that yet.
pub trait NodeOp {
    /// When a new update comes in to a node, this function is called with that update. The
    /// resulting update (if any) is sent to all child nodes. If the node is materialized, and the
    /// resulting update contains positive or negative records, the materialized state is updated
    /// appropriately.
    fn forward(&self,
               Update,
               flow::NodeIndex,
               i64,
               Option<&backlog::BufferedStore>,
               &AQ)
               -> Option<Update>;

    /// Called whenever this node is being queried for records, and it is not materialized. The
    /// node should use the list of ancestor query functions to fetch relevant data from upstream,
    /// and emit resulting records as they come in. Note that there may be no query, in which case
    /// all records should be returned.
    fn query(&self, Option<&query::Query>, i64, &AQ) -> Datas;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    fn suggest_indexes(&self, flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>>;

    /// Resolve where the given field originates from. If this view is materialized, None should be
    /// returned.
    fn resolve(&self, usize) -> Vec<(flow::NodeIndex, usize)>;
}

pub struct Node<O: NodeOp + Sized + 'static + Send + Sync> {
    fields: Vec<String>,
    data: sync::Arc<Option<sync::RwLock<backlog::BufferedStore>>>,
    inner: sync::Arc<O>,
}

impl<O> flow::View<query::Query> for Node<O>
    where O: NodeOp + Sized + 'static + Send + Sync
{
    type Update = Update;
    type Data = Vec<query::DataType>;
    type Params = Params;

    fn find(&self, aqs: &AQ, q: Option<query::Query>, ts: i64) -> Vec<Self::Data> {
        // find and return matching rows
        if let Some(ref data) = *self.data {
            let rlock = data.read().unwrap();
            if let Some(ref q) = q {
                rlock.find(&q.having[..], ts)
                    .into_iter()
                    .map(|r| q.project(r))
                    .collect()
            } else {
                rlock.find(&[], ts).into_iter().map(|r| r.iter().cloned().collect()).collect()
            }
        } else {
            // we are not materialized --- query
            self.inner.query(q.as_ref(), ts, aqs)
        }
    }

    fn process(&self,
               u: Self::Update,
               src: flow::NodeIndex,
               ts: i64,
               aqs: &AQ)
               -> Option<Self::Update> {
        use std::ops::Deref;
        let mut data = self.data.deref().as_ref().and_then(|l| Some(l.write().unwrap()));

        let new_u = self.inner.forward(u, src, ts, data.as_ref().and_then(|d| Some(&**d)), &*aqs);
        if let Some(ref new_u) = new_u {
            match *new_u {
                Update::Records(ref rs) => {
                    if let Some(ref mut data) = data {
                        data.add(rs.clone(), ts);
                    }
                }
            }
        }
        new_u
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        self.inner.suggest_indexes(this)
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        if self.data.is_some() {
            None
        } else {
            Some(self.inner.resolve(col))
        }
    }

    fn add_index(&mut self, col: usize) {
        if let Some(ref data) = *self.data {
            let mut w = data.write().unwrap();
            w.index(col, shortcut::idx::HashIndex::new());
        } else {
            unreachable!("should never add index to non-materialized view");
        }
    }

    fn safe(&self, ts: i64) {
        if let Some(ref data) = *self.data {
            let mut w = data.write().unwrap();
            w.absorb(ts);
        }
    }
}

pub fn new<'a, S: ?Sized, O>(fields: &[&'a S], materialized: bool, inner: O) -> Node<O>
    where &'a S: Into<String>,
          O: NodeOp + Sized + 'static + Send + Sync
{
    let mut data = None;
    if materialized {
        data = Some(sync::RwLock::new(backlog::BufferedStore::new(fields.len())));
    }

    Node {
        fields: fields.iter().map(|&s| s.into()).collect(),
        data: sync::Arc::new(data),
        inner: sync::Arc::new(inner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow;
    use query;
    use backlog;

    use std::time;
    use std::thread;

    use std::collections::HashMap;

    struct Tester(i64);

    impl NodeOp for Tester {
        fn forward(&self,
                   u: Update,
                   _: flow::NodeIndex,
                   _: i64,
                   _: Option<&backlog::BufferedStore>,
                   _: &AQ)
                   -> Option<Update> {
            // forward
            match u {
                Update::Records(mut rs) => {
                    if let Some(Record::Positive(r)) = rs.pop() {
                        if let query::DataType::Number(r) = r[0] {
                            Some(Update::Records(vec![Record::Positive(vec![(r + self.0).into()])]))
                        } else {
                            unreachable!();
                        }
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        fn query<'a>(&'a self, _: Option<&query::Query>, ts: i64, aqs: &AQ) -> Datas {
            // query all ancestors, emit r + c for each
            let rs = aqs.iter().flat_map(|(_, aq)| aq(vec![], ts));
            let c = self.0;
            rs.map(move |r| {
                    if let query::DataType::Number(r) = r[0] {
                        vec![(r + c).into()]
                    } else {
                        unreachable!();
                    }
                })
                .collect()
        }

        fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
            HashMap::new()
        }

        fn resolve(&self, _: usize) -> Vec<(flow::NodeIndex, usize)> {
            vec![]
        }
    }

    #[test]
    fn materialized() {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let all = query::Query::new(&[true], vec![]);
        let a = g.incorporate(new(&["x"], true, Tester(1)), vec![]);
        let b = g.incorporate(new(&["x"], true, Tester(2)), vec![]);
        let c = g.incorporate(new(&["x"], true, Tester(4)),
                              vec![(all.clone(), a), (all.clone(), b)]);
        let d = g.incorporate(new(&["x"], true, Tester(8)), vec![(all.clone(), c)]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(Update::Records(vec![Record::Positive(vec![1.into()])]));

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send another in
        put[&b].send(Update::Records(vec![Record::Positive(vec![16.into()])]));

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check state
        // a
        let set = get[&a](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&2));
        // b
        let set = get[&b](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&18), format!("18 not in {:?}", set));
        // c
        let set = get[&c](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&6), format!("6 not in {:?}", set));
        assert!(set.contains(&22), format!("22 not in {:?}", set));
        // d
        let set = get[&d](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        assert!(set.contains(&30), format!("30 not in {:?}", set));
    }

    #[test]
    fn not_materialized() {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let all = query::Query::new(&[true], vec![]);
        let a = g.incorporate(new(&["x"], true, Tester(1)), vec![]);
        let b = g.incorporate(new(&["x"], true, Tester(2)), vec![]);
        let c = g.incorporate(new(&["x"], false, Tester(4)),
                              vec![(all.clone(), a), (all.clone(), b)]);
        let d = g.incorporate(new(&["x"], false, Tester(8)), vec![(all.clone(), c)]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(Update::Records(vec![Record::Positive(vec![1.into()])]));

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send another in
        put[&b].send(Update::Records(vec![Record::Positive(vec![16.into()])]));

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check state
        // a
        let set = get[&a](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&2));
        // b
        let set = get[&b](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&18), format!("18 not in {:?}", set));
        // c
        let set = get[&c](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&6), format!("6 not in {:?}", set));
        assert!(set.contains(&22), format!("22 not in {:?}", set));
        // d
        let set = get[&d](None, i64::max_value())
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        assert!(set.contains(&30), format!("30 not in {:?}", set));
    }
}
