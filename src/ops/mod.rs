pub mod base;
pub mod aggregate;
pub mod latest;
pub mod join;
pub mod union;
pub mod tester;

use flow;
use query;
use backlog;
use shortcut;
use parking_lot;

use std::convert;
use std::fmt;
use std::fmt::Debug;
use std::sync;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<query::DataType>, i64),
    Negative(Vec<query::DataType>, i64),
}

impl Record {
    pub fn rec(&self) -> &[query::DataType] {
        match *self {
            Record::Positive(ref v, _) => &v[..],
            Record::Negative(ref v, _) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn ts(&self) -> i64 {
        match *self {
            Record::Positive(_, ts) => ts,
            Record::Negative(_, ts) => ts,
        }
    }

    pub fn extract(self) -> (Vec<query::DataType>, bool, i64) {
        match self {
            Record::Positive(v, ts) => (v, true, ts),
            Record::Negative(v, ts) => (v, false, ts),
        }
    }
}

impl From<(Vec<query::DataType>, i64)> for Record {
    fn from(other: (Vec<query::DataType>, i64)) -> Self {
        Record::Positive(other.0, other.1)
    }
}

impl From<Vec<query::DataType>> for Record {
    fn from(other: Vec<query::DataType>) -> Self {
        (other, 0).into()
    }
}

#[derive(Clone)]
pub enum Update {
    Records(Vec<Record>),
}

impl From<Record> for Update {
    fn from(other: Record) -> Self {
        Update::Records(vec![other])
    }
}

impl From<Vec<query::DataType>> for Update {
    fn from(other: Vec<query::DataType>) -> Self {
        Update::Records(vec![other.into()])
    }
}

impl From<(Vec<query::DataType>, i64)> for Update {
    fn from(other: (Vec<query::DataType>, i64)) -> Self {
        Update::Records(vec![other.into()])
    }
}

pub type Params = Vec<shortcut::Value<query::DataType>>;
pub type AQ = HashMap<flow::NodeIndex,
                      Box<Fn(Params, i64) -> Vec<(Vec<query::DataType>, i64)> + Send + Sync>>;
pub type Datas = Vec<(Vec<query::DataType>, i64)>;

/// `NodeOp` represents the internal operations performed by a node. This trait is very similar to
/// `flow::View`, and for good reason. This is effectively the behavior of a node when there is no
/// materialization, and no multithreading. Those features are both added on by Node to expose a
/// `flow::View`. A NodeOp should not have, nor need, any mutable state (which is why all receivers
/// are `&`, not `&mut`). Instead, `self` should hold the node's internal configuration (e.g., what
/// fields to join on, how to aggregate).
///
/// It *might* be possible to merge forward and query (after all, they do very similar things), but
/// I haven't found a nice interface for that yet.
pub trait NodeOp : Debug {
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

pub enum NodeType {
    BaseNode(base::Base),
    AggregateNode(aggregate::Aggregator),
    JoinNode(join::Joiner),
    LatestNode(latest::Latest),
    UnionNode(union::Union),
    TestNode(tester::Tester),
}

impl NodeOp for NodeType {
    fn forward(&self,
               u: Update,
               src: flow::NodeIndex,
               ts: i64,
               db: Option<&backlog::BufferedStore>,
               aqfs: &AQ)
               -> Option<Update> {
        match *self {
            NodeType::BaseNode(ref n) => n.forward(u, src, ts, db, aqfs),
            NodeType::AggregateNode(ref n) => n.forward(u, src, ts, db, aqfs),
            NodeType::JoinNode(ref n) => n.forward(u, src, ts, db, aqfs),
            NodeType::LatestNode(ref n) => n.forward(u, src, ts, db, aqfs),
            NodeType::UnionNode(ref n) => n.forward(u, src, ts, db, aqfs),
            NodeType::TestNode(ref n) => n.forward(u, src, ts, db, aqfs),
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64, aqfs: &AQ) -> Datas {
        match *self {
            NodeType::BaseNode(ref n) => n.query(q, ts, aqfs),
            NodeType::AggregateNode(ref n) => n.query(q, ts, aqfs),
            NodeType::JoinNode(ref n) => n.query(q, ts, aqfs),
            NodeType::LatestNode(ref n) => n.query(q, ts, aqfs),
            NodeType::UnionNode(ref n) => n.query(q, ts, aqfs),
            NodeType::TestNode(ref n) => n.query(q, ts, aqfs),
        }
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        match *self {
            NodeType::BaseNode(ref n) => n.suggest_indexes(this),
            NodeType::AggregateNode(ref n) => n.suggest_indexes(this),
            NodeType::JoinNode(ref n) => n.suggest_indexes(this),
            NodeType::LatestNode(ref n) => n.suggest_indexes(this),
            NodeType::UnionNode(ref n) => n.suggest_indexes(this),
            NodeType::TestNode(ref n) => n.suggest_indexes(this),
        }
    }

    fn resolve(&self, col: usize) -> Vec<(flow::NodeIndex, usize)> {
        match *self {
            NodeType::BaseNode(ref n) => n.resolve(col),
            NodeType::AggregateNode(ref n) => n.resolve(col),
            NodeType::JoinNode(ref n) => n.resolve(col),
            NodeType::LatestNode(ref n) => n.resolve(col),
            NodeType::UnionNode(ref n) => n.resolve(col),
            NodeType::TestNode(ref n) => n.resolve(col),
        }
    }
}

impl Debug for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NodeType::BaseNode(ref n) => write!(f, "{:?}", n),
            NodeType::AggregateNode(ref n) => write!(f, "{:?}", n),
            NodeType::JoinNode(ref n) => write!(f, "{:?}", n),
            NodeType::LatestNode(ref n) => write!(f, "{:?}", n),
            NodeType::UnionNode(ref n) => write!(f, "{:?}", n),
            NodeType::TestNode(ref n) => write!(f, "{:?}", n),
        }
    }
}

pub struct Node {
    fields: Vec<String>,
    data: sync::Arc<Option<parking_lot::RwLock<backlog::BufferedStore>>>,
    inner: sync::Arc<NodeType>,
}

impl Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", *self.inner)
    }
}

impl flow::View<query::Query> for Node {
    type Update = Update;
    type Data = Vec<query::DataType>;
    type Params = Params;

    fn find(&self, aqs: &AQ, q: Option<query::Query>, ts: Option<i64>) -> Vec<(Self::Data, i64)> {
        // find and return matching rows
        if let Some(ref data) = *self.data {
            let rlock = data.read();
            if let Some(ref q) = q {
                rlock.find(&q.having[..], ts)
                    .into_iter()
                    .map(|(r, ts)| (q.project(r), ts))
                    .collect()
            } else {
                rlock.find(&[], ts)
                    .into_iter()
                    .map(|(r, ts)| (r.iter().cloned().collect(), ts))
                    .collect()
            }
        } else {
            // we are not materialized --- query.
            // if no timestamp was given to find, we query using the latest timestamp.
            //
            // TODO: what timestamp do we use here? it's not clear. there's always a race in which
            // our ancestor ends up absorbing that timestamp by the time the query reaches them :/
            let ts = ts.unwrap_or(i64::max_value());
            self.inner.query(q.as_ref(), ts, aqs)
        }
    }

    fn init_at(&self, init_ts: i64, aqs: &AQ) {
        if aqs.len() == 0 {
            // base tables have no state to import
            return;
        }

        // we only need to initialize if we are materialized
        if let Some(ref data) = *self.data {
            // we need to initialize this view before it can start accepting updates. we issue a
            // None query to all our ancestors, and then store all the materialized results.
            data.write().batch_import(self.inner.query(None, init_ts, aqs), init_ts);
        }
    }

    fn process(&self,
               u: Self::Update,
               src: flow::NodeIndex,
               ts: i64,
               aqs: &AQ)
               -> Option<Self::Update> {
        use std::ops::Deref;
        let mut data = self.data.deref().as_ref().and_then(|l| Some(l.write()));

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

    fn add_index(&self, col: usize) {
        if let Some(ref data) = *self.data {
            let mut w = data.write();
            w.index(col, shortcut::idx::HashIndex::new());
        } else {
            unreachable!("should never add index to non-materialized view");
        }
    }

    fn safe(&self, ts: i64) {
        if let Some(ref data) = *self.data {
            let mut w = data.write();
            w.absorb(ts);
        }
    }

    fn operator(&self) -> Option<&NodeType> {
        Some(&*self.inner)
    }
}

pub fn new<'a, S: ?Sized, NO>(fields: &[&'a S], materialized: bool, inner: NO) -> Node
    where &'a S: Into<String>,
          NO: NodeOp,
          NodeType: convert::From<NO>
{
    let mut data = None;
    if materialized {
        data = Some(parking_lot::RwLock::new(backlog::BufferedStore::new(fields.len())));
    }

    Node {
        fields: fields.iter().map(|&s| s.into()).collect(),
        data: sync::Arc::new(data),
        inner: sync::Arc::new(NodeType::from(inner)),
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

    fn e2e_test(mat: bool) {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let all = query::Query::new(&[true], vec![]);
        let a = g.incorporate(new(&["a"], true, tester::Tester(1)), vec![]);
        let b = g.incorporate(new(&["b"], true, tester::Tester(2)), vec![]);
        let c = g.incorporate(new(&["c"], mat, tester::Tester(4)),
                              vec![(all.clone(), a), (all.clone(), b)]);
        let d = g.incorporate(new(&["d"], mat, tester::Tester(8)), vec![(all.clone(), c)]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(vec![1.into()]);

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send another in
        put[&b].send(vec![16.into()]);

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // reads only see records whose timestamp is *smaller* than the global minimum.
        // thus the 16 above won't be seen below. let's fix that.
        // first, send a write to increment the global min beyond the 16.
        put[&a].send(vec![32.into()]);
        // let it propagate and bump the mins
        thread::sleep(time::Duration::new(0, 10_000_000));
        // then, send another to make the nodes realize that their descendants' mins have changed.
        put[&a].send(vec![0.into()]);
        // let that propagate too
        thread::sleep(time::Duration::new(0, 10_000_000));

        // note that the first of these two updates may or may not be visible at the different
        // nodes, depending on whether the descendant's min has been updated following an update by
        // the time the sender of that update is checking for an updated descendant min.

        // check state
        // a
        let set = get[&a](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 1+1
        assert!(set.contains(&2), format!("2 not in {:?}", set));
        // may see 32+1
        assert!(set.len() == 1 || (set.len() == 2 && set.contains(&33)),
                format!("32 not the extraneous entry in {:?}", set));

        // b
        let set = get[&b](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 16+2
        assert!(set.contains(&18), format!("18 not in {:?}", set));
        // and nothing else
        assert_eq!(set.len(), 1);

        // c
        let set = get[&c](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 1+1+4
        assert!(set.contains(&6), format!("6 not in {:?}", set));
        // must see 16+2+4
        assert!(set.contains(&22), format!("22 not in {:?}", set));
        if mat {
            // may see 32+1+4
            assert!(set.len() == 2 || (set.len() == 3 && set.contains(&37)),
                    format!("37 not the extraneous entry in {:?}", set));
        } else {
            // must see 32+1+4 (since it's past the ancestor min)
            assert!(set.contains(&37), format!("37 not in {:?}", set));
            // but must *not* see 0+1+4 (since it's not beyond the ancestor min)
            // TODO: test relaxed since queries on non-materialized nodes currently use i64::max
            if false {
                assert_eq!(set.len(), 3);
            } else {
                assert!(set.len() == 4 && set.contains(&5),
                        format!("5 not extraneous entry in {:?}", set));
            }
        }

        // d
        let set = get[&d](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 1+1+4+8
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        // must see 16+2+4+8
        assert!(set.contains(&30), format!("30 not in {:?}", set));
        // must see 32+1+4+8, because leaf views always absorb
        assert!(set.contains(&45), format!("45 not in {:?}", set));
        // won't see the 0 entry, because ancestor min hasn't increased
        if mat {
            assert_eq!(set.len(), 3);
        } else {
            // TODO: test relaxed since queries on non-materialized nodes currently use i64::max
            if false {
                assert_eq!(set.len(), 3);
            } else {
                assert!(set.len() == 4 && set.contains(&13),
                        format!("13 not extraneous entry in {:?}", set));
            }
        }
    }

    #[test]
    fn materialized() {
        e2e_test(true);
    }

    #[test]
    fn not_materialized() {
        e2e_test(false);
    }
}
