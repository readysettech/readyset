pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod union;
pub mod identity;
#[cfg(test)]
pub mod gatedid;

use flow;
use flow::NodeIndex;
use query;
use backlog;
use petgraph;
use shortcut;

use std::fmt;
use std::fmt::Debug;
use std::sync;
use std::collections::HashMap;

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<query::DataType>, i64),
    Negative(Vec<query::DataType>, i64),
}

impl Record {
    pub fn rec(&self) -> &[query::DataType] {
        match *self {
            Record::Positive(ref v, _) |
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
            Record::Positive(_, ts) |
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

impl From<(Vec<query::DataType>, i64, bool)> for Record {
    fn from(other: (Vec<query::DataType>, i64, bool)) -> Self {
        if other.2 {
            Record::Positive(other.0, other.1)
        } else {
            Record::Negative(other.0, other.1)
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

/// Update is the smallest unit of data transmitted over edges in a data flow graph.
#[derive(Clone)]
pub enum Update {
    /// This update holds a set of records.
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

type Datas = Vec<(Vec<query::DataType>, i64)>;
pub type V = sync::Arc<flow::View<query::Query, Update = Update, Data = Vec<query::DataType>>>;
pub type Graph = petgraph::Graph<Option<V>, ()>;

/// `NodeOp` represents the internal operations performed by a node. This trait is very similar to
/// `flow::View`, and for good reason. This is effectively the behavior of a node when there is no
/// materialization, and no multithreading. Those features are both added on by Node to expose a
/// `flow::View`. A `NodeOp` should not have, nor need, any mutable state (which is why all
/// receivers are `&`, not `&mut`). Instead, `self` should hold the node's internal configuration
/// (e.g., what fields to join on, how to aggregate).
///
/// It *might* be possible to merge forward and query (after all, they do very similar things), but
/// I haven't found a nice interface for that yet.
pub trait NodeOp: Debug {
    /// See View::prime
    fn prime(&mut self, &Graph) -> Vec<NodeIndex>;

    /// When a new update comes in to a node, this function is called with that update. The
    /// resulting update (if any) is sent to all child nodes. If the node is materialized, and the
    /// resulting update contains positive or negative records, the materialized state is updated
    /// appropriately. See `View::process` for more documentation.
    fn forward(&self,
               u: Option<Update>,
               src: flow::NodeIndex,
               ts: i64,
               last: bool,
               mat: Option<&backlog::BufferedStore>)
               -> flow::ProcessingResult<Update>;

    /// Called whenever this node is being queried for records, and it is not materialized. The
    /// node should use the list of ancestor query functions to fetch relevant data from upstream,
    /// and emit resulting records as they come in. Note that there may be no query, in which case
    /// all records should be returned.
    fn query(&self, Option<&query::Query>, i64) -> Datas;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    fn suggest_indexes(&self, flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, usize) -> Option<Vec<(flow::NodeIndex, usize)>>;

    /// Returns true for base node types.
    fn is_base(&self) -> bool {
        false
    }
}

/// The set of node types supported by distributary.
pub enum NodeType {
    /// A base node. See `Base`.
    Base(base::Base),
    /// An aggregation. See `Aggregator`.
    Aggregate(grouped::GroupedOperator<grouped::aggregate::Aggregator>),
    /// A join. See `Joiner`.
    Join(join::Joiner),
    /// A latest. See `Latest`.
    Latest(latest::Latest),
    /// A union. See `Union`.
    Union(union::Union),
    /// A grouped operator. See `GroupConcat`.
    GroupConcat(grouped::GroupedOperator<grouped::concat::GroupConcat>),
    /// A identity operation. See `Identity`.
    Identity(identity::Identity),
    #[cfg(test)]
    /// A test operator for testing purposes.
    Test(tests::Tester),
    #[cfg(test)]
    /// A test operator to control the propogation of updates.
    GatedIdentity(gatedid::GatedIdentity),
}

impl NodeOp for NodeType {
    fn prime(&mut self, g: &Graph) -> Vec<NodeIndex> {
        match *self {
            NodeType::Base(ref mut n) => n.prime(g),
            NodeType::Aggregate(ref mut n) => n.prime(g),
            NodeType::Join(ref mut n) => n.prime(g),
            NodeType::Latest(ref mut n) => n.prime(g),
            NodeType::Union(ref mut n) => n.prime(g),
            NodeType::Identity(ref mut n) => n.prime(g),
            NodeType::GroupConcat(ref mut n) => n.prime(g),
            #[cfg(test)]
            NodeType::Test(ref mut n) => n.prime(g),
            #[cfg(test)]
            NodeType::GatedIdentity(ref mut n) => n.prime(g),
        }
    }

    fn forward(&self,
               u: Option<Update>,
               src: flow::NodeIndex,
               ts: i64,
               last: bool,
               db: Option<&backlog::BufferedStore>)
               -> flow::ProcessingResult<Update> {
        match *self {
            NodeType::Base(ref n) => n.forward(u, src, ts, last, db),
            NodeType::Aggregate(ref n) => n.forward(u, src, ts, last, db),
            NodeType::Join(ref n) => n.forward(u, src, ts, last, db),
            NodeType::Latest(ref n) => n.forward(u, src, ts, last, db),
            NodeType::Union(ref n) => n.forward(u, src, ts, last, db),
            NodeType::Identity(ref n) => n.forward(u, src, ts, last, db),
            NodeType::GroupConcat(ref n) => n.forward(u, src, ts, last, db),
            #[cfg(test)]
            NodeType::Test(ref n) => n.forward(u, src, ts, last, db),
            #[cfg(test)]
            NodeType::GatedIdentity(ref n) => n.forward(u, src, ts, last, db),
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> Datas {
        match *self {
            NodeType::Base(ref n) => n.query(q, ts),
            NodeType::Aggregate(ref n) => n.query(q, ts),
            NodeType::Join(ref n) => n.query(q, ts),
            NodeType::Latest(ref n) => n.query(q, ts),
            NodeType::Union(ref n) => n.query(q, ts),
            NodeType::Identity(ref n) => n.query(q, ts),
            NodeType::GroupConcat(ref n) => n.query(q, ts),
            #[cfg(test)]
            NodeType::Test(ref n) => n.query(q, ts),
            #[cfg(test)]
            NodeType::GatedIdentity(ref n) => n.query(q, ts),
        }
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        match *self {
            NodeType::Base(ref n) => n.suggest_indexes(this),
            NodeType::Aggregate(ref n) => n.suggest_indexes(this),
            NodeType::Join(ref n) => n.suggest_indexes(this),
            NodeType::Latest(ref n) => n.suggest_indexes(this),
            NodeType::Union(ref n) => n.suggest_indexes(this),
            NodeType::Identity(ref n) => n.suggest_indexes(this),
            NodeType::GroupConcat(ref n) => n.suggest_indexes(this),
            #[cfg(test)]
            NodeType::Test(ref n) => n.suggest_indexes(this),
            #[cfg(test)]
            NodeType::GatedIdentity(ref n) => n.suggest_indexes(this),
        }
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        match *self {
            NodeType::Base(ref n) => n.resolve(col),
            NodeType::Aggregate(ref n) => n.resolve(col),
            NodeType::Join(ref n) => n.resolve(col),
            NodeType::Latest(ref n) => n.resolve(col),
            NodeType::Union(ref n) => n.resolve(col),
            NodeType::Identity(ref n) => n.resolve(col),
            NodeType::GroupConcat(ref n) => n.resolve(col),
            #[cfg(test)]
            NodeType::Test(ref n) => n.resolve(col),
            #[cfg(test)]
            NodeType::GatedIdentity(ref n) => n.resolve(col),
        }
    }

    fn is_base(&self) -> bool {
        if let NodeType::Base(..) = *self {
            true
        } else {
            false
        }
    }
}

impl Debug for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NodeType::Base(ref n) => write!(f, "{:?}", n),
            NodeType::Aggregate(ref n) => write!(f, "{:?}", n),
            NodeType::Join(ref n) => write!(f, "{:?}", n),
            NodeType::Latest(ref n) => write!(f, "{:?}", n),
            NodeType::Union(ref n) => write!(f, "{:?}", n),
            NodeType::Identity(ref n) => write!(f, "{:?}", n),
            NodeType::GroupConcat(ref n) => write!(f, "{:?}", n),
            #[cfg(test)]
            NodeType::Test(ref n) => write!(f, "{:?}", n),
            #[cfg(test)]
            NodeType::GatedIdentity(ref n) => write!(f, "{:?}", n),
        }
    }
}

pub struct Node {
    name: String,
    fields: Vec<String>,
    data: sync::Arc<Option<backlog::BufferedStore>>,
    inner: sync::Arc<NodeType>,
    having: Option<query::Query>,
}

impl Node {
    /// Add an output filter to this node.
    ///
    /// Only records matching the given conditions will be output from this node. This filtering
    /// applies both to feed-forward and to queries. Note that adding conditions in this way does
    /// *not* modify a node's input, and so the node may end up performing computation whose result
    /// will simply be discarded.
    ///
    /// Adding a HAVING condition will not reduce the size of the node's materialized state.
    pub fn having(mut self, cond: Vec<shortcut::Condition<query::DataType>>) -> Self {
        self.having = Some(query::Query::new(&[], cond));
        self
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.name)
    }
}

impl flow::View<query::Query> for Node {
    type Update = Update;
    type Data = Vec<query::DataType>;

    fn prime(&mut self, g: &Graph) -> Vec<NodeIndex> {
        sync::Arc::get_mut(&mut self.inner).expect("prime should have exclusive access").prime(g)
    }

    fn find(&self, q: Option<&query::Query>, ts: Option<i64>) -> Vec<(Self::Data, i64)> {
        // find and return matching rows
        let mut res = if let Some(ref data) = *self.data {
            // data.find already applies the query
            // NOTE: self.having has already been applied
            data.find(q, ts)
        } else {
            // we are not materialized --- query.
            // if no timestamp was given to find, we query using the latest timestamp.
            //
            // TODO: what timestamp do we use here? it's not clear. there's always a race in which
            // our ancestor ends up absorbing that timestamp by the time the query reaches them :/
            let ts = ts.unwrap_or(i64::max_value());
            let rs = self.inner.query(q, ts);

            // to avoid repeating the projection logic in every op, we do it here instead
            if let Some(q) = q {
                rs.into_iter()
                    .filter_map(move |(r, ts)| q.feed(r).map(move |r| (r, ts)))
                    .collect()
            } else {
                rs
            }
        };

        // NOTE: in theory, we could mark records as 'internal' in Store, and have the materialized
        // find above only return non-internal records. That would get rid of this second-pass, and
        // might speed up common-case operation. However, it's not clear that this is something we
        // really need.
        if let Some(ref hq) = self.having {
            res.retain(|&(ref r, _)| hq.filter(r));
        }
        res
    }

    fn init_at(&self, init_ts: i64) {
        if self.inner.is_base() {
            // base tables have no state to import
            return;
        }

        // we only need to initialize if we are materialized
        if let Some(ref data) = *self.data {
            // we need to initialize this view before it can start accepting updates. we issue a
            // None query to all our ancestors, and then store all the materialized results.
            data.batch_import(self.inner.query(None, init_ts), init_ts);
        }
    }

    fn process(&self,
               u: Option<Self::Update>,
               src: flow::NodeIndex,
               ts: i64,
               last: bool)
               -> flow::ProcessingResult<Self::Update> {
        use std::ops::Deref;

        // TODO: the incoming update has not been projected through the query, and so does not fit
        // the expected input format. let's fix that.

        let mut new_u = self.inner.forward(u, src, ts, last, self.data.deref().as_ref());
        if let flow::ProcessingResult::Done(ref mut new_u) = new_u {
            match *new_u {
                Update::Records(ref mut rs) => {
                    if let Some(data) = self.data.deref().as_ref() {
                        // NOTE: data.add requires that we guarantee that there are not concurrent
                        // writers. since each node only processes one update at the time, this is
                        // the case.
                        unsafe { data.add(rs.clone(), ts) };
                    }

                    // notice that we always store forwarded updates in the materialized state,
                    // *even those that do not match the HAVING filter*. this is so that the node
                    // can still query into its own state to see those updates. to see why this is
                    // necessary, consider a materialized COUNT aggregation whose HAVING condition
                    // evaluates to true only when the COUNT is 1. when the first record for a
                    // group comes in, a +1 is emitted and materialized. when the second record
                    // comes in, [-1,+2] is emitted, but the +2 is removed by the HAVING. if the +2
                    // is not persisted in the materialized state, then the next record that comes
                    // along for the group will see that there is no current count, and so assume
                    // taht it is 0. the aggregation will then produce [-0,+1], which is obviously
                    // incorrect.
                    if let Some(ref hq) = self.having {
                        rs.retain(|r| hq.filter(r.rec()));
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
            self.inner.resolve(col)
        }
    }

    fn add_index(&self, col: usize) {
        if let Some(ref data) = *self.data {
            data.index(col, shortcut::idx::HashIndex::new());
        } else {
            unreachable!("should never add index to non-materialized view");
        }
    }

    fn safe(&self, ts: i64) {
        if let Some(ref data) = *self.data {
            data.absorb(ts);
        }
    }

    fn operator(&self) -> Option<&NodeType> {
        Some(&*self.inner)
    }

    fn name(&self) -> &str {
        &*self.name
    }

    fn args(&self) -> &[String] {
        &self.fields[..]
    }
}

/// Construct a new `View` from one of the `NodeType` variants.
///
/// This methods takes a distributary operator and turns it into a full `View`, which can then be
/// used as a node in a `FlowGraph`. By setting `materialied` to true, the operator's outputs will
/// be materialized and transparently used for queries when they arrive. `name` and `fields` are
/// used to give human-friendly values for the node and its record columns respectively.
pub fn new<'a, NS, S: ?Sized, N>(name: NS, fields: &[&'a S], materialized: bool, inner: N) -> Node
    where &'a S: Into<String>,
          NS: Into<String>,
          NodeType: From<N>
{
    let data = if materialized {
        Some(backlog::BufferedStore::new(fields.len()))
    } else {
        None
    };

    Node {
        name: name.into(),
        fields: fields.iter().map(|&s| s.into()).collect(),
        data: sync::Arc::new(data),
        inner: sync::Arc::new(NodeType::from(inner)),
        having: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::Datas;
    use flow;
    use flow::NodeIndex;
    use query;
    use backlog;

    use std::time;
    use std::thread;

    use std::collections::HashMap;

    #[derive(Debug)]
    pub struct Tester(i64, Vec<NodeIndex>, Vec<V>);

    impl Tester {
        pub fn new(ts: i64, anc: Vec<NodeIndex>) -> Tester {
            Tester(ts, anc, vec![])
        }
    }

    impl From<Tester> for NodeType {
        fn from(b: Tester) -> NodeType {
            NodeType::Test(b)
        }
    }

    impl NodeOp for Tester {
        fn prime(&mut self, g: &Graph) -> Vec<NodeIndex> {
            self.2.extend(self.1.iter().map(|&i| g[i].as_ref().unwrap().clone()));
            self.1.clone()
        }

        fn forward(&self,
                   u: Option<Update>,
                   _: flow::NodeIndex,
                   _: i64,
                   _: bool,
                   _: Option<&backlog::BufferedStore>)
                   -> flow::ProcessingResult<Update> {
            // forward
            match u {
                Some(Update::Records(mut rs)) => {
                    if let Some(Record::Positive(r, ts)) = rs.pop() {
                        if let query::DataType::Number(r) = r[0] {
                            flow::ProcessingResult::Done(
                                Update::Records(
                                    vec![Record::Positive(vec![(r + self.0).into()], ts)]
                                )
                            )
                        } else {
                            unreachable!();
                        }
                    } else {
                        unreachable!();
                    }
                }
                None => flow::ProcessingResult::Skip,
            }
        }

        fn query<'a>(&'a self, _: Option<&query::Query>, ts: i64) -> Datas {
            // query all ancestors, emit r + c for each
            let rs = self.2.iter().flat_map(|n| n.find(None, Some(ts)));
            let c = self.0;
            rs.map(move |(r, ts)| {
                    if let query::DataType::Number(r) = r[0] {
                        (vec![(r + c).into()], ts)
                    } else {
                        unreachable!();
                    }
                })
                .collect()
        }

        fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
            HashMap::new()
        }

        fn resolve(&self, _: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
            None
        }
    }
    fn e2e_test(mat: bool) {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let a = g.incorporate(new("a", &["a"], true, Tester::new(1, vec![])));
        let b = g.incorporate(new("b", &["b"], true, Tester::new(2, vec![])));
        let c = g.incorporate(new("c", &["c"], mat, Tester::new(4, vec![a, b])));
        let d = g.incorporate(new("d", &["d"], mat, Tester::new(8, vec![c])));
        let (put, get) = g.run(10);

        // send a value
        put[&a](vec![1.into()]);

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send another in
        put[&b](vec![16.into()]);

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check state
        // a
        let set = get[&a](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 1+1
        assert!(set.contains(&2), format!("2 not in {:?}", set));
        // must not see 32+1
        assert_eq!(set.len(), 1);

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
        // and nothing else
        assert_eq!(set.len(), 2);

        // d
        let set = get[&d](None)
            .into_iter()
            .map(|mut v| v.pop().unwrap().into())
            .collect::<HashSet<i64>>();
        // must see 1+1+4+8
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        // must see 16+2+4+8
        assert!(set.contains(&30), format!("30 not in {:?}", set));
        // and nothing anything else
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn materialized() {
        e2e_test(true);
    }

    #[test]
    fn not_materialized() {
        e2e_test(false);
    }
    #[test]
    fn having() {
        use shortcut;
        use petgraph;
        use flow::View;

        use std::sync;

        // need a graph to prime
        let mut g = petgraph::Graph::new();

        // we have to set up a base node so that s knows how many columns to expect
        let mut a = new("a", &["g", "so"], true, base::Base {});
        a.prime(&g);
        let a = g.add_node(Some(sync::Arc::new(a)));

        // sum incoming records
        let s = new("s",
                    &["g", "s"],
                    true,
                    grouped::aggregate::Aggregation::SUM.over(a, 1, &[0]));

        // condition over aggregation output
        let mut s = s.having(vec![shortcut::Condition {
                                  column: 1,
                                  cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::Number(1))),
                              }]);

        // prepare s
        s.prime(&g);

        // forward an update that changes counter from 0 to 1
        let u = s.process(Some(vec![0.into(), 1.into()].into()), 0.into(), 0, true);
        // we should get an update
        assert!(u.is_done());
        match u.unwrap() {
            Update::Records(rs) => {
                // the -0 should be masked by the HAVING
                assert!(!rs.iter().any(|r| !r.is_positive() && r.rec()[1] == 0.into()));
                // but the +1 should be visible
                assert!(rs.iter().any(|r| r.is_positive() && r.rec()[1] == 1.into()));
                // and there should be no other updates
                assert_eq!(rs.len(), 1);
            }
        }

        // querying should give value
        s.safe(0);
        assert_eq!(s.find(None, None), vec![(vec![0.into(), 1.into()], 0)]);

        // forward another update that changes counter from 1 to 2
        let u = s.process(Some((vec![0.into(), 1.into()], 1).into()),
                          0.into(),
                          1,
                          true);
        // we should get an update
        assert!(u.is_done());
        match u.unwrap() {
            Update::Records(rs) => {
                // the -1 should be visible
                assert!(rs.iter().any(|r| !r.is_positive() && r.rec()[1] == 1.into()));
                // but the +2 should be masked by the HAVING
                assert!(!rs.iter().any(|r| r.is_positive() && r.rec()[1] == 2.into()));
                // and there should be no other updates
                assert_eq!(rs.len(), 1);
            }
        }

        // querying should give nothing
        s.safe(1);
        assert_eq!(s.find(None, None), vec![]);

        // forward a third update that changes the counter from 2 to 3
        let u = s.process(Some((vec![0.into(), 1.into()], 2).into()),
                          0.into(),
                          2,
                          true);
        // we should still get an update
        assert!(u.is_done());
        match u.unwrap() {
            Update::Records(rs) => {
                // neither the -2 or the +3 should be visible
                assert!(rs.is_empty());
            }
        }

        // querying should give nothing
        s.safe(2);
        assert_eq!(s.find(None, None), vec![]);

        // forward a final update that changes the counter back to 1
        let u = s.process(Some((vec![0.into(), (-2).into()], 3).into()),
                          0.into(),
                          3,
                          true);
        // we should still get an update
        assert!(u.is_done());
        match u.unwrap() {
            Update::Records(rs) => {
                // the -3 should be masked by the HAVING
                assert!(!rs.iter().any(|r| !r.is_positive() && r.rec()[1] == 3.into()));
                // but the +1 should be visible
                assert!(rs.iter().any(|r| r.is_positive() && r.rec()[1] == 1.into()));
                // and there should be no other updates
                assert_eq!(rs.len(), 1);
            }
        }

        // querying should give again value
        s.safe(3);
        assert_eq!(s.find(None, None), vec![(vec![0.into(), 1.into()], 3)]);
    }

    #[test]
    fn having_find_unmat() {
        use shortcut;
        use petgraph;
        use flow::View;

        use std::sync;

        // need a graph to prime
        let mut g = petgraph::Graph::new();

        // we have to set up a base node so that s knows how many columns to expect
        let mut a = new("a", &["g", "so"], true, base::Base {});
        a.prime(&g);
        let ga = sync::Arc::new(a);
        let a = g.add_node(Some(ga.clone()));

        // sum incoming records
        let s = new("s",
                    &["g", "s"],
                    false, // only difference from above test
                    grouped::aggregate::Aggregation::SUM.over(a, 1, &[0]));

        // condition over aggregation output
        let mut s = s.having(vec![shortcut::Condition {
                                  column: 1,
                                  cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::Number(1))),
                              }]);

        // prepare s
        s.prime(&g);

        // initially, we should get no results
        assert_eq!(s.find(None, None), vec![]);

        // make the count 1
        ga.process(Some(vec![0.into(), 1.into()].into()), 0.into(), 0, true);

        // querying should now give value (1 matches HAVING)
        assert_eq!(s.find(None, None), vec![(vec![0.into(), 1.into()], 0)]);

        // make the count 2
        ga.process(Some((vec![0.into(), 1.into()], 1).into()),
                   0.into(),
                   1,
                   true);

        // querying should now give nothing
        assert_eq!(s.find(None, None), vec![]);

        // make the count 3
        ga.process(Some((vec![0.into(), 1.into()], 2).into()),
                   0.into(),
                   2,
                   true);

        // querying should still give nothing
        assert_eq!(s.find(None, None), vec![]);

        // forward a final update that changes the counter back to 1
        ga.process(Some((vec![0.into(), (-2).into()], 3).into()),
                   0.into(),
                   3,
                   true);

        // querying should give again value
        assert_eq!(s.find(None, None), vec![(vec![0.into(), 1.into()], 3)]);
    }
}
