use flow;
use query;
use ops;

use shortcut;

use std::sync;
use std::thread;
use std::sync::mpsc;

pub type Params = Vec<shortcut::Value<query::DataType>>;
pub type AQ =
    Vec<Box<Fn(mpsc::Sender<Vec<query::DataType>>) -> mpsc::Sender<Params> + Send + Sync>>;
pub type Datas = Box<Iterator<Item = Vec<query::DataType>>>;

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
    fn forward(&self, ops::Update, &AQ) -> Option<ops::Update>;

    /// Called whenever this node is being queried for records, and it is not materialized. The
    /// node should use the list of ancestor query functions to fetch relevant data from upstream,
    /// and emit resulting records as they come in. Note that there may be no query, in which case
    /// all records should be returned.
    fn query(&self, Option<query::Query>, &AQ) -> Datas;
}

pub struct Node<O: NodeOp + Sized + 'static + Send + Sync> {
    fields: Vec<String>,
    data: sync::Arc<Option<sync::RwLock<shortcut::Store<query::DataType>>>>,
    inner: sync::Arc<O>,
}

impl<O> flow::View<query::Query> for Node<O>
    where O: NodeOp + Sized + 'static + Send + Sync
{
    type Update = ops::Update;
    type Data = Vec<query::DataType>;
    type Params = Params;

    fn query(&self,
             aqs: sync::Arc<AQ>,
             q: Option<&query::Query>,
             tx: mpsc::Sender<Self::Data>)
             -> mpsc::Sender<Self::Params> {
        // we need a parameter channel
        let (ptx, prx) = mpsc::channel::<Self::Params>();

        // and the thread is going to need to access a bunch of state
        let q = q.and_then(|q| Some(q.clone()));
        let inner = self.inner.clone();
        let data = self.data.clone();

        // TODO: this should be a thread pool
        thread::spawn(move || {
            for mut p in prx.into_iter() {
                // insert all the query arguments
                p.reverse(); // so we can pop below
                let mut q_cur = q.clone();
                if let Some(ref mut q_cur) = q_cur {
                    for c in q_cur.having.iter_mut() {
                        match c.cmp {
                            shortcut::Comparison::Equal(ref mut v @ shortcut::Value::Const(query::DataType::None)) => {
                                *v = p.pop().expect("not enough query parameters were given");
                            }
                            _ => (),
                        }
                    }
                }

                // find and return matching rows
                if let Some(ref data) = *data {
                    // we are materialized --- give the results
                    let read = data.read().unwrap();
                    if let Some(q_cur) = q_cur {
                        for r in read.find(&q_cur.having[..]) {
                            tx.send(q_cur.project(r)).unwrap();
                        }
                    } else {
                        for r in read.find(&[]) {
                            tx.send(r.iter().cloned().collect()).unwrap();
                        }
                    }
                } else {
                    // we are not materialized --- query
                    for r in inner.query(q_cur, &*aqs) {
                        tx.send(r).unwrap()
                    }
                }
            }
        });
        ptx
    }

    fn process(&self, u: Self::Update, aqs: sync::Arc<AQ>) -> Option<Self::Update> {
        let new_u = self.inner.forward(u, &*aqs);
        if let Some(ref new_u) = new_u {
            match *new_u {
                ops::Update::Records(ref rs) => {
                    if let Some(ref data) = *self.data {
                        let mut w = data.write().unwrap();
                        for r in rs.iter() {
                            if let ops::Record::Positive(ref d) = *r {
                                w.insert(d.clone());
                            } else {
                                unimplemented!();
                            }
                        }
                    }
                }
            }
        }
        new_u
    }
}

pub fn new<'a, S: ?Sized, O>(fields: &[&'a S], materialized: bool, inner: O) -> Node<O>
    where &'a S: Into<String>,
          O: NodeOp + Sized + 'static + Send + Sync
{
    let mut data = None;
    if materialized {
        data = Some(sync::RwLock::new(shortcut::Store::new(fields.len())));
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
    use ops;
    use query;

    use std::time;
    use std::thread;
    use std::sync::mpsc;

    struct Tester(i64);

    impl NodeOp for Tester {
        fn forward(&self, u: ops::Update, _: &AQ) -> Option<ops::Update> {
            // forward
            match u {
                ops::Update::Records(mut rs) => {
                    if let Some(ops::Record::Positive(r)) = rs.pop() {
                        if let query::DataType::Number(r) = r[0] {
                            Some(
                                ops::Update::Records(
                                    vec![ops::Record::Positive(vec![(r + self.0).into()])]
                                )
                            )
                        } else {
                            unreachable!();
                        }
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        fn query(&self, _: Option<query::Query>, aqs: &AQ) -> Datas {
            // query all ancestors, emit r + c for each
            let (tx, rx) = mpsc::channel();
            for aq in aqs.iter() {
                aq(tx.clone()).send(vec![]).unwrap();
            }

            let (ptx, prx) = mpsc::channel();
            let c = self.0;
            thread::spawn(move || {
                for r in rx {
                    if let query::DataType::Number(r) = r[0] {
                        ptx.send(vec![(r + c).into()]).unwrap();
                    } else {
                        unreachable!();
                    }
                }
            });
            Box::new(prx.into_iter())
        }
    }

    #[test]
    fn materialized() {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let all = query::Query {
            select: vec![true],
            having: vec![],
        };
        let a = g.incorporate(new(&["x"], true, Tester(1)), vec![]);
        let b = g.incorporate(new(&["x"], true, Tester(2)), vec![]);
        let c = g.incorporate(new(&["x"], true, Tester(4)),
                              vec![(all.clone(), a), (all.clone(), b)]);
        let d = g.incorporate(new(&["x"], true, Tester(8)), vec![(all.clone(), c)]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(ops::Update::Records(vec![ops::Record::Positive(vec![1.into()])])).unwrap();

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send another in
        put[&b].send(ops::Update::Records(vec![ops::Record::Positive(vec![16.into()])])).unwrap();

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // prepare to query
        let (atx, arx) = mpsc::channel();
        let aargs = get[&a](atx);
        let (btx, brx) = mpsc::channel();
        let bargs = get[&b](btx);
        let (ctx, crx) = mpsc::channel();
        let cargs = get[&c](ctx);
        let (dtx, drx) = mpsc::channel();
        let dargs = get[&d](dtx);

        // check state
        // a
        aargs.send(vec![]).unwrap();
        drop(aargs);
        let set = arx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&2));
        // b
        bargs.send(vec![]).unwrap();
        drop(bargs);
        let set = brx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&18), format!("18 not in {:?}", set));
        // c
        cargs.send(vec![]).unwrap();
        drop(cargs);
        let set = crx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&6), format!("6 not in {:?}", set));
        assert!(set.contains(&22), format!("22 not in {:?}", set));
        // d
        dargs.send(vec![]).unwrap();
        drop(dargs);
        let set = drx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        assert!(set.contains(&30), format!("30 not in {:?}", set));
    }

    #[test]
    fn not_materialized() {
        use std::collections::HashSet;

        // set up graph
        let mut g = flow::FlowGraph::new();
        let all = query::Query {
            select: vec![true],
            having: vec![],
        };
        let a = g.incorporate(new(&["x"], true, Tester(1)), vec![]);
        let b = g.incorporate(new(&["x"], true, Tester(2)), vec![]);
        let c = g.incorporate(new(&["x"], false, Tester(4)),
                              vec![(all.clone(), a), (all.clone(), b)]);
        let d = g.incorporate(new(&["x"], false, Tester(8)), vec![(all.clone(), c)]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(ops::Update::Records(vec![ops::Record::Positive(vec![1.into()])])).unwrap();

        // state should now be:
        // a = [2]
        // b = []
        // c = [6]
        // d = [14]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send another in
        put[&b].send(ops::Update::Records(vec![ops::Record::Positive(vec![16.into()])])).unwrap();

        // state should now be:
        // a = [2]
        // b = [18]
        // c = [6, 22]
        // d = [14, 30]

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // prepare to query
        let (atx, arx) = mpsc::channel();
        let aargs = get[&a](atx);
        let (btx, brx) = mpsc::channel();
        let bargs = get[&b](btx);
        let (ctx, crx) = mpsc::channel();
        let cargs = get[&c](ctx);
        let (dtx, drx) = mpsc::channel();
        let dargs = get[&d](dtx);

        // check state
        // a
        aargs.send(vec![]).unwrap();
        drop(aargs);
        let set = arx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&2));
        // b
        bargs.send(vec![]).unwrap();
        drop(bargs);
        let set = brx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&18), format!("18 not in {:?}", set));
        // c
        cargs.send(vec![]).unwrap();
        drop(cargs);
        let set = crx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&6), format!("6 not in {:?}", set));
        assert!(set.contains(&22), format!("22 not in {:?}", set));
        // d
        dargs.send(vec![]).unwrap();
        drop(dargs);
        let set = drx.iter().map(|mut v| v.pop().unwrap().into()).collect::<HashSet<i64>>();
        assert!(set.contains(&14), format!("14 not in {:?}", set));
        assert!(set.contains(&30), format!("30 not in {:?}", set));
    }
}
