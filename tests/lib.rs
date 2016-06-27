extern crate distributary;

use std::time;
use std::thread;

use std::collections::HashMap;

#[test]
fn it_works() {
    // set up graph
    let mut g = distributary::FlowGraph::new();
    let a = g.incorporate(distributary::new(&["a", "b"], true, distributary::Base {}),
                          vec![]);
    let b = g.incorporate(distributary::new(&["a", "b"], true, distributary::Base {}),
                          vec![]);

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let mut cols = HashMap::new();
    cols.insert(a, 2);
    cols.insert(b, 2);
    let u = distributary::Union::new(emits, cols);
    let q = distributary::Query::new(&[true, true], Vec::new());
    let c = g.incorporate(distributary::new(&["a", "b"], false, u),
                          vec![(q.clone(), a), (q, b)]);
    let (put, get) = g.run(10);

    // send a value on a
    put[&a].send(distributary::Update::Records(vec![distributary::Record::Positive(vec![1.into(), 2.into()])]));

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 1_000_000));

    // send a query to c
    assert_eq!(get[&c]((vec![], i64::max_value())).collect::<Vec<_>>(),
               vec![vec![1.into(), 2.into()]]);

    // update value again
    put[&b].send(distributary::Update::Records(vec![distributary::Record::Positive(vec![2.into(), 4.into()])]));

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 1_000_000));

    // check that value was updated again
    let res = get[&c]((vec![], i64::max_value())).collect::<Vec<_>>();
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));
}
