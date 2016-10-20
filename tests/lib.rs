extern crate distributary;
extern crate shortcut;

use std::time;
use std::thread;

use std::collections::HashMap;

#[test]
fn it_works() {
    // set up graph
    let mut g = distributary::FlowGraph::new();
    let a = g.incorporate(distributary::new("a", &["a", "b"], true, distributary::Base {}));
    let b = g.incorporate(distributary::new("b", &["a", "b"], true, distributary::Base {}));

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = g.incorporate(distributary::new("c", &["a", "b"], false, u));
    let (put, get) = g.run(10);

    // send a value on a
    put[&a].send(vec![1.into(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(get[&c](None), vec![vec![1.into(), 2.into()]]);

    // update value again
    put[&b].send(vec![2.into(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = distributary::FlowGraph::new();
    let a = g.incorporate(distributary::new("a", &["a", "b"], true, distributary::Base {}));
    let b = g.incorporate(distributary::new("b", &["a", "b"], true, distributary::Base {}));

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = g.incorporate(distributary::new("c", &["a", "b"], true, u));
    let (put, get) = g.run(10);

    // send a few values on a
    put[&a].send(vec![1.into(), 1.into()]);
    put[&a].send(vec![1.into(), 2.into()]);
    put[&a].send(vec![1.into(), 3.into()]);

    // give them some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    // we should see at least the first one, and possibly the second
    // the third won't be seen, because its timestamp hasn't been passed yet
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 1.into()]));
    assert!(res.len() == 1 || res.iter().any(|r| r == &vec![1.into(), 2.into()]));

    // update value again (and again send some secondary updates)
    put[&b].send(vec![2.into(), 4.into()]);
    put[&b].send(vec![2.into(), 5.into()]);
    put[&b].send(vec![2.into(), 6.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![1.into(), 3.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));
    assert!(res.len() == 4 || res.iter().any(|r| r == &vec![2.into(), 5.into()]));
}

#[test]
fn it_migrates_wo_mat() {
    // set up graph
    let mut g = distributary::FlowGraph::new();
    let a = g.incorporate(distributary::new("a", &["a", "b"], true, distributary::Base {}));
    let (put_1, _) = g.run(10);

    // send a value on a
    put_1[&a].send(vec![1.into(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // add more of graph
    let b = g.incorporate(distributary::new("b", &["a", "b"], true, distributary::Base {}));

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = g.incorporate(distributary::new("c", &["a", "b"], false, u));
    let (put, get) = g.run(10);

    // wait a bit for initialization
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(get[&c](None), vec![vec![1.into(), 2.into()]]);

    // update value again
    put[&b].send(vec![2.into(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));
}

#[test]
fn it_migrates_w_mat() {
    // set up graph
    let mut g = distributary::FlowGraph::new();
    let a = g.incorporate(distributary::new("a", &["a", "b"], true, distributary::Base {}));
    let (put_1, _) = g.run(10);

    // send a few values on a
    put_1[&a].send(vec![1.into(), 1.into()]);
    put_1[&a].send(vec![1.into(), 2.into()]);
    put_1[&a].send(vec![1.into(), 3.into()]);

    // give them some time to propagate
    thread::sleep(time::Duration::new(0, 100_000_000));

    // add the rest of the graph
    let b = g.incorporate(distributary::new("b", &["a", "b"], true, distributary::Base {}));

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = g.incorporate(distributary::new("c", &["a", "b"], true, u));
    let (put, get) = g.run(10);

    // wait a bit for initialization
    thread::sleep(time::Duration::new(0, 100_000_000));

    // send a query to c
    // we should see at least the first one, and possibly the second
    // the third won't be seen, because its timestamp hasn't been passed yet
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 1.into()]));
    assert!(res.len() == 1 || res.iter().any(|r| r == &vec![1.into(), 2.into()]));

    // update value again (and again send some secondary updates)
    put[&b].send(vec![2.into(), 4.into()]);
    put[&b].send(vec![2.into(), 5.into()]);
    put[&b].send(vec![2.into(), 6.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 100_000_000));

    // check that value was updated again
    let res = get[&c](None);
    assert!(res.iter().any(|r| r == &vec![1.into(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![1.into(), 3.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));
    assert!(res.len() == 4 || res.iter().any(|r| r == &vec![2.into(), 5.into()]));
}

#[test]
fn votes() {
    use distributary::{Base, Union, Query, Aggregation, JoinBuilder, new};

    // set up graph
    let mut g = distributary::FlowGraph::new();

    // add article base nodes (we use two so we can exercise unions too)
    let article1 = g.incorporate(new("article1", &["id", "title"], true, Base {}));
    let article2 = g.incorporate(new("article2", &["id", "title"], true, Base {}));

    // add a (stupid) union of article1 + article2
    let mut emits = HashMap::new();
    emits.insert(article1, vec![0, 1]);
    emits.insert(article2, vec![0, 1]);
    let u = Union::new(emits);
    let article = g.incorporate(new("article", &["id", "title"], false, u));

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));

    // add vote count
    let vc = g.incorporate(new("vc",
                               &["id", "votes"],
                               true,
                               Aggregation::COUNT.new(vote, 0)));

    // add final join using first field from article and first from vc
    let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
        .from(article, vec![1, 0])
        .join(vc, vec![1, 0]);
    let end = g.incorporate(new("end", &["id", "title", "votes"], true, j));

    // start processing
    let (put, get) = g.run(10);

    // make one article
    put[&article1].send(vec![1.into(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles to see that it was absorbed
    assert_eq!(get[&article](None), vec![vec![1.into(), 2.into()]]);

    // make another article
    put[&article2].send(vec![2.into(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    let res = get[&article](None);
    assert!(res.len() == 2, "articles was {:?}", res);
    assert!(res.iter().any(|r| r == &vec![1.into(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![2.into(), 4.into()]));

    // zero values across joins are tricky...
    if false {
        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that both articles appear in the join view with a vote count of zero
        let res = get[&end](None);
        assert!(res.len() == 2, "end was {:?}", res);
        assert!(res.iter().any(|r| r == &vec![1.into(), 2.into(), 0.into()]));
        assert!(res.iter().any(|r| r == &vec![2.into(), 4.into(), 0.into()]));
    }

    // create a vote (user 1 votes for article 1)
    put[&vote].send(vec![1.into(), 1.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // this is stupid, but because the system is eventually consistent, we also need to inject two
    // extra updates. one to advances the global min such that the vote above is visible, and
    // another for all the nodes to realize that this has happened. unfortunately, this *also*
    // means that the different nodes may *or may not* see the middle vote.
    put[&vote].send(vec![2.into(), 1.into()]);
    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));
    put[&vote].send(vec![0.into(), 1.into()]);
    // give it too some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query vote count to see that the count was updated
    let res = get[&vc](None);
    assert!(res.iter().all(|r| r[0] == 1.into() && (r[1] == 1.into() || r[1] == 2.into())));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = get[&end](None);
    assert!(res.iter()
                .any(|r| {
                    r[0] == 1.into() && r[1] == 2.into() && (r[2] == 1.into() || r[2] == 2.into())
                }),
            "no entry for [1,2,1|2] in {:?}",
            res);
    assert!(res.len() <= 2); // could be 2 if we had zero result rows

    // check that this is the case also if we query for the ID directly
    let q = vec![shortcut::Condition {
                     column: 0,
                     cmp: shortcut::Comparison::Equal(shortcut::Value::Const(1.into())),
                 }];
    let res = get[&end](Some(&Query::new(&[true, true, true], q)));
    assert_eq!(res.len(), 1);
    assert!(res.iter()
        .any(|r| r[0] == 1.into() && r[1] == 2.into() && (r[2] == 1.into() || r[2] == 2.into())));

    // again, zero values are tricky...
    if false {
        // and that we get zero if the other one is queried
        let q = vec![shortcut::Condition {
                         column: 0,
                         cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                     }];
        let res = get[&end](Some(&Query::new(&[true, true, true], q)));
        assert_eq!(res.len(), 1);
        assert!(res.iter().any(|r| r == &vec![2.into(), 4.into(), 0.into()]));
    }
}
