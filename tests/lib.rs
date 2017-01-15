extern crate distributary;

use std::time;
use std::thread;

use std::collections::HashMap;

#[test]
fn it_works() {
    // set up graph
    let mut g = distributary::Blender::new();
    let mut mig = g.start_migration();
    let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
    let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = mig.add_ingredient("c", &["a", "b"], u);
    let cq = mig.maintain(c, 0);

    let put = mig.commit();
    let id: distributary::DataType = 1.into();

    // send a value on a
    put[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), vec![vec![1.into(), 2.into()]]);

    // update value again
    put[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = distributary::Blender::new();
    let mut mig = g.start_migration();
    let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
    let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = mig.add_ingredient("c", &["a", "b"], u);
    let cq = mig.maintain(c, 0);

    let id: distributary::DataType = 1.into();
    let put = mig.commit();

    // send a few values on a
    put[&a].0(vec![id.clone(), 1.into()]);
    put[&a].0(vec![id.clone(), 2.into()]);
    put[&a].0(vec![id.clone(), 3.into()]);

    // give them some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    // we should see all the a values
    let res = cq(&id);
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // update value again (and again send some secondary updates)
    put[&b].0(vec![id.clone(), 4.into()]);
    put[&b].0(vec![id.clone(), 5.into()]);
    put[&b].0(vec![id.clone(), 6.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id);
    assert_eq!(res.len(), 6);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 5.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 6.into()]));
}

#[test]
fn votes() {
    use distributary::{Base, Union, Aggregation, JoinBuilder};

    // set up graph
    let mut g = distributary::Blender::new();
    let mut mig = g.start_migration();

    // add article base nodes (we use two so we can exercise unions too)
    let article1 = mig.add_ingredient("article1", &["id", "title"], Base {});
    let article2 = mig.add_ingredient("article1", &["id", "title"], Base {});

    // add a (stupid) union of article1 + article2
    let mut emits = HashMap::new();
    emits.insert(article1, vec![0, 1]);
    emits.insert(article2, vec![0, 1]);
    let u = Union::new(emits);
    let article = mig.add_ingredient("article", &["id", "title"], u);
    let articleq = mig.maintain(article, 0);

    // add vote base table
    let vote = mig.add_ingredient("vote", &["user", "id"], Base {});

    // add vote count
    let vc = mig.add_ingredient("vc",
                                &["id", "votes"],
                                Aggregation::COUNT.over(vote, 0, &[1]));
    let vcq = mig.maintain(vc, 0);

    // add final join using first field from article and first from vc
    let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
        .from(article, vec![1, 0])
        .join(vc, vec![1, 0]);
    let end = mig.add_ingredient("end", &["id", "title", "votes"], j);
    let endq = mig.maintain(end, 0);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    // start processing
    let put = mig.commit();

    // make one article
    put[&article1].0(vec![a1.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles to see that it was updated
    assert_eq!(articleq(&a1), vec![vec![a1.clone(), 2.into()]]);

    // make another article
    put[&article2].0(vec![a2.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(articleq(&a1), vec![vec![a1.clone(), 2.into()]]);
    assert_eq!(articleq(&a2), vec![vec![a2.clone(), 4.into()]]);

    // create a vote (user 1 votes for article 1)
    put[&vote].0(vec![1.into(), a1.clone()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query vote count to see that the count was updated
    let res = vcq(&a1);
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq(&a1);
    assert!(res.iter().any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
            "no entry for [1,2,1|2] in {:?}",
            res);
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq(&a2);
    assert!(res.len() <= 1); // could be 1 if we had zero-rows
}

#[test]
fn transactional_vote() {
    use distributary::{Base, Union, Aggregation, JoinBuilder};

    // set up graph
    let mut g = distributary::Blender::new();
    let validate = g.get_validator();
    let mut mig = g.start_migration();

    // add article base nodes (we use two so we can exercise unions too)
    let article1 = mig.add_ingredient("article1", &["id", "title"], Base {});
    let article2 = mig.add_ingredient("article1", &["id", "title"], Base {});

    // add a (stupid) union of article1 + article2
    let mut emits = HashMap::new();
    emits.insert(article1, vec![0, 1]);
    emits.insert(article2, vec![0, 1]);
    let u = Union::new(emits);
    let article = mig.add_ingredient("article", &["id", "title"], u);
    let articleq = mig.transactional_maintain(article, 0);

    // add vote base table
    let vote = mig.add_ingredient("vote", &["user", "id"], Base {});

    // add vote count
    let vc = mig.add_ingredient("vc",
                                &["id", "votes"],
                                Aggregation::COUNT.over(vote, 0, &[1]));
    let vcq = mig.maintain(vc, 0);

    // add final join using first field from article and first from vc
    let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
        .from(article, vec![1, 0])
        .join(vc, vec![1, 0]);
    let end = mig.add_ingredient("end", &["id", "title", "votes"], j);
    let endq = mig.maintain(end, 0);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    // start processing
    let put = mig.commit();

    let token = articleq(&a1).1;

    // make one article
    assert!(put[&article1].1(vec![a1.clone(), 2.into()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles to see that it was absorbed
    let (res, token) = articleq(&a1);
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);

    // make another article
    assert!(put[&article2].1(vec![a2.clone(), 4.into()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    let (res, mut token) = articleq(&a1);
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);
    let (res, token2) = articleq(&a2);
    assert_eq!(res, vec![vec![a2.clone(), 4.into()]]);

    // Check that the two reads happened transactionally.
    token.merge(token2);
    assert!(validate(&token));

    // create a vote (user 1 votes for article 1)
    assert!(put[&vote].1(vec![1.into(), a1.clone()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query vote count to see that the count was updated
    let res = vcq(&a1);
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq(&a1);
    assert!(res.iter().any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
            "no entry for [1,2,1|2] in {:?}",
            res);
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq(&a2);
    assert!(res.len() <= 1); // could be 1 if we had zero-rows
}

#[test]
fn empty_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    {
        let mig = g.start_migration();
        mig.commit();
    }
    let mut mig = g.start_migration();
    let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
    let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});

    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = mig.add_ingredient("c", &["a", "b"], u);
    let cq = mig.maintain(c, 0);

    let put = mig.commit();
    let id: distributary::DataType = 1.into();

    // send a value on a
    put[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), vec![vec![1.into(), 2.into()]]);

    // update value again
    put[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
}

#[test]
fn simple_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (puta, a, aq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
        let aq = mig.maintain(a, 0);
        let put = mig.commit();
        (put, a, aq)
    };

    // send a value on a
    puta[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(aq(&id), vec![vec![1.into(), 2.into()]]);

    // add unrelated node b in a migration
    let (putb, b, bq) = {
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});
        let bq = mig.maintain(b, 0);
        let put = mig.commit();
        (put, b, bq)
    };

    // send a value on b
    putb[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(bq(&id), vec![vec![1.into(), 4.into()]]);
}

#[test]
fn crossing_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (put, a, b) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});
        (mig.commit(), a, b)
    };

    let mut mig = g.start_migration();
    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = mig.add_ingredient("c", &["a", "b"], u);
    let cq = mig.stream(c);

    mig.commit();

    let id: distributary::DataType = 1.into();

    // send a value on a
    put[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 2.into()]].into()));

    // update value again
    put[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 4.into()]].into()));
}

#[test]
fn independent_domain_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (puta, a, aq, domain) = {
        let mut mig = g.start_migration();
        let domain = mig.add_domain();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
        mig.assign_domain(a, domain);
        let aq = mig.maintain(a, 0);
        let put = mig.commit();
        (put, a, aq, domain)
    };

    // send a value on a
    puta[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(aq(&id), vec![vec![1.into(), 2.into()]]);

    // add unrelated node b in a migration
    let (putb, b, bq) = {
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});
        mig.assign_domain(b, domain);
        let bq = mig.maintain(b, 0);
        let put = mig.commit();
        (put, b, bq)
    };

    // TODO: check that b is actually running in `domain`

    // send a value on b
    putb[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(bq(&id), vec![vec![1.into(), 4.into()]]);
}

#[test]
fn domain_amend_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (put, a, b, domain) = {
        let mut mig = g.start_migration();
        let domain = mig.add_domain();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base {});
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base {});
        mig.assign_domain(a, domain);
        mig.assign_domain(b, domain);
        (mig.commit(), a, b, domain)
    };

    let mut mig = g.start_migration();
    let mut emits = HashMap::new();
    emits.insert(a, vec![0, 1]);
    emits.insert(b, vec![0, 1]);
    let u = distributary::Union::new(emits);
    let c = mig.add_ingredient("c", &["a", "b"], u);
    mig.assign_domain(c, domain);
    let cq = mig.stream(c);

    mig.commit();

    // TODO: check that c is actually running in `domain`

    let id: distributary::DataType = 1.into();

    // send a value on a
    put[&a].0(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 2.into()]].into()));

    // update value again
    put[&b].0(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 4.into()]].into()));
}
