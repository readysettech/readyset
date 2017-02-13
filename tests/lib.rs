extern crate distributary;

use std::time;
use std::thread;
use std::sync::mpsc;

use std::collections::HashMap;

#[test]
fn it_works() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a,b, cq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::new(0));
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::new(0));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.maintain(c, 0);
        mig.commit();
        (a, b, cq)
    };

    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), Ok(vec![vec![1.into(), 2.into()]]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));

    // Delete first record
    muta.delete(id.clone());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), Ok(vec![vec![1.into(), 4.into()]]));

    // Update second record
    mutb.update(vec![id.clone(), 6.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), Ok(vec![vec![1.into(), 6.into()]]));
}

#[test]
fn it_works_streaming() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, cq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.stream(c);
        mig.commit();
        (a, b, cq)
    };

    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 2.into()].into()]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 4.into()].into()]));
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, cq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.maintain(c, 0);
        mig.commit();
        (a, b, cq)
    };

    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a few values on a
    muta.put(vec![id.clone(), 1.into()]);
    muta.put(vec![id.clone(), 2.into()]);
    muta.put(vec![id.clone(), 3.into()]);

    // give them some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    // we should see all the a values
    let res = cq(&id).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // update value again (and again send some secondary updates)
    mutb.put(vec![id.clone(), 4.into()]);
    mutb.put(vec![id.clone(), 5.into()]);
    mutb.put(vec![id.clone(), 6.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id).unwrap();
    assert_eq!(res.len(), 6);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 5.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 6.into()]));
}

#[test]
fn it_works_deletion() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, cq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["x", "y"], distributary::Base::new(1));
        let b = mig.add_ingredient("b", &["_", "x", "y"], distributary::Base::new(2));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![1, 2]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["x", "y"], u);
        let cq = mig.stream(c);
        mig.commit();
        (a, b, cq)
    };

    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);

    // send a value on a
    muta.put(vec![1.into(), 2.into()]);
    assert_eq!(cq.recv(), Ok(vec![vec![1.into(), 2.into()].into()]));

    // update value again
    mutb.put(vec![0.into(), 1.into(), 4.into()]);
    assert_eq!(cq.recv(), Ok(vec![vec![1.into(), 4.into()].into()]));

    // delete first value
    use std::sync::Arc;
    use distributary::StreamUpdate::*;
    muta.delete(2.into());
    assert_eq!(cq.recv(), Ok(vec![DeleteRow(Arc::new(vec![1.into(), 2.into()]))]));
}

#[test]
fn votes() {
    use distributary::{Base, Union, Aggregation, JoinBuilder};

    // set up graph
    let mut g = distributary::Blender::new();
    let (article1, article2, vote, articleq, vcq, endq) = {
        let mut mig = g.start_migration();

        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_ingredient("article1", &["id", "title"], Base::default());
        let article2 = mig.add_ingredient("article1", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        let articleq = mig.maintain(article, 0);

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

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

        // start processing
        mig.commit();
        (article1, article2, vote, articleq, vcq, endq)
    };

    let mut1 = g.get_mutator(article1);
    let mut2 = g.get_mutator(article2);
    let mutv = g.get_mutator(vote);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    // make one article
    mut1.put(vec![a1.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles to see that it was updated
    assert_eq!(articleq(&a1), Ok(vec![vec![a1.clone(), 2.into()]]));

    // make another article
    mut2.put(vec![a2.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(articleq(&a1), Ok(vec![vec![a1.clone(), 2.into()]]));
    assert_eq!(articleq(&a2), Ok(vec![vec![a2.clone(), 4.into()]]));

    // create a vote (user 1 votes for article 1)
    mutv.put(vec![1.into(), a1.clone()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query vote count to see that the count was updated
    let res = vcq(&a1).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq(&a1).unwrap();
    assert!(res.iter().any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
            "no entry for [1,2,1|2] in {:?}",
            res);
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq(&a2).unwrap();
    assert!(res.len() <= 1) // could be 1 if we had zero-rows
}

#[test]
fn transactional_vote() {
    use distributary::{Base, Union, Aggregation, JoinBuilder, Identity};

    // set up graph
    let mut g = distributary::Blender::new();
    let validate = g.get_validator();

    let (article1, article2, vote, articleq, vcq, endq, endq_title, endq_votes) = {
        let mut mig = g.start_migration();

        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_ingredient("article1", &["id", "title"], Base::default());
        let article2 = mig.add_ingredient("article1", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        let articleq = mig.transactional_maintain(article, 0);

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

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
        let end2 = mig.add_ingredient("end2", &["id", "title", "votes"], Identity::new(end));
        let end3 = mig.add_ingredient("end2", &["id", "title", "votes"], Identity::new(end));

        let endq = mig.transactional_maintain(end, 0);
        let endq_title = mig.transactional_maintain(end2, 1);
        let endq_votes = mig.transactional_maintain(end3, 2);

        // start processing
        mig.commit();
        (article1, article2, vote, articleq, vcq, endq, endq_title, endq_votes)
    };

    let mut1 = g.get_mutator(article1);
    let mut2 = g.get_mutator(article2);
    let mutv = g.get_mutator(vote);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    let token = articleq(&a1).unwrap().1;

    let endq_token = endq(&a2).unwrap().1;
    let endq_title_token = endq_title(&4.into()).unwrap().1;
    let endq_votes_token = endq_votes(&0.into()).unwrap().1;

    // make one article
    assert!(mut1.transactional_put(vec![a1.clone(), 2.into()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles to see that it was absorbed
    let (res, token) = articleq(&a1).unwrap();
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);

    // check endq tokens are as expected
    assert!(validate(&endq_token));
    assert!(validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // make another article
    assert!(mut2.transactional_put(vec![a2.clone(), 4.into()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    let (res, mut token) = articleq(&a1).unwrap();
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);
    let (res, token2) = articleq(&a2).unwrap();
    assert_eq!(res, vec![vec![a2.clone(), 4.into()]]);
    // check endq tokens are as expected
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // Check that the two reads happened transactionally.
    token.merge(token2);
    assert!(validate(&token));

    let endq_token = endq(&a1).unwrap().1;
    let endq_title_token = endq_title(&4.into()).unwrap().1;
    let endq_votes_token = endq_votes(&0.into()).unwrap().1;

    // create a vote (user 1 votes for article 1)
    assert!(mutv.transactional_put(vec![1.into(), a1.clone()], token).ok());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check endq tokens
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // query vote count to see that the count was updated
    let res = vcq(&a1).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq(&a1).unwrap().0;
    assert!(res.iter().any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
            "no entry for [1,2,1|2] in {:?}",
            res);
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq(&a2).unwrap().0;
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

    let (a, b, cq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.maintain(c, 0);
        mig.commit();
        (a, b, cq)
    };

    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq(&id), Ok(vec![vec![1.into(), 2.into()]]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    let res = cq(&id).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
}

#[test]
fn simple_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (a, aq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let aq = mig.maintain(a, 0);
        mig.commit();
        (a, aq)
    };
    let muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(aq(&id), Ok(vec![vec![1.into(), 2.into()]]));

    // add unrelated node b in a migration
    let (b, bq) = {
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        let bq = mig.maintain(b, 0);
        mig.commit();
        (b, bq)
    };
    let mutb = g.get_mutator(b);

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that b got it
    assert_eq!(bq(&id), Ok(vec![vec![1.into(), 4.into()]]));
}

#[test]
fn transactional_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, aq) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let aq = mig.transactional_maintain(a, 0);
        mig.commit();
        (a, aq)
    };
    let muta = g.get_mutator(a);

    // send a value on a
    muta.transactional_put(vec![1.into(), 2.into()], distributary::Token::empty());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(aq(&1.into()).unwrap().0, vec![vec![1.into(), 2.into()]]);

    // add unrelated node b in a migration
    let (b, bq) = {
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        let bq = mig.transactional_maintain(b, 0);
        mig.commit();
        (b, bq)
    };
    let mutb = g.get_mutator(b);

    // send a value on b
    mutb.transactional_put(vec![2.into(), 4.into()], distributary::Token::empty());

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that b got it
    assert_eq!(bq(&2.into()).unwrap().0, vec![vec![2.into(), 4.into()]]);

    let cq = {
        let mut mig = g.start_migration();
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.transactional_maintain(c, 0);
        let _ = mig.commit();
        cq
    };

    // check that c has both previous entries
    assert_eq!(aq(&1.into()).unwrap().0, vec![vec![1.into(), 2.into()]]);
    assert_eq!(bq(&2.into()).unwrap().0, vec![vec![2.into(), 4.into()]]);

    // send a value on a and b
    muta.transactional_put(vec![3.into(), 5.into()], distributary::Token::empty());
    mutb.transactional_put(vec![3.into(), 6.into()], distributary::Token::empty());

    // give them some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that c got them
    assert_eq!(cq(&3.into()).unwrap().0,
               vec![vec![3.into(), 5.into()], vec![3.into(), 6.into()]]);
}

#[test]
fn crossing_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        mig.commit();
        (a, b)
    };
    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);

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
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 2.into()].into()]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 4.into()].into()]));
}

#[test]
fn independent_domain_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (a, aq, domain) = {
        let mut mig = g.start_migration();
        let domain = mig.add_domain();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        mig.assign_domain(a, domain);
        let aq = mig.maintain(a, 0);
        mig.commit();
        (a, aq, domain)
    };
    let muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(aq(&id), Ok(vec![vec![1.into(), 2.into()]]));

    // add unrelated node b in a migration
    let (b, bq) = {
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        mig.assign_domain(b, domain);
        let bq = mig.maintain(b, 0);
        mig.commit();
        (b, bq)
    };
    let mutb = g.get_mutator(b);

    // TODO: check that b is actually running in `domain`

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that a got it
    assert_eq!(bq(&id), Ok(vec![vec![1.into(), 4.into()]]));
}

#[test]
fn domain_amend_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, domain) = {
        let mut mig = g.start_migration();
        let domain = mig.add_domain();
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        mig.assign_domain(a, domain);
        mig.assign_domain(b, domain);
        mig.commit();
        (a, b, domain)
    };
    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);

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
    muta.put(vec![id.clone(), 2.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // send a query to c
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 2.into()].into()]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]);

    // give it some time to propagate
    thread::sleep(time::Duration::new(0, 10_000_000));

    // check that value was updated again
    assert_eq!(cq.recv(), Ok(vec![vec![id.clone(), 4.into()].into()]));
}

#[test]
fn state_replay_migration_stream() {
    // we're going to set up a migration test that requires replaying existing state
    // to do that, we'll first create a schema with just a base table, and write some stuff to it.
    // then, we'll do a migration that adds a join in a different domain (requiring state replay),
    // and send through some updates on the other (new) side of the join, and see that the expected
    // things come out the other end.

    let mut g = distributary::Blender::new();
    let a = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["x", "y"], distributary::Base::default());
        mig.commit();
        a
    };
    let muta = g.get_mutator(a);

    // make a couple of records
    muta.put(vec![1.into(), "a".into()]);
    muta.put(vec![1.into(), "b".into()]);
    muta.put(vec![2.into(), "c".into()]);

    let (out, b) = {
        // add a new base and a join
        let mut mig = g.start_migration();
        let b = mig.add_ingredient("b", &["x", "z"], distributary::Base::default());
        let j = distributary::JoinBuilder::new(vec![(a, 0), (a, 1), (b, 1)])
            .from(a, vec![1, 0])
            .join(b, vec![1, 0]);
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // for predictability, ensure the new nodes are in the same domain
        let domain = mig.add_domain();
        mig.assign_domain(b, domain);
        mig.assign_domain(j, domain);

        // we want to observe what comes out of the join
        let out = mig.stream(j);

        // do the migration
        mig.commit();

        (out, b)
    };
    let mutb = g.get_mutator(b);

    // if all went according to plan, the ingress to j's domains hould now contain all the records
    // that we initially inserted into a. thus, when we forward matching things through j, we
    // should see joined output records.

    // there are (/should be) two records in a with x == 1
    mutb.put(vec![1.into(), "n".into()]);
    // they may arrive in any order
    let res = out.recv().unwrap();
    assert!(res.iter().any(|r| r == &vec![1.into(), "a".into(), "n".into()].into()));
    assert!(res.iter().any(|r| r == &vec![1.into(), "b".into(), "n".into()].into()));

    // there are (/should be) one record in a with x == 2
    mutb.put(vec![2.into(), "o".into()]);
    assert_eq!(out.recv(),
               Ok(vec![vec![2.into(), "c".into(), "o".into()].into()]));

    // there should now be no more records
    drop(g);
    assert_eq!(out.recv(), Err(mpsc::RecvError));
}

#[test]
fn migration_depends_on_unchanged_domain() {
    // here's the case we want to test: before the migration, we have some domain that contains
    // some materialized node n, as well as an egress node. after the migration, we add a domain
    // that depends on n being materialized. the tricky part here is that n's domain hasn't changed
    // as far as the system is aware (in particular, because it didn't need to add an egress node).
    // this is tricky, because the system must realize that n is materialized, even though it
    // normally wouldn't even look at that part of the data flow graph!

    let mut g = distributary::Blender::new();
    let left = {
        let mut mig = g.start_migration();

        // base node, so will be materialized
        let left = mig.add_ingredient("foo", &["a", "b"], distributary::Base::default());

        // node in different domain that depends on foo causes egress to be added
        mig.add_ingredient("bar", &["a", "b"], distributary::Identity::new(left));

        // start processing
        mig.commit();
        left
    };

    let mut mig = g.start_migration();

    // joins require their inputs to be materialized
    // we need a new base as well so we can actually make a join
    let tmp = mig.add_ingredient("tmp", &["a", "b"], distributary::Base::default());
    let j = distributary::JoinBuilder::new(vec![(left, 0), (tmp, 1)])
        .from(left, vec![1, 0])
        .join(tmp, vec![1, 0]);
    let j = mig.add_ingredient("join", &["a", "b"], j);

    // we assign tmp and j to the same domain just to make the graph less complex
    let d = mig.add_domain();
    mig.assign_domain(tmp, d);
    mig.assign_domain(j, d);

    // start processing
    mig.commit();
    assert!(true);
}

#[test]
fn full_vote_migration() {
    // we're trying to force a very particular race, namely that a put arrives for a new join
    // *before* its state has been fully initialized. it may take a couple of iterations to hit
    // that, so we run the test a couple of times.
    for _ in 0..5 {
        use distributary::{Blender, Base, JoinBuilder, Aggregation, DataType};
        let mut g = Blender::new();
        let article;
        let vote;
        let vc;
        let end;
        let (article, vote) = {
            // migrate
            let mut mig = g.start_migration();

            // add article base node
            article = mig.add_ingredient("article", &["id", "title"], Base::default());

            // add vote base table
            vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

            // add vote count
            vc = mig.add_ingredient("votecount",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

            // add final join using first field from article and first from vc
            let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
                .from(article, vec![1, 0])
                .join(vc, vec![1, 0]);
            end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

            mig.maintain(end, 0);

            // start processing
            mig.commit();

            (article, vote)
        };
        let muta = g.get_mutator(article);
        let mutv = g.get_mutator(vote);

        let n = 1000i64;
        let title: DataType = "foo".into();
        let voten: DataType = 1.into();
        let raten: DataType = 5.into();

        for i in 0..n {
            muta.put(vec![i.into(), title.clone()]);
        }
        for i in 0..n {
            mutv.put(vec![1.into(), i.into()]);
        }

        // migrate
        let (rating, last) = {
            let mut mig = g.start_migration();

            let domain = mig.add_domain();

            // add new "ratings" base table
            let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base::default());

            // add sum of ratings
            let rs = mig.add_ingredient("rsum",
                                        &["id", "total"],
                                        Aggregation::SUM.over(rating, 2, &[1]));

            // join vote count and rsum (and in theory, sum them)
            let j = JoinBuilder::new(vec![(rs, 0), (rs, 1), (vc, 1)])
                .from(rs, vec![1, 0])
                .join(vc, vec![1, 0]);
            let total = mig.add_ingredient("total", &["id", "ratings", "votes"], j);

            mig.assign_domain(rating, domain);
            mig.assign_domain(rs, domain);
            mig.assign_domain(total, domain);

            // finally, produce end result
            let j = JoinBuilder::new(vec![(article, 0), (article, 1), (total, 1), (total, 2)])
                .from(article, vec![1, 0])
                .join(total, vec![1, 0, 0]);
            let newend = mig.add_ingredient("awr", &["id", "title", "ratings", "votes"], j);
            let last = mig.maintain(newend, 0);

            // start processing
            mig.commit();

            (rating, last)
        };
        let mutr = g.get_mutator(rating);
        for i in 0..n {
            mutr.put(vec![1.into(), i.into(), raten.clone()]);
        }

        // system does about 10k/s = 10/ms
        // wait for twice that before expecting to see results
        thread::sleep(::std::time::Duration::from_millis(2 * n as u64 / 10));
        for i in 0..n {
            let rows = last(&i.into()).unwrap();
            assert!(!rows.is_empty(), "every article should be voted for");
            assert_eq!(rows.len(), 1, "every article should have only one entry");
            let row = rows.into_iter().next().unwrap();
            assert_eq!(row[0],
                       i.into(),
                       "each article result should have the right id");
            assert_eq!(row[1], title, "all articles should have title 'foo'");
            assert_eq!(row[2], raten, "all articles should have one 5-star rating");
            assert_eq!(row[3], voten, "all articles should have one vote");
        }
    }

    assert!(true);
}

#[test]
fn live_writes() {
    use std::time::Duration;
    use distributary::{Blender, Aggregation, DataType};
    let mut g = Blender::new();
    let vc_state;
    let vote;
    let vc;
    {
        // migrate
        let mut mig = g.start_migration();

        // add vote base table
        vote = mig.add_ingredient("vote", &["user", "id"], distributary::Base::default());

        // add vote count
        vc = mig.add_ingredient("votecount",
                                &["id", "votes"],
                                Aggregation::COUNT.over(vote, 0, &[1]));

        vc_state = mig.maintain(vc, 0);

        // start processing
        mig.commit();
    }
    let add = g.get_mutator(vote);

    let ids = 10000;
    let votes = 7;

    // continuously write to vote
    let jh = thread::spawn(move || {
        let user: DataType = 0.into();
        for _ in 0..votes {
            for i in 0..ids {
                add.put(vec![user.clone(), i.into()]);
            }
        }
    });

    // let a few writes through to make migration take a while
    thread::sleep(Duration::from_millis(10));

    // now do a migration that's going to have to copy state
    let mut mig = g.start_migration();
    let vc2 = mig.add_ingredient("votecount2",
                                 &["id", "votes"],
                                 Aggregation::SUM.over(vc, 1, &[0]));
    let vc2_state = mig.maintain(vc2, 0);
    mig.commit();

    // TODO: check that the writer did indeed complete writes during the migration

    // wait for writer to finish
    jh.join().unwrap();

    // allow the system to catch up with the last writes
    thread::sleep(Duration::from_millis(100));

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(vc_state(&i.into()), Ok(vec![vec![i.into(), votes.into()]]));
        assert_eq!(vc2_state(&i.into()), Ok(vec![vec![i.into(), votes.into()]]));
    }
}

#[test]
fn state_replay_migration_query() {
    // similar to test above, except we will have a materialized Reader node that we're going to
    // read from rather than relying on forwarding. to further stress the graph, *both* base nodes
    // are created and populated before the migration, meaning we have to replay through a join.

    let mut g = distributary::Blender::new();
    let (a, b) = {
        let mut mig = g.start_migration();
        let a = mig.add_ingredient("a", &["x", "y"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["x", "z"], distributary::Base::default());

        let domain = mig.add_domain();
        mig.assign_domain(a, domain);
        mig.assign_domain(b, domain);
        mig.commit();

        (a, b)
    };
    let muta = g.get_mutator(a);
    let mutb = g.get_mutator(b);

    // make a couple of records
    muta.put(vec![1.into(), "a".into()]);
    muta.put(vec![1.into(), "b".into()]);
    muta.put(vec![2.into(), "c".into()]);
    mutb.put(vec![1.into(), "n".into()]);
    mutb.put(vec![2.into(), "o".into()]);

    let out = {
        // add join and a reader node
        let mut mig = g.start_migration();
        let j = distributary::JoinBuilder::new(vec![(a, 0), (a, 1), (b, 1)])
            .from(a, vec![1, 0])
            .join(b, vec![1, 0]);
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // we want to observe what comes out of the join
        let out = mig.maintain(j, 0);

        // do the migration
        let _ = mig.commit();

        out
    };

    // if all went according to plan, the join should now be fully populated!
    assert!(!out(&1.into()).is_err());

    // there are (/should be) two records in a with x == 1
    // they may appear in any order
    let res = out(&1.into()).unwrap();
    assert!(res.iter().any(|r| r == &vec![1.into(), "a".into(), "n".into()]));
    assert!(res.iter().any(|r| r == &vec![1.into(), "b".into(), "n".into()]));

    // there are (/should be) one record in a with x == 2
    assert_eq!(out(&2.into()),
               Ok(vec![vec![2.into(), "c".into(), "o".into()]]));

    // there are (/should be) no records with x == 3
    assert!(out(&3.into()).unwrap().is_empty());
}

#[test]
#[ignore]
fn tpc_w() {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = distributary::Blender::new();
    let mut inc = distributary::SqlIncorporator::default();
    let mut mig = g.start_migration();

    let mut f = File::open("tests/tpc-w-queries.txt").unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s.lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| if !(l.ends_with('\n') || l.ends_with(';')) {
            String::from(l) + "\n"
        } else {
            String::from(l)
        })
        .collect();

    // Add them one by one
    for (i, q) in lines.iter().enumerate() {
        println!("{}: {}", i, q);
        println!("{:?}", inc.add_query(q, None, &mut mig));
        // println!("{}", inc.graph);
    }
}
