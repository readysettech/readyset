extern crate distributary;

use std::time;
use std::thread;
use std::sync::mpsc;

use std::collections::HashMap;

const SETTLE_TIME_MS: u64 = 500;

#[test]
fn it_works_basic() {
    // set up graph
    let mut g = distributary::Blender::new();
    let pparams = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::DeleteOnExit,
        128,
        time::Duration::from_millis(1),
    );
    g.with_persistence_options(pparams);
    let (a, b, c) = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["a", "b"],
            distributary::Base::new(vec![]).with_key(vec![0]),
        );
        let b = mig.add_ingredient(
            "b",
            &["a", "b"],
            distributary::Base::new(vec![]).with_key(vec![0]),
        );

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain(c, 0);
        (a, b, c)
    });

    let cq = g.get_getter(c).unwrap();
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to c
    assert_eq!(cq.lookup(&id, true), Ok(vec![vec![1.into(), 2.into()]]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that value was updated again
    let res = cq.lookup(&id, true).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));

    // Delete first record
    muta.delete(vec![id.clone()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to c
    assert_eq!(cq.lookup(&id, true), Ok(vec![vec![1.into(), 4.into()]]));

    // Update second record
    mutb.update(vec![id.clone(), 6.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to c
    assert_eq!(cq.lookup(&id, true), Ok(vec![vec![1.into(), 6.into()]]));
}

#[test]
fn it_works_streaming() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, cq) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.stream(c);
        (a, b, cq)
    });

    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
}

#[test]
fn shared_interdomain_ancestor() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, bq, cq) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);

        let u = distributary::Union::new(emits.clone());
        let b = mig.add_ingredient("b", &["a", "b"], u);
        let bq = mig.stream(b);

        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        let cq = mig.stream(c);
        (a, bq, cq)
    });

    let mut muta = g.get_mutator(a);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    assert_eq!(
        bq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    muta.put(vec![id.clone(), 4.into()]).unwrap();
    assert_eq!(
        bq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b, c) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain(c, 0);
        (a, b, c)
    });

    let cq = g.get_getter(c).unwrap();
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a few values on a
    muta.put(vec![id.clone(), 1.into()]).unwrap();
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    muta.put(vec![id.clone(), 3.into()]).unwrap();

    // give them some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to c
    // we should see all the a values
    let res = cq.lookup(&id, true).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // update value again (and again send some secondary updates)
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
    mutb.put(vec![id.clone(), 5.into()]).unwrap();
    mutb.put(vec![id.clone(), 6.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that value was updated again
    let res = cq.lookup(&id, true).unwrap();
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
    let (a, b, cq) = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["x", "y"],
            distributary::Base::new(vec![]).with_key(vec![1]),
        );
        let b = mig.add_ingredient(
            "b",
            &["_", "x", "y"],
            distributary::Base::new(vec![]).with_key(vec![2]),
        );

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![1, 2]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["x", "y"], u);
        let cq = mig.stream(c);
        (a, b, cq)
    });

    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);

    // send a value on a
    muta.put(vec![1.into(), 2.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![1.into(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![0.into(), 1.into(), 4.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![1.into(), 4.into()].into()])
    );

    // delete first value
    use distributary::StreamUpdate::*;
    muta.delete(vec![2.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![DeleteRow(vec![1.into(), 2.into()])])
    );
}

#[test]
fn it_works_with_sql_recipe() {
    let mut g = distributary::Blender::new();
    let sql = "
        CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));
        CountCars: SELECT COUNT(*) FROM Car WHERE brand = ?;
    ";

    let recipe = g.migrate(|mig| {
        let mut recipe = distributary::Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
        recipe
    });

    let car_index = recipe.node_addr_for("Car").unwrap();
    let count_index = recipe.node_addr_for("CountCars").unwrap();
    let mut mutator = g.get_mutator(car_index);
    let getter = g.get_getter(count_index).unwrap();
    let brands = vec!["Volvo", "Volvo", "Volkswagen"];
    for (i, &brand) in brands.iter().enumerate() {
        let id = i as i32;
        mutator.put(vec![id.into(), brand.into()]).unwrap();
    }

    // Let writes propagate:
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // Retrieve the result of the count query:
    let result = getter.lookup(&"Volvo".into(), true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], 2.into());
}

#[test]
fn it_works_with_simple_arithmetic() {
    let mut g = distributary::Blender::new();
    let sql = "
        CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
        CarPrice: SELECT 2 * price FROM Car WHERE id = ?;
    ";

    let recipe = g.migrate(|mig| {
        let mut recipe = distributary::Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
        recipe
    });

    let car_index = recipe.node_addr_for("Car").unwrap();
    let count_index = recipe.node_addr_for("CarPrice").unwrap();
    let mut mutator = g.get_mutator(car_index);
    let getter = g.get_getter(count_index).unwrap();
    let id: distributary::DataType = 1.into();
    let price: distributary::DataType = 123.into();
    mutator.put(vec![id.clone(), price]).unwrap();

    // Let writes propagate:
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // Retrieve the result of the count query:
    let result = getter.lookup(&id, true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], 246.into());
}

#[test]
fn it_works_with_join_arithmetic() {
    let mut g = distributary::Blender::new();
    let sql = "
        CREATE TABLE Car (car_id int, price_id int, PRIMARY KEY(car_id));
        CREATE TABLE Price (price_id int, price int, PRIMARY KEY(price_id));
        CREATE TABLE Sales (sales_id int, price_id int, fraction float, PRIMARY KEY(sales_id));
        CarPrice: SELECT price * fraction FROM Car \
                  JOIN Price ON Car.price_id = Price.price_id \
                  JOIN Sales ON Price.price_id = Sales.price_id \
                  WHERE car_id = ?;
    ";

    let recipe = g.migrate(|mig| {
        let mut recipe = distributary::Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
        recipe
    });

    let car_index = recipe.node_addr_for("Car").unwrap();
    let price_index = recipe.node_addr_for("Price").unwrap();
    let sales_index = recipe.node_addr_for("Sales").unwrap();
    let query_index = recipe.node_addr_for("CarPrice").unwrap();
    let mut car_mutator = g.get_mutator(car_index);
    let mut price_mutator = g.get_mutator(price_index);
    let mut sales_mutator = g.get_mutator(sales_index);
    let getter = g.get_getter(query_index).unwrap();
    let id = 1;
    let price = 123;
    let fraction = 0.7;
    car_mutator.put(vec![id.into(), id.into()]).unwrap();
    price_mutator.put(vec![id.into(), price.into()]).unwrap();
    sales_mutator.put(vec![id.into(), id.into(), fraction.into()]).unwrap();

    // Let writes propagate:
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // Retrieve the result of the count query:
    let result = getter.lookup(&id.into(), true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (price as f64 * fraction).into());
}

#[test]
fn it_works_with_function_arithmetic() {
    let mut g = distributary::Blender::new();
    let sql = "
        CREATE TABLE Bread (id int, price int, PRIMARY KEY(id));
        Price: SELECT 2 * MAX(price) FROM Bread;
    ";

    let recipe = g.migrate(|mig| {
        let mut recipe = distributary::Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
        recipe
    });

    let bread_index = recipe.node_addr_for("Bread").unwrap();
    let query_index = recipe.node_addr_for("Price").unwrap();
    let mut mutator = g.get_mutator(bread_index);
    let getter = g.get_getter(query_index).unwrap();
    let max_price = 20;
    for (i, price) in (10..max_price + 1).enumerate() {
        let id = (i + 1) as i32;
        mutator.put(vec![id.into(), price.into()]).unwrap();
    }

    // Let writes propagate:
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // Retrieve the result of the count query:
    let key = distributary::DataType::BigInt(max_price * 2);
    let result = getter.lookup(&key, true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], key);
}

#[test]
fn votes() {
    use distributary::{Aggregation, Base, Join, JoinType, Union};

    // set up graph
    let mut g = distributary::Blender::new();
    let (article1, article2, vote, article, vc, end) = g.migrate(|mig| {
        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_ingredient("article1", &["id", "title"], Base::default());
        let article2 = mig.add_ingredient("article1", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        mig.maintain(article, 0);

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "vc",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );
        mig.maintain(vc, 0);

        // add final join using first field from article and first from vc
        use distributary::JoinSource::*;
        let j = Join::new(article, vc, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("end", &["id", "title", "votes"], j);
        mig.maintain(end, 0);

        (article1, article2, vote, article, vc, end)
    });

    let articleq = g.get_getter(article).unwrap();
    let vcq = g.get_getter(vc).unwrap();
    let endq = g.get_getter(end).unwrap();

    let mut mut1 = g.get_mutator(article1);
    let mut mut2 = g.get_mutator(article2);
    let mut mutv = g.get_mutator(vote);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    // make one article
    mut1.put(vec![a1.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // query articles to see that it was updated
    assert_eq!(
        articleq.lookup(&a1, true),
        Ok(vec![vec![a1.clone(), 2.into()]])
    );

    // make another article
    mut2.put(vec![a2.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(
        articleq.lookup(&a1, true),
        Ok(vec![vec![a1.clone(), 2.into()]])
    );
    assert_eq!(
        articleq.lookup(&a2, true),
        Ok(vec![vec![a2.clone(), 4.into()]])
    );

    // create a vote (user 1 votes for article 1)
    mutv.put(vec![1.into(), a1.clone()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // query vote count to see that the count was updated
    let res = vcq.lookup(&a1, true).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq.lookup(&a1, true).unwrap();
    assert!(
        res.iter().any(|r| {
            r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()
        }),
        "no entry for [1,2,1|2] in {:?}",
        res
    );
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq.lookup(&a2, true).unwrap();
    assert!(res.len() <= 1) // could be 1 if we had zero-rows
}

#[test]
fn transactional_vote() {
    use distributary::{Aggregation, Base, Identity, Join, JoinType, Union};

    // set up graph
    let mut g = distributary::Blender::new();
    g.disable_partial(); // because end_votes forces full below partial
    let validate = g.get_validator();

    let (article1, article2, vote, article, vc, end, end_title, end_votes) = g.migrate(|mig| {
        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_transactional_base("article1", &["id", "title"], Base::default());
        let article2 = mig.add_transactional_base("article1", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        mig.maintain(article, 0);

        // add vote base table
        let vote = mig.add_transactional_base("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "vc",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );
        mig.maintain(vc, 0);

        // add final join using first field from article and first from vc
        use distributary::JoinSource::*;
        let j = Join::new(article, vc, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("end", &["id", "title", "votes"], j);
        let end_title = mig.add_ingredient("end2", &["id", "title", "votes"], Identity::new(end));
        let end_votes = mig.add_ingredient("end2", &["id", "title", "votes"], Identity::new(end));

        mig.maintain(end, 0);
        mig.maintain(end_title, 1);
        mig.maintain(end_votes, 2);

        (
            article1,
            article2,
            vote,
            article,
            vc,
            end,
            end_title,
            end_votes,
        )
    });

    let mut articleq = g.get_getter(article).unwrap();
    let vcq = g.get_getter(vc).unwrap();
    let mut endq = g.get_getter(end).unwrap();
    let mut endq_title = g.get_getter(end_title).unwrap();
    let mut endq_votes = g.get_getter(end_votes).unwrap();

    let mut mut1 = g.get_mutator(article1);
    let mut mut2 = g.get_mutator(article2);
    let mut mutv = g.get_mutator(vote);

    let a1: distributary::DataType = 1.into();
    let a2: distributary::DataType = 2.into();

    let token = articleq.transactional_lookup(&a1).unwrap().1;

    let endq_token = endq.transactional_lookup(&a2).unwrap().1;
    let endq_title_token = endq_title.transactional_lookup(&4.into()).unwrap().1;
    let endq_votes_token = endq_votes.transactional_lookup(&0.into()).unwrap().1;

    // make one article
    assert!(
        mut1.transactional_put(vec![a1.clone(), 2.into()], token)
            .is_ok()
    );

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // query articles to see that it was absorbed
    let (res, token) = articleq.transactional_lookup(&a1).unwrap();
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);

    // check endq tokens are as expected
    assert!(validate(&endq_token));
    assert!(validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // make another article
    assert!(
        mut2.transactional_put(vec![a2.clone(), 4.into()], token)
            .is_ok()
    );

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    let (res, mut token) = articleq.transactional_lookup(&a1).unwrap();
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);
    let (res, token2) = articleq.transactional_lookup(&a2).unwrap();
    assert_eq!(res, vec![vec![a2.clone(), 4.into()]]);
    // check endq tokens are as expected
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // Check that the two reads happened transactionally.
    token.merge(token2);
    assert!(validate(&token));

    let endq_token = endq.transactional_lookup(&a1).unwrap().1;
    let endq_title_token = endq_title.transactional_lookup(&4.into()).unwrap().1;
    let endq_votes_token = endq_votes.transactional_lookup(&0.into()).unwrap().1;

    // create a vote (user 1 votes for article 1)
    assert!(
        mutv.transactional_put(vec![1.into(), a1.clone()], token)
            .is_ok()
    );

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check endq tokens
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // query vote count to see that the count was updated
    let res = vcq.lookup(&a1, true).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq.transactional_lookup(&a1).unwrap().0;
    assert_eq!(res.len(), 1);
    assert!(
        res.iter().any(|r| {
            r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()
        }),
        "no entry for [1,2,1|2] in {:?}",
        res
    );

    // check that article 2 doesn't have any votes
    let res = endq.transactional_lookup(&a2).unwrap().0;
    assert!(res.len() <= 1); // could be 1 if we had zero-rows
}

#[test]
fn empty_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    g.migrate(|_| {});

    let (a, b, c) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain(c, 0);
        (a, b, c)
    });

    let cq = g.get_getter(c).unwrap();
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);
    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to c
    assert_eq!(cq.lookup(&id, true), Ok(vec![vec![1.into(), 2.into()]]));

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that value was updated again
    let res = cq.lookup(&id, true).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
}

#[test]
fn simple_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        mig.maintain(a, 0);
        a
    });

    let aq = g.get_getter(a).unwrap();
    let mut muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that a got it
    assert_eq!(aq.lookup(&id, true), Ok(vec![vec![1.into(), 2.into()]]));

    // add unrelated node b in a migration
    let b = g.migrate(|mig| {
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        mig.maintain(b, 0);
        b
    });

    let bq = g.get_getter(b).unwrap();
    let mut mutb = g.get_mutator(b);

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that b got it
    assert_eq!(bq.lookup(&id, true), Ok(vec![vec![1.into(), 4.into()]]));
}

#[test]
fn add_columns() {
    let id: distributary::DataType = "x".into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (a, aq) = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["a", "b"],
            distributary::Base::new(vec![1.into(), 2.into()]),
        );
        let aq = mig.stream(a);
        (a, aq)
    });
    let mut muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), "y".into()]).unwrap();

    // check that a got it
    assert_eq!(
        aq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "y".into()].into()])
    );

    // add a third column to a
    g.migrate(|mig| {
        mig.add_column(a, "c", 3.into());
    });

    // send another (old) value on a
    muta.put(vec![id.clone(), "z".into()]).unwrap();

    // check that a got it, and added the new, third column's default
    assert_eq!(
        aq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "z".into(), 3.into()].into()])
    );

    // get a new muta and send a new value on it
    let mut muta = g.get_mutator(a);
    muta.put(vec![id.clone(), "a".into(), 10.into()]).unwrap();

    // check that a got it, and included the third column
    assert_eq!(
        aq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "a".into(), 10.into()].into()])
    );
}

#[test]
fn migrate_added_columns() {
    let id: distributary::DataType = "x".into();

    // set up graph
    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["a", "b"],
            distributary::Base::new(vec![1.into(), 2.into()]),
        );
        a
    });
    let mut muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), "y".into()]).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // add a third column to a, and a view that uses it
    let b = g.migrate(|mig| {
        mig.add_column(a, "c", 3.into());
        let b = mig.add_ingredient(
            "x",
            &["c", "b"],
            distributary::Project::new(a, &[2, 0], None, None),
        );
        mig.maintain(b, 1);
        b
    });

    let bq = g.get_getter(b).unwrap();

    // send another (old) value on a
    muta.put(vec![id.clone(), "z".into()]).unwrap();
    // and an entirely new value
    let mut muta = g.get_mutator(a);
    muta.put(vec![id.clone(), "a".into(), 10.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // we should now see the pre-migration write and the old post-migration write with the default
    // value, and the new post-migration write with the value it contained.
    let res = bq.lookup(&id, true).unwrap();
    assert_eq!(res.len(), 3);
    assert_eq!(
        res.iter()
            .filter(|&r| r == &vec![3.into(), id.clone()])
            .count(),
        2
    );
    assert!(res.iter().any(|r| r == &vec![10.into(), id.clone()]));
}

#[test]
fn migrate_drop_columns() {
    let id: distributary::DataType = "x".into();

    // set up graph
    let mut g = distributary::Blender::new();
    let (a, stream) = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["a", "b"],
            distributary::Base::new(vec!["a".into(), "b".into()]),
        );
        let stream = mig.stream(a);
        (a, stream)
    });
    let mut muta1 = g.get_mutator(a);

    // send a value on a
    muta1.put(vec![id.clone(), "bx".into()]).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // drop a column
    g.migrate(|mig| {
        mig.drop_column(a, 1);
    });

    // new mutator should only require one column
    // and should inject default for a.b
    let mut muta2 = g.get_mutator(a);
    muta2.put(vec![id.clone()]).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // add a new column
    g.migrate(|mig| {
        mig.add_column(a, "c", "c".into());
    });

    // new mutator allows putting two values, and injects default for a.b
    let mut muta3 = g.get_mutator(a);
    muta3.put(vec![id.clone(), "cy".into()]).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // using an old putter now should add default for c
    muta1.put(vec![id.clone(), "bz".into()]).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // using putter that knows of neither b nor c should result in defaults for both
    muta2.put(vec![id.clone()]).unwrap();

    assert_eq!(
        stream.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "bx".into()].into()])
    );
    assert_eq!(
        stream.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "b".into()].into()])
    );
    assert_eq!(
        stream.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "b".into(), "cy".into()].into()])
    );
    assert_eq!(
        stream.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "bz".into(), "c".into()].into()])
    );
    assert_eq!(
        stream.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), "b".into(), "c".into()].into()])
    );
    assert_eq!(stream.try_recv(), Err(mpsc::TryRecvError::Empty));
}

#[test]
fn key_on_added() {
    // set up graph
    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient(
            "a",
            &["a", "b"],
            distributary::Base::new(vec![1.into(), 2.into()]),
        );
        a
    });

    // add a maintained view keyed on newly added column
    let b = g.migrate(|mig| {
        mig.add_column(a, "c", 3.into());
        let b = mig.add_ingredient(
            "x",
            &["c", "b"],
            distributary::Project::new(a, &[2, 1], None, None),
        );
        mig.maintain(b, 0);
        b
    });

    // make sure we can read (may trigger a replay)
    let bq = g.get_getter(b).unwrap();
    assert!(bq.lookup(&3.into(), true).unwrap().is_empty());
}

#[test]
fn replay_during_replay() {
    // what we're trying to set up here is a case where a join receives a record with a value for
    // the join key that does not exist in the view the record was sent from. since joins only do
    // lookups into the origin view during forward processing when it receives things from the
    // right in a left join, that's what we have to construct.
    let mut g = distributary::Blender::new();
    g.disable_sharding();
    let (a, u1, u2) = g.migrate(|mig| {
        // we need three bases:
        //
        //  - a will be the left side of the left join
        //  - u1 and u2 will be joined together with a regular one-to-one join to produce a partial
        //    view (remember, we need to miss in the source of the replay, so it must be partial).
        let a = mig.add_ingredient("a", &["a"], distributary::Base::new(vec![1.into()]));
        let u1 = mig.add_ingredient("u1", &["u"], distributary::Base::new(vec![1.into()]));
        let u2 = mig.add_ingredient(
            "u2",
            &["u", "a"],
            distributary::Base::new(vec![1.into(), 2.into()]),
        );
        (a, u1, u2)
    });

    // add our joins
    let (u, target) = g.migrate(|mig| {
        use distributary::{Join, JoinType};
        use distributary::JoinSource::*;
        // u = u1 * u2
        let j = Join::new(u1, u2, JoinType::Inner, vec![B(0, 0), R(1)]);
        let u = mig.add_ingredient("u", &["u", "a"], j);
        let j = Join::new(a, u, JoinType::Left, vec![B(0, 1), R(0)]);
        let end = mig.add_ingredient("end", &["a", "u"], j);
        mig.maintain(end, 0);
        (u, end)
    });

    // at this point, there's no secondary index on `u`, so any records that are forwarded from `u`
    // must already be present in the one index that `u` has. let's do some writes and check that
    // nothing crashes.

    let mut muta = g.get_mutator(a);
    let mut mutu1 = g.get_mutator(u1);
    let mut mutu2 = g.get_mutator(u2);

    // as are numbers
    muta.put(vec![1.into()]).unwrap();
    muta.put(vec![2.into()]).unwrap();
    muta.put(vec![3.into()]).unwrap();

    // us are strings
    mutu1.put(vec!["a".into()]).unwrap();
    mutu1.put(vec!["b".into()]).unwrap();
    mutu1.put(vec!["c".into()]).unwrap();

    // we want there to be data for all keys
    mutu2.put(vec!["a".into(), 1.into()]).unwrap();
    mutu2.put(vec!["b".into(), 2.into()]).unwrap();
    mutu2.put(vec!["c".into(), 3.into()]).unwrap();

    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // since u and target are both partial, the writes should not actually have propagated through
    // yet. do a read to see that one makes it through correctly:
    let r = g.get_getter(target).unwrap();

    assert_eq!(
        r.lookup(&1.into(), true),
        Ok(vec![vec![1.into(), "a".into()]])
    );

    // we now know that u has key a=1 in its index
    // now we add a secondary index on u.u
    g.migrate(|mig| {
        mig.maintain(u, 0);
    });

    let second = g.get_getter(u).unwrap();

    // second is partial and empty, so any read should trigger a replay.
    // though that shouldn't interact with target in any way.
    assert_eq!(
        second.lookup(&"a".into(), true),
        Ok(vec![vec!["a".into(), 1.into()]])
    );

    // now we get to the funky part.
    // we're going to request a second key from the secondary index on `u`, which causes that hole
    // to disappear. then we're going to do a write to `u2` that has that second key, but has an
    // "a" value for which u has a hole. that record is then going to be forwarded to *both*
    // children, and it'll be interesting to see what the join then does.
    assert_eq!(
        second.lookup(&"b".into(), true),
        Ok(vec![vec!["b".into(), 2.into()]])
    );

    // u has a hole for a=2, but not for u=b, and so should forward this to both children
    mutu2.put(vec!["b".into(), 2.into()]).unwrap();

    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // what happens if we now query for 2?
    assert_eq!(
        r.lookup(&2.into(), true),
        Ok(vec![vec![2.into(), "b".into()], vec![2.into(), "b".into()]])
    );
}

#[test]
fn full_aggregation_with_bogokey() {
    // set up graph
    let mut g = distributary::Blender::new();
    let base = g.migrate(|mig| {
        mig.add_ingredient("base", &["x"], distributary::Base::new(vec![1.into()]))
    });

    // add an aggregation over the base with a bogo key.
    // in other words, the aggregation is across all rows.
    let agg = g.migrate(|mig| {
        let bogo = mig.add_ingredient(
            "bogo",
            &["x", "bogo"],
            distributary::Project::new(base, &[0], Some(vec![0.into()]), None),
        );
        let agg = mig.add_ingredient(
            "agg",
            &["bogo", "count"],
            distributary::Aggregation::COUNT.over(bogo, 0, &[1]),
        );
        mig.maintain(agg, 0);
        agg
    });

    let aggq = g.get_getter(agg).unwrap();
    let mut base = g.get_mutator(base);

    // insert some values
    base.put(vec![1.into()]).unwrap();
    base.put(vec![2.into()]).unwrap();
    base.put(vec![3.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // send a query to aggregation materialization
    assert_eq!(
        aggq.lookup(&0.into(), true),
        Ok(vec![vec![0.into(), 3.into()]])
    );

    // update value again
    base.put(vec![4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that value was updated again
    assert_eq!(
        aggq.lookup(&0.into(), true),
        Ok(vec![vec![0.into(), 4.into()]])
    );
}

#[test]
fn transactional_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_transactional_base("a", &["a", "b"], distributary::Base::default());
        mig.maintain(a, 0);
        a
    });

    let mut aq = g.get_getter(a).unwrap();
    let mut muta = g.get_mutator(a);

    // send a value on a
    muta.transactional_put(vec![1.into(), 2.into()], distributary::Token::empty())
        .unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that a got it
    assert_eq!(
        aq.transactional_lookup(&1.into()).unwrap().0,
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let b = g.migrate(|mig| {
        let b = mig.add_transactional_base("b", &["a", "b"], distributary::Base::default());
        mig.maintain(b, 0);
        b
    });

    let mut bq = g.get_getter(b).unwrap();
    let mut mutb = g.get_mutator(b);

    // send a value on b
    mutb.transactional_put(vec![2.into(), 4.into()], distributary::Token::empty())
        .unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that b got it
    assert_eq!(
        bq.transactional_lookup(&2.into()).unwrap().0,
        vec![vec![2.into(), 4.into()]]
    );

    let c = g.migrate(|mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain(c, 0);
        c
    });

    let mut cq = g.get_getter(c).unwrap();

    // check that c has both previous entries
    assert_eq!(
        aq.transactional_lookup(&1.into()).unwrap().0,
        vec![vec![1.into(), 2.into()]]
    );
    assert_eq!(
        bq.transactional_lookup(&2.into()).unwrap().0,
        vec![vec![2.into(), 4.into()]]
    );

    // send a value on a and b
    muta.transactional_put(vec![3.into(), 5.into()], distributary::Token::empty())
        .unwrap();
    mutb.transactional_put(vec![3.into(), 6.into()], distributary::Token::empty())
        .unwrap();

    // give them some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that c got them
    assert_eq!(
        cq.transactional_lookup(&3.into()).unwrap().0,
        vec![vec![3.into(), 5.into()], vec![3.into(), 6.into()]]
    );
}

#[test]
fn crossing_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        (a, b)
    });
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);

    let cq = g.migrate(|mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.stream(c)
    });

    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
}

#[test]
fn independent_domain_migration() {
    let id: distributary::DataType = 1.into();

    // set up graph
    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        mig.maintain(a, 0);
        a
    });

    let aq = g.get_getter(a).unwrap();
    let mut muta = g.get_mutator(a);

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that a got it
    assert_eq!(aq.lookup(&id, true), Ok(vec![vec![1.into(), 2.into()]]));

    // add unrelated node b in a migration
    let b = g.migrate(|mig| {
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        mig.maintain(b, 0);
        b
    });

    let bq = g.get_getter(b).unwrap();
    let mut mutb = g.get_mutator(b);

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // check that a got it
    assert_eq!(bq.lookup(&id, true), Ok(vec![vec![1.into(), 4.into()]]));
}

#[test]
fn domain_amend_migration() {
    // set up graph
    let mut g = distributary::Blender::new();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], distributary::Base::default());
        (a, b)
    });
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);

    let cq = g.migrate(|mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = distributary::Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.stream(c)
    });

    let id: distributary::DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
    assert_eq!(
        cq.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
}

#[test]
#[ignore]
// this test is ignored because partial materialization does not forward for keys unless they are
// explicitly queried for. to re-add support for streaming consumers of Readers, we would need to
// add a mechanism for registering interesting a key (effectively triggering a replay of that key
// when called). this should be fairly straightforward to add in the existing infrastructure (just
// use the same trigger that's given to the `backlog::ReadHandle` when it is partial), but it's
// work we're fine putting off for now.
fn state_replay_migration_stream() {
    // we're going to set up a migration test that requires replaying existing state
    // to do that, we'll first create a schema with just a base table, and write some stuff to it.
    // then, we'll do a migration that adds a join in a different domain (requiring state replay),
    // and send through some updates on the other (new) side of the join, and see that the expected
    // things come out the other end.

    let mut g = distributary::Blender::new();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["x", "y"], distributary::Base::default());
        a
    });
    let mut muta = g.get_mutator(a);

    // make a couple of records
    muta.put(vec![1.into(), "a".into()]).unwrap();
    muta.put(vec![1.into(), "b".into()]).unwrap();
    muta.put(vec![2.into(), "c".into()]).unwrap();

    let (out, b) = g.migrate(|mig| {
        // add a new base and a join
        let b = mig.add_ingredient("b", &["x", "z"], distributary::Base::default());
        use distributary::JoinSource::*;
        let j = distributary::Join::new(
            a,
            b,
            distributary::JoinType::Inner,
            vec![B(0, 0), L(1), R(1)],
        );
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // we want to observe what comes out of the join
        let out = mig.stream(j);

        (out, b)
    });
    let mut mutb = g.get_mutator(b);

    // if all went according to plan, the ingress to j's domains hould now contain all the records
    // that we initially inserted into a. thus, when we forward matching things through j, we
    // should see joined output records.

    // there are (/should be) two records in a with x == 1
    mutb.put(vec![1.into(), "n".into()]).unwrap();
    // they may arrive in any order
    let res = out.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS))
        .unwrap();
    assert!(
        res.iter()
            .any(|r| r == &vec![1.into(), "a".into(), "n".into()].into())
    );
    assert!(
        res.iter()
            .any(|r| r == &vec![1.into(), "b".into(), "n".into()].into())
    );

    // there are (/should be) one record in a with x == 2
    mutb.put(vec![2.into(), "o".into()]).unwrap();
    assert_eq!(
        out.recv_timeout(time::Duration::from_millis(SETTLE_TIME_MS)),
        Ok(vec![vec![2.into(), "c".into(), "o".into()].into()])
    );

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
    let left = g.migrate(|mig| {
        // base node, so will be materialized
        let left = mig.add_ingredient("foo", &["a", "b"], distributary::Base::default());

        // node in different domain that depends on foo causes egress to be added
        mig.add_ingredient("bar", &["a", "b"], distributary::Identity::new(left));
        left
    });

    g.migrate(|mig| {
        // joins require their inputs to be materialized
        // we need a new base as well so we can actually make a join
        let tmp = mig.add_ingredient("tmp", &["a", "b"], distributary::Base::default());
        let j = distributary::Join::new(
            left,
            tmp,
            distributary::JoinType::Inner,
            vec![
                distributary::JoinSource::B(0, 0),
                distributary::JoinSource::R(1),
            ],
        );
        mig.add_ingredient("join", &["a", "b"], j);
    });
    assert!(true);
}

fn do_full_vote_migration(old_puts_after: bool) {
    use distributary::{Aggregation, Base, Blender, DataType, Join, JoinType};
    let mut g = Blender::new();
    let (article, vote, vc, end) = g.migrate(|mig| {
        // migrate

        // add article base node
        let article = mig.add_ingredient("article", &["id", "title"], Base::default());

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default().with_key(vec![1]));

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        // add final join using first field from article and first from vc
        use distributary::JoinSource::*;
        let j = Join::new(article, vc, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        mig.maintain(end, 0);
        (article, vote, vc, end)
    });
    let mut muta = g.get_mutator(article);
    let mut mutv = g.get_mutator(vote);

    let n = 250i64;
    let title: DataType = "foo".into();
    let raten: DataType = 5.into();

    for i in 0..n {
        muta.put(vec![i.into(), title.clone()]).unwrap();
    }
    for i in 0..n {
        mutv.put(vec![1.into(), i.into()]).unwrap();
    }

    let last = g.get_getter(end).unwrap();
    thread::sleep(time::Duration::from_millis(3 * SETTLE_TIME_MS));
    for i in 0..n {
        let rows = last.lookup(&i.into(), true).unwrap();
        assert!(!rows.is_empty(), "every article should be voted for");
        assert_eq!(rows.len(), 1, "every article should have only one entry");
        let row = rows.into_iter().next().unwrap();
        assert_eq!(
            row[0],
            i.into(),
            "each article result should have the right id"
        );
        assert_eq!(row[1], title, "all articles should have title 'foo'");
        assert_eq!(row[2], 1.into(), "all articles should have one vote");
    }

    // migrate
    let (rating, last) = g.migrate(|mig| {
        // add new "ratings" base table
        let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base::default());

        // add sum of ratings
        let rs = mig.add_ingredient(
            "rsum",
            &["id", "total"],
            Aggregation::SUM.over(rating, 2, &[1]),
        );

        // join vote count and rsum (and in theory, sum them)
        use distributary::JoinSource::*;
        let j = Join::new(rs, vc, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let total = mig.add_ingredient("total", &["id", "ratings", "votes"], j);

        // finally, produce end result
        let j = Join::new(
            article,
            total,
            JoinType::Inner,
            vec![B(0, 0), L(1), R(1), R(2)],
        );
        let newend = mig.add_ingredient("awr", &["id", "title", "ratings", "votes"], j);
        mig.maintain(newend, 0);
        (rating, newend)
    });

    let last = g.get_getter(last).unwrap();
    let mut mutr = g.get_mutator(rating);
    for i in 0..n {
        if old_puts_after {
            mutv.put(vec![1.into(), i.into()]).unwrap();
        }
        mutr.put(vec![1.into(), i.into(), raten.clone()]).unwrap();
    }

    thread::sleep(time::Duration::from_millis(3 * SETTLE_TIME_MS));
    for i in 0..n {
        let rows = last.lookup(&i.into(), true).unwrap();
        assert!(!rows.is_empty(), "every article should be voted for");
        assert_eq!(rows.len(), 1, "every article should have only one entry");
        let row = rows.into_iter().next().unwrap();
        assert_eq!(
            row[0],
            i.into(),
            "each article result should have the right id"
        );
        assert_eq!(row[1], title, "all articles should have title 'foo'");
        assert_eq!(row[2], raten, "all articles should have one 5-star rating");
        if old_puts_after {
            assert_eq!(row[3], 2.into(), "all articles should have two votes");
        } else {
            assert_eq!(row[3], 1.into(), "all articles should have one vote");
        }
    }
}

#[test]
fn full_vote_migration_only_new() {
    do_full_vote_migration(false);
}

#[test]
fn full_vote_migration_new_and_old() {
    do_full_vote_migration(true);
}

#[test]
fn live_writes() {
    use std::time::Duration;
    use distributary::{Aggregation, Blender, DataType};
    let mut g = Blender::new();
    let (vote, vc) = g.migrate(|mig| {
        // migrate

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], distributary::Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        mig.maintain(vc, 0);
        (vote, vc)
    });

    let vc_state = g.get_getter(vc).unwrap();
    let mut add = g.get_mutator(vote);

    let ids = 1000;
    let votes = 7;

    // continuously write to vote
    let jh = thread::spawn(move || {
        let user: DataType = 0.into();
        for _ in 0..votes {
            for i in 0..ids {
                add.put(vec![user.clone(), i.into()]).unwrap();
            }
        }
    });

    // let a few writes through to make migration take a while
    thread::sleep(Duration::from_millis(SETTLE_TIME_MS));

    // now do a migration that's going to have to copy state
    let vc2 = g.migrate(|mig| {
        let vc2 = mig.add_ingredient(
            "votecount2",
            &["id", "votes"],
            Aggregation::SUM.over(vc, 1, &[0]),
        );
        mig.maintain(vc2, 0);
        vc2
    });

    let vc2_state = g.get_getter(vc2).unwrap();

    // TODO: check that the writer did indeed complete writes during the migration

    // wait for writer to finish
    jh.join().unwrap();

    // allow the system to catch up with the last writes
    thread::sleep(Duration::from_millis(SETTLE_TIME_MS));

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&i.into(), true),
            Ok(vec![vec![i.into(), votes.into()]])
        );
        assert_eq!(
            vc2_state.lookup(&i.into(), true),
            Ok(vec![vec![i.into(), votes.into()]])
        );
    }
}

#[test]
fn state_replay_migration_query() {
    // similar to test above, except we will have a materialized Reader node that we're going to
    // read from rather than relying on forwarding. to further stress the graph, *both* base nodes
    // are created and populated before the migration, meaning we have to replay through a join.

    let mut g = distributary::Blender::new();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["x", "y"], distributary::Base::default());
        let b = mig.add_ingredient("b", &["x", "z"], distributary::Base::default());

        (a, b)
    });
    let mut muta = g.get_mutator(a);
    let mut mutb = g.get_mutator(b);

    // make a couple of records
    muta.put(vec![1.into(), "a".into()]).unwrap();
    muta.put(vec![1.into(), "b".into()]).unwrap();
    muta.put(vec![2.into(), "c".into()]).unwrap();
    mutb.put(vec![1.into(), "n".into()]).unwrap();
    mutb.put(vec![2.into(), "o".into()]).unwrap();

    let out = g.migrate(|mig| {
        // add join and a reader node
        use distributary::JoinSource::*;
        let j = distributary::Join::new(
            a,
            b,
            distributary::JoinType::Inner,
            vec![B(0, 0), L(1), R(1)],
        );
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // we want to observe what comes out of the join
        mig.maintain(j, 0);
        j
    });
    let out = g.get_getter(out).unwrap();
    thread::sleep(time::Duration::from_millis(SETTLE_TIME_MS));

    // if all went according to plan, the join should now be fully populated!
    // there are (/should be) two records in a with x == 1
    // they may appear in any order
    let res = out.lookup(&1.into(), true).unwrap();
    assert!(
        res.iter()
            .any(|r| r == &vec![1.into(), "a".into(), "n".into()])
    );
    assert!(
        res.iter()
            .any(|r| r == &vec![1.into(), "b".into(), "n".into()])
    );

    // there are (/should be) one record in a with x == 2
    assert_eq!(
        out.lookup(&2.into(), true),
        Ok(vec![vec![2.into(), "c".into(), "o".into()]])
    );

    // there are (/should be) no records with x == 3
    assert!(out.lookup(&3.into(), true).unwrap().is_empty());
}

#[test]
fn recipe_activates() {
    let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
    let mut r = distributary::Recipe::from_str(r_txt, None).unwrap();
    assert_eq!(r.version(), 0);
    assert_eq!(r.expressions().len(), 1);
    assert_eq!(r.prior(), None);

    let mut g = distributary::Blender::new();
    g.migrate(|mig| {
        assert!(r.activate(mig, false).is_ok());
    });
    // one base node
    assert_eq!(g.inputs().len(), 1);
}

#[test]
fn recipe_activates_and_migrates() {
    let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
    let mut r = distributary::Recipe::from_str(r_txt, None).unwrap();
    assert_eq!(r.version(), 0);
    assert_eq!(r.expressions().len(), 1);
    assert_eq!(r.prior(), None);

    let mut g = distributary::Blender::new();
    g.migrate(|mig| {
        assert!(r.activate(mig, false).is_ok());
    });
    // one base node
    assert_eq!(g.inputs().len(), 1);

    let r_copy = r.clone();

    let r1_txt = "SELECT a FROM b;\n
                  SELECT a, c FROM b WHERE a = 42;";
    let mut r1 = r.extend(r1_txt).unwrap();
    assert_eq!(r1.version(), 1);
    assert_eq!(r1.expressions().len(), 3);
    assert_eq!(**r1.prior().unwrap(), r_copy);
    g.migrate(|mig| {
        assert!(r1.activate(mig, false).is_ok());
    });
    // still one base node
    assert_eq!(g.inputs().len(), 1);
    // two leaf nodes
    assert_eq!(g.outputs().len(), 2);
}

#[test]
fn recipe_activates_and_migrates_with_join() {
    let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                 CREATE TABLE b (r int, s int);\n";
    let mut r = distributary::Recipe::from_str(r_txt, None).unwrap();
    assert_eq!(r.version(), 0);
    assert_eq!(r.expressions().len(), 2);
    assert_eq!(r.prior(), None);

    let mut g = distributary::Blender::new();
    g.migrate(|mig| {
        assert!(r.activate(mig, false).is_ok());
    });
    // two base nodes
    assert_eq!(g.inputs().len(), 2);

    let r_copy = r.clone();

    let r1_txt = "SELECT y, s FROM a, b WHERE a.x = b.r;";
    let mut r1 = r.extend(r1_txt).unwrap();
    assert_eq!(r1.version(), 1);
    assert_eq!(r1.expressions().len(), 3);
    assert_eq!(**r1.prior().unwrap(), r_copy);
    g.migrate(|mig| {
        assert!(r1.activate(mig, false).is_ok());
    });
    // still two base nodes
    assert_eq!(g.inputs().len(), 2);
    // one leaf node
    assert_eq!(g.outputs().len(), 1);
}

#[test]
fn finkelstein1982_queries() {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = distributary::Blender::new();
    let mut inc = distributary::SqlIncorporator::default();
    g.migrate(|mig| {
        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s.lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| if !(l.ends_with("\n") || l.ends_with(";")) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            })
            .collect();

        // Add them one by one
        for q in lines.iter() {
            assert!(inc.add_query(q, None, mig).is_ok());
        }
    });

    println!("{}", g);
}

#[test]
fn tpc_w() {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = distributary::Blender::new();
    let mut r = distributary::Recipe::blank(None);
    g.migrate(|mig| {
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
            let or = r.clone();
            r = match r.extend(q) {
                Ok(mut nr) => {
                    assert!(nr.activate(mig, false).is_ok());
                    nr
                }
                Err(e) => {
                    println!("{:?}", e);
                    or
                }
            }
        }
    });

    println!("{}", g);
}
