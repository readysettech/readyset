use crate::controller::recipe::Recipe;
use crate::controller::sql::SqlIncorporator;
use crate::{Builder, Handle, SyncHandle};
use dataflow::node::special::Base;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::identity::Identity;
use dataflow::ops::join::JoinSource::*;
use dataflow::ops::join::{Join, JoinSource, JoinType};
use dataflow::ops::project::Project;
use dataflow::ops::union::Union;
use dataflow::{DurabilityMode, PersistenceParameters};
use futures::Future;
use noria::consensus::{Authority, LocalAuthority};
use noria::DataType;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

const DEFAULT_SETTLE_TIME_MS: u64 = 200;
const DEFAULT_SHARDING: Option<usize> = Some(2);

// PersistenceParameters with a log_name on the form of `prefix` + timestamp,
// avoiding collisions between separate test runs (in case an earlier panic causes clean-up to
// fail).
fn get_persistence_params(prefix: &str) -> PersistenceParameters {
    let mut params = PersistenceParameters::default();
    params.mode = DurabilityMode::DeleteOnExit;
    params.log_prefix = String::from(prefix);
    params
}

// Builds a local worker with the given log prefix.
pub fn start_simple(prefix: &str) -> SyncHandle<LocalAuthority> {
    build(prefix, DEFAULT_SHARDING, false)
}

fn wrap_sync<A, F>(fut: F) -> SyncHandle<A>
where
    A: Authority + 'static,
    F: Future<Item = Handle<A>> + Send + 'static,
    F::Error: std::fmt::Debug + Send,
{
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let wh = rt.block_on(fut).unwrap();
    SyncHandle::from_existing(rt, wh)
}

#[allow(dead_code)]
pub fn start_simple_unsharded(prefix: &str) -> SyncHandle<LocalAuthority> {
    build(prefix, None, false)
}

#[allow(dead_code)]
pub fn start_simple_logging(prefix: &str) -> SyncHandle<LocalAuthority> {
    build(prefix, DEFAULT_SHARDING, true)
}

fn build(prefix: &str, sharding: Option<usize>, log: bool) -> SyncHandle<LocalAuthority> {
    use crate::logger_pls;
    let mut builder = Builder::default();
    if log {
        builder.log_with(logger_pls());
    }
    builder.set_sharding(sharding);
    builder.set_persistence(get_persistence_params(prefix));
    builder.start_simple().unwrap()
}

fn get_settle_time() -> Duration {
    let settle_time: u64 = match env::var("SETTLE_TIME") {
        Ok(value) => value.parse().unwrap(),
        Err(_) => DEFAULT_SETTLE_TIME_MS,
    };

    Duration::from_millis(settle_time)
}

// Sleeps for either DEFAULT_SETTLE_TIME_MS milliseconds, or
// for the value given through the SETTLE_TIME environment variable.
fn sleep() {
    thread::sleep(get_settle_time());
}

#[test]
fn it_works_basic() {
    // set up graph
    let mut b = Builder::default();
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        Duration::from_millis(1),
        Some(String::from("it_works_basic")),
        1,
    ));
    let mut g = b.start_simple().unwrap();
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec![]).with_key(vec![0]));
        let b = mig.add_base("b", &["a", "b"], Base::new(vec![]).with_key(vec![0]));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    let id: DataType = 1.into();

    assert_eq!(muta.table_name(), "a");
    assert_eq!(muta.columns(), &["a", "b"]);

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));

    // Delete first record
    muta.delete(vec![id.clone()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 4.into()]]
    );

    // Update second record
    // TODO(malte): disabled until we have update support on bases; the current way of doing this
    // is incompatible with bases' enforcement of the primary key uniqueness constraint.
    //mutb.update(vec![id.clone(), 6.into()]).unwrap();

    // give it some time to propagate
    //sleep();

    // send a query to c
    //assert_eq!(cq.lookup(&[id.clone()], true), Ok(vec![vec![1.into(), 6.into()]]));
}

#[test]
fn base_mutation() {
    use noria::{Modification, Operation};

    let mut g = start_simple("base_mutation");
    g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec![]).with_key(vec![0]));
        mig.maintain_anonymous(a, &[0]);
    });

    let mut read = g.view("a").unwrap().into_sync();
    let mut write = g.table("a").unwrap().into_sync();

    // insert a new record
    write.insert(vec![1.into(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // update that record in place (set)
    write
        .update(vec![1.into()], vec![(1, Modification::Set(3.into()))])
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 3.into()]]
    );

    // update that record in place (add)
    write
        .update(
            vec![1.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 4.into()]]
    );

    // insert or update should update
    write
        .insert_or_update(
            vec![1.into(), 2.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 5.into()]]
    );

    // delete should, well, delete
    write.delete(vec![1.into()]).unwrap();
    sleep();
    assert!(read.lookup(&[1.into()], true).unwrap().is_empty());

    // insert or update should insert
    write
        .insert_or_update(
            vec![1.into(), 2.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );
}

#[test]
fn shared_interdomain_ancestor() {
    // set up graph
    let mut g = start_simple("shared_interdomain_ancestor");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);

        let u = Union::new(emits.clone());
        let b = mig.add_ingredient("b", &["a", "b"], u);
        mig.maintain_anonymous(b, &[0]);

        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut bq = g.view("b").unwrap().into_sync();
    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let id: DataType = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        bq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 2.into()]]
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    let id: DataType = 2.into();
    muta.insert(vec![id.clone(), 4.into()]).unwrap();
    sleep();
    assert_eq!(
        bq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 4.into()]]
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 4.into()]]
    );
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = start_simple("it_works_w_mat");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    let id: DataType = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).unwrap();
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    muta.insert(vec![id.clone(), 3.into()]).unwrap();

    // give them some time to propagate
    sleep();

    // send a query to c
    // we should see all the a values
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // update value again (and again send some secondary updates)
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();
    mutb.insert(vec![id.clone(), 5.into()]).unwrap();
    mutb.insert(vec![id.clone(), 6.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 6);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 5.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 6.into()]));
}

#[test]
fn it_works_w_partial_mat() {
    // set up graph
    let mut g = start_simple("it_works_w_partial_mat");
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());
        (a, b)
    });

    let mut muta = g.table("a").unwrap().into_sync();
    let id: DataType = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).unwrap();
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    muta.insert(vec![id.clone(), 3.into()]).unwrap();

    // give it some time to propagate
    sleep();

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });

    // give it some time to propagate
    sleep();

    let mut cq = g.view("c").unwrap().into_sync();

    // because the reader is partial, we should have no key until we read
    assert_eq!(cq.len().unwrap(), 0);

    // now do some reads
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // should have one key in the reader now
    assert_eq!(cq.len().unwrap(), 1);
}

#[test]
fn it_works_w_partial_mat_below_empty() {
    // set up graph with all nodes added in a single migration. The base tables are therefore empty
    // for now.
    let mut g = start_simple("it_works_w_partial_mat_below_empty");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut muta = g.table("a").unwrap().into_sync();
    let id: DataType = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).unwrap();
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    muta.insert(vec![id.clone(), 3.into()]).unwrap();

    // give it some time to propagate
    sleep();

    let mut cq = g.view("c").unwrap().into_sync();

    // despite the empty base tables, we'll make the reader partial and therefore we should have no
    // key until we read
    assert_eq!(cq.len().unwrap(), 0);

    // now do some reads
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| r == &vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 3.into()]));

    // should have one key in the reader now
    assert_eq!(cq.len().unwrap(), 1);
}

#[test]
fn it_works_deletion() {
    // set up graph
    let mut g = start_simple("it_works_deletion");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["x", "y"], Base::new(vec![]).with_key(vec![1]));
        let b = mig.add_base("b", &["_", "x", "y"], Base::new(vec![]).with_key(vec![2]));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![1, 2]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["x", "y"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    // send a value on a
    muta.insert(vec![1.into(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        cq.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // send a value on b
    mutb.insert(vec![0.into(), 1.into(), 4.into()]).unwrap();
    sleep();

    let res = cq.lookup(&[1.into()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![1.into(), 2.into()]));
    assert!(res.contains(&vec![1.into(), 4.into()]));

    // delete first value
    muta.delete(vec![2.into()]).unwrap();
    sleep();
    assert_eq!(
        cq.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), 4.into()]]
    );
}

#[test]
fn it_works_with_sql_recipe() {
    let mut g = start_simple("it_works_with_sql_recipe");
    let sql = "
        CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));
        QUERY CountCars: SELECT COUNT(*) FROM Car WHERE brand = ?;
    ";
    g.install_recipe(sql).unwrap();

    let mut mutator = g.table("Car").unwrap().into_sync();
    let mut getter = g.view("CountCars").unwrap().into_sync();

    assert_eq!(mutator.table_name(), "Car");
    assert_eq!(mutator.columns(), &["id", "brand"]);

    let brands = vec!["Volvo", "Volvo", "Volkswagen"];
    for (i, &brand) in brands.iter().enumerate() {
        mutator.insert(vec![i.into(), brand.into()]).unwrap();
    }

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&["Volvo".into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], 2.into());
}

#[test]
fn it_works_with_vote() {
    let mut g = start_simple("it_works_with_vote");
    let sql = "
        # base tables
        CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
        CREATE TABLE Vote (article_id int, user int);

        # read queries
        QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                    FROM Article \
                    LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                               FROM Vote GROUP BY Vote.article_id) AS VoteCount \
                    ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;
    ";

    g.install_recipe(sql).unwrap();
    let mut article = g.table("Article").unwrap().into_sync();
    let mut vote = g.table("Vote").unwrap().into_sync();
    let mut awvc = g.view("ArticleWithVoteCount").unwrap().into_sync();

    article.insert(vec![0i64.into(), "Article".into()]).unwrap();
    article.insert(vec![1i64.into(), "Article".into()]).unwrap();
    vote.insert(vec![0i64.into(), 0.into()]).unwrap();

    sleep();

    let rs = awvc.lookup(&[0i64.into()], true).unwrap();
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0], vec![0i64.into(), "Article".into(), 1.into()]);

    let empty = awvc.lookup(&[1i64.into()], true).unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(
        empty[0],
        vec![1i64.into(), "Article".into(), DataType::None]
    );
}

#[test]
fn it_works_with_double_query_through() {
    let mut builder = Builder::default();
    builder.set_persistence(get_persistence_params("it_works_with_double_query_through"));
    // TODO: sharding::shard picks the wrong column to shard on, since both aid and bid resolves to
    // all ancestors (and bid comes first). The reader is on aid though, so the sharder should pick
    // that as well (and not bid!).
    builder.set_sharding(None);
    let mut g = builder.start_simple().unwrap();
    let sql = "
        # base tables
        CREATE TABLE A (aid int, other int, PRIMARY KEY(aid));
        CREATE TABLE B (bid int, PRIMARY KEY(bid));

        # read queries
        QUERY ReadJoin: SELECT J.aid, J.other \
            FROM B \
            LEFT JOIN (SELECT A.aid, A.other FROM A \
                WHERE A.other = 5) AS J \
            ON (J.aid = B.bid) \
            WHERE J.aid = ?;
    ";

    g.install_recipe(sql).unwrap();
    let mut a = g.table("A").unwrap().into_sync();
    let mut b = g.table("B").unwrap().into_sync();
    let mut getter = g.view("ReadJoin").unwrap().into_sync();

    a.insert(vec![1i64.into(), 5.into()]).unwrap();
    a.insert(vec![2i64.into(), 10.into()]).unwrap();
    b.insert(vec![1i64.into()]).unwrap();

    sleep();

    let rs = getter.lookup(&[1i64.into()], true).unwrap();
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0], vec![1i64.into(), 5.into()]);

    let empty = getter.lookup(&[2i64.into()], true).unwrap();
    assert_eq!(empty.len(), 0);
}

#[test]
fn it_works_with_reads_before_writes() {
    let mut g = start_simple("it_works_with_reads_before_writes");
    let sql = "
        CREATE TABLE Article (aid int, PRIMARY KEY(aid));
        CREATE TABLE Vote (aid int, uid int, PRIMARY KEY(aid, uid));
        QUERY ArticleVote: SELECT Article.aid, Vote.uid \
            FROM Article, Vote \
            WHERE Article.aid = Vote.aid AND Article.aid = ?;
    ";

    g.install_recipe(sql).unwrap();
    let mut article = g.table("Article").unwrap().into_sync();
    let mut vote = g.table("Vote").unwrap().into_sync();
    let mut awvc = g.view("ArticleVote").unwrap().into_sync();

    let aid = 1;
    let uid = 10;

    assert!(awvc.lookup(&[aid.into()], true).unwrap().is_empty());
    article.insert(vec![aid.into()]).unwrap();
    sleep();

    vote.insert(vec![aid.into(), uid.into()]).unwrap();
    sleep();

    let result = awvc.lookup(&[aid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![aid.into(), uid.into()]);
}

#[test]
fn forced_shuffle_despite_same_shard() {
    // XXX: this test doesn't currently *fail* despite
    // multiple trailing replay responses that are simply ignored...

    let mut g = start_simple("forced_shuffle_despite_same_shard");
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(pid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        QUERY CarPrice: SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.install_recipe(sql).unwrap();

    let mut car_mutator = g.table("Car").unwrap().into_sync();
    let mut price_mutator = g.table("Price").unwrap().into_sync();
    let mut getter = g.view("CarPrice").unwrap().into_sync();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator
        .insert(vec![pid.into(), price.into()])
        .unwrap();
    car_mutator.insert(vec![cid.into(), pid.into()]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());
}

#[test]
fn double_shuffle() {
    let mut g = start_simple("double_shuffle");
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(cid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        QUERY CarPrice: SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.install_recipe(sql).unwrap();

    let mut car_mutator = g.table("Car").unwrap().into_sync();
    let mut price_mutator = g.table("Price").unwrap().into_sync();
    let mut getter = g.view("CarPrice").unwrap().into_sync();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator
        .insert(vec![pid.into(), price.into()])
        .unwrap();
    car_mutator.insert(vec![cid.into(), pid.into()]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());
}

#[test]
fn it_works_with_arithmetic_aliases() {
    let mut g = start_simple("it_works_with_arithmetic_aliases");
    let sql = "
        CREATE TABLE Price (pid int, cent_price int, PRIMARY KEY(pid));
        ModPrice: SELECT pid, cent_price / 100 AS price FROM Price;
        QUERY AltPrice: SELECT pid, price FROM ModPrice WHERE pid = ?;
    ";
    g.install_recipe(sql).unwrap();

    let mut price_mutator = g.table("Price").unwrap().into_sync();
    let mut getter = g.view("AltPrice").unwrap().into_sync();
    let pid = 1;
    let price = 10000;
    price_mutator
        .insert(vec![pid.into(), price.into()])
        .unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[pid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (price / 100).into());
}

#[test]
fn it_recovers_persisted_bases() {
    let authority = Arc::new(LocalAuthority::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("it_recovers_persisted_bases");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(path.to_string_lossy().into()),
        1,
    );

    {
        let mut g = Builder::default();
        g.set_persistence(persistence_params.clone());
        let mut g = wrap_sync(g.start(authority.clone()));

        let sql = "
            CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
            QUERY CarPrice: SELECT price FROM Car WHERE id = ?;
        ";
        g.install_recipe(sql).unwrap();

        let mut mutator = g.table("Car").unwrap().into_sync();

        for i in 1..10 {
            let price = i * 10;
            mutator.insert(vec![i.into(), price.into()]).unwrap();
        }

        // Let writes propagate:
        sleep();
    }

    let mut g = Builder::default();
    g.set_persistence(persistence_params);
    let mut g = wrap_sync(g.start(authority.clone()));
    let mut getter = g.view("CarPrice").unwrap().into_sync();

    // Make sure that the new graph contains the old writes
    for i in 1..10 {
        let price = i * 10;
        let result = getter.lookup(&[i.into()], true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], price.into());
    }
}

#[test]
fn mutator_churn() {
    let mut g = start_simple("mutator_churn");
    let _ = g.migrate(|mig| {
        // migrate

        // add vote base table
        let vote = mig.add_base("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        mig.maintain_anonymous(vc, &[0]);
        (vote, vc)
    });

    let mut vc_state = g.view("votecount").unwrap().into_sync();

    let ids = 10;
    let votes = 7;

    // continuously write to vote with new mutators
    let user: DataType = 0.into();
    for _ in 0..votes {
        for i in 0..ids {
            g.table("vote")
                .unwrap()
                .into_sync()
                .insert(vec![user.clone(), i.into()])
                .unwrap();
        }
    }

    // allow the system to catch up with the last writes
    sleep();

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true).unwrap(),
            vec![vec![i.into(), votes.into()]]
        );
    }
}

#[test]
fn it_recovers_persisted_bases_w_multiple_nodes() {
    let authority = Arc::new(LocalAuthority::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("it_recovers_persisted_bases_w_multiple_nodes");
    let tables = vec!["A", "B", "C"];
    let persistence_parameters = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(path.to_string_lossy().into()),
        1,
    );

    {
        let mut g = Builder::default();
        g.set_persistence(persistence_parameters.clone());
        let mut g = wrap_sync(g.start(authority.clone()));

        let sql = "
            CREATE TABLE A (id int, PRIMARY KEY(id));
            CREATE TABLE B (id int, PRIMARY KEY(id));
            CREATE TABLE C (id int, PRIMARY KEY(id));

            QUERY AID: SELECT id FROM A WHERE id = ?;
            QUERY BID: SELECT id FROM B WHERE id = ?;
            QUERY CID: SELECT id FROM C WHERE id = ?;
        ";
        g.install_recipe(sql).unwrap();
        for (i, table) in tables.iter().enumerate() {
            let mut mutator = g.table(table).unwrap().into_sync();
            mutator.insert(vec![i.into()]).unwrap();
        }
        sleep();
    }

    // Create a new controller with the same authority, and make sure that it recovers to the same
    // state that the other one had.
    let mut g = Builder::default();
    g.set_persistence(persistence_parameters);
    let mut g = wrap_sync(g.start(authority.clone()));
    for (i, table) in tables.iter().enumerate() {
        let mut getter = g.view(&format!("{}ID", table)).unwrap().into_sync();
        let result = getter.lookup(&[i.into()], true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], i.into());
    }
}

#[test]
fn it_works_with_simple_arithmetic() {
    let mut g = start_simple("it_works_with_simple_arithmetic");

    g.migrate(|mig| {
        let sql = "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
                   QUERY CarPrice: SELECT 2 * price FROM Car WHERE id = ?;";
        let mut recipe = Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig).unwrap();
    });

    let mut mutator = g.table("Car").unwrap().into_sync();
    let mut getter = g.view("CarPrice").unwrap().into_sync();
    let id: DataType = 1.into();
    let price: DataType = 123.into();
    mutator.insert(vec![id.clone(), price]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.clone()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], 246.into());
}

#[test]
fn it_works_with_multiple_arithmetic_expressions() {
    let mut g = start_simple("it_works_with_multiple_arithmetic_expressions");
    let sql = "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
               QUERY CarPrice: SELECT 10 * 10, 2 * price, 10 * price, FROM Car WHERE id = ?;
               ";
    g.install_recipe(sql).unwrap();

    let mut mutator = g.table("Car").unwrap().into_sync();
    let mut getter = g.view("CarPrice").unwrap().into_sync();
    let id: DataType = 1.into();
    let price: DataType = 123.into();
    mutator.insert(vec![id.clone(), price]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.clone()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], 100.into());
    assert_eq!(result[0][2], 246.into());
    assert_eq!(result[0][3], 1230.into());
}

#[test]
fn it_works_with_join_arithmetic() {
    let mut g = start_simple("it_works_with_join_arithmetic");
    let sql = "
        CREATE TABLE Car (car_id int, price_id int, PRIMARY KEY(car_id));
        CREATE TABLE Price (price_id int, price int, PRIMARY KEY(price_id));
        CREATE TABLE Sales (sales_id int, price_id int, fraction float, PRIMARY KEY(sales_id));
        QUERY CarPrice: SELECT price * fraction FROM Car \
                  JOIN Price ON Car.price_id = Price.price_id \
                  JOIN Sales ON Price.price_id = Sales.price_id \
                  WHERE car_id = ?;
    ";
    g.install_recipe(sql).unwrap();

    let mut car_mutator = g.table("Car").unwrap().into_sync();
    let mut price_mutator = g.table("Price").unwrap().into_sync();
    let mut sales_mutator = g.table("Sales").unwrap().into_sync();
    let mut getter = g.view("CarPrice").unwrap().into_sync();
    let id = 1;
    let price = 123;
    let fraction = 0.7;
    car_mutator.insert(vec![id.into(), id.into()]).unwrap();
    price_mutator.insert(vec![id.into(), price.into()]).unwrap();
    sales_mutator
        .insert(vec![id.into(), id.into(), fraction.into()])
        .unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (f64::from(price) * fraction).into());
}

#[test]
fn it_works_with_function_arithmetic() {
    let mut g = start_simple("it_works_with_function_arithmetic");
    let sql = "
        CREATE TABLE Bread (id int, price int, PRIMARY KEY(id));
        QUERY Price: SELECT 2 * MAX(price) FROM Bread;
    ";
    g.install_recipe(sql).unwrap();

    let mut mutator = g.table("Bread").unwrap().into_sync();
    let mut getter = g.view("Price").unwrap().into_sync();
    let max_price = 20;
    for (i, price) in (10..=max_price).enumerate() {
        let id = i + 1;
        mutator.insert(vec![id.into(), price.into()]).unwrap();
    }

    // Let writes propagate:
    sleep();

    let result = getter.lookup(&[0.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], DataType::from(max_price * 2));
}

#[test]
fn votes() {
    // set up graph
    let mut g = start_simple("votes");
    let _ = g.migrate(|mig| {
        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_base("article1", &["id", "title"], Base::default());
        let article2 = mig.add_base("article2", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        mig.maintain_anonymous(article, &[0]);

        // add vote base table
        let vote = mig.add_base("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "vc",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );
        mig.maintain_anonymous(vc, &[0]);

        // add final join using first field from article and first from vc
        let j = Join::new(article, vc, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("end", &["id", "title", "votes"], j);
        mig.maintain_anonymous(end, &[0]);

        (article1, article2, vote, article, vc, end)
    });

    let mut articleq = g.view("article").unwrap().into_sync();
    let mut vcq = g.view("vc").unwrap().into_sync();
    let mut endq = g.view("end").unwrap().into_sync();

    let mut mut1 = g.table("article1").unwrap().into_sync();
    let mut mut2 = g.table("article2").unwrap().into_sync();
    let mut mutv = g.table("vote").unwrap().into_sync();

    let a1: DataType = 1.into();
    let a2: DataType = 2.into();

    // make one article
    mut1.insert(vec![a1.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // query articles to see that it was updated
    assert_eq!(
        articleq.lookup(&[a1.clone()], true).unwrap(),
        vec![vec![a1.clone(), 2.into()]]
    );

    // make another article
    mut2.insert(vec![a2.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(
        articleq.lookup(&[a1.clone()], true).unwrap(),
        vec![vec![a1.clone(), 2.into()]]
    );
    assert_eq!(
        articleq.lookup(&[a2.clone()], true).unwrap(),
        vec![vec![a2.clone(), 4.into()]]
    );

    // create a vote (user 1 votes for article 1)
    mutv.insert(vec![1.into(), a1.clone()]).unwrap();

    // give it some time to propagate
    sleep();

    // query vote count to see that the count was updated
    let res = vcq.lookup(&[a1.clone()], true).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq.lookup(&[a1.clone()], true).unwrap();
    assert!(
        res.iter()
            .any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
        "no entry for [1,2,1|2] in {:?}",
        res
    );
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq.lookup(&[a2.clone()], true).unwrap();
    assert!(res.len() <= 1) // could be 1 if we had zero-rows
}

#[test]
fn empty_migration() {
    // set up graph
    let mut g = start_simple("empty_migration");
    g.migrate(|_| {});

    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    let id: DataType = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));
}

#[test]
fn simple_migration() {
    let id: DataType = 1.into();

    // set up graph
    let mut g = start_simple("simple_migration");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        mig.maintain_anonymous(a, &[0]);
        a
    });

    let mut aq = g.view("a").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let _ = g.migrate(|mig| {
        let b = mig.add_base("b", &["a", "b"], Base::default());
        mig.maintain_anonymous(b, &[0]);
        b
    });

    let mut bq = g.view("b").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    // send a value on b
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that b got it
    assert_eq!(
        bq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 4.into()]]
    );
}

#[test]
fn add_columns() {
    let id: DataType = "x".into();

    // set up graph
    let mut g = start_simple("add_columns");
    let a = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec![1.into(), 2.into()]));
        mig.maintain_anonymous(a, &[0]);
        a
    });
    let mut aq = g.view("a").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();

    // send a value on a
    muta.insert(vec![id.clone(), "y".into()]).unwrap();
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), "y".into()]]
    );

    // add a third column to a
    g.migrate(move |mig| {
        mig.add_column(a, "c", 3.into());
    });
    sleep();

    // send another (old) value on a
    muta.insert(vec![id.clone(), "z".into()]).unwrap();
    sleep();

    // check that a got it, and added the new, third column's default
    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), "y".into()]));
    assert!(res.contains(&vec![id.clone(), "z".into(), 3.into()]));

    // get a new muta and send a new value on it
    let mut muta = g.table("a").unwrap().into_sync();
    muta.insert(vec![id.clone(), "a".into(), 10.into()])
        .unwrap();
    sleep();

    // check that a got it, and included the third column
    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 3);
    assert!(res.contains(&vec![id.clone(), "y".into()]));
    assert!(res.contains(&vec![id.clone(), "z".into(), 3.into()]));
    assert!(res.contains(&vec![id.clone(), "a".into(), 10.into()]));
}

#[test]
fn migrate_added_columns() {
    let id: DataType = "x".into();

    // set up graph
    let mut g = start_simple("migrate_added_columns");
    let a = g.migrate(|mig| mig.add_base("a", &["a", "b"], Base::new(vec![1.into(), 2.into()])));
    let mut muta = g.table("a").unwrap().into_sync();

    // send a value on a
    muta.insert(vec![id.clone(), "y".into()]).unwrap();
    sleep();

    // add a third column to a, and a view that uses it
    let _ = g.migrate(move |mig| {
        mig.add_column(a, "c", 3.into());
        let b = mig.add_ingredient("x", &["c", "b"], Project::new(a, &[2, 0], None, None));
        mig.maintain_anonymous(b, &[1]);
        b
    });

    let mut bq = g.view("x").unwrap().into_sync();

    // send another (old) value on a
    muta.insert(vec![id.clone(), "z".into()]).unwrap();
    // and an entirely new value
    let mut muta = g.table("a").unwrap().into_sync();
    muta.insert(vec![id.clone(), "a".into(), 10.into()])
        .unwrap();

    // give it some time to propagate
    sleep();

    // we should now see the pre-migration write and the old post-migration write with the default
    // value, and the new post-migration write with the value it contained.
    let res = bq.lookup(&[id.clone()], true).unwrap();
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
    let id: DataType = "x".into();

    // set up graph
    let mut g = start_simple("migrate_drop_columns");
    let a = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec!["a".into(), "b".into()]));
        mig.maintain_anonymous(a, &[0]);
        a
    });
    let mut aq = g.view("a").unwrap().into_sync();
    let mut muta1 = g.table("a").unwrap().into_sync();

    // send a value on a
    muta1.insert(vec![id.clone(), "bx".into()]).unwrap();

    // check that it's there
    sleep();
    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 1);
    assert!(res.contains(&vec![id.clone(), "bx".into()]));

    // drop a column
    g.migrate(move |mig| {
        mig.drop_column(a, 1);
        mig.maintain_anonymous(a, &[0]);
    });

    // new mutator should only require one column
    // and should inject default for a.b
    let mut muta2 = g.table("a").unwrap().into_sync();
    muta2.insert(vec![id.clone()]).unwrap();

    // so two rows now!
    sleep();
    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), "bx".into()]));
    assert!(res.contains(&vec![id.clone(), "b".into()]));

    // add a new column
    g.migrate(move |mig| {
        mig.add_column(a, "c", "c".into());
    });

    // new mutator allows putting two values, and injects default for a.b
    let mut muta3 = g.table("a").unwrap().into_sync();
    muta3.insert(vec![id.clone(), "cy".into()]).unwrap();

    // using an old putter now should add default for c
    muta1.insert(vec![id.clone(), "bz".into()]).unwrap();

    // using putter that knows of neither b nor c should result in defaults for both
    muta2.insert(vec![id.clone()]).unwrap();
    sleep();

    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 5);
    // NOTE: if we *hadn't* read bx and b above, they would have also have c because it would have
    // been added when the lookups caused partial backfills.
    assert!(res.contains(&vec![id.clone(), "bx".into()]));
    assert!(res.contains(&vec![id.clone(), "b".into()]));
    assert!(res.contains(&vec![id.clone(), "b".into(), "cy".into()]));
    assert!(res.contains(&vec![id.clone(), "bz".into(), "c".into()]));
    assert!(res.contains(&vec![id.clone(), "b".into(), "c".into()]));
}

#[test]
fn key_on_added() {
    // set up graph
    let mut g = start_simple("key_on_added");
    let a = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec![1.into(), 2.into()]));
        a
    });

    // add a maintained view keyed on newly added column
    let _ = g.migrate(move |mig| {
        mig.add_column(a, "c", 3.into());
        let b = mig.add_ingredient("x", &["c", "b"], Project::new(a, &[2, 1], None, None));
        mig.maintain_anonymous(b, &[0]);
        b
    });

    // make sure we can read (may trigger a replay)
    let mut bq = g.view("x").unwrap().into_sync();
    assert!(bq.lookup(&[3.into()], true).unwrap().is_empty());
}

#[test]
fn replay_during_replay() {
    // what we're trying to set up here is a case where a join receives a record with a value for
    // the join key that does not exist in the view the record was sent from. since joins only do
    // lookups into the origin view during forward processing when it receives things from the
    // right in a left join, that's what we have to construct.
    let mut g = Builder::default();
    g.disable_partial();
    g.set_persistence(get_persistence_params("replay_during_replay"));
    let mut g = g.start_simple().unwrap();
    let (a, u1, u2) = g.migrate(|mig| {
        // we need three bases:
        //
        //  - a will be the left side of the left join
        //  - u1 and u2 will be joined together with a regular one-to-one join to produce a partial
        //    view (remember, we need to miss in the source of the replay, so it must be partial).
        let a = mig.add_base("a", &["a"], Base::new(vec![1.into()]));
        let u1 = mig.add_base("u1", &["u"], Base::new(vec![1.into()]));
        let u2 = mig.add_base("u2", &["u", "a"], Base::new(vec![1.into(), 2.into()]));
        (a, u1, u2)
    });

    // add our joins
    let (u, _) = g.migrate(move |mig| {
        // u = u1 * u2
        let j = Join::new(u1, u2, JoinType::Inner, vec![B(0, 0), R(1)]);
        let u = mig.add_ingredient("u", &["u", "a"], j);
        let j = Join::new(a, u, JoinType::Left, vec![B(0, 1), R(0)]);
        let end = mig.add_ingredient("end", &["a", "u"], j);
        mig.maintain_anonymous(end, &[0]);
        (u, end)
    });

    // at this point, there's no secondary index on `u`, so any records that are forwarded from `u`
    // must already be present in the one index that `u` has. let's do some writes and check that
    // nothing crashes.

    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutu1 = g.table("u1").unwrap().into_sync();
    let mut mutu2 = g.table("u2").unwrap().into_sync();

    // as are numbers
    muta.insert(vec![1.into()]).unwrap();
    muta.insert(vec![2.into()]).unwrap();
    muta.insert(vec![3.into()]).unwrap();

    // us are strings
    mutu1.insert(vec!["a".into()]).unwrap();
    mutu1.insert(vec!["b".into()]).unwrap();
    mutu1.insert(vec!["c".into()]).unwrap();

    // we want there to be data for all keys
    mutu2.insert(vec!["a".into(), 1.into()]).unwrap();
    mutu2.insert(vec!["b".into(), 2.into()]).unwrap();
    mutu2.insert(vec!["c".into(), 3.into()]).unwrap();

    sleep();

    // since u and target are both partial, the writes should not actually have propagated through
    // yet. do a read to see that one makes it through correctly:
    let mut r = g.view("end").unwrap().into_sync();

    assert_eq!(
        r.lookup(&[1.into()], true).unwrap(),
        vec![vec![1.into(), "a".into()]]
    );

    // we now know that u has key a=1 in its index
    // now we add a secondary index on u.u
    g.migrate(move |mig| {
        mig.maintain_anonymous(u, &[0]);
    });

    let mut second = g.view("u").unwrap().into_sync();

    // second is partial and empty, so any read should trigger a replay.
    // though that shouldn't interact with target in any way.
    assert_eq!(
        second.lookup(&["a".into()], true).unwrap(),
        vec![vec!["a".into(), 1.into()]]
    );

    // now we get to the funky part.
    // we're going to request a second key from the secondary index on `u`, which causes that hole
    // to disappear. then we're going to do a write to `u2` that has that second key, but has an
    // "a" value for which u has a hole. that record is then going to be forwarded to *both*
    // children, and it'll be interesting to see what the join then does.
    assert_eq!(
        second.lookup(&["b".into()], true).unwrap(),
        vec![vec!["b".into(), 2.into()]]
    );

    // u has a hole for a=2, but not for u=b, and so should forward this to both children
    mutu2.insert(vec!["b".into(), 2.into()]).unwrap();

    sleep();

    // what happens if we now query for 2?
    assert_eq!(
        r.lookup(&[2.into()], true).unwrap(),
        vec![vec![2.into(), "b".into()], vec![2.into(), "b".into()]]
    );
}

#[test]
fn cascading_replays_with_sharding() {
    let mut g = Builder::default();
    g.set_sharding(Some(2));
    g.set_persistence(get_persistence_params("cascading_replays_with_sharding"));
    let mut g = g.start_simple().unwrap();

    // add each two bases. these are initially unsharded, but f will end up being sharded by u1,
    // while v will be sharded by u

    // force v to be in a different domain by adding it in a separate migration
    let v = g.migrate(|mig| mig.add_base("v", &["u", "s"], Base::new(vec!["".into(), 1.into()])));
    // now add the rest
    let _ = g.migrate(move |mig| {
        let f = mig.add_base("f", &["f1", "f2"], Base::new(vec!["".into(), "".into()]));
        // add a join
        let jb = Join::new(f, v, JoinType::Inner, vec![B(0, 0), R(1), L(1)]);
        let j = mig.add_ingredient("j", &["u", "s", "f2"], jb);
        // aggregate over the join. this will force a shard merger to be inserted because the
        // group-by column ("f2") isn't the same as the join's output sharding column ("f1"/"u")
        let a = Aggregation::COUNT.over(j, 0, &[2]);
        let end = mig.add_ingredient("end", &["u", "c"], a);
        mig.maintain_anonymous(end, &[0]);
        (j, end)
    });

    let mut mutf = g.table("f").unwrap().into_sync();
    let mut mutv = g.table("v").unwrap().into_sync();

    //                f1           f2
    mutf.insert(vec!["u1".into(), "u3".into()]).unwrap();
    mutf.insert(vec!["u2".into(), "u3".into()]).unwrap();
    mutf.insert(vec!["u3".into(), "u1".into()]).unwrap();

    //                u
    mutv.insert(vec!["u1".into(), 1.into()]).unwrap();
    mutv.insert(vec!["u2".into(), 1.into()]).unwrap();
    mutv.insert(vec!["u3".into(), 1.into()]).unwrap();

    sleep();

    let mut e = g.view("end").unwrap().into_sync();

    assert_eq!(
        e.lookup(&["u1".into()], true).unwrap(),
        vec![vec!["u1".into(), 1.into()]]
    );
    assert_eq!(
        e.lookup(&["u2".into()], true).unwrap(),
        Vec::<Vec<DataType>>::new()
    );
    assert_eq!(
        e.lookup(&["u3".into()], true).unwrap(),
        vec![vec!["u3".into(), 2.into()]]
    );

    sleep();
}

#[test]
fn full_aggregation_with_bogokey() {
    // set up graph
    let mut g = start_simple("full_aggregation_with_bogokey");
    let base = g.migrate(|mig| mig.add_base("base", &["x"], Base::new(vec![1.into()])));

    // add an aggregation over the base with a bogo key.
    // in other words, the aggregation is across all rows.
    let _ = g.migrate(move |mig| {
        let bogo = mig.add_ingredient(
            "bogo",
            &["x", "bogo"],
            Project::new(base, &[0], Some(vec![0.into()]), None),
        );
        let agg = mig.add_ingredient(
            "agg",
            &["bogo", "count"],
            Aggregation::COUNT.over(bogo, 0, &[1]),
        );
        mig.maintain_anonymous(agg, &[0]);
        agg
    });

    let mut aggq = g.view("agg").unwrap().into_sync();
    let mut base = g.table("base").unwrap().into_sync();

    // insert some values
    base.insert(vec![1.into()]).unwrap();
    base.insert(vec![2.into()]).unwrap();
    base.insert(vec![3.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to aggregation materialization
    assert_eq!(
        aggq.lookup(&[0.into()], true).unwrap(),
        vec![vec![0.into(), 3.into()]]
    );

    // update value again
    base.insert(vec![4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that value was updated again
    assert_eq!(
        aggq.lookup(&[0.into()], true).unwrap(),
        vec![vec![0.into(), 4.into()]]
    );
}

#[test]
fn materialization_frontier() {
    // set up graph
    let mut g = start_simple_unsharded("materialization_frontier");
    g.migrate(|mig| {
        // migrate

        // add article base node
        let article = mig.add_base("article", &["id", "title"], Base::default());

        // add vote base table
        let vote = mig.add_base(
            "vote",
            &["user", "id"],
            Base::default().with_key(vec![0, 1]),
        );

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );
        mig.mark_shallow(vc);

        // add final join using first field from article and first from vc
        let j = Join::new(article, vc, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        let ri = mig.maintain_anonymous(end, &[0]);
        mig.mark_shallow(ri);
        (article, vote, vc, end)
    });

    let mut a = g.table("article").unwrap().into_sync();
    let mut v = g.table("vote").unwrap().into_sync();
    let mut r = g.view("awvc").unwrap().into_sync();

    // seed votes
    v.insert(vec!["a".into(), 1.into()]).unwrap();
    v.insert(vec!["a".into(), 2.into()]).unwrap();
    v.insert(vec!["b".into(), 1.into()]).unwrap();
    v.insert(vec!["c".into(), 2.into()]).unwrap();
    v.insert(vec!["d".into(), 2.into()]).unwrap();

    // seed articles
    a.insert(vec![1.into(), "Hello world #1".into()]).unwrap();
    a.insert(vec![2.into(), "Hello world #2".into()]).unwrap();
    sleep();

    // we want to alternately read article 1 and 2, knowing that reading one will purge the other.
    // we first "warm up" by reading both to ensure all other necessary state is present.
    let one = 1.into();
    let two = 2.into();
    assert_eq!(
        r.lookup(&[one], true).unwrap(),
        vec![vec![1.into(), "Hello world #1".into(), 2.into()]]
    );
    assert_eq!(
        r.lookup(&[two], true).unwrap(),
        vec![vec![2.into(), "Hello world #2".into(), 3.into()]]
    );

    for _ in 0..1_000 {
        for &id in &[1, 2] {
            let r = r.lookup(&[id.into()], true).unwrap();
            match id {
                1 => {
                    assert_eq!(r, vec![vec![1.into(), "Hello world #1".into(), 2.into()]]);
                }
                2 => {
                    assert_eq!(r, vec![vec![2.into(), "Hello world #2".into(), 3.into()]]);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn crossing_migration() {
    // set up graph
    let mut g = start_simple("crossing_migration");
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());
        (a, b)
    });
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });

    let mut cq = g.view("c").unwrap().into_sync();

    let id: DataType = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    sleep();

    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();
    sleep();

    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), 2.into()]));
    assert!(res.contains(&vec![id.clone(), 4.into()]));
}

#[test]
fn independent_domain_migration() {
    let id: DataType = 1.into();

    // set up graph
    let mut g = start_simple("independent_domain_migration");
    let _ = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        mig.maintain_anonymous(a, &[0]);
        a
    });

    let mut aq = g.view("a").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let _ = g.migrate(|mig| {
        let b = mig.add_base("b", &["a", "b"], Base::default());
        mig.maintain_anonymous(b, &[0]);
        b
    });

    let mut bq = g.view("b").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    // send a value on b
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        bq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 4.into()]]
    );
}

#[test]
fn domain_amend_migration() {
    // set up graph
    let mut g = start_simple("domain_amend_migration");
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::default());
        let b = mig.add_base("b", &["a", "b"], Base::default());
        (a, b)
    });
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });
    let mut cq = g.view("c").unwrap().into_sync();

    let id: DataType = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();
    sleep();

    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();
    sleep();

    let res = cq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), 2.into()]));
    assert!(res.contains(&vec![id.clone(), 4.into()]));
}

#[test]
fn migration_depends_on_unchanged_domain() {
    // here's the case we want to test: before the migration, we have some domain that contains
    // some materialized node n, as well as an egress node. after the migration, we add a domain
    // that depends on n being materialized. the tricky part here is that n's domain hasn't changed
    // as far as the system is aware (in particular, because it didn't need to add an egress node).
    // this is tricky, because the system must realize that n is materialized, even though it
    // normally wouldn't even look at that part of the data flow graph!

    let mut g = start_simple("migration_depends_on_unchanged_domain");
    let left = g.migrate(|mig| {
        // base node, so will be materialized
        let left = mig.add_base("foo", &["a", "b"], Base::default());

        // node in different domain that depends on foo causes egress to be added
        mig.add_ingredient("bar", &["a", "b"], Identity::new(left));
        left
    });

    g.migrate(move |mig| {
        // joins require their inputs to be materialized
        // we need a new base as well so we can actually make a join
        let tmp = mig.add_base("tmp", &["a", "b"], Base::default());
        let j = Join::new(
            left,
            tmp,
            JoinType::Inner,
            vec![JoinSource::B(0, 0), JoinSource::R(1)],
        );
        mig.add_ingredient("join", &["a", "b"], j);
    });
}

fn do_full_vote_migration(sharded: bool, old_puts_after: bool) {
    let name = format!("do_full_vote_migration_{}", old_puts_after);
    let mut g = if sharded {
        start_simple(&name)
    } else {
        start_simple_unsharded(&name)
    };
    let (article, _vote, vc, _end) = g.migrate(|mig| {
        // migrate

        // add article base node
        let article = mig.add_base("article", &["id", "title"], Base::default());

        // add vote base table
        // NOTE: the double-column key here means that we can't shard vote
        let vote = mig.add_base(
            "vote",
            &["user", "id"],
            Base::default().with_key(vec![0, 1]),
        );

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        // add final join using first field from article and first from vc
        let j = Join::new(article, vc, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        mig.maintain_anonymous(end, &[0]);
        (article, vote, vc, end)
    });
    let mut muta = g.table("article").unwrap().into_sync();
    let mut mutv = g.table("vote").unwrap().into_sync();

    let n = 250i64;
    let title: DataType = "foo".into();
    let raten: DataType = 5.into();

    for i in 0..n {
        muta.insert(vec![i.into(), title.clone()]).unwrap();
    }
    for i in 0..n {
        mutv.insert(vec![1.into(), i.into()]).unwrap();
    }

    let mut last = g.view("awvc").unwrap().into_sync();
    thread::sleep(get_settle_time().checked_mul(3).unwrap());
    for i in 0..n {
        let rows = last.lookup(&[i.into()], true).unwrap();
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
    let _ = g.migrate(move |mig| {
        // add new "ratings" base table
        let rating = mig.add_base("rating", &["user", "id", "stars"], Base::default());

        // add sum of ratings
        let rs = mig.add_ingredient(
            "rsum",
            &["id", "total"],
            Aggregation::SUM.over(rating, 2, &[1]),
        );

        // join vote count and rsum (and in theory, sum them)
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
        mig.maintain_anonymous(newend, &[0]);
        (rating, newend)
    });

    let mut last = g.view("awr").unwrap().into_sync();
    let mut mutr = g.table("rating").unwrap().into_sync();
    for i in 0..n {
        if old_puts_after {
            mutv.insert(vec![2.into(), i.into()]).unwrap();
        }
        mutr.insert(vec![2.into(), i.into(), raten.clone()])
            .unwrap();
    }

    thread::sleep(get_settle_time().checked_mul(3).unwrap());
    for i in 0..n {
        let rows = last.lookup(&[i.into()], true).unwrap();
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
    do_full_vote_migration(true, false);
}

#[test]
fn full_vote_migration_new_and_old() {
    do_full_vote_migration(true, true);
}

#[test]
fn full_vote_migration_new_and_old_unsharded() {
    do_full_vote_migration(false, true);
}

#[test]
fn live_writes() {
    let mut g = start_simple("live_writes");
    let (_vote, vc) = g.migrate(|mig| {
        // migrate

        // add vote base table
        let vote = mig.add_base("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        mig.maintain_anonymous(vc, &[0]);
        (vote, vc)
    });

    let mut vc_state = g.view("votecount").unwrap().into_sync();
    let mut add = g.table("vote").unwrap().into_sync();

    let ids = 1000;
    let votes = 7;

    // continuously write to vote
    let jh = thread::spawn(move || {
        let user: DataType = 0.into();
        // we need to use a batch putter because otherwise we'd wait for 7000 batch intervals
        add.perform_all((0..votes).flat_map(|_| (0..ids).map(|i| vec![user.clone(), i.into()])))
            .unwrap()
    });

    // let a few writes through to make migration take a while
    sleep();

    // now do a migration that's going to have to copy state
    let _ = g.migrate(move |mig| {
        let vc2 = mig.add_ingredient(
            "votecount2",
            &["id", "votes"],
            Aggregation::SUM.over(vc, 1, &[0]),
        );
        mig.maintain_anonymous(vc2, &[0]);
        vc2
    });

    let mut vc2_state = g.view("votecount2").unwrap().into_sync();

    // TODO: check that the writer did indeed complete writes during the migration

    // wait for writer to finish
    jh.join().unwrap();

    // allow the system to catch up with the last writes
    sleep();

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true).unwrap(),
            vec![vec![i.into(), votes.into()]]
        );
        assert_eq!(
            vc2_state.lookup(&[i.into()], true).unwrap(),
            vec![vec![i.into(), votes.into()]]
        );
    }
}

#[test]
fn state_replay_migration_query() {
    // similar to test above, except we will have a materialized Reader node that we're going to
    // read from rather than relying on forwarding. to further stress the graph, *both* base nodes
    // are created and populated before the migration, meaning we have to replay through a join.

    let mut g = start_simple("state_replay_migration_query");
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_base("a", &["x", "y"], Base::default());
        let b = mig.add_base("b", &["x", "z"], Base::default());

        (a, b)
    });
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();

    // make a couple of records
    muta.insert(vec![1.into(), "a".into()]).unwrap();
    muta.insert(vec![1.into(), "b".into()]).unwrap();
    muta.insert(vec![2.into(), "c".into()]).unwrap();
    mutb.insert(vec![1.into(), "n".into()]).unwrap();
    mutb.insert(vec![2.into(), "o".into()]).unwrap();

    let _ = g.migrate(move |mig| {
        // add join and a reader node
        let j = Join::new(a, b, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // we want to observe what comes out of the join
        mig.maintain_anonymous(j, &[0]);
        j
    });
    let mut out = g.view("j").unwrap().into_sync();
    sleep();

    // if all went according to plan, the join should now be fully populated!
    // there are (/should be) two records in a with x == 1
    // they may appear in any order
    let res = out.lookup(&[1.into()], true).unwrap();
    assert!(res
        .iter()
        .any(|r| r == &vec![1.into(), "a".into(), "n".into()]));
    assert!(res
        .iter()
        .any(|r| r == &vec![1.into(), "b".into(), "n".into()]));

    // there are (/should be) one record in a with x == 2
    assert_eq!(
        out.lookup(&[2.into()], true).unwrap(),
        vec![vec![2.into(), "c".into(), "o".into()]]
    );

    // there are (/should be) no records with x == 3
    assert!(out.lookup(&[3.into()], true).unwrap().is_empty());
}

#[test]
fn recipe_activates() {
    let mut g = start_simple("recipe_activates");
    g.migrate(|mig| {
        let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
        let mut r = Recipe::from_str(r_txt, None).unwrap();
        assert_eq!(r.version(), 0);
        assert_eq!(r.expressions().len(), 1);
        assert_eq!(r.prior(), None);
        assert!(r.activate(mig).is_ok());
    });
    // one base node
    assert_eq!(g.inputs().unwrap().len(), 1);
}

#[test]
fn recipe_activates_and_migrates() {
    let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
    let r1_txt = "QUERY qa: SELECT a FROM b;\n
                  QUERY qb: SELECT a, c FROM b WHERE a = 42;";

    let mut g = start_simple("recipe_activates_and_migrates");
    g.install_recipe(r_txt).unwrap();
    // one base node
    assert_eq!(g.inputs().unwrap().len(), 1);

    g.extend_recipe(r1_txt).unwrap();
    // still one base node
    assert_eq!(g.inputs().unwrap().len(), 1);
    // two leaf nodes
    assert_eq!(g.outputs().unwrap().len(), 2);
}

#[test]
fn recipe_activates_and_migrates_with_join() {
    let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                 CREATE TABLE b (r int, s int);\n";
    let r1_txt = "QUERY q: SELECT y, s FROM a, b WHERE a.x = b.r;";

    let mut g = start_simple("recipe_activates_and_migrates_with_join");
    g.install_recipe(r_txt).unwrap();

    // two base nodes
    assert_eq!(g.inputs().unwrap().len(), 2);

    g.extend_recipe(r1_txt).unwrap();

    // still two base nodes
    assert_eq!(g.inputs().unwrap().len(), 2);
    // one leaf node
    assert_eq!(g.outputs().unwrap().len(), 1);
}

fn test_queries(test: &str, file: &'static str, shard: bool, reuse: bool, log: bool) {
    use crate::logger_pls;
    use std::fs::File;
    use std::io::Read;

    let logger = if log { Some(logger_pls()) } else { None };

    // set up graph
    let mut g = if shard {
        start_simple(test)
    } else {
        start_simple_unsharded(test)
    };

    // move needed for some funny lifetime reason
    g.migrate(move |mig| {
        let mut r = Recipe::blank(logger);
        if !reuse {
            r.disable_reuse();
        }
        let mut f = File::open(&file).unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s
            .lines()
            .filter(|l| {
                !l.is_empty()
                    && !l.starts_with("--")
                    && !l.starts_with('#')
                    && !l.starts_with("DROP TABLE")
            })
            .map(|l| {
                if !(l.ends_with('\n') || l.ends_with(';')) {
                    String::from(l) + "\n"
                } else {
                    String::from(l)
                }
            })
            .collect();

        // Add them one by one
        for (_i, q) in lines.iter().enumerate() {
            //println!("{}: {}", i, q);
            r = match r.extend(q) {
                Ok(mut nr) => {
                    assert!(nr.activate(mig).is_ok());
                    nr
                }
                Err(e) => {
                    panic!("{:?}", e);
                }
            }
        }
    });
}

#[test]
fn finkelstein1982_queries() {
    use std::fs::File;
    use std::io::Read;

    // set up graph
    let mut g = start_simple("finkelstein1982_queries");
    g.migrate(|mig| {
        let mut inc = SqlIncorporator::default();
        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                if !(l.ends_with('\n') || l.ends_with(';')) {
                    String::from(l) + "\n"
                } else {
                    String::from(l)
                }
            })
            .collect();

        // Add them one by one
        for q in lines.iter() {
            assert!(inc.add_query(q, None, mig).is_ok());
        }
    });
}

#[test]
fn tpc_w() {
    test_queries("tpc-w", "tests/tpc-w-queries.txt", true, true, false);
}

#[test]
fn lobsters() {
    test_queries("lobsters", "tests/lobsters-schema.txt", false, false, false);
}

#[test]
fn soupy_lobsters() {
    test_queries(
        "soupy_lobsters",
        "tests/soupy-lobsters-schema.txt",
        false,
        false,
        false,
    );
}

#[test]
#[allow_fail]
fn node_removal() {
    // set up graph
    let mut b = Builder::default();
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        Duration::from_millis(1),
        Some(String::from("domain_removal")),
        1,
    ));
    let mut g = b.start_simple().unwrap();
    let cid = g.migrate(|mig| {
        let a = mig.add_base("a", &["a", "b"], Base::new(vec![]).with_key(vec![0]));
        let b = mig.add_base("b", &["a", "b"], Base::new(vec![]).with_key(vec![0]));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0])
    });

    let mut cq = g.view("c").unwrap().into_sync();
    let mut muta = g.table("a").unwrap().into_sync();
    let mut mutb = g.table("b").unwrap().into_sync();
    let id: DataType = 1.into();

    assert_eq!(muta.table_name(), "a");
    assert_eq!(muta.columns(), &["a", "b"]);

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).unwrap(),
        vec![vec![1.into(), 2.into()]]
    );

    {
        let fut = g.remove_node(cid);
        g.run(fut).unwrap();
    }

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // // check that value was updated again
    // let res = cq.lookup(&[id.clone()], true).unwrap();
    // assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    // assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));

    // Delete first record
    muta.delete(vec![id.clone()]).unwrap();

    // give it some time to propagate
    sleep();

    // // send a query to c
    // assert_eq!(
    //     cq.lookup(&[id.clone()], true),
    //     Ok(vec![vec![1.into(), 4.into()]])
    // );
}

#[test]
fn remove_query() {
    let r_txt = "CREATE TABLE b (a int, c text, x text);\n
                 QUERY qa: SELECT a FROM b;\n
                 QUERY qb: SELECT a, c FROM b WHERE a = 42;";

    let r2_txt = "CREATE TABLE b (a int, c text, x text);\n
                  QUERY qa: SELECT a FROM b;";

    let mut g = Builder::default().start_simple().unwrap();
    g.install_recipe(r_txt).unwrap();
    assert_eq!(g.inputs().unwrap().len(), 1);
    assert_eq!(g.outputs().unwrap().len(), 2);

    let mut mutb = g.table("b").unwrap().into_sync();
    let mut qa = g.view("qa").unwrap().into_sync();
    let mut qb = g.view("qb").unwrap().into_sync();

    mutb.insert(vec![42.into(), "2".into(), "3".into()])
        .unwrap();
    mutb.insert(vec![1.into(), "4".into(), "5".into()]).unwrap();
    sleep();

    assert_eq!(qa.lookup(&[0.into()], true).unwrap().len(), 2);
    assert_eq!(qb.lookup(&[0.into()], true).unwrap().len(), 1);

    // Remove qb and check that the graph still functions as expected.
    g.install_recipe(r2_txt).unwrap();
    assert_eq!(g.inputs().unwrap().len(), 1);
    assert_eq!(g.outputs().unwrap().len(), 1);
    assert!(g.view("qb").is_err());

    mutb.insert(vec![42.into(), "6".into(), "7".into()])
        .unwrap();
    sleep();

    assert_eq!(qa.lookup(&[0.into()], true).unwrap().len(), 3);
    assert_eq!(qb.lookup(&[0.into()], true).unwrap().len(), 1);
}

#[test]
fn correct_nested_view_schema() {
    use nom_sql::{ColumnSpecification, SqlType};

    let r_txt = "CREATE TABLE votes (story int, user int);
                 CREATE TABLE stories (id int, content text);
                 VIEW swvc: SELECT stories.id, stories.content, COUNT(votes.user) AS vc \
                     FROM stories \
                     JOIN votes ON (stories.id = votes.story) \
                     WHERE stories.id = ? GROUP BY votes.story;";

    let mut b = Builder::default();
    // need to disable partial due to lack of support for key subsumption (#99)
    b.disable_partial();
    b.set_sharding(None);
    let mut g = b.start_simple().unwrap();
    g.install_recipe(r_txt).unwrap();

    let q = g.view("swvc").unwrap().into_sync();

    let expected_schema = vec![
        ColumnSpecification::new("swvc.id".into(), SqlType::Int(32)),
        ColumnSpecification::new("swvc.content".into(), SqlType::Text),
        ColumnSpecification::new("swvc.vc".into(), SqlType::Bigint(64)),
    ];
    assert_eq!(q.schema(), Some(&expected_schema[..]));
}
