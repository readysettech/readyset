extern crate glob;

use consensus::LocalAuthority;
use controller::ControllerBuilder;
use controller::recipe::Recipe;
use controller::sql::SqlIncorporator;
use core::{DataType, DurabilityMode};
use dataflow::PersistenceParameters;
use dataflow::checktable::Token;
use dataflow::ops::base::Base;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::identity::Identity;
use dataflow::ops::join::JoinSource::*;
use dataflow::ops::join::{Join, JoinSource, JoinType};
use dataflow::ops::project::Project;
use dataflow::ops::union::Union;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

const DEFAULT_SETTLE_TIME_MS: u64 = 200;

// Suffixes the given log prefix with a timestamp, ensuring that
// subsequent test runs do not reuse log files in the case of failures.
fn get_log_name(prefix: &str) -> String {
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!(
        "{}.{}.{}",
        prefix,
        current_time.as_secs(),
        current_time.subsec_nanos()
    )
}

// Ensures correct handling of log file names, by deleting used log files in Drop.
struct LogName {
    name: String,
}

impl LogName {
    fn new(prefix: &str) -> LogName {
        let name = get_log_name(prefix);
        LogName { name }
    }
}

// Removes the log files matching the glob ./{log_name}-*.json.
// Used to clean up after recovery tests, where a persistent log is created.
impl Drop for LogName {
    fn drop(&mut self) {
        for log_path in glob::glob(&format!("./{}-*.json", self.name)).unwrap() {
            fs::remove_file(log_path.unwrap()).unwrap();
        }
    }
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
    let mut b = ControllerBuilder::default();
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        128,
        Duration::from_millis(1),
        Some(get_log_name("it_works_basic")),
        true,
    ));
    let mut g = b.build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec![]).with_key(vec![0]));
        let b = mig.add_ingredient("b", &["a", "b"], Base::new(vec![]).with_key(vec![0]));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.get_getter("c").unwrap();
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();
    let id: DataType = 1.into();

    assert_eq!(muta.table_name(), "a");
    assert_eq!(muta.columns(), &["a", "b"]);

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

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
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 4.into()]])
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
    use core::{Modification, Operation};

    let mut g = ControllerBuilder::default().build_local();
    g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec![]).with_key(vec![0]));
        mig.maintain_anonymous(a, &[0]);
    });

    let mut read = g.get_getter("a").unwrap();
    let mut write = g.get_mutator("a").unwrap();

    // insert a new record
    write.put(vec![1.into(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );

    // update that record in place (set)
    write
        .update(vec![1.into()], vec![(1, Modification::Set(3.into()))])
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 3.into()]])
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
        read.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 4.into()]])
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
        read.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 5.into()]])
    );

    // delete should, well, delete
    write.delete(vec![1.into()]).unwrap();
    sleep();
    assert_eq!(read.lookup(&[1.into()], true), Ok(vec![]));

    // insert or update should insert
    write
        .insert_or_update(
            vec![1.into(), 2.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .unwrap();
    sleep();
    assert_eq!(
        read.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );
}

#[test]
fn shared_interdomain_ancestor() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());

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

    let mut bq = g.get_getter("b").unwrap();
    let mut cq = g.get_getter("c").unwrap();
    let mut muta = g.get_mutator("a").unwrap();
    let id: DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        bq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    let id: DataType = 2.into();
    muta.put(vec![id.clone(), 4.into()]).unwrap();
    sleep();
    assert_eq!(
        bq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 4.into()].into()])
    );
}

#[test]
fn it_works_w_mat() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.get_getter("c").unwrap();
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();
    let id: DataType = 1.into();

    // send a few values on a
    muta.put(vec![id.clone(), 1.into()]).unwrap();
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    muta.put(vec![id.clone(), 3.into()]).unwrap();

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
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
    mutb.put(vec![id.clone(), 5.into()]).unwrap();
    mutb.put(vec![id.clone(), 6.into()]).unwrap();

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
    let b = ControllerBuilder::default();
    let mut g = b.build_local();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        (a, b)
    });

    let mut muta = g.get_mutator("a").unwrap();
    let id: DataType = 1.into();

    // send a few values on a
    muta.put(vec![id.clone(), 1.into()]).unwrap();
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    muta.put(vec![id.clone(), 3.into()]).unwrap();

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

    let mut cq = g.get_getter("c").unwrap();

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
    let b = ControllerBuilder::default();
    let mut g = b.build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut muta = g.get_mutator("a").unwrap();
    let id: DataType = 1.into();

    // send a few values on a
    muta.put(vec![id.clone(), 1.into()]).unwrap();
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    muta.put(vec![id.clone(), 3.into()]).unwrap();

    // give it some time to propagate
    sleep();

    let mut cq = g.get_getter("c").unwrap();

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
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["x", "y"], Base::new(vec![]).with_key(vec![1]));
        let b = mig.add_ingredient("b", &["_", "x", "y"], Base::new(vec![]).with_key(vec![2]));

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![1, 2]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["x", "y"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.get_getter("c").unwrap();
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    // send a value on a
    muta.put(vec![1.into(), 2.into()]).unwrap();
    sleep();
    assert_eq!(
        cq.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 2.into()].into()])
    );

    // send a value on b
    mutb.put(vec![0.into(), 1.into(), 4.into()]).unwrap();
    sleep();

    let res = cq.lookup(&[1.into()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![1.into(), 2.into()]));
    assert!(res.contains(&vec![1.into(), 4.into()]));

    // delete first value
    muta.delete(vec![2.into()]).unwrap();
    sleep();
    assert_eq!(
        cq.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), 4.into()]])
    );
}

#[test]
fn it_works_with_sql_recipe() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));
        QUERY CountCars: SELECT COUNT(*) FROM Car WHERE brand = ?;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut mutator = g.get_mutator("Car").unwrap();
    let mut getter = g.get_getter("CountCars").unwrap();

    assert_eq!(mutator.table_name(), "Car");
    assert_eq!(mutator.columns(), &["id", "brand"]);

    let brands = vec!["Volvo", "Volvo", "Volkswagen"];
    for (i, &brand) in brands.iter().enumerate() {
        mutator.put(vec![i.into(), brand.into()]).unwrap();
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
    let mut g = ControllerBuilder::default().build_local();
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

    g.install_recipe(sql.to_owned()).unwrap();
    let mut article = g.get_mutator("Article").unwrap();
    let mut vote = g.get_mutator("Vote").unwrap();
    let mut awvc = g.get_getter("ArticleWithVoteCount").unwrap();

    article.put(vec![0i64.into(), "Article".into()]).unwrap();
    article.put(vec![1i64.into(), "Article".into()]).unwrap();
    vote.put(vec![0i64.into(), 0.into()]).unwrap();

    sleep();

    let rs = awvc.lookup(&[0i64.into()], true).unwrap();
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0], vec![0i64.into(), "Article".into(), 1.into()]);

    let empty = awvc.lookup(&[1i64.into()], true).unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0], vec![1i64.into(), "Article".into(), DataType::None]);
}

#[test]
fn it_works_with_reads_before_writes() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Article (aid int, PRIMARY KEY(aid));
        CREATE TABLE Vote (aid int, uid int, PRIMARY KEY(aid, uid));
        QUERY ArticleVote: SELECT Article.aid, Vote.uid \
            FROM Article, Vote \
            WHERE Article.aid = Vote.aid AND Article.aid = ?;
    ";

    g.install_recipe(sql.to_owned()).unwrap();
    let mut article = g.get_mutator("Article").unwrap();
    let mut vote = g.get_mutator("Vote").unwrap();
    let mut awvc = g.get_getter("ArticleVote").unwrap();

    let aid = 1;
    let uid = 10;

    assert!(awvc.lookup(&[aid.into()], true).unwrap().is_empty());
    article.put(vec![aid.into()]).unwrap();
    sleep();

    vote.put(vec![aid.into(), uid.into()]).unwrap();
    sleep();

    let result = awvc.lookup(&[aid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![aid.into(), uid.into()]);
}

#[test]
#[allow_fail]
fn forced_shuffle_despite_same_shard() {
    // XXX: this test doesn't currently *fail* despite
    // multiple trailing replay responses that are simply ignored...

    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(pid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        QUERY CarPrice: SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut car_mutator = g.get_mutator("Car").unwrap();
    let mut price_mutator = g.get_mutator("Price").unwrap();
    let mut getter = g.get_getter("CarPrice").unwrap();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator.put(vec![pid.into(), price.into()]).unwrap();
    car_mutator.put(vec![cid.into(), pid.into()]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());
}

#[test]
#[allow_fail]
fn double_shuffle() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(cid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        QUERY CarPrice: SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut car_mutator = g.get_mutator("Car").unwrap();
    let mut price_mutator = g.get_mutator("Price").unwrap();
    let mut getter = g.get_getter("CarPrice").unwrap();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator.put(vec![pid.into(), price.into()]).unwrap();
    car_mutator.put(vec![cid.into(), pid.into()]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());
}

#[test]
fn it_works_with_arithmetic_aliases() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Price (pid int, cent_price int, PRIMARY KEY(pid));
        ModPrice: SELECT pid, cent_price / 100 AS price FROM Price;
        QUERY AltPrice: SELECT pid, price FROM ModPrice WHERE pid = ?;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut price_mutator = g.get_mutator("Price").unwrap();
    let mut getter = g.get_getter("AltPrice").unwrap();
    let pid = 1;
    let price = 10000;
    price_mutator.put(vec![pid.into(), price.into()]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[pid.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (price / 100).into());
}

#[test]
fn it_recovers_persisted_logs() {
    let authority = Arc::new(LocalAuthority::new());
    let log_name = LogName::new("it_recovers_persisted_logs");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        128,
        Duration::from_millis(1),
        Some(log_name.name.clone()),
        true,
    );

    {
        let mut g = ControllerBuilder::default();
        g.set_persistence(persistence_params.clone());
        let mut g = g.build(authority.clone());

        let sql = "
            CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
            QUERY CarPrice: SELECT price FROM Car WHERE id = ?;
        ";
        g.install_recipe(sql.to_owned()).unwrap();

        let mut mutator = g.get_mutator("Car").unwrap();

        for i in 1..10 {
            let price = i * 10;
            mutator.put(vec![i.into(), price.into()]).unwrap();
        }

        // Let writes propagate:
        sleep();
    }

    let mut g = ControllerBuilder::default();
    g.set_persistence(persistence_params);
    let mut g = g.build(authority.clone());
    let mut getter = g.get_getter("CarPrice").unwrap();

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
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        // migrate

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        mig.maintain_anonymous(vc, &[0]);
        (vote, vc)
    });

    let mut vc_state = g.get_getter("votecount").unwrap();

    let ids = 10;
    let votes = 7;

    // continuously write to vote with new mutators
    let user: DataType = 0.into();
    for _ in 0..votes {
        for i in 0..ids {
            g.get_mutator("vote")
                .unwrap()
                .put(vec![user.clone(), i.into()])
                .unwrap();
        }
    }

    // allow the system to catch up with the last writes
    sleep();

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true),
            Ok(vec![vec![i.into(), votes.into()]])
        );
    }
}

#[test]
fn it_recovers_persisted_logs_w_multiple_nodes() {
    let authority = Arc::new(LocalAuthority::new());
    let log_name = LogName::new("it_recovers_persisted_logs_w_multiple_nodes");
    let tables = vec!["A", "B", "C"];
    let persistence_parameters = PersistenceParameters::new(
        DurabilityMode::Permanent,
        128,
        Duration::from_millis(1),
        Some(log_name.name.clone()),
        true,
    );

    {
        let mut g = ControllerBuilder::default();
        g.set_persistence(persistence_parameters.clone());
        let mut g = g.build(authority.clone());

        let sql = "
            CREATE TABLE A (id int, PRIMARY KEY(id));
            CREATE TABLE B (id int, PRIMARY KEY(id));
            CREATE TABLE C (id int, PRIMARY KEY(id));

            QUERY AID: SELECT id FROM A WHERE id = ?;
            QUERY BID: SELECT id FROM B WHERE id = ?;
            QUERY CID: SELECT id FROM C WHERE id = ?;
        ";
        g.install_recipe(sql.to_owned()).unwrap();
        for (i, table) in tables.iter().enumerate() {
            let mut mutator = g.get_mutator(table.to_owned()).unwrap();
            mutator.put(vec![i.into()]).unwrap();
        }
        sleep();
    }

    // Create a new controller with the same authority, and make sure that it recovers to the same
    // state that the other one had.
    let mut g = ControllerBuilder::default();
    g.set_persistence(persistence_parameters);
    let mut g = g.build(authority.clone());
    for (i, table) in tables.iter().enumerate() {
        let mut getter = g.get_getter(&format!("{}ID", table)).unwrap();
        let result = getter.lookup(&[i.into()], true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], i.into());
    }
}

#[test]
#[ignore]
fn it_recovers_persisted_logs_w_transactions() {
    let authority = Arc::new(LocalAuthority::new());
    let log_name = LogName::new("it_recovers_persisted_logs_w_transactions");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        128,
        Duration::from_millis(1),
        Some(log_name.name.clone()),
        true,
    );

    {
        let mut g = ControllerBuilder::default();
        g.set_persistence(persistence_params.clone());
        let mut g = g.build(authority.clone());

        // TODO: Convert this to use SQL interface (because only migrations specified that way get
        // persisted...)
        let _ = g.migrate(|mig| {
            let a = mig.add_transactional_base("a", &["a", "b"], Base::default());
            mig.maintain_anonymous(a, &[0]);
            a
        });

        let mut mutator = g.get_mutator("a").unwrap();

        for i in 1..10 {
            let b = i * 10;
            mutator
                .transactional_put(vec![i.into(), b.into()], Token::empty())
                .unwrap();
        }

        // Let writes propagate:
        sleep();
    }

    let mut g = ControllerBuilder::default();
    g.set_persistence(persistence_params.clone());
    let mut g = g.build(authority.clone());
    let mut getter = g.get_getter("a").unwrap();
    for i in 1..10 {
        let b = i * 10;
        let (result, _token) = getter.transactional_lookup(&[i.into()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], i.into());
        assert_eq!(result[0][1], b.into());
    }
}

#[test]
fn it_works_with_simple_arithmetic() {
    let mut g = ControllerBuilder::default().build_local();

    g.migrate(|mig| {
        let sql = "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
                   QUERY CarPrice: SELECT 2 * price FROM Car WHERE id = ?;";
        let mut recipe = Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
    });

    let mut mutator = g.get_mutator("Car").unwrap();
    let mut getter = g.get_getter("CarPrice").unwrap();
    let id: DataType = 1.into();
    let price: DataType = 123.into();
    mutator.put(vec![id.clone(), price]).unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.clone()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], 246.into());
}

#[test]
fn it_works_with_multiple_arithmetic_expressions() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
               QUERY CarPrice: SELECT 10 * 10, 2 * price, 10 * price, FROM Car WHERE id = ?;
               ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut mutator = g.get_mutator("Car").unwrap();
    let mut getter = g.get_getter("CarPrice").unwrap();
    let id: DataType = 1.into();
    let price: DataType = 123.into();
    mutator.put(vec![id.clone(), price]).unwrap();

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
#[allow_fail]
fn it_works_with_join_arithmetic() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Car (car_id int, price_id int, PRIMARY KEY(car_id));
        CREATE TABLE Price (price_id int, price int, PRIMARY KEY(price_id));
        CREATE TABLE Sales (sales_id int, price_id int, fraction float, PRIMARY KEY(sales_id));
        QUERY CarPrice: SELECT price * fraction FROM Car \
                  JOIN Price ON Car.price_id = Price.price_id \
                  JOIN Sales ON Price.price_id = Sales.price_id \
                  WHERE car_id = ?;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut car_mutator = g.get_mutator("Car").unwrap();
    let mut price_mutator = g.get_mutator("Price").unwrap();
    let mut sales_mutator = g.get_mutator("Sales").unwrap();
    let mut getter = g.get_getter("CarPrice").unwrap();
    let id = 1;
    let price = 123;
    let fraction = 0.7;
    car_mutator.put(vec![id.into(), id.into()]).unwrap();
    price_mutator.put(vec![id.into(), price.into()]).unwrap();
    sales_mutator
        .put(vec![id.into(), id.into(), fraction.into()])
        .unwrap();

    // Let writes propagate:
    sleep();

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.into()], true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (price as f64 * fraction).into());
}

#[test]
fn it_works_with_function_arithmetic() {
    let mut g = ControllerBuilder::default().build_local();
    let sql = "
        CREATE TABLE Bread (id int, price int, PRIMARY KEY(id));
        QUERY Price: SELECT 2 * MAX(price) FROM Bread;
    ";
    g.install_recipe(sql.to_owned()).unwrap();

    let mut mutator = g.get_mutator("Bread").unwrap();
    let mut getter = g.get_getter("Price").unwrap();
    let max_price = 20;
    for (i, price) in (10..max_price + 1).enumerate() {
        let id = i + 1;
        mutator.put(vec![id.into(), price.into()]).unwrap();
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
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_ingredient("article1", &["id", "title"], Base::default());
        let article2 = mig.add_ingredient("article2", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        mig.maintain_anonymous(article, &[0]);

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

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

    let mut articleq = g.get_getter("article").unwrap();
    let mut vcq = g.get_getter("vc").unwrap();
    let mut endq = g.get_getter("end").unwrap();

    let mut mut1 = g.get_mutator("article1").unwrap();
    let mut mut2 = g.get_mutator("article2").unwrap();
    let mut mutv = g.get_mutator("vote").unwrap();

    let a1: DataType = 1.into();
    let a2: DataType = 2.into();

    // make one article
    mut1.put(vec![a1.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // query articles to see that it was updated
    assert_eq!(
        articleq.lookup(&[a1.clone()], true),
        Ok(vec![vec![a1.clone(), 2.into()]])
    );

    // make another article
    mut2.put(vec![a2.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(
        articleq.lookup(&[a1.clone()], true),
        Ok(vec![vec![a1.clone(), 2.into()]])
    );
    assert_eq!(
        articleq.lookup(&[a2.clone()], true),
        Ok(vec![vec![a2.clone(), 4.into()]])
    );

    // create a vote (user 1 votes for article 1)
    mutv.put(vec![1.into(), a1.clone()]).unwrap();

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
fn transactional_vote() {
    // set up graph
    let mut g = ControllerBuilder::default();
    g.disable_partial(); // because end_votes forces full below partial
    let mut g = g.build_local();
    let validate = g.get_validator();

    let _ = g.migrate(|mig| {
        // add article base nodes (we use two so we can exercise unions too)
        let article1 = mig.add_transactional_base("article1", &["id", "title"], Base::default());
        let article2 = mig.add_transactional_base("article2", &["id", "title"], Base::default());

        // add a (stupid) union of article1 + article2
        let mut emits = HashMap::new();
        emits.insert(article1, vec![0, 1]);
        emits.insert(article2, vec![0, 1]);
        let u = Union::new(emits);
        let article = mig.add_ingredient("article", &["id", "title"], u);
        mig.maintain_anonymous(article, &[0]);

        // add vote base table
        let vote = mig.add_transactional_base("vote", &["user", "id"], Base::default());

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
        let end_title =
            mig.add_ingredient("end_title", &["id", "title", "votes"], Identity::new(end));
        let end_votes =
            mig.add_ingredient("end_votes", &["id", "title", "votes"], Identity::new(end));

        mig.maintain_anonymous(end, &[0]);
        mig.maintain_anonymous(end_title, &[1]);
        mig.maintain_anonymous(end_votes, &[2]);

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

    let mut articleq = g.get_getter("article").unwrap();
    let mut vcq = g.get_getter("vc").unwrap();
    let mut endq = g.get_getter("end").unwrap();
    let mut endq_title = g.get_getter("end_title").unwrap();
    let mut endq_votes = g.get_getter("end_votes").unwrap();

    let mut mut1 = g.get_mutator("article1").unwrap();
    let mut mut2 = g.get_mutator("article2").unwrap();
    let mut mutv = g.get_mutator("vote").unwrap();

    let a1: DataType = 1.into();
    let a2: DataType = 2.into();

    let token = articleq.transactional_lookup(&[a1.clone()]).unwrap().1;

    let endq_token = endq.transactional_lookup(&[a2.clone()]).unwrap().1;
    let endq_title_token = endq_title.transactional_lookup(&[4.into()]).unwrap().1;
    let endq_votes_token = endq_votes.transactional_lookup(&[0.into()]).unwrap().1;

    // make one article
    assert!(
        mut1.transactional_put(vec![a1.clone(), 2.into()], token)
            .is_ok()
    );

    // give it some time to propagate
    sleep();

    // query articles to see that it was absorbed
    let (res, token) = articleq.transactional_lookup(&[a1.clone()]).unwrap();
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
    sleep();

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    let (res, mut token) = articleq.transactional_lookup(&[a1.clone()]).unwrap();
    assert_eq!(res, vec![vec![a1.clone(), 2.into()]]);
    let (res, token2) = articleq.transactional_lookup(&[a2.clone()]).unwrap();
    assert_eq!(res, vec![vec![a2.clone(), 4.into()]]);
    // check endq tokens are as expected
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // Check that the two reads happened transactionally.
    token.merge(token2);
    assert!(validate(&token));

    let endq_token = endq.transactional_lookup(&[a1.clone()]).unwrap().1;
    let endq_title_token = endq_title.transactional_lookup(&[4.into()]).unwrap().1;
    let endq_votes_token = endq_votes.transactional_lookup(&[0.into()]).unwrap().1;

    // create a vote (user 1 votes for article 1)
    assert!(
        mutv.transactional_put(vec![1.into(), a1.clone()], token)
            .is_ok()
    );

    // give it some time to propagate
    sleep();

    // check endq tokens
    assert!(!validate(&endq_token));
    assert!(!validate(&endq_title_token));
    assert!(!validate(&endq_votes_token));

    // query vote count to see that the count was updated
    let res = vcq.lookup(&[a1.clone()], true).unwrap();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq.transactional_lookup(&[a1.clone()]).unwrap().0;
    assert_eq!(res.len(), 1);
    assert!(
        res.iter()
            .any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
        "no entry for [1,2,1|2] in {:?}",
        res
    );

    // check that article 2 doesn't have any votes
    let res = endq.transactional_lookup(&[a2.clone()]).unwrap().0;
    assert!(res.len() <= 1); // could be 1 if we had zero-rows
}

#[test]
fn empty_migration() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    g.migrate(|_| {});

    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());

        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        (a, b, c)
    });

    let mut cq = g.get_getter("c").unwrap();
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();
    let id: DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

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
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        mig.maintain_anonymous(a, &[0]);
        a
    });

    let mut aq = g.get_getter("a").unwrap();
    let mut muta = g.get_mutator("a").unwrap();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );

    // add unrelated node b in a migration
    let _ = g.migrate(|mig| {
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        mig.maintain_anonymous(b, &[0]);
        b
    });

    let mut bq = g.get_getter("b").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that b got it
    assert_eq!(
        bq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 4.into()]])
    );
}

#[test]
fn add_columns() {
    let id: DataType = "x".into();

    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec![1.into(), 2.into()]));
        mig.maintain_anonymous(a, &[0]);
        a
    });
    let mut aq = g.get_getter("a").unwrap();
    let mut muta = g.get_mutator("a").unwrap();

    // send a value on a
    muta.put(vec![id.clone(), "y".into()]).unwrap();
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), "y".into()].into()])
    );

    // add a third column to a
    g.migrate(move |mig| {
        mig.add_column(a, "c", 3.into());
    });
    sleep();

    // send another (old) value on a
    muta.put(vec![id.clone(), "z".into()]).unwrap();
    sleep();

    // check that a got it, and added the new, third column's default
    let res = aq.lookup(&[id.clone()], true).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), "y".into()]));
    assert!(res.contains(&vec![id.clone(), "z".into(), 3.into()]));

    // get a new muta and send a new value on it
    let mut muta = g.get_mutator("a").unwrap();
    muta.put(vec![id.clone(), "a".into(), 10.into()]).unwrap();
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
    let mut g = ControllerBuilder::default().build_local();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec![1.into(), 2.into()]));
        a
    });
    let mut muta = g.get_mutator("a").unwrap();

    // send a value on a
    muta.put(vec![id.clone(), "y".into()]).unwrap();
    sleep();

    // add a third column to a, and a view that uses it
    let _ = g.migrate(move |mig| {
        mig.add_column(a, "c", 3.into());
        let b = mig.add_ingredient("x", &["c", "b"], Project::new(a, &[2, 0], None, None));
        mig.maintain_anonymous(b, &[1]);
        b
    });

    let mut bq = g.get_getter("x").unwrap();

    // send another (old) value on a
    muta.put(vec![id.clone(), "z".into()]).unwrap();
    // and an entirely new value
    let mut muta = g.get_mutator("a").unwrap();
    muta.put(vec![id.clone(), "a".into(), 10.into()]).unwrap();

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
    let mut g = ControllerBuilder::default().build_local();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec!["a".into(), "b".into()]));
        mig.maintain_anonymous(a, &[0]);
        a
    });
    let mut aq = g.get_getter("a").unwrap();
    let mut muta1 = g.get_mutator("a").unwrap();

    // send a value on a
    muta1.put(vec![id.clone(), "bx".into()]).unwrap();

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
    let mut muta2 = g.get_mutator("a").unwrap();
    muta2.put(vec![id.clone()]).unwrap();

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
    let mut muta3 = g.get_mutator("a").unwrap();
    muta3.put(vec![id.clone(), "cy".into()]).unwrap();

    // using an old putter now should add default for c
    muta1.put(vec![id.clone(), "bz".into()]).unwrap();

    // using putter that knows of neither b nor c should result in defaults for both
    muta2.put(vec![id.clone()]).unwrap();
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
    let mut g = ControllerBuilder::default().build_local();
    let a = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::new(vec![1.into(), 2.into()]));
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
    let mut bq = g.get_getter("x").unwrap();
    assert!(bq.lookup(&[3.into()], true).unwrap().is_empty());
}

#[test]
#[allow_fail]
fn replay_during_replay() {
    // what we're trying to set up here is a case where a join receives a record with a value for
    // the join key that does not exist in the view the record was sent from. since joins only do
    // lookups into the origin view during forward processing when it receives things from the
    // right in a left join, that's what we have to construct.
    let mut g = ControllerBuilder::default();
    g.disable_partial();
    let mut g = g.build_local();
    let (a, u1, u2) = g.migrate(|mig| {
        // we need three bases:
        //
        //  - a will be the left side of the left join
        //  - u1 and u2 will be joined together with a regular one-to-one join to produce a partial
        //    view (remember, we need to miss in the source of the replay, so it must be partial).
        let a = mig.add_ingredient("a", &["a"], Base::new(vec![1.into()]));
        let u1 = mig.add_ingredient("u1", &["u"], Base::new(vec![1.into()]));
        let u2 = mig.add_ingredient("u2", &["u", "a"], Base::new(vec![1.into(), 2.into()]));
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

    let mut muta = g.get_mutator("a").unwrap();
    let mut mutu1 = g.get_mutator("u1").unwrap();
    let mut mutu2 = g.get_mutator("u2").unwrap();

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

    sleep();

    // since u and target are both partial, the writes should not actually have propagated through
    // yet. do a read to see that one makes it through correctly:
    let mut r = g.get_getter("end").unwrap();

    assert_eq!(
        r.lookup(&[1.into()], true),
        Ok(vec![vec![1.into(), "a".into()]])
    );

    // we now know that u has key a=1 in its index
    // now we add a secondary index on u.u
    g.migrate(move |mig| {
        mig.maintain_anonymous(u, &[0]);
    });

    let mut second = g.get_getter("u").unwrap();

    // second is partial and empty, so any read should trigger a replay.
    // though that shouldn't interact with target in any way.
    assert_eq!(
        second.lookup(&["a".into()], true),
        Ok(vec![vec!["a".into(), 1.into()]])
    );

    // now we get to the funky part.
    // we're going to request a second key from the secondary index on `u`, which causes that hole
    // to disappear. then we're going to do a write to `u2` that has that second key, but has an
    // "a" value for which u has a hole. that record is then going to be forwarded to *both*
    // children, and it'll be interesting to see what the join then does.
    assert_eq!(
        second.lookup(&["b".into()], true),
        Ok(vec![vec!["b".into(), 2.into()]])
    );

    // u has a hole for a=2, but not for u=b, and so should forward this to both children
    mutu2.put(vec!["b".into(), 2.into()]).unwrap();

    sleep();

    // what happens if we now query for 2?
    assert_eq!(
        r.lookup(&[2.into()], true),
        Ok(vec![vec![2.into(), "b".into()], vec![2.into(), "b".into()]])
    );
}

#[test]
fn full_aggregation_with_bogokey() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let base = g.migrate(|mig| mig.add_ingredient("base", &["x"], Base::new(vec![1.into()])));

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

    let mut aggq = g.get_getter("agg").unwrap();
    let mut base = g.get_mutator("base").unwrap();

    // insert some values
    base.put(vec![1.into()]).unwrap();
    base.put(vec![2.into()]).unwrap();
    base.put(vec![3.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // send a query to aggregation materialization
    assert_eq!(
        aggq.lookup(&[0.into()], true),
        Ok(vec![vec![0.into(), 3.into()]])
    );

    // update value again
    base.put(vec![4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that value was updated again
    assert_eq!(
        aggq.lookup(&[0.into()], true),
        Ok(vec![vec![0.into(), 4.into()]])
    );
}

#[test]
fn transactional_migration() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let a = g.migrate(|mig| {
        let a = mig.add_transactional_base("a", &["a", "b"], Base::default());
        mig.maintain_anonymous(a, &[0]);
        a
    });

    let mut aq = g.get_getter("a").unwrap();
    let mut muta = g.get_mutator("a").unwrap();

    // send a value on a
    muta.transactional_put(vec![1.into(), 2.into()], Token::empty())
        .unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        aq.transactional_lookup(&[1.into()]).unwrap().0,
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let b = g.migrate(|mig| {
        let b = mig.add_transactional_base("b", &["a", "b"], Base::default());
        mig.maintain_anonymous(b, &[0]);
        b
    });

    let mut bq = g.get_getter("b").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    // send a value on b
    mutb.transactional_put(vec![2.into(), 4.into()], Token::empty())
        .unwrap();

    // give it some time to propagate
    sleep();

    // check that b got it
    assert_eq!(
        bq.transactional_lookup(&[2.into()]).unwrap().0,
        vec![vec![2.into(), 4.into()]]
    );

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });

    let mut cq = g.get_getter("c").unwrap();

    // check that c has both previous entries
    assert_eq!(
        aq.transactional_lookup(&[1.into()]).unwrap().0,
        vec![vec![1.into(), 2.into()]]
    );
    assert_eq!(
        bq.transactional_lookup(&[2.into()]).unwrap().0,
        vec![vec![2.into(), 4.into()]]
    );

    // send a value on a and b
    muta.transactional_put(vec![3.into(), 5.into()], Token::empty())
        .unwrap();
    mutb.transactional_put(vec![3.into(), 6.into()], Token::empty())
        .unwrap();

    // give them some time to propagate
    sleep();

    // check that c got them
    let res = cq.transactional_lookup(&[3.into()]).unwrap().0;

    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![3.into(), 5.into()]));

    assert!(res.contains(&vec![3.into(), 6.into()]));
}

#[test]
fn crossing_migration() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        (a, b)
    });
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });

    let mut cq = g.get_getter("c").unwrap();

    let id: DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    sleep();

    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
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
    let mut g = ControllerBuilder::default().build_local();
    let _ = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        mig.maintain_anonymous(a, &[0]);
        a
    });

    let mut aq = g.get_getter("a").unwrap();
    let mut muta = g.get_mutator("a").unwrap();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 2.into()]])
    );

    // add unrelated node b in a migration
    let _ = g.migrate(|mig| {
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        mig.maintain_anonymous(b, &[0]);
        b
    });

    let mut bq = g.get_getter("b").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    // send a value on b
    mutb.put(vec![id.clone(), 4.into()]).unwrap();

    // give it some time to propagate
    sleep();

    // check that a got it
    assert_eq!(
        bq.lookup(&[id.clone()], true),
        Ok(vec![vec![1.into(), 4.into()]])
    );
}

#[test]
fn domain_amend_migration() {
    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["a", "b"], Base::default());
        let b = mig.add_ingredient("b", &["a", "b"], Base::default());
        (a, b)
    });
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    let _ = g.migrate(move |mig| {
        let mut emits = HashMap::new();
        emits.insert(a, vec![0, 1]);
        emits.insert(b, vec![0, 1]);
        let u = Union::new(emits);
        let c = mig.add_ingredient("c", &["a", "b"], u);
        mig.maintain_anonymous(c, &[0]);
        c
    });
    let mut cq = g.get_getter("c").unwrap();

    let id: DataType = 1.into();

    // send a value on a
    muta.put(vec![id.clone(), 2.into()]).unwrap();
    sleep();

    assert_eq!(
        cq.lookup(&[id.clone()], true),
        Ok(vec![vec![id.clone(), 2.into()].into()])
    );

    // update value again
    mutb.put(vec![id.clone(), 4.into()]).unwrap();
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

    let mut g = ControllerBuilder::default().build_local();
    let left = g.migrate(|mig| {
        // base node, so will be materialized
        let left = mig.add_ingredient("foo", &["a", "b"], Base::default());

        // node in different domain that depends on foo causes egress to be added
        mig.add_ingredient("bar", &["a", "b"], Identity::new(left));
        left
    });

    g.migrate(move |mig| {
        // joins require their inputs to be materialized
        // we need a new base as well so we can actually make a join
        let tmp = mig.add_ingredient("tmp", &["a", "b"], Base::default());
        let j = Join::new(
            left,
            tmp,
            JoinType::Inner,
            vec![JoinSource::B(0, 0), JoinSource::R(1)],
        );
        mig.add_ingredient("join", &["a", "b"], j);
    });
    assert!(true);
}

fn do_full_vote_migration(old_puts_after: bool) {
    let mut g = ControllerBuilder::default().build_local();
    let (article, _vote, vc, _end) = g.migrate(|mig| {
        // migrate

        // add article base node
        let article = mig.add_ingredient("article", &["id", "title"], Base::default());

        // add vote base table
        // NOTE: the double-column key here means that we can't shard vote
        let vote = mig.add_ingredient(
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
    let mut muta = g.get_mutator("article").unwrap();
    let mut mutv = g.get_mutator("vote").unwrap();

    let n = 250i64;
    let title: DataType = "foo".into();
    let raten: DataType = 5.into();

    for i in 0..n {
        muta.put(vec![i.into(), title.clone()]).unwrap();
    }
    for i in 0..n {
        mutv.put(vec![1.into(), i.into()]).unwrap();
    }

    let mut last = g.get_getter("awvc").unwrap();
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
        let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base::default());

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

    let mut last = g.get_getter("awr").unwrap();
    let mut mutr = g.get_mutator("rating").unwrap();
    for i in 0..n {
        if old_puts_after {
            mutv.put(vec![2.into(), i.into()]).unwrap();
        }
        mutr.put(vec![2.into(), i.into(), raten.clone()]).unwrap();
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
    do_full_vote_migration(false);
}

#[test]
fn full_vote_migration_new_and_old() {
    do_full_vote_migration(true);
}

#[test]
fn live_writes() {
    let mut g = ControllerBuilder::default().build_local();
    let (_vote, vc) = g.migrate(|mig| {
        // migrate

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        mig.maintain_anonymous(vc, &[0]);
        (vote, vc)
    });

    let mut vc_state = g.get_getter("votecount").unwrap();
    let mut add = g.get_mutator("vote").unwrap().into_exclusive();

    let ids = 1000;
    let votes = 7;

    // continuously write to vote
    let jh = thread::spawn(move || {
        let user: DataType = 0.into();
        // we need to use a batch putter because otherwise we'd wait for 7000 batch intervals
        add.batch_put((0..votes).flat_map(|_| (0..ids).map(|i| vec![user.clone(), i.into()])))
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

    let mut vc2_state = g.get_getter("votecount2").unwrap();

    // TODO: check that the writer did indeed complete writes during the migration

    // wait for writer to finish
    jh.join().unwrap();

    // allow the system to catch up with the last writes
    sleep();

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true),
            Ok(vec![vec![i.into(), votes.into()]])
        );
        assert_eq!(
            vc2_state.lookup(&[i.into()], true),
            Ok(vec![vec![i.into(), votes.into()]])
        );
    }
}

#[test]
fn state_replay_migration_query() {
    // similar to test above, except we will have a materialized Reader node that we're going to
    // read from rather than relying on forwarding. to further stress the graph, *both* base nodes
    // are created and populated before the migration, meaning we have to replay through a join.

    let mut g = ControllerBuilder::default().build_local();
    let (a, b) = g.migrate(|mig| {
        let a = mig.add_ingredient("a", &["x", "y"], Base::default());
        let b = mig.add_ingredient("b", &["x", "z"], Base::default());

        (a, b)
    });
    let mut muta = g.get_mutator("a").unwrap();
    let mut mutb = g.get_mutator("b").unwrap();

    // make a couple of records
    muta.put(vec![1.into(), "a".into()]).unwrap();
    muta.put(vec![1.into(), "b".into()]).unwrap();
    muta.put(vec![2.into(), "c".into()]).unwrap();
    mutb.put(vec![1.into(), "n".into()]).unwrap();
    mutb.put(vec![2.into(), "o".into()]).unwrap();

    let _ = g.migrate(move |mig| {
        // add join and a reader node
        let j = Join::new(a, b, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let j = mig.add_ingredient("j", &["x", "y", "z"], j);

        // we want to observe what comes out of the join
        mig.maintain_anonymous(j, &[0]);
        j
    });
    let mut out = g.get_getter("j").unwrap();
    sleep();

    // if all went according to plan, the join should now be fully populated!
    // there are (/should be) two records in a with x == 1
    // they may appear in any order
    let res = out.lookup(&[1.into()], true).unwrap();
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
        out.lookup(&[2.into()], true),
        Ok(vec![vec![2.into(), "c".into(), "o".into()]])
    );

    // there are (/should be) no records with x == 3
    assert!(out.lookup(&[3.into()], true).unwrap().is_empty());
}

#[test]
fn recipe_activates() {
    let mut g = ControllerBuilder::default().build_local();
    g.migrate(|mig| {
        let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
        let mut r = Recipe::from_str(r_txt, None).unwrap();
        assert_eq!(r.version(), 0);
        assert_eq!(r.expressions().len(), 1);
        assert_eq!(r.prior(), None);
        assert!(r.activate(mig, false).is_ok());
    });
    // one base node
    assert_eq!(g.inputs().len(), 1);
}

#[test]
fn recipe_activates_and_migrates() {
    let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
    let r1_txt = "QUERY qa: SELECT a FROM b;\n
                  QUERY qb: SELECT a, c FROM b WHERE a = 42;";

    let mut g = ControllerBuilder::default().build_local();
    g.install_recipe(r_txt.to_owned()).unwrap();
    // one base node
    assert_eq!(g.inputs().len(), 1);

    g.extend_recipe(r1_txt.to_owned()).unwrap();
    // still one base node
    assert_eq!(g.inputs().len(), 1);
    // two leaf nodes
    assert_eq!(g.outputs().len(), 2);
}

#[test]
fn recipe_activates_and_migrates_with_join() {
    let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                 CREATE TABLE b (r int, s int);\n";
    let r1_txt = "QUERY q: SELECT y, s FROM a, b WHERE a.x = b.r;";

    let mut g = ControllerBuilder::default().build_local();
    g.install_recipe(r_txt.to_owned()).unwrap();

    // two base nodes
    assert_eq!(g.inputs().len(), 2);

    g.extend_recipe(r1_txt.to_owned()).unwrap();

    // still two base nodes
    assert_eq!(g.inputs().len(), 2);
    // one leaf node
    assert_eq!(g.outputs().len(), 1);
}

#[test]
fn finkelstein1982_queries() {
    use std::fs::File;
    use std::io::Read;

    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    g.migrate(|mig| {
        let mut inc = SqlIncorporator::default();
        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s.lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| {
                if !(l.ends_with("\n") || l.ends_with(";")) {
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
    use std::fs::File;
    use std::io::Read;

    // set up graph
    let mut g = ControllerBuilder::default().build_local();
    g.migrate(|mig| {
        let mut r = Recipe::blank(None);
        let mut f = File::open("tests/tpc-w-queries.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s.lines()
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
        for (_i, q) in lines.iter().enumerate() {
            //println!("{}: {}", i, q);
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
}
