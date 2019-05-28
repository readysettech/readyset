use noria::SyncControllerHandle;
use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql1 = "Article: CREATE TABLE Article (aid int, title varchar(255), \
                url text, PRIMARY KEY(aid));";
    // two internal views: you cannot query these directly from clients, but you can write
    // other view definitions that use them.
    let sql2 = "Vote: CREATE TABLE Vote (aid int, uid int);";
    let sql3 = "VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                FROM Vote GROUP BY Vote.aid;";
    // externally queryable materialized view
    let sql4 = "QUERY ArticleWithVoteCount: \
                SELECT Article.aid, title, url, VoteCount.votes AS votes \
                FROM Article, VoteCount \
                WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    // set up Noria via recipe
    let rt = tokio::runtime::Runtime::new().unwrap();
    let executor = rt.executor();
    let mut db = SyncControllerHandle::from_zk("127.0.0.1:2181/basicdist", executor).unwrap();
    db.extend_recipe(sql1).unwrap();
    db.extend_recipe(sql2).unwrap();
    db.extend_recipe(sql3).unwrap();
    db.extend_recipe(sql4).unwrap();
    println!("{}", db.graphviz().unwrap());

    let executor = rt.executor();
    let get_view = move |b: &mut SyncControllerHandle<_, _>, n| loop {
        match b.view(n) {
            Ok(v) => return v.into_sync(),
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                *b = SyncControllerHandle::from_zk("127.0.0.1:2181/basicdist", executor.clone())
                    .unwrap();
            }
        }
    };

    let executor = rt.executor();
    let get_table = move |b: &mut SyncControllerHandle<_, _>, n| loop {
        match b.table(n) {
            Ok(v) => return v.into_sync(),
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                *b = SyncControllerHandle::from_zk("127.0.0.1:2181/basicdist", executor.clone())
                    .unwrap();
            }
        }
    };

    // Get mutators and getter.
    let mut vote = get_table(&mut db, "Vote");
    let mut article = get_table(&mut db, "Article");
    let mut awvc = get_view(&mut db, "ArticleWithVoteCount");

    println!("Creating article...");
    let aid = 1;
    // Make sure the article exists:
    if awvc.lookup(&[aid.into()], true).unwrap().is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    let start = Instant::now();

    let mut last_print = Instant::now();

    let mut times = Vec::new();

    loop {
        let elapsed = last_print.elapsed();
        if elapsed >= Duration::from_secs(5) {
            last_print += elapsed;

            let mut counts = BTreeMap::new();
            for t in &times {
                *counts.entry(t).or_insert(0) += 1;
            }

            for (k, c) in counts {
                println!("{}: {}", k, c);
            }
            println!("---------")
        }

        let uid = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        while let Err(_) = vote.insert(vec![aid.into(), uid.into()]) {
            vote = get_table(&mut db, "Vote");
        }

        times.push(start.elapsed().as_secs());
        // thread::sleep(Duration::from_millis(1000));

        // while let Err(_) = awvc.lookup(&[1.into()], false) {
        //     awvc = get_view(&mut db, "ArticleWithVoteCount");
        // }
        // println!(" Done");
    }
}
