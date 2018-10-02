extern crate noria;
#[macro_use]
extern crate slog;
extern crate slog_term;

use noria::{ControllerHandle, ZookeeperAuthority};

use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql1 = "Article: CREATE TABLE Article (aid int, title varchar(255), \
                url text, PRIMARY KEY(aid));";
    let sql2 = "Vote: CREATE TABLE Vote (aid int, uid int);";
    let sql3 = "VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                FROM Vote GROUP BY Vote.aid;";
    let sql4 = "QUERY ArticleWithVoteCount: \
                SELECT Article.aid, title, url, VoteCount.votes AS votes \
                FROM Article, VoteCount \
                WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    // set up Noria via recipe
    let log = Logger::root(Mutex::new(term_full()).fuse(), o!());
    let mut auth = ZookeeperAuthority::new("127.0.0.1:2181/basicdist").unwrap();
    auth.log_with(log.clone());

    let mut blender = ControllerHandle::new(auth).unwrap();
    blender.extend_recipe(sql1).unwrap();
    blender.extend_recipe(sql2).unwrap();
    blender.extend_recipe(sql3).unwrap();
    blender.extend_recipe(sql4).unwrap();
    println!("{}", blender.graphviz().unwrap());

    let get_view = |b: &mut ControllerHandle<ZookeeperAuthority>, n| loop {
        match b.view(n) {
            Ok(v) => return v,
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                let mut auth = ZookeeperAuthority::new("127.0.0.1:2181/basicdist").unwrap();
                auth.log_with(log.clone());
                *b = ControllerHandle::new(auth).unwrap();
            }
        }
    };

    let get_table = |b: &mut ControllerHandle<ZookeeperAuthority>, n| loop {
        match b.table(n) {
            Ok(v) => return v,
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                let mut auth = ZookeeperAuthority::new("127.0.0.1:2181/basicdist").unwrap();
                auth.log_with(log.clone());
                *b = ControllerHandle::new(auth).unwrap();
            }
        }
    };

    // Get mutators and getter.
    let mut vote = get_table(&mut blender, "Vote");
    let mut article = get_table(&mut blender, "Article");
    let mut awvc = get_view(&mut blender, "ArticleWithVoteCount");

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
            vote = get_table(&mut blender, "Vote");
        }

        times.push(start.elapsed().as_secs());
        // thread::sleep(Duration::from_millis(1000));

        // while let Err(_) = awvc.lookup(&[1.into()], false) {
        //     awvc = get_view(&mut blender, "ArticleWithVoteCount");
        // }
        // println!(" Done");
    }
}
