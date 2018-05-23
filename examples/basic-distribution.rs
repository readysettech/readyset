extern crate distributary;

use distributary::{ControllerHandle, ZookeeperAuthority};

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql1 = "CREATE TABLE Article (aid int, title varchar(255), \
                url text, PRIMARY KEY(aid));";
    let sql2 = "CREATE TABLE Vote (aid int, uid int);";
    let sql3 = "VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                FROM Vote GROUP BY Vote.aid;";
    let sql4 = "QUERY ArticleWithVoteCount: \
                SELECT Article.aid, title, url, VoteCount.votes AS votes \
                FROM Article, VoteCount \
                WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    let log = distributary::logger_pls();

    // set up Soup via recipe
    let mut auth = ZookeeperAuthority::new("127.0.0.1:2181/basicdist");
    auth.log_with(log.clone());

    let mut blender = ControllerHandle::new(auth);
    blender.install_recipe(sql1.to_owned()).unwrap();
    blender.extend_recipe(sql2.to_owned()).unwrap();
    blender.extend_recipe(sql3.to_owned()).unwrap();
    blender.extend_recipe(sql4.to_owned()).unwrap();
    println!("{}", blender.graphviz());

    // Get mutators and getter.
    let mut article = blender.get_mutator("Article").unwrap();

    println!("Creating article...");
    let aid = 1;
    // Make sure the article exists:
    let mut awvc = blender.get_getter("ArticleWithVoteCount").unwrap();
    if awvc.lookup(&[aid.into()], true).unwrap().is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .put(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    loop {
        let mut vote = blender.get_mutator("Vote").unwrap();
        loop {
            // Then create a new vote:
            print!("Casting vote...");
            let uid = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            if let Err(_) = vote.put(vec![aid.into(), uid.into()]) {
                println!(" Failed!");
                break;
            }

            thread::sleep(Duration::from_millis(1000));

            if let Err(_) = awvc.lookup(&[1.into()], false) {
                break;
            }
            println!(" Done");
        }
        awvc = blender.get_getter("ArticleWithVoteCount").unwrap();
    }
}
