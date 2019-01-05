extern crate noria;

use noria::ControllerHandle;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(DISTINCT uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/basicdist").unwrap();

    srv.install_recipe(sql).unwrap();
    println!("{}", srv.graphviz().unwrap());

    // Get mutators and getter.
    let mut article = srv.table("Article").unwrap();
    let mut vote = srv.table("Vote").unwrap();
    let mut awvc = srv.view("ArticleWithVoteCount").unwrap();

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

    // Then create a new vote:
    println!("Casting vote...");
    let uid = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Double-voting has no effect on final count due to DISTINCT
    vote.insert(vec![aid.into(), uid.into()]).unwrap();
    vote.insert(vec![aid.into(), uid.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", awvc.lookup(&[aid.into()], true))
}
