extern crate noria;
extern crate tokio;
#[macro_use]
extern crate failure;

use noria::ControllerHandle;
use tokio::prelude::*;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    let aid = 1;

    // NOTE: this will get *so* much nicer with async/await
    tokio::run(
        ControllerHandle::from_zk("127.0.0.1:2181/basicdist")
            .and_then(move |mut srv| srv.install_recipe(sql).map(move |_| srv))
            .and_then(|mut srv| {
                srv.graphviz().map(move |g| {
                    println!("{}", g);
                    srv
                })
            })
            .and_then(|mut srv| {
                srv.view("ArticleWithVoteCount")
                    .map(move |awvc| (srv, awvc))
            })
            .and_then(move |(mut srv, awvc)| {
                println!("Creating article...");
                awvc.lookup(&[aid.into()], true)
                    .map_err(|e| format_err!("failed to look up article: {:?}", e))
                    .and_then(move |(awvc, article)| {
                        if article.is_empty() {
                            println!("Creating new article...");
                            let title = "test title";
                            let url = "http://pdos.csail.mit.edu";
                            future::Either::A(
                                srv.table("Article")
                                    .and_then(move |articles| {
                                        articles
                                            .insert(vec![aid.into(), title.into(), url.into()])
                                            .map_err(|e| {
                                                format_err!("failed to insert article: {:?}", e)
                                            })
                                    })
                                    .map(move |_| (srv, awvc)),
                            )
                        } else {
                            future::Either::B(future::ok((srv, awvc)))
                        }
                    })
            })
            .and_then(move |(mut srv, awvc)| {
                srv.table("Vote")
                    .and_then(move |vote| {
                        // Then create a new vote:
                        println!("Casting vote...");
                        let uid = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;

                        // Double-voting has no effect on final count due to DISTINCT
                        vote.insert(vec![aid.into(), uid.into()])
                            .and_then(move |vote| vote.insert(vec![aid.into(), uid.into()]))
                            .map_err(|e| format_err!("failed to insert vote: {:?}", e))
                    })
                    .map(move |_| (srv, awvc))
            })
            .and_then(|(srv, awvc)| {
                println!("Finished writing! Let's wait for things to propagate...");
                tokio::timer::Delay::new(Instant::now() + Duration::from_millis(1000))
                    .map(move |_| (srv, awvc))
                    .map_err(|_| unreachable!())
            })
            .and_then(move |(_, awvc)| {
                println!("Reading...");
                awvc.lookup(&[aid.into()], true)
                    .map_err(|e| format_err!("failed to look up article: {:?}", e))
            })
            .map(|article| {
                println!("{:#?}", article);
            })
            .map_err(|e| {
                eprintln!("{:?}", e);
            }),
    );
}
