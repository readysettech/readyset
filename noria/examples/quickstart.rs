use noria::ControllerHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # internal view, for shorthand below
               VoteCount: SELECT Vote.aid, COUNT(DISTINCT uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               # queryable materialized view
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";
    let aid = 1;

    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/basicdist")
        .await
        .unwrap();
    srv.install_recipe(sql).await.unwrap();
    let g = srv.graphviz().await.unwrap();
    println!("{}", g);

    let mut awvc = srv.view("ArticleWithVoteCount").await.unwrap();
    println!("Creating article...");
    let article = awvc.lookup(&[aid.into()], true).await.unwrap();
    if article.is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        let mut articles = srv.table("Article").await.unwrap();
        articles
            .insert(vec![aid.into(), title.into(), url.into()])
            .await
            .unwrap();
    }

    let mut vote = srv.table("Vote").await.unwrap();
    // Then create a new vote:
    println!("Casting vote...");
    let uid = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Double-voting has no effect on final count due to DISTINCT
    vote.insert(vec![aid.into(), uid.into()]).await.unwrap();
    vote.insert(vec![aid.into(), uid.into()]).await.unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    tokio::timer::delay(Instant::now() + Duration::from_millis(1000)).await;

    println!("Reading...");
    let article = awvc.lookup(&[aid.into()], true).await.unwrap();
    println!("{:#?}", article);
}
