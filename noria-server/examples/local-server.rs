use noria_server::Builder;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    let persistence_params = noria_server::PersistenceParameters::new(
        noria_server::DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    // set up Soup via recipe
    let mut builder = Builder::default();

    builder.log_with(noria_server::logger_pls());
    builder.set_persistence(persistence_params);

    let (mut blender, done) = builder.start_local().await.unwrap();
    blender.install_recipe(sql).await.unwrap();
    println!("{}", blender.graphviz().await.unwrap());

    // Get mutators and getter.
    let mut article = blender.table("Article").await.unwrap();
    let mut vote = blender.table("Vote").await.unwrap();
    let mut awvc = blender.view("ArticleWithVoteCount").await.unwrap();

    println!("Creating article...");
    let aid = 1;
    // Make sure the article exists:
    if awvc.lookup(&[aid.into()], true).await.unwrap().is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .await
            .unwrap();
    }

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
    tokio::time::delay_for(Duration::from_secs(1)).await;

    println!("Reading...");
    println!("{:#?}", awvc.lookup(&[aid.into()], true).await);

    done.await;
}
