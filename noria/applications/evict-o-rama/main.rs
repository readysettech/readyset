use noria::Builder;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static NUM_ARTICLES: usize = 10_000;

#[tokio::main]
async fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int, PRIMARY KEY(aid, uid));

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article LEFT JOIN VoteCount \
                                         ON (Article.aid = VoteCount.aid) \
                            WHERE Article.aid = ?;";

    let persistence_params = noria::PersistenceParameters::new(
        noria::DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(String::from("evictorama")),
        1,
    );

    // set up Soup via recipe
    let mut builder = Builder::default();
    builder.log_with(noria::logger_pls());
    builder.set_persistence(persistence_params);
    builder.set_memory_limit(100 * 1024, Duration::from_millis(1000));

    // TODO: This should be removed when the `it_works_with_reads_before_writes`
    // test passes again.
    //builder.disable_partial();

    let (mut blender, _done) = builder.start_local().await.unwrap();
    blender.install_recipe(sql).await.unwrap();

    // Get mutators and getter.
    let mut article = blender.table("Article").await.unwrap();
    let mut vote = blender.table("Vote").await.unwrap();
    let mut awvc = blender.view("ArticleWithVoteCount").await.unwrap();

    println!("Creating articles...");
    for aid in 1..NUM_ARTICLES {
        // Make sure the article exists:
        let title = format!("Article {}", aid);
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .await
            .unwrap();
        vote.insert(vec![aid.into(), 1.into()]).await.unwrap();
    }

    println!("Reading articles...");
    for aid in 1..NUM_ARTICLES {
        awvc.lookup(&[aid.into()], true).await.unwrap();
    }

    println!("Casting votes...");
    let mut aid = 0;
    loop {
        let uid = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        aid = (aid + 1) % NUM_ARTICLES;
        vote.insert(vec![(aid + 1).into(), uid.into()])
            .await
            .unwrap();

        awvc.lookup(&[aid.into()], true).await.unwrap();
    }
}
