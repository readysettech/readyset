extern crate distributary;

use distributary::ControllerBuilder;

use std::thread;
use std::time::{Duration};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, uid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               Create Table User (uid int, username text, test text);


               # read queries
               VoteCount: SELECT Vote.aid, COUNT(DISTINCT Vote.uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;

               QUERY Karma: \
                            SELECT User.uid, username, SUM(VoteCount.votes) AS karma \
                            FROM User
                            JOIN Article ON User.uid=Article.uid
                            JOIN VoteCount ON VoteCount.aid=Article.aid
                            WHERE User.uid = ?
                            GROUP BY User.uid;";
    let user_view_extension = "
                QUERY UserViewDist: \
                        SELECT DISTINCT User.username \
                        FROM User WHERE User.uid = ?;";

    let sql_extended = "
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    //let article_distinct = "
    //           QUERY DistinctArticles: \
    //                        SELECT DISTINCT title\
    //                        FROM Article
    //                        WHERE Article.aid = ?;";
    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(1),
        Some(String::from("example")),
    );

    // set up Soup via recipe default
    let mut builder = ControllerBuilder::default();
    //builder.log_with(distributary::logger_pls());
    builder.set_worker_threads(2);
    builder.set_persistence(persistence_params);

    // Get a local copy of the controller
    let mut blender = builder.build_local();
    // Convert ths string literal above into a String object
    blender.install_recipe(sql.to_owned()).unwrap();
    blender.extend_recipe(user_view_extension.to_owned()).unwrap();
    blender.extend_recipe(sql_extended.to_owned()).unwrap();
    //blender.extend_recipe(article_distinct.to_owned()).unwrap();
     println!("{}", blender.graphviz());

    // Get mutators and getter.
    let mut article = blender.get_mutator("Article").unwrap();
    // Find the code for gettes and mutators
    // let mut article_get = blender.get_getter("Articles").unwrap();
    let mut vote = blender.get_mutator("Vote").unwrap();
    let mut user = blender.get_mutator("User").unwrap();
    // let mut user_get = blender.get_getter("User").unwrap();
    //let mut karma = blender.get_getter("Karma").unwrap();
    let mut awvc = blender.get_getter("ArticleWithVoteCount").unwrap();
    //let mut adist = blender.get_getter("DistinctArticles").unwrap();
    // let mut user_get = blender.get_getter("UserView").unwrap();
    // let mut user_get_dist = blender.get_getter("UserViewDist").unwrap();

    println!("Creating article...");
    let aid_first = 1;
    let uid_jsegaran = 2;
    let uid_other = 3;
    let aid_second = 4;

    // Check if a user exists, if not create it
    println!("Creating new user");
    let jsegaran = "jsegaran";
    let other = "other";

    user.put(vec![uid_jsegaran.into(), jsegaran.into(), "a".into()]).unwrap();
    user.put(vec![uid_other.into(), other.into(), "d".into()]).unwrap();

    // Check if the article exists. If not create it
    println!("Creating new article...");
    let title = "test title";
    let url = "http://pdos.csail.mit.edu";
    article
        .put(vec![aid_second.into(), uid_jsegaran.into(), title.into(), url.into()])
        .unwrap();

    article
        .put(vec![aid_first.into(), uid_jsegaran.into(), title.into(), url.into()])
        .unwrap();

    // Then create a new vote:
    println!("Casting vote...");
    vote.put(vec![aid_first.into(), uid_jsegaran.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_jsegaran.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_jsegaran.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_other.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_other.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(3000));

    println!("Reading...");
    // println!("{:#?}", user_get_dist.lookup(&2.into(), true));
    // println!("{:#?}", user_get_dist.lookup(&3.into(), true));
    println!("{:#?}", awvc.lookup(&aid_first.into(), true));
    println!("{:#?}", awvc.lookup(&aid_second.into(), true));
}
