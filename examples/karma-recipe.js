extern crate distributary;

use distributary::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, uid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               Create Table User (uid int, username text);


               # read queries
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;

               QUERY Karma: \
                            SELECT User.uid, username, SUM(VoteCount.votes) AS karma \
                            FROM User
                            JOIN Article ON User.uid=Article.uid
                            JOIN VoteCount ON VoteCount.aid=Article.aid
                            WHERE User.uid = ?
                            GROUP BY User.uid;";
    let user_view_extension = "

                QUERY UserView: \
                        SELECT uid, username \
                        FROM User
                        WHERE User.uid = ?;";

    let sql_extended = "
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

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
    builder.disable_partial();
    builder.set_persistence(persistence_params);

    // Get a local copy of the controller
    let mut blender = builder.build_local();
    // Convert ths string literal above into a String object
    blender.install_recipe(sql.to_owned()).unwrap();
    blender.extend_recipe(user_view_extension.to_owned()).unwrap();
    blender.extend_recipe(sql_extended.to_owned()).unwrap();
     println!("{}", blender.graphviz());

    // Get mutators and getter.
    let mut article = blender.get_mutator("Article").unwrap();
    // Find the code for gettes and mutators
    // let mut article_get = blender.get_getter("Articles").unwrap();
    let mut vote = blender.get_mutator("Vote").unwrap();
    let mut user = blender.get_mutator("User").unwrap();
    // let mut user_get = blender.get_getter("User").unwrap();
    let mut karma = blender.get_getter("Karma").unwrap();
    let mut awvc = blender.get_getter("ArticleWithVoteCount").unwrap();
    let mut user_get = blender.get_getter("UserView").unwrap();

    println!("Creating article...");
    let aid_first = 1;
    let aid_second = 4;
    // Make sure the article exists:
    let uid_jsegaran = 2;
    let uid_other = 3;

    // Check if a user exists, if not create it
    println!("Creating new user");
    let jsegaran = "jsegaran";
    let other = "other";
    user.put(vec![uid_jsegaran.into(), jsegaran.into()]).unwrap();
    user.put(vec![uid_other.into(), other.into()]).unwrap();

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
    vote.put(vec![aid_first.into(), uid_other.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_jsegaran.into()]).unwrap();
    vote.put(vec![aid_second.into(), uid_other.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", karma.lookup(&uid_jsegaran.into(), true));
    println!("{:#?}", user_get.lookup(&uid_jsegaran.into(), true));
    println!("{:#?}", awvc.lookup(&1.into(), true))
}
