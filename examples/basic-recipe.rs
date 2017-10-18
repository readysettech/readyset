extern crate distributary;
extern crate time;

mod backend;

use distributary::ControllerBuilder;

use std::thread;
use std::time::Duration;

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               ArticleWithVoteCount: SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(1),
    );

    // set up Soup via recipe
    let blender = ControllerBuilder::default().build();
    blender.set_persistence(persistence_params);
    blender.install_recipe(sql.to_owned());

    // Get mutators and getter.
    let inputs = blender.inputs();
    let outputs = blender.outputs();
    let mut article = blender.get_mutator(inputs["Article"]).unwrap();
    let mut vote = blender.get_mutator(inputs["Vote"]).unwrap();
    let mut awvc = blender.get_getter(outputs["ArticleWithVoteCount"]).unwrap();

    println!("Creating article...");
    let aid = 1;
    // Make sure the article exists:
    if article.lookup(&aid.into(), true).is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .put(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    // Then create a new vote:
    println!("Casting vote...");
    let uid = time::get_time().sec;
    vote.put(vec![aid.into(), uid.into()]).unwrap();
    article
        .put(vec![
            1.into(),
            "test title".into(),
            "http://csail.mit.edu".into(),
        ])
        .unwrap();

    vote.put(vec![1.into(), 42.into()]).unwrap();
    vote.put(vec![1.into(), 43.into()]).unwrap();
    vote.put(vec![1.into(), 44.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", awvc.lookup(&1.into(), true))
}
