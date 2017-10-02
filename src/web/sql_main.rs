#[cfg(feature = "web")]
extern crate distributary;

use std::sync::{Arc, Mutex};

#[cfg(feature = "web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::Blender::new();
    g.log_with(distributary::logger_pls());

    // let's add SQL queries, rather than building the graph by hand
    let sql = "
        # Base tables:
        CREATE TABLE article (id int, user int, title varchar(255), url text);
        CREATE TABLE vote (user int, id int);
        CREATE TABLE user (id int, username varchar(40), hash varchar(64));

        # Queries:
        vc: SELECT vote.id, COUNT(vote.user) AS votes FROM vote GROUP BY vote.id;
        awvc: SELECT article.id, article.user, title, url, vc.votes \
              FROM article, vc WHERE article.id = vc.id;
        karma: SELECT awvc.user, SUM(awvc.votes) AS votes \
               FROM awvc GROUP BY awvc.user;
    ";

    g.migrate(|mig| {
        // migrate from the empty recipe to below
        let mut recipe = Recipe::from_str(&sql, None).unwrap();
        recipe.activate(mig, false).unwrap();
    });

    println!("{}", g);

    // run the application
    web::run(Arc::new(Mutex::new(g))).unwrap();
}

#[cfg(not(feature = "web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
