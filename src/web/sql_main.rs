#[cfg(feature="web")]
extern crate distributary;

use std::sync::{Arc, Mutex};

#[cfg(feature="web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::Blender::new();

    // let's add SQL queries, rather than building the graph by hand
    let mut inc = SqlIncorporator::default();

    {
        // migrate from the empty recipe to below
        let mut mig = g.start_migration();

        // add article base node
        let _article = inc.add_query("CREATE TABLE article \
                                     (id int, user int, title varchar(255), url text);",
                                     None,
                                     &mut mig)
            .unwrap();

        // add vote base table
        let _vote = inc.add_query("CREATE TABLE vote (user int, id int);", None, &mut mig)
            .unwrap();

        // add a user account base table
        let _user = inc.add_query("CREATE TABLE user \
                                  (id int, username varchar(40), hash varchar(64));",
                                  None,
                                  &mut mig)
            .unwrap();

        // add vote count
        inc.add_query("SELECT vote.id, COUNT(vote.user) AS votes FROM vote GROUP BY vote.id;",
                      Some("vc".into()),
                      &mut mig)
            .unwrap();

        println!("done vc");

        // add final join -- joins on first field of each input
        inc.add_query("SELECT article.id, article.user, title, url, vc.votes FROM article, \
                            vc WHERE article.id = vc.id;",
                      Some("awvc".into()),
                      &mut mig)
            .unwrap();

        println!("done awvc");

        // add user karma
        let _karma =
            inc.add_query("SELECT awvc.user, SUM(awvc.votes) AS votes FROM awvc GROUP BY \
                            awvc.user;",
                          Some("karma".into()),
                          &mut mig)
                .unwrap();
    }

    println!("{}", g);

    // run the application
    web::run(Arc::new(Mutex::new(g))).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
