extern crate distributary;
extern crate slog;
extern crate slog_term;

mod backend;

use std::{thread, time};
use distributary::{Blender, Recipe};
use backend::Backend;

use slog::DrainExt;

fn load_recipe() -> Result<Backend, String> {
    // inline recipe definition
    let sql = "# write types (SQL type names are ignored)
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read expressions
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               ArticleWithVoteCount: SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    // set up Soup via recipe
    let mut soup = Blender::new();
    soup.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let recipe;
    {
        let mut mig = soup.start_migration();

        // install recipe
        recipe = match Recipe::from_str(&sql, None) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig, false)?;
                recipe
            }
            Err(e) => return Err(e),
        };

        // brings up new graph for processing
        mig.commit();
    }

    Ok(Backend::new(soup, recipe))
}

fn main() {
    let mut backend = load_recipe().unwrap();

    println!("Soup graph:\n{}", backend.soup);

    println!("Writing...");
    let aid = 1;
    let title = "test title";
    let url = "http://pdos.csail.mit.edu";
    let uid = 42;
    backend
        .put("Article", &[aid.into(), title.into(), url.into()])
        .unwrap();
    backend.put("Vote", &[aid.into(), uid.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", backend.get("ArticleWithVoteCount", aid))
}
