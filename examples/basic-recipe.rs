extern crate distributary;

mod backend;

use std::{thread, time};
use distributary::Blender;
use backend::Backend;

fn load_recipe() -> Backend {
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

    // set up Soup via recipe
    let soup = Blender::new();
    soup.install_recipe(sql.to_owned());

    Backend::new(soup)
}

fn main() {
    let mut backend = load_recipe();

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
