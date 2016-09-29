#[cfg(feature="web")]
extern crate distributary;
#[cfg(feature="web")]
extern crate shortcut;

#[cfg(feature="web")]
fn main() {
    use distributary::*;
    use std::collections::HashMap;

    // set up graph
    let mut g = distributary::FlowGraph::new();

    // add article base node
    let article = g.incorporate(new("article", &["id", "user", "title", "url"], true, Base {}));

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));

    g.incorporate(new("user", &["id", "username", "hash"], true, Base {}));

    // add vote count
    let vc = g.incorporate(new("votecount",
                               &["id", "votes"],
                               true,
                               Aggregation::COUNT.new(vote, 0)));

    // add final join -- joins on first field of each input
    let mut join = HashMap::new();
    join.insert(article, vec![1, 0]);
    join.insert(vc, vec![1, 0]);
    // emit first, second, and third field from article (id + user + title + url)
    // and second field from right (votes)
    let emit = vec![(article, 0), (article, 1), (article, 2), (article, 3), (vc, 1)];
    let awvc = g.incorporate(new("awvc",
                                 &["id", "user", "title", "url", "votes"],
                                 true,
                                 Joiner::new(emit, join)));

    g.incorporate(new("karma",
                      &["user", "votes"],
                      true,
                      Aggregation::SUM.new(awvc, 1)));
    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
