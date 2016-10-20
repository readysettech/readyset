#[cfg(feature="web")]
extern crate distributary;
#[cfg(feature="web")]
extern crate shortcut;

#[cfg(feature="web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::FlowGraph::new();

    // add article base node
    let article = g.incorporate(new("article", &["id", "user", "title", "url"], true, Base {}));

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));

    // add a user account base table
    g.incorporate(new("user", &["id", "username", "hash"], true, Base {}));

    // add vote count
    let vc = g.incorporate(new("votecount",
                               &["id", "votes"],
                               true,
                               Aggregation::COUNT.new(vote, 0, &[1])));

    // add final join -- joins on first field of each input
    let j = JoinBuilder::new(vec![(article, 0), (article, 1), (article, 2), (article, 3), (vc, 1)])
        .from(article, vec![1, 0])
        .join(vc, vec![1, 0]);
    let awvc = g.incorporate(new("awvc", &["id", "user", "title", "url", "votes"], true, j));

    g.incorporate(new("karma",
                      &["user", "votes"],
                      true,
                      Aggregation::SUM.new(awvc, 4, &[1])));
    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
