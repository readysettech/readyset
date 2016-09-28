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
    let article = g.incorporate(new("article", &["id", "user", "title", "url"], true, Base {}),
                                vec![]);

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}), vec![]);

    g.incorporate(new("user", &["id", "username", "hash"], true, Base {}),
                             vec![]);

    let q = Query::new(&[true, true], Vec::new());
    // add vote count
    let vc = g.incorporate(new("votecount",
                               &["id", "votes"],
                               true,
                               Aggregation::COUNT.new(vote, 0, 2)),
                           vec![(q, vote)]);

    // add final join
    let mut join = HashMap::new();
    // if article joins against vote count, query and join using article's first field
    join.insert(article, vec![(article, vec![0]), (vc, vec![0])]);
    // if vote count joins against article, also query and join on the first field
    join.insert(vc, vec![(vc, vec![0]), (article, vec![0])]);
    // emit first, second, and third field from article (id + user + title + url)
    // and second field from right (votes)
    let emit = vec![(article, 0), (article, 1), (article, 2), (article, 3), (vc, 1)];
    let j = Joiner::new(emit, join);
    // query to article/vc should select all fields, and query on id
    let q_a = Query::new(&[true, true, true, true],
                         vec![shortcut::Condition {
                                column: 0,
                                cmp:
                                    shortcut::Comparison::Equal(
                                        shortcut::Value::Const(
                                            distributary::DataType::None
                                            )
                                        ),
                            }]);
    let q_vc = Query::new(&[true, true],
                          vec![shortcut::Condition {
                                column: 0,
                                cmp:
                                    shortcut::Comparison::Equal(
                                        shortcut::Value::Const(
                                            distributary::DataType::None
                                            )
                                        ),
                            }]);
    let awvc = g.incorporate(new("awvc", &["id", "user", "title", "url", "votes"], true, j),
                             vec![(q_a, article), (q_vc, vc)]);

    let q = Query::new(&[false, true, false, false, true], Vec::new());
    g.incorporate(new("karma",
                      &["user", "votes"],
                      true,
                      Aggregation::SUM.new(awvc, 1, 2)),
                  vec![(q, awvc)]);
    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
