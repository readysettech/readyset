#[cfg(feature="web")]
extern crate distributary;

#[cfg(feature="web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::Blender::new();

    {
        let mut mig = g.start_migration();

        // add article base node
        let article = mig.add_ingredient("article", &["id", "user", "title", "url"], Base {});

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base {});

        // add a user account base table
        mig.add_ingredient("user", &["id", "username", "hash"], Base {});

        // add vote count
        let vc = mig.add_ingredient("votecount",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join -- joins on first field of each input
        let j =
            JoinBuilder::new(vec![(article, 0), (article, 1), (article, 2), (article, 3), (vc, 1)])
                .from(article, vec![1, 0])
                .join(vc, vec![1, 0]);
        let awvc = mig.add_ingredient("awvc", &["id", "user", "title", "url", "votes"], j);

        let karma = mig.add_ingredient("karma",
                                       &["user", "votes"],
                                       Aggregation::SUM.over(awvc, 4, &[1]));

        mig.maintain(awvc, 0);
        mig.maintain(karma, 0);
    }

    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
