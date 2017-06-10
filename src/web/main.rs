#[cfg(feature = "web")]
extern crate distributary;

use std::sync::{Arc, Mutex};

#[cfg(feature = "web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::Blender::new();
    g.log_with(distributary::logger_pls());

    {
        let mut mig = g.start_migration();

        // add article base node
        let article =
            mig.add_ingredient("article", &["id", "user", "title", "url"], Base::default());

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add a user account base table
        let _user = mig.add_ingredient("user", &["id", "username", "hash"], Base::default());

        // add vote count
        let vc = mig.add_ingredient("votecount",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join -- joins on first field of each input
        use distributary::JoinSource::*;
        let j = Join::new(article,
                          vc,
                          JoinType::Inner,
                          vec![B(0, 0), L(1), L(2), L(3), R(1)]);
        let awvc = mig.add_ingredient("awvc", &["id", "user", "title", "url", "votes"], j);

        let karma = mig.add_ingredient("karma",
                                       &["user", "votes"],
                                       Aggregation::SUM.over(awvc, 4, &[1]));

        mig.maintain(awvc, 0);
        mig.maintain(karma, 0);

        // commit migration
        mig.commit();
    }

    web::run(Arc::new(Mutex::new(g))).unwrap();
    loop {}
}

#[cfg(not(feature = "web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
