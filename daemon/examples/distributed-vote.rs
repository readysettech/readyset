extern crate distributary;
extern crate gulaschkanone;
#[macro_use]
extern crate slog;
extern crate slog_term;

use std::thread;
use std::time::Duration;

use gulaschkanone::{Config, Controller};

fn main() {
    use distributary::*;

    let config = Config {
        hostname: String::from("localhost"),
        addr: String::from("127.0.0.1"),
        port: 9999,
        controller: None,        // we are the controller
        heartbeat_freq: 1000,    // 1s
        healthcheck_freq: 10000, // 10s
    };

    let log = distributary::logger_pls();

    let mut controller = Controller::new(
        &config.addr,
        config.port,
        Duration::from_millis(config.heartbeat_freq),
        Duration::from_millis(config.healthcheck_freq),
        log.clone(),
    );

    let blender = controller.get_blender();

    // run controller in the background
    let jh = thread::spawn(move || { controller.listen(); });

    // wait for a worker to connect
    info!(log, "waiting 10s for a worker to connect...");
    thread::sleep(Duration::from_millis(10000));

    info!(log, "setting up graph");

    // set up graph
    {
        let mut g = blender.lock().unwrap();

        g.log_with(log);

        let mut mig = g.start_migration();

        // add article base node
        let article =
            mig.add_ingredient("article", &["id", "user", "title", "url"], Base::default());

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add a user account base table
        let _user = mig.add_ingredient("user", &["id", "username", "hash"], Base::default());

        // add vote count
        let vc = mig.add_ingredient(
            "votecount",
            &["id", "votes"],
            Aggregation::COUNT.over(vote, 0, &[1]),
        );

        // add final join -- joins on first field of each input
        use distributary::JoinSource::*;
        let j = Join::new(
            article,
            vc,
            JoinType::Inner,
            vec![B(0, 0), L(1), L(2), L(3), R(1)],
        );
        let awvc = mig.add_ingredient("awvc", &["id", "user", "title", "url", "votes"], j);

        let karma = mig.add_ingredient(
            "karma",
            &["user", "votes"],
            Aggregation::SUM.over(awvc, 4, &[1]),
        );

        mig.maintain(awvc, 0);
        mig.maintain(karma, 0);

        // commit migration
        mig.commit();
    }

    jh.join().unwrap();
}
