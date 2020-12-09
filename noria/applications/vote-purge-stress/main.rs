#![allow(clippy::many_single_char_names)]

use clap::{value_t_or_exit, App, Arg};
use hdrhistogram::Histogram;
use noria::{Builder, DurabilityMode, FrontierStrategy, PersistenceParameters};
use std::time::{Duration, Instant};

const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (article_id int, user int);

# read queries
CREATE VIEW VoteCount AS \
  SELECT Vote.article_id, COUNT(user) AS votes FROM Vote GROUP BY Vote.article_id;

QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN VoteCount \
            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;";

#[tokio::main]
async fn main() {
    let args = App::new("purge-stress")
        .about("Benchmarks the latency of full replays in a user-curated news aggregator")
        .arg(
            Arg::with_name("flush-timeout")
                .long("flush-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to wait before processing a merged packet, in nanoseconds."),
        )
        .arg(
            Arg::with_name("replay-timeout")
                .long("replay-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to batch replay requests for, in nanoseconds."),
        )
        .arg(
            Arg::with_name("time")
                .short("t")
                .takes_value(true)
                .default_value("10")
                .help("Time to run benchmark for, in seconds."),
        )
        .arg(
            Arg::with_name("purge")
                .long("purge")
                .takes_value(true)
                .possible_values(&["none", "reader", "all"])
                .default_value("none")
                .help("Disable purging"),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let runtime = value_t_or_exit!(args, "time", u64);
    let mut builder = Builder::default();
    if args.is_present("verbose") {
        builder.log_with(noria::logger_pls());
    }

    builder.set_persistence(PersistenceParameters {
        mode: DurabilityMode::MemoryOnly,
        flush_timeout: Duration::new(0, value_t_or_exit!(args, "flush-timeout", u32)),
        ..Default::default()
    });
    builder.set_sharding(None);
    builder.set_partial_replay_batch_timeout(Duration::new(
        0,
        value_t_or_exit!(args, "replay-timeout", u32),
    ));
    match args.value_of("purge").unwrap() {
        "all" => {
            builder.set_frontier_strategy(FrontierStrategy::AllPartial);
        }
        "reader" => {
            builder.set_frontier_strategy(FrontierStrategy::Readers);
        }
        "none" => {}
        _ => unreachable!(),
    }

    let (mut g, done) = builder.start_local().await.unwrap();
    {
        g.ready().await.unwrap();
        g.install_recipe(RECIPE).await.unwrap();

        let mut a = g.table("Article").await.unwrap();
        let mut v = g.table("Vote").await.unwrap();
        let mut r = g.view("ArticleWithVoteCount").await.unwrap();

        // seed articles
        a.insert(vec![1.into(), "Hello world #1".into()])
            .await
            .unwrap();
        a.insert(vec![2.into(), "Hello world #2".into()])
            .await
            .unwrap();

        // seed votes
        v.insert(vec![1.into(), "a".into()]).await.unwrap();
        v.insert(vec![2.into(), "a".into()]).await.unwrap();
        v.insert(vec![1.into(), "b".into()]).await.unwrap();
        v.insert(vec![2.into(), "c".into()]).await.unwrap();
        v.insert(vec![2.into(), "d".into()]).await.unwrap();

        // now for the benchmark itself.
        // we want to alternately read article 1 and 2, knowing that reading one will purge the other.
        // we first "warm up" by reading both to ensure all other necessary state is present.
        let one = 1.into();
        let two = 2.into();
        assert_eq!(
            r.lookup(&[one], true).await.unwrap(),
            vec![vec![1.into(), "Hello world #1".into(), 2.into()]]
        );
        assert_eq!(
            r.lookup(&[two], true).await.unwrap(),
            vec![vec![2.into(), "Hello world #2".into(), 3.into()]]
        );

        // now time to alternate and measure
        let mut n = 0;
        let start = Instant::now();
        let mut stats = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        while start.elapsed() < Duration::from_secs(runtime) {
            for &id in &[1, 2] {
                let start = Instant::now();
                r.lookup(&[id.into()], true).await.unwrap();
                stats.saturating_record(start.elapsed().as_micros() as u64);
                n += 1;
                // ensure that entry gets evicted
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        println!("# purge mode: {}", args.value_of("purge").unwrap());
        println!(
            "# replays/s: {:.2}",
            f64::from(n) / start.elapsed().as_secs_f64()
        );
        println!("# op\tpct\ttime");
        println!("replay\t50\t{:.2}\tµs", stats.value_at_quantile(0.5));
        println!("replay\t95\t{:.2}\tµs", stats.value_at_quantile(0.95));
        println!("replay\t99\t{:.2}\tµs", stats.value_at_quantile(0.99));
        println!("replay\t100\t{:.2}\tµs", stats.max());
    }
    drop(g);
    done.await;
}
