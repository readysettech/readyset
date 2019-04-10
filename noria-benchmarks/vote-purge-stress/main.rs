#![feature(duration_float)]

extern crate clap;
extern crate futures;
extern crate hdrhistogram;
extern crate noria;
extern crate rand;

use clap::{App, Arg};
use futures::Future;
use hdrhistogram::Histogram;
use noria::{Builder, DurabilityMode, PersistenceParameters, SyncHandle};
use std::time::{Duration, Instant};

const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (article_id int, user int);

# read queries
QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                       FROM Vote GROUP BY Vote.article_id) AS VoteCount \
            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;";

fn main() {
    let verbose = false;

    let mut builder = Builder::default();
    if verbose {
        builder.log_with(noria::logger_pls());
    }

    builder.set_persistence(PersistenceParameters {
        mode: DurabilityMode::MemoryOnly,
        ..Default::default()
    });
    builder.set_sharding(None);

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let ex = rt.executor();
    let mut g = rt
        .block_on(
            builder
                .start_local()
                .map(move |wh| SyncHandle::from_executor(ex, wh))
                .and_then(|mut graph| graph.handle().install_recipe(RECIPE).map(move |_| graph)),
        )
        .unwrap();

    let mut a = g.table("Article").unwrap().into_sync();
    let mut v = g.table("Vote").unwrap().into_sync();
    let mut r = g.view("ArticleWithVoteCount").unwrap().into_sync();

    // seed articles
    a.insert(vec![1.into(), "Hello world #1".into()]).unwrap();
    a.insert(vec![2.into(), "Hello world #2".into()]).unwrap();

    // seed votes
    v.insert(vec![1.into(), "a".into()]).unwrap();
    v.insert(vec![2.into(), "a".into()]).unwrap();
    v.insert(vec![1.into(), "b".into()]).unwrap();
    v.insert(vec![2.into(), "c".into()]).unwrap();
    v.insert(vec![2.into(), "d".into()]).unwrap();

    // now for the benchmark itself.
    // we want to alternately read article 1 and 2, knowing that reading one will purge the other.
    // we first "warm up" by reading both to ensure all other necessary state is present.
    let one = 1.into();
    let two = 2.into();
    assert_eq!(
        r.lookup(&[one], true).unwrap(),
        vec![vec![1.into(), "Hello world #1".into(), 2.into()]]
    );
    assert_eq!(
        r.lookup(&[two], true).unwrap(),
        vec![vec![2.into(), "Hello world #2".into(), 3.into()]]
    );

    // now time to alternate and measure
    let mut n = 0;
    let start = Instant::now();
    let mut stats = Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap();
    while start.elapsed() < Duration::from_secs(10) {
        for _ in 0..1_000 {
            for &id in &[1, 2] {
                let start = Instant::now();
                r.lookup(&[id.into()], true).unwrap();
                stats.saturating_record(start.elapsed().as_micros() as u64);
                n += 1;
            }
        }
    }

    println!(
        "# replays/s: {:.2}",
        n as f64 / start.elapsed().as_float_secs()
    );
    println!("# op\tpct\ttime");
    println!("replay\t50\t{:.2}\tµs", stats.value_at_quantile(0.5));
    println!("replay\t95\t{:.2}\tµs", stats.value_at_quantile(0.95));
    println!("replay\t99\t{:.2}\tµs", stats.value_at_quantile(0.99));
    println!("replay\t100\t{:.2}\tµs", stats.max());
}
