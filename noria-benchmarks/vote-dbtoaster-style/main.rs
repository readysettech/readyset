#![feature(duration_as_u128)]

#[macro_use]
extern crate clap;
extern crate rand;

#[path = "../vote/clients/localsoup/graph.rs"]
mod graph;

use noria::{DataType, DurabilityMode, PersistenceParameters, TableOperation};
use rand::Rng;
use std::{thread, time};

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-dbtoaster-style")
        .about("Benchmarks Soup in a DBToaster-like vote setup")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .default_value("500000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("votes")
                .index(1)
                .value_name("VOTES")
                .default_value("50000000")
                .help("Number of votes to issue"),
        )
        .arg(
            Arg::with_name("batch-size")
                .short("b")
                .long("batch-size")
                .takes_value(true)
                .default_value("2500")
                .help("Size of batches"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("Include logging output"),
        )
        .get_matches();

    let articles = value_t_or_exit!(args, "articles", usize);
    let votes = value_t_or_exit!(args, "votes", usize);
    let batch = value_t_or_exit!(args, "batch-size", usize);

    let mut persistence = PersistenceParameters::default();
    persistence.mode = DurabilityMode::MemoryOnly;
    // force tuple-at-a-time
    persistence.flush_timeout = time::Duration::new(0, 0);
    persistence.log_prefix = "vote-dbtoaster".to_string();

    // setup db
    let mut s = graph::Setup::default();
    s.logging = args.is_present("verbose");
    s.sharding = None;
    s.stupid = false;
    s.partial = false;
    s.threads = Some(1);
    let mut g = s.make(persistence);

    // prepopulate
    if args.is_present("verbose") {
        eprintln!("==> prepopulating with {} articles", articles);
    }
    let mut a = g.graph.table("Article").unwrap();
    a.batch_insert((0..articles).map(|i| {
        vec![
            ((i + 1) as i32).into(),
            format!("Article #{}", i + 1).into(),
        ]
    }))
    .unwrap();
    if args.is_present("verbose") {
        eprintln!("==> done with prepopulation");
    }

    // allow writes to propagate
    thread::sleep(time::Duration::from_secs(1));

    let mut rng = rand::thread_rng();
    let mut v = g.graph.table("Vote").unwrap();
    v.i_promise_dst_is_same_process();

    // start the benchmark
    let start = time::Instant::now();
    let mut n = 0;
    v.batch_insert_then_wait((0..votes).step_by(batch).map(|_| {
        n += batch;
        (0..batch)
            .map(|_| {
                TableOperation::from(vec![
                    DataType::from(rng.gen_range(0, articles) + 1),
                    0.into(),
                ])
            })
            .collect()
    }))
    .unwrap();
    let took = start.elapsed();

    // all done!
    println!("# votes: {}", n);
    println!("# took: {:?}", took);
    println!(
        "# achieved ops/s: {:.2}",
        n as f64 / (took.as_nanos() as f64 / 1_000_000_000.)
    );
}
