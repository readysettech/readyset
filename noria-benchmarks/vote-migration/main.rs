const WRITE_BATCH_SIZE: usize = 1000;

#[path = "../vote/clients/localsoup/graph.rs"]
mod graph;

use clap::value_t_or_exit;
use noria::{DataType, TableOperation};
use rand::{distributions::Distribution, Rng};
use std::io::prelude::*;
use std::sync::mpsc;
use std::sync::Arc;
use std::{fs, thread, time};
use tokio::sync::Barrier;

const NANOS_PER_SEC: u64 = 1_000_000_000;

use zipf::ZipfDistribution;

struct Reporter {
    last: time::Instant,
    every: time::Duration,
    count: usize,
}

impl Reporter {
    pub fn report(&mut self, n: usize) -> Option<(time::Duration, usize)> {
        self.count += n;

        let now = time::Instant::now();
        let elapsed = now - self.last;
        if elapsed > self.every {
            let count = Some((elapsed, self.count));
            self.last = now;
            self.count = 0;
            count
        } else {
            None
        }
    }

    pub fn new(every: time::Duration) -> Self {
        Reporter {
            last: time::Instant::now(),
            every,
            count: 0,
        }
    }
}

async fn one(s: &graph::Builder, skewed: bool, args: &clap::ArgMatches<'_>, w: Option<fs::File>) {
    let narticles = value_t_or_exit!(args, "narticles", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = time::Duration::from_secs(value_t_or_exit!(args, "migrate", u64));
    assert!(migrate_after < runtime);

    // reporting config
    let every = time::Duration::from_millis(200);

    // default persistence (memory only)
    let mut persistence_params = noria::PersistenceParameters::default();
    persistence_params.mode = noria::DurabilityMode::MemoryOnly;

    // make the graph!
    eprintln!("Setting up soup");
    let mut g = s.start(persistence_params).await.unwrap();
    eprintln!("Getting accessors");
    let mut articles = g.graph.table("Article").await.unwrap();
    let mut votes = g.graph.table("Vote").await.unwrap();
    let mut read_old = g.graph.view("ArticleWithVoteCount").await.unwrap();

    // prepopulate
    eprintln!("Prepopulating with {} articles", narticles);
    articles
        .perform_all(
            (0..narticles).map(|i| vec![(i as i32).into(), format!("Article #{}", i + 1).into()]),
        )
        .await
        .unwrap();

    let (stat, stat_rx) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(3));
    let done = Arc::new(Barrier::new(5));

    // start writer that just does a bunch of old writes
    eprintln!("Starting old writer");
    {
        let stat = stat.clone();
        let done = done.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait().await;

            let mut reporter = Reporter::new(every);
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                votes
                    .perform_all((0..WRITE_BATCH_SIZE).map(|i| {
                        // always generate both so that we aren't artifically faster with one
                        let id_uniform = rand::thread_rng().gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rand::thread_rng());
                        let id = if skewed { id_zipf } else { id_uniform };
                        TableOperation::from(vec![DataType::from(id), i.into()])
                    }))
                    .await
                    .unwrap();

                if let Some((took, count)) = reporter.report(WRITE_BATCH_SIZE) {
                    let count_per_ns = count as f64 / took.as_nanos() as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    stat.send(("OLD", count_per_s)).unwrap();
                }
            }

            done.wait().await;
        });
    }

    // start a read that just reads old forever
    eprintln!("Starting old reader");
    {
        let done = done.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait().await;
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                let id_uniform = rand::thread_rng().gen_range(0, narticles);
                let id_zipf = zipf.sample(&mut rand::thread_rng());
                let id = if skewed { id_zipf } else { id_uniform };
                read_old.lookup(&[DataType::from(id)], false).await.unwrap();
                tokio::timer::delay(time::Instant::now() + time::Duration::from_micros(10)).await;
            }

            done.wait().await;
        });
    }

    // wait for other threads to be ready
    barrier.wait().await;
    let start = time::Instant::now();

    let stats = thread::spawn(move || {
        let mut w = w;
        for (stat, val) in stat_rx {
            let line = format!("{} {} {:.2}", start.elapsed().as_nanos(), stat, val);
            println!("{}", line);
            if let Some(ref mut w) = w {
                writeln!(w, "{}", line).unwrap();
            }
        }
    });

    // we now need to wait for migrate_after
    eprintln!("Waiting for migration time...");
    tokio::timer::delay(time::Instant::now() + migrate_after).await;

    // all right, migration time
    eprintln!("Starting migration");
    stat.send(("MIG START", 0.0)).unwrap();
    g.transition().await;
    stat.send(("MIG FINISHED", 0.0)).unwrap();
    let mut ratings = g.graph.table("Rating").await.unwrap();
    let mut read_new = g.graph.view("ArticleWithScore").await.unwrap();

    // start writer that just does a bunch of new writes
    eprintln!("Starting new writer");
    {
        let done = done.clone();
        let stat = stat.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait().await;

            let mut reporter = Reporter::new(every);
            while start.elapsed() < runtime {
                ratings
                    .perform_all((0..WRITE_BATCH_SIZE).map(|i| {
                        let id_uniform = rand::thread_rng().gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rand::thread_rng());
                        let id = if skewed { id_zipf } else { id_uniform };
                        TableOperation::from(vec![DataType::from(id), i.into(), 5.into()])
                    }))
                    .await
                    .unwrap();

                if let Some((took, count)) = reporter.report(WRITE_BATCH_SIZE) {
                    let count_per_ns = count as f64 / took.as_nanos() as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    stat.send(("NEW", count_per_s)).unwrap();
                }
            }

            done.wait().await;
        });
    }

    // start reader that keeps probing new read view
    eprintln!("Starting new read probe");
    {
        let stat = stat.clone();
        let done = done.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let n = 100;
            let mut hits = 0;
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            let mut reporter = Reporter::new(every);
            barrier.wait().await;
            while start.elapsed() < runtime {
                let ids = (0..n)
                    .map(|_| {
                        let id_uniform = rand::thread_rng().gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rand::thread_rng());
                        vec![DataType::from(if skewed { id_zipf } else { id_uniform })]
                    })
                    .collect();
                if let Ok(rss) = read_new.multi_lookup(ids, false).await {
                    hits += rss.into_iter().filter(|rs| !rs.is_empty()).count();
                } else {
                    // miss, or view not yet ready
                }

                if let Some((_, count)) = reporter.report(n) {
                    stat.send(("HITF", hits as f64 / count as f64)).unwrap();
                    hits = 0;
                }
                tokio::timer::delay(time::Instant::now() + time::Duration::from_millis(10)).await;
            }

            done.wait().await;
        });
    }

    // fire them both off!
    barrier.wait().await;

    // everything finishes!
    eprintln!("Waiting for experiment to end...");
    done.wait().await;

    stat.send(("FIN", 0.0)).unwrap();
    drop(stat);
    stats.join().unwrap();
}

#[tokio::main]
async fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .takes_value(true)
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .required(true)
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("migrate")
                .short("m")
                .long("migrate")
                .required(true)
                .takes_value(true)
                .help("Perform a migration after this many seconds")
                .conflicts_with("stage"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("Enable verbose logging output"),
        )
        .arg(
            Arg::with_name("relevant")
                .long("just-do-it")
                .help(
                    "Run all interesting benchmarks and store \
                     results to appropriately named files.",
                )
                .conflicts_with("all"),
        )
        .arg(
            Arg::with_name("all")
                .long("do-it-all")
                .help(
                    "Run all benchmarks and store \
                     results to appropriately named files.",
                )
                .conflicts_with("relevant"),
        )
        .arg(
            Arg::with_name("skewed")
                .long("skewed")
                .conflicts_with("all")
                .help("Run with a skewed id distribution"),
        )
        .arg(
            Arg::with_name("full")
                .long("full")
                .conflicts_with("all")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("stupid")
                .long("stupid")
                .conflicts_with("all")
                .help("Make the migration stupid"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .help("Use N-way sharding."),
        )
        .get_matches();

    // set config options
    let mut s = graph::Builder::default();
    s.sharding = args
        .value_of("shards")
        .map(|_| value_t_or_exit!(args, "shards", usize));
    s.logging = args.is_present("verbose");

    if args.is_present("all") || args.is_present("relevant") {
        let narticles = value_t_or_exit!(args, "narticles", usize);
        let mills = format!("{}", narticles as f64 / 1_000_000.0);

        // do the ones we need for the paper first
        eprintln!("==> full no reuse (zipf)");
        s.partial = false;
        s.stupid = true;
        one(
            &s,
            true,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-stupid-{}M.zipf1.08.log", mills))
                    .unwrap(),
            ),
        )
        .await;
        eprintln!("==> partial with reuse (uniform)");
        s.partial = true;
        s.stupid = false;
        one(
            &s,
            false,
            &args,
            Some(fs::File::create(format!("vote-partial-reuse-{}M.uniform.log", mills)).unwrap()),
        )
        .await;
        eprintln!("==> partial with reuse (zipf)");
        s.partial = true;
        s.stupid = false;
        one(
            &s,
            true,
            &args,
            Some(fs::File::create(format!("vote-partial-reuse-{}M.zipf1.08.log", mills)).unwrap()),
        )
        .await;

        if !args.is_present("all") {
            return;
        }

        // then the rest
        eprintln!("==> full no reuse (uniform)");
        s.partial = false;
        s.stupid = true;
        one(
            &s,
            false,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-stupid-{}M.uniform.log", mills)).unwrap(),
            ),
        )
        .await;
        eprintln!("==> full with reuse (uniform)");
        s.partial = false;
        s.stupid = false;
        one(
            &s,
            false,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-reuse-{}M.uniform.log", mills)).unwrap(),
            ),
        )
        .await;
        eprintln!("==> full with reuse (zipf)");
        s.partial = false;
        s.stupid = false;
        one(
            &s,
            true,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-reuse-{}M.zipf1.08.log", mills)).unwrap(),
            ),
        )
        .await;
        eprintln!("==> partial no reuse (uniform)");
        s.partial = true;
        s.stupid = true;
        one(
            &s,
            false,
            &args,
            Some(fs::File::create(format!("vote-partial-stupid-{}M.uniform.log", mills)).unwrap()),
        )
        .await;
        eprintln!("==> partial no reuse (zipf)");
        s.partial = true;
        s.stupid = true;
        one(
            &s,
            true,
            &args,
            Some(fs::File::create(format!("vote-partial-stupid-{}M.zipf1.08.log", mills)).unwrap()),
        )
        .await;
    } else {
        let skewed = args.is_present("skewed");
        s.partial = !args.is_present("full");
        s.stupid = args.is_present("stupid");
        one(&s, skewed, &args, None).await;
    }
}
