#![feature(type_alias_impl_trait)]

use clap::value_t_or_exit;
use failure::ResultExt;
use futures_util::future::{Either, FutureExt, TryFutureExt};
use futures_util::stream::futures_unordered::FuturesUnordered;
use noria_applications::Timeline;
use rand::prelude::*;
use rand_distr::Exp;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs;
use std::mem;
use std::sync::{atomic, Arc, Mutex};
use std::task::Poll;
use std::thread;
use std::time;
use tokio::stream::StreamExt;
use tower_service::Service;

thread_local! {
    static TIMING: RefCell<(Timeline, Timeline)> = RefCell::new(Default::default());
}

fn throughput(ops: usize, took: time::Duration) -> f64 {
    ops as f64 / took.as_secs_f64()
}

const MAX_BATCH_TIME: time::Duration = time::Duration::from_millis(10);

mod clients;
use self::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};

fn run<C>(global_args: &clap::ArgMatches, local_args: &clap::ArgMatches)
where
    C: VoteClient + Unpin + 'static,
    C: Service<ReadRequest, Response = (), Error = failure::Error> + Clone + Send,
    C: Service<WriteRequest, Response = (), Error = failure::Error> + Clone + Send,
    <C as Service<ReadRequest>>::Future: Send,
    <C as Service<ReadRequest>>::Response: Send,
    <C as Service<WriteRequest>>::Future: Send,
    <C as Service<WriteRequest>>::Response: Send,
{
    // zipf takes ~66ns to generate a random number depending on the CPU,
    // so each load generator cannot reasonably generate much more than ~1M reqs/s.
    let per_generator = 3_000_000;
    let mut target = value_t_or_exit!(global_args, "ops", f64);
    let ngen = (target as usize + per_generator - 1) / per_generator; // rounded up
    target /= ngen as f64;

    let articles = value_t_or_exit!(global_args, "articles", usize);

    let params = Parameters {
        prime: !global_args.is_present("no-prime"),
        articles,
    };

    let skewed = match global_args.value_of("distribution") {
        Some("skewed") => true,
        Some("uniform") => false,
        _ => unreachable!(),
    };

    let write_t = Arc::new(Mutex::new(Timeline::default()));
    let read_t = Arc::new(Mutex::new(Timeline::default()));

    let ts = (write_t.clone(), read_t.clone());

    let available_cores = num_cpus::get() - ngen;
    let mut rt = tokio::runtime::Builder::new()
        .enable_all()
        .threaded_scheduler()
        .thread_name("voter")
        .core_threads(available_cores)
        .on_thread_stop(move || {
            TIMING.with(|hs| {
                let hs = hs.borrow();
                ts.0.lock().unwrap().merge(&hs.0);
                ts.1.lock().unwrap().merge(&hs.1);
            })
        })
        .build()
        .unwrap();

    eprintln!("setting up client");

    let handle: C = {
        let local_args = local_args.clone();
        // we know that we won't drop the original args until the runtime has exited
        let local_args: clap::ArgMatches<'static> = unsafe { mem::transmute(local_args) };
        rt.block_on(async move { C::new(params, local_args).await.unwrap() })
    };

    eprintln!("setup completed");

    let start = std::time::SystemTime::now();
    let errd: &'static _ = &*Box::leak(Box::new(atomic::AtomicBool::new(false)));
    let generators: Vec<_> = (0..ngen)
        .map(|geni| {
            let ex = rt.handle().clone();
            let handle = handle.clone();
            let global_args = global_args.clone();

            // we know that we won't drop the original args until the thread has exited
            let global_args: clap::ArgMatches<'static> = unsafe { mem::transmute(global_args) };

            thread::Builder::new()
                .name(format!("load-gen{}", geni))
                .spawn(move || {
                    if skewed {
                        run_generator(
                            handle,
                            errd,
                            ex,
                            zipf::ZipfDistribution::new(articles, 1.08).unwrap(),
                            target,
                            global_args,
                        )
                    } else {
                        run_generator(
                            handle,
                            errd,
                            ex,
                            rand::distributions::Uniform::new(1, articles + 1),
                            target,
                            global_args,
                        )
                    }
                })
                .unwrap()
        })
        .collect();

    let mut ops = 0.0;
    let mut wops = 0.0;
    let mut duration = time::Duration::new(0, 0);
    for gen in generators {
        let (gen, completed, took) = gen.join().unwrap();
        ops += gen;
        wops += completed;
        duration = duration.max(took);
    }
    drop(handle);
    drop(rt);

    // all done!
    println!("# generated ops/s: {:.2}", ops);
    println!("# actual ops/s: {:.2}", wops);
    println!("# op\tpct\tsojourn\tremote");

    let mut write_t = write_t.lock().unwrap();
    let mut read_t = read_t.lock().unwrap();

    write_t.set_total_duration(duration);
    read_t.set_total_duration(duration);

    if let Some(h) = global_args.value_of("histogram") {
        match fs::File::create(h) {
            Ok(mut f) => {
                use hdrhistogram::serialization::interval_log;
                use hdrhistogram::serialization::V2DeflateSerializer;
                let mut s = V2DeflateSerializer::new();
                let mut w = interval_log::IntervalLogWriterBuilder::new()
                    .with_base_time(start)
                    .begin_log_with(&mut f, &mut s)
                    .unwrap();
                write_t.write(&mut w).unwrap();
                read_t.write(&mut w).unwrap();
            }
            Err(e) => {
                eprintln!("failed to open histogram file for writing: {:?}", e);
            }
        }
    }

    let (rmt_w_t, sjrn_w_t) = write_t.collapse();
    let (rmt_r_t, sjrn_r_t) = read_t.collapse();

    println!(
        "write\t50\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.5),
        rmt_w_t.value_at_quantile(0.5)
    );
    println!(
        "read\t50\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.5),
        rmt_r_t.value_at_quantile(0.5)
    );
    println!(
        "write\t95\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.95),
        rmt_w_t.value_at_quantile(0.95)
    );
    println!(
        "read\t95\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.95),
        rmt_r_t.value_at_quantile(0.95)
    );
    println!(
        "write\t99\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.99),
        rmt_w_t.value_at_quantile(0.99)
    );
    println!(
        "read\t99\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.99),
        rmt_r_t.value_at_quantile(0.99)
    );
    println!(
        "write\t100\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.max(),
        rmt_w_t.max()
    );
    println!(
        "read\t100\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.max(),
        rmt_r_t.max()
    );
}

fn run_generator<C, R>(
    mut handle: C,
    errd: &'static atomic::AtomicBool,
    ex: tokio::runtime::Handle,
    id_rng: R,
    target: f64,
    global_args: clap::ArgMatches,
) -> (f64, f64, time::Duration)
where
    C: VoteClient + Unpin + 'static,
    R: rand::distributions::Distribution<usize>,
    C: Service<ReadRequest, Response = (), Error = failure::Error> + Clone + Send,
    C: Service<WriteRequest, Response = (), Error = failure::Error> + Clone + Send,
    <C as Service<ReadRequest>>::Future: Send,
    <C as Service<ReadRequest>>::Response: Send,
    <C as Service<WriteRequest>>::Future: Send,
    <C as Service<WriteRequest>>::Response: Send,
{
    let runtime = time::Duration::from_secs(value_t_or_exit!(global_args, "runtime", u64));
    let every = value_t_or_exit!(global_args, "ratio", u32);

    let interarrival = Exp::new(target * 1e-9).unwrap();
    let mut next = time::Instant::now();

    // each of these is a VecDeque<(Vec<key>, Vec<time>)>, where each inner Vec is a batch
    // we separate time and keys so that we can pass the keys to the op without a memcpy
    let mut queued_w = VecDeque::new();
    let mut queued_r = VecDeque::new();
    let mut ops = 0;

    // keep track of how large batches are growing so we can generally allocate just once for each
    let mut w_capacity = 128;
    let mut r_capacity = 128;

    let mut rng = rand::thread_rng();
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();

    let start = time::Instant::now();
    let end = start + runtime;

    // we *could* use a rayon::scope here to safely access stack variables from inside each job,
    // but that would *also* force us to place the load generators *on* the thread pool (because of
    // https://github.com/rayon-rs/rayon/issues/562). that comes with a number of unfortunate
    // side-effects, such as having to manage allocations of clients to workers, clean exiting,
    // etc. we *instead* just leak the one thing we care about (`ndone`) so that they can be
    // accessed from inside the jobs.
    //
    // this may change with https://github.com/rayon-rs/rayon/issues/544, but that's what we have
    // to do for now.
    let ndone: &'static _ = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let nwrite: &'static _ = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let nread: &'static _ = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));

    // when https://github.com/rust-lang/rust/issues/56556 is fixed, take &[i32] instead, make
    // Request hold &'a [i32] (then need for<'a> C: Service<Request<'a>>). then we no longer need
    // .split_off in calls to enqueue.
    let enqueue = move |client: &mut C,
                        queued: Vec<_>,
                        mut keys: Vec<_>,
                        write,
                        wait: Option<&mut FuturesUnordered<_>>| {
        let n = keys.len();
        let sent = time::Instant::now();
        let fut = if write {
            nwrite.fetch_add(n, atomic::Ordering::AcqRel);
            Either::Left(
                client
                    .call(WriteRequest(keys))
                    .map(|r| r.context("failed to handle writes")),
            )
        } else {
            nread.fetch_add(n, atomic::Ordering::AcqRel);
            // deduplicate requested keys, because not doing so would be silly
            keys.sort_unstable();
            keys.dedup();
            Either::Right(
                client
                    .call(ReadRequest(keys))
                    .map(|r| r.context("failed to handle reads")),
            )
        }
        .map_ok(move |_| {
            let done = time::Instant::now();
            ndone.fetch_add(n, atomic::Ordering::AcqRel);
            if write {
                nwrite.fetch_sub(n, atomic::Ordering::AcqRel);
            } else {
                nread.fetch_sub(n, atomic::Ordering::AcqRel);
            }

            TIMING.with(|timing| {
                let mut timing = timing.borrow_mut();
                let timing = &mut *timing;
                let timing = if write { &mut timing.0 } else { &mut timing.1 };
                let hist = timing.histogram_for(sent.duration_since(start));

                let remote_t = done.duration_since(sent);
                let us =
                    remote_t.as_secs() * 1_000_000 + u64::from(remote_t.subsec_nanos()) / 1_000;
                hist.processing(us);

                for started in queued {
                    let sjrn_t = done.duration_since(started);
                    let us =
                        sjrn_t.as_secs() * 1_000_000 + u64::from(sjrn_t.subsec_nanos()) / 1_000;
                    hist.sojourn(us);
                }
            })
        });

        let th = ex.spawn(async move {
            if let Err(e) = fut.await {
                if time::Instant::now() < (end - time::Duration::from_secs(1))
                    && !errd.swap(true, atomic::Ordering::SeqCst)
                {
                    eprintln!("failed to enqueue request: {:?}", e)
                }
            }
        });
        if let Some(wait) = wait {
            wait.push(th);
        }
    };

    while next < end {
        let now = time::Instant::now();
        // only queue a new request if we're told to. if this is not the case, we've
        // just been woken up so we can realize we need to send a batch
        // NOTE: while, not if, in case we start falling behind
        while next <= now {
            let id = id_rng.sample(&mut rng) as i32;
            let (batches, cap_hint) = if rng.gen_bool(1.0 / f64::from(every)) {
                (&mut queued_w, &mut w_capacity)
            } else {
                (&mut queued_r, &mut r_capacity)
            };

            if batches.is_empty() {
                // there is no pending batch, so start one
                batches.push_back((Vec::with_capacity(*cap_hint), Vec::with_capacity(*cap_hint)));
            }

            let (keys, times) = batches.back_mut().expect("push if is_empty");
            keys.push(id);
            times.push(next);

            if next - times[0] >= MAX_BATCH_TIME {
                // no more operations should be added to this batch, so start a new one.
                // keep track of our largest batch size to avoid allocations later.
                *cap_hint = (*cap_hint).max(keys.len());
                batches.push_back((Vec::with_capacity(*cap_hint), Vec::with_capacity(*cap_hint)));
            }

            // schedule next delivery
            next += time::Duration::new(0, interarrival.sample(&mut rng) as u32);
        }

        // try to send some batches
        while !queued_w.is_empty() || !queued_r.is_empty() {
            if !queued_w.is_empty() {
                if let Poll::Ready(r) = rt.block_on(async {
                    futures_util::poll!(futures_util::future::poll_fn(|cx| {
                        Service::<WriteRequest>::poll_ready(&mut handle, cx)
                    }))
                }) {
                    r.unwrap();
                    let (keys, times) = queued_w.pop_front().expect("!is_empty");
                    ops += keys.len();
                    enqueue(&mut handle, times, keys, true, None);
                } else {
                    // we can't send the request yet -- keep generating batches
                }
            }

            if !queued_r.is_empty() {
                if let Poll::Ready(r) = rt.block_on(async {
                    futures_util::poll!(futures_util::future::poll_fn(|cx| {
                        Service::<ReadRequest>::poll_ready(&mut handle, cx)
                    }))
                }) {
                    r.unwrap();
                    let (keys, times) = queued_r.pop_front().expect("!is_empty");
                    ops += keys.len();
                    enqueue(&mut handle, times, keys, false, None);
                } else {
                    // we can't send the request yet -- keep generating batches
                }
            }
        }

        atomic::spin_loop_hint();
    }

    // force the client to also complete their queue
    rt.block_on(async {
        // both poll_ready calls need &mut C, so the borrow checker will get mad. but since we're
        // single-threaded, we know that only one mutable borrow happens _at a time_, so a RefCell
        // takes care of that.
        let handle = std::cell::RefCell::new(&mut handle);
        let wait = std::cell::RefCell::new(FuturesUnordered::new());
        while !queued_r.is_empty() || !queued_w.is_empty() {
            tokio::select! {
                r = futures_util::future::poll_fn(|cx| {
                    Service::<WriteRequest>::poll_ready(&mut *handle.borrow_mut(), cx)
                }), if !queued_w.is_empty() => {
                    r.unwrap();
                    let (keys, times) = queued_w.pop_front().expect("!is_empty");
                    ops += keys.len();
                    enqueue(
                        &mut *handle.borrow_mut(),
                        times,
                        keys,
                        true,
                        Some(&mut *wait.borrow_mut()),
                    );
                }
                r = futures_util::future::poll_fn(|cx| {
                    Service::<ReadRequest>::poll_ready(&mut *handle.borrow_mut(), cx)
                }), if !queued_r.is_empty() => {
                    r.unwrap();
                    let (keys, times) = queued_r.pop_front().expect("!is_empty");
                    ops += keys.len();
                    enqueue(
                        &mut *handle.borrow_mut(),
                        times,
                        keys,
                        false,
                        Some(&mut *wait.borrow_mut()),
                    );
                }
            };
        }
        let mut wait = wait.into_inner();
        while !wait.is_empty() {
            let _ = wait.next().await;
        }
    });
    // there _may_ be batches that were enqueued before the loop before (and therefore weren't in
    // `wait`), and that are still pending. we have little choice but to spin to wait for them,
    // since we have no wait to track their progress or completion beyond polling.
    while nwrite.load(atomic::Ordering::Acquire) != 0 {
        std::thread::yield_now();
    }
    while nread.load(atomic::Ordering::Acquire) != 0 {
        std::thread::yield_now();
    }

    // only now is it acceptable to measure throughput
    let took = start.elapsed();
    let gen = throughput(ops, took);
    let worker_ops = throughput(ndone.load(atomic::Ordering::Acquire), took);

    // need to drop the pool before waiting so that workers will exit
    // and thus hit the barrier
    drop(handle);
    (gen, worker_ops, took)
}

fn main() {
    use clap::{App, Arg, SubCommand};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .possible_values(&["uniform", "skewed"])
                .default_value("uniform")
                .help("Key distribution"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Output serialized HdrHistogram to a file")
                .takes_value(true)
                .long_help(
                    "If the file already exists, the existing histogram is extended.\
                     There are four histograms, written out in order: \
                     sojourn-write, sojourn-read, remote-write, and remote-read",
                ),
        )
        .arg(
            Arg::with_name("ops")
                .long("target")
                .default_value("1000000")
                .help("Target operations per second"),
        )
        .arg(
            Arg::with_name("ratio")
                .long("write-every")
                .default_value("19")
                .value_name("N")
                .help("1-in-N chance of a write"),
        )
        .arg(
            Arg::with_name("no-prime")
                .long("no-prime")
                .help("Indicates that the client should not set up the database"),
        )
        .subcommand(
            SubCommand::with_name("netsoup")
                .arg(
                    Arg::with_name("zookeeper")
                        .short("z")
                        .long("zookeeper")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:2181")
                        .help("Address of Zookeeper instance"),
                )
                .arg(
                    Arg::with_name("deployment")
                        .long("deployment")
                        .required(true)
                        .takes_value(true)
                        .help("Soup deployment ID."),
                ),
        )
        .subcommand(
            SubCommand::with_name("memcached")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:11211")
                        .help("Address of memcached"),
                )
                .arg(
                    Arg::with_name("fast")
                        .long("fast")
                        .help("Only fetch vote counts, not titles."),
                ),
        )
        .subcommand(
            SubCommand::with_name("mssql")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value(
                            "server=tcp:127.0.0.1,1433;username=SA;TrustServerCertificate=true;",
                        )
                        .help("Address of MsSQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MsSQL database to use"),
                ),
        )
        .subcommand(SubCommand::with_name("null"))
        .subcommand(
            SubCommand::with_name("mysql")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:3306")
                        .help("Address of MySQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MySQL database to use"),
                ),
        )
        .subcommand(
            SubCommand::with_name("hybrid")
                .arg(
                    Arg::with_name("memcached-address")
                        .long("memcached-address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:11211")
                        .help("Address of memcached"),
                )
                .arg(
                    Arg::with_name("mysql-address")
                        .long("mysql-address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:3306")
                        .help("Address of MySQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MySQL database to use"),
                ),
        )
        .subcommand(
            SubCommand::with_name("localsoup")
                .arg(
                    Arg::with_name("shards")
                        .long("shards")
                        .takes_value(true)
                        .default_value("2")
                        .help("Shard the graph this many ways (0 = disable sharding)."),
                )
                .arg(
                    Arg::with_name("durability")
                        .long("durability")
                        .takes_value(false)
                        .help("Enable durability for Base nodes"),
                )
                .arg(
                    Arg::with_name("fudge-rpcs")
                        .long("fudge-rpcs")
                        .help("Send pointers instead of serializing data for writes"),
                )
                .arg(
                    Arg::with_name("purge")
                        .long("purge")
                        .takes_value(true)
                        .possible_values(&["none", "reader", "all"])
                        .default_value("none")
                        .help("Choose which views, if any, are placed beyond the materialization_frontier"),
                )
                .arg(
                    Arg::with_name("log-dir")
                        .long("log-dir")
                        .takes_value(true)
                        .help(
                            "Absolute path to the directory where the log files will be written.",
                        ),
                )
                .arg(
                    Arg::with_name("retain-logs-on-exit")
                        .long("retain-logs-on-exit")
                        .takes_value(false)
                        .requires("durability")
                        .help("Do not delete the base node logs on exit."),
                )
                .arg(
                    Arg::with_name("flush-timeout")
                        .long("flush-timeout")
                        .takes_value(true)
                        .default_value("1000000")
                        .help("Time to wait before processing a merged packet, in nanoseconds."),
                )
                .arg(
                    Arg::with_name("persistence-threads")
                        .long("persistence-threads")
                        .takes_value(true)
                        .default_value("1")
                        .help("Number of background threads used by PersistentState."),
                )
                .arg(
                    Arg::with_name("stupid")
                        .long("stupid")
                        .help("Make the migration stupid")
                        .requires("migrate"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .help("Include logging output"),
                ),
        )
        .get_matches();

    match args.subcommand() {
        ("localsoup", Some(largs)) => run::<clients::localsoup::LocalNoria>(&args, largs),
        ("netsoup", Some(largs)) => run::<clients::netsoup::Conn>(&args, largs),
        ("memcached", Some(largs)) => run::<clients::memcached::Conn>(&args, largs),
        //("mssql", Some(largs)) => run::<clients::mssql::Conf>(&args, largs),
        ("mysql", Some(largs)) => run::<clients::mysql::Conn>(&args, largs),
        //("hybrid", Some(largs)) => run::<clients::hybrid::Conf>(&args, largs),
        //("null", Some(largs)) => run::<()>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
