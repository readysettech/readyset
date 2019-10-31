#![feature(try_blocks)]

#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;

mod backends;
mod server;
use self::backends::Backend;

use failure::Error;
use rusoto_core::Region;
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::borrow::Cow;
use std::fs::File;
use std::io::prelude::*;
use std::{io, thread, time};

const SOUP_AMI: &str = "ami-0fe49768bcb2d68f4";

#[derive(Clone, Copy)]
struct ClientParameters<'a> {
    listen_addr: &'a str,
    backend: &'a Backend,
    runtime: usize,
    warmup: usize,
    articles: usize,
    read_percentage: usize,
    skewed: bool,
}

impl<'a> ClientParameters<'a> {
    fn add_params(&'a self, cmd: &mut Vec<Cow<'a, str>>) {
        cmd.push(self.backend.multiclient_name().into());
        cmd.push(self.listen_addr.into());
        cmd.push("--warmup".into());
        cmd.push(format!("{}", self.warmup).into());
        cmd.push("-r".into());
        cmd.push(format!("{}", self.runtime).into());
        cmd.push("-d".into());
        if self.skewed {
            cmd.push("skewed".into());
        } else {
            cmd.push("uniform".into());
        }
        cmd.push("-a".into());
        cmd.push(format!("{}", self.articles).into());
        cmd.push("--write-every".into());
        let write_every = match self.read_percentage {
            0 => 1,
            100 => {
                // very rare
                2_000_000_000
            }
            n => {
                // 95 (%) == 1 in 20
                // we want a whole number x such that 1-1/x ~= n/100
                // so x - 1 = nx/100
                // so x - nx/100 = 1
                // so (1 - n/100) x = 1
                // so x = 1 / (1 - n/100)
                // so 1 / (100 - n)/100
                // so 100 / (100 - n)
                let div = 100usize.checked_sub(n).expect("percentage >= 100");
                if 100 % div != 0 {
                    panic!("{}% cannot be expressed as 1 in n", n);
                }
                100 / div
            }
        };
        cmd.push(format!("{}", write_every).into());
    }

    fn name(&self, target: usize, ext: &str) -> String {
        format!(
            "{}.{}a.{}t.{}r.{}.{}",
            self.backend.uniq_name(),
            self.articles,
            target,
            self.read_percentage,
            if self.skewed { "skewed" } else { "uniform" },
            ext
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Perf {
    None,
    Hot,
    Cold,
    TraceIoPool,
    TracePool,
}

impl Perf {
    fn is_active(&self) -> bool {
        if let Perf::None = *self {
            false
        } else {
            true
        }
    }

    fn extension(&self) -> &'static str {
        match *self {
            Perf::None => unreachable!(),
            Perf::Hot | Perf::Cold => "folded",
            Perf::TraceIoPool | Perf::TracePool => "strace",
        }
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-orchestrator")
        .version("0.1")
        .about("Orchestrate many runs of the vote benchmark")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("500000")
                .takes_value(true)
                .help("Number of articles to prepopulate the database with"),
        ).arg(
            Arg::with_name("availability_zone")
                .long("availability-zone")
                .value_name("AZ")
                .default_value("us-east-1a")
                .takes_value(true)
                .help("EC2 availability zone to use for launching instances"),
        ).arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("40")
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        ).arg(
            Arg::with_name("warmup")
                .long("warmup")
                .default_value("40")
                .takes_value(true)
                .help("Warmup time in seconds"),
        ).arg(
            Arg::with_name("metal")
                .long("metal")
                .help("Run on bare-metal hardware (i3.metal)"),
        ).arg(
            Arg::with_name("read_percentage")
                .short("p")
                .default_value("95")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help("The percentage of operations that are reads"),
        ).arg(
            Arg::with_name("distribution")
                .short("d")
                .possible_values(&["uniform", "skewed", "both"])
                .default_value("both")
                .takes_value(true)
                .help("How to distribute keys."),
        ).arg(
            Arg::with_name("backends")
                .long("backend")
                .multiple(true)
                .takes_value(true)
                .number_of_values(1)
                .possible_values(&[
                    "memcached",
                    "mysql",
                    "netsoup-0",
                    "hybrid",
                    "netsoup-4",
                    "mssql",
                ]).help("Which backends to run [all if none are given]"),
        ).arg(
            Arg::with_name("ctype")
                .long("client")
                .default_value("c5d.4xlarge")
                .required(true)
                .takes_value(true)
                .help("Instance type for clients"),
        ).arg(
            Arg::with_name("keep")
                .long("keep")
                .help("Keep the VMs running after the benchmarks have completed."),
        ).arg(
            Arg::with_name("perf")
                .long("perf")
                .takes_value(true)
                .possible_values(&["hot", "cold", "trace-io", "trace-pool"])
                .help("Run perf(hot) or offcputime(cold) and stackcollapse on each run; or produce straces of a pool thread"),
        ).arg(
            Arg::with_name("cohost")
                .long("cohost")
                .help("Host all clients on a single instance"),
        ).arg(
            Arg::with_name("branch")
                .long("branch")
                .takes_value(true)
                .help("Check out this branch before building Noria"),
        ).arg(
            Arg::with_name("clients")
                .long("clients")
                .short("c")
                .default_value("6")
                .required(true)
                .takes_value(true)
                .help("Number of client machines to spawn"),
        ).arg(
            Arg::with_name("targets")
                .index(1)
                .multiple(true)
                .help("Which target loads to run [something sensible by default]"),
        ).get_matches();

    let runtime = value_t_or_exit!(args, "runtime", usize);
    let warmup = value_t_or_exit!(args, "warmup", usize);
    let articles = value_t_or_exit!(args, "articles", usize);
    let read_percentages: Vec<usize> = args
        .values_of("read_percentage")
        .unwrap()
        .map(|rp| rp.parse().unwrap())
        .collect();
    let branch = args.value_of("branch").map(String::from);
    let skewed = match args.value_of("distribution").unwrap() {
        "uniform" => &[false][..],
        "skewed" => &[true][..],
        "both" => &[false, true][..],
        _ => unreachable!(),
    };
    let do_perf = match args.value_of("perf") {
        Some("hot") => Perf::Hot,
        Some("cold") => Perf::Cold,
        Some("trace-io") => Perf::TraceIoPool,
        Some("trace-pool") => Perf::TracePool,
        None => Perf::None,
        Some(v) => unreachable!("unknown perf type: {}", v),
    };
    let cohost_clients = args.is_present("cohost");
    let nclients = value_t_or_exit!(args, "clients", usize);
    let az = args.value_of("availability_zone").unwrap();

    let stype = if args.is_present("metal") {
        "i3.metal"
    } else {
        "c5d.4xlarge"
    };
    let ctype = args.value_of("ctype").unwrap();

    // guess the core counts
    let ccores = args
        .value_of("ctype")
        .and_then(ec2_instance_type_cores)
        .map(|mut cores| {
            if cohost_clients {
                cores /= nclients as u16;
            }

            // one core for load generators to be on the safe side
            match cores {
                1 => 1,
                2 => 1,
                n => n - 1,
            }
        })
        .expect("could not determine client core count");

    // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let sts = StsClient::new(Region::UsEast1);
    let provider = StsAssumeRoleSessionCredentialsProvider::new(
        sts,
        "arn:aws:sts::125163634912:role/soup".to_owned(),
        "vote-benchmark".to_owned(),
        None,
        None,
        None,
        None,
    );

    let xbranch = branch.clone();
    let mut b = tsunami::TsunamiBuilder::default();
    b.set_region(Region::UsEast1);
    b.set_availability_zone(az);
    b.use_term_logger();
    b.add_set(
        "server",
        1,
        tsunami::MachineSetup::new(stype, SOUP_AMI, move |host| {
            // ensure we don't have stale soup (yuck)
            let _ = host
                .just_exec(&["git", "-C", "noria", "pull", "2>&1"])?
                .is_ok();

            // check out desired branch (if any)
            if let Some(ref branch) = xbranch {
                eprintln!(" -> checking out branch '{}'", branch);
                let _ = host
                    .just_exec(&["git", "-C", "noria", "checkout", branch, "2>&1"])?
                    .is_ok();
            }

            if do_perf.is_active() {
                // allow kernel debugging
                let _ = host
                    .just_exec(&[
                        "echo",
                        "0",
                        "|",
                        "sudo",
                        "tee",
                        "/proc/sys/kernel/kptr_restrict",
                    ])?
                    .is_ok();
                let _ = host
                    .just_exec(&[
                        "echo",
                        "-1",
                        "|",
                        "sudo",
                        "tee",
                        "/proc/sys/kernel/perf_event_paranoid",
                    ])?
                    .is_ok();

                // get flamegraph
                let _ = host
                    .just_exec(&[
                        "git",
                        "clone",
                        "https://github.com/brendangregg/FlameGraph.git",
                    ])?
                    .is_ok();
            }

            eprintln!(" -> setting up mssql ramdisk");
            let _ = host.just_exec(&["sudo", "/opt/mssql/ramdisk.sh"])?.is_ok();

            let build = |host: &tsunami::Session| {
                eprintln!(" -> building noria-server");
                let _ = host
                    .just_exec(&[
                        "cd",
                        "noria",
                        "&&",
                        "cargo",
                        "b",
                        "--release",
                        "--bin",
                        "noria-server",
                        "2>&1",
                    ])?
                    .is_ok();
                Ok(())
            };

            if stype == "i3.metal" {
                tidy_metal(&host, build)?;
            } else {
                build(&host)?;
            }

            Ok(())
        })
        .as_user("ubuntu"),
    );
    let xctype = ctype.to_string();
    b.add_set(
        "client",
        if cohost_clients { 1 } else { nclients as u32 },
        tsunami::MachineSetup::new(ctype, SOUP_AMI, move |host| {
            let _ = host
                .just_exec(&["git", "-C", "noria", "pull", "2>&1"])?
                .is_ok();

            // check out desired branch (if any)
            if let Some(ref branch) = branch {
                eprintln!(" -> checking out branch '{}'", branch);
                let _ = host
                    .just_exec(&["git", "-C", "noria", "checkout", branch, "2>&1"])?
                    .is_ok();
            }

            let build = |host: &tsunami::Session| {
                eprintln!(" -> building vote client on client");
                let _ = host
                    .just_exec(&[
                        "cd",
                        "noria",
                        "&&",
                        "cargo",
                        "b",
                        "--release",
                        "--bin",
                        "vote",
                        "2>&1",
                    ])?
                    .is_ok();
                Ok(())
            };

            if xctype == "i3.metal" {
                tidy_metal(&host, build)?;
            } else {
                build(&host)?;
            }

            Ok(())
        })
        .as_user("ubuntu"),
    );

    // what backends are we benchmarking?
    let backends = if let Some(bs) = args.values_of("backends") {
        bs.filter_map(|b| match b {
            "memcached" => Some(Backend::Memcached),
            "mysql" => Some(Backend::Mysql),
            "netsoup-0" => Some(Backend::Netsoup { shards: None }),
            "hybrid" => Some(Backend::Hybrid),
            "netsoup-4" => Some(Backend::Netsoup { shards: Some(4) }),
            "mssql" => Some(Backend::Mssql),
            b => {
                eprintln!("unknown backend '{}' specified", b);
                None
            }
        })
        .collect()
    } else {
        vec![
            Backend::Memcached,
            Backend::Mysql,
            Backend::Netsoup { shards: None },
            Backend::Hybrid,
            Backend::Netsoup { shards: Some(4) },
            Backend::Mssql,
        ]
    };

    // if the user wants us to terminate, finish whatever we're currently doing first
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }) {
        eprintln!("==> failed to set ^C handler: {}", e);
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(nclients as usize + 1)
        .build_global()
        .unwrap();

    if stype == "i3.metal" || ctype == "i3.metal" {
        b.wait_limit(time::Duration::from_secs(10 * 60));
    } else {
        b.wait_limit(time::Duration::from_secs(2 * 60));
    }

    b.set_max_duration(6);
    b.run_as(provider, |mut hosts| {
        let server = hosts.remove("server").unwrap().swap_remove(0);
        let clients = hosts.remove("client").unwrap();
        let listen_addr = &server.private_ip;

        // write out host files for ergonomic ssh
        let r: io::Result<()> = try {
            let mut f = File::create("server.host")?;
            writeln!(f, "ubuntu@{}", server.public_dns)?;

            for (i, c) in clients.iter().enumerate() {
                let mut f = File::create(format!("client-{}.host", i))?;
                writeln!(f, "ubuntu@{}", c.public_dns)?;
            }
        };

        if let Err(e) = r {
            eprintln!("failed to write out host files: {:?}", e);
        }

        let targets = if let Some(ts) = args.values_of("targets") {
            ts.map(|t| t.parse().unwrap()).collect()
        } else {
            vec![
                5_000, 10_000, 50_000, 100_000, 175_000, 250_000, 500_000, 1_000_000, 1_500_000,
                2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 8_000_000, 10_000_000,
                11_000_000, 12_000_000, 13_000_000, 14_000_000, 15_000_000, 16_000_000,
            ]
        };

        for backend in backends {
            eprintln!("==> {}", backend.uniq_name());

            eprintln!(" -> starting server");
            let mut s =
                match server::start(server.ssh.as_ref().unwrap(), listen_addr, &backend).unwrap() {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("failed to start {:?}: {:?}", backend, e);
                        continue;
                    }
                };

            // give the server a little time to get its stuff together
            if let Err(e) = s.wait(clients[0].ssh.as_ref().unwrap(), &backend) {
                eprintln!("failed to start {:?}: {:?}", backend, e);
                continue;
            }
            eprintln!(" .. server started ");

            let mut first = true;
            'out: for &read_percentage in &read_percentages {
                for &skewed in skewed {
                    let params = ClientParameters {
                        backend: &backend,
                        listen_addr,
                        runtime,
                        warmup,
                        read_percentage,
                        articles,
                        skewed,
                    };

                    let iters = 1;
                    for iter in 0..iters {
                        for &target in &targets {
                            if first {
                                first = false;
                            } else {
                                s = s.between_targets(&backend).unwrap();

                                // wait in case server was restarted
                                if let Err(e) = s.wait(clients[0].ssh.as_ref().unwrap(), &backend) {
                                    eprintln!("failed to restart {:?}: {:?}", backend, e);
                                    continue;
                                }
                            }

                            eprintln!(
                                " -> {} [run {}/{}] @ {}",
                                params.name(target, ""),
                                iter + 1,
                                iters,
                                chrono::Local::now().time()
                            );
                            if !run_clients(
                                iter, nclients, do_perf, &clients, ccores, &mut s, target, params,
                            ) {
                                // backend clearly couldn't handle the load, so don't run higher targets
                                break;
                            }

                            if !running.load(Ordering::SeqCst) {
                                // user pressed ^C
                                break 'out;
                            }
                        }
                    }
                }
            }

            eprintln!(" -> stopping server");
            s.end(&backend).unwrap();
            eprintln!(" .. server stopped ");

            if !running.load(Ordering::SeqCst) {
                // user pressed ^C
                break;
            }

            thread::sleep(time::Duration::from_secs(5));
        }

        if !running.load(Ordering::SeqCst) {
            eprintln!("==> terminating early due to ^C");
        }

        eprintln!("==> about to terminate the following ec2 instances:");
        eprintln!("server: {}", server.public_dns);
        for client in &clients {
            eprintln!("client: {}", client.public_dns);
        }

        let mut keep = args.is_present("keep");
        if !keep {
            eprintln!("==> press enter to interrupt");
            match timeout_readwrite::TimeoutReader::new(
                io::stdin(),
                Some(time::Duration::from_secs(10)),
            )
            .read(&mut [0u8])
            {
                Ok(_) => {
                    // user pressed enter to interrupt
                    keep = true;
                }
                Err(_) => {
                    // doesn't really matter what the error was
                    // we should shutdown immediately (which we do by not waiting...)
                }
            }
        }

        if keep {
            eprintln!(" -> delaying shutdown; press enter to clean up");
            let _ = io::stdin().read(&mut [0u8]).is_ok();
        }

        Ok(())
    })
    .unwrap();
}

// returns true if next target is feasible
fn run_clients(
    iter: usize,
    nclients: usize,
    do_perf: Perf,
    clients: &Vec<tsunami::Machine>,
    ccores: u16,
    server: &mut server::Server,
    target: usize,
    params: ClientParameters,
) -> bool {
    // first, we need to prime from some host -- doesn't really matter which
    {
        clients[0].ssh.as_ref().unwrap().set_timeout(0);
        eprintln!(" .. prepopulating on {}", clients[0].public_dns);

        let mut prime_params = params.clone();
        prime_params.warmup = 0;
        prime_params.runtime = 0;
        let mut cmd = Vec::<Cow<str>>::new();
        cmd.push("env".into());
        cmd.push("RUST_BACKTRACE=1".into());
        cmd.push("multiclient.sh".into());
        prime_params.add_params(&mut cmd);
        cmd.push("--threads".into());
        cmd.push(format!("{}", 6 * ccores).into());
        cmd.push("2>&1".into());
        cmd.push("|".into());
        cmd.push("tee".into());
        cmd.push("prepop.out".into());
        let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();

        match clients[0].ssh.as_ref().unwrap().just_exec(&cmd[..]) {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("{} failed to populate:", clients[0].public_dns);
                eprintln!("{}", e);
                eprintln!("");
                return false;
            }
            Err(e) => {
                eprintln!("{} failed to populate:", clients[0].public_dns);
                eprintln!("{:?}", e);
                eprintln!("");
                return false;
            }
        }

        eprintln!(
            " .. finished prepopulation @ {}",
            chrono::Local::now().time()
        );
    }

    let target_per_client = (target as f64 / nclients as f64).ceil() as usize;

    // little trick to support cohosting
    let clients = clients.iter().cycle().take(nclients);

    // start all the benchmark clients
    let workers: Vec<_> = clients
        .filter_map(
            |&tsunami::Machine {
                 ref ssh,
                 ref public_dns,
                 ..
             }| {
                let ssh = ssh.as_ref().unwrap();

                eprintln!(
                    " .. starting benchmarker on {} with target {}",
                    public_dns, target
                );

                let mut cmd = Vec::<Cow<str>>::new();
                cmd.push("multiclient.sh".into());
                params.add_params(&mut cmd);
                cmd.push("--no-prime".into());
                cmd.push("--threads".into());
                cmd.push(format!("{}", 6 * ccores).into());
                cmd.push("--target".into());
                cmd.push(format!("{}", target_per_client).into());
                // tee stdout and stderr
                // https://stackoverflow.com/a/692407/472927
                cmd.push(">".into());
                cmd.push(">(tee run.out)".into());
                cmd.push("2>".into());
                cmd.push(">(tee run.err >&2)".into());

                let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                match ssh.exec(&cmd[..]) {
                    Ok(c) => Some((public_dns, c)),
                    Err(e) => {
                        eprintln!("{} failed to run benchmark client:", public_dns);
                        eprintln!("{:?}", e);
                        eprintln!("");
                        None
                    }
                }
            },
        )
        .collect();

    // wait for warmup to finish
    thread::sleep(time::Duration::from_secs(params.warmup as u64));

    // do some profiling
    let perf = if !do_perf.is_active() || iter != 0 {
        None
    } else if let Backend::Netsoup { .. } = params.backend {
        let r: Result<_, failure::Error> = try {
            server.server.set_compress(true);
            let mut c = server.server.exec(&["pgrep", "noria-server"])?;
            let mut stdout = String::new();
            c.read_to_string(&mut stdout)?;
            c.wait_eof()?;

            // set up

            match stdout
                .lines()
                .next()
                .and_then(|line| line.parse::<usize>().ok())
            {
                Some(pid) => {
                    let pid = format!("{}", pid);
                    match do_perf {
                        Perf::Hot => server.server.exec(&[
                            "perf",
                            "record",
                            "-g",
                            "--call-graph",
                            "dwarf",
                            "-p",
                            &pid,
                            ">",
                            "perf.data",
                        ])?,
                        Perf::Cold => {
                            // -5 so that we don't measure the tail end
                            let runtime = format!("{}", params.runtime - 5);
                            server.server.exec(&[
                                "sudo",
                                "offcputime-bpfcc",
                                "-f",
                                "-p",
                                &pid,
                                &runtime,
                                ">",
                                "offcputime.folded",
                                "2>",
                                "offcputime.err",
                            ])?
                        }
                        Perf::TraceIoPool | Perf::TracePool => {
                            let search = match do_perf {
                                Perf::TraceIoPool => {
                                    format!(r#"$2 == "tokio-io-pool-2" {{ print $1 }}"#)
                                }
                                Perf::TracePool => {
                                    format!(r#"$2 == "tokio-pool-2" {{ print $1 }}"#)
                                }
                                _ => unreachable!(),
                            };
                            let mut c = server
                                .server
                                .exec(&["ps", "H", "-o", "tid comm", &pid, "|", "awk", &search])?;
                            let mut stdout = String::new();
                            c.read_to_string(&mut stdout)?;
                            c.wait_eof()?;
                            let tid = stdout
                                .lines()
                                .next()
                                .and_then(|line| line.parse::<usize>().ok())
                                .unwrap();
                            let tid = format!("{}", tid);

                            // -5 so that we don't measure the tail end
                            let runtime = format!("{}s", params.runtime - 5);
                            server.server.exec(&[
                                "sudo",
                                "timeout",
                                &runtime,
                                "strace",
                                "-T",
                                "-yy",
                                "-p",
                                &tid,
                                "-o",
                                "strace.out",
                            ])?
                        }
                        _ => unreachable!(),
                    }
                }
                None => {
                    Err(format_err!("did not find pid for perf"))?;
                    unreachable!();
                }
            }
        };

        match r {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("failed to run perf on server:");
                eprintln!("{:?}", e);
                eprintln!("");
                None
            }
        }
    } else {
        None
    };

    // let's see how we did
    let mut overloaded = None;
    let mut any_not_overloaded = false;
    let fname = params.name(target, "log");
    let mut outf = if iter == 0 {
        File::create(&fname)
    } else {
        use std::fs::OpenOptions;
        OpenOptions::new()
            .truncate(false)
            .append(true)
            .create(true)
            .open(&fname)
    };
    if let Err(ref e) = outf {
        eprintln!(" !! failed to open output file {}: {}", fname, e);
    }

    eprintln!(" .. waiting for benchmark to complete");
    for (host, mut chan) in workers {
        let mut got_lines = true;
        if let Ok(ref mut f) = outf {
            // TODO: should we get histogram files here instead and merge them?

            let mut stdout = String::new();
            chan.read_to_string(&mut stdout).unwrap();
            f.write_all(stdout.as_bytes()).unwrap();

            got_lines = false;
            let mut is_overloaded = false;
            for line in stdout.lines() {
                if !line.starts_with('#') {
                    let mut fields = line.split_whitespace().skip(1);
                    let pct: u32 = fields.next().unwrap().parse().unwrap();
                    let sjrn: u32 = fields.next().unwrap().parse().unwrap();
                    got_lines = true;

                    if pct == 50 && sjrn > 100_000 {
                        is_overloaded = true;
                    }
                }
            }

            if is_overloaded {
                eprintln!(" !! client {} was overloaded", host);
            } else {
                any_not_overloaded = true;
            }

            let was_overloaded = overloaded.unwrap_or(false);
            if overloaded.is_some() && is_overloaded != was_overloaded {
                // one client was overloaded, while another wasn't....
                eprintln!(" !! unequal overload detected");
            }

            overloaded = Some(is_overloaded || was_overloaded);
        }

        let mut stderr = String::new();
        chan.stderr().read_to_string(&mut stderr).unwrap();
        chan.wait_eof().unwrap();
        chan.wait_close().unwrap();

        if !got_lines || chan.exit_status().unwrap() != 0 {
            eprintln!("{} failed to run benchmark client:", host);
            eprintln!("{}", stderr);
            eprintln!("");
            any_not_overloaded = false;
        } else if !stderr.is_empty() {
            eprintln!("{} reported:", host);
            let stderr = stderr.trim_end().replace('\n', "\n > ");
            eprintln!(" > {}", stderr);
        }
    }

    if let Ok(ref mut f) = outf {
        // also gather memory usage and stuff
        if f.write_all(b"# server stats:\n").is_ok() {
            if let Err(e) = server.write_stats(params.backend, f) {
                eprintln!(" !! failed to gather server stats: {}", e);
            }
        }
    }

    if let Some(mut c) = perf {
        eprintln!(" .. collecting perf results");

        // make perf exit
        if let Perf::Hot = do_perf {
            let mut k = server.server.exec(&["pkill", "perf"]).unwrap();
            k.wait_eof().unwrap();
        }

        // wait for perf to exit, and then fetch top entries from the report
        c.wait_eof().unwrap();

        let fname = params.name(target, do_perf.extension());
        if let Ok(mut f) = File::create(&fname) {
            let k = match do_perf {
                Perf::Hot => server.server.exec(&[
                    "perf",
                    "script",
                    "-i",
                    "perf.data",
                    "|",
                    "FlameGraph/stackcollapse-perf.pl",
                ]),
                Perf::Cold => server.server.exec(&["cat", "offcputime.folded"]),
                Perf::TraceIoPool | Perf::TracePool => server.server.exec(&["cat", "strace.out"]),
                _ => unreachable!(),
            };

            match k {
                Ok(mut c) => {
                    io::copy(&mut c, &mut f).unwrap();
                    c.wait_eof().unwrap();
                }
                Err(e) => {
                    eprintln!("failed to parse perf report");
                    eprintln!("{:?}", e);
                    eprintln!("");
                }
            }
        }
    }

    any_not_overloaded
}

fn ec2_instance_type_cores(it: &str) -> Option<u16> {
    if it == "i3.metal" {
        // https://aws.amazon.com/ec2/instance-types/i3/
        return Some(72);
    }

    it.rsplitn(2, '.').next().and_then(|itype| match itype {
        "nano" | "micro" | "small" => Some(1),
        "medium" | "large" => Some(2),
        t if t.ends_with("xlarge") => {
            let mult = t.trim_end_matches("xlarge").parse::<u16>().unwrap_or(1);
            Some(4 * mult)
        }
        _ => None,
    })
}

fn tidy_metal<F>(host: &tsunami::Session, build: F) -> Result<(), Error>
where
    F: FnOnce(&tsunami::Session) -> Result<(), Error>,
{
    // rebuild rocksdb and cargo clean
    host.just_exec(&["cd", "rocksdb", "&&", "make", "clean"])?
        .expect("rocksdb make clean");
    host.just_exec(&["cd", "rocksdb", "&&", "make", "-j32", "shared_lib"])?
        .expect("rocksdb make shared_lib");
    host.just_exec(&["cd", "noria", "&&", "cargo", "clean"])?
        .expect("cargo clean");

    // then build
    build(host)?;

    // and then disable all the extra cores
    let cores = ec2_instance_type_cores("i3.metal").unwrap();
    eprintln!(" -> disabling extra cores on bare-metal machine");
    for corei in 16..cores {
        let core = format!("/sys/devices/system/cpu/cpu{}/online", corei);
        host.just_exec(&["echo", "0", "|", "sudo", "tee", &core])?
            .expect(&format!("disable core {}", corei));
    }
    Ok(())
}

impl ConvenientSession for tsunami::Session {
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error> {
        let cmd: Vec<_> = cmd
            .iter()
            .map(|&arg| match arg {
                "&" | "&&" | "<" | ">" | "2>" | "2>&1" | "|" => arg.to_string(),
                arg if arg.starts_with(">(") => arg.to_string(),
                _ => shellwords::escape(arg),
            })
            .collect();
        let cmd = cmd.join(" ");
        eprintln!("    :> {}", cmd);

        // ensure we're using a Bourne shell (that's what shellwords supports too)
        let cmd = format!("bash -c {}", shellwords::escape(&cmd));

        let mut c = self.channel_session()?;
        c.exec(&cmd)?;
        Ok(c)
    }
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<(), String>, Error> {
        let mut c = self.exec(cmd)?;

        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        c.wait_eof()?;

        if c.exit_status()? != 0 {
            return Ok(Err(stdout));
        }
        Ok(Ok(()))
    }

    fn in_noria(&self, cmd: &[&str]) -> Result<Result<(), String>, Error> {
        let mut args = vec!["cd", "noria", "&&"];
        args.extend(cmd);
        self.just_exec(&args[..])
    }
}

trait ConvenientSession {
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error>;
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<(), String>, Error>;
    fn in_noria(&self, cmd: &[&str]) -> Result<Result<(), String>, Error>;
}
