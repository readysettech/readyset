#![deny(unused_extern_crates)]

#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;

use failure::Error;
use failure::ResultExt;
use rusoto_core::Region;
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::borrow::Cow;
use std::fs::File;
use std::io::prelude::*;
use std::{thread, time};

const SOUP_AMI: &str = "ami-05df93bcec8de09d8";

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-distributed")
        .version("0.1")
        .about("Orchestrate runs of the distributed vote benchmark")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("500000")
                .takes_value(true)
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("40")
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .default_value("40")
                .takes_value(true)
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("read_percentage")
                .short("p")
                .default_value("95")
                .takes_value(true)
                .help("The percentage of operations that are reads"),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .possible_values(&["uniform", "skewed"])
                .required(true)
                .takes_value(true)
                .help("How to distribute keys."),
        )
        .arg(
            Arg::with_name("stype")
                .long("server")
                .default_value("c5.4xlarge")
                .required(true)
                .takes_value(true)
                .help("Instance type for server"),
        )
        .arg(
            Arg::with_name("ctype")
                .long("client")
                .default_value("c5.4xlarge")
                .required(true)
                .takes_value(true)
                .help("Instance type for clients"),
        )
        .arg(
            Arg::with_name("servers")
                .long("servers")
                .short("s")
                .default_value("1")
                .required(true)
                .takes_value(true)
                .help("Number of server machines to spawn with a scale of 1"),
        )
        .arg(
            Arg::with_name("clients")
                .long("clients")
                .short("c")
                .default_value("1")
                .required(true)
                .takes_value(true)
                .help("Number of client machines to spawn with a scale of 1"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .required(true)
                .takes_value(true)
                .help("Number of shards per souplet"),
        )
        .arg(
            Arg::with_name("target")
                .long("load-per-client")
                .required(true)
                .default_value("6000000")
                .takes_value(true)
                .help("Load to generate on each client"),
        )
        .arg(
            Arg::with_name("scales")
                .index(1)
                .multiple(true)
                .required(true)
                .help("Scaling factors to try"),
        )
        .get_matches();

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

    // init lots of machines in parallel
    rayon::ThreadPoolBuilder::new()
        .num_threads(20)
        .build_global()
        .unwrap();

    let mut first = true;
    let nservers = value_t_or_exit!(args, "servers", u32);
    let nclients = value_t_or_exit!(args, "clients", u32);

    for scale in args.values_of("scales").unwrap() {
        match scale.parse::<u32>() {
            Ok(scale) => {
                eprintln!(
                    "==> {} servers and {} clients",
                    nservers * scale,
                    nclients * scale
                );

                run_one(&args, first, nservers * scale, nclients * scale)
            }
            Err(e) => eprintln!("Ignoring malformed scale factor {}", e),
        }
        first = false;

        if !running.load(Ordering::SeqCst) {
            // user pressed ^C
            break;
        }
    }
}

fn run_one(args: &clap::ArgMatches, first: bool, nservers: u32, nclients: u32) {
    let runtime = value_t_or_exit!(args, "runtime", usize);
    let warmup = value_t_or_exit!(args, "warmup", usize);
    let articles = value_t_or_exit!(args, "articles", usize);
    let read_percentage = value_t_or_exit!(args, "read_percentage", usize);
    let skewed = args.value_of("distribution").unwrap() == "skewed";
    let shards = value_t_or_exit!(args, "shards", u16);
    let target_per_client = value_t_or_exit!(args, "target", usize);

    let nshards = format!("{}", nservers * shards as u32);
    eprintln!("--> running with {} shards total", nshards);
    eprintln!(
        "--> generating an aggregate {} ops/s",
        target_per_client * nclients as usize
    );

    // guess the core counts
    let for_gen = ((target_per_client + (3_000_000 - 1)) / 3_000_000) as u16;
    let ccores = args
        .value_of("ctype")
        .and_then(ec2_instance_type_cores)
        .map(|cores| {
            assert!(cores > for_gen);
            cores - for_gen
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

    let mut b = tsunami::TsunamiBuilder::default();
    b.set_region(Region::UsEast1);
    b.set_availability_zone("us-east-1a");
    b.use_term_logger();
    b.add_set(
        "server",
        nservers,
        tsunami::MachineSetup::new(args.value_of("stype").unwrap(), SOUP_AMI, move |_host| {
            // ensure we don't have stale soup (yuck)
            /* We don't want to do a rebuild given that we have to re-do it for every scale
            eprintln!(" -> building souplet on server");
            host.just_exec(&["git", "-C", "distributary", "pull", "2>&1"])
                .context("git pull souplet")?
                .map_err(failure::err_msg)?;
            host.just_exec(&[
                "cd",
                "distributary",
                "&&",
                "cargo",
                "b",
                "--release",
                "--bin",
                "souplet",
            ]).context("build souplet")?
                .map_err(failure::err_msg)?;
            */
            Ok(())
        })
        .as_user("ubuntu"),
    );
    b.add_set(
        "client",
        nclients,
        tsunami::MachineSetup::new(args.value_of("ctype").unwrap(), SOUP_AMI, |_host| {
            /* same as above
            eprintln!(" -> building vote client on client");
            host.just_exec(&["git", "-C", "distributary", "pull", "2>&1"])
                .context("git pull distributary")?
                .map_err(failure::err_msg)?;
            host.just_exec(&[
                "cd",
                "distributary",
                "&&",
                "cargo",
                "b",
                "--release",
                "--manifest-path",
                "benchmarks/Cargo.toml",
                "--bin",
                "vote",
            ]).context("build vote")?
                .map_err(failure::err_msg)?;
            */
            Ok(())
        })
        .as_user("ubuntu"),
    );

    macro_rules! vote {
        ($ssh:expr, $args:expr, $bcmd:ident, $tcmd:ident) => {{
            let mut cmd = $bcmd.clone();
            cmd.extend($args.into_iter().map(Cow::from));
            cmd.extend($tcmd.clone());
            let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
            $ssh.exec(&cmd[..])
        }};
    }

    b.wait_limit(time::Duration::from_secs(5 * 60));
    b.set_max_duration(1);
    b.run_as(provider, |mut hosts| {
        let servers = hosts.remove("server").unwrap();
        let clients = hosts.remove("client").unwrap();
        let zookeeper_addr = format!("{}:2181", servers[0].private_ip);
        let zookeeper_addr = Cow::Borrowed(&*zookeeper_addr);

        if first {
            let rev = servers[0]
                .ssh
                .as_ref()
                .unwrap()
                .just_exec(&["git", "-C", "distributary", "rev-parse", "HEAD"])
                .context("git rev-parse HEAD")?
                .map_err(failure::err_msg)?;
            eprintln!("==> revision: {}", rev.trim_end());
            servers[0]
                .ssh
                .as_ref()
                .unwrap()
                .just_exec(&["git", "-C", "distributary", "fetch", "origin"])
                .context("git fetch origin")?
                .map_err(failure::err_msg)?;
            let missing = servers[0]
                .ssh
                .as_ref()
                .unwrap()
                .just_exec(&[
                    "git",
                    "-C",
                    "distributary",
                    "log",
                    "--oneline",
                    "..origin/master",
                ])
                .context("git log --online ..origin/master")?
                .map_err(failure::err_msg)?;
            if !missing.is_empty() {
                eprintln!("==> missing commits from origin:");
                eprint!("{}", missing);
            }
        }

        // first, start zookeeper
        servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(&["sudo", "systemctl", "stop", "zookeeper"])
            .context("stop zookeeper")?
            .map_err(failure::err_msg)?;
        servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(&["sudo", "rm", "-rf", "/var/lib/zookeeper/version-2"])
            .context("wipe zookeeper")?
            .map_err(failure::err_msg)?;
        servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(&["sudo", "systemctl", "start", "zookeeper"])
            .context("start zookeeper")?
            .map_err(failure::err_msg)?;

        // wait for zookeeper to be running
        let start = time::Instant::now();
        while servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(&["echo", "-n", ">", "/dev/tcp/127.0.0.1/2181"])?
            .is_err()
        {
            thread::sleep(time::Duration::from_secs(1));
            if start.elapsed() > time::Duration::from_secs(30) {
                bail!("zookeeper wouldn't start");
            }
        }

        // start all souplets
        // TODO: should we worry about the running directory being on an SSD here?
        let base_cmd: Vec<_> = {
            let mut cmd: Vec<Cow<str>> = ["cd", "distributary", "&&"]
                .iter()
                .map(|&s| s.into())
                .collect();
            cmd.extend(vec![
                "env".into(),
                "RUST_BACKTRACE=1".into(),
                "/home/ubuntu/target/release/souplet".into(),
                "--durability".into(),
                "memory".into(),
                "--shards".into(),
                nshards.clone().into(),
                "--deployment".into(),
                "votebench".into(),
                "--zookeeper".into(),
                zookeeper_addr.clone(),
            ]);
            cmd
        };

        let souplets: Result<Vec<_>, _> = servers
            .iter()
            .map(|s| {
                let mut cmd = base_cmd.clone();
                cmd.push("--address".into());
                cmd.push(Cow::Borrowed(&s.private_ip));
                let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                s.ssh.as_ref().unwrap().exec(&cmd[..])
            })
            .collect();
        let souplets = souplets?;

        // wait a little while for all the souplets to have joined together
        thread::sleep(time::Duration::from_secs(10));

        // --------------------------------------------------------------------------------

        // TODO: in the future, vote threads will be able to handle multiple concurrent threads and
        // we wouldn't need to lie here. for the time being though, we need to oversubscribe,
        // otherwise we're severely underutilizing the client machines.
        let threads = format!("{}", 6 * (ccores - for_gen)).into();
        let base_cmd = vec![
            "cd".into(),
            "distributary".into(),
            "&&".into(),
            "env".into(),
            "RUST_BACKTRACE=1".into(),
            "/home/ubuntu/target/release/vote".into(),
            "--threads".into(),
            threads,
            "--articles".into(),
            format!("{}", articles).into(),
        ];
        let tail_cmd = vec![
            "netsoup".into(),
            "--deployment".into(),
            "votebench".into(),
            "--zookeeper".into(),
            zookeeper_addr,
        ];

        // --------------------------------------------------------------------------------

        // prime
        eprintln!(
            " -> priming from {} @ {}",
            clients[0].public_dns,
            chrono::Local::now().time()
        );

        {
            let mut c = vote!(
                clients[0].ssh.as_ref().unwrap(),
                vec!["-r", "0", "--warmup", "0"],
                base_cmd,
                tail_cmd
            )?;
            let mut stderr = String::new();
            c.stderr().read_to_string(&mut stderr)?;
            c.wait_eof()?;
            if c.exit_status()? != 0 {
                bail!("failed to populate:\n{}", stderr);
            }
        }

        // --------------------------------------------------------------------------------

        let write_every = match read_percentage {
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

        let voters: Result<Vec<_>, _> = clients
            .iter()
            .map(|c| {
                eprintln!(" -> starting client on {}", c.public_dns);
                vote!(
                    c.ssh.as_ref().unwrap(),
                    vec![
                        format!("--no-prime"),
                        format!("-r"),
                        format!("{}", runtime),
                        format!("--warmup"),
                        format!("{}", warmup),
                        format!("-d"),
                        format!("{}", if skewed { "skewed" } else { "uniform" }),
                        format!("--write-every"),
                        format!("{}", write_every),
                        format!("--target"),
                        format!("{}", target_per_client),
                    ],
                    base_cmd,
                    tail_cmd
                )
            })
            .collect();
        let voters = voters?;

        // let's see how we did
        let mut outf = File::create(&format!(
            "vote-distributed-{}s.{}a.{}r.{}.{}t.{}h.log",
            shards,
            articles,
            read_percentage,
            if skewed { "skewed" } else { "uniform" },
            nclients as usize * target_per_client,
            nservers,
        ))?;

        eprintln!(" .. benchmark running @ {}", chrono::Local::now().time());
        for (i, mut chan) in voters.into_iter().enumerate() {
            let mut stdout = String::new();
            chan.read_to_string(&mut stdout)?;
            let mut stderr = String::new();
            chan.stderr().read_to_string(&mut stderr)?;

            chan.wait_eof()?;

            if chan.exit_status()? != 0 {
                eprintln!("{} failed to run benchmark client:", clients[i].public_dns);
                eprintln!("{}", stderr);
            } else if !stderr.is_empty() {
                eprintln!("{} reported:", clients[i].public_dns);
                let stderr = stderr.trim_end().replace('\n', "\n > ");
                eprintln!(" > {}", stderr);
            }

            // TODO: should we get histogram files here instead and merge them?
            outf.write_all(stdout.as_bytes())?;
        }

        // --------------------------------------------------------------------------------

        for (i, mut souplet) in souplets.into_iter().enumerate() {
            let killed = servers[i].ssh.as_ref().unwrap().just_exec(&[
                "pkill",
                "-f",
                "/home/ubuntu/target/release/souplet",
            ])?;

            if !killed.is_ok() {
                println!("souplet died");
            }

            let mut stdout = String::new();
            let mut stderr = String::new();
            souplet.stderr().read_to_string(&mut stderr)?;
            souplet.read_to_string(&mut stdout)?;
            let stdout = stdout.trim_end();
            if !stdout.is_empty() {
                println!("{}", stdout.replace('\n', "\n >> "));
            }
            let stderr = stderr.trim_end();
            if !stderr.is_empty() {
                println!("{}", stderr.replace('\n', "\n !> "));
            }

            souplet.wait_eof()?;
        }

        // also stop zookeeper
        servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(&["sudo", "systemctl", "stop", "zookeeper"])
            .context("stop zookeeper")?
            .map_err(failure::err_msg)?;

        Ok(())
    })
    .unwrap();
}

fn ec2_instance_type_cores(it: &str) -> Option<u16> {
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

impl ConvenientSession for tsunami::Session {
    fn exec(&self, cmd: &[&str]) -> Result<ssh2::Channel, Error> {
        let cmd: Vec<_> = cmd
            .iter()
            .map(|&arg| match arg {
                "&&" | "<" | ">" | "2>" | "2>&1" | "|" => arg.to_string(),
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
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<String, String>, Error> {
        let mut c = self.exec(cmd)?;

        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        let mut stderr = String::new();
        c.stderr().read_to_string(&mut stderr)?;
        c.wait_eof()?;

        if c.exit_status()? != 0 {
            return Ok(Err(stderr));
        }
        Ok(Ok(stdout))
    }
}

trait ConvenientSession {
    fn exec(&self, cmd: &[&str]) -> Result<ssh2::Channel, Error>;
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<String, String>, Error>;
}
