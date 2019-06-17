#![deny(unused_extern_crates)]

#[macro_use]
extern crate clap;

use failure::Error;
use failure::ResultExt;
use rusoto_core::Region;
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::borrow::Cow;
use std::fs::File;
use std::io::prelude::*;
use std::time;

const SOUP_AMI: &str = "ami-05df93bcec8de09d8";

fn main() {
    use clap::{App, Arg};

    let args = App::new("eintof-distributed")
        .version("0.1")
        .about("Orchestrate runs of the distributed eintopf benchmark")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .takes_value(true)
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("availability_zone")
                .long("availability-zone")
                .value_name("AZ")
                .default_value("us-east-1a")
                .takes_value(true)
                .help("EC2 availability zone to use for launching instances"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("60")
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
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
                .help("Instance type for servers"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .default_value("12")
                .takes_value(true)
                .help("Number of shards per souplet"),
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
            Arg::with_name("target")
                .long("load-per-client")
                .required(true)
                .default_value("5000000")
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
        .num_threads(100)
        .build_global()
        .unwrap();

    let nservers = value_t_or_exit!(args, "servers", u32);
    for scale in args.values_of("scales").unwrap() {
        match scale.parse::<u32>() {
            Ok(scale) => {
                eprintln!("==> {} servers", nservers * scale,);

                let load = run_one(&args, nservers * scale).unwrap();
                if load > 16.5 {
                    eprintln!(" !> server seems overloaded: {}/16", load);
                // break;
                } else {
                    eprintln!(" >> finished; registered load: {}/16", load);
                }
            }
            Err(e) => eprintln!("Ignoring malformed scale factor {}", e),
        }

        if !running.load(Ordering::SeqCst) {
            // user pressed ^C
            break;
        }
    }
}

fn run_one(args: &clap::ArgMatches, nservers: u32) -> Result<f64, failure::Error> {
    let runtime = value_t_or_exit!(args, "runtime", usize);
    let skewed = args.value_of("distribution").unwrap() == "skewed";
    let articles = value_t_or_exit!(args, "articles", usize);
    let shards = value_t_or_exit!(args, "shards", u16);
    let target_per_client = value_t_or_exit!(args, "target", usize);
    let az = args.value_of("availability_zone").unwrap();

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
    b.set_availability_zone(az);
    b.use_term_logger();
    b.add_set(
        "server",
        nservers,
        tsunami::MachineSetup::new(args.value_of("stype").unwrap(), SOUP_AMI, move |host| {
            eprintln!(" -> building eintopf on server");
            host.just_exec(&["git", "-C", "eintopf", "reset", "--hard", "2>&1"])
                .context("git reset")?
                .map_err(failure::err_msg)?;
            host.just_exec(&["git", "-C", "eintopf", "checkout", "dataplane", "2>&1"])
                .context("git checkout")?
                .map_err(failure::err_msg)?;
            host.just_exec(&["git", "-C", "eintopf", "pull", "2>&1"])
                .context("git pull")?
                .map_err(failure::err_msg)?;
            host.just_exec(&["cd", "eintopf", "&&", "cargo", "b", "--release"])
                .context("build")?
                .map_err(failure::err_msg)?;
            Ok(())
        })
        .as_user("ubuntu"),
    );

    b.wait_limit(time::Duration::from_secs(5 * 60));
    b.set_max_duration(1);
    b.run_as(provider, |mut hosts| {
        let servers = hosts.remove("server").unwrap();

        // write out hosts files
        let hosts_file = servers
            .iter()
            .map(|s| format!("{}:1234", s.private_ip))
            .collect::<Vec<_>>()
            .join("\n");
        for s in &servers {
            let mut c = s.ssh.as_ref().unwrap().exec(&["cat", ">", "hosts"])?;
            c.write_all(hosts_file.as_bytes())?;
            c.flush()?;
        }

        let eintopfs: Result<Vec<_>, _> = servers
            .iter()
            .enumerate()
            .map(|(i, s)| {
                eprintln!(" -> starting eintopf on {}", s.public_dns);
                let cmd: Vec<Cow<_>> = vec![
                    "env".into(),
                    "RUST_BACKTRACE=1".into(),
                    "/home/ubuntu/target/release/eintopf".into(),
                    "--open-loop".into(),
                    "-b".into(),
                    format!("{}", nservers as usize * target_per_client).into(),
                    "--workers".into(),
                    format!("{}", shards).into(),
                    "-a".into(),
                    format!("{}", articles).into(),
                    "-r".into(),
                    format!("{}", runtime).into(),
                    "-d".into(),
                    if skewed { "zipf:1.08" } else { "uniform" }.into(),
                    "-h".into(),
                    "hosts".into(),
                    "-p".into(),
                    format!("{}", i).into(),
                ];
                let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                s.ssh.as_ref().unwrap().exec(&cmd[..])
            })
            .collect();
        let eintopfs = eintopfs?;

        // let's see how we did
        let mut outf = File::create(&format!(
            "eintopf-12s.{}.{}t.{}h.log",
            if skewed { "skewed" } else { "uniform" },
            nservers as usize * target_per_client,
            nservers,
        ))?;

        eprintln!(" .. benchmark running @ {}", chrono::Local::now().time());
        for (i, mut chan) in eintopfs.into_iter().enumerate() {
            let mut stdout = String::new();
            chan.read_to_string(&mut stdout)?;
            let mut stderr = String::new();
            chan.stderr().read_to_string(&mut stderr)?;

            chan.wait_eof()?;
            chan.wait_close()?;

            if chan.exit_status()? != 0 {
                eprintln!("{} failed to run benchmark client:", servers[i].public_dns);
                eprintln!("{}", stderr);
            } else if !stderr.is_empty() {
                eprintln!("{} reported:", servers[i].public_dns);
                let stderr = stderr.trim_end().replace('\n', "\n > ");
                eprintln!(" > {}", stderr);
            }

            outf.write_all(stdout.as_bytes())?;
        }

        // gather server load
        let load = servers[0]
            .ssh
            .as_ref()
            .unwrap()
            .cmd("awk '{print $1\" \"$2}' /proc/loadavg")?;
        // stop iterating through scales for this backend if it's not keeping up
        let load: f64 = load
            .trim_end()
            .split_whitespace()
            .next()
            .and_then(|l| l.parse().ok())
            .unwrap_or(0.0);

        Ok(load)
    })
}

impl ConvenientSession for tsunami::Session {
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error> {
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
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error>;
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<String, String>, Error>;
}
