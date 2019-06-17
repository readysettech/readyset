#![feature(try_blocks)]

#[macro_use]
extern crate clap;

use clap::{App, Arg};
use rusoto_core::Region;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::{self, prelude::*};
use std::{fmt, thread, time};
use tsunami::*;

const AMI: &str = "ami-04db1e82afa245def";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Backend {
    Mysql,
    Noria,
    Natural,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Backend::Mysql => write!(f, "mysql"),
            Backend::Noria => write!(f, "noria"),
            Backend::Natural => write!(f, "natural"),
        }
    }
}

impl Backend {
    fn queries(&self) -> &'static str {
        match *self {
            Backend::Mysql => "original",
            Backend::Noria => "noria",
            Backend::Natural => "natural",
        }
    }
}

fn git_and_cargo(
    ssh: &mut Session,
    dir: &str,
    bin: &str,
    branch: Option<&str>,
) -> Result<(), failure::Error> {
    eprintln!(" -> git reset");
    ssh.cmd(&format!("bash -c 'git -C {} reset --hard 2>&1'", dir))
        .map(|out| {
            let out = out.trim_end();
            if !out.is_empty() && !out.contains("Already up-to-date.") {
                eprintln!("{}", out);
            }
        })?;

    if let Some(branch) = branch {
        eprintln!(" -> git checkout {}", branch);
        ssh.cmd(&format!(
            "bash -c 'git -C {} checkout {} 2>&1'",
            dir, branch
        ))
        .map(|out| {
            let out = out.trim_end();
            if !out.is_empty() {
                eprintln!("{}", out);
            }
        })?;
    }

    eprintln!(" -> git update");
    ssh.cmd(&format!("bash -c 'git -C {} pull 2>&1'", dir))
        .map(|out| {
            let out = out.trim_end();
            if !out.is_empty() && !out.contains("Already up-to-date.") {
                eprintln!("{}", out);
            }
        })?;

    if dir == "shim" {
        eprintln!(" -> force local noria");
        ssh.cmd(&format!("sed -i -e 's/^###//g' {}/Cargo.toml", dir))
            .map(|out| {
                let out = out.trim_end();
                if !out.is_empty() {
                    eprintln!("{}", out);
                }
            })?;
    }

    if !bin.is_empty() {
        let cmd = if dir.starts_with("benchmarks") {
            eprintln!(" -> rebuild (unshared)");
            format!(
                "bash -c 'cd {} && env -u CARGO_TARGET_DIR cargo b --release --bin {} 2>&1'",
                dir, bin
            )
        } else {
            eprintln!(" -> rebuild");
            format!(
                "bash -c 'cd {} && cargo b --release --bin {} 2>&1'",
                dir, bin
            )
        };
        ssh.cmd(&cmd)
            .map(|out| {
                let out = out.trim_end();
                if !out.is_empty() {
                    eprintln!("{}", out);
                }
            })
            .map_err(|e| {
                eprintln!(" -> rebuild failed!\n{:?}", e);
                e
            })?;
    }

    Ok(())
}

fn main() {
    let args = App::new("noria lobsters ec2 orchestrator")
        .about("Run the noria lobste.rs benchmark on ec2")
        .arg(
            Arg::with_name("memory_limit")
                .takes_value(true)
                .long("memory-limit")
                .help("Partial state size limit / eviction threshold [in bytes]."),
        )
        .arg(
            Arg::with_name("memscale")
                .takes_value(true)
                .default_value("1.0")
                .long("memscale")
                .help("Memory scale factor for workload"),
        )
        .arg(
            Arg::with_name("availability_zone")
                .long("availability-zone")
                .value_name("AZ")
                .default_value("us-east-1c")
                .takes_value(true)
                .help("EC2 availability zone to use for launching instances"),
        )
        .arg(
            Arg::with_name("branch")
                .takes_value(true)
                .default_value("master")
                .long("branch")
                .help("Which branch of noria to benchmark"),
        )
        .arg(
            Arg::with_name("SCALE")
                .help("Run the given scale(s).")
                .multiple(true),
        )
        .get_matches();

    let az = args.value_of("availability_zone").unwrap();

    let mut b = TsunamiBuilder::default();
    b.set_region(Region::UsEast1);
    b.set_availability_zone(az);
    b.use_term_logger();
    let branch = args.value_of("branch").map(String::from);
    b.add_set(
        "trawler",
        1,
        MachineSetup::new("m5.24xlarge", AMI, move |ssh| {
            eprintln!("==> setting up benchmark client");
            git_and_cargo(
                ssh,
                "noria",
                "lobsters",
                branch.as_ref().map(String::as_str),
            )?;
            eprintln!("==> setting up shim");
            git_and_cargo(ssh, "shim", "noria-mysql", None)?;
            Ok(())
        })
        .as_user("ubuntu"),
    );
    let branch = args.value_of("branch").map(String::from);
    b.add_set(
        "server",
        1,
        MachineSetup::new("c5d.4xlarge", AMI, move |ssh| {
            eprintln!("==> setting up noria-server");
            git_and_cargo(
                ssh,
                "noria",
                "noria-server",
                branch.as_ref().map(String::as_str),
            )?;
            eprintln!("==> setting up noria-zk");
            git_and_cargo(
                ssh,
                "noria",
                "noria-zk",
                branch.as_ref().map(String::as_str),
            )?;
            // we'll need zookeeper running
            ssh.cmd("sudo systemctl start zookeeper")?;

            Ok(())
        })
        .as_user("ubuntu"),
    );

    // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    //let sts = rusoto_sts::StsClient::new(rusoto_core::Region::EuCentral1);
    let sts = rusoto_sts::StsClient::new(rusoto_core::Region::UsEast1);
    let provider = rusoto_sts::StsAssumeRoleSessionCredentialsProvider::new(
        sts,
        "arn:aws:sts::125163634912:role/soup".to_owned(),
        "vote-benchmark".to_owned(),
        None,
        None,
        None,
        None,
    );

    b.set_max_duration(4);
    //b.set_region(rusoto_core::Region::EuCentral1);
    b.set_region(rusoto_core::Region::UsEast1);
    b.set_availability_zone("us-east-1a");
    b.wait_limit(time::Duration::from_secs(60));

    let scales = args
        .values_of("SCALE")
        .map(|it| it.map(|s| s.parse().unwrap()).collect())
        .unwrap_or_else(|| {
            vec![
                100usize, 500, 1000, 1250, 1500, 1750, 2000, 2500, 2750, 3000, 3500, 4500, 5500,
                6500, 7000, 7500, 8000, 8500, 9000, 9500, 10_000,
            ]
        });

    let memscale = value_t_or_exit!(args, "memscale", f64);
    let memlimit = args.value_of("memory_limit");

    let mut load = if args.is_present("SCALE") {
        OpenOptions::new()
            .write(true)
            .truncate(false)
            .append(true)
            .create(true)
            .open("load.log")
            .unwrap()
    } else {
        let mut f = File::create("load.log").unwrap();
        f.write_all(b"#reqscale backend sload1 sload5 cload1 cload5\n")
            .unwrap();
        f
    };
    b.run_as(provider, |mut vms: HashMap<String, Vec<Machine>>| {
        use chrono::prelude::*;

        let mut server = vms.remove("server").unwrap().swap_remove(0);
        let mut trawler = vms.remove("trawler").unwrap().swap_remove(0);

        // write out host files for ergonomic ssh
        let r: io::Result<()> = try {
            let mut f = File::create("server.host")?;
            writeln!(f, "ubuntu@{}", server.public_dns)?;

            let mut f = File::create("client.host")?;
            writeln!(f, "ubuntu@{}", trawler.public_dns)?;
        };

        if let Err(e) = r {
            eprintln!("failed to write out host files: {:?}", e);
        }

        let backends = [Backend::Mysql, Backend::Noria, Backend::Natural];

        // allow reuse of time-wait ports
        trawler
            .ssh
            .as_mut()
            .unwrap()
            .cmd("bash -c 'echo 1 | sudo tee /proc/sys/net/ipv4/tcp_tw_reuse'")?;

        for backend in &backends {
            let mut survived_last = true;
            for &scale in &scales {
                if !survived_last {
                    break;
                }

                eprintln!("==> benchmark {} w/ {}x load", backend, scale);

                match backend {
                    Backend::Mysql => {
                        let ssh = server.ssh.as_mut().unwrap();
                        ssh.cmd("sudo mount -t tmpfs -o size=16G tmpfs /mnt")?;
                        ssh.cmd("sudo cp -r /var/lib/mysql.clean /mnt/mysql")?;
                        ssh.cmd("sudo rm -rf /var/lib/mysql")?;
                        ssh.cmd("sudo ln -sfn /mnt/mysql /var/lib/mysql")?;
                        ssh.cmd("sudo chown -R mysql:mysql /var/lib/mysql/")?;
                    }
                    Backend::Noria | Backend::Natural => {
                        // just to make totally sure
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("bash -c 'pkill -9 -f noria-server 2>&1'")
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> force stopped noria...\n{}", out);
                                }
                            })?;
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("bash -c 'pkill -9 -f noria-mysql 2>&1'")
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> force stopped shim...\n{}", out);
                                }
                            })?;

                        // XXX: also delete log files if we later run with RocksDB?
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd(
                                "~/target/release/noria-zk \
                                 --clean --deployment trawler",
                            )
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> wiped noria state...\n{}", out);
                                }
                            })?;

                        // Don't hit Noria listening timeout think
                        thread::sleep(time::Duration::from_secs(10));
                    }
                }

                // start server again
                match backend {
                    Backend::Mysql => server
                        .ssh
                        .as_mut()
                        .unwrap()
                        .cmd("bash -c 'sudo systemctl start mariadb 2>&1'")
                        .map(|out| {
                            let out = out.trim_end();
                            if !out.is_empty() {
                                eprintln!(" -> started mysql...\n{}", out);
                            }
                        })?,
                    Backend::Noria | Backend::Natural => {
                        let mut cmd = format!(
                            "bash -c 'nohup \
                             env \
                             RUST_BACKTRACE=1 \
                             ~/target/release/noria-server \
                             --deployment trawler \
                             --durability memory \
                             --no-reuse \
                             --address {} \
                             --shards 0 ",
                            server.private_ip
                        );
                        if let Some(memlimit) = memlimit {
                            cmd.push_str(&format!("--memory {} ", memlimit));
                        }
                        cmd.push_str(" &> noria-server.log &'");

                        server.ssh.as_mut().unwrap().cmd(&cmd).map(|_| ())?;

                        // start the shim (which will block until noria is available)
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd(&format!(
                                "bash -c 'nohup \
                                 env RUST_BACKTRACE=1 \
                                 ~/target/release/noria-mysql \
                                 --deployment trawler \
                                 --no-sanitize --no-static-responses \
                                 -z {}:2181 \
                                 -p 3306 \
                                 &> shim.log &'",
                                server.private_ip,
                            ))
                            .map(|_| ())?;

                        // give noria a chance to start
                        thread::sleep(time::Duration::from_secs(5));
                    }
                }

                // run priming
                // XXX: with MySQL we *could* just reprime by copying over the old ramdisk again
                eprintln!(" -> priming at {}", Local::now().time().format("%H:%M:%S"));

                let ip = match backend {
                    Backend::Mysql => &*server.private_ip,
                    Backend::Noria | Backend::Natural => "127.0.0.1",
                };

                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd(&format!(
                        "env RUST_BACKTRACE=1 \
                         ~/target/release/lobsters \
                         --memscale {} \
                         --warmup 0 \
                         --runtime 0 \
                         --issuers 24 \
                         --prime \
                         --queries {} \
                         \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\" \
                         2>&1",
                        memscale,
                        backend.queries(),
                        ip
                    ))
                    .map(|out| {
                        let out = out.trim_end();
                        if !out.is_empty() {
                            eprintln!(" -> priming finished...\n{}", out);
                        }
                    })?;

                eprintln!(" -> warming at {}", Local::now().time().format("%H:%M:%S"));

                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd(&format!(
                        "env RUST_BACKTRACE=1 \
                         ~/target/release/lobsters \
                         --reqscale 3000 \
                         --memscale {} \
                         --warmup 120 \
                         --runtime 0 \
                         --issuers 24 \
                         --queries {} \
                         \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\" \
                         2>&1",
                        memscale,
                        backend.queries(),
                        ip
                    ))
                    .map(|out| {
                        let out = out.trim_end();
                        if !out.is_empty() {
                            eprintln!(" -> warming finished...\n{}", out);
                        }
                    })?;

                eprintln!(" -> started at {}", Local::now().time().format("%H:%M:%S"));

                let prefix = format!("lobsters-{}-{}", backend, scale);
                let mut output = File::create(format!("{}.log", prefix))?;
                let hist_output = if let Some(memlimit) = memlimit {
                    format!(
                        "--histogram lobsters-{}-m{}-r{}-l{}.hist ",
                        backend, memscale, scale, memlimit
                    )
                } else {
                    format!(
                        "--histogram lobsters-{}-m{}-r{}-unlimited.hist ",
                        backend, memscale, scale
                    )
                };
                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd_raw(&format!(
                        "env RUST_BACKTRACE=1 \
                         ~/target/release/lobsters \
                         --reqscale {} \
                         --memscale {} \
                         --warmup 20 \
                         --runtime 30 \
                         --issuers 24 \
                         --queries {} \
                         {} \
                         \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\" \
                         2> run.err",
                        scale,
                        memscale,
                        backend.queries(),
                        hist_output,
                        ip
                    ))
                    .and_then(|out| Ok(output.write_all(&out[..]).map(|_| ())?))?;

                drop(output);
                eprintln!(" -> finished at {}", Local::now().time().format("%H:%M:%S"));

                // gather server load
                let sload = server
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("awk '{print $1\" \"$2}' /proc/loadavg")?;
                let sload = sload.trim_end();

                // gather client load
                let cload = trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("awk '{print $1\" \"$2}' /proc/loadavg")?;
                let cload = cload.trim_end();

                load.write_all(format!("{} {} ", scale, backend).as_bytes())?;
                load.write_all(sload.as_bytes())?;
                load.write_all(b" ")?;
                load.write_all(cload.as_bytes())?;
                load.write_all(b"\n")?;

                let mut hist = File::create(format!("{}.hist", prefix))?;
                let hist_cmd = if let Some(memlimit) = memlimit {
                    format!(
                        "cat lobsters-{}-m{}-r{}-l{}.hist",
                        backend, memscale, scale, memlimit
                    )
                } else {
                    format!(
                        "cat lobsters-{}-m{}-r{}-unlimited.hist",
                        backend, memscale, scale
                    )
                };
                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd_raw(&hist_cmd)
                    .and_then(|out| Ok(hist.write_all(&out[..]).map(|_| ())?))?;

                // stop old server
                match backend {
                    Backend::Mysql => {
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("bash -c 'sudo systemctl stop mariadb 2>&1'")
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped mysql...\n{}", out);
                                }
                            })?;
                        server.ssh.as_mut().unwrap().cmd("sudo umount /mnt")?;
                    }
                    Backend::Noria | Backend::Natural => {
                        // gather state size
                        let mem_limit = if let Some(limit) = memlimit {
                            format!("l{}", limit)
                        } else {
                            "unlimited".to_owned()
                        };
                        let mut sizefile = File::create(format!(
                            "lobsters-{}-m{}-r{}-{}.json",
                            backend, memscale, scale, mem_limit
                        ))?;
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd_raw(&format!(
                                "wget http://{}:9000/get_statistics",
                                server.private_ip
                            ))
                            .and_then(|out| Ok(sizefile.write_all(&out[..]).map(|_| ())?))?;

                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("bash -c 'pkill -f noria-server 2>&1'")
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped noria...\n{}", out);
                                }
                            })?;
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("bash -c 'pkill -f noria-mysql 2>&1'")
                            .map(|out| {
                                let out = out.trim_end();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped shim...\n{}", out);
                                }
                            })?;

                        // give it some time
                        thread::sleep(time::Duration::from_secs(2));
                    }
                }

                // stop iterating through scales for this backend if it's not keeping up
                let sload: f64 = sload
                    .split_whitespace()
                    .next()
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(0.0);
                let cload: f64 = cload
                    .split_whitespace()
                    .next()
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(0.0);

                eprintln!(" -> backend load: s: {}/16, c: {}/48", sload, cload);

                if sload > 16.5 {
                    eprintln!(" -> backend is probably not keeping up");
                }

                // also parse achived ops/s to check that we're *really* keeping up
                let log = File::open(format!("{}.log", prefix))?;
                let log = BufReader::new(log);
                let mut target = None;
                let mut actual = None;
                for line in log.lines() {
                    let line = line?;
                    if line.starts_with("# target ops/s") {
                        target = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>()?);
                    } else if line.starts_with("# achieved ops/s") {
                        actual = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>()?);
                    }
                    match (target, actual) {
                        (Some(target), Some(actual)) => {
                            eprintln!(" -> achieved {} ops/s (target: {})", actual, target);
                            if actual < target * 3.0 / 4.0 {
                                eprintln!(" -> backend is really not keeping up");
                                survived_last = false;
                            }
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    })
    .unwrap();
}
