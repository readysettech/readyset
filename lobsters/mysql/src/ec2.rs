extern crate chrono;
extern crate clap;
extern crate failure;
extern crate rusoto_core;
extern crate rusoto_sts;
extern crate tsunami;

use clap::{App, Arg};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::{fmt, thread, time};
use tsunami::*;

const AMI: &str = "ami-2cbf1953";

enum Backend {
    Mysql,
    Soup,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Backend::Mysql => write!(f, "mysql"),
            Backend::Soup => write!(f, "soup"),
        }
    }
}

fn git_and_cargo(ssh: &mut Session, dir: &str, bin: &str) -> Result<(), failure::Error> {
    /*
    eprintln!(" -> git update");
    ssh.cmd(&format!("git -C {} pull", dir)).map(|out| {
        let out = out.trim_right();
        if !out.is_empty() && !out.contains("Already up-to-date.") {
            eprintln!("{}", out);
        }
    })?;
    */

    eprintln!(" -> rebuild");
    ssh.cmd(&format!(
        "sh -c 'cd {} && cargo b --release --bin {} 2>&1'",
        dir, bin
    )).map(|out| {
            let out = out.trim_right();
            if !out.is_empty() {
                eprintln!("{}", out);
            }
        })
        .map_err(|e| {
            eprintln!(" -> rebuild failed!\n{:?}", e);
            e
        })?;

    Ok(())
}

fn main() {
    let args = App::new("trawler-mysql ec2 orchestrator")
        .about("Run the MySQL trawler benchmark on ec2")
        .arg(
            Arg::with_name("SCALE")
                .help("Run the given scale(s).")
                .multiple(true),
        )
        .get_matches();

    let mut b = TsunamiBuilder::default();
    b.use_term_logger();
    b.add_set(
        "server",
        1,
        MachineSetup::new("c5.4xlarge", AMI, |ssh| {
            eprintln!("==> setting up souplet");
            git_and_cargo(ssh, "distributary", "souplet")?;
            eprintln!("==> setting up zk-util");
            git_and_cargo(ssh, "distributary/consensus", "zk-util")?;
            Ok(())
        }).as_user("ubuntu"),
    );
    b.add_set(
        "trawler",
        1,
        MachineSetup::new("c5.9xlarge", AMI, |ssh| {
            eprintln!("==> setting up trawler");
            git_and_cargo(ssh, "benchmarks/lobsters/mysql", "trawler-mysql")?;
            eprintln!("==> setting up trawler w/ soup hacks");
            git_and_cargo(ssh, "benchmarks-soup/lobsters/mysql", "trawler-mysql")?;
            eprintln!("==> setting up shim");
            git_and_cargo(ssh, "shim", "distributary-mysql")?;
            Ok(())
        }).as_user("ubuntu"),
    );

    // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    let sts = rusoto_sts::StsClient::new(
        rusoto_core::default_tls_client().unwrap(),
        rusoto_core::EnvironmentProvider,
        rusoto_core::Region::UsEast1,
    );
    let provider = rusoto_sts::StsAssumeRoleSessionCredentialsProvider::new(
        sts,
        "arn:aws:sts::125163634912:role/soup".to_owned(),
        "vote-benchmark".to_owned(),
        None,
        None,
        None,
        None,
    );

    b.set_max_duration(2);
    b.set_region(rusoto_core::Region::UsEast1);
    b.wait_limit(time::Duration::from_secs(20));

    let scales: Box<Iterator<Item = usize>> = args.values_of("SCALE")
        .map(|it| Box::new(it.map(|s| s.parse().unwrap())) as Box<_>)
        .unwrap_or(Box::new(
            [
                1usize, 100, 200, 400, 600, 800, 850, 900, 950, 1000, 1050, 1100, 1200, 1400, 1600
            ].into_iter()
                .map(|&s| s),
        ) as Box<_>);

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
        f.write_all(b"# reqscale sload1 sload5 cload1 cload5\n")
            .unwrap();
        f
    };
    b.run_as(provider, |mut vms: HashMap<String, Vec<Machine>>| {
        use chrono::prelude::*;

        let mut server = vms.remove("server").unwrap().swap_remove(0);
        let mut trawler = vms.remove("trawler").unwrap().swap_remove(0);

        for scale in scales {
            for backend in &[Backend::Mysql, Backend::Soup] {
                eprintln!("==> benchmark {} w/ {}x load", backend, scale);

                match backend {
                    Backend::Mysql => {
                        let ssh = server.ssh.as_mut().unwrap();
                        ssh.cmd("sudo mount -t tmpfs -o size=16G tmpfs /mnt")?;
                        // sudo rm -rf /var/lib/mysql
                        ssh.cmd("sudo cp -r /var/lib/mysql.clean /mnt/mysql")?;
                        // sudo ln -s /mnt/mysql /var/lib/mysql
                        ssh.cmd("sudo chown -R mysql:mysql /var/lib/mysql/")?;
                    }
                    Backend::Soup => {
                        // XXX: also delete log files if we later run with RocksDB?
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd(
                                "distributary/target/release/zk-util \
                                 --clean --deployment trawler",
                            )
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> wiped soup state...\n{}", out);
                                }
                            })?;

                        // Don't hit Soup listening timeout think
                        thread::sleep(time::Duration::from_secs(10));
                    }
                }

                // start server again
                match backend {
                    Backend::Mysql => server
                        .ssh
                        .as_mut()
                        .unwrap()
                        .cmd("sh -c 'sudo systemctl start mysql 2>&1'")
                        .map(|out| {
                            let out = out.trim_right();
                            if !out.is_empty() {
                                eprintln!(" -> started mysql...\n{}", out);
                            }
                        })?,
                    Backend::Soup => {
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd(&format!(
                                "sh -c 'distributary/target/release/souplet \
                                 --deployment trawler \
                                 --durability memory \
                                 --address {} \
                                 --readers 14 -w 2 \
                                 --shards 0 \
                                 &'",
                                server.private_ip,
                            ))
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> soup didn't start...\n{}", out);
                                }
                            })?;

                        // start the shim (which will block until soup is available)
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd(&format!(
                                "sh -c 'shim/target/release/distributary-mysql \
                                 --deployment trawler \
                                 -z {}:2181 \
                                 -p 3306 \
                                 &'",
                                server.private_ip,
                            ))
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> shim didn't start...\n{}", out);
                                }
                            })?;

                        // give soup a chance to start
                        thread::sleep(time::Duration::from_secs(5));
                    }
                }

                // run priming
                // XXX: with MySQL we *could* just reprime by copying over the old ramdisk again
                eprintln!(
                    " -> repriming at {}",
                    Local::now().time().format("%H:%M:%S")
                );

                let dir = match backend {
                    Backend::Mysql => "benchmarks",
                    Backend::Soup => "benchmarks-soup",
                };

                let ip = match backend {
                    Backend::Mysql => &*server.private_ip,
                    Backend::Soup => "127.0.0.1",
                };

                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd(&format!(
                        "{}/lobsters/mysql/target/release/trawler-mysql \
                         --warmup 0 \
                         --runtime 0 \
                         --issuers 15 \
                         --prime \
                         \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\"",
                        dir, ip
                    ))
                    .map(|out| {
                        let out = out.trim_right();
                        if !out.is_empty() {
                            eprintln!(" -> reprime finished...\n{}", out);
                        }
                    })?;

                eprintln!(" -> started at {}", Local::now().time().format("%H:%M:%S"));

                let mut output = File::create(format!("lobsters-{}-{}.log", backend, scale))?;
                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd_raw(&format!(
                        "{}/lobsters/mysql/target/release/trawler-mysql \
                         --reqscale {} \
                         --warmup 60 \
                         --runtime 30 \
                         --issuers 15 \
                         --histogram lobsters-mysql-{}.hist \
                         \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\"",
                        dir, scale, scale, ip
                    ))
                    .and_then(|out| Ok(output.write_all(&out[..]).map(|_| ())?))?;

                eprintln!(" -> finished at {}", Local::now().time().format("%H:%M:%S"));

                // gather server load
                let sload = server
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("awk '{print $1\" \"$2}' /proc/loadavg")?;
                let sload = sload.trim_right();

                // gather client load
                let cload = trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("awk '{print $1\" \"$2}' /proc/loadavg")?;
                let cload = cload.trim_right();

                load.write_all(format!("{} ", scale).as_bytes())?;
                load.write_all(sload.as_bytes())?;
                load.write_all(b" ")?;
                load.write_all(cload.as_bytes())?;
                load.write_all(b"\n")?;

                let mut hist = File::create(format!("lobsters-{}-{}.hist", backend, scale))?;
                trawler
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd_raw(&format!("cat lobsters-{}-{}.hist", backend, scale))
                    .and_then(|out| Ok(hist.write_all(&out[..]).map(|_| ())?))?;

                // stop old server
                match backend {
                    Backend::Mysql => {
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("sh -c 'sudo systemctl stop mysql 2>&1'")
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped mysql...\n{}", out);
                                }
                            })?;
                        server.ssh.as_mut().unwrap().cmd("sudo umount /mnt")?;
                    }
                    Backend::Soup => {
                        server
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("sh -c 'pkill -af souplet 2>&1'")
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped soup...\n{}", out);
                                }
                            })?;
                        trawler
                            .ssh
                            .as_mut()
                            .unwrap()
                            .cmd("sh -c 'pkill -af distributary-mysql 2>&1'")
                            .map(|out| {
                                let out = out.trim_right();
                                if !out.is_empty() {
                                    eprintln!(" -> stopped shim...\n{}", out);
                                }
                            })?;

                        // give it some time
                        thread::sleep(time::Duration::from_secs(2));
                    }
                }

                // TODO: stop iterating through scales for this backend if it's not keeping up
            }
        }

        Ok(())
    }).unwrap();
}
