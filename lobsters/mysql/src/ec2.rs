extern crate chrono;
extern crate clap;
extern crate rusoto_core;
extern crate rusoto_sts;
extern crate tsunami;

use clap::{App, Arg};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use tsunami::*;

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
        MachineSetup::new("m5.2xlarge", "ami-8178a0fc", |ssh| {
            eprintln!("==> priming ramdisk");
            ssh.cmd("./reprime-ramdisk.sh")
                .map(|out| {
                    let out = out.trim_right();
                    if !out.is_empty() {
                        eprintln!(" -> primed\n{}", out);
                    }
                })
                .map_err(|e| {
                    eprintln!(" -> failed:\n{:?}", e);
                    e
                })
        }).as_user("ubuntu"),
    );
    b.add_set(
        "trawler",
        1,
        MachineSetup::new("c5.4xlarge", "ami-44954e39", |ssh| {
            eprintln!("==> setting up trawler");
            eprintln!(" -> git update");
            ssh.cmd("git -C benchmarks pull").map(|out| {
                let out = out.trim_right();
                if !out.is_empty() && !out.contains("Already up-to-date.") {
                    eprintln!("{}", out);
                }
            })?;

            eprintln!(" -> rebuild");
            ssh.cmd("cd benchmarks/lobsters/mysql && cargo b --release --bin trawler-mysql 2>&1")
                .map(|out| {
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

    let scales: Box<Iterator<Item = usize>> = args.values_of("SCALE")
        .map(|it| Box::new(it.map(|s| s.parse().unwrap())) as Box<_>)
        .unwrap_or(Box::new(
            [1usize, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600]
                .into_iter()
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
            eprintln!("==> benchmark w/ {}x load", scale);

            if scale != 1 {
                // need to re-prime server
                eprintln!(" -> repriming db");
                server
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("./reprime-ramdisk.sh")
                    .map(|out| {
                        let out = out.trim_right();
                        if !out.is_empty() {
                            eprintln!(" -> reprimed db...\n{}", out);
                        }
                    })?;
            }

            eprintln!(" -> started at {}", Local::now().time().format("%H:%M:%S"));

            let mut output = File::create(format!("lobsters-mysql-{}.log", scale))?;
            trawler
                .ssh
                .as_mut()
                .unwrap()
                .cmd_raw(&format!(
                    "timeout 7m benchmarks/lobsters/mysql/target/release/trawler-mysql \
                     --reqscale {} \
                     --warmup 60 \
                     --runtime 120 \
                     --issuers 15 \
                     --histogram lobsters-mysql-{}.hist \
                     \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\"",
                    scale, scale, server.private_ip,
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

            let mut hist = File::create(format!("lobsters-mysql-{}.hist", scale))?;
            trawler
                .ssh
                .as_mut()
                .unwrap()
                .cmd_raw(&format!("cat lobsters-mysql-{}.hist", scale))
                .and_then(|out| Ok(hist.write_all(&out[..]).map(|_| ())?))?;
        }

        Ok(())
    }).unwrap();
}
