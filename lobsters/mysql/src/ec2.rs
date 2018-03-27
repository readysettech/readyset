extern crate tsunami;

use tsunami::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

fn main() {
    let mut b = TsunamiBuilder::default();
    b.use_term_logger();
    b.add_set(
        "server",
        1,
        MachineSetup::new("c5.2xlarge", "ami-2705da5a", |ssh| {
            ssh.cmd("./reprime-ramdisk.sh").map(|out| {
                eprintln!("==> initial ramdisk setup: {}", out);
            })
        }),
    );
    b.add_set(
        "trawler",
        1,
        MachineSetup::new("c5.4xlarge", "ami-6a03dc17", |ssh| {
            ssh.cmd("git -C benchmarks pull").map(|out| {
                eprintln!("==> git update:\n{}", out);
            })?;
            ssh.cmd("cd benchmarks/lobsters/mysql && cargo b --release --bin trawler-mysql")
                .map(|out| {
                    eprintln!("==> rebuild:\n{}", out);
                })?;
            Ok(())
        }),
    );

    b.run(|mut vms: HashMap<String, Vec<Machine>>| {
        let mut server = vms.remove("server").unwrap().swap_remove(0);
        let mut trawler = vms.remove("trawler").unwrap().swap_remove(0);

        for &scale in [1, 100, 200, 400, 800, 1000, 1200, 1400, 1600, 1800, 2000].into_iter() {
            eprintln!("==> benchmark w/ {}x load", scale);

            if scale != 1 {
                // need to re-prime server
                server
                    .ssh
                    .as_mut()
                    .unwrap()
                    .cmd("./reprime-ramdisk.sh")
                    .map(|out| {
                        eprintln!(" -> reprime db... {}", out);
                    })?;
            }

            let mut output = File::create(format!("lobsters-mysql-{}.log", scale))?;
            trawler
                .ssh
                .as_mut()
                .unwrap()
                .cmd(&format!(
                    "benchmarks/lobsters/mysql/target/release/trawler-mysql \
                     --reqscale {} \
                     --warmup 60 \
                     --runtime 240 \
                     --issuers 15 \
                     \"mysql://lobsters:$(cat ~/mysql.pass)@{}/lobsters\"",
                    scale, server.private_ip,
                ))
                .and_then(|out| Ok(output.write_all(out.as_bytes()).map(|_| ())?))?;
        }

        Ok(())
    }).unwrap();
}
