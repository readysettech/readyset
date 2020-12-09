extern crate clap;
extern crate noria;
extern crate serde_json;
extern crate zookeeper;

use noria::consensus::{CONTROLLER_KEY, STATE_KEY};
use serde_json::Value;
use std::process;
use std::time::Duration;

use zookeeper::{KeeperState, WatchedEvent, Watcher, ZkError, ZooKeeper};

struct EventWatcher;
impl Watcher for EventWatcher {
    fn handle(&self, e: WatchedEvent) {
        if e.keeper_state != KeeperState::SyncConnected {
            eprintln!("Lost connection to ZooKeeper! Aborting");
            process::abort();
        }
    }
}

fn main() {
    use clap::{App, Arg};
    let matches = App::new("zkUtil")
        .version("0.0.1")
        .about("Soup Zookeeper utility. Dumps and optionally cleans configuration stored in Zk.")
        .arg(
            Arg::with_name("zookeeper")
                .short("z")
                .long("zookeeper")
                .takes_value(true)
                .default_value("127.0.0.1:2181")
                .help("Zookeeper connection info."),
        )
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .short("d")
                .required(true)
                .takes_value(true)
                .help("Soup deployment ID."),
        )
        .arg(
            Arg::with_name("clean")
                .short("c")
                .long("clean")
                .takes_value(false)
                .required_unless("show")
                .help("Remove existing configuration."),
        )
        .arg(
            Arg::with_name("show")
                .short("s")
                .long("show")
                .takes_value(false)
                .required_unless("clean")
                .help("Print current configuration to stdout."),
        )
        .get_matches();

    let deployment = matches.value_of("deployment").unwrap();
    let zookeeper_addr = format!("{}/{}", matches.value_of("zookeeper").unwrap(), deployment);
    let clean = matches.is_present("clean");
    let dump = matches.is_present("show");

    let zk = ZooKeeper::connect(&zookeeper_addr, Duration::from_secs(1), EventWatcher).unwrap();

    if dump {
        let (ref current_ctrl, ref _stat) = match zk.get_data(CONTROLLER_KEY, false) {
            Ok(data) => data,
            Err(e) => match e {
                ZkError::NoNode => {
                    println!("no current Soup controller in Zookeeper!");
                    return;
                }
                _ => panic!("{:?}", e),
            },
        };

        let (ref current_data, ref _stat) = match zk.get_data(STATE_KEY, false) {
            Ok(data) => data,
            Err(e) => match e {
                ZkError::NoNode => {
                    println!("no current Soup configuration in Zookeeper!");
                    return;
                }
                _ => panic!("{:?}", e),
            },
        };

        let controller: Value = serde_json::from_slice(current_ctrl).unwrap();
        println!(
            "Current Soup controller in Zookeeper:\n{}\n\n",
            serde_json::to_string_pretty(&controller).unwrap()
        );

        let state: Value = serde_json::from_slice(current_data).unwrap();
        println!(
            "Current Soup configuration in Zookeeper:\n{}",
            serde_json::to_string_pretty(&state).unwrap()
        );
    }

    if clean {
        match zk.delete(CONTROLLER_KEY, None) {
            // any version
            Ok(_) => println!("Controller configuration cleaned."),
            Err(e) => println!("Failed to clean: {:?}", e),
        }

        match zk.delete(STATE_KEY, None) {
            // any version
            Ok(_) => println!("State cleaned."),
            Err(e) => println!("Failed to clean: {:?}", e),
        }
    }
}
