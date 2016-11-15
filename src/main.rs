#[macro_use]
extern crate log;
extern crate env_logger;

extern crate afterparty;
extern crate clap;
extern crate lettre;
extern crate git2;
extern crate hyper;
extern crate regex;
extern crate slack_hook;
extern crate toml;

mod auth;
mod config;
mod email;
mod repo;
mod slack;
mod taste;

use afterparty::{Delivery, Event, Hub};
use hyper::Server;
use std::path::Path;
use std::sync::Mutex;

#[cfg_attr(rustfmt, rustfmt_skip)]
const TASTER_USAGE: &'static str = "\
EXAMPLES:
  taste -w /path/to/workdir
  taste -l 0.0.0.0:1234 -w /path/to/workdir";

pub fn main() {
    use clap::{Arg, App};

    env_logger::init().unwrap();

    let args = App::new("taster")
        .version("0.0.1")
        .about("Tastes Soup commits.")
        .arg(Arg::with_name("listen_addr")
            .short("l")
            .long("listen_addr")
            .takes_value(true)
            .value_name("IP:PORT")
            .default_value("0.0.0.0:4567")
            .help("Listen address and port for webhook delivery"))
        .arg(Arg::with_name("github_repo")
            .short("r")
            .long("github_repo")
            .takes_value(true)
            .required(true)
            .value_name("GH_REPO")
            .default_value("https://github.com/ms705/taster")
            .help("GitHub repository to taste"))
        .arg(Arg::with_name("email_addr")
            .long("email_addr")
            .takes_value(true)
            .required(false)
            .help("Email address to send notifications to"))
        .arg(Arg::with_name("slack_hook_url")
            .long("slack_hook_url")
            .takes_value(true)
            .required(false)
            .help("Slack webhook URL to push notifications to"))
        .arg(Arg::with_name("slack_channel")
            .long("slack_channel")
            .takes_value(true)
            .required(false)
            .default_value("#soup-test")
            .help("Slack channel for notifications"))
        .arg(Arg::with_name("workdir")
            .short("w")
            .long("workdir")
            .takes_value(true)
            .required(true)
            .value_name("REPO_DIR")
            .help("Directory holding the workspace repo"))
        .after_help(TASTER_USAGE)
        .get_matches();

    let addr = args.value_of("listen_addr").unwrap();
    let email_notification_addr = args.value_of("email_addr");
    let repo = args.value_of("github_repo").unwrap();
    let slack_hook_url = args.value_of("slack_hook_url");
    let slack_channel = args.value_of("slack_channel");
    let workdir = Path::new(args.value_of("workdir").unwrap());

    let wsl = Mutex::new(repo::Workspace::new(repo, workdir));
    let en = if let Some(addr) = email_notification_addr {
        Some(email::EmailNotifier::new(addr, repo))
    } else {
        None
    };
    let sn = if let Some(url) = slack_hook_url {
        Some(slack::SlackNotifier::new(url, slack_channel.unwrap(), repo))
    } else {
        None
    };

    let mut hub = Hub::new();
    hub.handle("push", move |delivery: &Delivery| {
        match delivery.payload {
            Event::Push { ref commits, ref pusher, .. } => {
                println!("Handling {} commits pushed by {}",
                         commits.len(),
                         pusher.name);
                {
                    let ws = wsl.lock().unwrap();
                    ws.fetch().unwrap();
                }
                for c in commits.iter() {
                    // taste
                    let res = taste::taste_commit(&wsl, &c.id, &c.message);

                    // email notification
                    if en.is_some() {
                        en.as_ref().unwrap().notify(&res).unwrap();
                    }
                    // slack notification
                    if sn.is_some() {
                        sn.as_ref().unwrap().notify(&res).unwrap();
                    }
                }
            }
            _ => (),
        }
    });

    let srvc = Server::http(&addr[..])
        .unwrap()
        .handle(hub);

    println!("Taster listening on {}", addr);
    srvc.unwrap();
}
