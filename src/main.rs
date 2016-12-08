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
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[cfg_attr(rustfmt, rustfmt_skip)]
const TASTER_USAGE: &'static str = "\
EXAMPLES:
  taste -w /path/to/workdir -s my_secret
  taste -l 0.0.0.0:1234 -w /path/to/workdir -s my_secret";

pub fn main() {
    use clap::{Arg, App};

    env_logger::init().unwrap();

    let args = App::new("taster")
        .version("0.0.1")
        .about("Tastes GitHub commits.")
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
        .arg(Arg::with_name("secret")
            .short("s")
            .long("secret")
            .takes_value(true)
            .required(false)
            .help("GitHub webhook secret"))
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
        .arg(Arg::with_name("taste_commit")
            .long("taste_commit")
            .short("t")
            .takes_value(true)
            .required(false)
            .help("Do a one-off taste of a specific commit"))
        .arg(Arg::with_name("taste_head_only")
            .long("taste_head_only")
            .required(false)
            .help("When multiple commits are pushed, taste the head commit only"))
        .arg(Arg::with_name("verbose_notifications")
            .long("verbose_notifications")
            .required(false)
            .help("List all benchmarks in notifications even if the results have not changed \
                   significantly"))
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
    let secret = args.value_of("secret");
    let slack_hook_url = args.value_of("slack_hook_url");
    let slack_channel = args.value_of("slack_channel");
    let taste_commit = args.value_of("taste_commit");
    let taste_head_only = args.is_present("taste_head_only");
    let workdir = Path::new(args.value_of("workdir").unwrap());
    let verbose_notify = args.is_present("verbose_notifications");

    let mut history = HashMap::new();
    let ws = repo::Workspace::new(repo, workdir);
    let en = if let Some(addr) = email_notification_addr {
        Some(email::EmailNotifier::new(addr, repo))
    } else {
        None
    };
    let sn = if let Some(url) = slack_hook_url {
        Some(slack::SlackNotifier::new(url, slack_channel.unwrap(), repo, verbose_notify))
    } else {
        None
    };

    if taste_commit.is_some() {
        let cid = if let Some("HEAD") = taste_commit {
            let cid = ws.repo.head().unwrap().target().unwrap().clone();
            format!("{}", cid)
        } else {
            String::from(taste_commit.unwrap())
        };
        let res = taste::taste_commit(&ws, &mut history, "", &cid, &cid, "");
        match res {
            Err(e) => println!("ERROR: failed to taste{}: {}", cid, e),
            Ok(tr) => {
                // email notification
                if en.is_some() {
                    en.as_ref().unwrap().notify(&tr).unwrap();
                }
                // slack notification
                if sn.is_some() {
                    sn.as_ref().unwrap().notify(&tr).unwrap();
                }
                // We're done
                return;
            }
        }
    }

    // If we get here, we must be running in continuous mode
    if let None = secret {
        panic!("--secret must be set when in continuous webhook handler mode");
    }

    // Initialize history by tasting the HEAD commit of each branch
    {
        let branches = ws.branch_heads();
        for (b, c) in branches.iter() {
            println!("tasting HEAD of {}: {} / {}",
                     b,
                     c.id(),
                     c.message().unwrap());
            let res = taste::taste_commit(&ws,
                                          &mut history,
                                          &b,
                                          &format!("{}", c.id()),
                                          c.message().unwrap(),
                                          "");
            assert!(res.is_ok());
        }
    }

    let hl = Arc::new(Mutex::new(history));
    let wsl = Mutex::new(ws);

    let mut hub = Hub::new();
    hub.handle_authenticated("push", secret.unwrap(), move |delivery: &Delivery| {
        match delivery.payload {
            Event::Push { ref _ref, ref commits, ref head_commit, ref pusher, .. } => {
                let notify = |res: &taste::TastingResult| {
                    // email notification
                    if en.is_some() {
                        en.as_ref().unwrap().notify(&res).unwrap();
                    }
                    // slack notification
                    if sn.is_some() {
                        sn.as_ref().unwrap().notify(&res).unwrap();
                    }
                };
                println!("Handling {} commits pushed by {}",
                         commits.len(),
                         pusher.name);
                {
                    let ws = wsl.lock().unwrap();
                    let mut history = hl.lock().unwrap();
                    // First taste the head commit
                    ws.fetch().unwrap();
                    let head_res = taste::taste_commit(&ws,
                                                       &mut history,
                                                       &_ref,
                                                       &head_commit.id,
                                                       &head_commit.message,
                                                       &head_commit.url);
                    match head_res {
                        Err(e) => {
                            println!("ERROR: failed to taste HEAD commit {}: {}",
                                     head_commit.id,
                                     e)
                        }
                        Ok(tr) => {
                            notify(&tr);
                            // Taste others if needed
                            if !taste_head_only || !tr.build || !tr.bench {
                                for c in commits.iter() {
                                    if c.id == head_commit.id {
                                        // skip HEAD as we've already tested it
                                        continue;
                                    }
                                    // taste
                                    let res = taste::taste_commit(&ws,
                                                                  &mut history,
                                                                  &_ref,
                                                                  &c.id,
                                                                  &c.message,
                                                                  &c.url);
                                    match res {
                                        Err(e) => {
                                            println!("ERROR: failed to taste commit {}: {}",
                                                     c.id,
                                                     e)
                                        }
                                        Ok(tr) => notify(&tr),
                                    }
                                }
                            } else if !commits.is_empty() {
                                println!("Skipping {} remaining commits in push!",
                                         commits.len() - 1);
                            }
                        }
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
