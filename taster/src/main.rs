extern crate env_logger;
extern crate log;

extern crate afterparty;
#[macro_use]
extern crate clap;
extern crate git2;
extern crate github_rs;
extern crate hyper;
extern crate lettre;
extern crate regex;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate slack_hook;
extern crate toml;

mod auth;
mod config;
mod email;
mod repo;
mod slack;
mod taste;
mod github;

use afterparty::{Delivery, Event, Hub};
use hyper::Server;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};

use config::Config;

#[cfg_attr(rustfmt, rustfmt_skip)]
const TASTER_USAGE: &'static str = "\
EXAMPLES:
  taster -w /path/to/workdir -s my_secret
  taster -l 0.0.0.0:1234 -w /path/to/workdir -s my_secret";

#[derive(Clone, Debug)]
pub struct Commit {
    pub id: git2::Oid,
    pub msg: String,
    pub url: String,
}

#[derive(Clone, Debug)]
pub struct Push {
    pub head_commit: Commit,
    pub push_ref: Option<String>,
    pub pusher: Option<String>,
    pub owner_name: Option<String>,
    pub repo_name: Option<String>,
}

pub fn main() {
    use clap::{App, Arg, ErrorKind};

    env_logger::init().unwrap();

    let args = App::new("taster")
        .version("0.0.1")
        .about("Tastes GitHub commits.")
        .arg(
            Arg::with_name("listen_addr")
                .short("l")
                .long("listen_addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .default_value("0.0.0.0:4567")
                .help("Listen address and port for webhook delivery"),
        )
        .arg(
            Arg::with_name("github_repo")
                .short("r")
                .long("github_repo")
                .takes_value(true)
                .required(true)
                .value_name("GH_REPO")
                .default_value("https://github.com/ms705/taster")
                .help("GitHub repository to taste"),
        )
        .arg(
            Arg::with_name("email_addr")
                .long("email_addr")
                .takes_value(true)
                .required(false)
                .help("Email address to send notifications to"),
        )
        .arg(
            Arg::with_name("default_regression_reporting_threshold")
                .long("default_regression_reporting_threshold")
                .takes_value(true)
                .default_value("0.1")
                .help(
                    "Relative performance threshold below which a result is considered a \
                     regression that needs reporting (0.1 = +/-10%).",
                ),
        )
        .arg(
            Arg::with_name("default_improvement_reporting_threshold")
                .long("default_improvement_reporting_threshold")
                .takes_value(true)
                .default_value("0.1")
                .help(
                    "Relative performance threshold above which a result is considered an \
                     improvement that needs reporting (0.1 = +/-10%).",
                ),
        )
        .arg(
            Arg::with_name("secret")
                .short("s")
                .long("secret")
                .takes_value(true)
                .required(false)
                .help("GitHub webhook secret"),
        )
        .arg(
            Arg::with_name("slack_hook_url")
                .long("slack_hook_url")
                .takes_value(true)
                .required(false)
                .help("Slack webhook URL to push notifications to"),
        )
        .arg(
            Arg::with_name("slack_channel")
                .long("slack_channel")
                .takes_value(true)
                .required(false)
                .default_value("#soup-test")
                .help("Slack channel for notifications"),
        )
        .arg(
            Arg::with_name("github_api_key")
                .long("github_api_key")
                .takes_value(true)
                .required(false)
                .help("GitHub API key to provide status notifications"),
        )
        .arg(
            Arg::with_name("taste_commit")
                .long("taste_commit")
                .short("t")
                .takes_value(true)
                .required(false)
                .help("Do a one-off taste of a specific commit"),
        )
        .arg(
            Arg::with_name("taste_head_only")
                .long("taste_head_only")
                .required(false)
                .help("When multiple commits are pushed, taste the head commit only"),
        )
        .arg(
            Arg::with_name("timeout")
                .long("timeout")
                .required(false)
                .takes_value(true)
                .help("Timeout (in seconds) after which benchmarks should be killed"),
        )
        .arg(
            Arg::with_name("verbose_notifications")
                .long("verbose_notifications")
                .required(false)
                .help(
                    "List all benchmarks in notifications even if the results have not changed \
                     significantly",
                ),
        )
        .arg(
            Arg::with_name("workdir")
                .short("w")
                .long("workdir")
                .takes_value(true)
                .required(true)
                .value_name("REPO_DIR")
                .help("Directory holding the workspace repo"),
        )
        .after_help(TASTER_USAGE)
        .get_matches();

    let addr = args.value_of("listen_addr").unwrap();
    let email_notification_addr = args.value_of("email_addr");
    let repo = args.value_of("github_repo").unwrap();
    let secret = args.value_of("secret");
    let slack_hook_url = args.value_of("slack_hook_url");
    let slack_channel = args.value_of("slack_channel");
    let taste_commit = args.value_of("taste_commit");
    let github_api_key = args.value_of("github_api_key");
    let taste_head_only = args.is_present("taste_head_only");
    let workdir = Path::new(args.value_of("workdir").unwrap());
    let verbose_notify = args.is_present("verbose_notifications");
    let improvement_threshold =
        value_t_or_exit!(args, "default_improvement_reporting_threshold", f64);
    let regression_threshold =
        value_t_or_exit!(args, "default_regression_reporting_threshold", f64);
    let timeout = match value_t!(args, "timeout", u64) {
        Ok(timeout) => Some(timeout),
        Err(e) => match e.kind {
            ErrorKind::ArgumentNotFound => None,
            _ => panic!("failed to parse timeout: {:?}", e),
        },
    };

    let mut history = HashMap::new();
    let ws = repo::Workspace::new(repo, workdir);
    let en = if let Some(addr) = email_notification_addr {
        Some(email::EmailNotifier::new(addr, repo))
    } else {
        None
    };
    let sn = if let Some(url) = slack_hook_url {
        Some(slack::SlackNotifier::new(
            url,
            slack_channel.unwrap(),
            repo,
            verbose_notify,
        ))
    } else {
        None
    };
    let gn = if let Some(key) = github_api_key {
        Some(github::GithubNotifier::new(key))
    } else {
        None
    };

    if taste_commit.is_some() {
        let cid = if let Some("HEAD") = taste_commit {
            ws.repo.head().unwrap().target().unwrap().clone()
        } else {
            git2::Oid::from_str(taste_commit.unwrap()).unwrap()
        };
        match ws.repo.find_object(cid, None) {
            Err(e) => panic!(format!("{}", e.description())),
            Ok(o) => {
                let cobj = o.as_commit().unwrap();
                let hc = Commit {
                    id: cobj.id(),
                    msg: String::from(cobj.message().unwrap()),
                    url: format!("{}/commit/{}", repo, cobj.id()),
                };
                // fake a push
                let push = Push {
                    head_commit: hc,
                    push_ref: None,
                    pusher: None,
                    owner_name: None,
                    repo_name: None,
                };
                let res = taste::taste_commit(
                    &ws,
                    &mut history,
                    &push,
                    &push.head_commit,
                    improvement_threshold,
                    regression_threshold,
                    timeout,
                );
                match res {
                    Err(e) => println!("ERROR: failed to taste{}: {}", cid, e),
                    Ok((cfg, tr)) => {
                        // email notification
                        if en.is_some() {
                            en.as_ref()
                                .unwrap()
                                .notify(cfg.as_ref(), &tr, &push)
                                .unwrap();
                        }
                        // slack notification
                        if sn.is_some() {
                            sn.as_ref()
                                .unwrap()
                                .notify(cfg.as_ref(), &tr, &push)
                                .unwrap();
                        }
                        // We're done
                        return;
                    }
                }
            }
        };
    }

    // If we get here, we must be running in continuous mode
    if let None = secret {
        panic!("--secret must be set when in continuous webhook handler mode");
    }

    // Initialize history by tasting the HEAD commit of each branch
    {
        let branches = ws.branch_heads();
        for (b, c) in branches.iter() {
            if b != "origin/master" {
                continue;
            }
            println!(
                "tasting HEAD of {}: {} / {}",
                b,
                c.id(),
                c.message().unwrap()
            );
            let hc = Commit {
                id: c.id(),
                msg: String::from(c.message().unwrap()),
                url: format!("{}/commit/{}", repo, c.id()),
            };
            // fake a push
            let push = Push {
                head_commit: hc,
                push_ref: Some(b.clone()),
                pusher: None,
                owner_name: None,
                repo_name: None,
            };
            let res = taste::taste_commit(
                &ws,
                &mut history,
                &push,
                &push.head_commit,
                improvement_threshold,
                regression_threshold,
                timeout,
            );
            assert!(res.is_ok());
        }
    }

    let hl = Arc::new(Mutex::new(history));
    let wsl = Mutex::new(ws);

    let mut hub = Hub::new();
    hub.handle_authenticated("push", secret.unwrap(), move |delivery: &Delivery| {
        match delivery.payload {
            Event::Push {
                ref _ref,
                ref commits,
                ref head_commit,
                ref pusher,
                ref repository,
                ..
            } => {
                println!(
                    "Handling {} commits pushed by {}",
                    commits.len(),
                    pusher.name
                );

                // Data structures to represent info from webhook
                let hc = Commit {
                    id: git2::Oid::from_str(&head_commit.id).unwrap(),
                    msg: head_commit.message.clone(),
                    url: head_commit.url.clone(),
                };
                let push = Push {
                    head_commit: hc,
                    push_ref: Some(_ref.clone()),
                    pusher: Some(pusher.name.clone()),
                    owner_name: Some(repository.owner.name.clone()),
                    repo_name: Some(repository.name.clone()),
                };

                let notify_pending = |push: &Push, commit: &Commit| {
                    // github status notification
                    if gn.is_some() {
                        match gn.as_ref().unwrap().notify_pending(&push, &commit) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("failed to deliver GitHub status notification: {:?}", e)
                            }
                        }
                    }
                };

                let notify = |cfg: Option<&Config>,
                              res: &taste::TastingResult,
                              push: &Push,
                              commit: &Commit| {
                    // email notification
                    if en.is_some() {
                        en.as_ref().unwrap().notify(cfg, &res, &push).unwrap();
                    }
                    // slack notification
                    if sn.is_some() {
                        sn.as_ref().unwrap().notify(cfg, &res, &push).unwrap();
                    }
                    // github status notification
                    if gn.is_some() {
                        gn.as_ref()
                            .unwrap()
                            .notify(cfg, &res, &push, &commit)
                            .unwrap();
                    }
                };

                {
                    notify_pending(&push, &push.head_commit);
                    let ws = wsl.lock().unwrap();
                    let mut history = hl.lock().unwrap();
                    // First taste the head commit
                    ws.fetch().unwrap();
                    let head_res = taste::taste_commit(
                        &ws,
                        &mut history,
                        &push,
                        &push.head_commit,
                        improvement_threshold,
                        regression_threshold,
                        timeout,
                    );
                    match head_res {
                        Err(e) => println!(
                            "ERROR: failed to taste HEAD commit {}: {}",
                            head_commit.id,
                            e
                        ),
                        Ok((cfg, tr)) => {
                            notify(cfg.as_ref(), &tr, &push, &push.head_commit);
                            // Taste others if needed
                            if !taste_head_only {
                                for c in commits.iter() {
                                    if c.id == head_commit.id {
                                        // skip HEAD as we've already tested it
                                        continue;
                                    }
                                    let cur_c = Commit {
                                        id: git2::Oid::from_str(&c.id).unwrap(),
                                        msg: c.message.clone(),
                                        url: c.url.clone(),
                                    };
                                    notify_pending(&push, &cur_c);
                                    // taste
                                    let res = taste::taste_commit(
                                        &ws,
                                        &mut history,
                                        &push,
                                        &cur_c,
                                        improvement_threshold,
                                        regression_threshold,
                                        timeout,
                                    );
                                    match res {
                                        Err(e) => println!(
                                            "ERROR: failed to taste commit {}: {}",
                                            c.id,
                                            e
                                        ),
                                        Ok((cfg, tr)) => notify(cfg.as_ref(), &tr, &push, &cur_c),
                                    }
                                }
                            } else if !commits.is_empty() {
                                println!(
                                    "Skipping {} remaining commits in push!",
                                    commits.len() - 1
                                );
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    });

    let srvc = Server::http(&addr[..]).unwrap().handle(hub);

    println!("Taster listening on {}", addr);
    srvc.unwrap();
}
