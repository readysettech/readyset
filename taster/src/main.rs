#[macro_use]
extern crate clap;

mod auth;
mod config;
mod github;
mod persistence;
mod repo;
mod slack;
mod taste;
mod webhook;

use std::convert::TryFrom;
use std::io::Read;
use std::path::Path;
use std::sync::Mutex;
use std::thread;

use log::{error, info, warn};
use rouille::Response;
use serde_json::json;

use crate::config::Config;
use crate::persistence::Persistence;
use crate::webhook::authentication::authenticate_payload;
use crate::webhook::events::Event;

const TASTER_USAGE: &str = "\
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

pub struct Notifier {
    slack_notifier: Option<slack::SlackNotifier>,
    github_notifier: Option<github::GithubNotifier>,
}

impl<'a> From<&'a clap::ArgMatches<'a>> for Notifier {
    fn from(args: &'a clap::ArgMatches<'a>) -> Self {
        let repo = args.value_of("github_repo").unwrap();

        Self {
            slack_notifier: args.value_of("slack_hook_url").map(|url| {
                slack::SlackNotifier::new(
                    url,
                    args.value_of("slack_channel").unwrap(),
                    repo,
                    args.is_present("verbose_notifications"),
                )
            }),

            github_notifier: args
                .value_of("github_api_key")
                .map(|key| github::GithubNotifier::new(key)),
        }
    }
}

impl Notifier {
    fn notify(
        &self,
        cfg: Option<&Config>,
        result: &taste::TastingResult,
        push: &Push,
        commit: Option<&Commit>,
    ) -> Result<(), String> {
        if let Some(sn) = &self.slack_notifier {
            sn.notify(cfg, result, push)?;
        }
        if let (Some(gn), Some(commit)) = (&self.github_notifier, commit) {
            gn.notify(cfg, result, push, commit)?;
        }
        Ok(())
    }

    fn notify_pending(&self, push: &Push, commit: &Commit) {
        if let Some(gn) = &self.github_notifier {
            if let Err(e) = gn.notify_pending(&push, &commit) {
                error!("failed to deliver GitHub status notification: {:?}", e)
            }
        }
    }
}

macro_rules! require_header {
    ($req: expr, $header: expr) => {
        if let Some(h) = $req.header($header) {
            h
        } else {
            return Response::text(&format!("Missing required {} header", $header))
                .with_status_code(400);
        }
    };
}

pub fn main() {
    use clap::{App, Arg, ErrorKind};

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

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
                .env("WEBHOOK_SECRET")
                .takes_value(true)
                .required(false)
                .help("GitHub webhook secret"),
        )
        .arg(
            Arg::with_name("slack_hook_url")
                .long("slack_hook_url")
                .env("SLACK_HOOK_URL")
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
                .env("GITHUB_API_KEY")
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
        .arg(
            Arg::with_name("no_bootstrap")
                .long("no-bootstrap")
                .takes_value(false)
                .required(false)
                .help("Disable initial boostrapping of history"),
        )
        .after_help(TASTER_USAGE)
        .get_matches();

    let addr = args.value_of("listen_addr").unwrap();
    let repo = args.value_of("github_repo").unwrap().to_owned();
    let secret = args.value_of("secret").map(|v| v.to_owned());
    let taste_commit = args.value_of("taste_commit");
    let taste_head_only = args.is_present("taste_head_only");
    let workdir = Path::new(args.value_of("workdir").unwrap());
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

    let notifier: &'static Notifier = Box::leak(Box::new(Notifier::from(&args)));
    let persistence: &'static Persistence =
        Box::leak(Box::new(Persistence::try_from(&args).unwrap()));

    let ws = repo::Workspace::new(&repo, workdir);
    if taste_commit.is_some() {
        let cid = if let Some("HEAD") = taste_commit {
            ws.repo.head().unwrap().target().unwrap().clone()
        } else {
            git2::Oid::from_str(taste_commit.unwrap()).unwrap()
        };

        ws.fetch().unwrap();

        match ws.repo.find_object(cid, None) {
            Err(e) => panic!(format!("{}", e.to_string())),
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
                    &persistence,
                    &push,
                    &push.head_commit,
                    improvement_threshold,
                    regression_threshold,
                    timeout,
                );
                match res {
                    Err(e) => error!("failed to taste{}: {}", cid, e),
                    Ok((cfg, tr)) => {
                        notifier.notify(cfg.as_ref(), &tr, &push, None).unwrap();
                        // We're done
                        return;
                    }
                }
            }
        };
    }

    // If we get here, we must be running in continuous mode
    let secret = secret.expect("--secret must be set when in continuous webhook handler mode");
    let wsl: &'static Mutex<_> = Box::leak::<'static>(Box::new(Mutex::new(ws)));

    if !args.is_present("no_bootstrap") {
        // Initialize history by tasting the HEAD commit of each branch
        thread::spawn(move || {
            let ws = wsl.lock().unwrap();
            let branches = ws.branch_heads();
            for (b, c) in branches.iter() {
                if b != "origin/master" {
                    continue;
                }
                info!(
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
                    &persistence,
                    &push,
                    &push.head_commit,
                    improvement_threshold,
                    regression_threshold,
                    timeout,
                );
                assert!(res.is_ok());
            }
        });
    }

    info!("Taster listening on {}", addr);
    rouille::start_server(addr, move |req| {
        if req.url() != "/hook" {
            return Response::empty_404();
        }

        let signature = require_header!(req, "X-Hub-Signature");
        let event_type = require_header!(req, "X-Github-Event");
        let mut payload = String::new();
        req.data().unwrap().read_to_string(&mut payload).unwrap();

        if !authenticate_payload(secret.as_bytes(), payload.as_bytes(), signature.as_bytes()) {
            return Response::text("Invalid signature").with_status_code(400);
        }

        let event = match serde_json::from_str(&payload)
            .and_then(|val| Event::from_value(event_type, val))
        {
            Ok(ev) => ev,
            Err(e) => {
                return Response::json(&json!({ "error": e.to_string() })).with_status_code(400)
            }
        };
        match event {
            Event::Push {
                _ref,
                commits,
                head_commit,
                pusher,
                repository,
                ..
            } => {
                thread::spawn(move || {
                    info!(
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

                    {
                        notifier.notify_pending(&push, &push.head_commit);
                        let ws = wsl.lock().unwrap();
                        // First taste the head commit
                        ws.fetch().unwrap();
                        let head_res = taste::taste_commit(
                            &ws,
                            &persistence,
                            &push,
                            &push.head_commit,
                            improvement_threshold,
                            regression_threshold,
                            timeout,
                        );
                        match head_res {
                            Err(e) => {
                                error!("failed to taste HEAD commit {}: {}", head_commit.id, e)
                            }
                            Ok((cfg, tr)) => {
                                notifier
                                    .notify(cfg.as_ref(), &tr, &push, Some(&push.head_commit))
                                    .unwrap_or_else(|err| {
                                        error!("Failed to notify about result: {}", err)
                                    });
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
                                        notifier.notify_pending(&push, &cur_c);
                                        // taste
                                        let res = taste::taste_commit(
                                            &ws,
                                            &persistence,
                                            &push,
                                            &cur_c,
                                            improvement_threshold,
                                            regression_threshold,
                                            timeout,
                                        );
                                        match res {
                                            Err(e) => {
                                                error!("failed to taste commit {}: {}", c.id, e)
                                            }
                                            Ok((cfg, tr)) => {
                                                notifier
                                                    .notify(cfg.as_ref(), &tr, &push, Some(&cur_c))
                                                    .unwrap_or_else(|err| {
                                                        error!(
                                                            "Failed to notify about result: {}",
                                                            err
                                                        )
                                                    });
                                            }
                                        }
                                    }
                                } else if !commits.is_empty() {
                                    warn!(
                                        "Skipping {} remaining commits in push!",
                                        commits.len() - 1
                                    );
                                }
                            }
                        }
                    }
                });
            }
            _ => (),
        }
        Response::text("ok")
    });
}
