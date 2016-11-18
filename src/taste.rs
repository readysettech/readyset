use config::{Benchmark, parse_config};
use repo::Workspace;

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::str;
use std::sync::Mutex;

/// `(val, percentage_change)`
#[derive(Debug, Clone)]
pub enum BenchmarkResult<T> {
    Improvement(T, i64),
    Regression(T, i64),
    Neutral(T, i64),
}

#[derive(Debug, Clone)]
pub struct TastingResult {
    pub commit_id: String,
    pub commit_msg: String,
    pub build: bool,
    pub bench: bool,
    pub results: Option<Vec<HashMap<String, BenchmarkResult<String>>>>,
}

fn benchmark(workdir: &str,
             cfg: &Benchmark)
             -> (ExitStatus, HashMap<String, BenchmarkResult<String>>) {
    let mut cmd = Command::new("cargo");
    cmd.current_dir(workdir)
        .env("RUST_BACKTRACE", "1")
        .arg(&cfg.cmd)
        .args(cfg.args.as_slice());

    let output = cmd.output()
        .expect(&format!("Failed to execute benchmark '{}'!", cfg.name));

    let lines = str::from_utf8(output.stdout.as_slice()).unwrap().lines();
    let mut res = HashMap::new();
    for l in lines {
        for (i, regex) in cfg.result_expr.iter().enumerate() {
            for cap in regex.captures_iter(l) {
                for c in cap.iter() {
                    res.insert(format!("{}/{}", cfg.name, i),
                               BenchmarkResult::Neutral(String::from(c.unwrap()), 0));
                }
            }
        }
    }
    (output.status, res)
}

fn build(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("build")
        .arg("--release")
        .env("RUST_BACKTRACE", "1")
        .status()
        .expect("Failed to execute 'cargo build'!")
}

pub fn taste_commit(wsl: &Mutex<Workspace>, id: &str, msg: &str) -> TastingResult {
    println!("Tasting commit {}", id);
    let ws = wsl.lock().unwrap();
    ws.checkout_commit(id);

    let build_success = update(&ws.path).success() && build(&ws.path).success();

    let cfg = match parse_config(Path::new(&format!("{}/taster.toml", ws.path))) {
        Ok(c) => c,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => {
                    println!("Skipping commit {} which doesn't have a Taster config.", id);
                    return TastingResult {
                        commit_id: String::from(id),
                        commit_msg: String::from(msg),
                        build: build_success,
                        bench: false,
                        results: None,
                    };
                }
                _ => unimplemented!(),
            }
        }
    };

    let bench_out = cfg.iter()
        .map(|b| benchmark(&ws.path, b))
        .collect::<Vec<(ExitStatus, HashMap<String, BenchmarkResult<String>>)>>();
    let bench_success = bench_out.iter().all(|x| x.0.success());
    let bench_results = bench_out.iter().map(|x| x.1.clone()).collect();

    TastingResult {
        commit_id: String::from(id),
        commit_msg: String::from(msg),
        build: build_success,
        bench: bench_success,
        results: Some(bench_results),
    }
}

fn update(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("update")
        .status()
        .expect("Failed to execute 'cargo update'!")
}
