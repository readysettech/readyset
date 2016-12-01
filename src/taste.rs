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
    Improvement(T, f64),
    Regression(T, f64),
    Neutral(T, f64),
}

#[derive(Debug, Clone)]
pub struct TastingResult {
    pub branch: String,
    pub commit_id: String,
    pub commit_msg: String,
    pub commit_url: String,
    pub build: bool,
    pub test: bool,
    pub bench: bool,
    pub results: Option<Vec<HashMap<String, BenchmarkResult<f64>>>>,
}

fn benchmark(workdir: &str,
             cfg: &Benchmark,
             previous_result: Option<&HashMap<String, BenchmarkResult<f64>>>)
             -> (ExitStatus, HashMap<String, BenchmarkResult<f64>>) {
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
                let (metric, value) = if cap.len() > 2 {
                    (String::from(cap.at(1).unwrap()), cap.at(2))
                } else {
                    (format!("{}", i), cap.at(1))
                };
                let bm_name = format!("{}/{}", cfg.name, &metric);
                if let Some(c) = value {
                    use std::str::FromStr;
                    let val = match f64::from_str(&c) {
                        Ok(f) => f,
                        Err(_) => {
                            println!("failed to parse value '{}' for {} into f64 number, ignoring",
                                     c,
                                     bm_name);
                            continue;
                        }
                    };
                    let new_result = match previous_result {
                        None => BenchmarkResult::Neutral(val, 0.0),
                        Some(prev_res) => {
                            let old_val = match *prev_res.get(&bm_name).unwrap() {
                                BenchmarkResult::Improvement(v, _) => v,
                                BenchmarkResult::Regression(v, _) => v,
                                BenchmarkResult::Neutral(v, _) => v,
                            };
                            let new_result = if val >= old_val {
                                BenchmarkResult::Regression(val, (val / old_val) - 1.0)
                            } else if val < old_val * 0.9 {
                                BenchmarkResult::Improvement(val, (val / old_val) - 1.0)
                            } else {
                                BenchmarkResult::Neutral(val, (val / old_val) - 1.0)
                            };
                            new_result
                        }
                    };
                    res.insert(bm_name, new_result);
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

pub fn taste_commit(wsl: &Mutex<Workspace>,
                    history: &mut HashMap<String,
                                          HashMap<String, HashMap<String, BenchmarkResult<f64>>>>,
                    commit_ref: &str,
                    id: &str,
                    msg: &str,
                    url: &str)
                    -> Result<TastingResult, String> {
    println!("Tasting commit {}", id);
    let ws = wsl.lock().unwrap();
    ws.checkout_commit(id)?;

    let branch = String::from(&commit_ref[commit_ref.rfind("/").unwrap() + 1..]);

    let build_success = update(&ws.path).success() && build(&ws.path).success();
    let test_success = test(&ws.path).success();

    let cfg = match parse_config(Path::new(&format!("{}/taster.toml", ws.path))) {
        Ok(c) => c,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => {
                    println!("Skipping commit {} which doesn't have a Taster config.", id);
                    return Ok(TastingResult {
                        branch: branch,
                        commit_id: String::from(id),
                        commit_msg: String::from(msg),
                        commit_url: String::from(url),
                        build: build_success,
                        test: test_success,
                        bench: false,
                        results: None,
                    });
                }
                _ => unimplemented!(),
            }
        }
    };

    let branch_history = history.entry(branch.clone()).or_insert(HashMap::new());
    let bench_out = cfg.iter()
        .map(|b| {
            let new_result = benchmark(&ws.path, b, branch_history.get(&b.name));
            branch_history.insert(b.name.clone(), new_result.1.clone());
            new_result
        })
        .collect::<Vec<(ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>();
    let bench_success = bench_out.iter().all(|x| x.0.success());
    let bench_results = bench_out.iter().map(|x| x.1.clone()).collect();

    Ok(TastingResult {
        branch: branch,
        commit_id: String::from(id),
        commit_msg: String::from(msg),
        commit_url: String::from(url),
        build: build_success,
        test: test_success,
        bench: bench_success,
        results: Some(bench_results),
    })
}

fn test(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("test")
        .env("RUST_BACKTRACE", "1")
        .env("RUST_TEST_THREADS", "1")
        .status()
        .expect("Failed to execute 'cargo test'!")
}

fn update(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("update")
        .status()
        .expect("Failed to execute 'cargo update'!")
}
