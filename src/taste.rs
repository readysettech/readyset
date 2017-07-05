use config::{Benchmark, Config, parse_config};
use Commit;
use git2;
use Push;
use repo::Workspace;

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus, Output};
use std::str;

/// `(val, percentage_change)`
#[derive(Debug, Clone)]
pub enum BenchmarkResult<T> {
    Improvement(T, f64),
    Regression(T, f64),
    Neutral(T, f64),
}

#[derive(Debug, Clone)]
pub struct TastingResult {
    pub branch: Option<String>,
    pub commit: Commit,
    pub build: bool,
    pub test: bool,
    pub bench: bool,
    pub results: Option<Vec<HashMap<String, BenchmarkResult<f64>>>>,
}

fn run_benchmark(workdir: &str, cfg: &Config, bench: &Benchmark) -> Output {
    let mut cmd;

    if cfg.version.is_none() || cfg.version.unwrap() < 2 {
        // older taster configs assume an implied "cargo" prefix on each benchmark command
        cmd = Command::new("cargo");
        cmd.current_dir(workdir)
            .env("RUST_BACKTRACE", "1")
            .arg(&bench.cmd)
            .args(bench.args.as_slice());
    } else {
        // from taster config version 2, we no longer assume an implicit "cargo" prefix on
        // benchmark commands
        cmd = Command::new(&bench.cmd);
        cmd.current_dir(workdir)
            .env("RUST_BACKTRACE", "1")
            .args(bench.args.as_slice());
    }

    cmd.output()
        .expect(&format!("Failed to execute benchmark '{}'!", bench.name))
}

fn benchmark(
    workdir: &str,
    cfg: &Config,
    bench: &Benchmark,
    commit_id: git2::Oid,
    previous_result: Option<&HashMap<String, BenchmarkResult<f64>>>,
) -> (ExitStatus, HashMap<String, BenchmarkResult<f64>>) {
    use std::fs::File;
    use std::io::Write;

    // Run the benchmark and collect its output
    let output = run_benchmark(workdir, cfg, bench);

    // Write the output to a log file
    let mut stdout_file = File::create(&format!("{}-{}-stdout.log", commit_id, bench.name))
        .expect(&format!(
            "Failed to create stdout log file for benchmark '{}' at commit '{}'.",
            bench.name,
            commit_id
        ));
    stdout_file
        .write_all(output.stdout.as_slice())
        .expect("Failed to write benchmark output to stdout log file!");
    let mut stderr_file = File::create(&format!("{}-{}-stderr.log", commit_id, bench.name))
        .expect(&format!(
            "Failed to create stderr log file for benchmark '{}' at commit '{}'.",
            bench.name,
            commit_id
        ));
    stderr_file
        .write_all(output.stderr.as_slice())
        .expect("Failed to write benchmark output to stderr log file!");

    let lines = str::from_utf8(output.stdout.as_slice()).unwrap().lines();
    let mut res = HashMap::new();

    // Don't try parsing the output if we didn't succeed
    if !output.status.success() {
        return (output.status, res);
    }

    // Success, so let's look for the results
    for l in lines {
        for (i, regex) in bench.result_expr.iter().enumerate() {
            for cap in regex.captures_iter(l) {
                let (metric, value) = if cap.len() > 2 {
                    (String::from(cap.at(1).unwrap()), cap.at(2))
                } else {
                    (format!("{}", i), cap.at(1))
                };
                let bm_name = format!("{}/{}", bench.name, &metric);
                if let Some(c) = value {
                    use std::str::FromStr;
                    let val = match f64::from_str(&c) {
                        Ok(f) => f,
                        Err(_) => {
                            println!(
                                "failed to parse value '{}' for {} into f64 number, ignoring",
                                c,
                                bm_name
                            );
                            continue;
                        }
                    };
                    let new_result = match previous_result {
                        None => BenchmarkResult::Improvement(val, 0.0),
                        Some(prev_res) => {
                            let old_val = match prev_res.get(&bm_name) {
                                None => val,
                                Some(pv) => {
                                    match *pv {
                                        BenchmarkResult::Improvement(v, _) => v,
                                        BenchmarkResult::Regression(v, _) => v,
                                        BenchmarkResult::Neutral(v, _) => v,
                                    }
                                }
                            };
                            let new_result = if bench.lower_is_better {
                                if val >= old_val * (1.0 + bench.regression_threshold) {
                                    BenchmarkResult::Regression(val, (val / old_val) - 1.0)
                                } else if val < old_val * (1.0 - bench.improvement_threshold) {
                                    BenchmarkResult::Improvement(val, (val / old_val) - 1.0)
                                } else {
                                    BenchmarkResult::Neutral(val, (val / old_val) - 1.0)
                                }
                            } else {
                                if val >= old_val * (1.0 + bench.improvement_threshold) {
                                    BenchmarkResult::Improvement(val, (val / old_val) - 1.0)
                                } else if val < old_val * (1.0 - bench.regression_threshold) {
                                    BenchmarkResult::Regression(val, (val / old_val) - 1.0)
                                } else {
                                    BenchmarkResult::Neutral(val, (val / old_val) - 1.0)
                                }
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

pub fn taste_commit(
    ws: &Workspace,
    history: &mut HashMap<String, HashMap<String, HashMap<String, BenchmarkResult<f64>>>>,
    push: &Push,
    commit: &Commit,
    def_improvement_threshold: f64,
    def_regression_threshold: f64,
) -> Result<(Option<Config>, TastingResult), String> {
    println!("Tasting commit {}", commit.id);
    ws.checkout_commit(&commit.id)?;

    let branch = match push.push_ref {
        None => None,
        Some(ref pr) => {
            match pr.rfind("/") {
                None => None,
                Some(i) => Some(String::from(&pr[i + 1..])),
            }
        }
    };

    let build_success = update(&ws.path).success() && build(&ws.path).success();
    let test_success = test(&ws.path).success();

    let cfg = match parse_config(
        Path::new(&format!("{}/taster.toml", ws.path)),
        def_improvement_threshold,
        def_regression_threshold,
    ) {
        Ok(c) => c,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => {
                    println!(
                        "Skipping commit {} which doesn't have a Taster config.",
                        commit.id
                    );
                    return Ok((
                        None,
                        TastingResult {
                            branch: branch,
                            commit: commit.clone(),
                            build: build_success,
                            test: test_success,
                            bench: false,
                            results: None,
                        },
                    ));
                }
                io::ErrorKind::InvalidInput => {
                    println!(
                        "Skipping commit {} which has an invalid Taster config.",
                        commit.id
                    );
                    return Ok((
                        None,
                        TastingResult {
                            branch: branch,
                            commit: commit.clone(),
                            build: build_success,
                            test: test_success,
                            bench: false,
                            results: None,
                        },
                    ));
                }
                _ => unimplemented!(),
            }
        }
    };

    let bench_out = match branch {
        Some(ref branch) => {
            let branch_history = history.entry(branch.clone()).or_insert(HashMap::new());
            cfg.benchmarks
                .iter()
                .map(|b| {
                    let new_result =
                        benchmark(&ws.path, &cfg, b, commit.id, branch_history.get(&b.name));
                    branch_history.insert(b.name.clone(), new_result.1.clone());
                    new_result
                })
                .collect::<Vec<(ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>()
        }
        None => {
            cfg.benchmarks
                .iter()
                .map(|b| benchmark(&ws.path, &cfg, b, commit.id, None))
                .collect::<Vec<(ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>()
        }
    };
    let bench_success = bench_out.iter().all(|x| x.0.success());
    let bench_results = bench_out.iter().map(|x| x.1.clone()).collect();

    Ok((
        Some(cfg),
        TastingResult {
            branch: branch,
            commit: commit.clone(),
            build: build_success,
            test: test_success,
            bench: bench_success,
            results: Some(bench_results),
        },
    ))
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
