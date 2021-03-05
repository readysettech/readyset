use log::{error, info, warn};

use crate::config::{parse_config, Benchmark, Config};
use crate::repo::Workspace;
use crate::Commit;
use crate::Push;

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
    pub results: Option<Vec<(Benchmark, ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>,
}

fn run_benchmark(workdir: &str, bench: &Benchmark, timeout: Option<u64>) -> Output {
    let mut cmd = match timeout {
        None => Command::new(&bench.cmd),
        Some(timeout) => {
            let mut cmd = Command::new("timeout");
            cmd.arg("-k")
                .arg(&format!("{}s", timeout + 30))
                .arg(&format!("{}s", timeout))
                .arg(&bench.cmd);
            cmd
        }
    };

    cmd.current_dir(workdir)
        .env("RUST_BACKTRACE", "1")
        .args(bench.args.as_slice());

    cmd.output()
        .expect(&format!("Failed to execute benchmark '{}'!", bench.name))
}

fn write_output(output: &Output, commit_id: git2::Oid, name: &str) {
    use std::fs::File;
    use std::io::Write;

    let mut stdout_file =
        File::create(&format!("{}-{}-stdout.log", commit_id, name)).expect(&format!(
            "Failed to create stdout log file for '{}' at commit '{}'.",
            name, commit_id
        ));
    stdout_file
        .write_all(output.stdout.as_slice())
        .expect("Failed to write output to stdout log file!");
    let mut stderr_file =
        File::create(&format!("{}-{}-stderr.log", commit_id, name)).expect(&format!(
            "Failed to create stderr log file for '{}' at commit '{}'.",
            name, commit_id
        ));
    stderr_file
        .write_all(output.stderr.as_slice())
        .expect("Failed to write output to stderr log file!");
}

fn benchmark(
    workdir: &str,
    _cfg: &Config,
    bench: &Benchmark,
    commit_id: git2::Oid,
    previous_result: Option<&HashMap<String, BenchmarkResult<f64>>>,
    timeout: Option<u64>,
) -> (ExitStatus, HashMap<String, BenchmarkResult<f64>>) {
    // Run the benchmark and collect its output
    let output = run_benchmark(workdir, bench, timeout);
    write_output(&output, commit_id, &bench.name);

    let lines = str::from_utf8(output.stdout.as_slice())
        .unwrap()
        .lines()
        .chain(str::from_utf8(output.stderr.as_slice()).unwrap().lines());
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
                    (cap.get(1).unwrap().as_str().to_owned(), cap.get(2))
                } else {
                    (format!("{}", i), cap.get(1))
                };
                let bm_name = format!("{}/{}", bench.name, &metric);
                if let Some(c) = value {
                    use std::str::FromStr;
                    let val = match f64::from_str(c.as_str()) {
                        Ok(f) => f,
                        Err(_) => {
                            warn!(
                                "failed to parse value '{}' for {} into f64 number, ignoring",
                                c.as_str(),
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
                                Some(pv) => match *pv {
                                    BenchmarkResult::Improvement(v, _) => v,
                                    BenchmarkResult::Regression(v, _) => v,
                                    BenchmarkResult::Neutral(v, _) => v,
                                },
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

fn build(workdir: &str) -> Output {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("check")
        .arg("--all")
        .arg("--all-targets")
        .env("RUST_BACKTRACE", "1")
        .output()
        .expect("Failed to execute 'cargo build'!")
}

pub fn taste_commit(
    ws: &Workspace,
    history: &mut HashMap<String, HashMap<String, HashMap<String, BenchmarkResult<f64>>>>,
    push: &Push,
    commit: &Commit,
    def_improvement_threshold: f64,
    def_regression_threshold: f64,
    timeout: Option<u64>,
) -> Result<(Option<Config>, TastingResult), String> {
    info!("Tasting commit {}", commit.id);
    ws.checkout_commit(&commit.id)?;

    let branch = match push.push_ref {
        None => None,
        Some(ref pr) => match pr.rfind("/") {
            None => None,
            Some(i) => Some(String::from(&pr[i + 1..])),
        },
    };

    let version_output = version(&ws.path);
    write_output(&version_output, commit.id, "version");

    let do_update = !Path::new(&format!("{}/Cargo.lock", ws.path)).exists();

    let build_success = {
        let update_success = if do_update {
            info!("running 'cargo update'");
            let update_output = update(&ws.path);
            write_output(&update_output, commit.id, "update");
            if !update_output.status.success() {
                error!("update failed: output status is {:?}", update_output.status);
            }
            update_output.status.success()
        } else {
            // nothing to do, always succeeds
            true
        };

        let build_output = build(&ws.path);
        write_output(&build_output, commit.id, "build");
        if !build_output.status.success() {
            error!("build failed: output status is {:?}", build_output.status);
        }

        update_success && build_output.status.success()
    };

    let cfg = match parse_config(
        Path::new(&format!("{}/taster.toml", ws.path)),
        def_improvement_threshold,
        def_regression_threshold,
    ) {
        Ok(c) => c,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => {
                    warn!(
                        "Skipping commit {} which doesn't have a Taster config.",
                        commit.id
                    );
                }
                io::ErrorKind::InvalidInput => {
                    warn!(
                        "Skipping commit {} which has an invalid Taster config.",
                        commit.id
                    );
                }
                _ => unimplemented!(),
            }

            return Ok((
                None,
                TastingResult {
                    branch,
                    commit: commit.clone(),
                    build: build_success,
                    test: false,
                    bench: false,
                    results: None,
                },
            ));
        }
    };

    let test_success = !cfg.run_tests || {
        let test_output = test(&ws.path, timeout);
        write_output(&test_output, commit.id, "test");

        let success = test_output.status.success();
        if !success {
            error!("tests failed: output status is {:?}", test_output.status);
        }
        success
    };

    let bench_results = match branch {
        Some(ref branch) => {
            let branch_history = history.entry(branch.clone()).or_insert(HashMap::new());
            cfg.benchmarks
                .iter()
                .map(|b| {
                    let (status, res) = benchmark(
                        &ws.path,
                        &cfg,
                        b,
                        commit.id,
                        branch_history.get(&b.name),
                        timeout,
                    );
                    branch_history.insert(b.name.clone(), res.clone());
                    (b.clone(), status, res)
                })
                .collect::<Vec<(Benchmark, ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>()
        }
        None => cfg
            .benchmarks
            .iter()
            .map(|b| {
                let (status, res) = benchmark(&ws.path, &cfg, b, commit.id, None, timeout);
                (b.clone(), status, res)
            })
            .collect::<Vec<(Benchmark, ExitStatus, HashMap<String, BenchmarkResult<f64>>)>>(),
    };
    let bench_success = bench_results.iter().all(|x| x.1.success());

    Ok((
        Some(cfg),
        TastingResult {
            branch,
            commit: commit.clone(),
            build: build_success,
            test: test_success,
            bench: bench_success,
            results: Some(bench_results),
        },
    ))
}

fn test(workdir: &str, timeout: Option<u64>) -> Output {
    let mut cmd = match timeout {
        None => {
            let mut cmd = Command::new("cargo");
            cmd.arg("test").arg("--all");
            cmd
        }
        Some(timeout) => {
            let mut cmd = Command::new("timeout");
            cmd.arg("-k")
                .arg(&format!("{}s", timeout + 30))
                .arg(&format!("{}s", timeout))
                .arg("cargo")
                .arg("test")
                .arg("--all");
            cmd
        }
    };
    cmd.current_dir(workdir)
        .env("RUST_BACKTRACE", "1")
        .env("RUST_TEST_THREADS", "1")
        .output()
        .expect("Failed to execute 'cargo test'!")
}

fn update(workdir: &str) -> Output {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("update")
        .output()
        .expect("Failed to execute 'cargo update'!")
}

fn version(workdir: &str) -> Output {
    Command::new("rustc")
        .current_dir(workdir)
        .arg("--version")
        .output()
        .expect("Failed to execute 'rustc --version'!")
}
