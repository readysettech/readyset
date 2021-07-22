use anyhow::anyhow;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};

use crate::config::{self, parse_config, Benchmark, Config, OutputFormat};
use crate::persistence::Persistence;
use crate::repo::Workspace;
use crate::Commit;
use crate::Push;

use std::collections::HashMap;
use std::f64::{INFINITY, NEG_INFINITY};
use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus, Output};
use std::str;

/// `(val, percentage_change)`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BenchmarkResult<T> {
    Improvement(T, f64),
    Regression(T, f64),
    Neutral(T, f64),
}

impl<T> BenchmarkResult<T> {
    pub fn value(&self) -> &T {
        match self {
            BenchmarkResult::Improvement(x, _) => x,
            BenchmarkResult::Regression(x, _) => x,
            BenchmarkResult::Neutral(x, _) => x,
        }
    }

    pub fn change(&self) -> f64 {
        match self {
            BenchmarkResult::Improvement(_, x) => *x,
            BenchmarkResult::Regression(_, x) => *x,
            BenchmarkResult::Neutral(_, x) => *x,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TastingResult {
    pub branch: Option<String>,
    pub commit: Commit,
    pub build: bool,
    pub test: bool,
    pub bench: bool,
    #[allow(clippy::type_complexity)]
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
        .unwrap_or_else(|_| panic!("Failed to execute benchmark '{}'!", bench.name))
}

fn write_output(output: &Output, commit_id: git2::Oid, name: &str) {
    use std::fs::File;
    use std::io::Write;

    let mut stdout_file = File::create(&format!("{}-{}-stdout.log", commit_id, name))
        .unwrap_or_else(|_| {
            panic!(
                "Failed to create stdout log file for '{}' at commit '{}'.",
                name, commit_id
            )
        });
    stdout_file
        .write_all(output.stdout.as_slice())
        .expect("Failed to write output to stdout log file!");
    let mut stderr_file = File::create(&format!("{}-{}-stderr.log", commit_id, name))
        .unwrap_or_else(|_| {
            panic!(
                "Failed to create stderr log file for '{}' at commit '{}'.",
                name, commit_id
            )
        });
    stderr_file
        .write_all(output.stderr.as_slice())
        .expect("Failed to write output to stderr log file!");
}

fn parse_output(
    benchmark_name: &str,
    format: &OutputFormat,
    previous_result: Option<&HashMap<String, BenchmarkResult<f64>>>,
    output: &Output,
) -> anyhow::Result<HashMap<String, BenchmarkResult<f64>>> {
    let mut res = HashMap::new();

    let mut record_result = |bm_name: &str,
                             val: f64,
                             lower_is_better: bool,
                             improvement_threshold: f64,
                             regression_threshold: f64| {
        let new_result = match previous_result {
            None => BenchmarkResult::Improvement(val, 0.0),
            Some(prev_res) => {
                let old_val = match prev_res.get(bm_name) {
                    None => val,
                    Some(pv) => *pv.value(),
                };

                let change = |val: f64, old_val: f64| -> f64 {
                    if old_val == 0f64 {
                        if val == 0f64 {
                            0.0
                        } else if val < old_val {
                            NEG_INFINITY
                        } else {
                            INFINITY
                        }
                    } else {
                        (val / old_val) - 1.0
                    }
                };

                if lower_is_better {
                    if val >= old_val * (1.0 + regression_threshold) {
                        BenchmarkResult::Regression(val, change(val, old_val))
                    } else if val < old_val * (1.0 - improvement_threshold) {
                        BenchmarkResult::Improvement(val, change(val, old_val))
                    } else {
                        BenchmarkResult::Neutral(val, change(val, old_val))
                    }
                } else if val >= old_val * (1.0 + improvement_threshold) {
                    BenchmarkResult::Improvement(val, change(val, old_val))
                } else if val < old_val * (1.0 - regression_threshold) {
                    BenchmarkResult::Regression(val, change(val, old_val))
                } else {
                    BenchmarkResult::Neutral(val, change(val, old_val))
                }
            }
        };
        res.insert(bm_name.to_owned(), new_result);
    };

    match format {
        OutputFormat::Regex {
            result_expr,
            lower_is_better,
            improvement_threshold,
            regression_threshold,
        } => {
            let lines = str::from_utf8(output.stdout.as_slice())?
                .lines()
                .chain(str::from_utf8(output.stderr.as_slice()).unwrap().lines());

            for l in lines {
                for (i, regex) in result_expr.iter().enumerate() {
                    for cap in regex.captures_iter(l) {
                        let (metric, value) = if cap.len() > 2 {
                            (
                                cap.get(1)
                                    .ok_or_else(|| anyhow!("Wrong number of regex captures"))?
                                    .as_str()
                                    .to_owned(),
                                cap.get(2),
                            )
                        } else {
                            (format!("{}", i), cap.get(1))
                        };
                        let bm_name = format!("{}/{}", benchmark_name, &metric);
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
                            record_result(
                                &bm_name,
                                val,
                                *lower_is_better,
                                *improvement_threshold,
                                *regression_threshold,
                            );
                        }
                    }
                }
            }
        }
        OutputFormat::Json {
            benchmark_name_key,
            metrics,
        } => {
            let json: Vec<serde_json::Map<String, serde_json::Value>> =
                serde_json::from_slice(output.stdout.as_slice())?;
            let expected_key =
                |k| anyhow!("Benchmark result did not contain expected key \"{}\"", k);
            let wrong_type = |k, ty| anyhow!("Unexpected type for key \"{}\", expected {}", k, ty);
            for result in json {
                let name = result
                    .get(benchmark_name_key)
                    .ok_or_else(|| expected_key(benchmark_name_key))?
                    .as_str()
                    .ok_or_else(|| wrong_type(benchmark_name_key, "string"))?;
                for metric in metrics {
                    let value = result
                        .get(&metric.key)
                        .ok_or_else(|| expected_key(&metric.key))?
                        .as_f64()
                        .ok_or_else(|| wrong_type(&metric.key, "number"))?;
                    let name = format!("{}/{}/{}", benchmark_name, name, metric.name);
                    record_result(
                        &name,
                        value,
                        metric.lower_is_better,
                        metric.improvement_threshold,
                        metric.regression_threshold,
                    );
                }
            }
        }
    }

    Ok(res)
}

fn benchmark(
    workdir: &str,
    _cfg: &Config,
    bench: &Benchmark,
    commit_id: git2::Oid,
    previous_result: Option<&HashMap<String, BenchmarkResult<f64>>>,
    timeout: Option<u64>,
) -> anyhow::Result<(ExitStatus, HashMap<String, BenchmarkResult<f64>>)> {
    // Run the benchmark and collect its output
    let output = run_benchmark(workdir, bench, timeout);
    write_output(&output, commit_id, &bench.name);

    // Don't try parsing the output if we didn't succeed
    if !output.status.success() {
        error!(
            "Benchmark {} failed with exit code {}",
            bench.name,
            output
                .status
                .code()
                .map_or("unknown".to_string(), |code| code.to_string())
        );
        return Ok((output.status, Default::default()));
    }

    // Success, so let's look for the results
    let res =
        parse_output(&bench.name, &bench.output_format, previous_result, &output).map_err(|e| {
            error!("Error parsing output from benchmark {}: {}", bench.name, e);
            e
        })?;

    Ok((output.status, res))
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
    persistence: &Persistence,
    push: &Push,
    commit: &Commit,
    def_improvement_threshold: f64,
    def_regression_threshold: f64,
    timeout: Option<u64>,
) -> anyhow::Result<(Option<Config>, TastingResult)> {
    info!("Tasting commit {}", commit.id);
    ws.checkout_commit(&commit.id)?;

    let branch = match push.push_ref {
        None => None,
        Some(ref pr) => pr.rfind('/').map(|i| String::from(&pr[i + 1..])),
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

        info!("building commit {}", commit.id);
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
            warn!("Error parsing taster.toml from commit {}: {}", commit.id, e);
            match e {
                config::Error::IO(ioe) if ioe.kind() == io::ErrorKind::NotFound => {
                    warn!(
                        "Skipping commit {} which doesn't have a Taster config.",
                        commit.id
                    );
                }
                config::Error::Parse(_) | config::Error::Deserialize(_) => {
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
        info!("Running tests for commit {}", commit.id);
        let test_output = test(&ws.path, timeout);
        write_output(&test_output, commit.id, "test");

        let success = test_output.status.success();
        if !success {
            error!("tests failed: output status is {:?}", test_output.status);
        }
        success
    };

    info!("Running benchmarks for commit {}", commit.id);
    let bench_results = match branch {
        Some(ref branch) => {
            let branch_history = persistence.branch(branch)?;
            cfg.benchmarks
                .iter()
                .map(|b| {
                    benchmark(
                        &ws.path,
                        &cfg,
                        b,
                        commit.id,
                        branch_history.get(&b.name),
                        timeout,
                    )
                    .map(|(status, res)| {
                        if let Err(e) = persistence.save_result(branch, &b.name, &res) {
                            error!("Error persisting benchmark result: {}", e);
                        };
                        (b.clone(), status, res)
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        }
        None => cfg
            .benchmarks
            .iter()
            .map(|b| {
                benchmark(&ws.path, &cfg, b, commit.id, None, timeout)
                    .map(|(status, res)| (b.clone(), status, res))
            })
            .collect::<Result<Vec<_>, _>>(),
    };
    let bench_success = bench_results
        .as_ref()
        .map_or(false, |results| results.iter().all(|x| x.1.success()));

    info!("Finished tasting commit {}", commit.id);
    Ok((
        Some(cfg),
        TastingResult {
            branch,
            commit: commit.clone(),
            build: build_success,
            test: test_success,
            bench: bench_success,
            results: bench_results.ok(),
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
