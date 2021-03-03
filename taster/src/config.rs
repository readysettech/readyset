use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Read};
use std::path::Path;

#[derive(Clone, Debug)]
pub struct Benchmark {
    pub name: String,
    pub cmd: String,
    pub args: Vec<String>,
    pub result_expr: Vec<Regex>,
    pub lower_is_better: bool,
    pub improvement_threshold: f64,
    pub regression_threshold: f64,
}

pub struct Config {
    pub benchmarks: Vec<Benchmark>,
    pub slack_aliases: HashMap<String, String>,
    pub version: Option<i64>,
    pub run_tests: bool,
}

pub fn parse_config(
    cfg: &Path,
    def_imp_threshold: f64,
    def_reg_threshold: f64,
) -> Result<Config, Error> {
    // TODO(grfn): Actual error handling rather than unwrap()ing everywhere in this function
    let mut f = fs::File::open(cfg)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;

    let value: toml::value::Table = toml::from_str(&buf)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "failed to parse taster config!"))?;

    let to_bench = |t: (&String, &toml::Value)| Benchmark {
        name: t.0.clone(),
        cmd: String::from(t.1.get("command").unwrap().as_str().unwrap()),
        args: t.1.as_table().unwrap()["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| String::from(a.as_str().unwrap()))
            .collect(),
        result_expr: t
            .1
            .get("regexs")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|r| Regex::new(r.as_str().unwrap()).unwrap())
            .collect(),
        lower_is_better: match t.1.get("lower_better") {
            None => false,
            Some(v) => v.as_bool().unwrap(),
        },
        improvement_threshold: match t.1.get("improvement_threshold") {
            None => def_imp_threshold,
            Some(ref it) => it.as_float().unwrap(),
        },
        regression_threshold: match t.1.get("regression_threshold") {
            None => def_reg_threshold,
            Some(ref rt) => rt.as_float().unwrap(),
        },
    };

    // Github <-> Slack username mappings
    let slack_aliases = value
        .iter()
        .filter(|t| t.0 == "slack-aliases")
        .flat_map(|t| {
            t.1.as_table()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), String::from(v.as_str().unwrap())))
        })
        .collect::<HashMap<_, _>>();

    // Taster config version
    let version = match value.get("version") {
        None => None,
        Some(v) => Some(v.as_integer().unwrap()),
    };

    // Benchmark definitions
    let benchmarks = value
        .iter()
        .filter(|t| t.0 != "slack-aliases" && t.0 != "version")
        .map(|t| to_bench(t))
        .collect();

    let run_tests = value
        .get("run_tests")
        .map_or(true, |v| v.as_bool().unwrap());

    Ok(Config {
        benchmarks,
        slack_aliases,
        version,
        run_tests,
    })
}
