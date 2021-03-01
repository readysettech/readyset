use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Read};
use std::path::Path;
use toml;

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
}

pub fn parse_config(
    cfg: &Path,
    def_imp_threshold: f64,
    def_reg_threshold: f64,
) -> Result<Config, Error> {
    let mut f = try!(fs::File::open(cfg));
    let mut buf = String::new();
    try!(f.read_to_string(&mut buf));

    let value = match toml::Parser::new(&buf).parse() {
        None => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "failed to parse taster config!",
            ))
        }
        Some(v) => v,
    };

    let to_bench = |t: (&String, &toml::Value)| Benchmark {
        name: t.0.clone(),
        cmd: String::from(t.1.lookup("command").unwrap().as_str().unwrap()),
        args: t.1.as_table().unwrap()["args"]
            .as_slice()
            .unwrap()
            .iter()
            .map(|a| String::from(a.as_str().unwrap()))
            .collect(),
        result_expr: t
            .1
            .lookup("regexs")
            .unwrap()
            .as_slice()
            .unwrap()
            .iter()
            .map(|r| Regex::new(r.as_str().unwrap()).unwrap())
            .collect(),
        lower_is_better: match t.1.lookup("lower_better") {
            None => false,
            Some(v) => v.as_bool().unwrap(),
        },
        improvement_threshold: match t.1.lookup("improvement_threshold") {
            None => def_imp_threshold,
            Some(ref it) => it.as_float().unwrap(),
        },
        regression_threshold: match t.1.lookup("regression_threshold") {
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

    Ok(Config {
        benchmarks: benchmarks,
        slack_aliases: slack_aliases,
        version: version,
    })
}
