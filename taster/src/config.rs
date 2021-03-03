#![deny(clippy::unwrap_in_result)]

use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    IOError(#[from] io::Error),
    #[error("Error parsing taster config as TOML: {0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Error deserializing taster config: {0}")]
    DeserializeError(String),
}

impl Error {
    fn missing_key(k: &str) -> Self {
        Self::DeserializeError(format!("Missing key \"{}\"", k))
    }

    fn invalid_type(k: &str, expected_type: &str) -> Self {
        Self::DeserializeError(format!(
            "Key \"{}\" has invalid type, expected {}",
            k, expected_type
        ))
    }
}

pub fn parse_config(
    cfg: &Path,
    def_imp_threshold: f64,
    def_reg_threshold: f64,
) -> Result<Config, Error> {
    let mut f = fs::File::open(cfg)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;

    let value: toml::value::Table = toml::from_str(&buf)?;

    let to_bench = |t: (&String, &toml::Value)| -> Result<Benchmark, Error> {
        Ok(Benchmark {
            name: t.0.clone(),
            cmd: String::from(
                t.1.get("command")
                    .ok_or_else(|| Error::missing_key("command"))?
                    .as_str()
                    .ok_or_else(|| Error::invalid_type("command", "string"))?,
            ),
            args: t
                .1
                .as_table()
                .ok_or_else(|| Error::invalid_type(t.0, "table"))?
                .get("args")
                .ok_or_else(|| Error::missing_key("args"))?
                .as_array()
                .ok_or_else(|| Error::invalid_type("args", "array"))?
                .iter()
                .map(|a| {
                    Ok(String::from(a.as_str().ok_or_else(|| {
                        Error::invalid_type("args", "array of strings")
                    })?))
                })
                .collect::<Result<Vec<_>, Error>>()?,
            result_expr: t
                .1
                .get("regexs")
                .ok_or_else(|| Error::missing_key("regexs"))?
                .as_array()
                .ok_or_else(|| Error::invalid_type("regexs", "array"))?
                .iter()
                .map(|r| {
                    Ok(Regex::new(
                        r.as_str()
                            .ok_or_else(|| Error::invalid_type("regexs", "array of string"))?,
                    )
                    .map_err(|_| Error::invalid_type("regexs", "array of regular expression"))?)
                })
                .collect::<Result<Vec<_>, Error>>()?,
            lower_is_better: match t.1.get("lower_better") {
                None => false,
                Some(v) => v
                    .as_bool()
                    .ok_or_else(|| Error::invalid_type("lower_better", "bool"))?,
            },
            improvement_threshold: match t.1.get("improvement_threshold") {
                None => def_imp_threshold,
                Some(ref it) => it
                    .as_float()
                    .ok_or_else(|| Error::invalid_type("improvement_threshold", "float"))?,
            },
            regression_threshold: match t.1.get("regression_threshold") {
                None => def_reg_threshold,
                Some(ref rt) => rt
                    .as_float()
                    .ok_or_else(|| Error::invalid_type("regression_threshold", "float"))?,
            },
        })
    };

    // Github <-> Slack username mappings
    let slack_aliases = value
        .get("slack-aliases")
        .into_iter()
        .map(|t| {
            Ok(t.as_table()
                .ok_or_else(|| Error::invalid_type("slack-aliases", "table"))?
                .iter()
                .map(|(k, v)| {
                    Ok((
                        k.clone(),
                        String::from(v.as_str().ok_or_else(|| {
                            Error::invalid_type("slack-aliases", "table of string")
                        })?),
                    ))
                }))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .flatten()
        .collect::<Result<HashMap<_, _>, Error>>()?;

    // Taster config version
    let version = match value.get("version") {
        None => None,
        Some(v) => Some(
            v.as_integer()
                .ok_or_else(|| Error::invalid_type("version", "integer"))?,
        ),
    };

    // Benchmark definitions
    let benchmarks = value
        .iter()
        .filter(|t| t.0 != "slack-aliases" && t.0 != "version")
        .map(to_bench)
        .collect::<Result<Vec<_>, Error>>()?;

    let run_tests = value.get("run_tests").map_or(Ok(true), |v| {
        v.as_bool()
            .ok_or_else(|| Error::invalid_type("run_tests", "bool"))
    })?;

    Ok(Config {
        benchmarks,
        slack_aliases,
        version,
        run_tests,
    })
}
