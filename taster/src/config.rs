#![deny(clippy::unwrap_in_result)]

use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use thiserror::Error;

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

#[derive(Clone, Debug)]
pub struct JsonMetricFormat {
    pub name: String,
    pub key: String,
    pub lower_is_better: bool,
    pub improvement_threshold: f64,
    pub regression_threshold: f64,
}

#[derive(Clone, Debug)]
pub enum OutputFormat {
    Regex {
        result_expr: Vec<Regex>,
        lower_is_better: bool,
        improvement_threshold: f64,
        regression_threshold: f64,
    },

    Json {
        benchmark_name_key: String,
        metrics: Vec<JsonMetricFormat>,
    },
}

impl OutputFormat {
    fn from_value(
        val: &toml::Value,
        def_imp_threshold: f64,
        def_reg_threshold: f64,
    ) -> Result<Self, Error> {
        if let Some(regexes) = val.get("regexs") {
            Ok(Self::Regex {
                result_expr: regexes
                    .as_array()
                    .ok_or_else(|| Error::invalid_type("regexs", "array"))?
                    .iter()
                    .map(|r| {
                        Regex::new(
                            r.as_str()
                                .ok_or_else(|| Error::invalid_type("regexs", "array of string"))?,
                        )
                        .map_err(|_| Error::invalid_type("regexs", "array of regular expression"))
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
                lower_is_better: match val.get("lower_better") {
                    None => false,
                    Some(v) => v
                        .as_bool()
                        .ok_or_else(|| Error::invalid_type("lower_better", "bool"))?,
                },
                improvement_threshold: match val.get("improvement_threshold") {
                    None => def_imp_threshold,
                    Some(ref it) => it
                        .as_float()
                        .ok_or_else(|| Error::invalid_type("improvement_threshold", "float"))?,
                },
                regression_threshold: match val.get("regression_threshold") {
                    None => def_reg_threshold,
                    Some(ref rt) => rt
                        .as_float()
                        .ok_or_else(|| Error::invalid_type("regression_threshold", "float"))?,
                },
            })
        } else if val.get("format").and_then(|x| x.as_str()) == Some("json") {
            Ok(Self::Json {
                benchmark_name_key: val
                    .get("benchmark_name_key")
                    .ok_or_else(|| Error::missing_key("benchmark_name_key"))?
                    .as_str()
                    .ok_or_else(|| Error::invalid_type("benchmark_name_key", "string"))?
                    .to_owned(),
                metrics: val
                    .get("metrics")
                    .ok_or_else(|| Error::missing_key("metrics"))?
                    .as_table()
                    .ok_or_else(|| Error::invalid_type("metrics", "table"))?
                    .iter()
                    .map(|(name, v)| {
                        Ok(JsonMetricFormat {
                            name: name.clone(),
                            key: v
                                .get("key")
                                .map_or(Ok(name.as_str()), |k| {
                                    k.as_str()
                                        .ok_or_else(|| Error::invalid_type("key", "string"))
                                })?
                                .to_owned(),
                            lower_is_better: v.get("lower_better").map_or(Ok(false), |x| {
                                x.as_bool()
                                    .ok_or_else(|| Error::invalid_type("lower_better", "bool"))
                            })?,
                            improvement_threshold: v.get("improvement_threshold").map_or(
                                Ok(def_imp_threshold),
                                |x| {
                                    x.as_float().ok_or_else(|| {
                                        Error::invalid_type("improvement_threshold", "float")
                                    })
                                },
                            )?,
                            regression_threshold: v.get("regression_threshold").map_or(
                                Ok(def_reg_threshold),
                                |x| {
                                    x.as_float().ok_or_else(|| {
                                        Error::invalid_type("regression_threshold", "float")
                                    })
                                },
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            })
        } else {
            Err(Error::DeserializeError(
                "All benchmarks must have either a \"regexs\" key or a valid \"format\"".to_owned(),
            ))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Benchmark {
    pub name: String,
    pub cmd: String,
    pub args: Vec<String>,
    pub output_format: OutputFormat,
}

impl Benchmark {
    fn from_value(
        name: &str,
        val: &toml::Value,
        def_imp_threshold: f64,
        def_reg_threshold: f64,
    ) -> Result<Self, Error> {
        Ok(Self {
            name: name.to_owned(),
            cmd: String::from(
                val.get("command")
                    .ok_or_else(|| Error::missing_key("command"))?
                    .as_str()
                    .ok_or_else(|| Error::invalid_type("command", "string"))?,
            ),
            args: val
                .as_table()
                .ok_or_else(|| Error::invalid_type(name, "table"))?
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
            output_format: OutputFormat::from_value(val, def_imp_threshold, def_reg_threshold)?,
        })
    }
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
    let mut f = fs::File::open(cfg)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;

    let mut value: toml::value::Table = toml::from_str(&buf)?;

    // Github <-> Slack username mappings
    let slack_aliases = value
        .remove("slack-aliases")
        .into_iter()
        .map(|t| {
            Ok(t.as_table()
                .ok_or_else(|| Error::invalid_type("slack-aliases", "table"))?
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        k.clone(),
                        String::from(v.as_str().ok_or_else(|| {
                            Error::invalid_type("slack-aliases", "table of string")
                        })?),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>())
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .flatten()
        .flatten()
        .collect::<HashMap<_, _>>();

    // Taster config version
    let version = value
        .remove("version")
        .map(|v| {
            v.as_integer()
                .ok_or_else(|| Error::invalid_type("version", "integer"))
        })
        .transpose()?;

    let run_tests = value.remove("run_tests").map_or(Ok(true), |v| {
        v.as_bool()
            .ok_or_else(|| Error::invalid_type("run_tests", "bool"))
    })?;

    // Benchmark definitions
    let benchmarks = value
        .iter()
        .map(|t| Benchmark::from_value(t.0, t.1, def_imp_threshold, def_reg_threshold))
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(Config {
        benchmarks,
        slack_aliases,
        version,
        run_tests,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn noria_config() {
        let src = include_str!("../../taster.toml");
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(src.as_bytes()).unwrap();

        let res = parse_config(file.path(), 0.0, 0.0);
        if let Err(e) = res {
            panic!("{}", e);
        }
    }
}
