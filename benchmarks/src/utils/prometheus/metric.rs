use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

pub mod parser;
use parser::Error;

/// The types of metrics that can be parsed and re-exported
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Counter => write!(f, "counter"),
            Self::Gauge => write!(f, "gauge"),
            Self::Summary => write!(f, "summary"),
        }
    }
}

impl FromStr for MetricType {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_ascii_lowercase().as_ref() {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "summary" => Ok(Self::Summary),
            s => Err(Error::InvalidMetricType(s.into())),
        }
    }
}

/// Values that can be parsed and re-exported
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Integer(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
        }
    }
}

impl FromStr for Value {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(v) = input.parse() {
            Ok(Self::Integer(v))
        } else if let Ok(v) = input.parse() {
            Ok(Self::Float(v))
        } else {
            Err(Error::InvalidValue(input.to_string()))
        }
    }
}

/// A "whole" metric, containing one or more samples.
///
/// Example Prometheus output:
/// ```prometheus
/// # TYPE metric_a gauge
/// metric_a{key="value"} 1
/// metric_a{key="other_value"} 2
///
/// # TYPE metric_b gauge
/// metric_b{} 3
/// ```
///
/// In this example output, the top three lines are one metric, and the bottom two are another.
#[derive(Debug, PartialEq)]
pub struct Metric {
    pub kind: MetricType,
    pub name: String,
    pub description: Option<String>,
    pub samples: Vec<Sample>,
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(desc) = self.description.as_ref() {
            writeln!(f, "# HELP {} {}", self.name, desc)?;
        }
        writeln!(f, "# TYPE {} {}", self.name, self.kind)?;
        for sample in &self.samples {
            writeln!(f, "{}", sample)?;
        }
        writeln!(f)
    }
}

impl Metric {
    pub fn new(kind: MetricType, name: String) -> Self {
        Self {
            kind,
            name,
            description: None,
            samples: Vec::new(),
        }
    }

    pub fn add_global_labels(&mut self, global_labels: &Arc<Vec<(String, String)>>) {
        for sample in &mut self.samples {
            sample.labels.extend(global_labels.iter().cloned());
        }
    }
}

/// A single sample for a metric
///
/// Example Prometheus output:
/// ```prometheus
/// # TYPE metric_a gauge
/// metric_a{key="value"} 1
/// metric_a{key="other_value"} 2
///
/// # TYPE metric_b gauge
/// metric_b{} 3
/// ```
///
/// In this example output, lines 2 and 3 are each one sample in `metric_a`, while line 6 is one
/// (and the only) sample in `metric_b`.
#[derive(Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: Value,
    pub timestamp: Option<u64>,
}

impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut labels = self
            .labels
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect::<Vec<_>>();
        labels.sort_unstable();
        write!(f, "{}", self.name)?;
        if !labels.is_empty() {
            write!(f, "{{{}}}", labels.join(","))?;
        }
        write!(f, " {}", self.value)?;
        if let Some(ts) = self.timestamp {
            write!(f, " {}", ts)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_value() {
        assert_eq!("1234".parse::<Value>().unwrap(), Value::Integer(1234));
        assert_eq!("45.67".parse::<Value>().unwrap(), Value::Float(45.67));
        assert_eq!(
            "55.99710586500943".parse::<Value>().unwrap(),
            Value::Float(55.99710586500943)
        );
    }
}
