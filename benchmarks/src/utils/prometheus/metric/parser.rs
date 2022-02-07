use std::convert::{TryFrom, TryInto};
use std::str::FromStr;

use futures::stream::Stream;
use nom::bytes::complete::{tag, take_while};
use nom::character::complete::{alpha1, char, space0, space1};
use nom::combinator::{all_consuming, opt, rest};
use nom::multi::separated_list;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;

use super::{Metric, MetricType, Sample};

/// Contains all the error conditions that can be produced by the parser
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("invalid metric type '{0}'")]
    InvalidMetricType(String),
    #[error("invalid value '{0}'")]
    InvalidValue(String),
    #[error("invalid timestamp '{0}'")]
    InvalidTimestamp(String),
    #[error("invalid metric line '{0}'")]
    InvalidMetric(String),
    #[error("invalid line: '{0}'")]
    InvalidLine(String),
    #[error("tried to add sample type {0} to metric type {1}")]
    IncorrectSampleType(&'static str, MetricType),
    #[error("incomplete metric")]
    IncompleteMetric,
}

fn identifier(i: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_ascii_alphanumeric() || c == '_')(i)
}

#[derive(Clone, Debug, PartialEq)]
struct BorrowedSample<'a> {
    name: &'a str,
    labels: Vec<BorrowedLabel<'a>>,
    value: &'a str,
    timestamp: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq)]
struct BorrowedLabel<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Debug, Default)]
struct PartialMetric {
    kind: Option<MetricType>,
    name: String,
    description: Option<String>,
    samples: Vec<Sample>,
}

impl PartialMetric {
    fn from_type(name: String, kind: MetricType) -> Self {
        Self {
            name,
            kind: Some(kind),
            ..Default::default()
        }
    }

    fn from_help(name: String, description: String) -> Self {
        Self {
            name,
            description: Some(description),
            ..Default::default()
        }
    }

    fn is_complete(&self) -> bool {
        self.kind.is_some()
    }
}

impl TryFrom<PartialMetric> for Metric {
    type Error = Error;
    fn try_from(partial: PartialMetric) -> Result<Self, Self::Error> {
        if !partial.is_complete() {
            return Err(Error::IncompleteMetric);
        }
        Ok(Self {
            kind: partial.kind.unwrap(),
            name: partial.name,
            description: partial.description,
            samples: partial.samples,
        })
    }
}

impl<'a> TryFrom<BorrowedSample<'a>> for Sample {
    type Error = Error;
    fn try_from(input: BorrowedSample<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: input.name.into(),
            labels: input
                .labels
                .into_iter()
                .map(|l| (l.key.into(), l.value.into()))
                .collect(),
            value: input.value.parse()?,
            timestamp: input
                .timestamp
                .map(|ts| ts.parse().map_err(|_| Error::InvalidTimestamp(ts.into())))
                .map_or(Ok(None), |r| r.map(Some))?,
        })
    }
}

fn help_line(i: &str) -> IResult<&str, (&str, &str)> {
    let (i, _) = terminated(char('#'), space1)(i)?;
    let (i, _) = terminated(tag("HELP"), space1)(i)?;
    let (i, name) = terminated(identifier, space1)(i)?;
    let (i, description) = rest(i)?;
    Ok((i, (name, description)))
}

fn type_line(i: &str) -> IResult<&str, (&str, &str)> {
    all_consuming(move |i| {
        let (i, _) = terminated(char('#'), space1)(i)?;
        let (i, _) = terminated(tag("TYPE"), space1)(i)?;
        let (i, name) = terminated(identifier, space1)(i)?;
        let (i, kind) = alpha1(i)?;
        Ok((i, (name, kind)))
    })(i)
}

fn label(i: &str) -> IResult<&str, BorrowedLabel<'_>> {
    let (i, key) = identifier(i)?;
    let (i, _) = char('=')(i)?;
    let (i, (_, value, _)) = tuple((char('"'), take_while(|c: char| c != '"'), char('"')))(i)?;
    Ok((i, BorrowedLabel { key, value }))
}

fn labels(i: &str) -> IResult<&str, Vec<BorrowedLabel<'_>>> {
    separated_list(char(','), label)(i)
}

fn value(i: &str) -> IResult<&str, &str> {
    take_while(|c: char| !c.is_ascii_whitespace())(i)
}

fn sample(i: &str) -> IResult<&str, BorrowedSample<'_>> {
    all_consuming(move |i| {
        let (i, name) = identifier(i)?;
        let (i, labels) = opt(delimited(char('{'), labels, char('}')))(i)?;
        let (i, val) = preceded(space1, value)(i)?;
        let (i, timestamp) = opt(preceded(space1, value))(i)?;
        let (i, _) = space0(i)?;
        Ok((
            i,
            BorrowedSample {
                name,
                labels: labels.unwrap_or_default(),
                value: val,
                timestamp,
            },
        ))
    })(i)
}

/// If the format of the line doesn't match a `# HELP` line, this will return Ok(None); errors are
/// unused at this time.
fn parse_help(i: &str) -> Result<Option<(String, String)>, Error> {
    let (name, description) = match help_line(i) {
        Ok((_, v)) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some((name.to_string(), description.to_string())))
}

/// If the format of the line doesn't match a `# TYPE` line, this will return Ok(None); the only
/// errors it can return are failure to parse a MetricType from that part of the string
fn parse_type(i: &str) -> Result<Option<(String, MetricType)>, Error> {
    let (name, kind) = match type_line(i) {
        Ok((_, v)) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some((name.to_string(), MetricType::from_str(kind)?)))
}

/// If the format of the line doesn't match a sample line, this will return Ok(None); the only
/// errors it can return are failure to parse a Value from that part of the string
fn parse_sample(i: &str) -> Result<Option<Sample>, Error> {
    let sample = match sample(i) {
        Ok((_, v)) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some(sample.try_into()?))
}

#[derive(Debug, PartialEq)]
enum Line {
    Empty,
    Help(String, String),
    Type(String, MetricType),
    Sample(Sample),
}

impl FromStr for Line {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(Self::Empty)
        } else if let Some((name, description)) = parse_help(s)? {
            Ok(Self::Help(name, description))
        } else if let Some((name, kind)) = parse_type(s)? {
            Ok(Self::Type(name, kind))
        } else if let Some(sample) = parse_sample(s)? {
            Ok(Self::Sample(sample))
        } else {
            Err(Error::InvalidLine(s.into()))
        }
    }
}

/// Wraps an `impl Stream` with a parser that will emit metrics as they complete parsing.
///
/// Does not perform any string buffering; it is expected that `input` yields precisely one line
/// per entry, e.g. by calling `.lines()` on a [futures::AsyncBufRead].
pub fn parse<S>(input: S) -> impl Stream<Item = Result<Metric, Error>> + Unpin
where
    S: Stream<Item = Result<String, std::io::Error>> + Unpin,
{
    let stream = async_stream::stream! {
        let mut current_metric: Option<PartialMetric> = None;
        for await line in input {
            let line = line?;
            match (Line::from_str(&line)?, current_metric.is_some()) {
                (Line::Empty, _) => (),
                (Line::Help(name, description), true) => {
                    let metric = current_metric.as_mut().unwrap();
                    if(metric.name == name) {
                        metric.description = Some(description);
                    } else {
                        yield current_metric.take().unwrap().try_into();
                        current_metric = Some(PartialMetric::from_help(name, description));
                    }
                },
                (Line::Help(name, description), false) => {
                    current_metric = Some(PartialMetric::from_help(name, description));
                },
                (Line::Type(name, kind), true) => {
                    let metric = current_metric.as_mut().unwrap();
                    if(metric.name == name) {
                        metric.kind = Some(kind);
                    } else {
                        yield current_metric.take().unwrap().try_into();
                        current_metric = Some(PartialMetric::from_type(name, kind));
                    }
                },
                (Line::Type(name, kind), false) => {
                    current_metric = Some(PartialMetric::from_type(name, kind));
                },
                (Line::Sample(sample), true) => {
                    current_metric.as_mut().unwrap().samples.push(sample);
                },
                (Line::Sample(_), false) => ()
            }
        }
        if let Some(metric) = current_metric {
            yield metric.try_into();
        }
    };
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;

    use super::super::Value;
    use super::*;

    fn label<'a>(key: &'a str, value: &'a str) -> BorrowedLabel<'a> {
        BorrowedLabel { key, value }
    }

    fn sample(
        name: &str,
        labels: Vec<(&str, &str)>,
        value: Value,
        timestamp: Option<u64>,
    ) -> Sample {
        Sample {
            name: name.into(),
            labels: labels
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            value,
            timestamp,
        }
    }

    fn metric(kind: MetricType, name: &str, samples: Vec<Sample>) -> Metric {
        Metric {
            name: name.into(),
            kind,
            description: None,
            samples,
        }
    }

    #[test]
    fn line_empty() {
        let line: Line = "".parse().unwrap();
        assert_eq!(line, Line::Empty);
    }

    #[test]
    fn line_type() {
        let line = "# TYPE controller_rpc_overall_time counter"
            .parse::<Line>()
            .unwrap();
        assert_eq!(
            line,
            Line::Type("controller_rpc_overall_time".into(), MetricType::Counter)
        );
        let line = "# TYPE controller_migration_in_progress gauge"
            .parse::<Line>()
            .unwrap();
        assert_eq!(
            line,
            Line::Type("controller_migration_in_progress".into(), MetricType::Gauge)
        );
        let line = "# TYPE packet_write_propagation_time_us summary"
            .parse::<Line>()
            .unwrap();
        assert_eq!(
            line,
            Line::Type(
                "packet_write_propagation_time_us".into(),
                MetricType::Summary
            )
        );
        let line = "# TYPE something faketype".parse::<Line>();
        assert!(matches!(line, Err(Error::InvalidMetricType(_))));
    }

    #[test]
    fn line_sample() {
        let line = r#"domain_node_added{ntype="Reader",domain="2",node="0"} 1"#
            .parse::<Line>()
            .unwrap();
        assert_eq!(
            line,
            Line::Sample(sample(
                "domain_node_added",
                vec![("ntype", "Reader"), ("domain", "2"), ("node", "0")],
                Value::Integer(1),
                None
            ))
        );
        let line = r#"domain_handle_replay_time{shard="0",domain="0",tag="3",quantile="0.99"} 55.99710586500943"#.parse::<Line>().unwrap();
        assert_eq!(
            line,
            Line::Sample(sample(
                "domain_handle_replay_time",
                vec![
                    ("shard", "0"),
                    ("domain", "0"),
                    ("tag", "3"),
                    ("quantile", "0.99")
                ],
                Value::Float(55.99710586500943),
                None
            ))
        );
        let line = r#"domain_total_forward_time_us{shard="0",domain="0",from_node="0",to_node="0"} 4310176"#.parse::<Line>().unwrap();
        assert_eq!(
            line,
            Line::Sample(sample(
                "domain_total_forward_time_us",
                vec![
                    ("shard", "0"),
                    ("domain", "0"),
                    ("from_node", "0"),
                    ("to_node", "0")
                ],
                Value::Integer(4310176),
                None
            ))
        );
        let line =
            r#"controller_rpc_request_time_sum{path="/view_builder"} GOTTEM"#.parse::<Line>();
        assert!(matches!(line, Err(Error::InvalidValue(_))));
        let line =
			r#"replicator_snapshot_status{instance="localhost:9000",job="readyset-server",status="started"} 1 163889938.2418"#.parse::<Line>();
        assert!(matches!(line, Err(Error::InvalidTimestamp(_))));
    }

    #[test]
    fn line_invalid() {
        let line = "wat".parse::<Line>();
        assert!(matches!(line, Err(Error::InvalidLine(_))));
    }

    #[test]
    fn single_label() {
        let (_, parsed) = super::label("shard=\"0\"").unwrap();
        assert_eq!(parsed, label("shard", "0"));
        let (_, parsed) = super::label("quantile=\"0.99\"").unwrap();
        assert_eq!(parsed, label("quantile", "0.99"));
    }

    #[tokio::test]
    async fn stream_noria() {
        use super::super::MetricType::*;
        use super::super::Value::*;
        let input = include_str!("test/noria-prometheus.txt");
        let mut input = parse(futures::stream::iter(
            input
                .lines()
                .map(|s| Result::<_, std::io::Error>::Ok(s.to_string())),
        ));
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "base_table_lookup_requests",
                vec![sample(
                    "base_table_lookup_requests",
                    vec![("domain", "0"), ("node", "l0")],
                    Integer(1),
                    None
                )]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "egress_sent_packets",
                vec![
                    sample(
                        "egress_sent_packets",
                        vec![("node", "4")],
                        Integer(1001),
                        None
                    ),
                    sample("egress_sent_packets", vec![("node", "9")], Integer(4), None),
                    sample(
                        "egress_sent_packets",
                        vec![("node", "14")],
                        Integer(4),
                        None
                    ),
                ]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "server_external_requests",
                vec![sample(
                    "server_external_requests",
                    vec![],
                    Integer(21710),
                    None
                )]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "domain_total_forward_time_us",
                vec![
                    sample(
                        "domain_total_forward_time_us",
                        vec![
                            ("shard", "0"),
                            ("domain", "0"),
                            ("from_node", "0"),
                            ("to_node", "0")
                        ],
                        Integer(4310176),
                        None
                    ),
                    sample(
                        "domain_total_forward_time_us",
                        vec![
                            ("shard", "0"),
                            ("domain", "1"),
                            ("from_node", "0"),
                            ("to_node", "1")
                        ],
                        Integer(42865),
                        None
                    ),
                ]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "server_view_query_result",
                vec![
                    sample(
                        "server_view_query_result",
                        vec![("result", "replay")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "server_view_query_result",
                        vec![("result", "served_from_cache")],
                        Integer(2),
                        None
                    ),
                ]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "domain_reader_total_replay_request_time_us",
                vec![sample(
                    "domain_reader_total_replay_request_time_us",
                    vec![("shard", "0"), ("domain", "1"), ("node", "0")],
                    Integer(31),
                    None
                )]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "domain_total_finish_replay_time_us",
                vec![
                    sample(
                        "domain_total_finish_replay_time_us",
                        vec![("shard", "0"), ("domain", "2"), ("tag", "2")],
                        Integer(35),
                        None
                    ),
                    sample(
                        "domain_total_finish_replay_time_us",
                        vec![("shard", "0"), ("domain", "3"), ("tag", "3")],
                        Integer(52),
                        None
                    ),
                ]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "domain_node_added",
                vec![
                    sample(
                        "domain_node_added",
                        vec![
                            ("ntype", "Internal (Project)"),
                            ("domain", "0"),
                            ("node", "4")
                        ],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![
                            ("ntype", "Internal (Project)"),
                            ("domain", "0"),
                            ("node", "1")
                        ],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Ingress"), ("domain", "2"), ("node", "1")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Reader"), ("domain", "3"), ("node", "0")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Base"), ("domain", "0"), ("node", "0")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![
                            ("ntype", "Internal (Project)"),
                            ("domain", "0"),
                            ("node", "7")
                        ],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Egress"), ("domain", "0"), ("node", "5")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Reader"), ("domain", "2"), ("node", "0")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Reader"), ("domain", "1"), ("node", "0")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Egress"), ("domain", "0"), ("node", "2")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![
                            ("ntype", "Internal (Filter)"),
                            ("domain", "0"),
                            ("node", "3")
                        ],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Ingress"), ("domain", "3"), ("node", "1")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Ingress"), ("domain", "1"), ("node", "1")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![("ntype", "Egress"), ("domain", "0"), ("node", "8")],
                        Integer(1),
                        None
                    ),
                    sample(
                        "domain_node_added",
                        vec![
                            ("ntype", "Internal (Filter)"),
                            ("domain", "0"),
                            ("node", "6")
                        ],
                        Integer(1),
                        None
                    ),
                ]
            )
        );
        assert_eq!(
            input.next().await.unwrap().unwrap(),
            metric(
                Counter,
                "domain_total_chunked_replay_time_us",
                vec![
                    sample(
                        "domain_total_chunked_replay_time_us",
                        vec![("shard", "0"), ("domain", "0"), ("from_node", "3")],
                        Integer(74),
                        None
                    ),
                    sample(
                        "domain_total_chunked_replay_time_us",
                        vec![("shard", "0"), ("domain", "0"), ("from_node", "6")],
                        Integer(74),
                        None
                    ),
                ]
            )
        );
        assert!(input.next().await.is_none());
    }

    #[tokio::test]
    async fn stream_noria_roundtrip() {
        let input = include_str!("test/noria-prometheus.txt");
        let mut stream = parse(futures::stream::iter(
            input
                .lines()
                .map(|s| Result::<_, std::io::Error>::Ok(s.to_string())),
        ));
        let mut output = Vec::new();
        while let Some(metric) = stream.next().await {
            output.push(metric.unwrap().to_string());
        }
        // NOTE:  This only works because all the labels in noria-prometheus.txt are already sorted
        assert_eq!(input, output.join(""));
    }

    #[tokio::test]
    async fn stream_prometheus_roundtrip() {
        let input = include_str!("test/prometheus-internal.txt");
        let mut stream = parse(futures::stream::iter(
            input
                .lines()
                .map(|s| Result::<_, std::io::Error>::Ok(s.to_string())),
        ));
        let mut output = Vec::new();
        while let Some(metric) = stream.next().await {
            output.push(metric.unwrap().to_string());
        }
        // NOTE:  This works because none of the metrics in prometheus-intenral.txt have labels,
        // and was manually changed to remove scientific notation and add newlines between metrics
        assert_eq!(input, output.join(""));
    }
}
