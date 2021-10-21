use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;
use std::{collections::HashMap, iter::FromIterator, time::Duration};

/// A list of allowed queries along with relevant query latency metrics.
#[derive(Serialize, Debug, PartialEq, Eq, Default)]
pub struct AllowList(HashMap<String, ComparativeQueryLatencies>);

impl AllowList {
    pub fn new() -> AllowList {
        AllowList(HashMap::new())
    }
}

impl FromIterator<QueryWithLatencies> for AllowList {
    fn from_iter<I: IntoIterator<Item = QueryWithLatencies>>(iter: I) -> Self {
        let inner = iter
            .into_iter()
            .map(|a| (a.anonymized_query, a.comparative_latencies))
            .collect();
        AllowList(inner)
    }
}

/// Query latencies for both noria and upstream for a given query. Used primarily to provide a
/// clean way to display the aggregated allow-list to an end consumer as json.
#[derive(Serialize, Debug, PartialEq, Eq, Default)]
pub struct ComparativeQueryLatencies {
    /// Noria latency metrics for the allowed query.
    pub noria_latencies: Option<QueryLatencies>,
    /// Upstream (mysql/postgres) latency metrics for the allowed query.
    pub upstream_latencies: Option<QueryLatencies>,
}

/// Represents an query along with query latency metrics.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct QueryWithLatencies {
    /// The anonymized query that we are allowing.
    pub anonymized_query: String,
    /// Query latencies for the given query from both Noria and upstream.
    pub comparative_latencies: ComparativeQueryLatencies,
}

impl QueryWithLatencies {
    pub fn new(
        anonymized_query: String,
        noria_latencies: Option<QueryLatencies>,
        upstream_latencies: Option<QueryLatencies>,
    ) -> QueryWithLatencies {
        QueryWithLatencies {
            anonymized_query,
            comparative_latencies: ComparativeQueryLatencies {
                noria_latencies,
                upstream_latencies,
            },
        }
    }
}

type Percentile = u8;

/// Latency metrics that should be related to a single given type of query. By type of query we
/// mean a query that has had its literals anonymized, and therefore may match up with other
/// queries that have had their literals anonymized.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct QueryLatencies(HashMap<Percentile, Duration>);

impl Serialize for QueryLatencies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, &format!("{}ms", v.as_millis()))?;
        }
        map.end()
    }
}

#[derive(Default, Debug)]
pub struct QueryLatenciesBuilder {
    noria: LatenciesBuilder,
    upstream: LatenciesBuilder,
}

#[derive(Default, Debug)]
struct LatenciesBuilder {
    p99: Option<Duration>,
    p95: Option<Duration>,
    p90: Option<Duration>,
    p50: Option<Duration>,
}

pub enum Database {
    Noria,
    Upstream,
}

impl QueryLatenciesBuilder {
    pub fn new() -> QueryLatenciesBuilder {
        QueryLatenciesBuilder {
            noria: Default::default(),
            upstream: Default::default(),
        }
    }

    /// Consumes the QueryLatenciesBuilder returning a tuple of query latencies for noria and
    /// upstream respectively.
    pub fn into_query_latencies(mut self) -> (Option<QueryLatencies>, Option<QueryLatencies>) {
        let noria = self.noria_query_latencies();
        let upstream = self.upstream_query_latencies();
        (noria, upstream)
    }

    /// Returns noria QueryLatencies if all fields were
    /// filled in, and otherwise returns None.
    pub fn noria_query_latencies(&mut self) -> Option<QueryLatencies> {
        match (
            self.noria.p99.take(),
            self.noria.p95.take(),
            self.noria.p90.take(),
            self.noria.p50.take(),
        ) {
            (Some(p99), Some(p95), Some(p90), Some(p50)) => Some(QueryLatencies(HashMap::from([
                (99, p99),
                (95, p95),
                (90, p90),
                (50, p50),
            ]))),
            _ => None,
        }
    }

    /// Returns upstream QueryLatencies if all fields were
    /// filled in, and otherwise returns None.
    pub fn upstream_query_latencies(&mut self) -> Option<QueryLatencies> {
        match (
            self.upstream.p99.take(),
            self.upstream.p95.take(),
            self.upstream.p90.take(),
            self.upstream.p50.take(),
        ) {
            (Some(p99), Some(p95), Some(p90), Some(p50)) => Some(QueryLatencies(HashMap::from([
                (99, p99),
                (95, p95),
                (90, p90),
                (50, p50),
            ]))),
            _ => None,
        }
    }

    pub fn set_p99(&mut self, p99: Duration, database: Database) {
        match database {
            Database::Noria => self.noria.p99 = Some(p99),
            Database::Upstream => self.upstream.p99 = Some(p99),
        }
    }

    pub fn set_p95(&mut self, p95: Duration, database: Database) {
        match database {
            Database::Noria => self.noria.p95 = Some(p95),
            Database::Upstream => self.upstream.p95 = Some(p95),
        }
    }

    pub fn set_p90(&mut self, p90: Duration, database: Database) {
        match database {
            Database::Noria => self.noria.p90 = Some(p90),
            Database::Upstream => self.upstream.p90 = Some(p90),
        }
    }

    pub fn set_p50(&mut self, p50: Duration, database: Database) {
        match database {
            Database::Noria => self.noria.p50 = Some(p50),
            Database::Upstream => self.upstream.p50 = Some(p50),
        }
    }
}

/// Stores the subset of queries returned from the readyset-adapter's /allow-list
/// and /deny-list endpoints and their respective execution latencies across
/// several percentiles (see QueryLatencies).
#[derive(Default)]
pub struct QueryMetricsCache {
    /// Holds the current allow list, aggregated across all adapters in the system.
    pub allow_list: AllowList,
    /// Holds the current deny list, aggregated across all adapters in the system.
    pub deny_list: Vec<String>,
}

impl QueryMetricsCache {
    pub fn new() -> QueryMetricsCache {
        QueryMetricsCache {
            allow_list: AllowList::new(),
            deny_list: vec![],
        }
    }
}
