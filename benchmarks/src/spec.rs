//! This module contains [`WorkloadSpec`], that specifies the yaml schema for the workload generator
//! benchmark.
//!
//! The schema is as follows:
//!
//! * `distributions` - A list of named distributions to use. A distribution is essentially the
//!   source of data to be used for the workloads.
//!   * `name` - String. The name of the distribution, that may be referenced by queries.
//!   * `zipf` - Float. Means data will be sampled with a zipfian distribution, with the given
//!     parameter. Mutually exclusive with uniform.
//!   * `uniform` - Empty. Means data will be sampled uniformly.  Mutually exclusive with zipf.
//!   * `range` - Data will be sampled from an integer (i64) range.
//!     * `start` - Integer. Range start, inclusive
//!     * `end` - Integer. Range end, exclusive.
//!   * `pair` - First column will be sampled from an integer range, and the second column will be
//!     sampled from the distance parameter and added to the first value. Useful to generate
//!     parameters for a between statement.
//!     * `range` - First column will be sampled from an integer (i64) range.
//!       * `start` - Integer. Range start, inclusive
//!       * `end` - Integer. Range end, exclusive.
//!     * `distance` - Second column will be the value of the first column plus a value sampled from
//!       an integer (i64) range. Always uniformly distributed in range.
//!       * `start` - Integer. Distance start, inclusive
//!       * `end` - Integer. Distance end, exclusive.
//!   * `query` - String. Data will be sampled from the results of a SQL query, the query is run on
//!     the setup database and the results are stored in a vector of rows.
//!
//! Example:
//! ```yaml
//! distributions:
//! - name: ids
//!   range:
//!     start: 0
//!     end: 10000
//!   uniform:
//! - name: auth_token
//!   zipf: 1.15
//!   query: select auth_token from user_auth_tokens
//! - name: between
//!   pair:
//!     range:
//!       start: 0
//!       end: 1000000
//!     distance:
//!       start: 1
//!       end: 200
//! ```
//!
//! * `queries` - A list of queries to run.
//!   * `spec` - String. The query to run.
//!   * `params` - List of parameters to pass to the query. Each parameter corresponds to a single
//!     `?` placeholder in spec. The number of parameters must be equal to the number of
//!     placeholders.
//!     * `sql_type` - String. The SQL type to coerce data to before making the query. I.e. int,
//!       char.
//!     * `distribution` - String. Name of one of the distributions from the `distributions`
//!       section.
//!     * `col` - Integer. The column of the data to use from the distribution, if the rows contain
//!       more than one column. There is a subtlety here. The data is sampled only when the column
//!       `0` is specified for a distribution, so for example using `col: 0` twice will sample two
//!       rows from the dataset, however uing `col: 0` followed by `col: 1` will reuse the same row.
//!   * `weight` - Integer. The absolute weight of the query. I.e. how often this query will be run.
//!     The relative weight is computed out of the total weight of the queries.
//!   * `migrate` - Boolean. Specifies if `CREATE CACHE` should be run for this query before the
//!     benchmark begins.
//!
//! Example:
//! ```yaml
//! queries:
//!   - spec: select * from users where users.id in ( ?, ?, ? )
//!     params:
//!     - sql_type: bigint
//!       distribution: ids
//!     - sql_type: bigint
//!       distribution: ids
//!     - sql_type: bigint
//!       distribution: ids
//!     weight: 2000000
//!     migrate: true
//!   - spec: select * from invites_users where invites_users.invite_id in ( ? ) and invites_users.user_id = ?
//!     params:
//!     - sql_type: bigint
//!       distribution: invites
//!       col: 0
//!     - sql_type: bigint
//!       distribution: invites
//!       col: 1
//!     weight: 500000
//!     migrate: true
//! ```
//!
//! * `setup` - A list of SQL statements to run on the connection before the workload is started
//!
//! Example:
//! ```yaml
//! setup:
//!   - SET NAMES latin1
//! ```
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use database_utils::{DatabaseConnection, DatabaseStatement, QueryableConnection};
use rand::distr::Uniform;
use rand::Rng;
use rand_distr::weighted::WeightedAliasIndex;
use rand_distr::{Distribution, Zipf};
use readyset_data::{DfType, DfValue};
use readyset_sql::{ast::*, Dialect, DialectDisplay};
use readyset_sql_parsing::{parse_query_with_config, ParsingConfig, ParsingPreset};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::info;

pub type Distributions = HashMap<String, Arc<(Vec<Vec<DfValue>>, Sampler)>>;

/// A Noria/upstream workload specification, consisting of a list of distributions that specify how
/// to generate data for the workload, and a list of weighted queries to run
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkloadSpec {
    pub distributions: Vec<WorkloadDistribution>,
    pub queries: Vec<WorkloadQuery>,
    #[serde(default)]
    pub setup: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkloadDistribution {
    /// The name of the distribution, to be referenced by the queries later
    pub name: String,
    /// How the data for the distribution is generated (statically or by querying a db)
    #[serde(flatten)]
    pub from: WorkloadDistributionSource,
    /// How the data is sampled (uniform or zipf with an alpha parameter, default is uniform)
    #[serde(flatten, default)]
    pub distribution: WorkloadDistributionKind,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadQuery {
    /// The query to run
    pub spec: String,
    /// The list of parameters to generate for each instance of the query
    pub params: Vec<WorkloadParam>,
    /// Specify wherever to call `CREATE CACHE` for this query
    pub migrate: bool,
    /// The absolute weight of this query. The relative weight will depend on the total weight of
    /// all queries.
    pub weight: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pair {
    pub range: Range<i64>,
    pub distance: Range<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum WorkloadDistributionSource {
    Range { range: Range<i64> },
    Pair { pair: Pair },
    Query { query: String },
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum WorkloadDistributionKind {
    Uniform {
        // This field is unfortunate, but needed for a nice flatten
        uniform: (),
    },
    Zipf {
        zipf: f64,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadParam {
    #[serde(deserialize_with = "deserialize_sql_type")]
    #[serde(serialize_with = "serialize_sql_type")]
    pub sql_type: SqlType,
    pub distribution: String,
    #[serde(default)]
    pub col: usize,
}

/// The type of cache to create.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QueryCacheType {
    /// A deep cache.
    #[default]
    Deep,
    /// A shallow cache with `Duration` TTL.
    Shallow(Duration),
}

impl QueryCacheType {
    fn cache_type(&self) -> CacheType {
        match self {
            QueryCacheType::Deep => CacheType::Deep,
            QueryCacheType::Shallow(_) => CacheType::Shallow,
        }
    }

    fn policy(&self) -> Option<EvictionPolicy> {
        match self {
            QueryCacheType::Deep => None,
            QueryCacheType::Shallow(duration) => Some(EvictionPolicy::Ttl { ttl: *duration }),
        }
    }
}

impl std::str::FromStr for QueryCacheType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deep" => Ok(QueryCacheType::Deep),
            s if s.starts_with("shallow:") => {
                let duration_str = s.strip_prefix("shallow:").unwrap();
                let seconds: u64 = duration_str
                    .parse()
                    .map_err(|_| format!("Invalid duration for shallow cache: {duration_str}"))?;
                Ok(QueryCacheType::Shallow(Duration::from_secs(seconds)))
            }
            _ => Err(format!(
                "Invalid cache type: '{}'. Expected 'deep' or 'shallow:<seconds>'",
                s
            )),
        }
    }
}

fn serialize_sql_type<S: Serializer>(sql_type: &SqlType, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&sql_type.display(readyset_sql::Dialect::MySQL).to_string())
}

fn deserialize_sql_type<'de, D>(deserializer: D) -> Result<SqlType, D::Error>
where
    D: Deserializer<'de>,
{
    readyset_sql_parsing::parse_sql_type(Dialect::MySQL, String::deserialize(deserializer)?)
        .map_err(serde::de::Error::custom)
}

impl WorkloadSpec {
    pub fn from_yaml(yaml: &str) -> Result<Self, impl std::error::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    pub async fn load_distributions(
        &self,
        conn: &mut DatabaseConnection,
    ) -> anyhow::Result<Distributions> {
        let mut distributions = HashMap::new();

        for WorkloadDistribution {
            name,
            from,
            distribution,
        } in &self.distributions
        {
            info!("Generating distribution {name}");

            let data: Vec<Vec<DfValue>> = match from {
                WorkloadDistributionSource::Range { range } => {
                    range.clone().map(|n| vec![DfValue::from(n)]).collect()
                }
                WorkloadDistributionSource::Pair {
                    pair: Pair { range, distance },
                } => {
                    let mut rng = rand::rng();
                    range
                        .clone()
                        .map(|n| {
                            vec![
                                DfValue::from(n),
                                DfValue::from(n + rng.random_range(distance.clone())),
                            ]
                        })
                        .collect()
                }
                WorkloadDistributionSource::Query { query } => {
                    conn.query(query).await?.try_into()?
                }
            };

            info!("Generated distribution {name}, rows {}", data.len());

            let generator = match distribution {
                WorkloadDistributionKind::Uniform { .. } => {
                    Sampler::Uniform(Uniform::new(0, data.len() - 1)?)
                }
                WorkloadDistributionKind::Zipf { zipf } => {
                    Sampler::Zipf(Zipf::new((data.len() - 1) as _, *zipf).unwrap())
                }
            };

            distributions.insert(name.clone(), Arc::new((data, generator)));
        }

        Ok(distributions)
    }

    pub async fn load_queries(
        &self,
        distributions: &Distributions,
        conn: &mut DatabaseConnection,
    ) -> anyhow::Result<QuerySet> {
        let parsing_config = ParsingPreset::for_tests().into_config();
        self.load_queries_with_config(distributions, conn, parsing_config, Default::default())
            .await
    }

    pub async fn load_queries_with_config(
        &self,
        distributions: &Distributions,
        conn: &mut DatabaseConnection,
        parsing_config: ParsingConfig,
        query_cache_type: QueryCacheType,
    ) -> anyhow::Result<QuerySet> {
        let weights =
            WeightedAliasIndex::new(self.queries.iter().map(|q| q.weight).collect()).unwrap();
        let mut queries = Vec::with_capacity(self.queries.len());

        for (
            i,
            WorkloadQuery {
                spec,
                params,
                migrate,
                ..
            },
        ) in self.queries.iter().enumerate()
        {
            if *migrate {
                let stmt = match parse_query_with_config(parsing_config, conn.dialect(), spec) {
                    Ok(SqlQuery::Select(stmt)) => stmt,
                    _ => panic!("Can only migrate SELECT statements"),
                };

                let create_cache_query = CreateCacheStatement {
                    name: None,
                    cache_type: Some(query_cache_type.cache_type()),
                    policy: query_cache_type.policy(),
                    coalesce_ms: None,
                    inner: CacheInner::Statement(Ok(Box::new(stmt))),
                    unparsed_create_cache_statement: None,
                    always: false,
                    concurrently: false,
                };

                let _ = conn
                    .query_drop(create_cache_query.display(conn.dialect()).to_string())
                    .await;
            }

            let cols = params
                .iter()
                .map(|p| {
                    let dist = distributions[p.distribution.as_str()].clone();
                    ColGenerator {
                        dist,
                        sql_type: p.sql_type.clone(),
                        col: p.col,
                    }
                })
                .collect();

            queries.push(Query {
                idx: i,
                spec: spec.clone(),
                cols,
                migrate: *migrate,
                dialect: conn.dialect().into(),
            })
        }

        Ok(QuerySet { weights, queries })
    }
}

/// A query with its index and generator
#[derive(Debug)]
pub struct Query {
    pub(crate) spec: String,
    /// The index of this query within the enclosing `QuerySet`.
    pub idx: usize,
    pub(crate) cols: Vec<ColGenerator>,
    pub migrate: bool,
    dialect: readyset_data::Dialect,
}

impl Query {
    /// Get params for this query in a specific index
    pub fn get_params_index(&self, index: usize) -> Option<Vec<DfValue>> {
        if self.cols.is_empty() {
            return None;
        }

        let mut ret = Vec::with_capacity(self.cols.len());
        let mut last_row: &Vec<DfValue> = &vec![];
        let mut last_set: Option<Arc<_>> = None;

        for ColGenerator {
            dist,
            sql_type,
            col,
        } in &self.cols
        {
            if *col == 0
                || last_set
                    .as_ref()
                    .map(|s| !Arc::ptr_eq(dist, s))
                    .unwrap_or(false)
            {
                last_set = Some(dist.clone());
                last_row = dist.0.get(index)?;
            }

            let target_type =
                DfType::from_sql_type(sql_type, self.dialect, |_| None, None).unwrap();

            ret.push(
                last_row[*col]
                    .coerce_to(&target_type, &DfType::Unknown) // No from_ty, we're dealing with literal params
                    .unwrap(),
            )
        }

        Some(ret)
    }

    pub fn get_params(&self) -> Vec<DfValue> {
        let mut ret = Vec::with_capacity(self.cols.len());
        let mut rng = rand::rng();

        let mut last_row: &Vec<DfValue> = &vec![];
        let mut last_set: Option<Arc<_>> = None;

        for ColGenerator {
            dist,
            sql_type,
            col,
        } in &self.cols
        {
            if *col == 0
                || last_set
                    .as_ref()
                    .map(|s| !Arc::ptr_eq(dist, s))
                    .unwrap_or(false)
            {
                last_set = Some(dist.clone());
                last_row = &dist.0[dist.1.sample(&mut rng)];
            }

            let target_type =
                DfType::from_sql_type(sql_type, self.dialect, |_| None, None).unwrap();

            ret.push(
                last_row[*col]
                    .coerce_to(&target_type, &DfType::Unknown) // No from_ty, we're dealing with literal params
                    .unwrap(),
            )
        }

        ret
    }
}

/// A vector of queries and weights
#[derive(Debug)]
pub struct QuerySet {
    pub(crate) queries: Vec<Query>,
    pub(crate) weights: WeightedAliasIndex<usize>,
}

impl QuerySet {
    pub async fn prepare_all(
        &self,
        conn: &mut DatabaseConnection,
    ) -> anyhow::Result<Vec<DatabaseStatement>> {
        let mut prepared = Vec::with_capacity(self.queries.len());
        for query in self.queries.iter() {
            prepared.push(conn.prepare(query.spec.to_string()).await?);
        }
        Ok(prepared)
    }

    pub fn get_query(&self) -> &Query {
        if self.queries.len() == 1 {
            return &self.queries[0];
        }

        let mut rng = rand::rng();
        &self.queries[self.weights.sample(&mut rng)]
    }

    pub fn queries(&self) -> &[Query] {
        &self.queries
    }
}

#[derive(Debug)]
pub enum Sampler {
    Zipf(Zipf<f64>),
    Uniform(Uniform<usize>),
}

impl Sampler {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Sampler::Zipf(z) => z.sample(rng).round() as _,
            Sampler::Uniform(u) => u.sample(rng),
        }
    }
}

/// Generates parameter data for a single placeholder in the query
#[derive(Debug)]
pub(crate) struct ColGenerator {
    pub(crate) dist: Arc<(Vec<Vec<DfValue>>, Sampler)>,
    pub(crate) sql_type: SqlType,
    pub(crate) col: usize,
}

#[cfg(test)]
mod test {
    use SqlType;

    use super::*;

    #[test]
    fn deserialize() {
        let spec = r#"
distributions:
    - name: ids
      range:
        start: 0
        end: 10000
      uniform:
    - name: auth_token
      zipf: 1.15
      query: select auth_token from user_auth_tokens
    - name: invites
      zipf: 1.15
      query: select invite_id, user_id from invites_users
queries:
    - spec: select * from users where users.id in ( ?, ?, ? )
      params:
        - sql_type: bigint
          distribution: ids
        - sql_type: bigint
          distribution: ids
        - sql_type: bigint
          distribution: ids
      weight: 2000000
      migrate: true
    - spec: select * from user_auth_tokens where auth_token = ?
      params:
        - sql_type: char
          distribution: auth_token
          col: 0
      weight: 1000000
      migrate: true
    - spec: select * from invites_users where invites_users.invite_id in ( ? ) and invites_users.user_id = ?
      params:
        - sql_type: bigint
          distribution: invites
          col: 0
        - sql_type: bigint
          distribution: invites
          col: 1
      weight: 500000
      migrate: true
    - spec: update users set updated_at = now() where id = ?
      params:
        - sql_type: bigint
          distribution: ids
      weight: 10000
      migrate: false
setup:
    - SET NAMES latin1
    - SET @@character_set_results = utf8mb4
"#;

        let _: WorkloadSpec = serde_yaml_ng::from_str(spec).unwrap();
    }

    #[test]
    fn serde() {
        let spec = WorkloadSpec {
            distributions: [
                WorkloadDistribution {
                    name: "ids".into(),
                    from: WorkloadDistributionSource::Query {
                        query: "asdsads".into(),
                    },
                    distribution: WorkloadDistributionKind::Uniform { uniform: () },
                },
                WorkloadDistribution {
                    name: "ids2".into(),
                    from: WorkloadDistributionSource::Range { range: 0..500 },
                    distribution: WorkloadDistributionKind::Zipf { zipf: 0.4 },
                },
            ]
            .into(),

            queries: [WorkloadQuery {
                spec: "select * from users where users.id in ( ?, ?, ? )"
                    .parse()
                    .unwrap(),
                params: vec![
                    WorkloadParam {
                        sql_type: SqlType::BigInt(None),
                        distribution: "ids".into(),
                        col: 0,
                    },
                    WorkloadParam {
                        sql_type: SqlType::Char(None),
                        distribution: "ids".into(),
                        col: 1,
                    },
                ],
                migrate: true,
                weight: 200_000,
            }]
            .into(),

            setup: vec![
                "SET NAMES latin1".into(),
                "SET @@character_set_results = utf8mb4".into(),
            ],
        };

        let s = serde_yaml_ng::to_string(&spec).unwrap();
        let der: WorkloadSpec = serde_yaml_ng::from_str(&s).unwrap();
        assert_eq!(der, spec);
    }
}
