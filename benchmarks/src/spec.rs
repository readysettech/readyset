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
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use database_utils::{DatabaseConnection, QueryableConnection};
use nom_sql::{DialectDisplay, SqlQuery};
use rand::Rng;
use rand_distr::{Uniform, WeightedAliasIndex};
use readyset_data::DfValue;
use serde::{Deserialize, Serialize, Serializer};
use tracing::info;
use zipf::ZipfDistribution;

use crate::workload_emulator::{ColGenerator, Distributions, Query, QuerySet, Sampler};

/// A Noria/upstream workload specification, consisting of a list of distributions that specify how
/// to generate data for the workload, and a list of weighted queries to run
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkloadSpec {
    pub distributions: Vec<WorkloadDistribution>,
    pub queries: Vec<WorkloadQuery>,
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
    #[serde(deserialize_with = "serde_with::rust::display_fromstr::deserialize")]
    #[serde(serialize_with = "serialize_sql_type")]
    pub sql_type: nom_sql::SqlType,
    pub distribution: String,
    #[serde(default)]
    pub col: usize,
}

fn serialize_sql_type<S: Serializer>(
    sql_type: &nom_sql::SqlType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&sql_type.display(nom_sql::Dialect::MySQL).to_string())
}

impl WorkloadSpec {
    pub fn from_yaml(yaml: &str) -> Result<Self, impl std::error::Error> {
        serde_yaml::from_str(yaml)
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
                    let mut rng = rand::thread_rng();
                    range
                        .clone()
                        .map(|n| {
                            vec![
                                DfValue::from(n),
                                DfValue::from(n + rng.gen_range(distance.clone())),
                            ]
                        })
                        .collect()
                }
                WorkloadDistributionSource::Query { query } => {
                    // Make sure we don't get a timeout when inserting into the database
                    conn.query_drop("SET SESSION MAX_EXECUTION_TIME=0").await?;

                    conn.query(query).await?.try_into()?
                }
            };

            info!("Generated distribution {name}, rows {}", data.len());

            let generator = match distribution {
                WorkloadDistributionKind::Uniform { .. } => {
                    Sampler::Uniform(Uniform::new(0, data.len() - 1))
                }
                WorkloadDistributionKind::Zipf { zipf } => {
                    Sampler::Zipf(ZipfDistribution::new(data.len() - 1, *zipf).unwrap())
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
                let stmt = match spec.parse::<SqlQuery>() {
                    Ok(SqlQuery::Select(stmt)) => stmt,
                    _ => panic!("Can only migrate SELECT statements"),
                };

                let create_cache_query = nom_sql::CreateCacheStatement {
                    name: None,
                    inner: Ok(nom_sql::CacheInner::Statement(Box::new(stmt))),
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
            })
        }

        Ok(QuerySet { weights, queries })
    }
}

#[cfg(test)]
mod test {
    use nom_sql::SqlType;

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
"#;

        let _: WorkloadSpec = serde_yaml::from_str(spec).unwrap();
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
        };

        let s = serde_yaml::to_string(&spec).unwrap();
        let der: WorkloadSpec = serde_yaml::from_str(&s).unwrap();
        assert_eq!(der, spec);
    }
}
