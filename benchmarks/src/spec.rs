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
//!   * `uniform` - Empty. Means data will be sampled uniformely.  Mutually exclusive with zipf.
//!   * `range` - Data will be sampled from an integer (i64) range.
//!     * `start` - Integer. Range start, inclusive
//!     * `end` - Integer. Range end, exclusive.
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
//! ```
//!
//! * `queries` - A list of queries to run.
//!   * `spec` - String. The query to run.
//!   * `params` - List of parameteres to pass to the query. Each parameter corresponds to a single
//!     `?` placeholder in spec. The number of parameters must be equal to the number of
//!     placeholders.
//!     * `sql_type` - String. The SQL type to coerce data to before making the query. I.e. int,
//!       char.
//!     * `distribution` - String. Name of one of the distributions from the `distributions`
//!       section.
//!     * `col` - Integer. The column of the data to use from the distribution, if the rows contain
//!       more than one column. There is a subtelty here. The data is sampled only when the column
//!       `0` is specified for a distribution, so for example using `col: 0` twice will sample two
//!       rows from the dataset, however uing `col: 0` followed by `col: 1` will reuse the same row.
//!   * `weight` - Integer. The absolute weight of the query. I.e. how often this query will be run.
//!     The relative weight is computed out of the total weight of the queries.
//!   * `migrate` - Boolean. Specifies if `CREATE CACHED QUERY` should be run for this query before
//!     the benchmark begins.
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
use serde::{Deserialize, Serialize};

/// A Noria/MySQL workload specification, consisting of a list of distributions that specify how to
/// generate data for the workload, and a list of weighted queries to run
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkloadQuery {
    /// The query to run
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub spec: nom_sql::SqlQuery,
    /// The list of parameters to generate for each instance of the query
    pub params: Vec<WorkloadParam>,
    /// Specify whever to call `CREATE CACHED QUERY` for this query
    pub migrate: bool,
    /// The absolute weight of this query. The relative weight will depend on the total weight of
    /// all queries.
    pub weight: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum WorkloadDistributionSource {
    Range { range: std::ops::Range<i64> },
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkloadParam {
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub sql_type: nom_sql::SqlType,
    pub distribution: String,
    #[serde(default)]
    pub col: usize,
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
                        sql_type: SqlType::Bigint(None),
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
