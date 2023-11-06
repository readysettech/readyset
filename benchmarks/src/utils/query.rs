//! Utilities for working with parameterized queries. Benchmarks that
//! execute arbitrary parameterized queries may benefit from these
//! definitions and these utilities.
//!
//! The core of these utilities is the PreparedStatement struct. This
//! packages a parameterized prepared statement we intend to execute
//! for a benchmark, with "how" to generate the parameters for this
//! statement. Each parameter in a prepared statement is generated
//! based on a DistributionAnnotation.

use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::Parser;
use data_generator::{ColumnGenerator, DistributionAnnotation};
use database_utils::{DatabaseConnection, DatabaseStatement, QueryableConnection};
use nom_sql::{Dialect, DialectDisplay, Literal, SqlType};
use readyset_data::DfValue;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::utils::path::benchmark_path;

/// The number of times we will try to generate a cache miss using the random
/// generator before giving up. It is possible that we have generated cache hits
/// on all values in the table, and as a result, will no longer be able to
/// generate misses.
const MAX_RANDOM_GENERATIONS: u32 = 100;

/// A wrapper around a PathBuf that eagerly caches the query when constructed
#[derive(Clone)]
pub struct QueryFile {
    path: PathBuf,
    query: String,
}

impl Serialize for QueryFile {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        self.path.serialize(ser)
    }
}

impl<'de> Deserialize<'de> for QueryFile {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let path = PathBuf::deserialize(de)?;
        Self::try_from(path).map_err(D::Error::custom)
    }
}

impl TryFrom<PathBuf> for QueryFile {
    type Error = anyhow::Error;
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let query = fs::read_to_string(benchmark_path(&path)?)
            .map_err(|e| anyhow!("Could not load query from file: {}", e))?;
        Ok(Self { path, query })
    }
}

impl FromStr for QueryFile {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PathBuf::from(s).try_into()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum QuerySpec {
    /// A path to a file containing the query that we are benchmarking
    #[serde(rename = "file")]
    File(QueryFile),
    /// A plain-text query
    #[serde(rename = "query")]
    Query(String),
}

impl Default for QuerySpec {
    fn default() -> Self {
        Self::Query(String::default())
    }
}

impl FromStr for QuerySpec {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO(mc):  Once Postgres is better supported by benchmarks, try parsing it both ways
        match nom_sql::parse_query(nom_sql::Dialect::MySQL, s) {
            Ok(_) => Ok(Self::Query(s.to_owned())),
            Err(_) => Ok(Self::File(QueryFile::from_str(s).map_err(|e| anyhow!("Could not parse '{}' as a query; attempted to load a file at that path, but failed:  {}", s, e))?)),
        }
    }
}

impl QuerySpec {
    pub fn query(&self) -> &str {
        match self {
            QuerySpec::File(file) => &file.query,
            QuerySpec::Query(query) => query,
        }
    }
}

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct ArbitraryQueryParameters {
    /// A path to the query that we are benchmarking, or a string containing the query that we are
    /// benchmarking.
    #[clap(long)]
    query: QuerySpec,

    // The dialect that should be used to parse the query string.
    #[clap(long, default_value = "mysql")]
    #[serde(default = "default_dialect")]
    dialect: Dialect,

    /// A annotation spec for each of the parameters in query. See
    /// `DistributionAnnotations` for the format of the file.
    #[clap(long)]
    query_spec_file: Option<PathBuf>,

    /// An query spec passed in as a semicolon-separated list. See `DistributionAnnotation` for the
    /// format for each parameters annotation.
    ///
    /// Query specs give a specification for how parameters are generated for queries
    #[clap(long, conflicts_with = "query_spec_file")]
    query_spec: Option<String>,
}

fn default_dialect() -> Dialect {
    Dialect::MySQL
}

impl Default for ArbitraryQueryParameters {
    fn default() -> Self {
        Self {
            query: QuerySpec::default(),
            dialect: default_dialect(),
            query_spec_file: Option::default(),
            query_spec: Option::default(),
        }
    }
}

impl ArbitraryQueryParameters {
    pub fn new(
        query: QuerySpec,
        query_spec_file: Option<PathBuf>,
        query_spec: Option<String>,
        dialect: Dialect,
    ) -> Self {
        Self {
            query,
            query_spec_file,
            query_spec,
            dialect,
        }
    }

    pub async fn prepared_statement(
        &self,
        conn: &mut DatabaseConnection,
    ) -> Result<PreparedStatement> {
        // Mapping against two different parameters.
        #[allow(clippy::manual_map)]
        let spec = if let Some(f) = &self.query_spec_file {
            Some(DistributionAnnotations::try_from(benchmark_path(f)?.as_path()).unwrap())
        } else if let Some(s) = &self.query_spec {
            Some(DistributionAnnotations::try_from(s.clone()).unwrap())
        } else {
            None
        };
        let query = self.query.query();

        Ok(match spec {
            None => PreparedStatement::new(conn, query.to_owned()).await?,
            Some(s) => PreparedStatement::new_with_annotation(conn, query.to_owned(), s).await?,
        })
    }

    pub fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        match &self.query {
            QuerySpec::File(f) => labels.insert(
                "query_file".to_string(),
                f.path.to_string_lossy().to_string(),
            ),
            QuerySpec::Query(s) => labels.insert("query".to_string(), s.to_owned()),
        };
        if let Some(query_spec_file) = self.query_spec_file.as_ref() {
            labels.insert(
                "query_spec_file".to_string(),
                query_spec_file.to_string_lossy().to_string(),
            );
        }
        if let Some(query_spec) = self.query_spec.clone() {
            labels.insert("query_spec".to_string(), query_spec);
        }
        labels
    }

    pub async fn migrate(&self, conn: &mut DatabaseConnection) -> Result<()> {
        // Remove any query q if it is exists before migration.
        let _ = self.unmigrate(conn).await;

        let stmt = match nom_sql::parse_query(nom_sql::Dialect::MySQL, self.query.query()) {
            Ok(nom_sql::SqlQuery::Select(stmt)) => stmt,
            _ => panic!("Can only migrate SELECT statements"),
        };

        let create_cache_query = nom_sql::CreateCacheStatement {
            name: Some("q".into()),
            inner: Ok(nom_sql::CacheInner::Statement(Box::new(stmt))),
            always: false,
            concurrently: false,
            unparsed_create_cache_statement: None,
        };

        conn.query_drop(create_cache_query.display(conn.dialect()).to_string())
            .await?;

        Ok(())
    }

    pub async fn unmigrate(&self, conn: &mut DatabaseConnection) -> anyhow::Result<()> {
        let stmt = "DROP CACHE q";
        conn.query_drop(stmt).await?;
        Ok(())
    }
}

/// Utility wrapper around Vec<DistributionAnnotation>. A list of DistributionAnnotation delimited
/// by a semicolon ';' or newline '\n' can be converted from a String through
/// DistributionAnnotations::try_from. A wrapper to convert from a file including
/// DistributionAnnotations is also provided.
pub struct DistributionAnnotations(Vec<DistributionAnnotation>);

impl TryFrom<String> for DistributionAnnotations {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(DistributionAnnotations(
            s.split(&[';', '\n'][..])
                .filter_map(|m| {
                    if m.trim().is_empty() {
                        return None;
                    }
                    Some(m.parse())
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}

impl TryFrom<&Path> for DistributionAnnotations {
    type Error = anyhow::Error;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        DistributionAnnotations::try_from(std::fs::read_to_string(path)?)
    }
}

pub struct ParameterGenerationSpec {
    pub column_type: SqlType,
    pub generator: ColumnGenerator,
}

/// A query prepared against the upstream database and the corresponding specification for
/// parameters in the prepared statement.
pub struct PreparedStatement {
    pub statement: DatabaseStatement,
    pub query: String,
    pub params: Vec<ParameterGenerationSpec>,
    dialect: Dialect,
}

impl PreparedStatement {
    pub async fn new(conn: &mut DatabaseConnection, query: String) -> anyhow::Result<Self> {
        let statement = conn.prepare(&query).await?;
        Ok(Self {
            params: statement
                .query_param_types()
                .into_iter()
                .map(|sql_type| ParameterGenerationSpec {
                    column_type: sql_type.clone(),
                    generator: ColumnGenerator::Random(sql_type.into()),
                })
                .collect(),
            query,
            statement,
            dialect: conn.dialect(),
        })
    }

    pub async fn new_with_annotation(
        conn: &mut DatabaseConnection,
        query: String,
        spec: DistributionAnnotations,
    ) -> anyhow::Result<Self> {
        let statement = conn.prepare(&query).await?;
        let params = statement
            .query_param_types()
            .into_iter()
            .zip(spec.0.into_iter())
            .map(|(sql_type, annotation)| ParameterGenerationSpec {
                column_type: sql_type.clone(),
                generator: annotation.spec.generator_for_col(sql_type),
            })
            .collect();

        Ok(Self {
            query,
            params,
            statement,
            dialect: conn.dialect(),
        })
    }

    /// Returns the prepared statement and a set of parameters that can be used to
    /// execute this prepared statement.
    pub fn generate_query(&mut self) -> (&DatabaseStatement, Vec<DfValue>) {
        let params = self.generate_parameters();

        (&self.statement, params)
    }

    pub fn generate_ad_hoc_query(&mut self) -> String {
        let params = self.generate_parameters();
        let q = self
            .query
            .split('?')
            .zip(&params)
            .map(|(text, value)| {
                // TODO(ethan): Eventually, we should replace the query construction in this method
                // with a query builder library of some sort to ensure that all values are escaped
                // and quoted safely.
                let value_string = if let Ok(s) = <&str>::try_from(value) {
                    Literal::String(s.into()).display(self.dialect).to_string()
                } else {
                    value.to_string()
                };

                text.to_owned() + &value_string
            })
            .collect::<Vec<String>>()
            .join("");

        if q.is_empty() {
            self.query.clone()
        } else {
            q
        }
    }

    /// Returns the prepared statement and a set of generators that can be used to
    /// execute this prepared statement.
    pub fn query_generators(&self) -> (&DatabaseStatement, GeneratorSet) {
        (
            &self.statement,
            GeneratorSet(self.params.iter().map(|t| t.generator.clone()).collect()),
        )
    }

    /// Returns just the parameters to execute our prepared statement
    pub fn generate_parameters(&mut self) -> Vec<DfValue> {
        self.params.iter_mut().map(|t| t.generator.gen()).collect()
    }
}

pub struct GeneratorSet(Vec<ColumnGenerator>);

impl GeneratorSet {
    /// Generate a value from each generator into a vector
    pub fn generate(&mut self) -> Vec<DfValue> {
        self.0.iter_mut().map(|g| g.gen()).collect()
    }

    /// Generate a value from each generator into a vector but scaling the output
    /// of Uniform and Zipfian down by the factor scale.
    ///
    /// # Panics
    ///
    /// If scale > 1.0 or scale <= 0.0
    pub fn generate_scaled(&mut self, scale: f64) -> Vec<DfValue> {
        // Can only scale down, scaling integers up doesn't make much sense
        assert!(scale <= 1.0 && scale > 0.0);
        self.0
            .iter_mut()
            .map(|g| {
                let v = g.gen();
                if matches!(g, ColumnGenerator::Uniform(_) | ColumnGenerator::Zipfian(_)) {
                    match v {
                        DfValue::Int(i) => DfValue::Int((i as f64 * scale) as i64),
                        DfValue::UnsignedInt(i) => DfValue::UnsignedInt((i as f64 * scale) as u64),
                        _ => unreachable!("Uniform and Zipfian generate integers"),
                    }
                } else {
                    v
                }
            })
            .collect()
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Query {
    pub prep: DatabaseStatement,
    pub params: Vec<String>,
}

// Values cannot be hashed so we turn them into sql text before putting
// them in the Query struct.
impl From<(&DatabaseStatement, Vec<DfValue>)> for Query {
    fn from(v: (&DatabaseStatement, Vec<DfValue>)) -> Query {
        Query {
            prep: v.0.clone(),
            params: v.1.into_iter().map(|s| s.to_string()).collect(),
        }
    }
}

// Assumes that we don't ever perform eviction.
pub struct CachingQueryGenerator {
    prepared_statement: PreparedStatement,
    /// A set of previously generated and executed statement. We can re-execute
    /// this statement to guarantee a cache hit if we are not performing
    /// eviction.
    // TODO(justin): Replace with bloom filter for mem efficiency.
    seen: HashSet<Query>,
}

impl From<PreparedStatement> for CachingQueryGenerator {
    fn from(prepared_statement: PreparedStatement) -> CachingQueryGenerator {
        CachingQueryGenerator {
            prepared_statement,
            seen: HashSet::new(),
        }
    }
}

impl CachingQueryGenerator {
    pub fn generate_cache_miss(&mut self) -> Result<Query> {
        let mut attempts = 0;
        while attempts < MAX_RANDOM_GENERATIONS {
            let q = Query::from(self.prepared_statement.generate_query());
            if !self.seen.contains(&q) {
                self.seen.insert(q.clone());
                return Ok(q);
            }

            attempts += 1;
        }

        Err(anyhow!(
            "Unable to generate cache miss in {} attempts",
            MAX_RANDOM_GENERATIONS
        ))
    }

    pub fn generate_cache_hit(&self) -> Result<Query> {
        match self.seen.iter().next() {
            Some(q) => Ok(q.clone()),
            None => Err(anyhow!(
                "Unable to generate cache hit without first generating a cache miss"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_annotation_specs() {
        let q = "
            uniform 4 100
            uniform 5 101"
            .to_string();
        let s = DistributionAnnotations::try_from(q).unwrap();
        assert_eq!(s.0.len(), 2);
    }
}
