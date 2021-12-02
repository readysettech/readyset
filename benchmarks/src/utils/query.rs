//! Utilities for working with parameterized queries. Benchmarks that
//! execute arbitrary parameterized queries may benefit from these
//! definitions and these utilities.
//!
//! The core of these utilities is the PreparedStatement struct. This
//! packages a parameterized prepared statement we intend to execute
//! for a benchmark, with "how" to generate the parameters for this
//! statement. Each parameter in a prepared statement is generated
//! based on a DistributionAnnotation.

use clap::Parser;
use mysql::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::Statement;
use mysql_async::Value;
use nom_sql::SqlType;
use query_generator::DistributionAnnotation;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs;
use std::path::{Path, PathBuf};

use crate::utils::random::random_value_for_sql_type;

#[derive(Parser, Clone)]
pub struct ArbitraryQueryParameters {
    /// A path to the query that we are benchmarking.
    #[clap(long)]
    query: PathBuf,

    /// A annotation spec for each of the parameters in query. See
    /// `DistributionAnnotations` for the format of the file.
    #[clap(long)]
    query_spec_file: Option<PathBuf>,

    /// An query spec passed in as a comma separated list. See
    /// `DistributionAnnotation` for the format for each parameters annotation.
    #[clap(long, conflicts_with = "query-spec-file")]
    query_spec: Option<String>,
}

impl ArbitraryQueryParameters {
    pub async fn prepared_statement(
        &self,
        conn: &mut mysql_async::Conn,
    ) -> anyhow::Result<PreparedStatement> {
        // Mapping against two different parameters.
        #[allow(clippy::manual_map)]
        let spec = if let Some(f) = &self.query_spec_file {
            Some(DistributionAnnotations::try_from(f.as_path()).unwrap())
        } else if let Some(s) = &self.query_spec {
            Some(DistributionAnnotations::try_from(s.clone()).unwrap())
        } else {
            None
        };
        let query = fs::read_to_string(&self.query).unwrap();
        let stmt = conn.prep(query.clone()).await?;

        Ok(match spec {
            None => PreparedStatement::new(query, stmt),
            Some(s) => PreparedStatement::new_with_annotation(query, stmt, s),
        })
    }

    pub fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(
            "query_file".to_string(),
            self.query.to_string_lossy().to_string(),
        );
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
}

/// Utility wrapper around Vec<DistributionAnnotation>. A list of DistributionAnnotation
/// delimited by a comma ',' or newline '\n' can be converted from a String
/// through DistributionAnnotations::try_from. A wrapper to convert from a file
/// including DistributionAnnotations is also provided.
pub struct DistributionAnnotations(Vec<DistributionAnnotation>);

impl TryFrom<String> for DistributionAnnotations {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(DistributionAnnotations(
            s.split(&[',', '\n'][..])
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

/// Converts from a MySQL column type to a nom_sql::SqlType. Most ReadySet
/// internal utilities use SqlTypes so this utility enables using them for
/// MySQL types.
pub(crate) fn column_to_sqltype(c: &ColumnType) -> SqlType {
    use mysql_async::consts::ColumnType::*;
    match c {
        // TODO(justin): Abstract random value generation to utilities crate
        // TODO(justin): These columns may have fixed sizes.
        MYSQL_TYPE_VAR_STRING => SqlType::Varchar(None),
        MYSQL_TYPE_BLOB => SqlType::Text,
        MYSQL_TYPE_TINY => SqlType::Tinyint(None),
        MYSQL_TYPE_SHORT => SqlType::Smallint(None),
        MYSQL_TYPE_BIT => SqlType::Bool,
        MYSQL_TYPE_FLOAT => SqlType::Float,
        MYSQL_TYPE_STRING => SqlType::Char(None),
        MYSQL_TYPE_LONGLONG | MYSQL_TYPE_LONG => SqlType::UnsignedInt(None),
        MYSQL_TYPE_DATETIME => SqlType::DateTime(None),
        MYSQL_TYPE_DATE => SqlType::Date,
        MYSQL_TYPE_TIMESTAMP => SqlType::Timestamp,
        MYSQL_TYPE_TIME => SqlType::Time,
        MYSQL_TYPE_JSON => SqlType::Json,
        t => unimplemented!("Unsupported type: {:?}", t),
    }
}

pub struct ParameterGenerationSpec {
    pub column_type: SqlType,
    pub annotation: Option<DistributionAnnotation>,
}

/// A query prepared against MySQL and the corresponding specification for
/// parameters in the prepared statement.
pub struct PreparedStatement {
    pub query: String,
    pub params: Vec<ParameterGenerationSpec>,
}

impl PreparedStatement {
    pub fn new(query: String, stmt: Statement) -> Self {
        Self {
            query,
            params: stmt
                .params()
                .iter()
                .map(|c| ParameterGenerationSpec {
                    column_type: column_to_sqltype(&c.column_type()),
                    annotation: None,
                })
                .collect(),
        }
    }

    pub fn new_with_annotation(
        query: String,
        stmt: Statement,
        spec: DistributionAnnotations,
    ) -> Self {
        let params = stmt
            .params()
            .iter()
            .zip(spec.0.into_iter())
            .map(|(column, annotation)| ParameterGenerationSpec {
                column_type: column_to_sqltype(&column.column_type()),
                annotation: Some(annotation),
            })
            .collect();

        Self { query, params }
    }

    /// Returns the query text and a set of parameters that can be used to
    /// execute this prepared statement.
    pub fn generate_query(&self) -> (String, Vec<Value>) {
        (self.query.clone(), self.generate_parameters())
    }

    /// Returns just the parameters to execute our prepared statement
    pub fn generate_parameters(&self) -> Vec<Value> {
        self.params
            .iter()
            .map(|t| match &t.annotation {
                None => random_value_for_sql_type(&t.column_type),
                Some(annotation) => annotation
                    .spec
                    .generator_for_col(t.column_type.clone())
                    .gen()
                    .try_into()
                    .unwrap(),
            })
            .collect()
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
