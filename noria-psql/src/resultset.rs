use crate::row::Row;
use crate::schema::{MysqlType, SelectSchema};
use noria::results::Results;
use psql_srv as ps;
use std::convert::TryInto;
use std::iter;
use std::rc::Rc;

/// A structure that contains a `Vec<Results>`, as provided by `QueryResult::NoriaSelect`, and
/// facilitates iteration over these results as `Row` values.
pub struct Resultset {
    /// The data types of the fields in each row.
    col_types: Rc<Vec<ps::ColType>>,

    /// The query result data, comprising nested `Vec`s of rows that may come from separate Noria
    /// interface lookups performed by the backend.
    results: Vec<Results>,

    /// The fields to project for each row. A `Results` returned by a Noria interface lookup may
    /// contain extraneous fields that should not be projected into the query result output. In
    /// particular, bogokeys and other lookup keys that are not requested for projection by the SQL
    /// query may be present in `results` but should be excluded from query output. This
    /// `project_fields` attribute contains the indices of the fields that _should_ be projected
    /// into the output.
    project_fields: Rc<Vec<usize>>,
}

impl Resultset {
    pub fn try_new(results: Vec<Results>, schema: &SelectSchema) -> Result<Self, ps::Error> {
        let col_types = Rc::new(
            schema
                .0
                .schema
                .iter()
                .map(|c| MysqlType(c.coltype).try_into())
                .collect::<Result<Vec<ps::ColType>, ps::Error>>()?,
        );
        let project_fields = Rc::new(
            schema
                .0
                .schema
                .iter()
                .map(|col| -> Result<usize, ps::Error> {
                    schema
                        .0
                        .columns
                        .iter()
                        .position(|name| name == &col.column)
                        .ok_or_else(|| ps::Error::InternalError("inconsistent schema".to_string()))
                })
                .collect::<Result<Vec<usize>, ps::Error>>()?,
        );
        Ok(Resultset {
            col_types,
            results,
            project_fields,
        })
    }
}

impl IntoIterator for Resultset {
    type Item = Row;
    type IntoIter = std::iter::Map<
        std::iter::Zip<
            std::iter::Flatten<std::vec::IntoIter<Results>>,
            std::iter::Repeat<(Rc<Vec<ps::ColType>>, Rc<Vec<usize>>)>,
        >,
        fn((noria::results::Row, (Rc<Vec<ps::ColType>>, Rc<Vec<usize>>))) -> Row,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.results
            .into_iter()
            .flatten()
            .zip(iter::repeat((self.col_types, self.project_fields)))
            .map(|(values, (col_types, project_fields))| Row {
                col_types,
                values: values.into(),
                project_fields,
            })
    }
}
