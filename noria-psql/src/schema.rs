use noria_client::backend as cl;
use psql_srv as ps;

/// A simple wrapper around `noria_client`'s `SelectSchema` facilitating conversion to
/// `psql_srv::Schema`.
pub struct SelectSchema(pub cl::SelectSchema);

impl From<SelectSchema> for ps::Schema {
    fn from(s: SelectSchema) -> Self {
        s.0.schema
            .into_iter()
            .map(|c| ps::Column {
                name: c.spec.column.name,
                col_type: c.spec.sql_type,
            })
            .collect()
    }
}

pub struct NoriaSchema(pub Vec<noria::ColumnSchema>);

impl From<NoriaSchema> for ps::Schema {
    fn from(s: NoriaSchema) -> Self {
        s.0.into_iter()
            .map(|c| ps::Column {
                name: c.spec.column.name,
                col_type: c.spec.sql_type,
            })
            .collect()
    }
}
