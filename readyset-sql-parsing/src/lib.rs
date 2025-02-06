use readyset_sql::{ast::SqlQuery, Dialect};

pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    nom_sql::parse_query(dialect, input.as_ref())
}
