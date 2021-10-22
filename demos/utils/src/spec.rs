// Generate the FastlySchema object from file.
// Certain columns are required in the fastly schema, verify that these columns
// exist in the specification.
//
// Once we have a fastly schema we want to generate rows for the article TableSpec.
// Specify how we select values for each column type.

use anyhow::{anyhow, bail, Context};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_until, take_while};
use nom::character::complete::multispace1;
use nom::combinator::{map, opt};
use nom::multi::many1;
use nom::sequence::{preceded, terminated, tuple};
use nom_sql::{sql_query, Dialect, SqlQuery};
use query_generator::{TableName, TableSpec};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::read_to_string;
use std::path::PathBuf;

/// Set of parameters used to generate a single table's data.
#[derive(Clone)]
pub struct TableGenerationSpec {
    pub table: TableSpec,
    pub num_rows: usize,
}

/// Set of parameters used to generate a database's data.
#[derive(Clone)]
pub struct DatabaseGenerationSpec {
    pub tables: HashMap<TableName, TableGenerationSpec>,
}

impl DatabaseGenerationSpec {
    pub fn new(schema: DatabaseSchema) -> Self {
        Self {
            tables: schema
                .tables
                .iter()
                .map(|t| {
                    (
                        t.0.clone(),
                        TableGenerationSpec {
                            table: t.1.clone(),
                            num_rows: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Sets the number of rows to generate for `table`. If the table does
    /// not exist in the spec, this is a no-op.
    pub fn table_rows(mut self, table_name: &str, num_rows: usize) -> Self {
        if let Some(t) = self.tables.get_mut(table_name) {
            t.num_rows = num_rows;
            self
        } else {
            panic!(
                "Attempted to set number of rows for a table, {}, that is not in
             the database spec",
                table_name
            );
        }
    }

    /// Retrieves the TableSpec for a table, `table_name`. Panics if the
    /// table does not exist in the DatabaseGenerationSpec.
    pub fn table_spec(&mut self, table_name: &str) -> &mut TableSpec {
        &mut self.tables.get_mut(table_name).unwrap().table
    }
}

/// A database schema parsed from a file of CREATE TABLE
/// operations.
#[derive(Clone)]
pub struct DatabaseSchema {
    tables: HashMap<TableName, TableSpec>,
}

fn comments_or_var_set(i: &[u8]) -> nom::IResult<&[u8], ()> {
    map(
        many1(tuple((
            opt(alt((
                map(tuple((tag("/*"), take_until("*/;"), tag("*/;"))), |_| ()),
                map(tuple((tag("--"), take_while(|c| c != b'\n'))), |_| ()),
                map(tuple((tag("SET"), take_while(|c| c != b'\n'))), |_| ()),
            ))),
            multispace1,
        ))),
        |_| (),
    )(i)
}

fn parse_ddl(input: &[u8]) -> anyhow::Result<Vec<SqlQuery>> {
    let (remain, query) = terminated(
        many1(preceded(
            // Skip comments, newlines, variables
            opt(comments_or_var_set),
            sql_query(Dialect::MySQL),
        )),
        opt(comments_or_var_set),
    )(input)
    .map_err(|err| anyhow!("Error parsing ddl into SqlQuery {:?}", err))?;

    if !remain.is_empty() {
        return Err(anyhow!(
            "Unable to parse schema, beginning with {:?}",
            std::str::from_utf8(remain.chunks(100).next().unwrap())
        ));
    }

    Ok(query)
}

impl TryFrom<PathBuf> for DatabaseSchema {
    type Error = anyhow::Error;
    fn try_from(path: PathBuf) -> anyhow::Result<Self> {
        let ddl = read_to_string(&path).with_context(|| "Failed to read fastly schema file")?;
        let ddl = parse_ddl(ddl.as_bytes()).unwrap();

        let mut schema = DatabaseSchema {
            tables: HashMap::new(),
        };

        for query in ddl {
            match query {
                SqlQuery::CreateTable(s) => {
                    let spec = TableSpec::from(s);
                    schema.tables.insert(spec.name.clone(), spec);
                }
                SqlQuery::DropTable(_) => {}
                _ => bail!("Unsupported statement in schema file"),
            }
        }

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ddl_test() {
        let q = "
    CREATE TABLE articles (
        id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        author_id int(11) NOT NULL,
        creation_time DATETIME NOT NULL,
        keywords varchar(40) NOT NULL,
        title varchar(128) NOT NULL,
        full_text varchar(255) NOT NULL,
        image_url varchar(128) NOT NULL,
        url varchar(128) NOT NULL,
        type varchar(20) DEFAULT NULL,
        priority int(8) DEFAULT 1
    );

    CREATE TABLE authors (
        id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name varchar(40) NOT NULL,
        image_url varchar(128) NOT NULL
    );

    CREATE TABLE users (
        id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY
    );

    CREATE TABLE recommendations (
        user_id int(11) NOT NULL,
        article_id int(11) NOT NULL
    );";
        parse_ddl(q.as_bytes()).unwrap();
    }
}
