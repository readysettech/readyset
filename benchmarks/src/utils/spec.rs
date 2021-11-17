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
use nom_sql::{sql_query, Dialect, Expression, SqlQuery};
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
            tables: schema.tables,
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
    tables: HashMap<TableName, TableGenerationSpec>,
}

fn comments(i: &[u8]) -> nom::IResult<&[u8], ()> {
    map(
        many1(tuple((
            opt(alt((
                map(tuple((tag("/*"), take_until("*/;"), tag("*/;"))), |_| ()),
                map(tuple((tag("--"), take_while(|c| c != b'\n'))), |_| ()),
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
            opt(comments),
            sql_query(Dialect::MySQL),
        )),
        opt(comments),
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

impl DatabaseSchema {
    fn new(ddl: &str, mut user_vars: HashMap<String, String>) -> anyhow::Result<Self> {
        let ddl = parse_ddl(ddl.as_bytes()).unwrap();

        let mut schema = DatabaseSchema {
            tables: HashMap::new(),
        };

        for query in ddl {
            match query {
                SqlQuery::CreateTable(mut s) => {
                    // To get the number of rows for a table we first look for a comment of
                    // the form ROWS=, missing that we are looking for an AUTOINCREMENT value
                    // otherwise we set to 0
                    let num_rows = s
                        .get_comment()
                        .map(|comment| {
                            if let Some(val) = comment
                                .split("ROWS=")
                                .nth(1)
                                .map(str::trim)
                                .and_then(|s| s.split(char::is_whitespace).next())
                            {
                                // We have a length indicated using the ROWS= directive, it can be a var or a number
                                if let Some(var) = val.strip_prefix('@') {
                                    user_vars[var]
                                        .parse::<usize>()
                                        .expect("user variable should be a number")
                                } else {
                                    val.parse::<usize>().expect("rows should be a number")
                                }
                            } else {
                                0
                            }
                        })
                        .unwrap_or_else(|| {
                            if let Some(autoinc) = s.get_autoincrement() {
                                autoinc as usize
                            } else {
                                0
                            }
                        });

                    for col in &mut s.fields {
                        if let Some(ref mut comment) = col.comment {
                            // We stupidly go over each comment and replace each var
                            // definition with its value instead
                            for (var, value) in &user_vars {
                                *comment = comment.replace(&format!("@{}", var), value);
                            }
                        }
                    }

                    let spec = TableSpec::from(s);

                    schema.tables.insert(
                        spec.name.clone(),
                        TableGenerationSpec {
                            table: spec,
                            num_rows,
                        },
                    );
                }
                SqlQuery::Set(set) => {
                    if let Some(v) = set.variables() {
                        v.iter().for_each(|(var, value)| {
                            if let (Some(name), Expression::Literal(lit)) =
                                (var.as_user_var(), value)
                            {
                                // Only keep the original value for each variable
                                user_vars
                                    .entry(name.to_owned())
                                    .or_insert_with(|| lit.to_string());
                            }
                        })
                    };
                }
                SqlQuery::DropTable(_) => {}
                _ => bail!("Unsupported statement in schema file"),
            }
        }

        Ok(schema)
    }
}

impl TryFrom<PathBuf> for DatabaseSchema {
    type Error = anyhow::Error;
    fn try_from(path: PathBuf) -> anyhow::Result<Self> {
        let ddl = read_to_string(&path).with_context(|| "Failed to read fastly schema file")?;
        DatabaseSchema::new(&ddl, HashMap::new())
    }
}

impl TryFrom<(PathBuf, HashMap<String, String>)> for DatabaseSchema {
    type Error = anyhow::Error;
    fn try_from(p: (PathBuf, HashMap<String, String>)) -> anyhow::Result<Self> {
        let ddl = read_to_string(&p.0).with_context(|| "Failed to read fastly schema file")?;
        DatabaseSchema::new(&ddl, p.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use query_generator::ColumnGenerator;

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

    #[test]
    fn parse_ddl_annotations_test() {
        let ddl = r#"
            SET @articles_rows = 200;
            CREATE TABLE articles (
                id int(11) NOT NULL PRIMARY KEY,
                author_id int(11) NOT NULL COMMENT 'UNIFORM 0 @authors_rows',
                creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                keywords varchar(40) NOT NULL COMMENT 'REGEX "[A-Za-z]{10,40}"',
                title varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z]{15,60}" UNIQUE',
                full_text varchar(255) NOT NULL,
                short_text varchar(512) NOT NULL,
                image_url varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{20,80}"',
                url varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{20,80}"',
                type varchar(20) DEFAULT NULL,
                priority int(8) DEFAULT 1
            ) COMMENT = 'ROWS=@articles_rows';"#;

        let schema = DatabaseSchema::new(
            ddl,
            [("authors_rows".to_owned(), "100".to_owned())]
                .iter()
                .cloned()
                .collect(),
        )
        .unwrap();

        let schema = &schema.tables["articles"];

        assert_eq!(schema.num_rows, 200);
        assert!(matches!(
            schema.table.columns[&"author_id".into()].gen_spec,
            ColumnGenerator::Uniform(_)
        ));
        assert!(matches!(
            schema.table.columns[&"keywords".into()].gen_spec,
            ColumnGenerator::RandomString(_)
        ));
        assert!(matches!(
            schema.table.columns[&"title".into()].gen_spec,
            ColumnGenerator::NonRepeating(_)
        ));
    }
}
