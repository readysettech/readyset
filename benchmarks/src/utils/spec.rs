// Generate the NewsAppSchema object from file.
// Certain columns are required in the news app schema, verify that these columns
// exist in the specification.
//
// Once we have a news app schema we want to generate rows for the article TableSpec.
// Specify how we select values for each column type.

use std::collections::HashMap;

use anyhow::{anyhow, bail};
use database_utils::{DatabaseConnection, DatabaseType};
use itertools::{Either, Itertools};
use nom::multi::many1;
use nom::sequence::{preceded, terminated};
use nom_locate::LocatedSpan;
use nom_sql::whitespace::whitespace0;
use nom_sql::{
    sql_query, CommentStatement, CreateTableOption, Dialect, Expr, SqlQuery, VariableScope,
};
use query_generator::{TableName, TableSpec};

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
    #[must_use]
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

pub enum SchemaKind {
    MySQL { user_vars: HashMap<String, String> },
    PostgreSQL,
}

impl From<Dialect> for SchemaKind {
    fn from(dialect: Dialect) -> Self {
        match dialect {
            Dialect::MySQL => Self::MySQL {
                user_vars: HashMap::new(),
            },
            Dialect::PostgreSQL => Self::PostgreSQL,
        }
    }
}

impl From<DatabaseType> for SchemaKind {
    fn from(dialect: DatabaseType) -> Self {
        match dialect {
            DatabaseType::MySQL | DatabaseType::Vitess => Self::MySQL {
                user_vars: HashMap::new(),
            },
            DatabaseType::PostgreSQL => Self::PostgreSQL,
        }
    }
}

impl From<&DatabaseConnection> for SchemaKind {
    fn from(conn: &DatabaseConnection) -> Self {
        match conn {
            DatabaseConnection::MySQL(_) | DatabaseConnection::Vitess(_) => Self::MySQL {
                user_vars: HashMap::new(),
            },
            DatabaseConnection::PostgreSQL(_, _) => Self::PostgreSQL,
            DatabaseConnection::PostgreSQLPool(_) => Self::PostgreSQL,
        }
    }
}

fn parse_ddl(input: LocatedSpan<&[u8]>, dialect: Dialect) -> anyhow::Result<Vec<SqlQuery>> {
    let (remain, query) = terminated(
        many1(preceded(
            // Skip comments, newlines, variables
            whitespace0,
            sql_query(dialect),
        )),
        whitespace0,
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

fn parse_row_count_assignment(comment: &str) -> Option<&str> {
    comment
        .split("ROWS=")
        .nth(1)
        .map(str::trim)
        .and_then(|s| s.split(char::is_whitespace).next())
}

impl DatabaseSchema {
    pub fn new(ddl: &str, schema_kind: SchemaKind) -> anyhow::Result<Self> {
        match schema_kind {
            SchemaKind::MySQL { user_vars } => Self::new_mysql(ddl, user_vars),
            SchemaKind::PostgreSQL => Self::new_postgres(ddl),
        }
    }

    fn new_mysql(ddl: &str, mut user_vars: HashMap<String, String>) -> anyhow::Result<Self> {
        let ddl = parse_ddl(LocatedSpan::new(ddl.as_bytes()), Dialect::MySQL).unwrap();

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
                            if let Some(val) = parse_row_count_assignment(comment) {
                                // We have a length indicated using the ROWS= directive, it can be a
                                // var or a number
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

                    for col in &mut s.body.as_mut().unwrap().fields {
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
                            if var.scope != VariableScope::User {
                                return;
                            }
                            if let Expr::Literal(lit) = value {
                                // Only keep the original value for each variable
                                user_vars
                                    .entry(var.name.clone().into())
                                    .or_insert_with(|| lit.display(Dialect::MySQL).to_string());
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

    fn new_postgres(ddl: &str) -> anyhow::Result<Self> {
        let ddl = parse_ddl(LocatedSpan::new(ddl.as_bytes()), Dialect::PostgreSQL).unwrap();
        let (comments, ddl): (Vec<CommentStatement>, Vec<SqlQuery>) =
            ddl.into_iter().partition_map(|stmt| match stmt {
                SqlQuery::Comment(comment) => Either::Left(comment),
                sql_query => Either::Right(sql_query),
            });

        let mut schema = DatabaseSchema {
            tables: HashMap::new(),
        };

        for mut query in ddl.into_iter() {
            match query {
                SqlQuery::CreateTable(ref mut s) => {
                    let num_rows = comments
                        .iter()
                        // Find the comment associated with the table being created (if one exists)
                        .find_map(|stmt| {
                            if let CommentStatement::Table {
                                table_name,
                                comment,
                            } = stmt
                            {
                                if table_name == &s.table.name {
                                    // Parse the key-value pair that specifies the number rows
                                    Some(
                                        parse_row_count_assignment(comment)
                                            .map(|val| {
                                                val.parse::<usize>()
                                                    .expect("rows should be a number")
                                            })
                                            .unwrap_or(0),
                                    )
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);

                    // We need to insert all of the PostgreSQL-style comments (which exist
                    // separately from the table itself) into the "CREATE TABLE" statement before
                    // we pass it to TableSpec::from
                    for stmt in comments.iter() {
                        match stmt {
                            CommentStatement::Table {
                                table_name,
                                comment,
                            } if table_name == &s.table.name => {
                                if let Ok(ref mut options) = s.options {
                                    options.push(CreateTableOption::Comment(comment.clone()));
                                } else {
                                    anyhow::bail!("could not add comment to table options")
                                }
                            }
                            CommentStatement::Column {
                                table_name,
                                column_name,
                                comment,
                            } if table_name == &s.table.name => {
                                if let Ok(ref mut body) = s.body {
                                    if let Some(col) = body
                                        .fields
                                        .iter_mut()
                                        .find(|col| col.column.name == column_name)
                                    {
                                        col.comment = Some(comment.clone());
                                    } else {
                                        anyhow::bail!("comment references non-existent column {column_name} on table {table_name}")
                                    }
                                } else {
                                    anyhow::bail!("could not add comment to table options")
                                }
                            }
                            _ => {}
                        }
                    }

                    let spec = TableSpec::from(s.clone());

                    schema.tables.insert(
                        spec.name.clone(),
                        TableGenerationSpec {
                            table: spec,
                            num_rows,
                        },
                    );
                }
                SqlQuery::DropTable(_) => {}
                _ => bail!("Unsupported statement in schema file"),
            }
        }

        Ok(schema)
    }

    pub fn tables(&self) -> &HashMap<TableName, TableGenerationSpec> {
        &self.tables
    }
}

#[cfg(test)]
mod tests {
    use data_generator::ColumnGenerator;

    use super::*;

    #[test]
    fn parse_ddl_test_mysql() {
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
        parse_ddl(LocatedSpan::new(q.as_bytes()), Dialect::MySQL).unwrap();
    }

    #[test]
    fn parse_ddl_annotations_test_mysql() {
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

        let schema = DatabaseSchema::new_mysql(
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
            schema.table.columns[&"author_id".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::Uniform(_)
        ));
        assert!(matches!(
            schema.table.columns[&"keywords".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::RandomString(_)
        ));
        assert!(matches!(
            schema.table.columns[&"title".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::NonRepeating(_)
        ));
    }

    #[test]
    fn parse_ddl_test_postgres() {
        let q = "
    CREATE TABLE articles (
        id serial PRIMARY KEY,
        author_id int NOT NULL,
        creation_time DATETIME NOT NULL,
        keywords text NOT NULL,
        title text NOT NULL,
        full_text text NOT NULL,
        image_url text NOT NULL,
        url text NOT NULL,
        type text DEFAULT NULL,
        priority int DEFAULT 1
    );

    CREATE TABLE authors (
        id serial PRIMARY KEY,
        name varchar NOT NULL,
        image_url varchar NOT NULL
    );

    CREATE TABLE users (
        id SERIAL PRIMARY KEY
    );

    CREATE TABLE recommendations (
        user_id int NOT NULL,
        article_id int NOT NULL
    );";
        parse_ddl(LocatedSpan::new(q.as_bytes()), Dialect::PostgreSQL).unwrap();
    }

    #[test]
    fn parse_ddl_annotations_test_postgres() {
        let ddl = r#"
            CREATE TABLE articles (
                id int NOT NULL PRIMARY KEY,
                author_id int NOT NULL,
                creation_time TIMESTAMP NOT NULL DEFAULT NOW(),
                keywords text NOT NULL,
                title text NOT NULL,
                full_text text NOT NULL,
                short_text text NOT NULL,
                image_url text NOT NULL,
                url text NOT NULL,
                type text DEFAULT NULL,
                priority int DEFAULT 1
            );

            COMMENT ON TABLE articles IS 'ROWS=200';
            COMMENT ON COLUMN articles.author_id IS 'UNIFORM 0 200';
            COMMENT ON COLUMN articles.keywords IS 'REGEX "[A-Za-z]{10,40}"';
            COMMENT ON COLUMN articles.title IS 'REGEX "[A-Za-z]{15,60}" UNIQUE';
            COMMENT ON COLUMN articles.image_url IS 'REGEX "[A-Za-z0-9/]{20,80}"';
            COMMENT ON COLUMN articles.url IS 'REGEX "[A-Za-z0-9/]{20,80}"';
        "#;

        let schema = DatabaseSchema::new_postgres(ddl).unwrap();

        let schema = &schema.tables["articles"];

        assert_eq!(schema.num_rows, 200);
        assert!(matches!(
            schema.table.columns[&"author_id".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::Uniform(_)
        ));
        assert!(matches!(
            schema.table.columns[&"keywords".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::RandomString(_)
        ));
        assert!(matches!(
            schema.table.columns[&"title".into()]
                .gen_spec
                .lock()
                .generator,
            ColumnGenerator::NonRepeating(_)
        ));
    }
}
