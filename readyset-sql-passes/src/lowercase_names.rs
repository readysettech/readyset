use dataflow_expression::Dialect;
use readyset_data::dialect::SqlEngine;
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{Column, CreateTableStatement, Relation, SelectStatement};

#[derive(Debug, Default)]
struct LowercaseDatabasesVisitor;

impl<'ast> VisitorMut<'ast> for LowercaseDatabasesVisitor {
    type Error = std::convert::Infallible;

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        if let Some(ref mut schema) = table.schema {
            *schema = schema.to_lowercase().into();
        }
        visit_mut::walk_relation(self, table)
    }
}

pub(crate) trait LowercaseDatabaseNames {
    fn lowercase_database_names(self, lower: bool) -> Self;
}

impl LowercaseDatabaseNames for SelectStatement {
    fn lowercase_database_names(mut self, lower: bool) -> Self {
        if lower {
            let Ok(()) = LowercaseDatabasesVisitor.visit_select_statement(&mut self);
        }
        self
    }
}

#[derive(Debug, Default)]
struct LowercaseTablesVisitor;

impl<'ast> VisitorMut<'ast> for LowercaseTablesVisitor {
    type Error = std::convert::Infallible;

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        table.name = table.name.to_lowercase().into();
        visit_mut::walk_relation(self, table)
    }
}

pub(crate) trait LowercaseTableNames {
    fn lowercase_table_names(self, lower: bool) -> Self;
}

impl LowercaseTableNames for SelectStatement {
    fn lowercase_table_names(mut self, lower: bool) -> Self {
        if lower {
            let Ok(()) = LowercaseTablesVisitor.visit_select_statement(&mut self);
        }
        self
    }
}

#[derive(Debug)]
struct LowercaseColumnsVisitor;

impl LowercaseColumnsVisitor {
    fn new(dialect: Dialect) -> Option<Self> {
        match dialect.engine() {
            SqlEngine::MySQL => Some(Self),
            SqlEngine::PostgreSQL => None,
        }
    }
}

impl<'ast> VisitorMut<'ast> for LowercaseColumnsVisitor {
    type Error = std::convert::Infallible;

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        column.name = column.name.to_lowercase().into();
        Ok(())
    }
}

pub(crate) trait LowercaseColumnNames {
    fn lowercase_column_names(self, dialect: Dialect) -> Self;
}

impl LowercaseColumnNames for CreateTableStatement {
    fn lowercase_column_names(mut self, dialect: Dialect) -> Self {
        if let Some(mut visitor) = LowercaseColumnsVisitor::new(dialect) {
            let Ok(()) = visitor.visit_create_table_statement(&mut self);
        }
        self
    }
}

impl LowercaseColumnNames for SelectStatement {
    fn lowercase_column_names(mut self, dialect: Dialect) -> Self {
        if let Some(mut visitor) = LowercaseColumnsVisitor::new(dialect) {
            let Ok(()) = visitor.visit_select_statement(&mut self);
        }
        self
    }
}

#[cfg(test)]
mod test {
    use nom_sql::parse_query;
    use readyset_sql::ast::SqlQuery;
    use readyset_sql::Dialect;

    use super::*;

    fn test_lowercase(source: &str, expected: &str) {
        let mut source = parse_query(Dialect::MySQL, source).unwrap();
        let SqlQuery::Select(ref mut select) = source else {
            panic!("not a select!");
        };
        *select = select
            .clone()
            .lowercase_database_names(true)
            .lowercase_table_names(true)
            .lowercase_column_names(Dialect::MySQL.into());
        let expected = parse_query(Dialect::MySQL, expected).unwrap();
        assert_eq!(source, expected);
    }

    #[test]
    fn lowercase_database() {
        test_lowercase("select a from FOO.bar", "select a from foo.bar");
    }

    #[test]
    fn lowercase_table() {
        test_lowercase("select a from foo.BAR", "select a from foo.bar");
    }

    #[test]
    fn lowercase_columns() {
        test_lowercase("select AaAa from foo", "select aaaa from foo");
    }
}
