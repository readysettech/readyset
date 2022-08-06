use nom_sql::analysis::visit::{self, Visitor};
use nom_sql::{Column, CreateTableStatement, Table};

#[derive(Debug, Default)]
struct CreateTableColumnsVisitor {
    table: Option<Table>,
}

impl<'ast> Visitor<'ast> for CreateTableColumnsVisitor {
    type Error = !;

    fn visit_create_table_statement(
        &mut self,
        create_table_statement: &'ast mut CreateTableStatement,
    ) -> Result<(), Self::Error> {
        self.table = Some(create_table_statement.table.clone());
        visit::walk_create_table_statement(self, create_table_statement)
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        column.table.get_or_insert_with(|| {
            self.table
                .as_ref()
                .expect("Must have visited the CREATE TABLE statement by now")
                .clone()
        });
        visit::walk_column(self, column)
    }
}

pub trait CreateTableColumns {
    fn normalize_create_table_columns(self) -> Self;
}

impl CreateTableColumns for CreateTableStatement {
    fn normalize_create_table_columns(mut self) -> Self {
        let Ok(()) = CreateTableColumnsVisitor::default().visit_create_table_statement(&mut self);
        self
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_create_table, Dialect};

    use super::*;

    #[test]
    fn simple_create_table() {
        let orig = parse_create_table(
            Dialect::MySQL,
            "CREATE TABLE x.t (a int, b int, unique (a))",
        )
        .unwrap();
        let expected = parse_create_table(
            Dialect::MySQL,
            "CREATE TABLE x.t (x.t.a int, x.t.b int, unique (x.t.a))",
        )
        .unwrap();
        assert_eq!(orig.normalize_create_table_columns(), expected);
    }
}
