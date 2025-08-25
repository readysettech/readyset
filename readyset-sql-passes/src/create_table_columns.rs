use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{Column, CreateTableStatement, Relation};

#[derive(Debug, Default)]
struct CreateTableColumnsVisitor {
    table: Option<Relation>,
}

impl<'ast> VisitorMut<'ast> for CreateTableColumnsVisitor {
    type Error = std::convert::Infallible;

    fn visit_create_table_statement(
        &mut self,
        create_table_statement: &'ast mut CreateTableStatement,
    ) -> Result<(), Self::Error> {
        self.table = Some(create_table_statement.table.clone());
        visit_mut::walk_create_table_statement(self, create_table_statement)
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        column.table.get_or_insert_with(|| {
            self.table
                .as_ref()
                .expect("Must have visited the CREATE TABLE statement by now")
                .clone()
        });
        visit_mut::walk_column(self, column)
    }
}

pub trait CreateTableColumns {
    fn normalize_create_table_columns(&mut self) -> &mut Self;
}

impl CreateTableColumns for CreateTableStatement {
    fn normalize_create_table_columns(&mut self) -> &mut Self {
        let Ok(()) = CreateTableColumnsVisitor::default().visit_create_table_statement(self);
        self
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::parse_create_table;

    use super::*;

    #[test]
    fn simple_create_table() {
        let mut orig = parse_create_table(
            Dialect::MySQL,
            "CREATE TABLE x.t (a int, b int, unique (a))",
        )
        .unwrap();
        let expected = "CREATE TABLE `x`.`t` (`a` INT, `b` INT, UNIQUE KEY (`x`.`t`.`a`))";
        orig.normalize_create_table_columns();
        assert_eq!(format!("{}", orig.display(Dialect::MySQL)), expected);
    }
}
