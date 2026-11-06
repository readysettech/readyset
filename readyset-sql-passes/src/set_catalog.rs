use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{CreateTableStatement, Relation, SqlIdentifier, SqlType};

struct SetCatalogVisitor<'a> {
    catalog: &'a SqlIdentifier,
}

impl<'ast> VisitorMut<'ast> for SetCatalogVisitor<'_> {
    type Error = std::convert::Infallible;

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        table.catalog = Some(self.catalog.clone());
        visit_mut::walk_relation(self, table)
    }

    fn visit_target_table_fk(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        table.catalog = Some(self.catalog.clone());
        visit_mut::walk_relation(self, table)
    }

    fn visit_sql_type(&mut self, sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        if let SqlType::Other(ty) = sql_type {
            ty.catalog = Some(self.catalog.clone());
        }
        visit_mut::walk_sql_type(self, sql_type)
    }
}

/// Put every relation and custom type in a statement in `catalog`.
pub trait SetCatalog {
    fn set_catalog(&mut self, catalog: &SqlIdentifier) -> &mut Self;
}

impl SetCatalog for CreateTableStatement {
    fn set_catalog(&mut self, catalog: &SqlIdentifier) -> &mut Self {
        let Ok(()) = SetCatalogVisitor { catalog }.visit_create_table_statement(self);
        self
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql::ast::TableKey;
    use readyset_sql_parsing::parse_create_table;

    use super::*;

    fn in_db2(schema: &str, name: &str) -> Relation {
        let mut relation = Relation::new(Some(schema.into()), name.into());
        relation.catalog = Some("db2".into());
        relation
    }

    #[test]
    fn create_table() {
        let mut stmt = parse_create_table(
            Dialect::PostgreSQL,
            "CREATE TABLE public.t (a public.e, b public.e[], c int, \
             FOREIGN KEY (c) REFERENCES public.u (id))",
        )
        .unwrap();
        stmt.set_catalog(&"db2".into());

        assert_eq!(stmt.table, in_db2("public", "t"));
        let body = stmt.body.unwrap();
        assert_eq!(
            body.fields[0].sql_type,
            SqlType::Other(in_db2("public", "e"))
        );
        assert_eq!(
            body.fields[1].sql_type,
            SqlType::Array(Box::new(SqlType::Other(in_db2("public", "e"))))
        );
        let Some(TableKey::ForeignKey { target_table, .. }) = body.keys.unwrap().pop() else {
            panic!("expected a foreign key");
        };
        assert_eq!(target_table, in_db2("public", "u"));
    }
}
