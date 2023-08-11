//! Anonymize
//!
//! Provides helpers that provide anonymizaion of SQL statements

use std::collections::HashMap;

use nom_sql::analysis::visit_mut::VisitorMut;
use nom_sql::{
    CreateTableOption, CreateTableStatement, CreateViewStatement, Literal, SelectStatement,
    SqlIdentifier,
};

pub trait Anonymize {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer);
}

impl Anonymize for CreateTableStatement {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        let Ok(()) = AnonymizeVisitor { anonymizer }.visit_create_table_statement(self);
    }
}

impl Anonymize for CreateViewStatement {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        let Ok(()) = AnonymizeVisitor { anonymizer }.visit_create_view_statement(self);
    }
}

impl Anonymize for SqlIdentifier {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        anonymizer.replace(self);
    }
}

impl Anonymize for String {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        let mut sql_id: SqlIdentifier = SqlIdentifier::from(self.as_str());
        anonymizer.replace(&mut sql_id);
        *self = sql_id.to_string();
    }
}
impl Anonymize for SelectStatement {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        let Ok(()) = AnonymizeVisitor { anonymizer }.visit_select_statement(self);
    }
}

impl Anonymize for [SqlIdentifier] {
    fn anonymize(&mut self, anonymizer: &mut Anonymizer) {
        for sql_id in self.iter_mut() {
            anonymizer.replace(sql_id);
        }
    }
}

/// This pass replaces every instance of `Literal` which could contain sensitive data (so excluding
/// placeholders, booleans, and `NULL`) in the AST with `Literal::String("<anonymized>")`
struct AnonymizeLiteralsVisitor;
impl<'ast> VisitorMut<'ast> for AnonymizeLiteralsVisitor {
    type Error = !;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(
            literal,
            Literal::Null | Literal::Placeholder(_) | Literal::Boolean(_)
        ) {
            *literal = Literal::String("<anonymized>".to_owned());
        }
        Ok(())
    }
}

/// Replaces every instance of `Literal` in the AST with `Literal::String("<anonymized>")`
pub fn anonymize_literals(query: &mut SelectStatement) {
    #[allow(clippy::unwrap_used)] // error is !, which can never be returned
    AnonymizeLiteralsVisitor
        .visit_select_statement(query)
        .unwrap();
}

pub struct Anonymizer {
    /// A map of symbols to anonymized symbols
    anonymizations: HashMap<SqlIdentifier, SqlIdentifier>,
    next_id: u32,
}

impl Anonymizer {
    pub fn new() -> Self {
        Self {
            anonymizations: HashMap::new(),
            next_id: 0,
        }
    }

    fn next_token(&mut self) -> SqlIdentifier {
        let ret = format!("anon_id_{}", self.next_id);
        self.next_id += 1;
        ret.into()
    }

    /// Anonymizes s, replacing it with an anonymous token
    pub fn replace(&mut self, s: &mut SqlIdentifier) {
        match self.anonymizations.get(s) {
            Some(anon_s) => *s = anon_s.clone(),
            None => {
                let anon_s = self.next_token();
                self.anonymizations.insert(s.clone(), anon_s.clone());
                *s = anon_s;
            }
        }
    }

    pub fn anonymize_create_table(&mut self, stmt: &mut CreateTableStatement) {
        let Ok(()) = AnonymizeVisitor { anonymizer: self }.visit_create_table_statement(stmt);
    }

    pub fn anonymize_create_view(&mut self, stmt: &mut CreateViewStatement) {
        let Ok(()) = AnonymizeVisitor { anonymizer: self }.visit_create_view_statement(stmt);
    }

    // This converts any SqlIdentifier::TinyText to SqlIdentifier::Text for now, as anonymized
    // schemas are not expected to care as much about performance.
    pub fn anonymize_sql_identifier(&mut self, sql_ident: &mut SqlIdentifier) {
        self.replace(sql_ident);
    }

    pub fn anonymize_string(&mut self, string: &mut String) {
        let mut sql_id: SqlIdentifier = SqlIdentifier::from(string.as_str());
        self.replace(&mut sql_id);
        *string = sql_id.to_string();
    }
}

impl Default for Anonymizer {
    fn default() -> Self {
        Self::new()
    }
}

struct AnonymizeVisitor<'a> {
    anonymizer: &'a mut Anonymizer,
}

impl AnonymizeVisitor<'_> {
    pub fn anonymize_string(&mut self, string: &mut String) {
        string.anonymize(self.anonymizer);
    }

    pub fn anonymize_sql_identifier(&mut self, sql_ident: &mut SqlIdentifier) {
        sql_ident.anonymize(self.anonymizer);
    }
}

impl<'ast> VisitorMut<'ast> for AnonymizeVisitor<'_> {
    type Error = !;

    fn visit_sql_identifier(
        &mut self,
        sql_ident: &'ast mut SqlIdentifier,
    ) -> Result<(), Self::Error> {
        self.anonymize_sql_identifier(sql_ident);
        Ok(())
    }

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(literal, Literal::Placeholder(_)) {
            *literal = Literal::String("<anonymized>".to_owned());
        }
        Ok(())
    }

    fn visit_create_table_option(
        &mut self,
        create_table_option: &'ast mut CreateTableOption,
    ) -> Result<(), Self::Error> {
        match create_table_option {
            CreateTableOption::Comment(ref mut comment) => *comment = "<anonymized>".to_string(),
            // No anonymization needed for any of these
            CreateTableOption::Collate(_)
            | CreateTableOption::AutoIncrement(_)
            | CreateTableOption::Engine(_)
            | CreateTableOption::Charset(_)
            | CreateTableOption::Other => {}
        }
        Ok(())
    }

    fn visit_show_statement(
        &mut self,
        show_statement: &'ast mut nom_sql::ShowStatement,
    ) -> Result<(), Self::Error> {
        match show_statement {
            nom_sql::ShowStatement::Tables(ref mut tables) => {
                if let Some(ref mut from_db) = tables.from_db {
                    self.anonymize_string(from_db)
                }
            }
            // No anonymizaion needed
            nom_sql::ShowStatement::Events
            | nom_sql::ShowStatement::CachedQueries(..)
            | nom_sql::ShowStatement::ProxiedQueries(..)
            | nom_sql::ShowStatement::ReadySetStatus
            | nom_sql::ShowStatement::ReadySetMigrationStatus(..)
            | nom_sql::ShowStatement::ReadySetVersion
            | nom_sql::ShowStatement::ReadySetTables => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{CreateTableStatement, Dialect};

    use super::*;

    fn parse_select_statement(q: &str) -> SelectStatement {
        nom_sql::parse_select_statement(Dialect::MySQL, q).unwrap()
    }

    fn parse_create_table_statement(q: &str) -> CreateTableStatement {
        nom_sql::parse_create_table(Dialect::MySQL, q).unwrap()
    }

    fn parse_create_view_statement(q: &str) -> CreateViewStatement {
        nom_sql::parse_create_view(Dialect::MySQL, q).unwrap()
    }

    #[test]
    fn simple_query() {
        let mut query = parse_select_statement(
            "SELECT id + 3 FROM users WHERE credit_card_number = \"look at this PII\"",
        );
        let expected = parse_select_statement(
            "SELECT id + \"<anonymized>\" FROM users WHERE credit_card_number = \"<anonymized>\"",
        );
        anonymize_literals(&mut query);
        assert_eq!(query, expected);
    }

    #[test]
    fn parameterized_no_literals() {
        let mut query = parse_select_statement(
            "SELECT id FROM users WHERE credit_card_number = $1 AND id = $2",
        );
        let expected = query.clone();
        anonymize_literals(&mut query);
        assert_eq!(
            query, expected,
            "Anonymization shouldn't have caused any changes"
        );
    }

    #[test]
    fn parameterized_single_literal() {
        let mut query =
            parse_select_statement("SELECT id + 3 FROM users WHERE credit_card_number = $1");
        let expected = parse_select_statement(
            "SELECT id +  \"<anonymized>\" FROM users WHERE credit_card_number = $1",
        );
        anonymize_literals(&mut query);
        assert_eq!(query, expected);
    }

    #[test]
    fn test_anonymize_create_table_with_backticks() {
        let mut anonymizer = Anonymizer::new();
        let mut stmt = parse_create_table_statement(
            "CREATE TABLE `posts` (
            `post_no`      INT             NOT NULL,
            `user_no`      INT             NOT NULL,
            KEY         (`user_no`),
            FOREIGN KEY (`user_no`) REFERENCES `users` (`user_no`) ON DELETE CASCADE,
            PRIMARY KEY (`post_no`)
        ) COMMENT='Arbitrary PII Comment'",
        );

        let expected = parse_create_table_statement(
            "CREATE TABLE `anon_id_0` (
            `anon_id_1`      INT             NOT NULL,
            `anon_id_2`      INT             NOT NULL,
            KEY         (`anon_id_2`),
            FOREIGN KEY (`anon_id_2`) REFERENCES `anon_id_3` (`anon_id_2`) ON DELETE CASCADE,
            PRIMARY KEY (`anon_id_1`)
        ) COMMENT='<anonymized>'",
        );

        stmt.anonymize(&mut anonymizer);

        assert_eq!(stmt, expected);
    }

    #[test]
    fn anonymize_create_view() {
        let mut anonymizer = Anonymizer::new();
        let mut create_view = parse_create_view_statement(
            "CREATE VIEW v AS SELECT * FROM users WHERE username = \"bob\";",
        );
        let expected = parse_create_view_statement(
            "CREATE VIEW anon_id_0 AS SELECT * FROM anon_id_1 WHERE anon_id_2 = \"<anonymized>\";",
        );

        create_view.anonymize(&mut anonymizer);

        assert_eq!(create_view, expected);
    }

    #[test]
    fn anonymize_select_with_as() {
        let mut anonymizer = Anonymizer::new();
        let mut create_view = parse_select_statement("SELECT foo AS renamed_foo FROM bar");
        let expected = parse_select_statement("SELECT `anon_id_1` AS `anon_id_2` FROM `anon_id_0`");

        create_view.anonymize(&mut anonymizer);

        assert_eq!(create_view, expected);
    }
}
