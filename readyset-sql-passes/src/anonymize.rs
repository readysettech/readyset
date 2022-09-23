//! Anonymize
//!
//! Provides helpers that provide anonymizaion of SQL statements

use nom_sql::analysis::visit::Visitor;
use nom_sql::{Literal, SelectStatement};

/// This pass replaces every instance of `Literal`, except Placeholders, in the AST with
/// `Literal::String("<anonymized>")`
struct AnonymizeLiteralsVisitor;
impl<'ast> Visitor<'ast> for AnonymizeLiteralsVisitor {
    type Error = !;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(literal, Literal::Placeholder(_)) {
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

#[cfg(test)]
mod tests {
    use nom_sql::Dialect;

    use super::*;

    fn parse_select_statement(q: &str) -> SelectStatement {
        nom_sql::parse_select_statement(Dialect::MySQL, q).unwrap()
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
}
