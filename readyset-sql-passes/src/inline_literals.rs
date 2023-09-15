use std::collections::HashMap;

use nom_sql::analysis::visit_mut::VisitorMut;
use nom_sql::{ItemPlaceholder, Literal, SelectStatement};

struct InlineLiteralsVisitor<'a> {
    placeholder_literal_map: &'a HashMap<usize, Literal>,
}

pub trait InlineLiterals {
    fn inline_literals(self, placeholder_literal_map: &HashMap<usize, Literal>) -> Self;
}

impl<'ast, 'a> VisitorMut<'ast> for InlineLiteralsVisitor<'a> {
    type Error = !;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if let Literal::Placeholder(ItemPlaceholder::DollarNumber(p)) = literal {
            if let Some(inlined_literal) = self.placeholder_literal_map.get(&(*p as usize)) {
                *literal = inlined_literal.clone();
            }
        }
        Ok(())
    }
}

impl InlineLiterals for SelectStatement {
    fn inline_literals(mut self, placeholder_literal_map: &HashMap<usize, Literal>) -> Self {
        let mut visitor = InlineLiteralsVisitor {
            placeholder_literal_map,
        };
        let Ok(()) = visitor.visit_select_statement(&mut self);
        self
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use nom_sql::Literal;

    use super::InlineLiterals;
    use crate::util::parse_select_statement;

    #[test]
    fn replaces_literals() {
        let select = parse_select_statement(
            "SELECT $2, a FROM t WHERE t.a = $3 GROUP BY t.b HAVING sum(t.b) = $1",
        );
        let literals = vec![
            (1, Literal::String("one".into())),
            (2, Literal::Integer(2)),
            (3, Literal::Integer(3)),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let expected = parse_select_statement(
            "SELECT 2, a FROM t WHERE t.a = 3 GROUP BY t.b HAVING sum(t.b) = 'one'",
        );

        let select = select.inline_literals(&literals);
        assert_eq!(select, expected);
    }
}
