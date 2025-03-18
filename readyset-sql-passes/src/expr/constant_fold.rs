use dataflow_expression::{Dialect, Expr as DataflowExpr, LowerContext};
use readyset_data::{DfType, DfValue};
use readyset_errors::{internal, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{Column, Expr, Literal, Relation};

/// Statically evaluate the given expression, returning a literal value representing the result.
///
/// Returns an error if the expression evaluation failed, or if the expression is not constant
fn const_eval(expr: &Expr, dialect: Dialect) -> ReadySetResult<Literal> {
    #[derive(Clone)]
    struct ConstEvalLowerContext;
    impl LowerContext for ConstEvalLowerContext {
        fn resolve_column(&self, _col: Column) -> ReadySetResult<(usize, DfType)> {
            internal!("Can't resolve column")
        }

        fn resolve_type(&self, _ty: Relation) -> Option<DfType> {
            // TODO(aspen): Support custom types in constant folding
            None
        }
    }

    let dataflow_expr = DataflowExpr::lower(expr.clone(), dialect, &ConstEvalLowerContext)?;
    let res = dataflow_expr.eval::<DfValue>(&[])?;
    res.try_into()
}

struct ConstantFoldVisitor {
    dialect: Dialect,
}

impl<'ast> VisitorMut<'ast> for ConstantFoldVisitor {
    type Error = std::convert::Infallible;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        if matches!(expr, Expr::Literal(_)) {
            return Ok(());
        }

        // Since we have to recursively traverse the expression's AST to convert it into a dataflow
        // expression anyway, we don't need to do an extra pass here to find if the expression is
        // constant-valued; we just try to evaluate it in a context where we return errors for
        // column references and placeholders, and then only use the result if that error doesn't
        // happen.
        match const_eval(expr, self.dialect) {
            Ok(res) => {
                *expr = Expr::Literal(res);
                Ok(())
            }
            Err(_) => visit_mut::walk_expr(self, expr),
        }
    }
}

/// Recursively normalize any subexpressions of the given expression which are *constant-valued*
/// (contain no references to columns) by evaluating them, and replacing them with their literal
/// result.
///
/// For example, this function would transform the following expression:
///
/// ```sql
/// x = ifnull(y, 1 + (4 * 5))
/// ```
///
/// into:
///
/// ```sql
/// x = ifnull(y, 21)
/// ```
pub fn constant_fold_expr(expr: &mut Expr, dialect: Dialect) {
    let Ok(()) = ConstantFoldVisitor { dialect }.visit_expr(expr);
}

#[cfg(test)]
mod tests {
    use readyset_sql::DialectDisplay;
    use readyset_sql_parsing::parse_expr;

    use super::*;

    fn rewrites_to(input: &str, expected: &str) {
        let mut expr = parse_expr(readyset_sql::Dialect::MySQL, input).unwrap();
        let expected = parse_expr(readyset_sql::Dialect::MySQL, expected).unwrap();
        constant_fold_expr(&mut expr, Dialect::DEFAULT_MYSQL);

        let expr = expr.display(readyset_sql::Dialect::MySQL).to_string();
        let expected = expected.display(readyset_sql::Dialect::MySQL).to_string();
        assert_eq!(expr, expected, "\nExpected; {expected}\n     Got: {expr}");
    }

    macro_rules! cf_tests {
        () => {};
        ($name:ident($input: expr, $expected: expr);$($rest:tt)*) => {
            #[test]
            fn $name() {
                rewrites_to($input, $expected)
            }

            cf_tests!($($rest)*);
        };
    }

    cf_tests! {
        add_simple("1 + 1", "2");
        and_simple("1 and 1", "1");
        eq_simple("1 = 1", "1");
        if_null_builtin("ifnull(1, 1)", "1");
        within_larger_expression_grouped("t.x + (4 + 5)", "t.x + 9");
        within_larger_expression_left_associative("4 + 5 + t.x", "9 + t.x");
        doc_example("x = ifnull(y, 1 + (4 * 5))", "x = ifnull(y, 21)");
    }
}
