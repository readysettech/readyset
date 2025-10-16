use dataflow_expression::{Dialect, Expr as DataflowExpr, LowerContext};
use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetResult, internal};
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
    preserve_casts: bool,
}

impl<'ast> VisitorMut<'ast> for ConstantFoldVisitor {
    type Error = std::convert::Infallible;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        if matches!(expr, Expr::Literal(_)) {
            return Ok(());
        }

        // When preserve_casts is enabled, don't fold away Cast expressions — they carry type
        // information that is semantically important. For example, PostgreSQL uses casts for
        // function overload resolution: `ARRAY_AGG('A'::CHAR)` is valid but `ARRAY_AGG('A')`
        // fails because the literal 'A' has type `unknown` which is ambiguous. Instead, recurse
        // into the inner expression so that sub-expressions still get folded (e.g.,
        // `CAST(1 + 2 AS INT)` becomes `CAST(3 AS INT)`).
        if self.preserve_casts && matches!(expr, Expr::Cast { .. }) {
            return visit_mut::walk_expr(self, expr);
        }

        if let Expr::Row { exprs, .. } = expr {
            for e in exprs {
                self.visit_expr(e)?;
            }
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
///
/// Note: this will also fold away `CAST` expressions (e.g. `'A'::CHAR` becomes `'A'`). If you
/// need to preserve `CAST` type annotations (e.g. for PostgreSQL function overload resolution),
/// use [`constant_fold_expr_preserving_casts`] instead.
pub fn constant_fold_expr(expr: &mut Expr, dialect: Dialect) {
    let Ok(()) = ConstantFoldVisitor {
        dialect,
        preserve_casts: false,
    }
    .visit_expr(expr);
}

/// Like [`constant_fold_expr`], but preserves `CAST`/`::` type annotations instead of folding
/// them away.
///
/// This is important for PostgreSQL where casts carry semantic meaning for function overload
/// resolution. For example, `ARRAY_AGG('A'::CHAR)` requires the `::CHAR` cast — without it,
/// PostgreSQL cannot resolve the `array_agg(unknown)` overload.
///
/// Non-Cast inner sub-expressions of a Cast are still folded: `CAST(1 + 2 AS INT)` becomes
/// `CAST(3 AS INT)`. Nested Casts are also preserved (e.g., `CAST(CAST(1+2 AS INT) AS TEXT)`
/// becomes `CAST(CAST(3 AS INT) AS TEXT)`).
pub fn constant_fold_expr_preserving_casts(expr: &mut Expr, dialect: Dialect) {
    let Ok(()) = ConstantFoldVisitor {
        dialect,
        preserve_casts: true,
    }
    .visit_expr(expr);
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
        ($name:ident($input: expr_2021, $expected: expr_2021);$($rest:tt)*) => {
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

    /// Helper for PostgreSQL-dialect constant folding tests that preserve casts.
    fn pg_preserving_casts_rewrites_to(input: &str, expected: &str) {
        let mut expr = parse_expr(readyset_sql::Dialect::PostgreSQL, input).unwrap();
        let expected = parse_expr(readyset_sql::Dialect::PostgreSQL, expected).unwrap();
        constant_fold_expr_preserving_casts(&mut expr, Dialect::DEFAULT_POSTGRESQL);

        let expr = expr.display(readyset_sql::Dialect::PostgreSQL).to_string();
        let expected = expected
            .display(readyset_sql::Dialect::PostgreSQL)
            .to_string();
        assert_eq!(expr, expected, "\nExpected: {expected}\n     Got: {expr}");
    }

    /// Cast of a literal must not be folded away — the type annotation carries semantic
    /// meaning in PostgreSQL (REA-6285).
    #[test]
    fn cast_literal_preserved() {
        pg_preserving_casts_rewrites_to("'A'::char", "'A'::char");
    }

    /// Cast with a foldable inner expression: the inner arithmetic should be folded
    /// but the outer Cast must remain.
    #[test]
    fn cast_with_foldable_inner() {
        pg_preserving_casts_rewrites_to("CAST(1 + 2 AS int)", "CAST(3 AS int)");
    }

    /// Cast must be preserved inside a non-constant expression so PostgreSQL can
    /// resolve function overloads (e.g., ARRAY_AGG('A'::CHAR) — REA-6285).
    /// Here we use a column reference to make the outer expression non-constant,
    /// simulating the aggregate case.
    #[test]
    fn cast_preserved_in_non_constant_context() {
        pg_preserving_casts_rewrites_to("t.x = 'A'::char", "t.x = 'A'::char");
    }

    /// Nested casts: both Cast wrappers are preserved, but non-Cast inner
    /// sub-expressions (like arithmetic) are still folded.
    #[test]
    fn nested_casts_both_preserved() {
        pg_preserving_casts_rewrites_to(
            "CAST(CAST(1 + 2 AS int) AS text)",
            "CAST(CAST(3 AS int) AS text)",
        );
    }

    /// PostgreSQL helper for non-preserving constant folding tests.
    fn pg_rewrites_to(input: &str, expected: &str) {
        let mut expr = parse_expr(readyset_sql::Dialect::PostgreSQL, input).unwrap();
        let expected = parse_expr(readyset_sql::Dialect::PostgreSQL, expected).unwrap();
        constant_fold_expr(&mut expr, Dialect::DEFAULT_POSTGRESQL);

        let expr = expr.display(readyset_sql::Dialect::PostgreSQL).to_string();
        let expected = expected
            .display(readyset_sql::Dialect::PostgreSQL)
            .to_string();
        assert_eq!(expr, expected, "\nExpected: {expected}\n     Got: {expr}");
    }

    /// Array constructors with all-literal elements must NOT be folded into a string literal.
    /// Folding `ARRAY[0, 10, 20]` to `'{0,10,20}'` breaks `IN` comparisons because the
    /// subsequent equality check compares an array against a string (REA-6335).
    #[test]
    fn array_literal_not_folded_to_string() {
        pg_rewrites_to("ARRAY[0, 10, 20]", "ARRAY[0, 10, 20]");
    }

    /// When an array constructor appears in an `IN` expression, constant folding must leave the
    /// RHS array constructors intact so the `IN` desugars to element-wise array equality.
    #[test]
    fn array_in_list_not_folded() {
        pg_rewrites_to(
            "t.x IN (ARRAY[0, 10, 20], ARRAY[1, 2, 3])",
            "t.x IN (ARRAY[0, 10, 20], ARRAY[1, 2, 3])",
        );
    }

    /// Verify that the non-preserving `constant_fold_expr` still folds casts to literals,
    /// since `rewrite_utils.rs` callers depend on this for fallback value computation.
    #[test]
    fn non_preserving_folds_cast() {
        let mut expr = parse_expr(readyset_sql::Dialect::PostgreSQL, "'A'::char").unwrap();
        constant_fold_expr(&mut expr, Dialect::DEFAULT_POSTGRESQL);
        assert!(
            matches!(expr, Expr::Literal(_)),
            "constant_fold_expr should fold 'A'::char to a literal, got: {expr:?}"
        );
    }
}
