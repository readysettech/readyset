use dataflow_expression::Dialect;
use readyset_sql::analysis::visit_mut::VisitorMut;
use readyset_sql::ast::{Expr, SelectStatement};

use crate::RewriteDialectContext;

use self::constant_fold::constant_fold_expr_preserving_casts;
use self::normalize_negation::normalize_negation;

pub(crate) mod constant_fold;
mod normalize_negation;

pub fn scalar_optimize_expr(expr: &mut Expr, dialect: Dialect) {
    // Use the cast-preserving variant so that type annotations like `'A'::CHAR` are retained in
    // stored query definitions. PostgreSQL needs these for function overload resolution (REA-6285).
    // This also preserves MySQL casts, which is harmless -- preserving type info is strictly more
    // conservative than folding it away.
    constant_fold_expr_preserving_casts(expr, dialect);
    normalize_negation(expr);
}

struct ScalarOptimizeExpressionsVisitor {
    dialect: Dialect,
}

impl<'ast> VisitorMut<'ast> for ScalarOptimizeExpressionsVisitor {
    type Error = std::convert::Infallible;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        scalar_optimize_expr(expr, self.dialect);
        Ok(())
    }
}

pub trait ScalarOptimizeExpressions {
    fn scalar_optimize_expressions<C: RewriteDialectContext>(&mut self, context: C) -> &mut Self;
}

impl ScalarOptimizeExpressions for SelectStatement {
    fn scalar_optimize_expressions<C: RewriteDialectContext>(&mut self, context: C) -> &mut Self {
        let Ok(()) = ScalarOptimizeExpressionsVisitor {
            dialect: context.dialect(),
        }
        .visit_select_statement(self);
        self
    }
}
