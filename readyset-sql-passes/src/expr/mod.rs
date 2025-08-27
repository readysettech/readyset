use dataflow_expression::Dialect;
use readyset_sql::analysis::visit_mut::VisitorMut;
use readyset_sql::ast::{Expr, SelectStatement};

use crate::RewriteDialectContext;

use self::constant_fold::constant_fold_expr;
use self::normalize_negation::normalize_negation;

pub(crate) mod constant_fold;
mod normalize_negation;

pub fn scalar_optimize_expr(expr: &mut Expr, dialect: Dialect) {
    constant_fold_expr(expr, dialect);
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
