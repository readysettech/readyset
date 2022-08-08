use nom_sql::analysis::visit::Visitor;
use nom_sql::{Expr, SelectStatement};

use self::constant_fold::constant_fold_expr;
use self::normalize_negation::normalize_negation;

mod constant_fold;
mod normalize_negation;

pub fn scalar_optimize_expr(expr: &mut Expr) {
    constant_fold_expr(expr);
    normalize_negation(expr);
}

struct ScalarOptimizeExpressionsVisitor;

impl<'ast> Visitor<'ast> for ScalarOptimizeExpressionsVisitor {
    type Error = !;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        scalar_optimize_expr(expr);
        Ok(())
    }
}

pub trait ScalarOptimizeExpressions {
    fn scalar_optimize_expressions(self) -> Self;
}

impl ScalarOptimizeExpressions for SelectStatement {
    fn scalar_optimize_expressions(mut self) -> Self {
        let Ok(()) = ScalarOptimizeExpressionsVisitor.visit_select_statement(&mut self);
        self
    }
}
