use dataflow_expression::Dialect;
use nom_sql::analysis::visit_mut::VisitorMut;
use nom_sql::{Expr, SelectStatement};

use self::constant_fold::constant_fold_expr;
use self::normalize_negation::normalize_negation;

mod constant_fold;
mod normalize_negation;

pub fn scalar_optimize_expr(expr: &mut Expr, dialect: Dialect) {
    constant_fold_expr(expr, dialect);
    normalize_negation(expr);
}

struct ScalarOptimizeExpressionsVisitor {
    dialect: Dialect,
}

impl<'ast> VisitorMut<'ast> for ScalarOptimizeExpressionsVisitor {
    type Error = !;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        scalar_optimize_expr(expr, self.dialect);
        Ok(())
    }
}

pub trait ScalarOptimizeExpressions {
    fn scalar_optimize_expressions(self, dialect: Dialect) -> Self;
}

impl ScalarOptimizeExpressions for SelectStatement {
    fn scalar_optimize_expressions(mut self, dialect: Dialect) -> Self {
        let Ok(()) = ScalarOptimizeExpressionsVisitor { dialect }.visit_select_statement(&mut self);
        self
    }
}
