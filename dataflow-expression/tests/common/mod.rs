use dataflow_expression::{Expr, LowerContext};
use nom_sql::{parse_expr, Column, Relation};
use readyset_data::{DfType, DfValue};
use readyset_errors::{internal, ReadySetResult};

#[derive(Debug, Clone, Copy)]
struct TestLowerContext;
impl LowerContext for TestLowerContext {
    fn resolve_column(&self, _col: Column) -> ReadySetResult<(usize, DfType)> {
        internal!("Column references not allowed")
    }

    fn resolve_type(&self, _ty: Relation) -> Option<DfType> {
        None
    }
}

pub fn parse_lower_eval(
    expr: &str,
    parser_dialect: nom_sql::Dialect,
    expr_dialect: dataflow_expression::Dialect,
) -> DfValue {
    let ast = parse_expr(parser_dialect, expr).unwrap();
    let lowered = Expr::lower(ast, expr_dialect, TestLowerContext).unwrap();
    lowered.eval::<DfValue>(&[]).unwrap()
}
