use dataflow_expression::{Expr, LowerContext};
use readyset_data::{DfType, DfValue};
use readyset_errors::{internal, ReadySetResult};
use readyset_sql::ast::{Column, Relation};
use readyset_sql_parsing::parse_expr;

#[derive(Debug, Clone, Copy)]
struct TestLowerContext;
impl LowerContext for TestLowerContext {
    fn resolve_column(&self, _col: Column) -> ReadySetResult<(usize, DfType)> {
        internal!("Column references not allowed")
    }

    fn resolve_type(&self, ty: Relation) -> Option<DfType> {
        match ty.name.as_str() {
            "abc" => Some(DfType::from_enum_variants(
                vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                None,
            )),
            "cba" => Some(DfType::from_enum_variants(
                vec!["c".to_owned(), "b".to_owned(), "a".to_owned()],
                None,
            )),
            _ => None,
        }
    }
}

pub fn parse_lower_eval(
    expr: &str,
    parser_dialect: readyset_sql::Dialect,
    expr_dialect: dataflow_expression::Dialect,
) -> ReadySetResult<DfValue> {
    let ast = parse_expr(parser_dialect, expr).unwrap();
    let lowered = Expr::lower(ast, expr_dialect, &TestLowerContext).unwrap();
    lowered.eval::<DfValue>(&[])
}
