pub use nom_sql::analysis::{contains_aggregate, is_aggregate};
use nom_sql::{Column, Expression, FunctionExpression};

#[must_use]
pub(crate) fn map_aggregates(expr: &mut Expression) -> Vec<(FunctionExpression, String)> {
    let mut ret = Vec::new();
    match expr {
        Expression::Arithmetic(ari) => {
            ret.append(&mut map_aggregates(&mut ari.left));
            ret.append(&mut map_aggregates(&mut ari.right));
        }
        Expression::Call(f) if is_aggregate(f) => {
            let name = f.to_string();
            ret.push((f.clone(), name.clone()));
            *expr = Expression::Column(Column {
                name,
                table: None,
                function: None,
            });
        }
        Expression::CaseWhen {
            condition: _,
            then_expr,
            else_expr,
        } => {
            // FIXME(grfn): see above
            ret.append(&mut map_aggregates(then_expr));
            if let Some(else_expr) = else_expr {
                ret.append(&mut map_aggregates(else_expr));
            }
        }
        Expression::Call(_) | Expression::Literal(_) | Expression::Column(_) => {}
    }
    ret
}
