use nom_sql::{Expr, FieldDefinitionExpr, FieldReference, OrderBy, SelectStatement, SqlQuery};
use readyset_errors::{internal, invalid_query_err, ReadySetResult};

pub trait RemoveNumericFieldReferences: Sized {
    /// Rewrite any [numeric field references][0] in the `GROUP BY` and `ORDER BY` clauses of the
    /// query into [expressions][1] given by the field in the `SELECT` list they reference.
    ///
    /// [0]: FieldReference::Numeric
    /// [0]: FieldReference::Expr
    fn remove_numeric_field_references(self) -> ReadySetResult<Self>;
}

impl RemoveNumericFieldReferences for SelectStatement {
    fn remove_numeric_field_references(mut self) -> ReadySetResult<Self> {
        let lookup_field = |n: usize| -> ReadySetResult<Expr> {
            let oob = invalid_query_err!("Out-of-bounds index in numeric field reference");

            if n == 0 {
                return Err(oob);
            }

            let fde: &FieldDefinitionExpr = self
                .fields
                .get(n - 1 /* numeric field references are 1-based */)
                .ok_or(oob)?;
            match fde {
                FieldDefinitionExpr::AllInTable(_) | FieldDefinitionExpr::All => {
                    internal!("Star should have been removed by now")
                }
                FieldDefinitionExpr::Expr { expr, .. } => Ok(expr.clone()),
            }
        };

        if let Some(gb) = &mut self.group_by {
            for field in &mut gb.fields {
                if let FieldReference::Numeric(n) = field {
                    *field = FieldReference::Expr(lookup_field(*n as _)?);
                }
            }
        }

        if let Some(order) = &mut self.order {
            for OrderBy { field, .. } in &mut order.order_by {
                if let FieldReference::Numeric(n) = field {
                    *field = FieldReference::Expr(lookup_field(*n as _)?);
                }
            }
        }

        Ok(self)
    }
}

impl RemoveNumericFieldReferences for SqlQuery {
    fn remove_numeric_field_references(self) -> ReadySetResult<Self> {
        match self {
            SqlQuery::CompoundSelect(mut cs) => {
                for (_, stmt) in &mut cs.selects {
                    *stmt = stmt.clone().remove_numeric_field_references()?;
                }
                Ok(SqlQuery::CompoundSelect(cs))
            }
            SqlQuery::Select(stmt) => Ok(SqlQuery::Select(stmt.remove_numeric_field_references()?)),
            _ => Ok(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Expr, GroupByClause, OrderClause, OrderType};

    use super::*;
    use crate::util::parse_select_statement;

    #[test]
    fn simple_group_by() {
        let query = parse_select_statement("select id, count(*) from t group by 1");
        let result = query.remove_numeric_field_references().unwrap();
        assert_eq!(
            result.group_by,
            Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column("id".into()))]
            })
        )
    }

    #[test]
    fn simple_order_by() {
        let query = parse_select_statement("select id from t order by 1 asc");
        let result = query.remove_numeric_field_references().unwrap();
        assert_eq!(
            result.order,
            Some(OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column("id".into())),
                    order_type: Some(OrderType::OrderAscending),
                    null_order: None
                }]
            })
        )
    }
}
