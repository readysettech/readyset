use readyset_errors::{ReadySetResult, internal, invalid_query_err};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, FieldReference, OrderBy, SelectStatement, SqlQuery,
};

struct RemoveNumericFieldRefsVisitor;

impl<'ast> VisitorMut<'ast> for RemoveNumericFieldRefsVisitor {
    type Error = readyset_errors::ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Recurse into subqueries first
        visit_mut::walk_select_statement(self, select_statement)?;

        // Now rewrite numeric references in this statement's GROUP BY and ORDER BY
        let lookup_field = |n: usize| -> ReadySetResult<Expr> {
            let oob = invalid_query_err!("Out-of-bounds index in numeric field reference");

            if n == 0 {
                return Err(oob);
            }

            let fde: &FieldDefinitionExpr = select_statement
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

        if let Some(gb) = &mut select_statement.group_by {
            for field in &mut gb.fields {
                if let FieldReference::Numeric(n) = field {
                    *field = FieldReference::Expr(lookup_field(*n as _)?);
                }
            }
        }

        if let Some(order) = &mut select_statement.order {
            for OrderBy { field, .. } in &mut order.order_by {
                if let FieldReference::Numeric(n) = field {
                    *field = FieldReference::Expr(lookup_field(*n as _)?);
                }
            }
        }

        Ok(())
    }
}

pub trait RemoveNumericFieldReferences: Sized {
    /// Rewrite any [numeric field references][0] in the `GROUP BY` and `ORDER BY` clauses of the
    /// query into [expressions][1] given by the field in the `SELECT` list they reference.
    ///
    /// [0]: FieldReference::Numeric
    /// [0]: FieldReference::Expr
    fn remove_numeric_field_references(&mut self) -> ReadySetResult<&mut Self>;
}

impl RemoveNumericFieldReferences for SelectStatement {
    fn remove_numeric_field_references(&mut self) -> ReadySetResult<&mut Self> {
        let mut visitor = RemoveNumericFieldRefsVisitor;
        visitor.visit_select_statement(self)?;
        Ok(self)
    }
}

impl RemoveNumericFieldReferences for SqlQuery {
    fn remove_numeric_field_references(&mut self) -> ReadySetResult<&mut Self> {
        match self {
            SqlQuery::CompoundSelect(cs) => {
                for (_, stmt) in &mut cs.selects {
                    stmt.remove_numeric_field_references()?;
                }
            }
            SqlQuery::Select(stmt) => {
                stmt.remove_numeric_field_references()?;
            }
            _ => {}
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::DialectDisplay;
    use readyset_sql::ast::{Expr, GroupByClause, NullOrder, OrderClause, OrderType};

    use super::*;
    use crate::util::parse_select_statement;

    #[test]
    fn simple_group_by() {
        let mut query = parse_select_statement("select id, count(*) from t group by 1");
        query.remove_numeric_field_references().unwrap();
        assert_eq!(
            query.group_by,
            Some(GroupByClause {
                fields: vec![FieldReference::Expr(Expr::Column("id".into()))]
            })
        )
    }

    #[test]
    fn simple_order_by() {
        let mut query = parse_select_statement("select id from t order by 1 asc");
        query.remove_numeric_field_references().unwrap();
        assert_eq!(
            query.order,
            Some(OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column("id".into())),
                    order_type: Some(OrderType::OrderAscending),
                    null_order: NullOrder::NullsFirst
                }]
            })
        )
    }

    #[test]
    fn subquery_in_where_clause() {
        let mut query =
            parse_select_statement("select id from t where id in (select id from t2 group by 1)");
        query.remove_numeric_field_references().unwrap();
        // The outer query should be unchanged (no numeric refs)
        assert_eq!(query.group_by, None);
        // The subquery's GROUP BY should have been rewritten - check via display
        let sql = format!("{}", query.display(readyset_sql::Dialect::MySQL));
        assert!(
            !sql.contains("GROUP BY 1"),
            "numeric ref not removed from subquery: {sql}"
        );
    }

    #[test]
    fn subquery_in_from_clause() {
        let mut query =
            parse_select_statement("select x from (select id, count(*) from t group by 1) as sub");
        query.remove_numeric_field_references().unwrap();
        let sql = format!("{}", query.display(readyset_sql::Dialect::MySQL));
        assert!(
            !sql.contains("GROUP BY 1"),
            "numeric ref not removed from FROM subquery: {sql}"
        );
    }
}
