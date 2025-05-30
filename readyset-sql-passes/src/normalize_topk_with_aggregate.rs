use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::analysis::contains_aggregate;
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, FieldReference, LimitClause, OrderBy, SelectStatement, SqlQuery,
};
use readyset_sql::DialectDisplay;

pub trait NormalizeTopKWithAggregate: Sized {
    /// Remove any topk clause (order by, limit, offset) from a query with an aggregate without a
    /// GROUP BY clause, as in that case the topk clause won't change the results of the query
    /// (since it's only ever going to return one row)
    ///
    /// If the query *has* a GROUP BY clause, this query checks that all the columns in the ORDER BY
    /// clause either appear in the GROUP BY clause, or reference the results of aggregates, and
    /// returns an error otherwise.
    fn normalize_topk_with_aggregate(self) -> ReadySetResult<Self>;
}

impl NormalizeTopKWithAggregate for SelectStatement {
    fn normalize_topk_with_aggregate(mut self) -> ReadySetResult<Self> {
        if let Some(order) = self.order.take() {
            let aggs = self
                .fields
                .iter()
                .enumerate()
                .filter_map(|(i, f)| match f {
                    FieldDefinitionExpr::Expr { expr, alias } if contains_aggregate(expr) => {
                        Some((i, expr, alias))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();

            if !aggs.is_empty() {
                match &self.group_by {
                    Some(group_by) => {
                        // Each field in the order clause...
                        for OrderBy {
                            field: order_field, ..
                        } in &order.order_by
                        {
                            // ...must either appear in the group by clause...
                            let in_group_by_clause = group_by
                                .fields
                                .iter()
                                .any(|group_by_field| order_field == group_by_field);

                            // ...or reference the result of an aggregate...
                            let references_aggregate = match order_field {
                                // ... by number...
                                FieldReference::Numeric(n) => {
                                    aggs.iter().any(|(i, _, _)| *i == *n as usize)
                                }
                                // ... or by name
                                FieldReference::Expr(expr) => aggs.iter().any(|(_, agg, alias)| {
                                    *agg == expr
                                        || matches!(
                                            expr,
                                            Expr::Column(col)
                                                if alias.as_ref() == Some(&col.name)
                                        )
                                }),
                            };

                            if !in_group_by_clause && !references_aggregate {
                                return Err(ReadySetError::ExprNotInGroupBy {
                                    // FIXME(REA-2168): Use correct dialect.
                                    expression: order_field
                                        .display(readyset_sql::Dialect::MySQL)
                                        .to_string(),
                                    position: "ORDER BY".to_owned(),
                                });
                            }
                        }
                    }
                    None => {
                        // Save the offset.
                        let offset = match self.limit_clause {
                            LimitClause::LimitOffset { offset, .. } => offset,
                            LimitClause::OffsetCommaLimit { offset, .. } => Some(offset),
                        };
                        // order taken above, just leave it as None
                        self.limit_clause = LimitClause::LimitOffset {
                            limit: None,
                            offset,
                        };
                        return Ok(self);
                    }
                }
            }
            self.order = Some(order)
        }

        Ok(self)
    }
}

impl NormalizeTopKWithAggregate for SqlQuery {
    fn normalize_topk_with_aggregate(self) -> ReadySetResult<Self> {
        match self {
            SqlQuery::Select(stmt) => Ok(SqlQuery::Select(stmt.normalize_topk_with_aggregate()?)),
            _ => Ok(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::ast::{Expr, LimitValue, OrderClause, OrderType};
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_query;

    use super::*;

    fn removes_all_topk(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        let actual = input_query.normalize_topk_with_aggregate().unwrap();
        match actual {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(matches!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset { limit: None, .. }
                ));
            }
            _ => panic!("Invalid query returned: {actual:?}"),
        }
    }

    #[test]
    fn single_order_by_no_group_by() {
        removes_all_topk(
            "SELECT sum(table_1.column_10) AS alias_2
             FROM table_1
             ORDER BY table_1.column_4;",
        )
    }

    #[test]
    fn multiple_aggregates() {
        removes_all_topk(
            "SELECT sum(table_1.column_10), count(*)
             FROM table_1
             ORDER BY table_1.column_4;",
        )
    }

    #[test]
    fn single_aggregate_multiple_order() {
        removes_all_topk(
            "SELECT sum(table_1.column_10), count(*)
             FROM table_1
             ORDER BY table_1.column_4;",
        )
    }

    #[test]
    fn multi_aggregate_multi_order() {
        removes_all_topk(
            "SELECT sum(table_1.column_10), count(*)
             FROM table_1
             ORDER BY table_1.column_4, table_1.column_3 LIMIT 5;",
        )
    }

    #[test]
    fn no_topk() {
        removes_all_topk(
            "SELECT sum(table_1.column_10) AS alias_2
             FROM table_1",
        )
    }

    #[test]
    fn no_aggregate_leaves_topk() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT table_1.column_1 FROM table_1 order by column_3 asc limit 4;",
        )
        .unwrap();
        let result = query.normalize_topk_with_aggregate().unwrap();

        match result {
            SqlQuery::Select(stmt) => {
                assert_eq!(
                    stmt.order,
                    Some(OrderClause {
                        order_by: vec![OrderBy {
                            field: FieldReference::Expr(Expr::Column("column_3".into())),
                            order_type: Some(OrderType::OrderAscending),
                            null_order: None
                        }]
                    })
                );

                assert_eq!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset {
                        limit: Some(LimitValue::Literal(4.into())),
                        offset: None
                    }
                );
            }
            _ => panic!("Invalid query returned: {result:?}"),
        }
    }

    #[test]
    fn group_by_reference() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT table_1.column_2, sum(table_1.column_1)
             FROM table_1
             GROUP BY 1",
        )
        .unwrap();
        let result = query.clone().normalize_topk_with_aggregate().unwrap();

        assert_eq!(result, query);
    }

    #[test]
    fn order_by_not_in_group_by_returns_error() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT sum(table_1.column_1)
             FROM table_1
             GROUP BY column_2
             ORDER BY column_3 DESC",
        )
        .unwrap();
        let result = query.normalize_topk_with_aggregate();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(ReadySetError::ExprNotInGroupBy { .. })
        ))
    }

    #[test]
    fn order_by_in_group_by_does_nothing() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT sum(table_1.column_1)
             FROM table_1
             GROUP BY column_2
             ORDER BY column_2 DESC",
        )
        .unwrap();
        let result = query.clone().normalize_topk_with_aggregate().unwrap();
        assert_eq!(result, query);
    }

    #[test]
    fn order_by_aggregate_alias_does_nothing() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT sum(table_1.column_1) as sum
             FROM table_1
             GROUP BY column_2
             ORDER BY sum DESC",
        )
        .unwrap();
        let result = query.clone().normalize_topk_with_aggregate().unwrap();
        assert_eq!(result, query);
    }

    #[test]
    #[ignore] // TODO once we can properly parse expressions in ORDER position (ENG-418)
    fn order_by_aggregate_expr_does_nothing() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT sum(table_1.column_1)
             FROM table_1
             GROUP BY column_2
             ORDER BY sum(table_1.column_1) DESC",
        )
        .unwrap();
        let result = query.clone().normalize_topk_with_aggregate().unwrap();
        assert_eq!(result, query);
    }
}
