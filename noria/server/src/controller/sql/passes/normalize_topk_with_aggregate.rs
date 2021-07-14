use nom_sql::analysis::contains_aggregate;
use nom_sql::{FieldDefinitionExpression, SqlQuery};
use noria::{ReadySetError, ReadySetResult};

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

impl NormalizeTopKWithAggregate for SqlQuery {
    fn normalize_topk_with_aggregate(mut self) -> ReadySetResult<Self> {
        if let SqlQuery::Select(stmt) = &mut self {
            if let Some(order) = stmt.order.take() {
                let aggs = stmt
                    .fields
                    .iter()
                    .filter_map(|f| match f {
                        FieldDefinitionExpression::Expression { expr, alias }
                            if contains_aggregate(expr) =>
                        {
                            Some((expr, alias))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                if !aggs.is_empty() {
                    match &stmt.group_by {
                        Some(group_by) => {
                            for (order_col, _) in &order.columns {
                                if !(group_by
                                    .columns
                                    .iter()
                                    .any(|group_by_col| group_by_col == order_col)
                                    || aggs
                                        .iter()
                                        .any(|(_, alias)| alias.as_ref() == Some(&order_col.name)))
                                {
                                    return Err(ReadySetError::ExpressionNotInGroupBy {
                                        expression: order_col.to_string(),
                                        position: "ORDER BY".to_owned(),
                                    });
                                }
                            }
                        }
                        None => {
                            // order taken above, just leave it as None
                            stmt.limit = None;
                            return Ok(self);
                        }
                    }
                }
                stmt.order = Some(order)
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, LimitClause, OrderClause, OrderType};

    use super::*;

    fn removes_all_topk(input: &str) {
        let input_query = parse_query(input).unwrap();
        let actual = input_query.normalize_topk_with_aggregate().unwrap();
        match actual {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(stmt.limit.is_none());
            }
            _ => panic!("Invalid query returned: {:?}", actual),
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
        let query =
            parse_query("SELECT table_1.column_1 FROM table_1 order by column_3 asc limit 4;")
                .unwrap();
        let result = query.normalize_topk_with_aggregate().unwrap();

        match result {
            SqlQuery::Select(stmt) => {
                assert_eq!(
                    stmt.order,
                    Some(OrderClause {
                        columns: vec![("column_3".into(), OrderType::OrderAscending)]
                    })
                );

                assert_eq!(
                    stmt.limit,
                    Some(LimitClause {
                        limit: 4,
                        offset: 0,
                    })
                );
            }
            _ => panic!("Invalid query returned: {:?}", result),
        }
    }

    #[test]
    fn order_by_not_in_group_by_returns_error() {
        let query = parse_query(
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
            Some(ReadySetError::ExpressionNotInGroupBy { .. })
        ))
    }

    #[test]
    fn order_by_in_group_by_does_nothing() {
        let query = parse_query(
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
