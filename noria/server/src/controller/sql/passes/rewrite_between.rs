use nom_sql::{
    ConditionBase, ConditionExpression, ConditionTree, DeleteStatement, Operator, SelectStatement,
    SqlQuery, UpdateStatement,
};
use ConditionExpression::*;

/// Things that contain subexpressions of type `ConditionExpression` that can be targeted for the
/// desugaring of BETWEEN
pub trait RewriteBetween {
    /// Recursively rewrite all BETWEEN conditions in the given query into an ANDed pair of
    /// (inclusive) comparison operators. For example, the following query:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE n BETWEEN 1 AND 2;
    /// ```
    ///
    /// becomes:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE n >= 1 AND n >= 2;
    /// ```
    ///
    /// Invariant: The return value will have no recursive subexpressions of type
    /// `ConditionExpression::Between`
    fn rewrite_between(self) -> Self;
}

impl RewriteBetween for SelectStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_condition(where_clause));
        }
        self
    }
}

impl RewriteBetween for DeleteStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_condition(where_clause));
        }
        self
    }
}

impl RewriteBetween for UpdateStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_condition(where_clause));
        }
        self
    }
}

impl RewriteBetween for SqlQuery {
    fn rewrite_between(self) -> Self {
        match self {
            SqlQuery::Select(select) => SqlQuery::Select(select.rewrite_between()),
            SqlQuery::Delete(del) => SqlQuery::Delete(del.rewrite_between()),
            SqlQuery::CompoundSelect(mut compound_select) => {
                for (_, ref mut select) in compound_select.selects.iter_mut() {
                    *select = select.clone().rewrite_between()
                }
                SqlQuery::CompoundSelect(compound_select)
            }
            SqlQuery::Update(upd) => SqlQuery::Update(upd.rewrite_between()),
            _ => self,
        }
    }
}

fn rewrite_condition(cond: ConditionExpression) -> ConditionExpression {
    match cond {
        ComparisonOp(mut tree) => {
            tree.left = Box::new(rewrite_condition(*tree.left));
            tree.right = Box::new(rewrite_condition(*tree.right));
            ComparisonOp(tree)
        }
        LogicalOp(mut tree) => {
            tree.left = Box::new(rewrite_condition(*tree.left));
            tree.right = Box::new(rewrite_condition(*tree.right));
            LogicalOp(tree)
        }
        NegationOp(expr) => NegationOp(Box::new(rewrite_condition(*expr))),
        Base(ConditionBase::NestedSelect(sel)) => {
            Base(ConditionBase::NestedSelect(Box::new(sel.rewrite_between())))
        }
        base @ Base(_) => base,
        ari @ Arithmetic(_) => ari,
        Bracketed(cond) => rewrite_condition(*cond),
        Between { operand, min, max } => rewrite_between_condition(*operand, *min, *max),
        ExistsOp(select_stmt) => ExistsOp(Box::new(select_stmt.rewrite_between())),
    }
}

fn rewrite_between_condition(
    operand: ConditionExpression,
    min: ConditionExpression,
    max: ConditionExpression,
) -> ConditionExpression {
    ConditionExpression::LogicalOp(ConditionTree {
        operator: Operator::And,
        left: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
            operator: Operator::GreaterOrEqual,
            left: Box::new(operand.clone()),
            right: Box::new(rewrite_condition(min)),
        })),
        right: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
            operator: Operator::LessOrEqual,
            left: Box::new(operand.clone()),
            right: Box::new(rewrite_condition(max)),
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::parse_query;

    #[test]
    fn test_rewrite_top_level_between_in_select() {
        let query =
            parse_query("SELECT id FROM things WHERE frobulation BETWEEN 10 AND 17;").unwrap();
        let expected =
            parse_query("SELECT id FROM things WHERE frobulation >= 10 AND frobulation <= 17;")
                .unwrap();
        let result = query.rewrite_between();
        assert_eq!(result, expected, "result = {}", result);
    }
}
