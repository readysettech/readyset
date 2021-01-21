use nom_sql::{
    BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, DeleteStatement, Literal,
    SelectStatement, SqlQuery, UpdateStatement,
};
use ConditionExpression::*;

pub trait StripPostFilters {
    /// Remove all filters from the given query that cannot be done as nodes in the query graph, and
    /// require a post-lookup filter. Currently, this is LIKE and ILIKE against a placeholder.
    fn strip_post_filters(self) -> Self;
}

impl StripPostFilters for Option<ConditionExpression> {
    fn strip_post_filters(self) -> Self {
        self.and_then(|conds| match conds {
            ComparisonOp(ConditionTree {
                operator: BinaryOperator::ILike | BinaryOperator::Like,
                left: box Base(ConditionBase::Field(_)),
                right: box Base(ConditionBase::Literal(Literal::Placeholder(_))),
            }) => None,
            LogicalOp(ConditionTree {
                operator,
                left,
                right,
            }) => match (
                Some(*left).strip_post_filters(),
                Some(*right).strip_post_filters(),
            ) {
                (None, None) => None,
                (Some(cond), None) | (None, Some(cond)) => Some(cond),
                (Some(left), Some(right)) => Some(LogicalOp(ConditionTree {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                })),
            },
            Bracketed(box cond) => Some(cond).strip_post_filters(),
            _ => Some(conds),
        })
    }
}

impl StripPostFilters for SelectStatement {
    fn strip_post_filters(mut self) -> Self {
        self.where_clause = self.where_clause.strip_post_filters();
        self
    }
}

impl StripPostFilters for DeleteStatement {
    fn strip_post_filters(mut self) -> Self {
        self.where_clause = self.where_clause.strip_post_filters();
        self
    }
}

impl StripPostFilters for UpdateStatement {
    fn strip_post_filters(mut self) -> Self {
        self.where_clause = self.where_clause.strip_post_filters();
        self
    }
}

impl StripPostFilters for SqlQuery {
    fn strip_post_filters(self) -> Self {
        match self {
            SqlQuery::Select(select) => SqlQuery::Select(select.strip_post_filters()),
            SqlQuery::Delete(del) => SqlQuery::Delete(del.strip_post_filters()),
            SqlQuery::CompoundSelect(mut compound_select) => {
                compound_select.selects = compound_select
                    .selects
                    .drain(..)
                    .map(|(op, stmt)| (op, stmt.strip_post_filters()))
                    .collect();
                SqlQuery::CompoundSelect(compound_select)
            }
            SqlQuery::Update(upd) => SqlQuery::Update(upd.strip_post_filters()),
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::parse_query;

    #[test]
    fn strip_ilike() {
        let query = parse_query("SELECT id FROM posts WHERE title ILIKE ?;").unwrap();
        let expected = parse_query("SELECT id FROM posts;").unwrap();
        let result = query.strip_post_filters();
        assert_eq!(result, expected, "result = {}", result);
    }

    #[test]
    fn strip_ilike_with_other_conds() {
        let query = parse_query("SELECT id FROM posts WHERE title ILIKE ? AND id < 5;").unwrap();
        let expected = parse_query("SELECT id FROM posts WHERE id < 5;").unwrap();
        let result = query.strip_post_filters();
        assert_eq!(result, expected, "result = {}", result);
    }
}
