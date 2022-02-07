pub(crate) mod alias_removal;
mod count_star_rewrite;
mod detect_problematic_self_joins;
mod implied_tables;
mod key_def_coalescing;
mod negation_removal;
mod normalize_topk_with_aggregate;
mod order_limit_removal;
mod rewrite_between;
mod star_expansion;
mod strip_post_filters;
pub(crate) mod subqueries;

use std::collections::HashMap;

pub(crate) use alias_removal::AliasRemoval;
pub(crate) use count_star_rewrite::CountStarRewrite;
pub(crate) use detect_problematic_self_joins::DetectProblematicSelfJoins;
pub(crate) use implied_tables::ImpliedTableExpansion;
pub(crate) use key_def_coalescing::KeyDefinitionCoalescing;
pub(crate) use negation_removal::NegationRemoval;
use nom_sql::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, JoinClause,
    JoinRightSide, SelectStatement,
};
pub(crate) use normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub(crate) use order_limit_removal::OrderLimitRemoval;
pub(crate) use rewrite_between::RewriteBetween;
pub(crate) use star_expansion::StarExpansion;
pub(crate) use strip_post_filters::StripPostFilters;
pub(crate) use subqueries::SubQueries;

fn field_names(statement: &SelectStatement) -> impl Iterator<Item = &str> {
    statement.fields.iter().filter_map(|field| match &field {
        FieldDefinitionExpression::Expression {
            alias: Some(alias), ..
        } => Some(alias.as_str()),
        FieldDefinitionExpression::Expression {
            expr: Expression::Column(Column { name, .. }),
            ..
        } => Some(name.as_str()),
        _ => None,
    })
}

/// Returns a map from subquery aliases to vectors of the fields in those subqueries.
///
/// Takes only the CTEs and join clause so that it doesn't have to borrow the entire statement.
pub(self) fn subquery_schemas<'a>(
    ctes: &'a [CommonTableExpression],
    join: &'a [JoinClause],
) -> HashMap<&'a str, Vec<&'a str>> {
    ctes.iter()
        .map(|cte| (cte.name.as_str(), &cte.statement))
        .chain(join.iter().filter_map(|join| match &join.right {
            JoinRightSide::NestedSelect(stmt, Some(name)) => Some((name.as_str(), stmt.as_ref())),
            _ => None,
        }))
        .map(|(name, stmt)| (name, field_names(stmt).collect()))
        .collect()
}
