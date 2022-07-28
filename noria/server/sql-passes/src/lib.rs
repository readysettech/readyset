#![feature(box_patterns, result_flattening, never_type, exhaustive_patterns)]

pub mod alias_removal;
mod count_star_rewrite;
mod detect_problematic_self_joins;
mod implied_tables;
mod key_def_coalescing;
mod normalize_negation;
mod normalize_topk_with_aggregate;
mod order_limit_removal;
mod remove_numeric_field_references;
mod rewrite_between;
mod star_expansion;
mod strip_post_filters;
mod util;

use std::collections::HashMap;

pub use nom_sql::analysis::{contains_aggregate, is_aggregate};
use nom_sql::{CreateTableStatement, SelectStatement, SqlIdentifier};
use noria_errors::ReadySetResult;

pub use crate::alias_removal::AliasRemoval;
pub use crate::count_star_rewrite::CountStarRewrite;
pub use crate::detect_problematic_self_joins::DetectProblematicSelfJoins;
pub use crate::implied_tables::ImpliedTableExpansion;
pub use crate::key_def_coalescing::KeyDefinitionCoalescing;
pub use crate::normalize_negation::NormalizeNegation;
pub use crate::normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub use crate::order_limit_removal::OrderLimitRemoval;
pub use crate::remove_numeric_field_references::RemoveNumericFieldReferences;
pub use crate::rewrite_between::RewriteBetween;
pub use crate::star_expansion::StarExpansion;
pub use crate::strip_post_filters::StripPostFilters;
pub use crate::util::{
    is_correlated, is_logical_op, is_predicate, map_aggregates, outermost_table_exprs, LogicalOp,
};

/// Context provided to all query rewriting passes.
#[derive(Debug, Clone, Copy)]
pub struct RewriteContext<'a> {
    /// Map from names of views and tables in the database, to (ordered) lists of the column names
    /// in those views
    pub view_schemas: &'a HashMap<SqlIdentifier, Vec<SqlIdentifier>>,

    /// Map from names of *tables* in the database, to the [`CreateTableStatement`] that was used
    /// to create that table. Each key in this map should also exist in [`view_schemas`].
    pub base_schemas: &'a HashMap<SqlIdentifier, CreateTableStatement>,
}

/// Extension trait providing the ability to rewrite a query to normalize, validate and desugar it.
///
/// Rewriting, which should never change the semantics of a query, can happen for any SQL statement,
/// and is provided a [context] with the schema of the database.
///
/// [context]: RewriteContext
pub trait Rewrite: Sized {
    /// Rewrite this SQL statement to normalize, validate, and desugar it
    fn rewrite(self, _context: RewriteContext) -> ReadySetResult<Self> {
        Ok(self)
    }
}

impl Rewrite for CreateTableStatement {
    fn rewrite(self, _context: RewriteContext) -> ReadySetResult<Self> {
        Ok(self.coalesce_key_definitions())
    }
}

impl Rewrite for SelectStatement {
    fn rewrite(self, context: RewriteContext) -> ReadySetResult<Self> {
        self.rewrite_between()
            .normalize_negation()
            .strip_post_filters()
            .expand_stars(context.view_schemas)?
            .expand_implied_tables(context.view_schemas)?
            .normalize_topk_with_aggregate()?
            .rewrite_count_star(context.view_schemas)?
            .detect_problematic_self_joins()?
            .remove_numeric_field_references()?
            .order_limit_removal(context.base_schemas)
    }
}
