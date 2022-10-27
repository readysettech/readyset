#![feature(box_patterns, result_flattening, never_type, exhaustive_patterns)]

pub mod alias_removal;
pub mod anonymize;
mod count_star_rewrite;
mod create_table_columns;
mod detect_problematic_self_joins;
pub mod expr;
mod implied_tables;
mod key_def_coalescing;
mod normalize_topk_with_aggregate;
mod order_limit_removal;
mod remove_numeric_field_references;
mod resolve_schemas;
mod rewrite_between;
mod star_expansion;
mod strip_post_filters;
mod util;

use std::collections::{HashMap, HashSet};

use dataflow_expression::Dialect;
pub use nom_sql::analysis::{contains_aggregate, is_aggregate};
use nom_sql::{
    CompoundSelectStatement, CreateTableStatement, CreateViewStatement, Relation,
    SelectSpecification, SelectStatement, SqlIdentifier,
};
use readyset_errors::ReadySetResult;

pub use crate::alias_removal::AliasRemoval;
pub use crate::count_star_rewrite::CountStarRewrite;
pub use crate::create_table_columns::CreateTableColumns;
pub use crate::detect_problematic_self_joins::DetectProblematicSelfJoins;
pub use crate::expr::ScalarOptimizeExpressions;
pub use crate::implied_tables::ImpliedTableExpansion;
pub use crate::key_def_coalescing::KeyDefinitionCoalescing;
pub use crate::normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub use crate::order_limit_removal::OrderLimitRemoval;
pub use crate::remove_numeric_field_references::RemoveNumericFieldReferences;
pub use crate::resolve_schemas::ResolveSchemas;
pub use crate::rewrite_between::RewriteBetween;
pub use crate::star_expansion::StarExpansion;
pub use crate::strip_post_filters::StripPostFilters;
pub use crate::util::{
    is_correlated, is_logical_op, is_predicate, map_aggregates, outermost_table_exprs, LogicalOp,
};

/// Context provided to all query rewriting passes.
#[derive(Debug)]
pub struct RewriteContext<'a> {
    /// Map from names of views and tables in the database, to (ordered) lists of the column names
    /// in those views
    pub view_schemas: &'a HashMap<Relation, Vec<SqlIdentifier>>,

    /// Map from names of *tables* in the database, to the [`CreateTableStatement`] that was used
    /// to create that table. Each key in this map should also exist in [`view_schemas`].
    pub base_schemas: &'a HashMap<Relation, CreateTableStatement>,

    /// Map from schema name to the set of custom types in that schema
    pub custom_types: &'a HashMap<&'a SqlIdentifier, HashSet<&'a SqlIdentifier>>,

    /// Ordered list of schema names to search in when resolving schema names of tables
    pub search_path: &'a [SqlIdentifier],

    /// SQL dialect to use for all expressions and types within the query
    pub dialect: Dialect,

    /// Optional list of tables which, if created, should invalidate this query.
    ///
    /// This is (optionally) inserted into during rewriting of certain queries when the
    /// [resolve_schemas pass][] attempts to resolve a table within a schema but is unable to.
    ///
    /// [resolve_schemas pass]: crate::resolve_schemas
    pub invalidating_tables: Option<&'a mut Vec<Relation>>,
}

impl<'a> RewriteContext<'a> {
    pub(crate) fn tables(&self) -> HashMap<&'a SqlIdentifier, HashSet<&'a SqlIdentifier>> {
        self.view_schemas.keys().fold(
            HashMap::<&SqlIdentifier, HashSet<&SqlIdentifier>>::new(),
            |mut acc, tbl| {
                if let Some(schema) = &tbl.schema {
                    acc.entry(schema).or_default().insert(&tbl.name);
                }
                acc
            },
        )
    }
}

/// Extension trait providing the ability to rewrite a query to normalize, validate and desugar it.
///
/// Rewriting, which should never change the semantics of a query, can happen for any SQL statement,
/// and is provided a [context] with the schema of the database.
///
/// [context]: RewriteContext
pub trait Rewrite: Sized {
    /// Rewrite this SQL statement to normalize, validate, and desugar it
    fn rewrite(self, _context: &mut RewriteContext) -> ReadySetResult<Self> {
        Ok(self)
    }
}

impl Rewrite for CreateTableStatement {
    fn rewrite(self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        Ok(self
            .resolve_schemas(
                context.tables(),
                context.custom_types,
                context.search_path,
                context.invalidating_tables.as_deref_mut(),
            )
            .normalize_create_table_columns()
            .coalesce_key_definitions())
    }
}

impl Rewrite for SelectStatement {
    fn rewrite(self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        self.rewrite_between()
            .scalar_optimize_expressions(context.dialect)
            .strip_post_filters()
            .resolve_schemas(
                context.tables(),
                context.custom_types,
                context.search_path,
                context.invalidating_tables.as_deref_mut(),
            )
            .expand_stars(context.view_schemas)?
            .expand_implied_tables(context.view_schemas)?
            .normalize_topk_with_aggregate()?
            .rewrite_count_star(context.view_schemas)?
            .detect_problematic_self_joins()?
            .remove_numeric_field_references()?
            .order_limit_removal(context.base_schemas)
    }
}

impl Rewrite for CreateViewStatement {
    fn rewrite(mut self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        if self.name.schema.is_none() {
            if let Some(first_schema) = context.search_path.first() {
                self.name.schema = Some(first_schema.clone())
            }
        }

        Ok(Self {
            definition: Box::new(match *self.definition {
                SelectSpecification::Compound(cqs) => {
                    SelectSpecification::Compound(CompoundSelectStatement {
                        selects: cqs
                            .selects
                            .into_iter()
                            .map(|(op, sq)| Ok((op, sq.rewrite(context)?)))
                            .collect::<ReadySetResult<_>>()?,
                        ..cqs
                    })
                }
                SelectSpecification::Simple(sq) => {
                    SelectSpecification::Simple(sq.rewrite(context)?)
                }
            }),
            ..self
        })
    }
}
