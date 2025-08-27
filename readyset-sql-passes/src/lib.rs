pub mod adapter_rewrites;
pub mod alias_removal;
pub mod anonymize;
mod create_table_columns;
mod detect_bucket_functions;
mod detect_problematic_self_joins;
pub mod detect_unsupported_placeholders;
mod disallow_row;
pub mod expr;
mod implied_tables;
mod infer_nullability;
mod inline_literals;
mod key_def_coalescing;
mod lateral_join;
mod normalize_topk_with_aggregate;
mod order_limit_removal;
mod remove_numeric_field_references;
mod resolve_schemas;
mod rewrite_between;
mod rewrite_utils;
mod star_expansion;
mod strip_literals;
mod tests;
mod unnest_subqueries;
mod unnest_subqueries_3vl;
mod util;
mod validate_window_functions;

use std::cell::RefMut;
use std::collections::{HashMap, HashSet};

use alias_removal::TableAliasRewrite;
use dataflow_expression::Dialect;
use disallow_row::DisallowRow;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{
    CompoundSelectStatement, CreateCacheStatement, CreateTableBody, CreateTableStatement,
    CreateViewStatement, NonReplicatedRelation, Relation, SelectSpecification, SelectStatement,
    SqlIdentifier,
};

pub use crate::alias_removal::AliasRemoval;
pub use crate::create_table_columns::CreateTableColumns;
pub use crate::detect_bucket_functions::DetectBucketFunctions;
pub use crate::detect_problematic_self_joins::DetectProblematicSelfJoins;
pub use crate::detect_unsupported_placeholders::DetectUnsupportedPlaceholders;
pub use crate::expr::ScalarOptimizeExpressions;
pub use crate::implied_tables::ImpliedTableExpansion;
pub use crate::inline_literals::InlineLiterals;
pub use crate::key_def_coalescing::KeyDefinitionCoalescing;
pub use crate::normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub use crate::order_limit_removal::OrderLimitRemoval;
pub use crate::remove_numeric_field_references::RemoveNumericFieldReferences;
pub use crate::resolve_schemas::ResolveSchemas;
pub use crate::resolve_schemas::ResolveSchemasContext;
pub use crate::rewrite_between::RewriteBetween;
pub use crate::star_expansion::StarExpansion;
pub use crate::strip_literals::{SelectStatementSkeleton, StripLiterals};
use crate::unnest_subqueries::UnnestSubqueries;
pub use crate::util::{
    LogicalOp, is_correlated, is_logical_op, is_predicate, map_aggregates, outermost_table_exprs,
};
pub use crate::validate_window_functions::ValidateWindowFunctions;

/// Can a particular relation (in the map passed to [`ResolveSchemas::resolve_schemas`]) be queried
/// from?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanQuery {
    Yes,
    No,
}

/// Context provided to all server-side query rewriting passes, i.e. those performed at migration
/// time, not those performed in the adapter on the hot path. For those passes, see
/// [`crate::adapter_rewrites`].
pub trait RewriteContext: ResolveSchemasContext {
    /// Map from names of views and tables in the database, to (ordered) lists of the column names
    /// in those views
    fn view_schemas(&self) -> &HashMap<Relation, Vec<SqlIdentifier>>;

    /// Map from names of *tables* in the database, to the body of the `CREATE TABLE` statement
    /// that was used to create that table. Each key in this map should also exist in
    /// [`view_schemas`].
    fn base_schemas(&self) -> &HashMap<&Relation, &CreateTableBody>;

    /// List of views that are known to exist but have not yet been compiled (so we can't know
    /// their fields yet)
    fn uncompiled_views(&self) -> &[&Relation];

    /// Set of relations that are known to exist in the upstream database, but are not being
    /// replicated. Used as part of schema resolution to ensure that queries that would resolve to
    /// these tables if they *were* being replicated correctly return an error
    fn non_replicated_relations(&self) -> &HashSet<NonReplicatedRelation>;

    /// SQL dialect to use for all expressions and types within the query
    fn dialect(&self) -> Dialect;

    /// Optional list of aliases that were removed during a rewrite.
    ///
    /// This is (optionally) inserted into during rewriting of certain queries when the
    /// [alias_removal][] removes aliases to other relations in a query.
    ///
    /// [alias_removal]: crate::alias_removal
    fn table_alias_rewrites(&self) -> Option<RefMut<'_, Vec<TableAliasRewrite>>>;

    /// The name of the cache for a migrated query.
    fn query_name(&self) -> Option<&str>;

    /// All tables, views, uncompiled views, and non-replicated relations indexable by schema and
    /// tagged with whether they can be queried.
    //
    // TODO(mvzink): Don't recalculate this on the fly; this may mean not providing it as a default
    // implementation for implementors and making them do it on construction.
    fn tables(&self) -> HashMap<&SqlIdentifier, HashMap<&SqlIdentifier, CanQuery>> {
        self.view_schemas()
            .keys()
            .chain(self.uncompiled_views().iter().copied())
            .map(|t| (t, CanQuery::Yes))
            .chain(
                self.non_replicated_relations()
                    .iter()
                    .map(|t| (&(t.name), CanQuery::No)),
            )
            .fold(
                HashMap::<&SqlIdentifier, HashMap<&SqlIdentifier, CanQuery>>::new(),
                |mut acc, (tbl, replicated)| {
                    if let Some(schema) = &tbl.schema {
                        acc.entry(schema).or_default().insert(&tbl.name, replicated);
                    }
                    acc
                },
            )
    }
}

impl<C: RewriteContext> RewriteContext for &C {
    fn view_schemas(&self) -> &HashMap<Relation, Vec<SqlIdentifier>> {
        (*self).view_schemas()
    }

    fn base_schemas(&self) -> &HashMap<&Relation, &CreateTableBody> {
        (*self).base_schemas()
    }

    fn uncompiled_views(&self) -> &[&Relation] {
        (*self).uncompiled_views()
    }

    fn non_replicated_relations(&self) -> &HashSet<NonReplicatedRelation> {
        (*self).non_replicated_relations()
    }

    fn dialect(&self) -> Dialect {
        (*self).dialect()
    }

    fn table_alias_rewrites(&self) -> Option<RefMut<'_, Vec<TableAliasRewrite>>> {
        (*self).table_alias_rewrites()
    }

    fn query_name(&self) -> Option<&str> {
        (*self).query_name()
    }
}

/// Extension trait providing the ability to rewrite a query to normalize, validate and desugar it.
///
/// Rewriting, which should never change the semantics of a query, can happen for any SQL statement,
/// and is provided a [`RewriteContext`] with the schema of the database.
pub trait Rewrite: Sized {
    /// Rewrite this SQL statement to normalize, validate, and desugar it
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self>;
}

impl Rewrite for CreateTableStatement {
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self> {
        self.resolve_schemas(&context)?
            .normalize_create_table_columns()
            .coalesce_key_definitions();
        Ok(self)
    }
}

impl Rewrite for SelectStatement {
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self> {
        let query_name = context.query_name().unwrap_or("unknown");

        self.rewrite_between()
            .disallow_row()?
            .validate_window_functions()?
            .scalar_optimize_expressions(context.dialect())
            .resolve_schemas(&context)?
            .expand_stars(
                context.view_schemas(),
                context.non_replicated_relations(),
                context.dialect().into(),
            )?
            .expand_implied_tables(context.view_schemas(), context.dialect().into())?
            .unnest_subqueries(&context)?
            .normalize_topk_with_aggregate(context.dialect().into())?
            .detect_problematic_self_joins()?
            .remove_numeric_field_references()?
            .order_limit_removal(context.base_schemas())?
            .rewrite_table_aliases(query_name, context.table_alias_rewrites())?;

        Ok(self)
    }
}

impl Rewrite for CompoundSelectStatement {
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self> {
        for (_op, sq) in &mut self.selects {
            sq.rewrite(&context)?;
        }
        Ok(self)
    }
}

impl Rewrite for SelectSpecification {
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self> {
        match self {
            SelectSpecification::Compound(csq) => {
                csq.rewrite(context)?;
            }
            SelectSpecification::Simple(sq) => {
                sq.rewrite(context)?;
            }
        }
        Ok(self)
    }
}

impl Rewrite for CreateViewStatement {
    fn rewrite<C: RewriteContext>(&mut self, context: C) -> ReadySetResult<&mut Self> {
        if self.name.schema.is_none()
            && let Some(first_schema) = context.search_path().first()
        {
            self.name.schema = Some(first_schema.clone())
        }
        if let Ok(def) = &mut self.definition {
            def.rewrite(context)?;
        }
        Ok(self)
    }
}

impl Rewrite for CreateCacheStatement {
    fn rewrite<C: RewriteContext>(&mut self, _context: C) -> ReadySetResult<&mut Self> {
        self.detect_and_validate_bucket_always()?;
        Ok(self)
    }
}
