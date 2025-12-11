pub mod adapter_rewrites;
pub mod alias_removal;
pub mod anonymize;
mod array_constructor;
mod create_table_columns;
mod detect_bucket_functions;
mod detect_problematic_self_joins;
pub mod detect_unsupported_placeholders;
mod disallow_row;
mod drop_redundant_join;
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
use disallow_row::DisallowRow;
use readyset_data::Dialect;
use readyset_errors::ReadySetResult;
use readyset_sql::DialectDisplay as _;
use readyset_sql::ast::{
    CompoundSelectStatement, CreateCacheStatement, CreateTableBody, CreateTableStatement,
    CreateViewStatement, NonReplicatedRelation, Relation, SelectSpecification, SelectStatement,
    SqlIdentifier,
};
use tracing::{trace, trace_span};

pub use crate::alias_removal::AliasRemoval;
pub use crate::array_constructor::ArrayConstructorRewrite;
pub use crate::create_table_columns::CreateTableColumns;
pub use crate::detect_bucket_functions::DetectBucketFunctions;
pub use crate::detect_problematic_self_joins::DetectProblematicSelfJoins;
pub use crate::detect_unsupported_placeholders::DetectUnsupportedPlaceholders;
use crate::drop_redundant_join::DropRedundantSelfJoin;
pub use crate::expr::ScalarOptimizeExpressions;
pub use crate::implied_tables::ImpliedTableExpansion;
pub use crate::implied_tables::ImpliedTablesContext;
pub use crate::inline_literals::InlineLiterals;
pub use crate::key_def_coalescing::KeyDefinitionCoalescing;
pub use crate::normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub use crate::order_limit_removal::OrderLimitRemoval;
pub use crate::remove_numeric_field_references::RemoveNumericFieldReferences;
pub use crate::resolve_schemas::ResolveSchemas;
pub use crate::resolve_schemas::ResolveSchemasContext;
pub use crate::rewrite_between::RewriteBetween;
pub use crate::star_expansion::StarExpansion;
pub use crate::star_expansion::StarExpansionContext;
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

pub trait RewriteDialectContext {
    /// The dialect used for dataflow expression evaluation.
    fn dialect(&self) -> Dialect;
}

impl<C: RewriteDialectContext> RewriteDialectContext for &C {
    fn dialect(&self) -> Dialect {
        (*self).dialect()
    }
}

impl RewriteDialectContext for Dialect {
    fn dialect(&self) -> Dialect {
        *self
    }
}

impl RewriteDialectContext for readyset_sql::Dialect {
    fn dialect(&self) -> Dialect {
        (*self).into()
    }
}

/// Context provided to all server-side query rewriting passes, i.e. those performed at migration
/// time, not those performed in the adapter on the hot path. For those passes, see
/// [`crate::adapter_rewrites`].
pub trait RewriteContext:
    ResolveSchemasContext + StarExpansionContext + ImpliedTablesContext + RewriteDialectContext
{
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

    /// Optional list of aliases that were removed during a rewrite.
    ///
    /// This is (optionally) inserted into during rewriting of certain queries when the
    /// [alias_removal][] removes aliases to other relations in a query.
    ///
    /// [alias_removal]: crate::alias_removal
    fn table_alias_rewrites(&self) -> Option<RefMut<'_, Vec<TableAliasRewrite>>>;

    /// The name of the cache for a migrated query.
    fn query_name(&self) -> Option<&str>;
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
        let span = trace_span!("server_rewrites").entered();
        let query_name = context.query_name().unwrap_or("unknown");
        let sql_dialect = context.dialect().into();
        trace!(query_pre = %self.display(sql_dialect), "Rewriting query");

        self.rewrite_between();
        trace!(parent: &span, pass="rewrite_between", query = %self.display(sql_dialect));
        self.disallow_row()?;
        trace!(parent: &span, pass="disallow_row", query = %self.display(sql_dialect));
        self.validate_window_functions()?;
        trace!(parent: &span, pass="validate_window_functions", query = %self.display(sql_dialect));
        self.scalar_optimize_expressions(&context);
        trace!(parent: &span, pass="scalar_optimize_expressions", query = %self.display(sql_dialect));
        self.resolve_schemas(&context)?;
        trace!(parent: &span, pass="resolve_schemas", query = %self.display(sql_dialect));
        self.expand_stars(&context)?;
        trace!(parent: &span, pass="expand_stars", query = %self.display(sql_dialect));
        self.expand_implied_tables(&context)?;
        trace!(parent: &span, pass="expand_implied_tables", query = %self.display(sql_dialect));
        self.rewrite_array_constructors()?;
        trace!(parent: &span, pass="rewrite_array_constructors", query = %self.display(sql_dialect));
        self.drop_redundant_join(&context)?;
        trace!(parent: &span, pass="drop_redundant_join", query = %self.display(sql_dialect));
        self.unnest_subqueries(&context)?;
        trace!(parent: &span, pass="unnest_subqueries", query = %self.display(sql_dialect));
        self.normalize_topk_with_aggregate(&context)?;
        trace!(parent: &span, pass="normalize_topk_with_aggregate", query = %self.display(sql_dialect));
        self.detect_problematic_self_joins()?;
        trace!(parent: &span, pass="detect_problematic_self_joins", query = %self.display(sql_dialect));
        self.remove_numeric_field_references()?;
        trace!(parent: &span, pass="remove_numeric_field_references", query = %self.display(sql_dialect));
        self.order_limit_removal(context.base_schemas())?;
        trace!(parent: &span, pass="order_limit_removal", query = %self.display(sql_dialect));
        self.rewrite_table_aliases(query_name, context.table_alias_rewrites())?;
        trace!(parent: &span, pass="rewrite_table_aliases", query = %self.display(sql_dialect));

        trace!(query_post = %self.display(sql_dialect), "Query rewritten");
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
