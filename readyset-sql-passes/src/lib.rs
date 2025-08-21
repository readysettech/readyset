pub mod adapter_rewrites;
pub mod alias_removal;
pub mod anonymize;
mod create_table_columns;
mod detect_problematic_self_joins;
pub mod detect_unsupported_placeholders;
mod disallow_row;
pub mod expr;
mod implied_tables;
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
mod util;
mod validate_window_functions;

use std::collections::{HashMap, HashSet};

use alias_removal::TableAliasRewrite;
use dataflow_expression::Dialect;
use disallow_row::DisallowRow;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{
    CompoundSelectStatement, CreateTableBody, CreateTableStatement, CreateViewStatement,
    NonReplicatedRelation, Relation, SelectSpecification, SelectStatement, SqlIdentifier,
};

pub use crate::alias_removal::AliasRemoval;
pub use crate::create_table_columns::CreateTableColumns;
pub use crate::detect_problematic_self_joins::DetectProblematicSelfJoins;
pub use crate::detect_unsupported_placeholders::DetectUnsupportedPlaceholders;
pub use crate::expr::ScalarOptimizeExpressions;
pub use crate::implied_tables::ImpliedTableExpansion;
pub use crate::inline_literals::InlineLiterals;
pub use crate::key_def_coalescing::KeyDefinitionCoalescing;
use crate::lateral_join::RewriteLateralJoin;
pub use crate::normalize_topk_with_aggregate::NormalizeTopKWithAggregate;
pub use crate::order_limit_removal::OrderLimitRemoval;
pub use crate::remove_numeric_field_references::RemoveNumericFieldReferences;
pub use crate::resolve_schemas::ResolveSchemas;
pub use crate::rewrite_between::RewriteBetween;
pub use crate::star_expansion::StarExpansion;
pub use crate::strip_literals::{SelectStatementSkeleton, StripLiterals};
pub use crate::util::{
    is_correlated, is_logical_op, is_predicate, map_aggregates, outermost_table_exprs, LogicalOp,
};
pub use crate::validate_window_functions::ValidateWindowFunctions;

/// Context provided to all query rewriting passes.
#[derive(Debug)]
pub struct RewriteContext<'a> {
    /// Map from names of views and tables in the database, to (ordered) lists of the column names
    /// in those views
    pub view_schemas: &'a HashMap<Relation, Vec<SqlIdentifier>>,

    /// Map from names of *tables* in the database, to the body of the `CREATE TABLE` statement
    /// that was used to create that table. Each key in this map should also exist in
    /// [`view_schemas`].
    pub base_schemas: HashMap<&'a Relation, &'a CreateTableBody>,

    /// List of views that are known to exist but have not yet been compiled (so we can't know
    /// their fields yet)
    pub uncompiled_views: &'a [&'a Relation],

    /// Set of relations that are known to exist in the upstream database, but are not being
    /// replicated. Used as part of schema resolution to ensure that queries that would resolve to
    /// these tables if they *were* being replicated correctly return an error
    pub non_replicated_relations: &'a HashSet<NonReplicatedRelation>,

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

    /// Optional list of aliases that were removed during a rewrite.
    ///
    /// This is (optionally) inserted into during rewriting of certain queries when the
    /// [alias_removal][] removes aliases to other relations in a query.
    ///
    /// [alias_removal]: crate::alias_removal
    pub table_alias_rewrites: Option<&'a mut Vec<TableAliasRewrite>>,

    /// The name of the cache for a migrated query.
    pub query_name: Option<&'a str>,
}

/// Can a particular relation (in the map passed to [`ResolveSchemas::resolve_schemas`]) be queried
/// from?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanQuery {
    Yes,
    No,
}

impl<'a> RewriteContext<'a> {
    pub(crate) fn tables(
        &self,
    ) -> HashMap<&'a SqlIdentifier, HashMap<&'a SqlIdentifier, CanQuery>> {
        self.view_schemas
            .keys()
            .chain(self.uncompiled_views.iter().copied())
            .map(|t| (t, CanQuery::Yes))
            .chain(
                self.non_replicated_relations
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
            )?
            .normalize_create_table_columns()
            .coalesce_key_definitions())
    }
}

impl Rewrite for SelectStatement {
    fn rewrite(self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        let query_name = context.query_name.unwrap_or("unknown");

        let mut s = self
            .rewrite_between()
            .disallow_row()?
            .validate_window_functions()?
            .scalar_optimize_expressions(context.dialect)
            .resolve_schemas(
                context.tables(),
                context.custom_types,
                context.search_path,
                context.invalidating_tables.as_deref_mut(),
            )?
            .expand_stars(context.view_schemas, context.non_replicated_relations)?;
        s.expand_implied_tables(context.view_schemas)?
            .rewrite_lateral_joins()?
            .normalize_topk_with_aggregate()?
            .detect_problematic_self_joins()?
            .remove_numeric_field_references()?
            .order_limit_removal(&context.base_schemas)?
            .rewrite_table_aliases(query_name, context.table_alias_rewrites.as_deref_mut())?;
        Ok(s)
    }
}

impl Rewrite for CompoundSelectStatement {
    fn rewrite(self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        Ok(CompoundSelectStatement {
            selects: self
                .selects
                .into_iter()
                .map(|(op, sq)| Ok((op, sq.rewrite(context)?)))
                .collect::<ReadySetResult<_>>()?,
            ..self
        })
    }
}

impl Rewrite for SelectSpecification {
    fn rewrite(self, context: &mut RewriteContext) -> ReadySetResult<Self> {
        Ok(match self {
            SelectSpecification::Compound(csq) => {
                SelectSpecification::Compound(csq.rewrite(context)?)
            }
            SelectSpecification::Simple(sq) => SelectSpecification::Simple(sq.rewrite(context)?),
        })
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
            definition: match self.definition {
                Ok(def) => Ok(Box::new(def.rewrite(context)?)),
                Err(unparsed) => Err(unparsed),
            },
            ..self
        })
    }
}
