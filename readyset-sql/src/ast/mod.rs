mod alter;
mod column;
mod comment;
mod common;
mod compound_select;
mod create;
mod create_index;
mod create_table_options;
mod deallocate;
mod delete;
mod drop;
mod explain;
mod expression;
mod index_hint;
mod insert;
mod interval;
mod join;
mod literal;
mod order;
mod query;
mod rename;
pub mod rls;
mod select;
mod set;
mod show;
mod sql_identifier;
mod sql_type;
mod table;
mod transaction;
mod truncate;
mod update;
mod use_statement;

pub use alter::*;
pub use column::*;
pub use comment::*;
pub use common::*;
pub use compound_select::*;
pub use create::*;
pub use create_index::*;
pub use create_table_options::*;
pub use deallocate::*;
pub use delete::*;
pub use drop::*;
pub use explain::*;
pub use expression::*;
pub use index_hint::*;
pub use insert::*;
pub use interval::*;
pub use join::*;
pub use literal::*;
pub use order::*;
pub use query::*;
pub use rename::*;
pub use rls::*;
pub use select::*;
pub use set::*;
pub use show::*;
pub use sql_identifier::*;
pub use sql_type::*;
pub use table::*;
pub use transaction::*;
pub use truncate::*;
pub use update::*;
pub use use_statement::*;

use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};

use derive_more::Display;
use proptest::arbitrary::Arbitrary;
use proptest::prelude::Just;
use serde::{Deserialize, Serialize};

use crate::{Dialect, DialectDisplay};

#[derive(Clone, Display, Debug, Serialize, Deserialize, Eq)]
pub struct ShallowCacheQuery(sqlparser::ast::Query);

// Use sqlparser's Display implementation to convert AST to SQL For consistent normalization, we
// could apply additional formatting here (e.g. sql_insight::normalizer::normalize)
// For now, just use the Display implementation
impl Hash for ShallowCacheQuery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let query = format!("{self}");
        query.hash(state);
    }
}

impl PartialEq for ShallowCacheQuery {
    fn eq(&self, other: &Self) -> bool {
        format!("{self}") == format!("{other}")
    }
}

impl Deref for ShallowCacheQuery {
    type Target = sqlparser::ast::Query;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ShallowCacheQuery {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for ShallowCacheQuery {
    /// Build the AST of a minimal SELECT 1 query.
    fn default() -> Self {
        use sqlparser::ast::helpers::attached_token::AttachedToken;
        use sqlparser::ast::{
            Expr, GroupByExpr, Query, Select, SelectFlavor, SelectItem, SetExpr, Value,
        };

        // We can instead call the parser on SELECT 1, but that requires calling unwrap
        // on an external library, which we do not like.
        let select_item =
            SelectItem::UnnamedExpr(Expr::Value(Value::Number("1".into(), false).into()));
        let select = Select {
            distinct: None,
            top: None,
            projection: vec![select_item],
            into: None,
            from: vec![],
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
            named_window: vec![],
            connect_by: vec![],
            exclude: None,
            flavor: SelectFlavor::Standard,
            select_token: AttachedToken::empty(),
            top_before_distinct: false,
            prewhere: None,
            window_before_qualify: false,
            value_table_mode: None,
            select_modifiers: Default::default(),
            optimizer_hints: vec![],
        };

        Self(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select))),
            order_by: None,
            fetch: None,
            limit_clause: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        })
    }
}

impl Arbitrary for ShallowCacheQuery {
    type Parameters = ();
    type Strategy = Just<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Just(ShallowCacheQuery::default())
    }
}

impl DialectDisplay for ShallowCacheQuery {
    fn display(&self, _dialect: Dialect) -> impl std::fmt::Display + '_ {
        // sqlparser preserves dialect-specific formatting, so we don't need to do anything
        &self.0
    }
}

impl From<sqlparser::ast::Query> for ShallowCacheQuery {
    fn from(query: sqlparser::ast::Query) -> Self {
        ShallowCacheQuery(query)
    }
}

impl From<sqlparser::ast::Query> for Box<ShallowCacheQuery> {
    fn from(query: sqlparser::ast::Query) -> Self {
        Box::new(ShallowCacheQuery(query))
    }
}
