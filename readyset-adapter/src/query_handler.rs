use readyset_errors::ReadySetResult;
use readyset_sql::ast::{SetStatement, SqlIdentifier, SqlQuery};

use crate::backend::noria_connector;

/// Classification for how we should be handling a SQL `SET` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetBehavior {
    /// This `SET` statement is unsupported.
    Unsupported,
    /// This `SET` statement is meaningless to ReadySet, so should be proxied upstream verbatim.
    Proxy,
    /// This `SET` statement represents the `autocommit` flag being set either on or off.
    SetAutocommit(bool),
    /// This `SET` statement represents the current schema search path being changed
    SetSearchPath(Vec<SqlIdentifier>),
}

impl SetBehavior {
    /// Return a [`SetBehavior`] specifying that a statement should be proxied if the argument is
    /// `true`, or unsupported if the argument is `false`
    pub fn proxy_if(b: bool) -> Self {
        if b {
            Self::Proxy
        } else {
            Self::Unsupported
        }
    }
}

/// A trait describing the behavior of how specific queries should be handled by a noria-client
/// [`Backend`].
pub trait QueryHandler: Sized + Send {
    /// Whether or not a given query requires fallback.
    fn requires_fallback(query: &SqlQuery) -> bool;

    /// Provides a default response for the given query.
    /// This should only be used in cases where the query can't be executed by ReadySet
    /// and there is no fallback mechanism enabled or we deliberately want to return a default
    /// response based on the rules from [`return_default_response`].
    fn default_response(query: &SqlQuery) -> ReadySetResult<noria_connector::QueryResult<'static>>;

    /// Whether or not a given query should return a default response.
    fn return_default_response(query: &SqlQuery) -> bool;

    /// Classify the given SET statement based on how we should handle it
    ///
    /// See the documentation of [`SetStatement`] for more information.
    fn handle_set_statement(stmt: &SetStatement) -> SetBehavior;
}
