use nom_sql::SqlQuery;
use noria::ReadySetResult;

use crate::backend::noria_connector;

/// A trait describing the behaviour of how specific queries should be
/// handled by a noria-client [`Backend`].
pub trait QueryHandler: Sized + Send {
    /// Whether or not a given query requires fallback.
    fn requires_fallback(query: &SqlQuery) -> bool;

    /// Provides a default response for the given query.
    /// This should only be used in cases where the query can't be executed by Noria
    /// and there is no fallback mechanism enabled.
    fn default_response(query: &SqlQuery) -> ReadySetResult<noria_connector::QueryResult<'static>>;

    /// Is the given SET statement allowed?
    ///
    /// If this function returns true, the statement will be proxied to the upstream database, and
    /// hence cannot change the behavior of upstream queries in a way that would not also be
    /// reflected by ReadySet
    fn is_set_allowed(stmt: &nom_sql::SetStatement) -> bool;
}
