use readyset_errors::ReadySetResult;
use readyset_sql::ast::{SetStatement, SqlIdentifier, SqlQuery};

use crate::backend::noria_connector;

/// How we should be handling a SQL `SET` statement.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetBehavior {
    /// This `SET` statement is unsupported and should error.
    pub unsupported: bool,
    /// This `SET` statement should be proxied upstream verbatim.
    pub proxy: bool,
    /// This `SET` statement turns `autocommit` flag being set either on or off.
    pub set_autocommit: Option<bool>,
    /// This `SET` statement changes the current schema search path.
    pub set_search_path: Option<Vec<SqlIdentifier>>,
    /// This `SET` statement changes the encoding to be used for results. Corresponds to `SET
    /// @@character_set_results` in MySQL or `SET NAMES` in Postgres or MySQL.
    pub set_results_encoding: Option<readyset_data::encoding::Encoding>,
}

impl SetBehavior {
    pub fn unsupported(mut self, unsupported: bool) -> Self {
        self.unsupported = self.unsupported || unsupported;
        self
    }

    pub fn set_autocommit(mut self, autocommit: bool) -> Self {
        self.set_autocommit = Some(autocommit);
        self
    }

    pub fn set_search_path(mut self, search_path: Vec<SqlIdentifier>) -> Self {
        self.set_search_path = Some(search_path);
        self
    }

    pub fn set_results_encoding(
        mut self,
        encoding: Option<readyset_data::encoding::Encoding>,
    ) -> Self {
        if let Some(encoding) = encoding {
            self.set_results_encoding = Some(encoding);
        } else {
            self.unsupported = true;
        }
        self
    }
}

impl Default for SetBehavior {
    fn default() -> Self {
        Self {
            unsupported: false,
            proxy: true,
            set_autocommit: None,
            set_search_path: None,
            set_results_encoding: None,
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
