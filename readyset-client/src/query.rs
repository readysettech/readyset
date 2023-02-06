//! Query
//!
//! ReadySet attempts to parse queries it is provided, which may succeed or fail.
//! ReadySet tracks state about each query related to whether it is supported or not, whether it is
//! in the process of being migrated, or whether the migration succeeded or failed
//! This module contains the types that handle that tracking.

use std::borrow::Borrow;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use readyset_sql_passes::anonymize::{Anonymize, Anonymizer};
use readyset_util::fmt::fmt_with;
use readyset_util::hash::hash;
use serde::ser::{SerializeSeq, SerializeTuple};
use serde::{Deserialize, Serialize, Serializer};

use crate::ViewCreateRequest;

/// A QueryId is a string with the prefix `q_` and the suffix of the hash of the query
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct QueryId(u64);

impl QueryId {
    /// Wrap a u64 in a [`QueryId`]
    pub fn new(id: u64) -> Self {
        QueryId(id)
    }

    /// Constructs a new [`QueryId`] from a hash of the given [`ViewCreateRequest`]
    pub fn from_view_create_request(q: &ViewCreateRequest) -> Self {
        QueryId::new(hash(q))
    }
}

impl Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "q_{:x}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
/// A Query that was made against readyset, which could have either been parsed successfully or
/// failed to parse.
pub enum Query {
    /// A Query that was successfully parsed by ReadySet
    Parsed(Arc<ViewCreateRequest>),
    /// A Query that ReadySet failed to parse, but Upstream was able to parse.
    ParseFailed(Arc<String>),
}

impl From<Arc<ViewCreateRequest>> for Query {
    fn from(vcr: Arc<ViewCreateRequest>) -> Self {
        Self::Parsed(vcr)
    }
}

impl From<ViewCreateRequest> for Query {
    fn from(stmt: ViewCreateRequest) -> Self {
        Self::Parsed(Arc::new(stmt))
    }
}

impl From<Arc<String>> for Query {
    fn from(s: Arc<String>) -> Self {
        Self::ParseFailed(s)
    }
}

impl From<String> for Query {
    fn from(s: String) -> Self {
        Self::ParseFailed(Arc::new(s))
    }
}

impl From<&str> for Query {
    fn from(s: &str) -> Self {
        Self::from(s.to_owned())
    }
}

impl Borrow<String> for Query {
    fn borrow(&self) -> &String {
        match self {
            Query::ParseFailed(str) => str,
            Query::Parsed(_) => {
                panic!("cannot borrow a query that was parsed as a string")
            }
        }
    }
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Parsed(l0), Self::Parsed(r0)) => l0 == r0,
            (Self::ParseFailed(l0), Self::ParseFailed(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Hash for Query {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Query::Parsed(stmt) => stmt.hash(state),
            Query::ParseFailed(str) => str.hash(state),
        }
    }
}

impl Query {
    /// Displays the query using appropriate formatting for the given dialect.
    pub fn display(&self, dialect: nom_sql::Dialect) -> impl Display + Copy + '_ {
        fmt_with(move |f| match self {
            Query::Parsed(q) => write!(f, "{}", q.statement.display(dialect)),
            Query::ParseFailed(s) => write!(f, "{s}"),
        })
    }

    /// Clones the inner query into a String and anonymizes the literals in it if it is a parsed
    /// SelectStatement. If the query failed to parse, the query is fully anonymized
    pub fn to_anonymized_string(&self, anonymizer: &mut Anonymizer) -> String {
        match self {
            Query::Parsed(q) => {
                let mut statement = q.statement.clone();
                statement.anonymize(anonymizer);
                // NOTE: Without `return`, there is a compile error that `statement` does not live
                // long enough.
                // FIXME: Use correct dialect.
                return statement.display(nom_sql::Dialect::MySQL).to_string();
            }
            Query::ParseFailed(_) => "<redacted: parsing failed>".to_string(),
        }
    }

    /// If this query was successfully parsed, returns the inner [`Arc<ViewCreateRequest>`],
    /// otherwise returns [`None`]
    pub fn into_parsed(self) -> Option<Arc<ViewCreateRequest>> {
        match self {
            Query::Parsed(vcr) => Some(vcr),
            Query::ParseFailed(_) => None,
        }
    }
}

/// A Query that should not be cached vy ReadySet
#[derive(Debug, Clone)]
pub struct DeniedQuery {
    /// The query id
    pub id: QueryId,
    /// The query
    pub query: Query,
    /// The query status
    pub status: QueryStatus,
}

impl Serialize for DeniedQuery {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_tuple(2)?;
        let mut anonymizer = Anonymizer::new();
        ser.serialize_element(&self.id)?;
        ser.serialize_element(self.query.to_anonymized_string(&mut anonymizer).as_str())?;
        // We intentionally do not serialize the QueryStatus
        ser.end()
    }
}

/// The status of the query, which we use to determine if the query should be cached or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStatus {
    /// The migration state of the query
    pub migration_state: MigrationState,
    /// The execution info of the query, if any
    pub execution_info: Option<ExecutionInfo>,
    /// If we should always cache the query (never proxy to upstream)
    pub always: bool,
}

impl QueryStatus {
    /// Constructs a QueryStatus with the default migration state for the query, no migration state,
    /// and always set to false
    pub fn default_for_query(query: &Query) -> Self {
        Self {
            migration_state: MigrationState::default_for_query(query),
            execution_info: None,
            always: false,
        }
    }

    /// Constructs a QueryStatus with the provided migration_state
    pub fn with_migration_state(migration_state: MigrationState) -> Self {
        Self {
            migration_state,
            execution_info: None,
            always: false,
        }
    }

    /// Returns true if this query status represents a [pending][] query
    ///
    /// [pending]: MigrationState::Pending
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.migration_state == MigrationState::Pending
    }

    /// Returns true if this query status represents a [successfully migrated][] query
    ///
    /// [successfully migrated]: MigrationState::Successful
    #[must_use]
    pub fn is_successful(&self) -> bool {
        self.migration_state == MigrationState::Successful
    }

    /// Returns true if this query status represents an [unsupported][] query
    ///
    /// [unsupported]: MigrationState::Unsupported
    #[must_use]
    pub fn is_unsupported(&self) -> bool {
        self.migration_state == MigrationState::Unsupported
    }

    /// Returns true if this query status represents a [successfully dry-run][] query
    ///
    /// [successfully dry-run]: MigrationState::DryRunSucceeded
    #[must_use]
    pub fn is_dry_run_succeeded(&self) -> bool {
        self.migration_state == MigrationState::DryRunSucceeded
    }

    /// Returns true if the query should be considered "denied"
    #[must_use]
    pub fn is_denied(&self) -> bool {
        self.is_unsupported() || self.is_pending() || self.is_dry_run_succeeded()
    }
}

/// Represents the current migration state of a given query. This state should be updated any time
/// a migration is performed, or we learn that the migration state has changed, i.e. we receive a
/// ViewNotFound error indicating a query is not migrated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    /// A migration has not been completed for this query. There may be one in progress depending
    /// on the adapters MigrationMode.
    Pending,
    /// This query has been migrated and a view exists.
    Successful,
    /// This query is not supported and should not be tried against ReadySet.
    Unsupported,
    /// Indicates that a dry run of the query has succeeded. It's very likely but not guaranteed
    /// that migration of the query will succeed if it's attempted.
    DryRunSucceeded,
}

impl MigrationState {
    /// Returns the default migration state for the provided query.
    ///
    /// Parsed queries have a default migration state of Pending
    /// ParseFailed queries have a default migration state of Unsupported
    pub fn default_for_query(query: &Query) -> Self {
        match query {
            Query::Parsed { .. } => Self::Pending,
            Query::ParseFailed(_) => Self::Unsupported,
        }
    }

    /// Returns true if the migration state of the query indicates that we are still processing it
    pub fn is_pending(&self) -> bool {
        matches!(
            self,
            MigrationState::Pending | MigrationState::DryRunSucceeded
        )
    }
}

impl Display for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationState::Pending => write!(f, "pending"),
            MigrationState::Successful => write!(f, "successful"),
            MigrationState::Unsupported => write!(f, "unsupported"),
            MigrationState::DryRunSucceeded => write!(f, "dry run succeeded"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// ExecutionInfo contains the current ExecutionState of the query along with the last time the
/// state was transitioned.
pub struct ExecutionInfo {
    /// The current execution state of the query
    pub state: ExecutionState,
    /// The last time the state was transitioned
    pub last_transition_time: Instant,
}

impl ExecutionInfo {
    /// Used to update the inner state type, if our current state is something different, and
    /// update the last transition time accordingly.
    fn update_inner(&mut self, state: ExecutionState) {
        self.last_transition_time = Instant::now();
        self.state = state;
    }

    /// Update ExecutionInfo to indicate that a recent execute succeeded.
    pub fn execute_succeeded(&mut self) {
        if matches!(self.state, ExecutionState::Successful) {
            return;
        }

        self.update_inner(ExecutionState::Successful)
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to a networking error.
    pub fn execute_network_failure(&mut self) {
        if matches!(self.state, ExecutionState::NetworkFailure) {
            return;
        }

        self.update_inner(ExecutionState::NetworkFailure)
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to some reason other than
    /// a networking error.
    pub fn execute_failed(&mut self) {
        if matches!(self.state, ExecutionState::Failed) {
            return;
        }

        self.update_inner(ExecutionState::Failed)
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to some reason other than
    /// a networking error.
    pub fn execute_unsupported(&mut self) {
        if matches!(self.state, ExecutionState::Unsupported) {
            return;
        }

        self.update_inner(ExecutionState::Unsupported)
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to the view being dropped
    pub fn execute_dropped(&mut self) {
        if matches!(self.state, ExecutionState::Dropped) {
            return;
        }

        self.update_inner(ExecutionState::Dropped)
    }

    /// Resets the internal transition time to now. This should be used with extreme caution.
    pub fn reset_transition_time(&mut self) {
        self.last_transition_time = Instant::now();
    }

    /// Resets the transition time for the query if we have exceeded the recovery window.
    /// Returns true if data was mutated and false if not.
    pub fn reset_if_exceeded_recovery(
        &mut self,
        query_max_failure_duration: Duration,
        fallback_recovery_duration: Duration,
    ) -> bool {
        if self.execute_network_failure_exceeded(
            query_max_failure_duration + fallback_recovery_duration,
        ) {
            // We've exceeded the window, so we'll reset the transition time. This should
            // ensure it gets tried again the next time. If it fails again due to a networking
            // error, it will get automatically set to the NetworkFailure state with an updated
            // transition time, which will eventually retrigger the fallback
            // recovery window.
            self.reset_transition_time();
            true
        } else {
            false
        }
    }

    /// If the current ExecutionState is ExecutionState::NetworkFailure, then this method will
    /// return true if that state has persisted for longer than the supplied duration, otherwise,
    /// it will return false.
    pub fn execute_network_failure_exceeded(&self, duration: Duration) -> bool {
        if let ExecutionState::NetworkFailure = self.state {
            return self.last_transition_time.elapsed() > duration;
        }

        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// The execution state of a query
pub enum ExecutionState {
    /// The query was executed successfully
    Successful,
    /// The query was executed unsuccessfully due to a network failure
    NetworkFailure,
    /// The query was executed unsuccessfully for no specified reason
    Failed,
    /// The query is unsupported by ReadySet
    Unsupported,
    /// The query was dropped
    Dropped,
}

#[derive(Debug, PartialEq, Eq)]
/// A collection of queries and their associated statuses
pub struct QueryList {
    queries: Vec<(Query, QueryStatus)>,
}

impl QueryList {
    /// The length of the QueryList
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Returns true if the QueryList is empty, otherwise returns false
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

impl From<Vec<(Query, QueryStatus)>> for QueryList {
    fn from(queries: Vec<(Query, QueryStatus)>) -> Self {
        QueryList { queries }
    }
}

impl IntoIterator for QueryList {
    type Item = (Query, QueryStatus);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.queries.into_iter()
    }
}

impl Serialize for QueryList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.queries.len()))?;

        let mut anonymizer = Anonymizer::new();

        for q in &self.queries {
            seq.serialize_element(&q.0.to_anonymized_string(&mut anonymizer))?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::Arbitrary;
    use readyset_util::hash_laws;

    use super::*;

    impl Arbitrary for Query {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Query>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            use proptest::arbitrary::any;
            use proptest::prelude::*;

            any::<String>().prop_map(Into::into).boxed()
        }
    }

    hash_laws!(Query);
}
