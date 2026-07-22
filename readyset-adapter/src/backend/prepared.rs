//! The extended-query protocol: turning a statement into a reusable handle, then running it.
//!
//! [`Backend::prepare`] classifies the statement, plans it against Readyset and the upstream, and
//! registers it in [`PreparedStatements`] under an id the client keeps. [`Backend::execute`] takes
//! that id back, re-checks the cache bypass, and dispatches. The simple-query path shares the
//! routing and session decisions but never enters here -- it has no handle to keep.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{self, OptionFuture};
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_client::query::{ExecutionInfo, ExecutionState, MigrationState, QueryId};
use readyset_client::{ShallowViewRequest, ViewCreateRequest};
use readyset_client_metrics::{
    EventType, QueryDestination, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent,
    SqlQueryType,
};
use readyset_data::DfValue;
use readyset_errors::ReadySetError::{self, PreparedStatementMissing};
use readyset_errors::{ReadySetResult, internal, internal_err, unsupported, unsupported_err};
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{
    CacheType, DiscardObject, DiscardStatement, ReadysetHintDirective, SelectStatement,
    SetStatement, SqlIdentifier, SqlQuery, TrxCachePolicy,
};
use readyset_sql_passes::adapter_rewrites::{self, ShallowQueryParameters};
use readyset_util::SizeOf;
use readyset_util::redacted::Sensitive;
use schema_catalog::RewriteContext;
use slab::Slab;
use tracing::{debug, error, warn};

use super::noria_connector::{self, ExecuteSelectContext, NoriaConnector, PreparedSelectTypes};
use super::routing::{ProxyState, SelectRouter, record_skip_cache};
use super::{
    Backend, MigrationMode, PrepareResult, PrepareResultInner, QueryInfo, QueryResult, StatementId,
    acl_decline_reason, convert_or_parse_query, log_query, no_upstream_err, parse_query,
    parse_shallow_query,
};
use crate::query_handler::UpstreamSetRewrite;
use crate::query_status_cache::ManualCacheEntry;
use crate::session_mutation::{self, SessionMutationTemplate};
use crate::upstream_database::UpstreamPrepare;
use crate::{QueryHandler, UpstreamDatabase};

/// Query metadata used to plan query prepare
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum PrepareMeta {
    /// Query was received in a state that should unconditionally proxy upstream
    Proxy,
    /// Query could not be parsed
    FailedToParse,
    /// Query could not be rewritten for processing in noria
    FailedToRewrite(ReadySetError),
    /// ReadySet does not implement this prepared statement. The statement may also be invalid SQL
    Unimplemented(ReadySetError),
    /// A write query (Insert, Update, Delete)
    Write { stmt: SqlQuery },
    /// A read (Select; may be extended in the future)
    Select(PrepareSelectMeta),
    /// A shallow read.
    ShallowSelect(PrepareShallowSelectMeta),
    /// A transaction boundary (Start, Commit, Rollback)
    Transaction { stmt: SqlQuery },
    /// A set command
    Set { stmt: SetStatement },
    /// A `DISCARD` / `RESET ALL` full-session reset
    Discard { stmt: DiscardStatement },
}

#[derive(Debug)]
struct PrepareSelectMeta {
    stmt: SelectStatement,
    view_request: ViewCreateRequest,
    query_id: Option<QueryId>,
    migration_state: MigrationState,
    must_migrate: bool,
    should_do_noria: bool,
    trx_cache_policy: TrxCachePolicy,
    /// Set when the query's standard shape maps to a manually parameterized cache
    /// (`CREATE CACHE WITH (AUTOPARAM ...)`): the SELECT is served by that cache instead.
    manual_cache: Option<ManualCacheEntry>,
}

#[derive(Debug)]
struct PrepareShallowSelectMeta {
    query_id: QueryId,
    stmt: ShallowViewRequest,
    params: ShallowQueryParameters,
    trx_cache_policy: TrxCachePolicy,
}

/// A [`PreparedStatement`] stores the data needed for an immediate execution of a prepared
/// statement on either noria or the upstream connection.
pub(super) struct PreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Indicates if the statement was prepared for ReadySet, Fallback, Shallow, or multiple
    prep: PrepareResult<DB>,
    /// The current ReadySet migration state
    migration_state: MigrationState,
    /// The transaction cache policy for the cached query, captured at prepare time. See
    /// [`TrxCachePolicy`].
    /// This is imperfect, but leans on performance over correctness. It requires a user to
    /// re-prepare queries if they decide to change the policy.
    trx_cache_policy: TrxCachePolicy,
    /// Holds information about if executes have been succeeding, or failing, along with a state
    /// transition timestamp. None if prepared statement has never been executed.
    execution_info: Option<ExecutionInfo>,
    /// If query was successfully parsed, will store the parsed query
    parsed_query: Option<Arc<SqlQuery>>,
    /// If was able to hash the query, will store the generated hash
    query_id: Option<QueryId>,
    /// If statement was successfully rewritten, will store all information necessary to install
    /// the view in readyset
    view_request: Option<ViewCreateRequest>,
    /// Query used for shallow caching.
    shallow: Option<ShallowViewRequest>,
    /// Query parameters from rewrite_shallow, used for shallow cache key generation
    params: Option<ShallowQueryParameters>,
    /// Whether the original query contained a SKIP CACHE hint directive.
    /// Stored at prepare time so the execute path can emit the skip-cache
    /// metric with `reason => "hint"`.
    is_skip_cache: bool,
    /// Why planning declined a cache Readyset holds for this statement (the
    /// serve seams stage it while the prepare runs). Executes of the
    /// resulting upstream-only plan surface it in EXPLAIN LAST STATEMENT.
    prepare_proxy_reason: Option<&'static str>,
}

impl<DB> PreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Returns whether we are currently in fallback recovery mode for the given prepared statement
    /// we are attempting to execute.
    /// WARNING: This will also mutate execution info timestamp if we have exceeded the supplied
    /// recovery period.
    pub(crate) fn in_fallback_recovery(
        &mut self,
        query_max_failure_duration: Duration,
        fallback_recovery_duration: Duration,
    ) -> bool {
        if let Some(info) = self.execution_info.as_mut() {
            info.reset_if_exceeded_recovery(query_max_failure_duration, fallback_recovery_duration);
            info.execute_network_failure_exceeded(query_max_failure_duration)
        } else {
            false
        }
    }

    pub(crate) fn is_unsupported_execute(&self) -> bool {
        if let Some(info) = self.execution_info.as_ref() {
            matches!(info.state, ExecutionState::Unsupported)
        } else {
            false
        }
    }

    /// Get a reference to the `ViewRequest` or return an error
    fn as_view_request(&self) -> ReadySetResult<&ViewCreateRequest> {
        self.view_request
            .as_ref()
            .ok_or_else(|| internal_err!("Expected ViewRequest for CachedPreparedStatement"))
    }

    fn as_shallow(&self) -> ReadySetResult<&ShallowViewRequest> {
        self.shallow
            .as_ref()
            .ok_or_else(|| internal_err!("Missing shallow query"))
    }
}

/// The prepared statements of one connection, keyed by statement id.
///
/// All three fields are keyed by that id and [`Self::remove`] clears all three, so they live
/// behind one type and only it touches them.
pub(super) struct PreparedStatements<DB>
where
    DB: UpstreamDatabase,
{
    /// The slab position is the statement id.
    statements: Slab<PreparedStatement<DB>>,
    /// Memoized unnamed statements: `query text -> (search_path, statement_id, reusable)`.
    ///
    /// The text alone does not identify a plan, since an unqualified name resolves against the
    /// session `search_path`. `reusable` is false for a plan that resolved upstream-only, which
    /// a transient cache bypass may have forced, so it is re-planned on the next Parse.
    unnamed: HashMap<String, (Vec<SqlIdentifier>, StatementId, bool)>,
    /// Session mutations recognised at prepare time, for the execute-time applier. Held here
    /// because the shallow and proxy paths null out [`PreparedStatement::parsed_query`], so the
    /// AST is gone by execute time.
    session_mutations: HashMap<StatementId, SessionMutationTemplate>,
}

impl<DB> Default for PreparedStatements<DB>
where
    DB: UpstreamDatabase,
{
    fn default() -> Self {
        Self {
            statements: Slab::new(),
            unnamed: HashMap::new(),
            session_mutations: HashMap::new(),
        }
    }
}

impl<DB> PreparedStatements<DB>
where
    DB: UpstreamDatabase,
{
    /// The id the next [`Self::insert`] will use. Callers need it to build the statement, which
    /// embeds its own id.
    pub(super) fn vacant_id(&self) -> StatementId {
        self.statements
            .vacant_key()
            .try_into()
            .expect("Cannot prepare more than u32::MAX statements with a single connection")
    }

    pub(super) fn insert(
        &mut self,
        statement: PreparedStatement<DB>,
        session_mutation: Option<SessionMutationTemplate>,
    ) -> StatementId {
        let id = self.statements.insert(statement) as StatementId;
        if let Some(template) = session_mutation {
            self.session_mutations.insert(id, template);
        }
        id
    }

    pub(super) fn get(&self, id: StatementId) -> Option<&PreparedStatement<DB>> {
        self.statements.get(id as usize)
    }

    pub(super) fn get_mut(&mut self, id: StatementId) -> Option<&mut PreparedStatement<DB>> {
        self.statements.get_mut(id as usize)
    }

    /// Forgets `id` entirely: the slab slot and any session-mutation template keyed by it.
    pub(super) fn remove(&mut self, id: StatementId) -> Option<PreparedStatement<DB>> {
        self.session_mutations.remove(&id);
        self.statements.try_remove(id as usize)
    }

    pub(super) fn clear(&mut self) {
        self.statements.clear();
        self.unnamed.clear();
        self.session_mutations.clear();
    }

    pub(super) fn session_mutation(&self, id: StatementId) -> Option<&SessionMutationTemplate> {
        self.session_mutations.get(&id)
    }

    /// The id memoized for an unnamed `query` resolved against `search_path`, if that plan is
    /// reusable across transaction-state changes.
    pub(super) fn reuse_unnamed(
        &self,
        query: &str,
        search_path: &[SqlIdentifier],
    ) -> Option<StatementId> {
        self.unnamed
            .get(query)
            .filter(|(path, _, reusable)| *reusable && path.as_slice() == search_path)
            .map(|(_, id, _)| *id)
    }

    /// Memoizes `id` as the unnamed slot for `query`, dropping whatever it supersedes. The
    /// unnamed slot holds one statement per query text, so a changed search path or a
    /// re-planned non-reusable slot leaves a slab slot to reclaim.
    pub(super) fn record_unnamed(
        &mut self,
        query: &str,
        search_path: Vec<SqlIdentifier>,
        id: StatementId,
        reusable: bool,
    ) {
        if let Some((_, superseded, _)) = self
            .unnamed
            .insert(query.to_string(), (search_path, id, reusable))
        {
            self.remove(superseded);
        }
    }

    /// Forces every statement upstream, marking those backed by a cache of `cache_type` (or any
    /// type, when `None`) as pending. Used when caches are dropped wholesale.
    pub(super) fn invalidate_all(&mut self, cache_type: Option<CacheType>) {
        self.statements.iter_mut().for_each(
            |(
                _,
                PreparedStatement {
                    prep,
                    migration_state,
                    ..
                },
            )| {
                if matches!(*migration_state,
                    MigrationState::Successful(t) if cache_type == Some(t) || cache_type.is_none())
                {
                    *migration_state = MigrationState::Pending;
                }
                prep.make_upstream_only();
            },
        );
    }

    /// Marks every statement backed by `stmt` as pending and forces it upstream, so a dropped
    /// or re-migrated cache is not served from a stale plan.
    pub(super) fn invalidate(&mut self, stmt: &ViewCreateRequest) {
        // Linear scan, but we shouldn't be doing it often, right?
        self.statements
            .iter_mut()
            .filter_map(
                |(
                    _,
                    PreparedStatement {
                        prep,
                        migration_state,
                        view_request,
                        ..
                    },
                )| {
                    if matches!(*migration_state, MigrationState::Successful(_))
                        && view_request.as_ref() == Some(stmt)
                    {
                        *migration_state = MigrationState::Pending;
                        Some(prep)
                    } else {
                        None
                    }
                },
            )
            .for_each(|ps| ps.make_upstream_only());
    }
}

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    /// Prepares query against ReadySet. If an upstream database exists, the prepare is mirrored to
    /// the upstream database.
    ///
    /// This function may perform a migration and update a query's migration state, if
    /// InRequestPath mode is enabled or of not upstream is set
    async fn mirror_prepare(
        &mut self,
        select_meta: &PrepareSelectMeta,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        let rewrite_context =
            Self::rewrite_context(&self.connectors, &self.settings, &self.state, None).await?;
        let up_prep: OptionFuture<_> = self
            .connectors
            .upstream
            .as_mut()
            .map(|u| u.prepare(query, data, statement_type))
            .into();
        let noria_prep: OptionFuture<_> = select_meta
            .should_do_noria
            .then_some(self.connectors.noria.prepare_select(
                select_meta.stmt.clone(),
                select_meta.must_migrate,
                &rewrite_context,
                select_meta.manual_cache.as_ref(),
            ))
            .into();

        let (upstream_res, noria_res) = future::join(up_prep, noria_prep).await;

        let destination = match (upstream_res.is_some(), noria_res.is_some()) {
            (true, true) => Some(QueryDestination::Both),
            (false, true) => Some(QueryDestination::Readyset(None)),
            (true, false) => Some(QueryDestination::Upstream),
            (false, false) => None,
        };

        let reason = self.state.take_proxy_reason();
        self.state.last_query = destination.map(|d| QueryInfo {
            destination: d,
            reason,
        });

        // Update noria migration state for query
        match &noria_res {
            // Don't promote the standard (fully autoparameterized) shape when this prepare was
            // served by a manual cache (`AUTOPARAM`): that shape has no deep cache of its own, so
            // a stale `Successful` would make later queries attempt a non-existent view once the
            // manual cache is dropped.
            Some(Ok(noria_connector::PrepareResult::Select { .. }))
                if select_meta.manual_cache.is_none() =>
            {
                self.state.query_status_cache.update_query_migration_state(
                    &select_meta.view_request,
                    MigrationState::Successful(CacheType::Deep),
                    None,
                );
            }
            // Manual-cache prepare: routed by the standard shape's mapping, leave its status alone.
            Some(Ok(noria_connector::PrepareResult::Select { .. })) => {}
            Some(Err(e)) => {
                if e.caused_by_view_not_found() {
                    debug!(error = %e, "View not found during mirror_prepare()");
                    self.state
                        .query_status_cache
                        .view_not_found_for_query(&select_meta.view_request);
                } else if e.caused_by_unsupported() {
                    self.state.query_status_cache.update_query_migration_state(
                        &select_meta.view_request,
                        MigrationState::Unsupported(e.unsupported_cause().unwrap_or_default()),
                        None,
                    );
                } else {
                    error!(
                        error = %e,
                        "Error received from noria during mirror_prepare()"
                    );
                }
                event.set_noria_error(e);
            }
            None => {}
            _ => internal!("Can only return SELECT result or error"),
        }

        let prep_result = match (upstream_res, noria_res) {
            (Some(upstream_res), Some(Ok(noria_res))) => {
                PrepareResultInner::NoriaAndUpstream(noria_res, upstream_res?)
            }
            (None, Some(Ok(noria_res))) => {
                if matches!(
                    noria_res,
                    noria_connector::PrepareResult::Select {
                        types: PreparedSelectTypes::NoSchema,
                        ..
                    }
                ) {
                    // We fail when attempting to borrow a cache without an upstream here in case
                    // the connection to the upstream is temporarily down.
                    internal!(
                        "Cannot create PrepareResult for borrowed cache without an upstream result"
                    );
                }
                PrepareResultInner::Noria(noria_res)
            }
            (None, Some(Err(noria_err))) => return Err(noria_err.into()),
            (Some(upstream_res), _) => PrepareResultInner::Upstream(upstream_res?),
            (None, None) => return Err(ReadySetError::Unsupported(query.to_string()).into()),
        };

        Ok(prep_result)
    }

    /// Prepares Insert, Delete, and Update statements
    async fn prepare_write(
        &mut self,
        query: &str,
        stmt: &SqlQuery,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        event.sql_type = SqlQueryType::Write;
        if let Some(ref mut upstream) = self.connectors.upstream {
            let _t = event.start_upstream_timer();
            let res = upstream
                .prepare(query, data, statement_type)
                .await
                .map(PrepareResultInner::Upstream);
            self.state.last_query = Some(QueryInfo {
                destination: QueryDestination::Upstream,
                reason: self.state.take_proxy_reason(),
            });
            res
        } else {
            let start = Instant::now();
            let res = match stmt {
                SqlQuery::Insert(stmt) => {
                    self.connectors.noria.prepare_insert(stmt.clone()).await?
                }
                SqlQuery::Delete(stmt) => {
                    self.connectors.noria.prepare_delete(stmt.clone()).await?
                }
                SqlQuery::Update(stmt) => {
                    self.connectors.noria.prepare_update(stmt.clone()).await?
                }
                // prepare_write does not support other statements
                _ => internal!(),
            };
            self.state.last_query = Some(QueryInfo {
                destination: QueryDestination::Readyset(None),
                reason: self.state.take_proxy_reason(),
            });

            event.readyset_event = Some(ReadysetExecutionEvent::Other {
                duration: start.elapsed(),
            });

            Ok(PrepareResultInner::Noria(res))
        }
    }

    /// Ensure we are allowed to handle the SET statement.
    async fn prepare_set(
        &mut self,
        stmt: &SetStatement,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        // if `handle_set()` returns an error, we aren't supposed to process
        // the SET anyway, so propagating the error is expected.
        // Then we need to determine if we're actually going to proxy to the upstream.
        let (upstream_set_rewrite, pending_set_state) = Self::handle_set(
            &mut self.connectors,
            &self.settings,
            &mut self.state,
            query,
            stmt,
            event,
        )?;
        // A SET must always reach the upstream connection to take effect there,
        // matching the simple-protocol path which proxies SET regardless of
        // proxy state. `should_proxy()` governs ordinary reads, not session-
        // mutating utility statements: gating on it dropped SET in autocommit,
        // leaving the session mirror ahead of an upstream that never saw the
        // statement. Fall back to a Noria no-op only with no upstream to carry
        // the side effect, or when Readyset handles the statement entirely.
        let res = match (self.connectors.upstream.as_mut(), upstream_set_rewrite) {
            (Some(upstream), UpstreamSetRewrite::ProxyVerbatim) => {
                let prep = upstream.prepare(query, data, statement_type).await?;
                PrepareResultInner::Upstream(prep)
            }
            (Some(upstream), UpstreamSetRewrite::Rewrite(rewritten)) => {
                let prep = upstream.prepare(rewritten, data, statement_type).await?;
                PrepareResultInner::Upstream(prep)
            }
            (None, _) | (Some(_), UpstreamSetRewrite::Skip) => {
                PrepareResultInner::Noria(noria_connector::PrepareResult::Set {
                    statement: stmt.clone(),
                })
            }
        };
        // Mirror session state only if the upstream accepted the statement.
        pending_set_state.apply(&mut self.connectors.noria);

        Ok(res)
    }

    /// Prepare a `DISCARD` / `RESET ALL`: mirror the full-session reset into the
    /// `SessionContext` (matching the simple-protocol pre-dispatch path) and
    /// proxy the statement upstream so it takes effect on that connection. The
    /// extended path otherwise routed `DISCARD` through `prepare_fallback`, which
    /// proxies upstream but never resets the mirror, leaving stale role/GUC
    /// state behind a reset session.
    async fn prepare_discard(
        &mut self,
        stmt: &DiscardStatement,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        if stmt.object_type == DiscardObject::All
            && let Some(session) = self.connectors.session.as_ref()
        {
            session.discard_all();
        }

        let Some(upstream) = self.connectors.upstream.as_mut() else {
            return Err(unsupported_err!("DISCARD not supported without an upstream").into());
        };
        let prep = upstream.prepare(query, data, statement_type).await?;
        Ok(PrepareResultInner::Upstream(prep))
    }

    /// Provides metadata required to prepare a select query
    async fn plan_prepare_select(
        &mut self,
        stmt: SelectStatement,
        is_skip_cache: bool,
    ) -> ReadySetResult<PrepareMeta> {
        let rewrite_context =
            Self::rewrite_context(&self.connectors, &self.settings, &self.state, None).await?;
        let mut rewritten = stmt.clone();
        if let Err(e) = adapter_rewrites::rewrite_query(
            &mut rewritten,
            self.connectors.noria.rewrite_params(),
            &rewrite_context,
        ) {
            warn!(
                statement = %Sensitive(&stmt.display(self.settings.dialect)),
                "This statement could not be rewritten for Readyset"
            );
            return Ok(PrepareMeta::FailedToRewrite(e));
        };

        if is_skip_cache {
            return Ok(PrepareMeta::Proxy);
        }

        let view_request = ViewCreateRequest::new(
            rewritten,
            self.connectors.noria.schema_search_path().to_owned(),
        );
        let status = self
            .state
            .query_status_cache
            .query_status(&view_request, rewrite_context.schema_generation());
        if self.state.proxy_state.is_proxy_always()
            && !matches!(status.trx_cache_policy, TrxCachePolicy::Always)
        {
            Ok(PrepareMeta::Proxy)
        } else {
            let query_id = QueryId::from(&view_request);
            // A manually parameterized cache (`AUTOPARAM`) claiming this query's standard shape
            // serves the SELECT, regardless of the standard shape's own migration state.
            let manual_cache = self.state.query_status_cache.manual_cache(&query_id);
            let migration_state = if manual_cache.is_some() {
                MigrationState::Successful(CacheType::Deep)
            } else {
                status.migration_state
            };
            let should_do_readyset = !matches!(migration_state, MigrationState::Unsupported(_));
            Ok(PrepareMeta::Select(PrepareSelectMeta {
                stmt,
                view_request,
                query_id: Some(query_id),
                migration_state,
                // For select statements only InRequestPath should trigger migrations
                // synchronously, or if no upstream is present.
                must_migrate: self.settings.migration_mode == MigrationMode::InRequestPath
                    || !self.connectors.has_fallback(),
                should_do_noria: should_do_readyset,
                trx_cache_policy: status.trx_cache_policy,
                manual_cache,
            }))
        }
    }

    /// Provides metadata required to prepare a query.
    ///
    /// Returns `(PrepareMeta, is_skip_cache)` where `is_skip_cache` is true
    /// when the query contained a `/*rs+ SKIP CACHE */` hint directive.
    async fn plan_prepare(
        &mut self,
        query: &str,
        query_shallow: &mut Option<ShallowViewRequest>,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<(PrepareMeta, bool)> {
        let (shallow_parsed, hint) = {
            let _t = event.start_parse_timer();
            parse_shallow_query(&self.settings, query)
        };

        let is_skip_cache = matches!(&hint, Some(ReadysetHintDirective::SkipCache));

        // Keep a copy of the sqlparser AST before the shallow rewrite mutates it, so the
        // fall-through below can derive the Readyset AST without a second text parse.
        let deep_ast = match &shallow_parsed {
            Ok(q) if self.settings.retain_shallow_ast() => Some((**q).clone()),
            _ => None,
        };

        if let Some((shallow, params)) = self.connectors.prepare_shallow_query(shallow_parsed) {
            *query_shallow = Some(shallow.clone());
            if let Some((query_id, trx_cache_policy)) = Self::should_query_shallow(
                &mut self.connectors,
                &self.settings,
                &mut self.state,
                &shallow,
                query,
                hint,
            )
            .await
            {
                return Ok((
                    PrepareMeta::ShallowSelect(PrepareShallowSelectMeta {
                        query_id,
                        stmt: shallow,
                        params,
                        trx_cache_policy,
                    }),
                    is_skip_cache,
                ));
            }
        }

        // The full parse or AST conversion runs only when the shallow path declines.
        let parsed = match self.state.parsed_query_cache.get(query) {
            Some(cached_query) => Ok(cached_query.clone()),
            None => {
                let parsed = {
                    let _t = event.start_parse_timer();
                    convert_or_parse_query(&self.settings, deep_ast, query)
                };
                if let Ok(parsed) = &parsed {
                    self.state
                        .parsed_query_cache
                        .put(query.to_string(), parsed.clone());
                }
                parsed
            }
        };

        let meta = match parsed {
            Ok(SqlQuery::Select(stmt)) if self.settings.cache_mode.is_shallow() => {
                let view_request = ViewCreateRequest::new(
                    stmt.clone(),
                    self.connectors.noria.schema_search_path().to_owned(),
                );
                PrepareMeta::Select(PrepareSelectMeta {
                    stmt,
                    view_request,
                    query_id: query_shallow.as_ref().map(QueryId::from),
                    migration_state: MigrationState::Unsupported("shallow-only mode".into()),
                    must_migrate: false,
                    should_do_noria: false,
                    trx_cache_policy: TrxCachePolicy::Never,
                    manual_cache: None,
                })
            }
            Ok(SqlQuery::Select(stmt)) => self.plan_prepare_select(stmt, is_skip_cache).await?,
            Ok(
                query @ SqlQuery::Insert(_)
                | query @ SqlQuery::Update(_)
                | query @ SqlQuery::Delete(_),
            ) => PrepareMeta::Write { stmt: query },
            Ok(
                query @ SqlQuery::StartTransaction(_)
                | query @ SqlQuery::Commit(_)
                | query @ SqlQuery::Rollback(_),
            ) => PrepareMeta::Transaction { stmt: query },
            Ok(SqlQuery::Set(s)) => PrepareMeta::Set { stmt: s },
            Ok(SqlQuery::Discard(d)) => PrepareMeta::Discard { stmt: d },
            Ok(pq) => {
                debug!(
                    statement = %pq.display(self.settings.dialect),
                    "Statement cannot be prepared by Readyset"
                );
                PrepareMeta::Unimplemented(unsupported_err!(
                    "{} not supported without an upstream",
                    pq.query_type()
                ))
            }
            Err(_) => {
                let mode = if self.state.proxy_state == ProxyState::Never {
                    PrepareMeta::FailedToParse
                } else {
                    PrepareMeta::Proxy
                };
                debug!(query = %Sensitive(&query), plan = ?mode, "Readyset failed to parse query");
                mode
            }
        };
        Ok((meta, is_skip_cache))
    }

    /// Prepares a query on noria and upstream based on the provided PrepareMeta
    async fn do_prepare(
        &mut self,
        meta: &PrepareMeta,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        match meta {
            PrepareMeta::Select(select_meta) => {
                self.mirror_prepare(select_meta, query, data, statement_type, event)
                    .await
            }
            PrepareMeta::ShallowSelect(..) => {
                let Some(upstream) = self.connectors.upstream.as_mut() else {
                    internal!("Shallow cache needs upstream");
                };
                let _t = event.start_upstream_timer();
                upstream
                    .prepare(query, data, statement_type)
                    .await
                    .map(PrepareResultInner::Shallow)
            }
            PrepareMeta::Write { stmt } => {
                self.prepare_write(query, stmt, data, statement_type, event)
                    .await
            }
            PrepareMeta::Set { stmt } => {
                self.prepare_set(stmt, query, data, statement_type, event)
                    .await
            }
            PrepareMeta::Discard { stmt } => {
                self.prepare_discard(stmt, query, data, statement_type)
                    .await
            }
            PrepareMeta::Proxy
            | PrepareMeta::FailedToParse
            | PrepareMeta::FailedToRewrite(_)
            | PrepareMeta::Unimplemented(_)
            | PrepareMeta::Transaction { .. }
                if self.connectors.upstream.is_some() =>
            {
                let _t = event.start_upstream_timer();
                let res = self
                    .prepare_fallback(query, data, statement_type)
                    .await
                    .map(PrepareResultInner::Upstream);

                self.state.last_query = Some(QueryInfo {
                    destination: QueryDestination::Upstream,
                    reason: self.state.take_proxy_reason(),
                });

                res
            }
            PrepareMeta::Proxy => {
                unsupported!("No upstream, so query cannot be proxied")
            }
            PrepareMeta::Transaction { .. } => {
                unsupported!("No upstream, transactions not supported")
            }
            PrepareMeta::FailedToParse => unsupported!("Query failed to parse"),
            PrepareMeta::FailedToRewrite(e) | PrepareMeta::Unimplemented(e) => {
                Err(e.clone().into())
            }
        }
    }

    #[inline]
    fn create_prepared_statement(
        &mut self,
        prepare_meta: PrepareMeta,
        prep: PrepareResultInner<DB>,
        statement_id: StatementId,
        is_skip_cache: bool,
        prepare_proxy_reason: Option<&'static str>,
    ) -> PreparedStatement<DB> {
        match prepare_meta {
            PrepareMeta::Write { stmt } | PrepareMeta::Transaction { stmt } => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful(CacheType::Deep),
                execution_info: None,
                parsed_query: Some(Arc::new(stmt)),
                view_request: None,
                shallow: None,
                trx_cache_policy: TrxCachePolicy::Never,
                params: None,
                is_skip_cache,
                prepare_proxy_reason,
            },
            PrepareMeta::Set { stmt } => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful(CacheType::Deep),
                execution_info: None,
                parsed_query: Some(Arc::new(SqlQuery::Set(stmt))),
                view_request: None,
                shallow: None,
                trx_cache_policy: TrxCachePolicy::Never,
                params: None,
                is_skip_cache,
                prepare_proxy_reason,
            },
            PrepareMeta::Discard { stmt } => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful(CacheType::Deep),
                execution_info: None,
                parsed_query: Some(Arc::new(SqlQuery::Discard(stmt))),
                view_request: None,
                shallow: None,
                trx_cache_policy: TrxCachePolicy::Never,
                params: None,
                is_skip_cache,
                prepare_proxy_reason,
            },
            PrepareMeta::Select(PrepareSelectMeta {
                stmt,
                view_request,
                query_id,
                migration_state,
                trx_cache_policy,
                ..
            }) => PreparedStatement {
                query_id,
                prep: PrepareResult::new(statement_id, prep),
                migration_state,
                execution_info: None,
                parsed_query: Some(Arc::new(SqlQuery::Select(stmt))),
                view_request: Some(view_request),
                shallow: None,
                trx_cache_policy,
                params: None,
                is_skip_cache,
                prepare_proxy_reason,
            },
            PrepareMeta::ShallowSelect(PrepareShallowSelectMeta {
                query_id,
                stmt,
                params,
                trx_cache_policy,
            }) => PreparedStatement {
                query_id: Some(query_id),
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful(CacheType::Shallow),
                execution_info: None,
                parsed_query: None,
                view_request: None,
                shallow: Some(stmt),
                trx_cache_policy,
                params: Some(params),
                is_skip_cache,
                prepare_proxy_reason,
            },
            PrepareMeta::Proxy
            | PrepareMeta::FailedToParse
            | PrepareMeta::FailedToRewrite(..)
            | PrepareMeta::Unimplemented(..) => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful(CacheType::Deep),
                execution_info: None,
                parsed_query: None,
                view_request: None,
                shallow: None,
                trx_cache_policy: TrxCachePolicy::Never,
                params: None,
                is_skip_cache,
                prepare_proxy_reason,
            },
        }
    }

    async fn prepare_inner(
        &mut self,
        query: &str,
        query_shallow: &mut Option<ShallowViewRequest>,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<StatementId, DB::Error> {
        if matches!(statement_type, PreparedStatementType::Unnamed)
            && let Some(id) = self
                .state
                .prepared
                .reuse_unnamed(query, self.connectors.noria.schema_search_path())
        {
            return Ok(id);
        }

        let (meta, is_skip_cache) = self.plan_prepare(query, query_shallow, event).await?;
        // Capture the serve seams' staged decline before do_prepare's own
        // QueryInfo consumes it, so executes of the resulting plan can still
        // surface it.
        let prepare_proxy_reason = self.state.pending_proxy_reason;
        let prep = self
            .do_prepare(&meta, query, data, statement_type, event)
            .await?;
        // An upstream-only plan may have been forced there by a transient cache bypass; keep it out
        // of the reuse fast path so a later Parse re-plans once the bypass clears. Cache-backed
        // plans re-evaluate the bypass at execute time, so they stay reusable in any state.
        // Permanently-upstream shapes (unsupported/proxied) re-plan every Parse too, matching
        // Postgres.
        let reusable = !matches!(prep, PrepareResultInner::Upstream(_));

        let next_id = self.state.prepared.vacant_id();
        // Recognise session-mutating shapes (PostgREST's set_config
        // batch today; SET LOCAL / SET ROLE via Parse/Bind in future
        // variants) at prepare time, while the parsed AST is still
        // available -- it gets nulled out on the shallow-cache and
        // proxy paths inside `create_prepared_statement`.
        let session_mutation = parse_query(&self.settings, query)
            .ok()
            .as_ref()
            .and_then(session_mutation::recognize);
        let prepared_statement = self.create_prepared_statement(
            meta,
            prep,
            next_id,
            is_skip_cache,
            prepare_proxy_reason,
        );
        let statement_id = self
            .state
            .prepared
            .insert(prepared_statement, session_mutation);

        if matches!(statement_type, PreparedStatementType::Unnamed) {
            let path = self.connectors.noria.schema_search_path().to_vec();
            self.state
                .prepared
                .record_unnamed(query, path, statement_id, reusable);
        }

        Ok(statement_id)
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    pub async fn prepare(
        &mut self,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
    ) -> Result<&PrepareResult<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        let mut query_shallow = None;
        let mut event = QueryExecutionEvent::new(EventType::Prepare);
        let result = self
            .prepare_inner(query, &mut query_shallow, data, statement_type, &mut event)
            .await;

        Self::update_shallow_support(&self.state, &query_shallow, result.as_ref().err());

        let statement_id = result?;
        let prepared_statement = self
            .state
            .prepared
            .get(statement_id)
            .expect("prepare_inner returns a live statement id");

        if let Some(QueryLogMode::Verbose) = self.state.query_log_mode {
            // We only use the full query in verbose mode, so avoid cloning if we don't need to
            if let Some(parsed) = &prepared_statement.parsed_query {
                event.query = Some(parsed.clone());
            }
        }

        event.query_id = prepared_statement.query_id;
        log_query(
            self.state.query_log_sender.as_ref(),
            event,
            self.settings.slowlog,
            self.settings.dialect,
        );

        Ok(&prepared_statement.prep)
    }

    /// Executes a prepared statement on ReadySet
    async fn execute_noria<'a>(
        noria: &'a mut NoriaConnector,
        prep: &noria_connector::PrepareResult,
        params: &[DfValue],
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<QueryResult<'a, DB>> {
        use noria_connector::PrepareResult::*;

        event.destination = Some(QueryDestination::Readyset(None));

        let res = match prep {
            Select { statement, .. } => {
                let ctx = ExecuteSelectContext::Prepared {
                    ps: statement,
                    params,
                };
                noria.execute_select(ctx, event).await
            }
            Insert { statement, .. } => noria.execute_prepared_insert(statement, params).await,
            Update { statement, .. } => noria.execute_prepared_update(statement, params).await,
            Delete { statement, .. } => noria.execute_prepared_delete(statement, params).await,
            // we do not (yet) handle SET commands internal to readyset.
            Set { .. } => Ok(noria_connector::QueryResult::Empty),
        }
        .map(Into::into);

        if let Err(e) = &res {
            event.set_noria_error(e);
        }

        res
    }

    /// Execute on ReadySet, and if fails execute on upstream
    #[allow(clippy::too_many_arguments)] // meh.
    async fn execute_cascade<'a>(
        noria: &'a mut NoriaConnector,
        upstream: &'a mut DB,
        noria_prep: &noria_connector::PrepareResult,
        upstream_prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
        exec_meta: &'a DB::ExecMeta,
        ex_info: Option<&mut ExecutionInfo>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let noria_res = Self::execute_noria(noria, noria_prep, params, event).await;
        match noria_res {
            Ok(noria_ok) => {
                if let Some(info) = ex_info {
                    info.execute_succeeded();
                }
                Ok(noria_ok)
            }
            Err(noria_err) => {
                if let Some(info) = ex_info {
                    if noria_err.is_networking_related() {
                        info.execute_network_failure();
                    } else if noria_err.caused_by_data_type_conversion() {
                        // Consider queries that fail due to data type conversion errors as
                        // unsupported. These queries will likely fail on each query to noria,
                        // introducing increased latency.
                        info.execute_unsupported();
                    }
                }
                if !noria_err.any_cause(|e| {
                    matches!(
                        e,
                        ReadySetError::ReaderMissingKey
                            | ReadySetError::NoCacheForQuery
                            | ReadySetError::UpqueryTimeout
                            | ReadySetError::UnparseableQuery { .. }
                    )
                }) {
                    warn!(error = %noria_err,
                          "Error received from noria, sending query to fallback");
                }

                Self::execute_upstream(
                    upstream,
                    upstream_prep,
                    params,
                    exec_meta,
                    None,
                    event,
                    true,
                    None,
                )
                .await
            }
        }
    }

    /// Attempts to migrate a query on noria, after
    /// - the query was marked as `MigrationState::Successful(_)` in the cache -or-
    /// - the epoch stored in `MigrationState::Inlined` advanced but the query is not yet prepared
    ///   on noria.
    ///
    /// If the migration is successful, the prepare result is updated with the noria result. If the
    /// state was previously `MigrationState::Pending`, it is updated to
    /// `MigrationState::Successful(CacheType::Deep)`.
    ///
    /// Returns an error if the statement is already prepared on noria.
    ///
    /// # Panics
    ///
    /// If the query is not in the `MigrationState::Pending` or `MigrationState::Inlined` state
    async fn update_noria_prepare(
        noria: &mut NoriaConnector,
        cached_entry: &mut PreparedStatement<DB>,
        rewrite_context: &RewriteContext,
        manual_cache: Option<&ManualCacheEntry>,
    ) -> ReadySetResult<()> {
        debug_assert!(
            cached_entry.migration_state.is_pending() || cached_entry.migration_state.is_inlined()
        );

        let upstream_prep: UpstreamPrepare<DB> = match &cached_entry.prep.inner {
            PrepareResultInner::Upstream(prep) => prep.clone(),
            _ => internal!("Update may only be called for Upstream prepares"),
        };

        let parsed_statement = cached_entry
            .parsed_query
            .as_ref()
            .expect("Cached entry for pending state");

        let noria_prep = match &**parsed_statement {
            SqlQuery::Select(stmt) => {
                noria
                    .prepare_select(stmt.clone(), false, rewrite_context, manual_cache)
                    .await?
            }
            _ => internal!("Only SELECT statements can be pending migration"),
        };

        // At this point we got a successful noria prepare, so we want to replace the Upstream
        // result with a NoriaAndUpstream result
        cached_entry.prep = PrepareResult::new(
            cached_entry.prep.statement_id,
            PrepareResultInner::NoriaAndUpstream(noria_prep, upstream_prep),
        );
        // If the query was previously `Pending`, update to `Successful`. If it was inlined, we do
        // not update the migration state.
        if cached_entry.migration_state == MigrationState::Pending {
            cached_entry.migration_state = MigrationState::Successful(CacheType::Deep);
        }

        Ok(())
    }

    fn upstream_mut(upstream: &mut Option<DB>) -> ReadySetResult<&mut DB> {
        upstream
            .as_mut()
            .ok_or_else(|| no_upstream_err("Execution upstream requires an upstream"))
    }

    /// Executes a prepared statement identified by `id` with parameters specified by the client
    /// `params`.
    /// A [`QueryExecutionEvent`], is used to track metrics and behavior scoped to the
    /// execute operation.
    #[inline]
    pub async fn execute<'a>(
        &'a mut self,
        id: u32,
        params: &[DfValue],
        exec_meta: &'a DB::ExecMeta,
    ) -> Result<(QueryResult<'a, DB>, ProxyState), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        self.state.last_query = None;
        self.state.pending_proxy_reason = None;
        let schema_search_path = self.connectors.noria.schema_search_path().to_vec();
        // Taken before the statement is borrowed, since that borrow spans the dispatch below and
        // the template is only applied afterwards. `None` for everything but the recognised
        // session-mutating shapes.
        let session_mutation = self.state.prepared.session_mutation(id).cloned();
        let cached_statement = self
            .state
            .prepared
            .get_mut(id)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let mut event = QueryExecutionEvent::new(EventType::Execute);
        event.query.clone_from(&cached_statement.parsed_query);
        event.query_id = cached_statement.query_id;

        // Stamp the session's `last_write_at` before dispatching, so that the routing
        // rule for `TrxCachePolicy::UntilWrite` kicks in for subsequent reads in this
        // transaction. The timestamp is dropped at the next BEGIN / ROLLBACK.
        if cached_statement
            .parsed_query
            .as_deref()
            .is_some_and(SqlQuery::is_write)
        {
            self.state.write_tracker.mark_write();
        }

        let results_encoding = self.connectors.noria.results_encoding();
        let upstream = &mut self.connectors.upstream;
        let noria = &mut self.connectors.noria;

        // If the query is pending, check the query status cache to see if it is now successful.
        //
        // If the query is inlined, we have to check the epoch of the current state in the query
        // status cache to see if we should prepare the statement again.
        if cached_statement.migration_state.is_pending()
            || cached_statement.migration_state.is_inlined()
        {
            // We got a statement with a pending migration, we want to check if migration is
            // finished by now
            // Use try_query_migration_state (read-only) rather than query_migration_state
            // to avoid overwriting the stored schema generation. The generation was set
            // at prepare time and must not be updated to the current generation here.
            let (query_id, new_migration_state) = self
                .state
                .query_status_cache
                .try_query_migration_state(cached_statement.as_view_request()?);

            // A manually parameterized cache (`AUTOPARAM`) created after this statement was
            // prepared claims its standard shape; route to it even though the shape's own
            // migration state never transitions to Successful.
            let manual_cache = self.state.query_status_cache.manual_cache(&query_id);

            let search_path = cached_statement
                .view_request
                .as_ref()
                .map(|pr| pr.schema_search_path.clone())
                .unwrap_or(schema_search_path);

            let rewrite_context = RewriteContext::new(
                self.settings.dialect.into(),
                self.state.schema_handle.get_catalog_retrying().await?,
                search_path,
            );

            if manual_cache.is_some()
                || matches!(new_migration_state, Some(MigrationState::Successful(_)))
            {
                // Attempt to prepare on ReadySet
                let _ = Self::update_noria_prepare(
                    noria,
                    cached_statement,
                    &rewrite_context,
                    manual_cache.as_ref(),
                )
                .await;
            } else if let Some(MigrationState::Inlined(new_state)) = new_migration_state
                && let MigrationState::Inlined(ref old_state) = cached_statement.migration_state
            {
                // if the epoch has advanced, then we've made changes to the inlined caches so
                // we should refresh the view cache and prepare if necessary.
                if new_state.epoch > old_state.epoch {
                    let view_request = cached_statement.as_view_request()?;
                    // Request a new view from ReadySet.
                    let updated_view_cache = noria
                        .update_view_cache(
                            &view_request.statement,
                            Some(view_request.schema_search_path.clone()),
                            false, // create_if_not_exists
                            true,  // is_prepared
                            rewrite_context.schema_generation(),
                        )
                        .await
                        .is_ok();
                    // If we got a new view from ReadySet and we have only prepared against
                    // upstream, prepare the statement against ReadySet.
                    //
                    // Update the migration state if we updated the view_cache and, if
                    // necessary, the PrepareResult.
                    if updated_view_cache
                        && matches!(cached_statement.prep.inner, PrepareResultInner::Upstream(_))
                    {
                        if Self::update_noria_prepare(
                            noria,
                            cached_statement,
                            &rewrite_context,
                            None,
                        )
                        .await
                        .is_ok()
                        {
                            cached_statement.migration_state = MigrationState::Inlined(new_state);
                        }
                    } else if updated_view_cache {
                        cached_statement.migration_state = MigrationState::Inlined(new_state);
                    }
                }
            }
        }

        let should_fallback = {
            let policy = cached_statement.trx_cache_policy;
            // Per-execute ACL gate: a statement prepared before a revocation sees the new
            // verdict here. Checked ahead of the ALWAYS pin, which it overrides.
            let acl_reason =
                if matches!(&cached_statement.prep.inner, PrepareResultInner::Shallow(_)) {
                    cached_statement.query_id.and_then(|query_id| {
                        acl_decline_reason(
                            &self.state.acl,
                            self.connectors.session.as_ref(),
                            self.state.client_identity.as_ref(),
                            self.settings.require_authentication,
                            query_id,
                        )
                    })
                } else {
                    None
                };
            if let Some(reason) = acl_reason {
                self.state.pending_proxy_reason = Some(reason);
                true
            } else if matches!(policy, TrxCachePolicy::Always) {
                false
            } else {
                let is_recovering = cached_statement.in_fallback_recovery(
                    self.settings.query_max_failure_duration,
                    self.settings.fallback_recovery_duration,
                );
                let has_cache = matches!(
                    &cached_statement.prep.inner,
                    PrepareResultInner::Noria(_)
                        | PrepareResultInner::NoriaAndUpstream(..)
                        | PrepareResultInner::Shallow(_)
                );
                let cache_type =
                    if matches!(&cached_statement.prep.inner, PrepareResultInner::Shallow(_)) {
                        "shallow"
                    } else {
                        "deep"
                    };
                // A bypassed cache is only worth reporting when the policy is why we fell back.
                let record_skip =
                    has_cache && !is_recovering && !cached_statement.is_unsupported_execute();
                let skip_reason = SelectRouter::cache_skip_reason(
                    self.state.proxy_state,
                    &mut self.state.write_tracker,
                    policy,
                    cache_type,
                    record_skip,
                    || {
                        cached_statement
                            .query_id
                            .as_ref()
                            .map(|id| id.to_string())
                            .unwrap_or_default()
                    },
                );
                // Only the policy's own bypass is worth reporting: recovery and unsupported
                // executes leave the cache for a different reason, tagged elsewhere.
                if let (true, Some(reason)) = (record_skip, skip_reason) {
                    self.state.pending_proxy_reason = Some(reason);
                }

                cached_statement.is_unsupported_execute() || is_recovering || skip_reason.is_some()
            }
        };

        let result = match &cached_statement.prep.inner {
            PrepareResultInner::Noria(prep) => Self::execute_noria(noria, prep, params, &mut event)
                .await
                .map_err(Into::into),
            PrepareResultInner::Upstream(prep) => {
                // No inlined caches for this query exist if we are only prepared on upstream.
                if cached_statement.migration_state.is_inlined() {
                    self.state
                        .query_status_cache
                        .inlined_cache_miss(cached_statement.as_view_request()?, params.to_vec())
                }
                if cached_statement.is_skip_cache {
                    let query_id = cached_statement
                        .query_id
                        .as_ref()
                        .map(|id| id.to_string())
                        .unwrap_or_default();
                    record_skip_cache(query_id, "deep", "hint");
                }
                Self::execute_upstream(
                    Self::upstream_mut(upstream)?,
                    prep,
                    params,
                    exec_meta,
                    None,
                    &mut event,
                    false,
                    None,
                )
                .await
            }
            PrepareResultInner::NoriaAndUpstream(.., uprep)
            | PrepareResultInner::Shallow(uprep)
                if should_fallback =>
            {
                Self::execute_upstream(
                    Self::upstream_mut(upstream)?,
                    uprep,
                    params,
                    exec_meta,
                    None,
                    &mut event,
                    false,
                    None,
                )
                .await
            }
            PrepareResultInner::NoriaAndUpstream(nprep, uprep)
                if !nprep.frozen_satisfied(params)? =>
            {
                // A manually parameterized cache (`AUTOPARAM`) backs this statement, but its
                // frozen literals don't match these params: the query isn't served by the cache,
                // so go straight upstream as a clean miss rather than attempting (and declining)
                // the readyset read.
                Self::execute_upstream(
                    Self::upstream_mut(upstream)?,
                    uprep,
                    params,
                    exec_meta,
                    None,
                    &mut event,
                    false,
                    None,
                )
                .await
            }
            PrepareResultInner::NoriaAndUpstream(nprep, uprep) => {
                if cached_statement.execution_info.is_none() {
                    cached_statement.execution_info = Some(ExecutionInfo {
                        state: ExecutionState::Failed,
                        last_transition_time: Instant::now(),
                    });
                }
                Self::execute_cascade(
                    noria,
                    Self::upstream_mut(upstream)?,
                    nprep,
                    uprep,
                    params,
                    exec_meta,
                    cached_statement.execution_info.as_mut(),
                    &mut event,
                )
                .await
            }
            PrepareResultInner::Shallow(prep) => {
                let query_id = cached_statement
                    .query_id
                    .as_ref()
                    .ok_or_else(|| internal_err!("Shallow prepare missing query_id"))?;
                let query_params = cached_statement
                    .params
                    .as_ref()
                    .ok_or_else(|| internal_err!("Shallow prepare missing params"))?;
                let view_request = cached_statement.as_shallow()?;

                Self::execute_shallow(
                    Self::upstream_mut(upstream)?,
                    &self.state.shallow,
                    self.state.rls_coordinator.as_ref(),
                    self.connectors.session.as_ref(),
                    prep,
                    params,
                    exec_meta,
                    &mut event,
                    query_id,
                    query_params,
                    self.state.shallow_refresh_pool.as_ref(),
                    view_request,
                    results_encoding,
                )
                .await
            }
        };

        // Mirror a session-mutating prepared statement (PostgREST's set_config
        // batch today; future SET LOCAL / SET ROLE shapes tomorrow) into the
        // per-connection SessionContext so the RLS shallow cache sees the bound
        // role and JWT claims for the next user query. Only after the upstream
        // has applied it: mirroring before dispatch would leave our tracking
        // state ahead of the database if the statement failed, and a later read
        // would key against a role / JWT / GUC the upstream never adopted. The
        // template was recognised at prepare time and indexed by statement id;
        // non-matching statements miss cheaply. With RLS disabled there are no
        // scoped caches to key, so the mirror is skipped.
        if result.is_ok()
            && let Some(registry) = self.state.policy_registry.as_ref()
            && let Some(session) = self.connectors.session.as_ref()
            && let Some(template) = session_mutation.as_ref()
        {
            session_mutation::apply(template, params, session, registry);
        }

        // Mirror a prepared `SET [LOCAL] SESSION AUTHORIZATION` once upstream
        // has applied it, matching `query_adhoc_non_select` on the simple
        // path. Without this the extended protocol changes the upstream
        // identity while the mirror keeps the old one, and later scoped
        // lookups would key against a stale partition.
        if result.is_ok()
            && let Some(session) = self.connectors.session.as_ref()
            && let Some(SqlQuery::Set(SetStatement::SessionAuthorization(auth))) =
                cached_statement.parsed_query.as_deref()
        {
            Self::mirror_session_authorization(session, self.state.policy_registry.as_ref(), auth);
        }

        // Mirror a prepared `SET [LOCAL] ROLE` once upstream has applied it, matching the simple
        // path. Role membership is an authorization boundary, so a `SET ROLE` upstream rejects must
        // not advance the mirror; gating on `result.is_ok()` (the execute outcome) enforces that.
        if result.is_ok()
            && let Some(session) = self.connectors.session.as_ref()
            && let Some(SqlQuery::Set(set)) = cached_statement.parsed_query.as_deref()
        {
            Self::mirror_set_role(session, self.state.policy_registry.as_ref(), set);
        }

        if let Some(q) = &cached_statement.parsed_query {
            Self::update_transaction_boundaries(
                &mut self.state.proxy_state,
                &mut self.state.write_tracker,
                q.as_ref(),
            );
        }

        if let Some(e) = event.noria_error.as_ref() {
            if e.caused_by_view_not_found() {
                // This can happen during cascade execution if the noria query was removed from
                // another connection
                cached_statement.prep.make_upstream_only();
            } else if e.caused_by_unsupported() {
                // On an unsupported execute we update the query migration state to be unsupported.
                self.state.query_status_cache.update_query_migration_state(
                    cached_statement.as_view_request()?,
                    MigrationState::Unsupported(e.unsupported_cause().unwrap_or_default()),
                    None,
                );
            } else if matches!(e, ReadySetError::NoCacheForQuery) {
                self.state
                    .query_status_cache
                    .inlined_cache_miss(cached_statement.as_view_request()?, params.to_vec())
            }
        };

        let staged = self
            .state
            .pending_proxy_reason
            .take()
            .or(cached_statement.prepare_proxy_reason);
        self.state.last_query = QueryInfo::from_event(&event).map(|i| i.or_reason(staged));
        log_query(
            self.state.query_log_sender.as_ref(),
            event,
            self.settings.slowlog,
            self.settings.dialect,
        );

        let proxy_state = self.state.proxy_state;
        result.map(|r| (r, proxy_state))
    }

    pub async fn remove_statement(&mut self, deallocate_id: DeallocateId) -> Result<(), DB::Error> {
        // in all cases, we need to call upstream.remove_statement(), but in the case
        // of a Numeric id and it's in the prepared-statement registry, we need to use
        // that id instead when we call upstream.remove_statement().
        let mut dealloc_id = deallocate_id.clone();
        match deallocate_id {
            DeallocateId::Numeric(id) => {
                if let Some(statement) = self.state.prepared.remove(id) {
                    match statement.prep.into_upstream() {
                        Some(ur) => {
                            dealloc_id = DeallocateId::Numeric(ur.statement_id);
                        }
                        _ => {
                            // this is the case where a prepared statement was created for readyset
                            // use, and not prepared/executed on the upstream.
                            return Ok(());
                        }
                    }
                }
            }
            DeallocateId::All => {
                self.state.prepared.clear();
            }
            DeallocateId::Named(_) => {}
        }

        if let Some(upstream) = &mut self.connectors.upstream {
            upstream.remove_statement(dealloc_id).await?;
        }
        Ok(())
    }
}
