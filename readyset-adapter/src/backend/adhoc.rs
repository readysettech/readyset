//! The simple-query protocol: one statement of text in, results out, no handle kept.
//!
//! [`Backend::query`] parses the statement, hands the Readyset extensions, shallow reads and
//! deep SELECTs to the modules that own those decisions, and proxies whatever is left upstream.
//! The extended-query path in [`super::prepared`] answers the same questions, but has to split
//! them across Parse and Bind.

use std::sync::Arc;
use std::time::Instant;

use readyset_adapter_types::{DeallocateId, ParsedCommand};
use readyset_client::query::{
    ExecutionInfo, ExecutionState, MigrationState, Query, QueryId, QueryStatus,
};
use readyset_client::{ShallowViewRequest, ViewCreateRequest};
use readyset_client_metrics::{
    EventType, QueryDestination, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent,
    SqlQueryType,
};
use readyset_errors::{ReadySetError, ReadySetResult, internal_err, unsupported};
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{
    CacheType, DeallocateStatement, DiscardObject, ReadysetHintDirective, SetStatement,
    ShallowCacheQuery, SqlQuery, StatementIdentifier, TrxCachePolicy, UseStatement,
};
use readyset_sql_passes::adapter_rewrites::{self, DfQueryParameters, QueryParameters};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent};
use readyset_util::SizeOf;
use schema_catalog::SchemaGeneration;
use tracing::{error, trace, warn};

use super::noria_connector::{self, ExecuteSelectContext};
use super::routing::{ProxyState, SelectRouter, ShouldTrySelect};
use super::{
    Backend, BackendConnectors, BackendSettings, BackendState, MigrationMode, QueryInfo,
    QueryResult, convert_or_parse_query, log_query, parse_shallow_query,
};
use crate::query_handler::UpstreamSetRewrite;
use crate::session_mutation;
use crate::{QueryHandler, UpstreamDatabase};

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    #[allow(clippy::too_many_arguments)]
    async fn try_noria_adhoc_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &'a str,
        mut view_request: ViewCreateRequest,
        params: QueryParameters,
        schema_generation: SchemaGeneration,
        event: &mut QueryExecutionEvent,
        is_skip_cache: bool,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let verdict = SelectRouter::new(
            settings.dialect,
            connectors.noria.rewrite_params(),
            state.query_status_cache,
            state.proxy_state,
            &mut state.write_tracker,
        )
        .route(&mut view_request, params, schema_generation, is_skip_cache);
        match verdict {
            ShouldTrySelect::Yes {
                status,
                params,
                schema_generation,
            } => {
                Self::noria_adhoc_select(
                    connectors,
                    settings,
                    state,
                    query,
                    view_request,
                    status,
                    event,
                    params,
                    schema_generation,
                )
                .await
            }
            ShouldTrySelect::No { error } => {
                if connectors.upstream.is_none() {
                    Err(error
                        .unwrap_or(ReadySetError::InvalidUpstreamDatabase)
                        .into())
                } else {
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn noria_adhoc_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        original_query: &'a str,
        view_request: ViewCreateRequest,
        mut status: QueryStatus,
        event: &mut QueryExecutionEvent,
        mut params: DfQueryParameters,
        schema_generation: SchemaGeneration,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        // Track the schema generation that was used to rewrite this query so that
        // CREATE CACHE FROM <query_id> can retrieve it later. Without this, ad-hoc
        // (text protocol) queries would never store their generation in the QSC.
        status.schema_generation = Some(schema_generation);

        let original_status = status.clone();
        let did_work = if let Some(ref mut i) = status.execution_info {
            i.reset_if_exceeded_recovery(
                settings.query_max_failure_duration,
                settings.fallback_recovery_duration,
            )
        } else {
            false
        };

        // A manually parameterized cache (`AUTOPARAM`) claiming this query's standard shape
        // serves the read regardless of the shape's own migration state.
        let manual_cache = state
            .query_status_cache
            .manual_cache(&QueryId::from(&view_request));
        let served_via_manual_cache = manual_cache.is_some();
        if let Some(mc) = &manual_cache {
            // The frozen literals travel with the params so the readsider can strip them from the
            // lookup key; if they don't match these params the cache doesn't serve this query, so
            // go straight upstream as a clean miss.
            params.set_frozen(mc.frozen.clone());
            if connectors.upstream.is_some() && !params.frozen_satisfied(&[])? {
                return Self::query_fallback(
                    connectors.upstream.as_mut(),
                    original_query,
                    event,
                    None,
                )
                .await;
            }
        }

        // Test several conditions to see if we should proxy
        let upstream_exists = connectors.upstream.is_some();
        let proxy_out_of_band = settings.migration_mode != MigrationMode::InRequestPath
            && !matches!(status.migration_state, MigrationState::Successful(_))
            && manual_cache.is_none();
        let unsupported = matches!(&status.migration_state, MigrationState::Unsupported(_))
            && manual_cache.is_none();
        let exceeded_network_failure = status
            .execution_info
            .as_mut()
            .map(|i| i.execute_network_failure_exceeded(settings.query_max_failure_duration))
            .unwrap_or(false);

        if !matches!(status.trx_cache_policy, TrxCachePolicy::Always)
            && (upstream_exists && (proxy_out_of_band || unsupported || exceeded_network_failure))
        {
            if did_work {
                state.query_status_cache.update_transition_time(
                    &view_request,
                    &status.execution_info.unwrap().last_transition_time,
                );
            }
            return Self::query_fallback(connectors.upstream.as_mut(), original_query, event, None)
                .await;
        }

        event.destination = Some(QueryDestination::Readyset(None));
        let create_if_missing = settings.migration_mode == MigrationMode::InRequestPath;

        let ctx = ExecuteSelectContext::AdHoc {
            statement: &view_request.statement,
            create_if_missing,
            processed_query_params: params,
            schema_generation,
            manual_cache_name: manual_cache.map(|mc| mc.name),
        };
        let res = connectors.noria.execute_select(ctx, event).await;
        if status.execution_info.is_none() {
            status.execution_info = Some(ExecutionInfo {
                state: ExecutionState::Failed,
                last_transition_time: Instant::now(),
            });
        }

        match res {
            Ok(noria_ok) => {
                // We managed to select on ReadySet, good for us. Don't promote the standard
                // (fully autoparameterized) shape's status when the read was served by a manual
                // cache: that shape has no deep cache of its own, so a stale `Successful` would
                // make later queries attempt a non-existent view once the manual cache is dropped.
                if !served_via_manual_cache {
                    status.migration_state = MigrationState::Successful(CacheType::Deep);
                }
                if let Some(i) = status.execution_info.as_mut() {
                    i.execute_succeeded()
                }
                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(&view_request, status);
                }
                // Enqueue the original query for background sampling if enabled.
                if !state.is_internal_connection
                    && let Some(tx) = state.sampler_tx.as_ref()
                {
                    let schema_search_path = view_request.schema_search_path.clone();
                    let _ = tx.try_send((
                        event.clone(),
                        original_query.to_string(),
                        schema_search_path,
                    ));
                }
                Ok(noria_ok.into())
            }
            Err(noria_err) => {
                event.set_noria_error(&noria_err);

                if let Some(i) = status.execution_info.as_mut() {
                    if noria_err.is_networking_related() {
                        i.execute_network_failure();
                    } else if noria_err.caused_by_view_destroyed() {
                        i.execute_dropped();
                    }
                }

                if noria_err.caused_by_view_not_found() {
                    status.migration_state = MigrationState::Pending;
                } else if noria_err.caused_by_unsupported() {
                    status.migration_state = MigrationState::Unsupported(
                        noria_err.unsupported_cause().unwrap_or_default(),
                    );
                };

                let always = matches!(status.trx_cache_policy, TrxCachePolicy::Always);

                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(&view_request, status);
                }

                // Try to execute on fallback if present, as long as query is not an `always`
                // query.
                match (always, connectors.upstream.as_mut()) {
                    (true, _) | (_, None) => {
                        // Enqueue the original query for background sampling if enabled.
                        if !state.is_internal_connection
                            && let Some(tx) = state.sampler_tx.as_ref()
                        {
                            let schema_search_path = view_request.schema_search_path.clone();
                            let _ = tx.try_send((
                                event.clone(),
                                original_query.to_string(),
                                schema_search_path,
                            ));
                        }
                        Err(noria_err.into())
                    }
                    (false, Some(fallback)) => {
                        event.destination = Some(QueryDestination::ReadysetThenUpstream(None));
                        let _t = event.start_upstream_timer();
                        fallback
                            .query(original_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }
                }
            }
        }
    }

    async fn query_adhoc_non_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        raw_query: &'a str,
        event: &mut QueryExecutionEvent,
        query: SqlQuery,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let mut upstream_set_rewrite = UpstreamSetRewrite::ProxyVerbatim;
        match &query {
            SqlQuery::Set(s) => {
                upstream_set_rewrite =
                    Self::handle_set(connectors, settings, state, raw_query, s, event)?
            }
            SqlQuery::Use(UseStatement { database }) => connectors
                .noria
                .set_schema_search_path(vec![database.clone()]),
            SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                // `ROLLBACK TO SAVEPOINT` does not end the transaction, so it
                // must not revert transaction-local RLS state or clear a
                // transaction-scoped trust gap.
                let ends_transaction = match &query {
                    SqlQuery::Rollback(rollback_stmt) => rollback_stmt.ends_transaction(),
                    _ => true,
                };
                if ends_transaction && let Some(session) = connectors.session.as_ref() {
                    session.on_trx_end();
                }
            }
            _ => (),
        }

        {
            // Upstream reads are tried when noria reads produce an error. Upstream writes are done
            // by default when the upstream connector is present.
            if let Some(upstream) = connectors.upstream.as_mut() {
                match query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                        event.sql_type = SqlQueryType::Write;
                        event.destination = Some(QueryDestination::Upstream);
                        let _t = event.start_upstream_timer();

                        let query_result = upstream.query(raw_query).await;
                        query_result.map(|r| QueryResult::Upstream(r, None, None))
                    }

                    SqlQuery::CreateDatabase(_)
                    | SqlQuery::CreateView(_)
                    | SqlQuery::CreateTable(_)
                    | SqlQuery::DropTable(_)
                    | SqlQuery::DropView(_)
                    | SqlQuery::AlterTable(_)
                    | SqlQuery::RenameTable(_)
                    | SqlQuery::Truncate(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::CreateIndex(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream
                            .query(raw_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }
                    SqlQuery::Set(set) => {
                        event.sql_type = SqlQueryType::Other;
                        match upstream_set_rewrite {
                            UpstreamSetRewrite::ProxyVerbatim => {}
                            UpstreamSetRewrite::Rewrite(stmt) => {
                                // The trait ties the result's lifetime to the query text, so a
                                // rewritten statement's result can't be returned; a SET only
                                // produces an OK, so respond as the no-upstream branch does.
                                let rewritten = stmt.display(settings.dialect).to_string();
                                upstream.query(&rewritten).await?;
                                return Ok(QueryResult::Noria(noria_connector::QueryResult::Empty));
                            }
                            UpstreamSetRewrite::Skip => {
                                return Ok(QueryResult::Noria(noria_connector::QueryResult::Empty));
                            }
                        }
                        let result = upstream.query(raw_query).await?;
                        // Mirror an identity change only now that upstream has accepted it: a
                        // rejected statement must not leave the session mirror pointing at an
                        // identity upstream never adopted. `SET ROLE` is an authorization boundary
                        // (role membership), so mirroring it before the forward would let a client
                        // assume a role upstream refused it.
                        if let Some(session) = connectors.session.as_ref() {
                            match &set {
                                SetStatement::SessionAuthorization(auth) => {
                                    Self::mirror_session_authorization(
                                        session,
                                        state.policy_registry.as_ref(),
                                        auth,
                                    );
                                }
                                _ => Self::mirror_set_role(
                                    session,
                                    state.policy_registry.as_ref(),
                                    &set,
                                ),
                            }
                        }
                        Ok(QueryResult::Upstream(result, None, None))
                    }
                    SqlQuery::CompoundSelect(_)
                    | SqlQuery::Show(_)
                    | SqlQuery::Discard(_)
                    | SqlQuery::Comment(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream
                            .query(raw_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }

                    SqlQuery::StartTransaction(_) | SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                        Self::handle_transaction_boundaries(
                            Some(upstream),
                            &mut state.proxy_state,
                            &mut state.write_tracker,
                            &query,
                            raw_query,
                        )
                        .await
                    }

                    SqlQuery::CreateCache(_)
                    | SqlQuery::Deallocate(_)
                    | SqlQuery::DropCache(_)
                    | SqlQuery::DropAllCaches(_)
                    | SqlQuery::FlushAllShallowCaches(_)
                    | SqlQuery::FlushCache(_)
                    | SqlQuery::DropAllProxiedQueries(_)
                    | SqlQuery::AlterReadySet(_)
                    | SqlQuery::Explain(_)
                    | SqlQuery::CreateRls(_)
                    | SqlQuery::DropRls(_)
                    | SqlQuery::CreateMcpToken(_)
                    | SqlQuery::DropMcpToken(_)
                    | SqlQuery::AlterMcpToken(_) => {
                        unreachable!("path returns prior")
                    }
                }
            } else {
                event.destination = Some(QueryDestination::Readyset(None));
                let start = Instant::now();

                let res = match &query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    // CREATE VIEW will still trigger migrations with explicit-migrations enabled
                    SqlQuery::CreateView(q) => connectors.noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::AlterTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::DropTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::DropView(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::Insert(q) => connectors.noria.handle_insert(q).await,
                    SqlQuery::Update(q) => connectors.noria.handle_update(q).await,
                    SqlQuery::Delete(q) => connectors.noria.handle_delete(q).await,
                    SqlQuery::Truncate(q) => connectors.noria.handle_truncate(q).await,
                    SqlQuery::Deallocate(_) => unreachable!("deallocate path returns prior"),

                    // Return an empty result as we are allowing unsupported set statements. Commit
                    // messages are dropped - we do not support transactions in noria standalone.
                    // We return an empty result set instead of an error to support test
                    // applications.
                    SqlQuery::Set(_)
                    | SqlQuery::Commit(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::Comment(_) => Ok(noria_connector::QueryResult::Empty),
                    q => {
                        error!(query = ?q, "unsupported query");
                        unsupported!("query type unsupported: {q:?}");
                    }
                };

                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.noria_error = res.as_ref().err().cloned();
                Ok(QueryResult::Noria(res?))
            }
        }
    }

    fn handle_deallocate_statement<'a>(stmt: DeallocateStatement) -> QueryResult<'a, DB> {
        let dealloc_id = match stmt.identifier {
            StatementIdentifier::SingleStatement(name) => DeallocateId::from(name.clone()),
            StatementIdentifier::AllStatements => DeallocateId::All,
        };
        QueryResult::Parser(ParsedCommand::Deallocate(dealloc_id))
    }

    async fn query_inner<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &'a str,
        query_shallow: &mut Option<ShallowViewRequest>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let (shallow_parsed, hint) = {
            let _t = event.start_parse_timer();
            parse_shallow_query(settings, query)
        };

        let is_skip_cache = matches!(&hint, Some(ReadysetHintDirective::SkipCache));

        // A successful shallow parse is always SELECT-shaped, so the Set/Use handling in
        // `check_readyset_schema_routing` cannot apply: route and serve the query on the
        // shallow parse alone, deferring the cost of the full parse to the fall-through path.
        let mut deep_ast = None;
        let shallow_parsed: Option<ReadySetResult<ShallowCacheQuery>> = match shallow_parsed {
            Ok(shallow_query) => {
                // Keep a copy of the sqlparser AST before the shallow rewrite mutates it, so
                // the fall-through below can derive the Readyset AST without a second text
                // parse.
                if settings.retain_shallow_ast() {
                    deep_ast = Some((*shallow_query).clone());
                }
                let shallow_parsed = Ok(shallow_query);
                if state.should_query_readyset_schema(settings, &shallow_parsed) {
                    let session = Self::readyset_schema_session(connectors, state)?;
                    let result = session.query(query).await?;
                    return Ok(QueryResult::ReadysetSchema(result));
                }

                if let Some((shallow, params)) = connectors.prepare_shallow_query(shallow_parsed) {
                    if let Some((query_id, _)) = Self::should_query_shallow(
                        connectors, settings, state, &shallow, query, hint,
                    )
                    .await
                        && !Self::acl_declines_serve(connectors, settings, state, query_id)
                    {
                        let result =
                            Self::query_shallow(connectors, state, query, query_id, event, params)
                                .await;

                        event.sql_type = SqlQueryType::Read;
                        event.query_id = Some(query_id);
                        if let Err(e) = &result {
                            event.set_noria_error(&internal_err!("{e}"));
                        }
                        *query_shallow = Some(shallow);
                        return result;
                    }
                    *query_shallow = Some(shallow);
                }
                None
            }
            Err(e) => Some(Err(e)),
        };

        let parsed = {
            let _t = event.start_parse_timer();
            convert_or_parse_query(settings, deep_ast, query)
        };

        // Mirror full-session resets into the SessionContext before the query
        // runs. `DISCARD ALL` / `RESET ALL` fully reset run-time state. A `SET
        // [LOCAL] SESSION AUTHORIZATION` identity change is mirrored separately,
        // *after* upstream accepts it (see `query_adhoc_non_select`), so a
        // rejected statement cannot leave the mirror pointing at an identity
        // upstream never adopted.
        if let Some(session) = connectors.session.as_ref() {
            match &parsed {
                Ok(SqlQuery::Discard(d)) if d.object_type == DiscardObject::All => {
                    session.discard_all()
                }
                _ => {}
            }
        }

        // Mirror a simple-protocol `set_config(...)` batch into the session.
        // The extended protocol recognizes it at prepare and applies it at
        // execute with the bound parameters; the simple path carries no bound
        // params, so the literal-valued form is recognized and applied here
        // (an empty parameter vector resolves the literal sources, and any
        // stray `$N` source resolves to nothing and fails the batch closed).
        if let Some(session) = connectors.session.as_ref()
            && let Some(registry) = state.policy_registry.as_ref()
            && let Ok(q) = &parsed
            && let Some(template) = session_mutation::recognize(q)
        {
            session_mutation::apply(&template, &[], session, registry);
        }

        if let Some(result) = Self::check_readyset_schema_routing(state, &parsed) {
            return Ok(result);
        }

        // Statements that don't parse as a shallow SELECT still route to the readyset schema
        // when the session's search path points at it (`readyset_schema_route_all`). This must
        // stay after `check_readyset_schema_routing` so a SET/USE can first switch routing off.
        if let Some(shallow_parsed) = shallow_parsed
            && state.should_query_readyset_schema(settings, &shallow_parsed)
        {
            let session = Self::readyset_schema_session(connectors, state)?;
            let result = session.query(query).await?;
            return Ok(QueryResult::ReadysetSchema(result));
        }

        // Maintain the session-level `last_write_at` that gates
        // `TrxCachePolicy::UntilWrite` caches. Mark a write whenever the parsed query
        // is a write, and conservatively mark on parse failure inside any transaction
        // (some writes -- e.g. `SELECT ... FOR UPDATE`, stored-proc `CALL`,
        // CTE-embedded `INSERT` -- never reach an `SqlQuery` variant).
        match &parsed {
            Ok(q) if q.is_write() => state.write_tracker.mark_write(),
            Err(_) if state.proxy_state.in_transaction_or_implicit() => {
                state.write_tracker.mark_write()
            }
            _ => {}
        }

        match parsed {
            // Parse error, but no fallback exists
            Err(e) if !connectors.has_fallback() => {
                error!("{}", e);
                event.set_noria_error(&e);
                Err(e.into())
            }
            // Parse error, send to fallback
            Err(e) => {
                if !matches!(
                    e,
                    ReadySetError::ReaderMissingKey
                        | ReadySetError::NoCacheForQuery
                        | ReadySetError::UnparseableQuery { .. }
                ) {
                    warn!(error = %e, "Error received from noria, sending query to fallback");
                    event.set_noria_error(&e);
                }
                let fallback_res =
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await;
                if fallback_res.is_ok() {
                    let (id, _) = state
                        .query_status_cache
                        .insert(Query::ParseFailed(query.to_string().into(), e.to_string()));
                    if let Some(ref telemetry_sender) = state.telemetry_sender {
                        if let Err(e) = telemetry_sender.send_event_with_payload(
                            TelemetryEvent::QueryParseFailed,
                            TelemetryBuilder::new()
                                .server_version(
                                    option_env!("CARGO_PKG_VERSION").unwrap_or_default(),
                                )
                                .query_id(id.to_string())
                                .build(),
                        ) {
                            warn!(error = %e, "Failed to send parse failed metric");
                        }
                    } else {
                        trace!("No telemetry sender. not sending metric for {query}");
                    }
                }
                fallback_res
            }
            // Check for COMMIT+ROLLBACK before we check whether we should proxy, since we need to
            // know when a COMMIT or ROLLBACK happens so we can leave `ProxyState::InTransaction`
            Ok(parsed_query @ (SqlQuery::Commit(_) | SqlQuery::Rollback(_))) => {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    parsed_query,
                )
                .await
            }
            Ok(ref parsed_query) if parsed_query.is_readyset_extension() => {
                Self::query_readyset_extensions(connectors, settings, state, parsed_query, event)
                    .await
                    .map(Into::into)
                    .map_err(Into::into)
            }
            // SET autocommit=1 needs to be handled explicitly or it will end up getting proxied in
            // most cases.
            Ok(SqlQuery::Set(s))
                if Handler::handle_set_statement(&s).set_autocommit == Some(true) =>
            {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    SqlQuery::Set(s),
                )
                .await
            }
            // SET [LOCAL] SESSION AUTHORIZATION must reach `query_adhoc_non_select`
            // even inside a transaction (where the proxy path would otherwise
            // forward it without mirroring), so the identity is mirrored into the
            // session after upstream accepts it.
            Ok(SqlQuery::Set(s @ SetStatement::SessionAuthorization(_))) => {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    SqlQuery::Set(s),
                )
                .await
            }
            Ok(ref parsed_query) if Handler::requires_fallback(parsed_query) => {
                if !Handler::return_default_response(parsed_query) && connectors.has_fallback() {
                    if let SqlQuery::Select(stmt) = parsed_query {
                        event.sql_type = SqlQueryType::Read;
                        event.query = Some(Arc::new(parsed_query.clone()));
                        event.query_id = Some(QueryId::from_select(
                            stmt,
                            connectors.noria.schema_search_path(),
                        ));
                    }

                    // Query requires a fallback and we can send it to fallback
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
                } else {
                    // Query should return a default response or requires a fallback, but none is
                    // available
                    Handler::default_response(parsed_query)
                        .map(QueryResult::Noria)
                        .map_err(Into::into)
                }
            }
            Ok(SqlQuery::Select(mut stmt)) => {
                event.sql_type = SqlQueryType::Read;
                if settings.cache_mode.is_shallow() {
                    event.query_id = query_shallow.as_ref().map(QueryId::from);
                    return Self::query_fallback(connectors.upstream.as_mut(), query, event, None)
                        .await;
                }

                let rewrite_context =
                    Self::rewrite_context(connectors, settings, state, None).await?;
                let params = match adapter_rewrites::rewrite_equivalent_deep(
                    &mut stmt,
                    connectors.noria.rewrite_params(),
                    &rewrite_context,
                ) {
                    Ok(params) => params,
                    Err(_) if connectors.has_fallback() => {
                        let result =
                            Self::query_fallback(connectors.upstream.as_mut(), query, event, None)
                                .await;
                        return result;
                    }
                    Err(e) => return Err(e.into()),
                };

                let view_request =
                    ViewCreateRequest::new(stmt, connectors.noria.schema_search_path().to_owned());

                if let Some(QueryLogMode::Verbose) = state.query_log_mode {
                    event.query = Some(Arc::new(SqlQuery::Select(view_request.statement.clone())));
                }
                event.query_id = event.query_id.or(Some(QueryId::from(&view_request)));

                Self::try_noria_adhoc_select(
                    connectors,
                    settings,
                    state,
                    query,
                    view_request,
                    params,
                    rewrite_context.schema_generation(),
                    event,
                    is_skip_cache,
                )
                .await
            }
            Ok(SqlQuery::Deallocate(stmt)) => Ok(Self::handle_deallocate_statement(stmt)),
            Ok(_) if state.proxy_state.should_proxy() => {
                Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
            }
            Ok(parsed_query) => {
                let result = Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    parsed_query.clone(),
                )
                .await;

                if let SqlQuery::DropTable(drop_stmt) = &parsed_query
                    && result.is_ok()
                {
                    state
                        .query_status_cache
                        .invalidate_queries_referencing_tables(&drop_stmt.tables);
                }
                result
            }
        }
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<(QueryResult<'a, DB>, ProxyState), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        let mut query_shallow = None;
        let mut event = QueryExecutionEvent::new(EventType::Query);
        let result = Self::query_inner(
            &mut self.connectors,
            &self.settings,
            &mut self.state,
            query,
            &mut query_shallow,
            &mut event,
        )
        .await;

        Self::update_shallow_support(&self.state, &query_shallow, result.as_ref().err());

        let staged = self.state.pending_proxy_reason.take();
        self.state.last_query = QueryInfo::from_event(&event).map(|i| i.or_reason(staged));

        log_query(
            self.state.query_log_sender.as_ref(),
            event,
            self.settings.slowlog,
            self.settings.dialect,
        );

        result.map(|r| (r, self.state.proxy_state))
    }
}
