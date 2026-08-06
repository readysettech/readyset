//! The shallow cache: whether one can serve a read, creating one on demand, and reading through
//! it.
//!
//! Unlike a deep cache, which Readyset materializes and the adapter reaches over the wire, a
//! shallow cache lives in this process -- so deciding, filling and serving all happen here.
//!
//! Both protocols share the decision, but each has its own read: the simple-query one takes no
//! bound parameters, and the extended-query one merges them with the literals frozen at prepare
//! time, judges an existing entry against the statement's execution metadata, and hands a
//! prepared handle to a background refresh rather than the query text.

use std::sync::Arc;
use std::time::Instant;

use metrics::counter;
use readyset_client::ShallowViewRequest;
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::{MigrationState, QueryId};
use readyset_client_metrics::{QueryDestination, QueryExecutionEvent, ReadysetExecutionEvent};
use readyset_data::DfValue;
use readyset_data::encoding::Encoding;
use readyset_errors::ReadySetError;
use readyset_shallow::{CacheManager, CacheResult};
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{CacheType, CreateCacheOptions, ReadysetHintDirective, TrxCachePolicy};
use readyset_sql_passes::adapter_rewrites::ShallowQueryParameters;
use readyset_sql_passes::shallow::auto_cache_skip_reasons;
use readyset_util::SizeOf;
use tracing::{debug, warn};

use super::routing::{SelectRouter, record_skip_cache};
use super::{
    AutoCreateTrigger, Backend, BackendConnectors, BackendSettings, BackendState, MigrationMode,
    QueryResult, build_hint_ddl_string,
};
use crate::rls_coordinator::RlsCoordinator;
use crate::session_context::SessionContext;
use crate::shallow_key::{SessionInputValues, ShallowKey};
use crate::shallow_refresh_pool::{ShallowRefreshPool, ShallowRefreshRequest};
use crate::upstream_database::UpstreamPrepare;
use crate::{QueryHandler, UpstreamDatabase};

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    /// Check whether a shallow cache exists for this query and should be used for routing.  If no
    /// cache exists and a `CreateCache` hint directive is present, attempt to create one first.
    /// Returns `(query_id, always)` when the query should be served from the shallow cache, `None`
    /// otherwise.
    ///
    /// If we haven't seen this query before, add it as pending to the query status cache.
    pub(super) async fn should_query_shallow(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        shallow: &ShallowViewRequest,
        shallow_orig: &str,
        hint_directive: Option<ReadysetHintDirective>,
    ) -> Option<(QueryId, TrxCachePolicy)> {
        let (query_id, migration) =
            match state.query_status_cache.try_query_migration_state(shallow) {
                (id, Some(migration)) => (id, migration),
                (_, None) => state.query_status_cache.insert(ShallowViewRequest::new(
                    shallow.query.clone(),
                    shallow.schema_search_path.clone(),
                    Some(shallow_orig.to_string()),
                )),
            };

        if matches!(&hint_directive, Some(ReadysetHintDirective::SkipCache)) {
            if migration == MigrationState::Successful(CacheType::Shallow) {
                record_skip_cache(query_id.to_string(), "shallow", "hint");
            }
            return None;
        }
        if migration != MigrationState::Successful(CacheType::Shallow) {
            // No cache yet — try auto-creation (hint-driven or in-request-path).
            let migration = Self::try_auto_create_shallow_cache(
                connectors,
                settings,
                state,
                shallow,
                shallow_orig,
                hint_directive,
            )
            .await;
            if migration != Some(MigrationState::Successful(CacheType::Shallow)) {
                return None;
            }
        }
        let trx_cache_policy = state
            .query_status_cache
            .try_query_status(shallow)
            .map(|status| status.trx_cache_policy)
            .unwrap_or_default();
        if !SelectRouter::may_serve_from_cache(
            state.proxy_state,
            &mut state.write_tracker,
            trx_cache_policy,
            "shallow",
            true,
            || query_id.to_string(),
        ) {
            return None;
        }
        Some((query_id, trx_cache_policy))
    }

    /// Attempt to auto-create a shallow cache via [`create_shallow_cache_core`]
    /// and return the resulting migration state. Two triggers are supported:
    ///
    /// 1. An explicit `/*rs+ CREATE SHALLOW CACHE */` hint directive.
    /// 2. Implicit in-request-path auto-creation when the adapter runs with
    ///    `--query-caching=inrequestpath` and `--cache-mode=shallow`, mirroring
    ///    the `create_if_missing` behaviour on the deep side.
    async fn try_auto_create_shallow_cache(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        shallow: &ShallowViewRequest,
        shallow_orig: &str,
        hint_directive: Option<ReadysetHintDirective>,
    ) -> Option<MigrationState> {
        let (mut opts, trigger) = match hint_directive {
            Some(ReadysetHintDirective::CreateCache(opts)) => {
                let wants_shallow = match opts.cache_type {
                    Some(CacheType::Shallow) => true,
                    Some(CacheType::Deep) => false,
                    None => settings.cache_mode.is_shallow(),
                };
                if !wants_shallow {
                    return None;
                }
                (opts, AutoCreateTrigger::Hint)
            }
            None if settings.migration_mode == MigrationMode::InRequestPath
                && settings.cache_mode.is_shallow() =>
            {
                (
                    CreateCacheOptions::default(),
                    AutoCreateTrigger::InRequestPath,
                )
            }
            _ => return None,
        };

        // Filter implicit in-request-path attempts to prevent driver/ORM
        // bootstrap traffic (system-schema introspection, session variables,
        // non-deterministic functions) from polluting the cache.  Explicit
        // hints opt the user in deliberately and bypass the filter.
        //
        // The skip set in `query_status_cache` is consulted only here, so a
        // remembered rejection cannot block an explicit `CREATE SHALLOW
        // CACHE` DDL or a `/*rs+ CREATE SHALLOW CACHE */` hint.
        let query_id = QueryId::from(shallow);
        if matches!(trigger, AutoCreateTrigger::InRequestPath) {
            if state
                .query_status_cache
                .is_shallow_auto_create_skipped(query_id)
            {
                debug!(
                    "Shallow cache auto-creation skipped: previously rejected by eligibility filter"
                );
                return None;
            }
            let skip_reasons = auto_cache_skip_reasons(
                &shallow.query,
                settings.dialect,
                &settings.shallow_cache_eligibility,
                &state.shallow_cache_allowlists,
            );
            if !skip_reasons.is_empty() {
                let reasons = skip_reasons
                    .iter()
                    .map(|reason| reason.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");
                state
                    .query_status_cache
                    .record_shallow_auto_create_skip(query_id, reasons);
                for reason in &skip_reasons {
                    counter!(metric::SHALLOW_AUTO_CREATE_SKIPPED, "reason" => reason.reason)
                        .increment(1);
                }
                debug!(
                    ?skip_reasons,
                    "Shallow cache auto-creation skipped: query not eligible"
                );
                return None;
            }
        }

        // Caches created automatically (in-request-path or hint with no policy keyword)
        // default to `UntilWrite` so that read-only-so-far transactions can still serve from
        // cache. Hints that explicitly set `ALWAYS` or `UNTIL WRITE` are respected.
        if matches!(opts.trx_cache_policy, TrxCachePolicy::Never) {
            opts.trx_cache_policy = TrxCachePolicy::UntilWrite;
        }

        // Implicit auto-cache creation turns on adaptive refresh; hints configure it
        // explicitly.
        if matches!(trigger, AutoCreateTrigger::InRequestPath) {
            opts.adaptive = true;
        }

        if !settings.allow_cache_ddl {
            warn!(
                trigger = trigger.as_str(),
                "Shallow cache auto-creation skipped: cache DDL is disabled"
            );
            return None;
        }

        if let Err(error) = connectors.upstream_supports(shallow_orig).await {
            warn!(
                trigger = trigger.as_str(),
                %error,
                "Shallow cache auto-creation failed: upstream unsupported"
            );
            return None;
        }

        let (query_id, name) = Self::resolve_id_and_name(None, query_id);
        let query_text = shallow.query.display(DB::SQL_DIALECT).to_string();
        let ddl_stmt = build_hint_ddl_string(DB::SQL_DIALECT, &opts, &query_text);
        let ddl_req = CacheDDLRequest {
            unparsed_stmt: ddl_stmt,
            schema_search_path: connectors.noria.schema_search_path().to_owned(),
            dialect: settings.dialect.into(),
            cache_name: None,
        };

        match Self::create_shallow_cache_core(
            settings,
            state,
            query_id,
            name,
            shallow,
            opts.policy,
            opts.trx_cache_policy,
            opts.coalesce_ms,
            opts.adaptive,
            ddl_req,
            true,
        )
        .await
        {
            Ok(()) | Err(ReadySetError::ViewAlreadyExists(_)) => {}
            Err(e) => {
                warn!(trigger = trigger.as_str(), error = %e, "Shallow cache auto-creation failed");
            }
        }

        state
            .query_status_cache
            .try_query_migration_state(shallow)
            .1
    }

    pub(super) async fn query_shallow<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        state: &BackendState<DB>,
        query: &'a str,
        query_id: QueryId,
        event: &mut QueryExecutionEvent,
        params: ShallowQueryParameters,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let params_key = params.make_keys(&[])?;
        let start = Instant::now();

        let mut session_values = SessionInputValues::default();
        if let Some(coordinator) = state.rls_coordinator.as_ref()
            && coordinator
                .fill_rls_session_inputs(
                    &query_id,
                    connectors.session.as_ref().map(|s| s.as_ref()),
                    &mut session_values,
                )
                .is_err()
        {
            return Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await;
        }
        let shallow_key = ShallowKey {
            params: params_key,
            session: session_values,
            charset: connectors.noria.results_encoding(),
        };

        // An entry keyed on session state does not refresh via the
        // session-less pool, which has no session to resolve those values.
        let session_keyed = !shallow_key.session.is_empty();

        let res = state
            .shallow
            .get_or_start_insert(&query_id, shallow_key, |_| true)
            .await;

        let cache_name = state
            .shallow
            .get(None, Some(&query_id))
            .and_then(|cache| cache.display_name());

        match res {
            CacheResult::Hit(values) => {
                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::HitAndRefresh(values, cache) => {
                if let (false, Some(refresh)) = (session_keyed, state.shallow_refresh_pool.as_ref())
                {
                    let request = ShallowRefreshRequest {
                        query_id,
                        path: connectors.noria.schema_search_path().to_vec(),
                        query: query.to_string(),
                        cache,
                        shallow_exec_meta: None,
                    };
                    refresh.send(request).await;
                }

                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::Miss(mut cache) => {
                if let (false, Some(refresh)) = (session_keyed, state.shallow_refresh_pool.as_ref())
                    && cache.is_scheduled()
                {
                    let refresh = refresh.clone();
                    let path = connectors.noria.schema_search_path().to_vec();
                    let q = query.to_string();
                    let callback = Arc::new(move |cache| {
                        let req = ShallowRefreshRequest::<DB::CacheEntry, DB::ShallowExecMeta> {
                            query_id,
                            path: path.clone(),
                            query: q.clone(),
                            cache,
                            shallow_exec_meta: None,
                        };
                        refresh.spawn_send(req);
                    });
                    cache.schedule_refresh(callback).await;
                };
                Self::query_fallback(connectors.upstream.as_mut(), query, event, Some(cache)).await
            }
            CacheResult::NotCached => Err(ReadySetError::NoCacheForQuery.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_shallow<'a>(
        upstream: &'a mut DB,
        shallow: &Arc<CacheManager<ShallowKey, DB::CacheEntry>>,
        coordinator: Option<&Arc<RlsCoordinator<DB::CacheEntry>>>,
        session: Option<&Arc<SessionContext>>,
        prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
        exec_meta: &'a DB::ExecMeta,
        event: &mut QueryExecutionEvent,
        query_id: &QueryId,
        query_params: &ShallowQueryParameters,
        refresh: Option<&Arc<ShallowRefreshPool<DB>>>,
        view_request: &ShallowViewRequest,
        results_encoding: Encoding,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let merged = query_params.merge_params(params)?.unwrap_or_default();
        let params_key = query_params.make_keys_from_merged(&merged)?;
        let start = Instant::now();

        // Assemble the key's session half. With no coordinator (RLS
        // disabled) it stays empty and every cache is plain. A scoped
        // cache whose session values cannot be resolved safely refuses;
        // we then serve from upstream uncached rather than risk a
        // cross-tenant entry.
        let mut session_values = SessionInputValues::default();
        if let Some(coordinator) = coordinator
            && coordinator
                .fill_rls_session_inputs(query_id, session.map(|s| s.as_ref()), &mut session_values)
                .is_err()
        {
            let shallow_exec_meta = upstream.shallow_exec_meta(exec_meta).await?;
            return Self::execute_upstream(
                upstream,
                prep,
                params,
                exec_meta,
                Some(&shallow_exec_meta),
                event,
                false,
                None,
            )
            .await;
        }
        let shallow_key = ShallowKey {
            params: params_key,
            session: session_values,
            charset: results_encoding,
        };

        // An entry keyed on session state must not refresh through the
        // session-less pool: a refresh worker has no session to resolve
        // those values, so it would refill under a stale or bypass key.
        // Serve such a hit without scheduling a refresh.
        let session_keyed = !shallow_key.session.is_empty();

        let res = shallow
            .get_or_start_insert(query_id, shallow_key, DB::is_meta_compatible)
            .await;

        let cache_name = shallow
            .get(None, Some(query_id))
            .and_then(|cache| cache.display_name());

        match res {
            CacheResult::Hit(values) => {
                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::HitAndRefresh(values, cache) => {
                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                if let (false, Some(refresh)) = (session_keyed, refresh) {
                    let shallow_exec_meta = upstream.shallow_exec_meta(exec_meta).await.ok();
                    let query =
                        query_params.literalize_from_merged(&view_request.query, &merged)?;

                    let request = ShallowRefreshRequest {
                        query_id: *query_id,
                        path: view_request.schema_search_path.clone(),
                        query,
                        cache,
                        shallow_exec_meta,
                    };
                    refresh.send(request).await;
                }

                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::Miss(mut cache) => {
                let query = query_params.literalize_from_merged(&view_request.query, &merged)?;
                let shallow_exec_meta = upstream.shallow_exec_meta(exec_meta).await?;

                if let (false, Some(refresh)) = (session_keyed, refresh)
                    && cache.is_scheduled()
                {
                    let callback = {
                        let query_id = *query_id;
                        let path = view_request.schema_search_path.clone();
                        let query = query.clone();
                        let shallow_exec_meta = shallow_exec_meta.clone();
                        let refresh = refresh.clone();

                        Arc::new(move |cache| {
                            let request = ShallowRefreshRequest {
                                query_id,
                                path: path.clone(),
                                query: query.clone(),
                                cache,
                                shallow_exec_meta: Some(shallow_exec_meta.clone()),
                            };
                            refresh.spawn_send(request);
                        })
                    };
                    cache.schedule_refresh(callback).await;
                }

                Self::execute_upstream(
                    upstream,
                    prep,
                    params,
                    exec_meta,
                    Some(&shallow_exec_meta),
                    event,
                    false,
                    Some(cache),
                )
                .await
            }
            CacheResult::NotCached => Err(ReadySetError::NoCacheForQuery.into()),
        }
    }

    pub(super) fn update_shallow_support(
        state: &BackendState<DB>,
        shallow: &Option<ShallowViewRequest>,
        error: Option<&DB::Error>,
    ) {
        if let Some(shallow) = shallow {
            state
                .query_status_cache
                .with_mut_migration_state(shallow, |state| {
                    if state.is_proxied() {
                        *state = match error {
                            None => MigrationState::Supported,
                            Some(e) => MigrationState::Unsupported(e.to_string()),
                        }
                    }
                });
        }
    }
}
