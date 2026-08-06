//! Readyset's SQL extensions: `CREATE CACHE`, `DROP CACHE`, `EXPLAIN ...`, `SHOW ...` and
//! `ALTER READYSET ...`, along with the MCP-token and user-management statements that share
//! their dispatch.
//!
//! [`Backend::query_readyset_extensions`] is the entry point; every other function here serves
//! it. These statements are answered by the adapter itself rather than being planned as reads
//! or proxied upstream, so they share nothing with the query path but the session state they
//! report on.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use database_utils::DatabaseURL;
use readyset_client::consensus::mcp_tokens::McpTokenStore;
use readyset_client::consensus::mcp_tokens::{McpToken, McpTokenScope as AuthorityMcpTokenScope};
use readyset_client::consensus::{Authority, AuthorityControl, CacheDDLRequest, UserStore};
use readyset_client::post_processing::Results;
use readyset_client::recipe::CacheExpr;
use readyset_client::schema::{ColumnSchema, SelectSchema};
use readyset_client::status::CacheProperties;
use readyset_client::{CacheMode, PlaceholderIdx, ShallowViewRequest, ViewCreateRequest, query::*};
use readyset_client_metrics::{
    QueryDestination, QueryExecutionEvent, ReadysetExecutionEvent, SqlQueryType,
};
use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetError, ReadySetResult, internal, internal_err, unsupported};
use readyset_metrics::metrics_handle;
use readyset_rls::InvalidationSink;
use readyset_shallow::CacheInfo;
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{
    self, AddUserStatement, AlterMcpTokenStatement, AlterReadysetStatement, CacheInner, CacheType,
    ChangeCdcStatement, ChangeUpstreamStatement, CreateCacheStatement, CreateMcpTokenStatement,
    DropAllCachesStatement, DropMcpTokenStatement, DropUserStatement, ExplainStatement,
    FlushCacheStatement, McpTokenExpiresChange, McpTokenScope as ParserMcpTokenScope,
    ModifyUserStatement, ProxiedQueriesOptions, Relation, ShallowCacheAllowlistChange,
    ShallowCacheAllowlistKind, ShowStatement, SqlQuery, TrxCachePolicy,
};
use readyset_sql_passes::shallow::rewrite_shallow;
use readyset_sql_passes::{DetectBucketFunctions, adapter_rewrites};
use readyset_telemetry_reporter::TelemetryEvent;
use readyset_util::SizeOf;
use readyset_util::redacted::{RedactedString, Sensitive};
use readyset_util::retry_with_exponential_backoff;
use schema_catalog::SchemaGeneration;
use tracing::{error, info, trace, warn};
use vec1::Vec1;

use super::noria_connector::{self, MetaVariable};
use super::{
    Backend, BackendConnectors, BackendSettings, BackendState, UNSUPPORTED_CACHE_DDL_MSG,
    readyset_version, resolve_coalesce, resolve_eviction_policy,
};
use crate::query_status_cache::ManualCacheEntry;
use crate::utils::create_dummy_column;
use crate::{QueryHandler, UpstreamDatabase, create_dummy_schema};

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    pub(super) fn resolve_id_and_name(
        name: Option<Relation>,
        query_id: QueryId,
    ) -> (QueryId, Relation) {
        let name = name.unwrap_or_else(|| query_id.into());
        (query_id, name)
    }

    /// Forwards a `CREATE CACHE` request to ReadySet
    #[allow(clippy::too_many_arguments)]
    async fn create_cached_query(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        name: Relation,
        query_id: QueryId,
        deep: ViewCreateRequest,
        shallow: ReadySetResult<ShallowViewRequest>,
        trx_cache_policy: TrxCachePolicy,
        concurrently: bool,
        topk_buffer_multiplier: Option<usize>,
        schema_generation: SchemaGeneration,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        // If we have existing caches with the same query_id or name, drop them first.
        Self::drop_caches_on_collision(connectors, settings, state, Some(query_id), Some(&name))
            .await?;
        if let Ok(shallow) = shallow {
            Self::drop_caches_on_collision(
                connectors,
                settings,
                state,
                Some(QueryId::from(&shallow)),
                None,
            )
            .await?;
        }

        // Now migrate the new query
        let migration_state = match connectors
            .noria
            .handle_create_cached_query(
                Some(&name),
                deep.clone(),
                trx_cache_policy,
                concurrently,
                topk_buffer_multiplier,
                schema_generation,
            )
            .await
        {
            Ok(None) => MigrationState::Successful(CacheType::Deep),
            Ok(Some(id)) => {
                return Ok(noria_connector::QueryResult::Meta(vec![
                    ("Migration Id".to_string(), id.to_string()).into(),
                ]));
            }
            // If the query fails because it contains unsupported placeholders, then mark it as an
            // inlined query in the query status cache.
            Err(e) => {
                if let Some(placeholders) = e.unsupported_placeholders_cause() {
                    let placeholders = Vec1::try_from(
                        placeholders
                            .into_iter()
                            .map(|p| p as PlaceholderIdx)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
                    if settings.placeholder_inlining {
                        MigrationState::Inlined(InlinedState::from_placeholders(placeholders))
                    } else {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
            }
        };
        state
            .query_status_cache
            .update_query_migration_state(&deep, migration_state, None);
        state
            .query_status_cache
            .set_trx_cache_policy(&deep, trx_cache_policy);
        let query = Self::format_query_text(deep.statement.display(DB::SQL_DIALECT).to_string());
        Ok(Self::create_cache_result(
            query_id,
            &name,
            query,
            CacheType::Deep,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_deep_cache(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        name: Option<Relation>,
        deep: ReadySetResult<ViewCreateRequest>,
        shallow: ReadySetResult<ShallowViewRequest>,
        trx_cache_policy: TrxCachePolicy,
        concurrently: bool,
        topk_buffer_multiplier: Option<usize>,
        schema_generation: SchemaGeneration,
        manual_mapping: Option<ManualMappingInfo>,
        mut ddl_req: Option<CacheDDLRequest>,
        quiet: bool,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let deep = deep?;
        let (query_id, name) = Self::resolve_id_and_name(name, QueryId::from(&deep));

        // A manually parameterized cache owns its standard (fully autoparameterized) shape
        // exclusively: reject the creation up front when a different manual cache already
        // claims it, so incoming SELECTs are never routed ambiguously.
        let manual_registration = manual_mapping
            .map(|info| {
                let lookup_id = QueryId::from(&info.lookup);
                let entry = ManualCacheEntry {
                    name: name.clone(),
                    manual: deep.clone(),
                    frozen: info.frozen,
                };
                match state.query_status_cache.manual_cache(&lookup_id) {
                    Some(existing) if existing.manual != entry.manual => {
                        Err(ReadySetError::CreateCacheError(format!(
                            "manually parameterized cache {} already serves this query's \
                             auto-parameterized shape. DROP CACHE {}, or add more parameters \
                             so the shapes differ",
                            existing.name.display_unquoted(),
                            existing.name.display_unquoted(),
                        )))
                    }
                    _ => Ok((lookup_id, entry)),
                }
            })
            .transpose()?;

        if let Some(req) = &mut ddl_req {
            req.cache_name = Some(name.clone());
        }
        if let Some(ref ddl_req) = ddl_req {
            state
                .authority
                .add_cache_ddl_request(ddl_req.clone())
                .await?;
        }

        let res = Self::create_cached_query(
            connectors,
            settings,
            state,
            name.clone(),
            query_id,
            deep,
            shallow,
            trx_cache_policy,
            concurrently,
            topk_buffer_multiplier,
            schema_generation,
        )
        .await;

        if res.is_ok()
            && let Some((lookup_id, entry)) = manual_registration
            && let Err(existing) = state
                .query_status_cache
                .insert_manual_cache(lookup_id, entry)
        {
            // Pre-checked above; only a concurrent CREATE CACHE can race us here.
            warn!(
                existing = %existing.name.display_unquoted(),
                "manual cache mapping already claimed concurrently"
            );
        }

        remove_ddl_on_error(
            &res,
            &state.authority,
            ddl_req,
            Some(name),
            "deep",
            |auth, req| async move { auth.remove_cache_ddl_request(req).await },
            quiet,
        )
        .await;

        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_shallow_cache(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        name: Option<Relation>,
        deep: ReadySetResult<ViewCreateRequest>,
        shallow: ReadySetResult<ShallowViewRequest>,
        policy: Option<ast::EvictionPolicy>,
        ddl_req: Option<CacheDDLRequest>,
        trx_cache_policy: TrxCachePolicy,
        coalesce_ms: Option<Duration>,
        adaptive: bool,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let ddl_req =
            ddl_req.ok_or_else(|| internal_err!("No statement supplied to shallow cache"))?;

        let shallow = shallow?;
        if let Err(e) = connectors
            .upstream_supports(&shallow.original_query(settings.dialect))
            .await
        {
            return Err(ReadySetError::CreateCacheError(e.to_string()));
        }

        // DDL-specific: drop collisions before creating.
        let (query_id, name) = Self::resolve_id_and_name(name, QueryId::from(&shallow));
        Self::drop_caches_on_collision(connectors, settings, state, Some(query_id), Some(&name))
            .await?;
        if let Ok(deep) = deep {
            Self::drop_caches_on_collision(
                connectors,
                settings,
                state,
                Some(QueryId::from(&deep)),
                None,
            )
            .await?;
        }

        // Propagate upstream-validation and DDL-persistence errors to the
        // caller. ViewAlreadyExists from a concurrent race is not a real
        // failure — treat it the same as success.
        match Self::create_shallow_cache_core(
            settings,
            state,
            query_id,
            name.clone(),
            &shallow,
            policy,
            trx_cache_policy,
            coalesce_ms,
            adaptive,
            ddl_req,
            false,
        )
        .await
        {
            Ok(()) | Err(ReadySetError::ViewAlreadyExists(_)) => {
                let query =
                    Self::format_query_text(shallow.query.display(DB::SQL_DIALECT).to_string());
                Ok(Self::create_cache_result(
                    query_id,
                    &name,
                    query,
                    CacheType::Shallow,
                ))
            }
            Err(e) => Err(e),
        }
    }

    /// Shared creation logic for shallow caches: DDL persistence,
    /// `shallow.create_cache()`, status updates, and error cleanup.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn create_shallow_cache_core(
        settings: &BackendSettings,
        state: &BackendState<DB>,
        query_id: QueryId,
        name: Relation,
        shallow: &ShallowViewRequest,
        policy: Option<ast::EvictionPolicy>,
        trx_cache_policy: TrxCachePolicy,
        coalesce_ms: Option<Duration>,
        adaptive: bool,
        mut ddl_req: CacheDDLRequest,
        quiet: bool,
    ) -> ReadySetResult<()> {
        ddl_req.cache_name = Some(name.clone());

        // RLS analyzer gate, active only when a policy registry is
        // wired (RLS enabled). Resolves the cacheability verdict
        // against the registry; a `Refuse` returns the typed reason so
        // EXPLAIN CACHE SUPPORT can surface it. An unresolved relation
        // is fail-closed: the analyzer cannot decide RLS status against
        // a table the registry has not yet observed, so we refuse the
        // cache rather than fall through to a `Plain` backing. With
        // RLS disabled every cache is created Plain.
        // The relations and RLS analysis to register with the coordinator
        // after the cache is created. `None` means RLS is disabled (no
        // registry); the cache is plain and not coordinator-tracked.
        let mut rls_registration: Option<(Vec<readyset_rls::Oid>, readyset_rls::CacheSessionDeps)> =
            None;
        if let Some(registry) = &state.policy_registry {
            let referenced_relations = match crate::rls_relations::extract_referenced_relation_oids(
                &shallow.query,
                registry,
                &shallow.schema_search_path,
            ) {
                Ok(oids) => oids,
                Err(unknown) => {
                    let joined: Vec<String> = unknown.iter().map(|u| u.qualified()).collect();
                    return Err(ReadySetError::Internal(format!(
                        "rls_uncacheable[code=unknown_relation, relations={}]",
                        joined.join(",")
                    )));
                }
            };
            let analysis = readyset_rls::analyze_cache(registry, &referenced_relations);
            if let readyset_rls::Cacheability::Refuse(reason) = &analysis.cacheability {
                // Structured form (RLS-20): downstream EXPLAIN CACHE
                // SUPPORT / MCP tooling switches on `reason.code()`
                // rather than parsing the human-readable text.
                return Err(ReadySetError::Internal(reason.structured_display()));
            }
            // Track every query-referenced relation, not only the
            // currently RLS-active ones, so RLS-6 lifecycle catches a
            // Plain cache when relrowsecurity later flips true on a
            // table the cache references. Also include the analyzer's
            // expanded RLS tables: for a view query the referenced relation
            // is the view, but invalidation must key on the underlying base
            // tables the policies live on, or a base-table policy change
            // would never invalidate the view's cache.
            let mut relations: Vec<readyset_rls::Oid> = referenced_relations;
            for &t in analysis.rls_active_for_tables.iter() {
                if !relations.contains(&t) {
                    relations.push(t);
                }
            }
            rls_registration = Some((relations, analysis));
        }

        state
            .authority
            .add_shallow_cache_ddl_request(ddl_req.clone())
            .await?;

        let res = state.shallow.create_cache(
            Some(name.clone()),
            query_id,
            shallow.query.clone(),
            shallow.schema_search_path.clone(),
            resolve_eviction_policy(policy, settings.default_ttl_ms),
            ddl_req.clone(),
            trx_cache_policy,
            resolve_coalesce(coalesce_ms, settings.default_coalesce_ms),
            adaptive,
        );

        match &res {
            Ok(()) | Err(ReadySetError::ViewAlreadyExists(_)) => {
                // Success or concurrent creation race — update status cache
                // either way. ViewAlreadyExists is not a real failure: the
                // cache exists, so we must NOT remove the DDL.

                // Register the freshly-created cache with the coordinator.
                // An RLS-active analysis registers a scoped descriptor; a
                // plain cache that still references RLS-eligible relations
                // registers relation-only so `on_rls_flag_enabled` can find
                // it when its relation later turns RLS-active.
                if matches!(res, Ok(()))
                    && let Some(coordinator) = &state.rls_coordinator
                    && let Some((relations, analysis)) = &rls_registration
                {
                    if analysis.rls_active_for_tables.is_empty() {
                        coordinator.register_relations(query_id, relations.clone());
                    } else {
                        coordinator.register_scoped(
                            query_id,
                            analysis.session_rls_inputs.clone(),
                            relations.clone(),
                        );
                    }

                    // Close the analyze->register race: the analyzer captured
                    // `analysis.snapshot_generation`, but a catalog poll reload
                    // can bump the generation and run its invalidation pass
                    // before this cache entered the reverse index, skipping it.
                    // If the generation advanced, re-apply the missed pass over
                    // every tracked relation now that the cache is registered,
                    // mirroring the poller's dispatch: a policy change refreshes
                    // a scoped descriptor, and a relation that turned RLS-active
                    // drops a plain cache that would otherwise serve one tenant's
                    // rows to all. Iterating only `rls_active_for_tables` would
                    // miss the plain-cache case, whose set is empty.
                    if let Some(registry) = &state.policy_registry
                        && registry.generation() != analysis.snapshot_generation
                    {
                        for &relid in relations.iter() {
                            coordinator.on_relation_changed(relid);
                            let now_rls_active = registry
                                .flags_for(relid)
                                .map(|f| f.relrowsecurity)
                                .unwrap_or(false);
                            if now_rls_active {
                                coordinator.on_rls_flag_enabled(relid);
                            }
                        }
                    }
                }

                state.query_status_cache.update_query_migration_state(
                    shallow,
                    MigrationState::Successful(CacheType::Shallow),
                    None,
                );
                state
                    .query_status_cache
                    .set_trx_cache_policy(shallow, trx_cache_policy);
            }
            Err(_) => {
                remove_ddl_on_error(
                    &res,
                    &state.authority,
                    Some(ddl_req),
                    Some(name),
                    "shallow",
                    |auth, req| async move { auth.remove_shallow_cache_ddl_request(req).await },
                    quiet,
                )
                .await;
            }
        }

        res
    }

    /// Extract any requested cache type from the EXPLAIN statement.
    fn requested_cache_type(explain: &ExplainStatement) -> ReadySetResult<Option<CacheType>> {
        let ExplainStatement::CreateCache { cache_type, .. } = explain else {
            internal!("Unexpected EXPLAIN: {explain:?}");
        };
        Ok(*cache_type)
    }

    /// Extract the deep and shallow representations of the query. When autoparameterization is
    /// suppressed or scoped (`AUTOPARAM OFF` / `AUTOPARAM (EXCLUDE_*)`), also computes the
    /// [`ManualMappingInfo`] that routes incoming SELECTs (which still autoparameterize fully)
    /// to the manually parameterized cache.
    #[allow(clippy::type_complexity)]
    async fn query_from_cache_inner(
        connectors: &BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        inner: &CacheInner,
        autoparam: ast::AutoparamControl,
    ) -> ReadySetResult<(
        ReadySetResult<ViewCreateRequest>,
        ReadySetResult<ShallowViewRequest>,
        SchemaGeneration,
        Option<ManualMappingInfo>,
    )> {
        match inner {
            CacheInner::Statement { deep, shallow } => {
                let deep = deep.clone();
                let shallow = shallow.clone();

                // Rewrite for deep.
                let rewrite_context =
                    Self::rewrite_context(connectors, settings, state, None).await?;
                let schema_generation = rewrite_context.schema_generation();
                let mut manual_mapping = None;
                let deep = if settings.cache_mode.is_shallow() {
                    Err(ReadySetError::Unsupported("shallow-only mode".into()))
                } else {
                    // AUTOPARAM OFF turns off the autoparameterization pass for this cache so
                    // it's built with exactly the placeholders the user wrote.
                    let mut rewrite_params = connectors.noria.rewrite_params();
                    rewrite_params.autoparameterize = !autoparam.off;
                    match deep {
                        Ok(mut deep) => {
                            // Incoming SELECTs hash to the standard (fully autoparameterized)
                            // form, so keep a pre-rewrite copy to compute that form alongside
                            // the manual one.
                            let standard_src = (!autoparam.is_default()).then(|| (*deep).clone());
                            // EXCLUDE_* scopes mark their literals before the rewrite pipeline
                            // hoists them out of their clause of origin.
                            adapter_rewrites::wrap_autoparam_exclusions(&mut deep, &autoparam);
                            match adapter_rewrites::rewrite_query(
                                &mut deep,
                                rewrite_params,
                                &rewrite_context,
                            ) {
                                Ok(_params) => {
                                    if let Some(mut standard) = standard_src {
                                        let params = adapter_rewrites::rewrite_query(
                                            &mut standard,
                                            connectors.noria.rewrite_params(),
                                            &rewrite_context,
                                        )?;
                                        let frozen =
                                            adapter_rewrites::derive_frozen(&standard, &deep)?;
                                        // With nothing frozen the two forms agree and the
                                        // regular lookup path already finds the cache.
                                        if !frozen.is_empty() {
                                            if params.has_rewritten_in_conditions() {
                                                unsupported!(
                                                    "AUTOPARAM is not supported for queries \
                                                     whose IN clauses would be autoparameterized"
                                                );
                                            }
                                            if standard.limit_clause != deep.limit_clause {
                                                unsupported!(
                                                    "AUTOPARAM is not supported when LIMIT or \
                                                     OFFSET would be autoparameterized"
                                                );
                                            }
                                            manual_mapping = Some(ManualMappingInfo {
                                                lookup: ViewCreateRequest::new(
                                                    standard,
                                                    rewrite_context.search_path().to_owned(),
                                                ),
                                                frozen,
                                            });
                                        }
                                    }
                                    Ok(ViewCreateRequest::new(
                                        *deep,
                                        rewrite_context.search_path().to_owned(),
                                    ))
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(ReadySetError::UnparseableQuery(e)),
                    }
                };

                // Rewrite for shallow, first rendering a copy of the AST as plaintext before the
                // rewrite potentially puts placeholders in places the upstream doesn't support.
                let shallow = match shallow {
                    Ok(mut shallow) => {
                        let shallow_orig = shallow.display(settings.dialect).to_string();
                        rewrite_shallow(&mut shallow, connectors.noria.rewrite_params())?;
                        Ok(ShallowViewRequest::new(
                            *shallow,
                            connectors.noria.schema_search_path().to_owned(),
                            Some(shallow_orig),
                        ))
                    }
                    Err(e) => Err(ReadySetError::UnparseableQuery(e)),
                };

                Ok((deep, shallow, schema_generation, manual_mapping))
            }
            CacheInner::Id(id) => match state
                .query_status_cache
                .query_with_schema_generation(id.as_str())
            {
                Some((q, schema_gen)) => match q {
                    Query::Parsed(deep) => {
                        // Deep queries must have a stored generation from rewrite time;
                        // missing generation here is a programming error since all deep
                        // queries go through query_migration_state during prepare.
                        let Some(generation) = schema_gen else {
                            internal!("deep query {id} in QSC without schema_generation")
                        };
                        Ok((
                            Ok((*deep).clone()),
                            Err(ReadySetError::NoQueryForId { id: id.to_string() }),
                            generation,
                            None,
                        ))
                    }
                    Query::ShallowParsed(shallow) => {
                        // Shallow queries are schema-insensitive; generation is unused
                        // by the shallow path but we need to return something. Use
                        // INITIAL since the shallow create_cache path ignores it.
                        let generation = schema_gen.unwrap_or(SchemaGeneration::INITIAL);
                        Ok((
                            Err(ReadySetError::NoQueryForId { id: id.to_string() }),
                            Ok((*shallow).clone()),
                            generation,
                            None,
                        ))
                    }
                    Query::ParseFailed(_, e) => Err(ReadySetError::UnparseableQuery(e)),
                },
                None => Err(ReadySetError::NoQueryForId { id: id.to_string() }),
            },
        }
    }

    /// Extract the deep and shallow representations of the query from the EXPLAIN.
    async fn query_from_explain(
        connectors: &BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        explain: &ExplainStatement,
    ) -> ReadySetResult<(
        ReadySetResult<ViewCreateRequest>,
        ReadySetResult<ShallowViewRequest>,
        SchemaGeneration,
    )> {
        let ExplainStatement::CreateCache { inner, .. } = explain else {
            internal!("Unexpected EXPLAIN: {explain:?}");
        };

        let (deep, shallow, schema_generation, _) =
            Self::query_from_cache_inner(connectors, settings, state, inner, Default::default())
                .await?;
        Ok((deep, shallow, schema_generation))
    }

    // Determine the migration state of the deep representation, performing a dry run if necessary.
    async fn explain_migration_state(
        connectors: &mut BackendConnectors<DB>,
        state: &BackendState<DB>,
        deep: &ReadySetResult<ViewCreateRequest>,
        cache_mode: CacheMode,
        cache_type: Option<CacheType>,
        schema_generation: SchemaGeneration,
    ) -> MigrationState {
        let deep = match deep {
            Ok(deep) => deep,
            Err(e) => {
                return MigrationState::Unsupported(e.to_string());
            }
        };

        // Check if we already know the migration state for this query.
        let (id, migration_state) = state.query_status_cache.try_query_migration_state(deep);

        // Alternatively ask the controller if it knows about this query.
        let migration_state = match migration_state {
            Some(migration_state) => migration_state,
            None => {
                if connectors
                    .noria
                    .get_view_name(deep.clone())
                    .await
                    .is_ok_and(|r| r.is_some())
                {
                    MigrationState::Successful(CacheType::Deep)
                } else {
                    MigrationState::Pending
                }
            }
        };

        // If we already know the migration state, return it.
        if migration_state != MigrationState::Pending {
            return migration_state;
        }

        // If a shallow cache was explicitly requested, just return the migration state we have.
        if cache_type == Some(CacheType::Shallow) {
            return migration_state;
        }

        // The default cache mode won't consider deep, and no one asked for deep.
        if cache_mode == CacheMode::Shallow && cache_type != Some(CacheType::Deep) {
            return migration_state;
        }

        // We don't yet know the migration state and are considering a deep cache.
        match connectors
            .noria
            .handle_dry_run(id, deep, schema_generation)
            .await
        {
            Ok(()) => MigrationState::Supported,
            Err(e) if e.is_transient() => MigrationState::Pending,
            Err(e) => {
                MigrationState::Unsupported(e.unsupported_cause().unwrap_or_else(|| e.to_string()))
            }
        }
    }

    fn output_explain_create_cache(
        query_id: QueryId,
        query: String,
        supported: &str,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        Ok(noria_connector::QueryResult::Meta(vec![
            MetaVariable {
                name: "query_id".into(),
                value: query_id.to_string(),
            },
            MetaVariable {
                name: "query".into(),
                value: query,
            },
            MetaVariable {
                name: "readyset_supported".into(),
                value: supported.into(),
            },
        ]))
    }

    /// Process an EXPLAIN CREATE CACHE request.
    ///
    /// If necessary, first perform a dry run migration.  If the migration state is inlined, allow
    /// the migration handler to advance the query's processing in the background.  A result of
    /// pending indicates that the caller should try again later.
    async fn explain_create_cache(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        explain: &ExplainStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let cache_mode = settings.cache_mode;
        let cache_type = Self::requested_cache_type(explain)?;

        // Get the deep and shallow representations of the query.
        let (deep, shallow, schema_generation) =
            Self::query_from_explain(connectors, settings, state, explain).await?;

        // The only time we care about the migration state of a shallow representation is if we've
        // marked one as cached in the query status cache.
        if let Ok(shallow) = &shallow
            && let (query_id, Some(MigrationState::Successful(CacheType::Shallow))) =
                state.query_status_cache.try_query_migration_state(shallow)
        {
            let query = shallow.query.display(settings.dialect).to_string();
            return Self::output_explain_create_cache(query_id, query, "cached");
        }

        // Determine support.
        let migration_state = Self::explain_migration_state(
            connectors,
            state,
            &deep,
            cache_mode,
            cache_type,
            schema_generation,
        )
        .await;
        match cache_type {
            Some(CacheType::Deep) => {
                let deep = deep?;
                let supported = match migration_state {
                    MigrationState::Successful(..) => "cached",
                    MigrationState::Supported => "yes",
                    MigrationState::Unsupported(ref e) => &format!("no: {e}"),
                    MigrationState::Inlined(..) | MigrationState::Pending => "pending",
                };

                let query = deep.statement.display(settings.dialect).to_string();
                Self::output_explain_create_cache(QueryId::from(&deep), query, supported)
            }
            Some(CacheType::Shallow) => {
                let shallow = shallow?;
                let supported = if let Err(e) = connectors
                    .upstream_supports(&shallow.original_query(settings.dialect))
                    .await
                {
                    &format!("no: {e}")
                } else {
                    "yes"
                };

                let query = shallow.query.display(settings.dialect).to_string();
                Self::output_explain_create_cache(QueryId::from(&shallow), query, supported)
            }
            None => {
                let defaults_deep = cache_mode.defaults_deep();
                let (deep, shallow, supported): (_, _, &str) = match migration_state {
                    MigrationState::Successful(..) => (Some(deep?), None, "cached"),
                    MigrationState::Inlined(..) | MigrationState::Pending if defaults_deep => {
                        (Some(deep?), None, "pending")
                    }
                    MigrationState::Supported if defaults_deep => (Some(deep?), None, "yes"),
                    MigrationState::Unsupported(ref e) if cache_mode.is_deep() => {
                        (Some(deep?), None, &format!("no: {e}"))
                    }
                    MigrationState::Inlined(..)
                    | MigrationState::Pending
                    | MigrationState::Supported
                    | MigrationState::Unsupported(..) => {
                        let shallow = shallow?;
                        if let Err(e) = connectors
                            .upstream_supports(&shallow.original_query(settings.dialect))
                            .await
                        {
                            (None, Some(shallow), &format!("no: {e}"))
                        } else {
                            (None, Some(shallow), "yes")
                        }
                    }
                };

                let (query_id, query) = match (deep, shallow) {
                    (Some(deep), None) => (
                        QueryId::from(&deep),
                        deep.statement.display(settings.dialect).to_string(),
                    ),
                    (None, Some(shallow)) => (
                        QueryId::from(&shallow),
                        shallow.query.display(settings.dialect).to_string(),
                    ),
                    _ => internal!("Expected either deep or shallow AST"),
                };

                Self::output_explain_create_cache(query_id, query, supported)
            }
        }
    }

    /// Forwards a `DROP CACHE` request to noria
    async fn drop_cached_query(
        connectors: &mut BackendConnectors<DB>,
        state: &mut BackendState<DB>,
        name: &Relation,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let maybe_view_request = connectors.noria.view_create_request_from_name(name).await;
        let result = connectors.noria.drop_view(name).await?;
        if let Some(view_request) = maybe_view_request {
            state.drop_view_request(&view_request);
        }
        // A dropped manually parameterized cache must stop capturing its standard shape.
        state.query_status_cache.remove_manual_cache_by_name(name);
        Ok(noria_connector::QueryResult::Delete {
            num_rows_deleted: result,
        })
    }

    /// Flush all shallow caches: clears cached data but preserves cache definitions,
    /// schedulers, query_id mappings, and all metadata. Deep caches, query status,
    /// and prepared statements are not affected.
    async fn flush_all_shallow_caches(
        state: &mut BackendState<DB>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        state.shallow.flush_all_caches().await;
        if let Some(ref telemetry_sender) = state.telemetry_sender
            && let Err(e) = telemetry_sender.send_event(TelemetryEvent::FlushAllShallowCaches)
        {
            warn!(error = %e, "Failed to send FLUSH ALL SHALLOW CACHES telemetry");
        }
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Flush a single shallow cache by name.
    async fn flush_shallow_cache(
        state: &mut BackendState<DB>,
        stmt: &FlushCacheStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        state.shallow.flush_cache(Some(&stmt.name), None).await?;
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Forwards a `DROP ALL CACHES` request to noria
    async fn drop_all_caches(
        connectors: &mut BackendConnectors<DB>,
        state: &mut BackendState<DB>,
        cache_type: Option<CacheType>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        if matches!(cache_type, Some(CacheType::Deep) | None) {
            state.authority.remove_all_cache_ddl_requests().await?;
            connectors.noria.drop_all_caches().await?;
            state.query_status_cache.clear_manual_caches();
        }
        if matches!(cache_type, Some(CacheType::Shallow) | None) {
            state
                .authority
                .remove_all_shallow_cache_ddl_requests()
                .await?;
            state.shallow.drop_all_caches();
            if let Some(coordinator) = &state.rls_coordinator {
                coordinator.clear();
            }
        }
        state.query_status_cache.clear(cache_type);
        state.prepared.invalidate_all(cache_type);
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Drop caches with matching query_id or name.
    async fn drop_caches_on_collision(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query_id: Option<QueryId>,
        name: Option<&Relation>,
    ) -> ReadySetResult<()> {
        if query_id.is_none() && name.is_none() {
            return Ok(());
        }

        for CacheExpr {
            name,
            statement,
            query_id,
            ..
        } in connectors.noria.verbose_views(query_id, name).await?
        {
            warn!(
                %query_id,
                name = %name.display(DB::SQL_DIALECT),
                statement = %Sensitive(&statement.display(settings.dialect)),
                "Dropping previously cached query",
            );
            Self::drop_cached_query(connectors, state, &name).await?;
        }
        for CacheInfo {
            name,
            query_id,
            query,
            ..
        } in state.shallow.list_caches(query_id, name)
        {
            let none = || "None".to_string();
            warn!(
                %query_id,
                name = %name
                    .as_ref()
                    .map_or_else(none, |name| name.display(DB::SQL_DIALECT).to_string()),
                statement = %Sensitive(&query.display(settings.dialect)),
                "Dropping previously shallow-cached query",
            );
            state
                .drop_shallow_cached_query(name.as_ref(), Some(query_id))
                .await?;
        }
        Ok(())
    }

    /// Responds to a `SHOW PROXIED QUERIES` query
    async fn show_proxied_queries(
        state: &mut BackendState<DB>,
        settings: &BackendSettings,
        query_id: &Option<String>,
        only_supported: bool,
        limit: Option<u64>,
        cache_type: Option<CacheType>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let cache_type = cache_type.unwrap_or(match settings.cache_mode {
            CacheMode::Deep | CacheMode::DeepThenShallow => CacheType::Deep,
            CacheMode::Shallow => CacheType::Shallow,
        });
        let mut queries = state.query_status_cache.proxied_list(cache_type);
        if let Some(q_id) = query_id {
            queries.retain(|q| &q.id.to_string() == q_id);
        }

        if only_supported {
            queries.retain(|q| q.status.migration_state.is_supported());
        }

        let exec_counts = metrics_handle().map(|h| {
            let [counts] = h.counters_by_label(
                [metric::QUERY_LOG_EXECUTION_COUNT],
                "query_id",
                [("database_type", "upstream")],
            );
            counts
        });

        let select_schema = if exec_counts.is_some() {
            let mut select_schema = create_dummy_schema!("query_id", "query", "readyset_supported");

            // Add count separately with a different type (UnsignedInt)
            let count_schema = ColumnSchema {
                column: ast::Column {
                    name: "count".into(),
                    table: None,
                },
                column_type: DfType::UnsignedInt,
                base: None,
            };
            select_schema.schema.to_mut().push(count_schema);
            select_schema.columns.to_mut().push("count".into());

            select_schema
        } else {
            create_dummy_schema!("query_id", "query", "readyset_supported")
        };

        let query_status_cache = &state.query_status_cache;
        let mut data = queries
            .into_iter()
            .map(|ProxiedQuery { id, query, status }| {
                let s = match status.migration_state {
                    MigrationState::Successful(_) => "yes".to_string(),
                    MigrationState::Supported => {
                        match query_status_cache.shallow_auto_create_skip_reason(id) {
                            Some(reason) => format!("skipped: {reason}"),
                            None => "yes".to_string(),
                        }
                    }
                    MigrationState::Pending | MigrationState::Inlined(_) => "pending".to_string(),
                    MigrationState::Unsupported(reason) if reason.is_empty() => {
                        "unsupported: unknown reason".to_string()
                    }
                    MigrationState::Unsupported(reason) => format!("unsupported: {reason}"),
                };

                let mut row = vec![
                    DfValue::from(id.to_string()),
                    DfValue::from(Self::format_query_text(
                        query.display(DB::SQL_DIALECT).to_string(),
                    )),
                    DfValue::from(s),
                ];

                if let Some(exec_counts) = &exec_counts {
                    row.push(DfValue::UnsignedInt(exec_counts.get(&id.to_string())));
                }

                row
            })
            .collect::<Vec<_>>();

        data.sort_by(|a, b| {
            let status_order = |s: &str| match s {
                "yes" => 0,
                // we sometimes provide the reason for unsupported queries
                // like so "unsupported: xyz"
                unsupported if unsupported.starts_with("unsupported") => 1,
                // and the reason we declined to cache one automatically
                skipped if skipped.starts_with("skipped") => 2,
                "pending" => 3,
                _ => 4,
            };

            let a_status = status_order(&a[2].to_string());
            let b_status = status_order(&b[2].to_string());

            // If we don't have counts from metrics, give them all the same count for sorting
            // purposes
            let a_count = match a.get(3) {
                Some(DfValue::UnsignedInt(val)) => *val,
                _ => 0,
            };

            let b_count = match b.get(3) {
                Some(DfValue::UnsignedInt(val)) => *val,
                _ => 0,
            };

            // Reverse for descending order
            match a_status.cmp(&b_status) {
                std::cmp::Ordering::Equal => b_count.cmp(&a_count),
                other => other,
            }
        });

        if let Some(limit) = limit {
            data.truncate(limit as usize);
        }

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(data)],
        ))
    }

    /// Responds to a `SHOW CACHES` query
    async fn show_caches(
        connectors: &mut BackendConnectors<DB>,
        state: &mut BackendState<DB>,
        cache_type: Option<CacheType>,
        query_id: Option<&str>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let query_id = match query_id {
            // Bail if query_id is specified and invalid.
            Some(query_id) => Some(query_id.parse()?),
            None => None,
        };

        let exec_counts = metrics_handle().map(|h| {
            let [counts] = h.counters_by_label(
                [metric::QUERY_LOG_EXECUTION_COUNT],
                "query_id",
                [("database_type", "readyset")],
            );
            counts
        });

        let select_schema = if exec_counts.is_some() {
            create_dummy_schema!("query_id", "name", "query", "properties", "count")
        } else {
            create_dummy_schema!("query_id", "name", "query", "properties")
        };

        let mut rows = vec![];
        let mut push_row = |query_id, name, query, properties, count| {
            let row = if let Some(count) = count {
                vec![query_id, name, query, properties, count]
            } else {
                vec![query_id, name, query, properties]
            };
            rows.push(row);
        };

        if matches!(cache_type, Some(CacheType::Deep) | None) {
            for view in connectors.noria.verbose_views(query_id, None).await? {
                let query_id = view.query_id.to_string();
                let name = view.name.display_unquoted().to_string().into();
                let query =
                    Self::format_query_text(view.statement.display(DB::SQL_DIALECT).to_string())
                        .into();
                let properties = {
                    let mut properties = CacheProperties::new(CacheType::Deep);
                    properties.set_trx_cache_policy(view.trx_cache_policy);
                    if let Some(m) = view.topk_buffer_multiplier {
                        properties.set_topk_buffer_multiplier(m);
                    }
                    properties.to_string().into()
                };
                let count = exec_counts
                    .as_ref()
                    .map(|c| c.get(&query_id).to_string().into());
                let query_id = query_id.into();

                push_row(query_id, name, query, properties, count);
            }
        }
        if matches!(cache_type, Some(CacheType::Shallow) | None) {
            for CacheInfo {
                name,
                query_id,
                query,
                ttl_ms,
                refresh_ms,
                coalesce_ms,
                trx_cache_policy,
                schedule,
                adaptive,
                ..
            } in state.shallow.list_caches(query_id, None)
            {
                let query_id = query_id.to_string();
                let name = name
                    .map(|n| n.display_unquoted().to_string().into())
                    .unwrap_or("".into());
                let query = query.display(DB::SQL_DIALECT).to_string().into();
                let properties = {
                    let mut properties = CacheProperties::new(CacheType::Shallow);
                    if let Some(ttl_ms) = ttl_ms {
                        properties.set_ttl_ms(ttl_ms);
                    }
                    if let Some(refresh_ms) = refresh_ms {
                        properties.set_refresh_ms(refresh_ms);
                    }
                    if let Some(coalesce_ms) = coalesce_ms {
                        properties.set_coalesce_ms(coalesce_ms);
                    }
                    properties.set_trx_cache_policy(trx_cache_policy);
                    properties.set_schedule(schedule);
                    properties.set_adaptive(adaptive);
                    properties.to_string().into()
                };
                let count = exec_counts
                    .as_ref()
                    .map(|c| c.get(&query_id).to_string().into());
                let query_id = query_id.into();

                push_row(query_id, name, query, properties, count);
            }
        }

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(rows)],
        ))
    }

    /// Parse an RFC 3339 EXPIRES timestamp and normalize it to UTC.
    fn parse_expires_rfc3339(s: &str) -> ReadySetResult<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| internal_err!("invalid EXPIRES timestamp: {e}"))
    }

    /// Handle `CREATE MCP TOKEN '<name>' [WITH SCOPE <scope>] [EXPIRES '<datetime>']`.
    ///
    /// Generates a fresh random token value, stores its hash in the Authority,
    /// and returns the raw value in a single-row result. This is the only time the
    /// raw value is exposed.
    async fn create_mcp_token(
        state: &mut BackendState<DB>,
        stmt: &CreateMcpTokenStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        use rand::RngExt;
        use rand::distr::Alphanumeric;

        let scope = match stmt.scope.as_ref() {
            None | Some(ParserMcpTokenScope::ReadOnly) => AuthorityMcpTokenScope::ReadOnly,
            Some(ParserMcpTokenScope::CacheAdmin) => AuthorityMcpTokenScope::CacheAdmin,
            Some(ParserMcpTokenScope::Full) => AuthorityMcpTokenScope::Full,
        };

        let expires_at = stmt
            .expires
            .as_deref()
            .map(Self::parse_expires_rfc3339)
            .transpose()?;

        let secret: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let value = format!("rs_mcp_{secret}");

        let token = McpToken {
            name: stmt.name.clone(),
            hash: McpToken::hash_value(&value),
            scope,
            created_at: chrono::Utc::now(),
            expires_at,
        };
        state.authority.add_mcp_token(token).await?;

        let schema = SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: ast::Column {
                    name: "token".into(),
                    table: None,
                },
                column_type: DfType::DEFAULT_TEXT,
                base: None,
            }]),
            columns: Cow::Owned(vec!["token".into()]),
        };
        let rows = vec![vec![DfValue::from(value)]];
        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(rows)],
        ))
    }

    /// Handle `DROP MCP TOKEN '<name>'`.
    async fn drop_mcp_token(
        state: &mut BackendState<DB>,
        stmt: &DropMcpTokenStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        state.authority.remove_mcp_token(&stmt.name).await?;
        Ok(noria_connector::QueryResult::Delete {
            num_rows_deleted: 1,
        })
    }

    /// Handle `ALTER MCP TOKEN '<name>' SET (EXPIRES '<datetime>' | NEVER EXPIRES)`.
    async fn alter_mcp_token(
        state: &mut BackendState<DB>,
        stmt: &AlterMcpTokenStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let expires_at = match &stmt.expires {
            McpTokenExpiresChange::Never => None,
            McpTokenExpiresChange::At(s) => Some(Self::parse_expires_rfc3339(s)?),
        };
        state
            .authority
            .set_mcp_token_expires_at(&stmt.name, expires_at)
            .await?;
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Returns whether `user` is the user implied by `--upstream-db-url`. Dropping or rotating
    /// that user would break MCP loopback auth and upstream-credential login, and the original
    /// password could not be re-derived, so those mutations are rejected.
    async fn is_upstream_url_user(state: &BackendState<DB>, user: &str) -> bool {
        if let Some(cfg) = state.upstream_config.as_ref() {
            let upstream_url = cfg.read().await.upstream_db_url.clone();
            if let Some(url) = upstream_url
                && let Ok(parsed) = url.parse::<DatabaseURL>()
            {
                return parsed.user() == Some(user);
            }
        }
        false
    }

    /// Persist a mutated allowed-users map to the Authority and swap it into the in-memory handle.
    /// The mutation guard is held across the whole snapshot -> persist -> replace sequence so
    /// concurrent `ALTER READYSET ... USER` statements cannot leave the map and the Authority (or
    /// the MySQL `AuthCache`) disagreeing.
    async fn persist_user_mutation<F, Fut>(
        state: &mut BackendState<DB>,
        mutate: F,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>>
    where
        F: FnOnce(Arc<Authority>, HashMap<String, String>) -> Fut,
        Fut: std::future::Future<Output = ReadySetResult<HashMap<String, String>>>,
    {
        let _guard = state.users.lock_mutations().await;
        let seed = state.users.snapshot();
        let new_map = mutate(Arc::clone(&state.authority), seed).await?;
        state.users.replace(new_map);
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Handle `ALTER READYSET ADD USER '<user>' PASSWORD '<password>'`.
    async fn add_user(
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        stmt: &AddUserStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        if !settings.require_authentication {
            unsupported!("ALTER READYSET ADD USER requires authentication to be enabled");
        }
        let user = stmt.user.to_string();
        let password = stmt.password.0.clone();
        let result = Self::persist_user_mutation(state, |authority, seed| async move {
            authority
                .add_allowed_user(seed, user.clone(), password)
                .await
        })
        .await;
        if result.is_ok() {
            info!(user = %stmt.user, "ALTER READYSET ADD USER");
        }
        result
    }

    /// Handle `ALTER READYSET MODIFY USER '<user>' SET PASSWORD '<password>'`.
    async fn modify_user(
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        stmt: &ModifyUserStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        if !settings.require_authentication {
            unsupported!("ALTER READYSET MODIFY USER requires authentication to be enabled");
        }
        let user = stmt.user.to_string();
        if Self::is_upstream_url_user(state, &user).await {
            unsupported!("cannot MODIFY the user from --upstream-db-url");
        }
        let password = stmt.password.0.clone();
        let result = Self::persist_user_mutation(state, |authority, seed| async move {
            authority
                .modify_allowed_user(seed, user.clone(), password)
                .await
        })
        .await;
        if result.is_ok() {
            info!(user = %stmt.user, "ALTER READYSET MODIFY USER");
        }
        result
    }

    /// Handle `ALTER READYSET DROP USER '<user>'`.
    async fn drop_user(
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        stmt: &DropUserStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        if !settings.require_authentication {
            unsupported!("ALTER READYSET DROP USER requires authentication to be enabled");
        }
        let user = stmt.user.to_string();
        if Self::is_upstream_url_user(state, &user).await {
            unsupported!("cannot DROP the user from --upstream-db-url");
        }
        let result = Self::persist_user_mutation(state, |authority, seed| async move {
            authority.drop_allowed_user(seed, user.clone()).await
        })
        .await;
        if result.is_ok() {
            info!(user = %stmt.user, "ALTER READYSET DROP USER");
        }
        result
    }

    /// Handle `SHOW MCP TOKENS`.
    async fn show_mcp_tokens(
        state: &BackendState<DB>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let tokens = state.authority.mcp_tokens().await?;

        let col = |name: &str| ColumnSchema {
            column: ast::Column {
                name: name.into(),
                table: None,
            },
            column_type: DfType::DEFAULT_TEXT,
            base: None,
        };
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                col("name"),
                col("scope"),
                col("created_at"),
                col("expires_at"),
            ]),
            columns: Cow::Owned(vec![
                "name".into(),
                "scope".into(),
                "created_at".into(),
                "expires_at".into(),
            ]),
        };

        let rows: Vec<Vec<DfValue>> = tokens
            .into_iter()
            .map(|t| {
                vec![
                    DfValue::from(t.name),
                    DfValue::from(t.scope.to_string()),
                    DfValue::from(t.created_at.to_rfc3339()),
                    t.expires_at
                        .map(|e| DfValue::from(e.to_rfc3339()))
                        .unwrap_or(DfValue::None),
                ]
            })
            .collect();

        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(rows)],
        ))
    }

    /// Handle `ALTER READYSET {ADD|DROP} SHALLOW CACHE ALLOWED
    /// {FUNCTION|VARIABLE|SCHEMA} <name>[, ...]`.
    ///
    /// Persists the change to the authority first (so it survives a restart),
    /// then updates the in-memory allowlist shared by every connection. On
    /// `ADD` it also clears the auto-create skip set, so a query previously
    /// rejected for a now-allowed name is re-evaluated on its next execution
    /// rather than staying skipped until the next restart.
    async fn alter_shallow_cache_allowlist(
        state: &mut BackendState<DB>,
        stmt: &ShallowCacheAllowlistChange,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let names: Vec<String> = stmt
            .names
            .iter()
            .map(|f| f.to_string().to_ascii_lowercase())
            .collect();
        // The targeted allowlist mutates a shared set through interior mutability
        // (insert/remove/lock all take &self), so a borrow suffices and the
        // mutations below are visible to every connection.
        let allowlist = state.shallow_cache_allowlists.for_kind(stmt.kind);
        // Serialize concurrent allowlist updates so the authority write and the
        // in-memory mirror below stay in the same order: without this, a
        // concurrent add and drop of the same name could apply to the authority
        // and the shared set in opposite orders, leaving them disagreeing.
        let _update_guard = allowlist.lock_for_update().await;
        // Persist every name in one authority round-trip so the change is
        // atomic (all or nothing on failure), then mirror it into the in-memory
        // set shared by every connection.
        state
            .authority
            .modify_shallow_cache_allowlist(stmt.kind, stmt.add, names.clone())
            .await?;
        for name in &names {
            if stmt.add {
                allowlist.insert(name);
            } else {
                allowlist.remove(name);
            }
        }
        if stmt.add {
            state.query_status_cache.clear_shallow_auto_create_skips();
        }
        info!(
            kind = stmt.kind.singular_keyword(),
            names = %names.join(", "),
            action = if stmt.add { "add" } else { "drop" },
            "Updated shallow-cache allowlist"
        );
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Handle `SHOW SHALLOW CACHE ALLOWED {FUNCTIONS|VARIABLES|SCHEMAS}`.
    ///
    /// Returns only the runtime allowlist for `kind`: the names added with
    /// `ALTER READYSET ADD SHALLOW CACHE ALLOWED ...`. This is not the full set
    /// of names eligible for shallow caching. The IMMUTABLE builtins, and any
    /// category opened by a `--shallow-cache-allow-*` flag, are cacheable without
    /// appearing here.
    async fn show_shallow_cache_allowlist(
        state: &BackendState<DB>,
        kind: ShallowCacheAllowlistKind,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let column = kind.singular_keyword().to_ascii_lowercase();
        let schema = SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: ast::Column {
                    name: column.clone().into(),
                    table: None,
                },
                column_type: DfType::DEFAULT_TEXT,
                base: None,
            }]),
            columns: Cow::Owned(vec![column.into()]),
        };
        let rows: Vec<Vec<DfValue>> = state
            .shallow_cache_allowlists
            .for_kind(kind)
            .snapshot()
            .into_iter()
            .map(|name| vec![DfValue::from(name)])
            .collect();
        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(rows)],
        ))
    }

    pub(super) async fn query_readyset_extensions<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &'a BackendSettings,
        state: &'a mut BackendState<DB>,
        query: &'a SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        event.sql_type = SqlQueryType::Other;
        event.destination = Some(QueryDestination::Readyset(None));

        let start = Instant::now();

        let res = match query {
            SqlQuery::Explain(ExplainStatement::LastStatement) => state.explain_last_statement(),
            SqlQuery::Explain(ExplainStatement::Graphviz {
                simplified: _,
                for_cache,
            }) => connectors.noria.graphviz(for_cache.clone()).await,
            SqlQuery::Explain(ExplainStatement::Domains) => {
                connectors.noria.explain_domains().await
            }
            SqlQuery::Explain(ExplainStatement::Caches) => {
                Self::explain_caches(connectors, state).await
            }
            SqlQuery::Explain(ExplainStatement::Materializations { for_cache }) => {
                connectors
                    .noria
                    .explain_materializations(for_cache.clone())
                    .await
            }
            SqlQuery::Explain(explain @ ExplainStatement::CreateCache { .. }) => {
                Self::explain_create_cache(connectors, settings, state, explain).await
            }
            SqlQuery::CreateCache(create_cache_stmt) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }

                create_cache_stmt.detect_and_validate_bucket_always()?;

                let CreateCacheStatement {
                    name,
                    cache_type,
                    policy,
                    coalesce_ms,
                    adaptive,
                    inner,
                    trx_cache_policy,
                    concurrently,
                    unparsed_create_cache_statement,
                    topk_buffer_multiplier,
                    autoparam,
                } = create_cache_stmt;
                let (deep, shallow, schema_generation, manual_mapping) =
                    Self::query_from_cache_inner(connectors, settings, state, inner, *autoparam)
                        .await?;

                // Log a telemetry event
                if let Some(ref telemetry_sender) = state.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::CreateCache) {
                        warn!(error = %e, "Failed to send CREATE CACHE metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for CREATE CACHE");
                }

                let ddl_req = if let Some(unparsed_create_cache_statement) =
                    unparsed_create_cache_statement
                {
                    let ddl_req = CacheDDLRequest {
                        unparsed_stmt: unparsed_create_cache_statement.clone(),
                        schema_search_path: connectors.noria.schema_search_path().to_owned(),
                        dialect: settings.dialect.into(),
                        cache_name: None,
                    };
                    Some(ddl_req)
                } else {
                    None
                };

                let cache_mode = settings.cache_mode;
                let deep_requested = *cache_type == Some(CacheType::Deep);
                let shallow_requested = *cache_type == Some(CacheType::Shallow);

                if deep_requested || (cache_mode.is_deep() && !shallow_requested) {
                    Self::create_deep_cache(
                        connectors,
                        settings,
                        state,
                        name.clone(),
                        deep,
                        shallow,
                        *trx_cache_policy,
                        *concurrently,
                        *topk_buffer_multiplier,
                        schema_generation,
                        manual_mapping,
                        ddl_req,
                        false,
                    )
                    .await
                } else if shallow_requested || (cache_mode.is_shallow() && !deep_requested) {
                    Self::create_shallow_cache(
                        connectors,
                        settings,
                        state,
                        name.clone(),
                        deep,
                        shallow,
                        *policy,
                        ddl_req,
                        *trx_cache_policy,
                        *coalesce_ms,
                        *adaptive,
                    )
                    .await
                } else {
                    let res = Self::create_deep_cache(
                        connectors,
                        settings,
                        state,
                        name.clone(),
                        deep.clone(),
                        shallow.clone(),
                        *trx_cache_policy,
                        *concurrently,
                        *topk_buffer_multiplier,
                        schema_generation,
                        manual_mapping,
                        ddl_req.clone(),
                        true,
                    )
                    .await;
                    match res {
                        Ok(res) => Ok(res),
                        Err(error) if error.is_transient() => {
                            info!(%error, "Skipping CREATE CACHE due to transient error");
                            Err(ReadySetError::CreateCacheError(format!(
                                "Please retry due to transient error: {error}"
                            )))
                        }
                        Err(error) => {
                            info!(
                                %error,
                                "Deep cache creation failed; falling back to shallow cache"
                            );
                            Self::create_shallow_cache(
                                connectors,
                                settings,
                                state,
                                name.clone(),
                                deep,
                                shallow,
                                *policy,
                                ddl_req,
                                *trx_cache_policy,
                                *coalesce_ms,
                                *adaptive,
                            )
                            .await
                        }
                    }
                }
            }
            SqlQuery::DropCache(drop_cache) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG)
                }
                let name = &drop_cache.name;

                // Try shallow first: a shallow cache also removes its persisted entry, matched by
                // the cache's own stored request.
                if state
                    .drop_shallow_cached_query(Some(name), None)
                    .await
                    .is_ok()
                {
                    Ok(noria_connector::QueryResult::Delete {
                        num_rows_deleted: 1,
                    })
                } else {
                    let res = Self::drop_cached_query(connectors, state, name).await;
                    // `drop_cached_query` can report Ok with zero rows deleted, in which case
                    // nothing was cached and there is no persisted entry to remove.
                    let dropped = matches!(
                        res,
                        Ok(noria_connector::QueryResult::Delete { num_rows_deleted }) if num_rows_deleted >= 1
                    );
                    if dropped {
                        let matched = retry_with_exponential_backoff!(
                            || async {
                                state.authority.remove_cache_ddl_requests_named(name).await
                            },
                            retries: 5,
                            delay: 1,
                            backoff: 2,
                        )
                        .unwrap_or(false);
                        // Entries written before `cache_name` existed can't be matched by name.
                        // Store a DROP marker so the controller still cancels them on replay.
                        if !matched {
                            let marker = CacheDDLRequest {
                                unparsed_stmt: drop_cache.display_unquoted().to_string(),
                                schema_search_path: vec![],
                                dialect: settings.dialect.into(),
                                cache_name: None,
                            };
                            if let Err(e) = state.authority.add_cache_ddl_request(marker).await {
                                error!(error = %e, "Failed to store 'drop cache' fallback request");
                            }
                        }
                    }
                    res
                }
            }
            SqlQuery::DropAllCaches(DropAllCachesStatement { cache_type }) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                Self::drop_all_caches(connectors, state, *cache_type).await
            }
            SqlQuery::FlushAllShallowCaches(_) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                Self::flush_all_shallow_caches(state).await
            }
            SqlQuery::FlushCache(stmt) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                Self::flush_shallow_cache(state, stmt).await
            }
            SqlQuery::DropAllProxiedQueries(_) => {
                if !settings.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                state.drop_all_proxied_queries().await
            }
            SqlQuery::Show(ShowStatement::CachedQueries(cache_type, query_id)) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = state.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowCaches) {
                        warn!(error = %e, "Failed to send SHOW CACHES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW CACHES");
                }

                Self::show_caches(connectors, state, *cache_type, query_id.as_deref()).await
            }
            SqlQuery::Show(ShowStatement::ShallowCacheEntries { query_id, limit }) => {
                state
                    .show_shallow_entries(query_id.as_deref(), *limit)
                    .await
            }
            SqlQuery::Show(ShowStatement::ReadySetStatus) => Ok(state
                .status_reporter
                .report_status()
                .await
                .into_query_result()),
            SqlQuery::Show(ShowStatement::ReadySetStatusAdapter) => state.readyset_adapter_status(),
            SqlQuery::Show(ShowStatement::ReadySetMigrationStatus(id)) => {
                connectors.noria.migration_status(*id).await
            }
            SqlQuery::Show(ShowStatement::ReadySetVersion) => readyset_version(),
            SqlQuery::Show(ShowStatement::ReadySetTables(options)) => {
                connectors.noria.table_statuses(options.all).await
            }
            SqlQuery::Show(ShowStatement::Connections) => state.show_connections(),
            SqlQuery::Show(ShowStatement::ProxiedQueries(ProxiedQueriesOptions {
                query_id,
                only_supported,
                limit,
                cache_type,
            })) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = state.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowProxiedQueries)
                    {
                        warn!(error = %e, "Failed to send SHOW PROXIED QUERIES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW PROXIED QUERIES");
                }

                Self::show_proxied_queries(
                    state,
                    settings,
                    query_id,
                    *only_supported,
                    *limit,
                    *cache_type,
                )
                .await
            }
            SqlQuery::Show(ShowStatement::ReplayPaths) => connectors.show_replay_paths().await,
            SqlQuery::Show(ShowStatement::Rls(_maybe_table)) => {
                unsupported!("SHOW RLS statement is not yet supported")
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ResnapshotTable(stmt)) => {
                let mut table = stmt.table.clone();
                connectors.noria.resnapshot_table(&mut table).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::AddTables(stmt)) => {
                let mut tables = stmt.tables.clone();
                connectors.noria.add_filter_tables(&mut tables).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::EnterMaintenanceMode) => {
                connectors.noria.enter_maintenance_mode().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ExitMaintenanceMode) => {
                connectors.noria.exit_maintenance_mode().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::SetLogLevel(directives)) => {
                match readyset_tracing::set_log_level(directives) {
                    Ok(()) => Ok(noria_connector::QueryResult::Empty),
                    Err(e) => Err(internal_err!("Failed to set log level: {e}")),
                }
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::SetEviction(stmt)) => {
                use std::time::Duration;

                let period = stmt.period.map(Duration::from_millis);
                let limit = stmt.limit.map(|l| l as usize);

                info!(
                    limit_bytes = ?limit,
                    period_ms = ?period,
                    "Setting eviction configuration"
                );

                connectors.noria.set_eviction(period, limit).await?;
                Ok(noria_connector::QueryResult::Empty)
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ChangeUpstream(
                ChangeUpstreamStatement { url },
            )) => {
                if settings.replication_enabled {
                    unsupported!("CHANGE UPSTREAM is only allowed when replication is disabled");
                }
                let parsed: DatabaseURL = url
                    .parse()
                    .map_err(|e| internal_err!("invalid upstream URL: {e}"))?;
                if parsed.dialect() != settings.dialect {
                    internal!("wrong database type for upstream");
                }
                let url = url.clone();
                let redacted = RedactedString::from(url.clone());
                let config = state
                    .upstream_config
                    .as_ref()
                    .ok_or_else(|| internal_err!("upstream config is not configured"))?;
                let mut config = config.write().await;
                config.upstream_db_url = Some(url.into());
                drop(config);
                info!(url = %redacted, "Changed upstream configuration");
                Ok(noria_connector::QueryResult::Empty)
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::StopReplication) => {
                connectors.noria.stop_replication().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::StartReplication) => {
                connectors.noria.start_replication().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::SetReplicationPosition(stmt)) => {
                connectors
                    .noria
                    .set_replication_position(&stmt.position)
                    .await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ChangeCdc(ChangeCdcStatement {
                url,
            })) => connectors.noria.change_cdc_url(url).await,
            SqlQuery::AlterReadySet(AlterReadysetStatement::ShallowCacheAllowlistChange(stmt)) => {
                // Mutating an allowlist steers what auto-caches and persists to
                // the authority, so it belongs to the cache-DDL family and is
                // gated the same way (read-only SHOW is not).
                if settings.allow_cache_ddl {
                    Self::alter_shallow_cache_allowlist(state, stmt).await
                } else {
                    unsupported!(
                        "ALTER READYSET SHALLOW CACHE ALLOWED requires cache DDL to be enabled"
                    )
                }
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::AddUser(stmt)) => {
                Self::add_user(settings, state, stmt).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ModifyUser(stmt)) => {
                Self::modify_user(settings, state, stmt).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::DropUser(stmt)) => {
                Self::drop_user(settings, state, stmt).await
            }
            SqlQuery::CreateRls(_create_rls) => {
                unsupported!("CREATE RLS statement is not yet supported")
            }
            SqlQuery::DropRls(_drop_rls) => {
                unsupported!("DROP RLS statement is not yet supported")
            }
            SqlQuery::CreateMcpToken(stmt) => Self::create_mcp_token(state, stmt).await,
            SqlQuery::DropMcpToken(stmt) => Self::drop_mcp_token(state, stmt).await,
            SqlQuery::AlterMcpToken(stmt) => Self::alter_mcp_token(state, stmt).await,
            SqlQuery::Show(ShowStatement::McpTokens) => Self::show_mcp_tokens(state).await,
            SqlQuery::Show(ShowStatement::ShallowCacheAllowlist(kind)) => {
                Self::show_shallow_cache_allowlist(state, *kind).await
            }
            _ => Err(internal_err!("Provided query is not a Readyset extension")),
        };

        event.readyset_event = Some(ReadysetExecutionEvent::Other {
            duration: start.elapsed(),
        });

        res
    }

    /// Gets a list of all `CREATE CACHE ...` statements
    async fn explain_caches(
        connectors: &mut BackendConnectors<DB>,
        state: &BackendState<DB>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let mut results: Vec<Vec<DfValue>> = connectors
            .noria
            .list_create_cache_stmts()
            .await?
            .into_iter()
            .map(|s| vec![DfValue::from(s)])
            .collect();
        results.extend(
            state
                .shallow
                .list_caches(None, None)
                .into_iter()
                .map(CreateCacheStatement::from)
                .map(|create| vec![DfValue::from(create.display(DB::SQL_DIALECT).to_string())]),
        );

        let select_schema = create_dummy_schema!("query");

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(results)],
        ))
    }
}

/// Mapping data computed while creating a deep cache with autoparameterization suppressed
/// (`CREATE CACHE WITH (AUTOPARAM ...)`): the standard (fully autoparameterized) form of the
/// query, which is the shape incoming SELECTs hash to, and the literals the manual form keeps
/// inline (by position in the standard form's merged parameter order).
#[derive(Debug, Clone)]
struct ManualMappingInfo {
    lookup: ViewCreateRequest,
    frozen: Vec<(usize, ast::Literal)>,
}

/// Remove a DDL request from authority when cache creation fails.
///
/// The extend_recipe may have failed, in which case we should remove our intention
/// to create this cache. Extend recipe waits a bit and then returns an
/// Ok(ExtendRecipeResult::Pending) if it is still creating a cache in the
/// background, so we don't remove the ddl request for timeouts.
async fn remove_ddl_on_error<T, F, Fut>(
    res: &Result<T, ReadySetError>,
    authority: &Arc<Authority>,
    ddl_req: Option<CacheDDLRequest>,
    name: Option<Relation>,
    cache_type: &str,
    remove: F,
    quiet: bool,
) where
    F: Fn(Arc<Authority>, CacheDDLRequest) -> Fut,
    Fut: Future<Output = ReadySetResult<()>>,
{
    if res.is_ok() {
        return;
    }

    let Some(ddl_req) = ddl_req else {
        return;
    };

    let remove = retry_with_exponential_backoff!(
        || async {
            let ddl_req = ddl_req.clone();
            remove(authority.clone(), ddl_req).await
        },
        retries: 5,
        delay: 1,
        backoff: 2,
    );
    if remove.is_err() {
        error!(
            "Failed to remove stored 'create {cache_type} cache' request. \
             It will be re-run if there is a backwards incompatible upgrade.",
        );
    }

    if let Err(e) = res
        && !quiet
    {
        error!(
            name = %name.unwrap_or("".into()).display_unquoted(),
            "Failed to create {cache_type} cache: {e}",
        );
    }
}
