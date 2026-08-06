//! `SET` handling: deciding what a session-level `SET` means for Readyset, and mirroring the
//! ones that change identity into the session context.
//!
//! Both protocols route here. A `SET` the adapter accepts is forwarded upstream so the session
//! there stays authoritative; what varies is whether Readyset can model the change, and whether
//! the adapter has to remember it for cache keying. A `SET` it does not support is rejected or
//! ignored per `unsupported_set_mode`, and never reaches the upstream at all.

use std::sync::Arc;

use metrics::counter;
use readyset_client_metrics::QueryExecutionEvent;
use readyset_errors::ReadySetError;
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{SessionAuthorizationValue, SetSessionAuthorization, SetStatement};
use readyset_util::SizeOf;
use tracing::{debug, error, trace, warn};

use super::routing::ProxyState;
use super::{Backend, BackendConnectors, BackendSettings, BackendState, UnsupportedSetMode};
use crate::query_handler::{SetBehavior, UpstreamSetRewrite};
use crate::session_context::SessionContext;
use crate::{QueryHandler, UpstreamDatabase};

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    /// Handles a parsed set statement by deferring to `Handler::handle_set_statement` and
    /// respecting `BackendSettings::unsupported_set_mode`. When the search path is changed
    /// (SetBehavior::SetSearchPath) or other sets need to be handled (certain variables being
    /// changed), the `noria` instance gets updated accordingly.
    ///
    /// - If upstream exists, valid set statements are forwarded to it.
    /// - If no upstream is present, statements are typically ignored.
    /// - Disallowed set statements always produce an error.
    pub(super) fn handle_set(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &str,
        set: &SetStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<UpstreamSetRewrite, DB::Error> {
        let SetBehavior {
            unsupported,
            proxy: _, // Basically ignored, caller will proxy unless we return an error
            set_autocommit,
            set_search_path,
            set_results_encoding,
            set_client_encoding,
            upstream_rewrite,
            set_timezone,
        } = Handler::handle_set_statement(set);

        // NOTE: The unsupported check runs before autocommit processing intentionally.
        // A compound SET like `SET autocommit=0, unknown_var=1` is rejected atomically
        // in Error mode — the autocommit state change is not applied. This matches
        // MySQL's all-or-nothing SET semantics.
        if unsupported {
            match settings.unsupported_set_mode {
                UnsupportedSetMode::Error => {
                    let e = ReadySetError::SetDisallowed {
                        statement: query.to_string(),
                    };
                    if connectors.upstream.is_some() {
                        event.set_noria_error(&e);
                    }
                    error!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    return Err(e.into());
                }
                UnsupportedSetMode::Proxy => {
                    warn!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    state.proxy_state = ProxyState::ProxyAlways;
                }
                UnsupportedSetMode::Allow => {}
            }
        }
        if let Some(enabled) = set_autocommit {
            let prev = state.proxy_state;
            state.proxy_state.set_autocommit(enabled);
            if state.proxy_state != prev {
                // `SET autocommit=1` from a transactional state does an implicit COMMIT;
                // refresh `last_write_at` so any RYW window fires from now.
                if enabled && matches!(prev, ProxyState::InTransaction | ProxyState::AutocommitOff)
                {
                    state.write_tracker.on_commit();
                }
                if matches!(state.proxy_state, ProxyState::AutocommitOff) {
                    debug!(
                        set = %set.display(settings.dialect),
                        "Autocommit disabled; all queries will be proxied upstream"
                    );
                    counter!(metric::SET_AUTOCOMMIT_DISABLED).increment(1);
                } else if matches!(prev, ProxyState::AutocommitOff) {
                    debug!(
                        set = %set.display(settings.dialect),
                        "Autocommit re-enabled"
                    );
                    counter!(metric::SET_AUTOCOMMIT_ENABLED).increment(1);
                }
            }
        }
        if let Some(search_path) = set_search_path {
            trace!(?search_path, "Setting search_path");
            connectors.noria.set_schema_search_path(search_path);
        }
        if let Some(encoding) = set_results_encoding {
            trace!(?encoding, "Setting results_encoding");
            connectors.noria.set_results_encoding(encoding);
        }
        if let Some(encoding) = set_client_encoding {
            trace!(?encoding, "Setting client_encoding");
            connectors.noria.set_client_encoding(encoding);
        }
        // The handler records `set_timezone` even for non-UTC values so a
        // future eval-side fix can read it unchanged; only apply it here when
        // the SET resolved to a UTC-equivalent zone — otherwise cached
        // results (UTC-wallclock today) would be silently localized.
        if let Some(tz) = set_timezone
            && !unsupported
        {
            trace!(?tz, "Setting timezone");
            connectors.noria.set_timezone(tz);
        }

        // Mirror the SET into the per-connection SessionContext so the RLS shallow cache can hash
        // by the relevant subset of session state. GUC sets and `RESET ROLE` are applied now; they
        // cannot be rejected as an authorization decision. `SET ROLE` (the `RoleSet` effect) is
        // deliberately not applied here -- role membership is an authorization boundary, so it is
        // mirrored only after upstream accepts it (`mirror_set_role`), matching
        // `SET SESSION AUTHORIZATION`. `apply_set_statement` does not mutate for `RoleSet`, so
        // discarding its result leaves the effective role untouched.
        if let Some(session) = connectors.session.as_ref() {
            let _ = session.apply_set_statement(set);
        }

        Ok(upstream_rewrite)
    }

    /// Mirror `SET [LOCAL] ROLE <role>` into the session context, called only after upstream
    /// accepted the statement. Resolves `bypass_rls` against the policy registry. Non-`SET ROLE`
    /// statements (and `RESET ROLE`, already applied by `handle_set`) are ignored.
    pub(super) fn mirror_set_role(
        session: &SessionContext,
        policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
        set: &SetStatement,
    ) {
        if let Some((role, local)) = SessionContext::pending_set_role(set) {
            let bypass = policy_registry.is_some_and(|reg| reg.bypass_rls_for_role(role.as_str()));
            session.set_effective_role_scoped(role, bypass, local);
        }
    }

    /// Mirror a `SET [LOCAL] SESSION AUTHORIZATION` into the session context,
    /// called only after upstream accepted the statement.
    ///
    /// A session-scope change (`local = false`) resolves to a concrete identity
    /// -- `DEFAULT` (and `RESET SESSION AUTHORIZATION`) to the startup user, a
    /// named user directly -- with `bypass_rls` resolved against the policy
    /// registry, and updates the mirror so later reads partition by it. A
    /// transaction-local change (`local = true`) reverts at the transaction
    /// boundary, which the mirror cannot model for `session_user`, so it fails
    /// closed (transaction-scoped) until `COMMIT` / `ROLLBACK`.
    pub(super) fn mirror_session_authorization(
        session: &SessionContext,
        policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
        auth: &SetSessionAuthorization,
    ) {
        if auth.local {
            session.mark_transaction_untrusted();
            return;
        }
        let role = match &auth.value {
            SessionAuthorizationValue::Default => session.startup_user.clone(),
            SessionAuthorizationValue::User(user) => user.clone(),
        };
        let bypass = policy_registry.is_some_and(|reg| reg.bypass_rls_for_role(role.as_str()));
        session.apply_session_authorization(role, bypass);
    }
}
