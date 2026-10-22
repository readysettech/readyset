//! `SET` handling: deciding what a session-level `SET` means for Readyset, and recording the
//! changes in Readyset's copy of the session state once the upstream accepts them.
//!
//! Both protocols route here. A `SET` the adapter accepts is forwarded upstream so the session
//! there stays authoritative; what varies is whether Readyset can model the change, and whether
//! the adapter has to remember it for cache keying. A `SET` it does not support is rejected or
//! ignored per `unsupported_set_mode`, and never reaches the upstream at all.

use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;

use metrics::counter;
use readyset_client_metrics::QueryExecutionEvent;
use readyset_data::encoding::Encoding;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{
    Literal, PostgresParameterScope, SessionAuthorizationValue, SetSessionAuthorization,
    SetStatement, SqlIdentifier,
};
use readyset_util::SizeOf;
use tracing::{debug, error, trace, warn};

use super::noria_connector::NoriaConnector;
use super::routing::ProxyState;
use super::{Backend, BackendConnectors, BackendSettings, BackendState, UnsupportedSetMode};
use crate::query_handler::{SessionTimezone, SetBehavior, UpstreamSetRewrite};
use crate::session_context::{SessionContext, SetSessionEffect};
use crate::session_mutation::SessionMutationTemplate;
use crate::{QueryHandler, UpstreamDatabase};

/// Changes a `SET` makes to Readyset's copy of the upstream session's state. Apply them only after
/// the upstream accepts the statement.
#[derive(Debug, Default, Clone)]
pub(super) struct PendingSetState {
    autocommit: Option<bool>,
    search_path: Option<Vec<SqlIdentifier>>,
    timezone: Option<SessionTimezone>,
    results_collation: Option<u16>,
    results_encoding: Option<Encoding>,
    client_encoding: Option<Encoding>,
    session_vars: Vec<(String, Option<Literal>)>,
}

impl PendingSetState {
    fn new(behavior: SetBehavior) -> Self {
        let SetBehavior {
            unsupported,
            set_autocommit,
            set_search_path,
            set_results_collation,
            set_results_encoding,
            set_client_encoding,
            set_session_vars,
            set_timezone,
            ..
        } = behavior;
        Self {
            autocommit: set_autocommit,
            search_path: set_search_path,
            // The handler records `set_timezone` even for non-UTC values so a future eval-side fix
            // can read it unchanged. Only a UTC-equivalent zone is applied. Cached results are
            // UTC wall-clock times, so another zone would silently localize them.
            timezone: set_timezone.filter(|_| !unsupported),
            results_collation: set_results_collation,
            results_encoding: set_results_encoding,
            client_encoding: set_client_encoding,
            session_vars: set_session_vars,
        }
    }

    fn apply(self, noria: &mut NoriaConnector) {
        if let Some(search_path) = self.search_path {
            trace!(?search_path, "Setting search_path");
            noria.set_schema_search_path(search_path);
        }
        if let Some(tz) = self.timezone {
            trace!(?tz, "Setting timezone");
            noria.set_timezone(tz);
        }
        if let Some(collation) = self.results_collation {
            trace!(collation, "Setting results_collation");
            noria.set_results_collation(collation);
        }
        if let Some(encoding) = self.results_encoding {
            trace!(?encoding, "Setting results_encoding");
            noria.set_results_encoding(encoding);
        }
        if let Some(encoding) = self.client_encoding {
            trace!(?encoding, "Setting client_encoding");
            noria.set_client_encoding(encoding);
        }
        for (name, value) in self.session_vars {
            trace!(%name, ?value, "Setting session variable");
            noria.set_session_var(&name, value);
        }
    }
}

/// Names of the session settings a transaction changed. A rollback reverts them upstream, so
/// Readyset reads them back before the next statement.
#[derive(Debug, Default)]
pub(super) struct TransactionSettings {
    /// Settings the open transaction changed.
    changed: BTreeSet<String>,
    /// Settings to read back from upstream before the next statement.
    reread: BTreeSet<String>,
}

impl TransactionSettings {
    /// Note that an open transaction changed the setting `set` changes.
    pub(super) fn record<H>(&mut self, proxy_state: ProxyState, set: &SetStatement)
    where
        H: QueryHandler,
    {
        if proxy_state.in_transaction()
            && let Some(name) = H::setting_reverted_by_rollback(set)
        {
            self.changed.insert(name);
        }
    }

    /// Note that an open transaction changed the settings a `set_config(...)` batch sets.
    pub(super) fn record_set_config(
        &mut self,
        proxy_state: ProxyState,
        template: &SessionMutationTemplate,
    ) {
        if proxy_state.in_transaction() {
            let SessionMutationTemplate::SetConfig(calls) = template;
            self.changed
                .extend(calls.iter().map(|call| call.guc_name.to_ascii_lowercase()));
        }
    }

    /// Note that an open transaction ran `RESET ALL`, which changes the settings `session` holds.
    /// Call this before `session` is reset.
    pub(super) fn record_reset_all(&mut self, proxy_state: ProxyState, session: &SessionContext) {
        if proxy_state.in_transaction() {
            self.changed.extend(session.setting_names());
        }
    }

    /// The open transaction committed and keeps the settings it changed.
    pub(super) fn commit(&mut self) {
        self.changed.clear();
    }

    /// The open transaction rolled back, so the settings it changed should be read back from
    /// upstream.
    pub(super) fn rollback(&mut self) {
        self.reread.append(&mut self.changed);
    }

    /// The open transaction rolled back to a savepoint, so the settings it changed should be read
    /// back from upstream. They stay recorded, since the transaction can still roll back.
    pub(super) fn rollback_to_savepoint(&mut self) {
        self.reread.extend(self.changed.iter().cloned());
    }
}

impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    /// Return the effect of `set` on Readyset, with variables set to their default resolved to the
    /// upstream's default values.
    fn set_behavior(
        connectors: &BackendConnectors<DB>,
        state: &BackendState<DB>,
        set: &SetStatement,
    ) -> SetBehavior {
        let mut behavior = Handler::handle_set_statement(set);
        // A variable set to its default takes the upstream's default value for the session.
        let mut defaults = None;
        for (name, value) in &mut behavior.set_session_vars {
            if value.is_none() {
                *value = defaults
                    .get_or_insert_with(|| Self::session_defaults(connectors, state))
                    .get(name)
                    .cloned();
            }
        }
        let set_values = behavior
            .set_session_vars
            .iter()
            .filter_map(|(name, value)| Some((name.clone(), value.clone()?)))
            .collect();
        behavior.unsupported |= Handler::unsupported_session_vars(&set_values);
        behavior
    }

    /// Handles a parsed set statement by deferring to `Handler::handle_set_statement` and
    /// respecting `BackendSettings::unsupported_set_mode`.
    ///
    /// - If upstream exists, valid set statements are forwarded to it.
    /// - If no upstream is present, statements are typically ignored.
    /// - Disallowed set statements always produce an error.
    ///
    /// The caller passes the returned [`PendingSetState`] to [`Self::apply_set`] after the
    /// statement succeeds.
    pub(super) fn handle_set(
        connectors: &BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &str,
        set: &SetStatement,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<(UpstreamSetRewrite, PendingSetState)> {
        let mut behavior = Self::set_behavior(connectors, state, set);
        let unsupported = behavior.unsupported;

        // A compound SET like `SET autocommit=0, unknown_var=1` is rejected as a whole in Error
        // mode, so its autocommit change is never applied. This matches MySQL's all-or-nothing SET
        // semantics.
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
                    return Err(e);
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
        let upstream_rewrite = mem::take(&mut behavior.upstream_rewrite);
        Ok((upstream_rewrite, PendingSetState::new(behavior)))
    }

    /// Apply a `SET` that the upstream accepted to Readyset's copy of the session state. The
    /// SessionContext gets the settings, role and session authorization the RLS shallow cache keys
    /// on.
    pub(super) fn apply_set(
        pending: PendingSetState,
        noria: &mut NoriaConnector,
        session: Option<&SessionContext>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        set: &SetStatement,
    ) {
        if let Some(enabled) = pending.autocommit {
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
                    // Leaving autocommit mode starts an implicit transaction, as BEGIN does.
                    if !prev.in_transaction_or_implicit() {
                        state.write_tracker.on_start_transaction();
                    }
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
        pending.apply(noria);
        let Some(session) = session else {
            return;
        };
        let policy_registry = state.policy_registry.as_ref();
        match set {
            SetStatement::SessionAuthorization(auth) => {
                Self::mirror_session_authorization(session, policy_registry, auth)
            }
            _ => {
                if let SetSessionEffect::RoleSet { role, scope } = session.apply_set_statement(set)
                {
                    let bypass =
                        policy_registry.is_some_and(|reg| reg.bypass_rls_for_role(role.as_str()));
                    let local = matches!(scope, Some(PostgresParameterScope::Local));
                    session.set_effective_role_scoped(role, bypass, local);
                }
            }
        }
    }

    /// Read back from upstream the settings a rolled-back transaction reverted, and apply them to
    /// Readyset's copy of the session state. If that fails, the copy can't be trusted, so RLS reads
    /// go upstream and the session proxies all queries.
    pub(super) async fn reread_settings(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
    ) {
        if state.transaction_settings.reread.is_empty() {
            return;
        }
        let names = mem::take(&mut state.transaction_settings.reread);
        let Some(upstream) = connectors.upstream.as_mut() else {
            return;
        };
        match upstream.current_settings(names).await {
            Ok(sets) => {
                for set in sets {
                    let pending = PendingSetState::new(Self::set_behavior(connectors, state, &set));
                    Self::apply_set(
                        pending,
                        &mut connectors.noria,
                        connectors.session.as_deref(),
                        settings,
                        state,
                        &set,
                    );
                }
            }
            Err(error) => {
                debug!(%error, "Failed to read session settings back after a rollback");
                if let Some(session) = connectors.session.as_ref() {
                    session.mark_session_untrusted();
                }
                state.proxy_state = ProxyState::ProxyAlways;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Reset an untrusted session that has claims set inside a transaction, as `RESET ALL` does.
    fn reset_all_untrusted_session() -> (TransactionSettings, Arc<SessionContext>) {
        let session = SessionContext::new("alice".into());
        session.set_session_guc("request.jwt.claims", r#"{"sub":"bob"}"#);
        session.mark_session_untrusted();
        let mut settings = TransactionSettings::default();
        settings.record_reset_all(ProxyState::InTransaction, &session);
        session.reset_all();
        assert!(session.is_session_untrusted());
        (settings, session)
    }

    #[test]
    fn rolled_back_reset_all_rereads_settings() {
        let (mut settings, session) = reset_all_untrusted_session();
        settings.rollback();
        assert!(settings.reread.contains("request.jwt.claims"));
        assert!(session.is_session_untrusted());
    }

    #[test]
    fn reset_all_rolled_back_to_savepoint_rereads_settings() {
        let (mut settings, session) = reset_all_untrusted_session();
        settings.rollback_to_savepoint();
        assert!(settings.reread.contains("request.jwt.claims"));
        assert!(session.is_session_untrusted());
    }

    #[test]
    fn committed_reset_all_rereads_nothing() {
        let (mut settings, session) = reset_all_untrusted_session();
        settings.commit();
        settings.rollback();
        assert!(settings.reread.is_empty());
        assert!(session.is_session_untrusted());
    }
}
