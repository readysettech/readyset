//! Per-session security context for Postgres RLS-aware shallow caches.
//!
//! Tracks the role, GUCs, and JWT claims that an RLS policy can read against. The shallow cache
//! hashes the relevant subset of this state into its key when serving an RLS-active query, so two
//! sessions with different security contexts get distinct cache partitions. Only constructed on
//! Postgres connections.

use std::collections::HashMap;
use std::sync::Arc;

use crate::shallow_key::SessionInputValue;
use parking_lot::RwLock;
use readyset_rls::SessionInputType;
use readyset_sql::ast::{
    Literal, PostgresParameterScope, PostgresParameterValue, PostgresParameterValueInner,
    SetPostgresParameter, SetPostgresParameterValue, SetStatement, SqlIdentifier,
};
use readyset_sql::{Dialect, DialectDisplay};
use serde_json::Value as JsonValue;

/// Per-Postgres-connection security context.
///
/// Shared through an [`Arc`]. The mutable state lives in a single `parking_lot::RwLock` over
/// [`SessionInner`] so the cache lookup hot path takes one read guard and sees a consistent tuple.
/// `parking_lot::RwLock` does not poison on panic, so a single bad write does not break every
/// subsequent read on the connection.
#[derive(Debug)]
pub struct SessionContext {
    /// User from the Postgres `StartupMessage`. Set once on connect and never mutated.
    pub startup_user: SqlIdentifier,
    inner: RwLock<SessionInner>,
}

/// Mutable session state kept together under a single [`parking_lot::RwLock`] so the hot path takes
/// one read guard and writers update role + bypass atomically.
#[derive(Debug)]
struct SessionInner {
    /// Role used to evaluate RLS policies for the current statement. `SET ROLE`, `SET LOCAL ROLE`,
    /// and `SET SESSION AUTHORIZATION` update it; `RESET ROLE` reverts it to `startup_user`.
    effective_role: SqlIdentifier,
    /// `current_user` per Postgres semantics. Only `SET SESSION AUTHORIZATION` writes both this and
    /// `effective_role`; other forms leave it untouched.
    session_user: SqlIdentifier,
    /// True when `effective_role` has `rolbypassrls` or is a superuser. Kept in the inner state so
    /// readers taking the read lock always see a consistent `(effective_role, bypass_rls)` tuple.
    bypass_rls: bool,
    /// Session-level GUCs set by `SET name = value`. Cleared by `DISCARD ALL` / `RESET ALL`.
    session_gucs: HashMap<String, String>,
    /// Transaction-local GUCs set by `SET LOCAL` and the PostgREST `set_config(name, value, true)`
    /// shape. Cleared on `COMMIT`, `ROLLBACK`, `DISCARD ALL`, and `RESET ALL`.
    trx_local_gucs: HashMap<String, String>,
    /// Parsed `request.jwt.claims` blob, kept so a keyed claim-path lookup can pull a single claim's
    /// value without re-parsing the JSON on every cache lookup.
    jwt_claims: Option<Arc<JsonValue>>,
    /// Session-scope role baseline that a transaction-local role write reverts to at transaction
    /// end: `SET LOCAL ROLE` (and `set_config('role', _, true)`) only changes the role for the
    /// current transaction, so `COMMIT` / `ROLLBACK` restore the session-scope role.
    session_role: SqlIdentifier,
    /// Bypass flag paired with `session_role`; restored alongside it.
    session_bypass_rls: bool,
    /// True when the current `effective_role` / `bypass_rls` came from a transaction-local role
    /// write and must revert to `session_role` / `session_bypass_rls` on transaction end.
    role_is_trx_local: bool,
    /// Transaction-scoped trust gap: a malformed `set_config(...)` batch whose calls were all
    /// transaction-local. Upstream may have applied some, so the mirror is untrustworthy until the
    /// transaction ends -- at which point the un-mirrored trx-local writes roll back upstream too.
    /// Cleared only by the transaction ending or `DISCARD ALL`; a clean `set_config(...)` batch does
    /// not clear it, since it re-establishes only the GUCs it names.
    trx_untrusted: bool,
    /// Session-scoped trust gap: a `SET SESSION AUTHORIZATION` (an identity change we cannot mirror)
    /// or a malformed `set_config(...)` batch with any session-scoped or unresolvable call. Survives
    /// transaction end and a clean `set_config(...)` batch, so it clears only at `DISCARD ALL` /
    /// `RESET ALL`. While either flag is set `rls_input_values` returns `None`, routing off-cache.
    session_untrusted: bool,
    /// Login-time snapshot of the connection role's default GUCs (`ALTER ROLE ... SET`). Postgres
    /// applies these at login keyed on the login role and freezes them, so this is captured once and
    /// never mutated. Consulted as the lowest-precedence tier when resolving a keyed GUC, so an unset
    /// session GUC projects the default Postgres would substitute into the policy rather than `None`.
    role_default_gucs: HashMap<String, String>,
    /// Whether the `role_default_gucs` snapshot is trustworthy: `false` when the loader could not
    /// read `pg_db_role_setting` at connection time, in which case an unset keyed GUC may be masking
    /// an unknown default and the cache lookup fails closed.
    role_defaults_available: bool,
}

/// Side effects of [`SessionContext::apply_set_statement`] that the caller must propagate before the
/// next cache lookup. The caller owns the role -> `bypass_rls` resolution because that depends on
/// the `readyset-rls` policy registry, held outside the adapter by the catalog poller.
#[derive(Debug, Clone)]
pub enum SetSessionEffect {
    /// Nothing role-affecting happened; the GUC update (if any) is already applied.
    None,
    /// `SET ROLE` (or `SET LOCAL ROLE`). The caller must look up `bypass_rls` for `role` and call
    /// [`SessionContext::set_effective_role`].
    RoleSet {
        role: SqlIdentifier,
        scope: Option<PostgresParameterScope>,
    },
    /// `SET ROLE DEFAULT`; already applied by reverting to `startup_user` with `bypass = false`.
    RoleReset,
}

fn identifier_value(v: &PostgresParameterValue) -> Option<SqlIdentifier> {
    match v {
        PostgresParameterValue::Single(inner) => match inner {
            PostgresParameterValueInner::Identifier(i) => Some(i.clone()),
            PostgresParameterValueInner::Literal(Literal::String(s)) => {
                Some(SqlIdentifier::from(s.as_str()))
            }
            _ => None,
        },
        PostgresParameterValue::List(_) => None,
    }
}

fn render_parameter_value(v: &PostgresParameterValue) -> String {
    match v {
        PostgresParameterValue::Single(inner) => render_inner(inner),
        PostgresParameterValue::List(items) => {
            items.iter().map(render_inner).collect::<Vec<_>>().join(",")
        }
    }
}

fn render_inner(inner: &PostgresParameterValueInner) -> String {
    match inner {
        PostgresParameterValueInner::Identifier(i) => i.as_str().to_owned(),
        PostgresParameterValueInner::Literal(Literal::String(s)) => s.clone(),
        PostgresParameterValueInner::Literal(lit) => lit.display(Dialect::PostgreSQL).to_string(),
    }
}

impl SessionContext {
    /// Build a session context for a newly-authenticated connection.
    ///
    /// `effective_role` and `session_user` start as copies of `startup_user`; subsequent `SET ROLE`
    /// / `SET SESSION AUTHORIZATION` writes update them. `bypass_rls` defaults to false. No
    /// role-default GUC snapshot is captured; use [`Self::with_role_defaults`] to supply one.
    pub fn new(startup_user: SqlIdentifier) -> Arc<Self> {
        Self::with_role_defaults(startup_user, HashMap::new(), false)
    }

    /// Build a session context with the login role's default-GUC snapshot.
    ///
    /// `role_default_gucs` is the connection role's `ALTER ROLE ... SET` defaults, captured once at
    /// login and frozen. `role_defaults_available` is `false` when the loader could not read the
    /// defaults, so the lookup fails closed on an unset keyed GUC.
    pub fn with_role_defaults(
        startup_user: SqlIdentifier,
        role_default_gucs: HashMap<String, String>,
        role_defaults_available: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(SessionInner {
                effective_role: startup_user.clone(),
                session_user: startup_user.clone(),
                bypass_rls: false,
                session_gucs: HashMap::new(),
                trx_local_gucs: HashMap::new(),
                jwt_claims: None,
                session_role: startup_user.clone(),
                session_bypass_rls: false,
                role_is_trx_local: false,
                trx_untrusted: false,
                session_untrusted: false,
                role_default_gucs,
                role_defaults_available,
            }),
            startup_user,
        })
    }

    /// Whether the login-time role-default snapshot was available. When `false`, an unset keyed GUC
    /// must fail closed (it may be masking an unknown role default).
    pub fn role_defaults_available(&self) -> bool {
        self.inner.read().role_defaults_available
    }

    /// Mark a transaction-scoped trust gap: a malformed `set_config(...)` batch whose calls were all
    /// transaction-local. Cleared when the transaction ends, since the un-mirrored trx-local writes
    /// roll back upstream too. While set, [`Self::rls_input_values`] returns `None`.
    pub fn mark_transaction_untrusted(&self) {
        self.inner.write().trx_untrusted = true;
    }

    /// Mark a session-scoped trust gap: a `SET SESSION AUTHORIZATION`, or a malformed
    /// `set_config(...)` batch with any session-scoped or unresolvable call. Survives transaction
    /// end and a clean `set_config(...)` batch, so it clears only at `DISCARD ALL` / `RESET ALL`.
    pub fn mark_session_untrusted(&self) {
        self.inner.write().session_untrusted = true;
    }

    /// `true` when either trust gap is set and cache lookups must route off-cache.
    pub fn is_untrusted(&self) -> bool {
        let inner = self.inner.read();
        inner.trx_untrusted || inner.session_untrusted
    }

    /// Set a session-scope GUC.
    pub fn set_session_guc(&self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let value = value.into();
        let mut inner = self.inner.write();
        update_jwt_claims_if_needed(&mut inner, &name, Some(&value));
        inner.session_gucs.insert(name, value);
    }

    /// Set a transaction-local GUC.
    pub fn set_trx_local_guc(&self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let value = value.into();
        let mut inner = self.inner.write();
        update_jwt_claims_if_needed(&mut inner, &name, Some(&value));
        inner.trx_local_gucs.insert(name, value);
    }

    /// Reset a single GUC across both scopes. Mirrors Postgres `RESET`: the variable returns to its
    /// boot-default rather than being left at any prior session value.
    pub fn reset_guc(&self, name: &str) {
        let mut inner = self.inner.write();
        update_jwt_claims_if_needed(&mut inner, name, None);
        inner.session_gucs.remove(name);
        inner.trx_local_gucs.remove(name);
    }

    /// Clear all session and transaction-local GUC state. Used for `DISCARD ALL` and `RESET ALL`.
    pub fn discard_all(&self) {
        let mut inner = self.inner.write();
        inner.session_gucs.clear();
        inner.trx_local_gucs.clear();
        inner.jwt_claims = None;
        inner.effective_role = self.startup_user.clone();
        inner.session_user = self.startup_user.clone();
        inner.bypass_rls = false;
        inner.session_role = self.startup_user.clone();
        inner.session_bypass_rls = false;
        inner.role_is_trx_local = false;
        inner.trx_untrusted = false;
        inner.session_untrusted = false;
    }

    /// Clear transaction-local state on `COMMIT` / `ROLLBACK`: drop trx-local GUCs, revert a
    /// transaction-local role back to the session-scope baseline so `SET LOCAL ROLE` does not leak
    /// past the transaction, and clear a transaction-scoped trust gap. A session-scoped gap
    /// (`SET SESSION AUTHORIZATION`) survives.
    pub fn on_trx_end(&self) {
        let mut inner = self.inner.write();
        // Clear the trust flag before the early return: a malformed trx-local batch applies no
        // mirror writes, so `trx_untrusted` can be set with no trx-local GUC or role to revert.
        inner.trx_untrusted = false;
        let had_trx_gucs = !inner.trx_local_gucs.is_empty();
        if !had_trx_gucs && !inner.role_is_trx_local {
            return;
        }
        if inner.role_is_trx_local {
            inner.effective_role = inner.session_role.clone();
            inner.bypass_rls = inner.session_bypass_rls;
            inner.role_is_trx_local = false;
        }
        if had_trx_gucs && inner.trx_local_gucs.contains_key("request.jwt.claims") {
            inner.jwt_claims = inner
                .session_gucs
                .get("request.jwt.claims")
                .and_then(|s| parse_jwt_claims(s));
        }
        inner.trx_local_gucs.clear();
    }

    /// Set the effective role at session scope. The `bypass` flag is the caller's pre-resolved view
    /// of whether the new role has `rolbypassrls = true` or is a superuser. Role and bypass update
    /// atomically with respect to readers because both writes happen under the same write guard.
    pub fn set_effective_role(&self, role: SqlIdentifier, bypass: bool) {
        self.set_effective_role_scoped(role, bypass, false);
    }

    /// Set the effective role, honouring `local` (the `SET LOCAL` / `set_config(..., is_local=true)`
    /// scope). A session-scope write (`local = false`) also moves the baseline that a later
    /// transaction-local write reverts to; a transaction-local write (`local = true`) leaves the
    /// baseline intact and marks the role for reversion at the next `COMMIT` / `ROLLBACK`.
    pub fn set_effective_role_scoped(&self, role: SqlIdentifier, bypass: bool, local: bool) {
        {
            let mut inner = self.inner.write();
            inner.effective_role = role.clone();
            inner.bypass_rls = bypass;
            if local {
                inner.role_is_trx_local = true;
            } else {
                inner.session_role = role;
                inner.session_bypass_rls = bypass;
                inner.role_is_trx_local = false;
            }
        }
    }

    /// Set both `session_user` and `effective_role`, matching `SET SESSION AUTHORIZATION` semantics.
    /// The role tuple updates atomically with `bypass_rls` under the same write guard.
    pub fn set_session_authorization(&self, role: SqlIdentifier, bypass: bool) {
        {
            let mut inner = self.inner.write();
            inner.session_user = role.clone();
            inner.effective_role = role.clone();
            inner.bypass_rls = bypass;
            inner.session_role = role;
            inner.session_bypass_rls = bypass;
            inner.role_is_trx_local = false;
        }
    }

    /// `true` when the current effective role can bypass RLS.
    pub fn bypass_rls(&self) -> bool {
        self.inner.read().bypass_rls
    }

    /// Apply a parsed Postgres `SET` statement to this session.
    ///
    /// Handles `SET name = value`, `SET SESSION name = value`, `SET LOCAL name = value`, and the
    /// `DEFAULT` form (treated as `RESET name`). `SET ROLE` and `SET SESSION AUTHORIZATION` are
    /// dispatched through the role hooks. Returns the new role name (without bypass resolution) when
    /// one is set; the caller re-resolves `bypass_rls` against an external registry.
    pub fn apply_set_statement(&self, set: &SetStatement) -> SetSessionEffect {
        let param = match set {
            SetStatement::PostgresParameter(param) => param,
            // `SET NAMES` only affects client_encoding, which no RLS policy reads, so ignoring it
            // cannot desync the partition mirror.
            SetStatement::Names(_) => return SetSessionEffect::None,
            // `SET SESSION AUTHORIZATION` is mirrored after upstream accepts it, at the right scope
            // (transaction-local for `SET LOCAL`, so it clears on COMMIT). Leave it untouched here;
            // a session-scoped untrust would outlive the transaction and wrongly keep it off-cache.
            SetStatement::SessionAuthorization(_) => return SetSessionEffect::None,
            // A SET we do not model (MySQL-style `Variable`) could change an identity a policy keys
            // on that we cannot mirror, so fail closed and route off-cache rather than key against a
            // stale partition. Only reachable on a Postgres session, where this form should not
            // appear.
            SetStatement::Variable(_) => {
                self.mark_session_untrusted();
                return SetSessionEffect::None;
            }
        };
        let SetPostgresParameter { scope, name, value } = param;
        let lower = name.as_str().to_ascii_lowercase();

        if lower == "role" {
            let local = matches!(scope, Some(PostgresParameterScope::Local));
            return match value {
                SetPostgresParameterValue::Default => {
                    self.set_effective_role_scoped(self.startup_user.clone(), false, local);
                    SetSessionEffect::RoleReset
                }
                SetPostgresParameterValue::Value(v) => match identifier_value(v) {
                    Some(role) => SetSessionEffect::RoleSet {
                        role,
                        scope: *scope,
                    },
                    None => SetSessionEffect::None,
                },
            };
        }

        if lower == "session" {
            // `SET SESSION AUTHORIZATION` is modeled as its own `SetStatement::SessionAuthorization`
            // and never reaches here. This guards the hypothetical `SET session = ...` parameter
            // form: we cannot resolve its identity/bypass cleanly, so fail closed.
            self.mark_session_untrusted();
            return SetSessionEffect::None;
        }

        match value {
            SetPostgresParameterValue::Default => {
                self.reset_guc(&lower);
                SetSessionEffect::None
            }
            SetPostgresParameterValue::Value(v) => {
                let rendered = render_parameter_value(v);
                match scope {
                    Some(PostgresParameterScope::Local) => {
                        self.set_trx_local_guc(lower, rendered);
                    }
                    _ => {
                        self.set_session_guc(lower, rendered);
                    }
                }
                SetSessionEffect::None
            }
        }
    }

    /// Apply `SET SESSION AUTHORIZATION <role>`. The caller pre-resolves `bypass`; otherwise
    /// symmetric to [`Self::set_session_authorization`].
    pub fn apply_session_authorization(&self, role: SqlIdentifier, bypass: bool) {
        self.set_session_authorization(role, bypass);
    }

    /// If `set` is `SET [LOCAL] ROLE <role>` for a concrete role, return the role and whether the
    /// change is transaction-local. This is the one `SET` form whose mirroring must wait for
    /// upstream to accept it: role membership is an authorization boundary, so a rejected
    /// `SET ROLE` must not advance `effective_role`. `RESET ROLE` / `SET ROLE DEFAULT`, GUC sets,
    /// and `SET SESSION AUTHORIZATION` return `None` -- the first two are applied eagerly by
    /// [`Self::apply_set_statement`] (they cannot be rejected as an authorization decision) and the
    /// last has its own post-acceptance mirror.
    pub fn pending_set_role(set: &SetStatement) -> Option<(SqlIdentifier, bool)> {
        let SetStatement::PostgresParameter(SetPostgresParameter { scope, name, value }) = set
        else {
            return None;
        };
        if !name.as_str().eq_ignore_ascii_case("role") {
            return None;
        }
        match value {
            SetPostgresParameterValue::Value(v) => {
                let role = identifier_value(v)?;
                Some((role, matches!(scope, Some(PostgresParameterScope::Local))))
            }
            SetPostgresParameterValue::Default => None,
        }
    }

    /// Resolve this session's values for a cache's keyed input set, or `None` when the session
    /// cannot be keyed safely (untrusted mirror) and the lookup must route off-cache. The registry
    /// generation is not part of the result -- the coordinator stamps it per lookup.
    pub fn rls_input_values(
        &self,
        inputs: &[SessionInputType],
    ) -> Option<Arc<[SessionInputValue]>> {
        // Read role + bypass + trust under the same lock as the projection so a concurrent SET ROLE
        // or failed set_config batch never produces a mismatched tuple. Either trust gap routes
        // off-cache (None); a bypass session collapses to the canonical shared bypass marker.
        let inner = self.inner.read();
        if inner.trx_untrusted || inner.session_untrusted {
            None
        } else if inner.bypass_rls {
            Some(Arc::from(vec![SessionInputValue::Bypass]))
        } else {
            Some(Arc::from(compute_input_values(&inner, inputs)))
        }
    }

    /// Snapshot of the effective role (cloned under the read lock).
    pub fn effective_role(&self) -> SqlIdentifier {
        self.inner.read().effective_role.clone()
    }

    /// Snapshot of the value of a session-scope GUC, if set.
    pub fn session_guc(&self, name: &str) -> Option<String> {
        self.inner.read().session_gucs.get(name).cloned()
    }

    /// Snapshot of the value of a transaction-local GUC, if set.
    pub fn trx_local_guc(&self, name: &str) -> Option<String> {
        self.inner.read().trx_local_gucs.get(name).cloned()
    }

    /// `true` if any transaction-local GUC is currently set.
    pub fn has_trx_local_gucs(&self) -> bool {
        !self.inner.read().trx_local_gucs.is_empty()
    }

    /// `true` if any session-scope GUC is currently set.
    pub fn has_session_gucs(&self) -> bool {
        !self.inner.read().session_gucs.is_empty()
    }
}

fn update_jwt_claims_if_needed(inner: &mut SessionInner, name: &str, value: Option<&str>) {
    if name != "request.jwt.claims" {
        return;
    }
    inner.jwt_claims = value.and_then(parse_jwt_claims);
}

/// Project the per-session state into the values a scoped cache keys on. The caller already holds
/// the inner read guard so role, bypass, and GUC values are mutually consistent. Every non-bypass
/// scoped key folds in the effective role and session user regardless of what the policy reads.
fn compute_input_values(
    inner: &SessionInner,
    inputs: &[SessionInputType],
) -> Vec<SessionInputValue> {
    let mut out = Vec::with_capacity(inputs.len() + 2);
    out.push(SessionInputValue::Role(inner.effective_role.clone()));
    out.push(SessionInputValue::SessionUser(inner.session_user.clone()));
    for input in inputs {
        let value = resolve_input_value(inner, input).map(String::into_boxed_str);
        out.push(SessionInputValue::Rls(input.clone(), value));
    }
    out
}

/// Resolve a keyed session input to its value, reconciling the two JWT-claim representations
/// PostgREST can use: the JSON blob `request.jwt.claims` addressed by claim path segments
/// ([`SessionInputType::JwtClaim`]), and the flattened per-claim GUC (`request.jwt.claim.sub`).
///
/// A policy reads one form; the session may have set the other. Each input is resolved against its
/// native representation first, then the other; if both fail the value is `None`. A mismatch
/// over-partitions rather than collapsing every session onto a shared `None`, which would serve one
/// tenant's rows to another. Transaction-local GUCs take precedence, matching `current_setting`.
fn resolve_input_value(inner: &SessionInner, input: &SessionInputType) -> Option<String> {
    match input {
        // A whole-blob read keys on the raw GUC value.
        SessionInputType::JwtClaim(segments) if segments.is_empty() => {
            lookup_guc(inner, "request.jwt.claims")
        }
        SessionInputType::JwtClaim(segments) => {
            if let Some(v) = inner
                .jwt_claims
                .as_ref()
                .and_then(|c| canonical_lookup(c, segments.iter().map(|s| &**s)))
            {
                return Some(v);
            }
            // Fall back to the flattened GUC form, e.g. `["sub"]` -> `request.jwt.claim.sub`.
            lookup_guc(inner, &flattened_claim_name(segments))
        }
        SessionInputType::Guc(name) => {
            if let Some(suffix) = name.strip_prefix("request.jwt.claim.") {
                if let Some(v) = lookup_guc(inner, name) {
                    return Some(v);
                }
                // Fall back to the JSON blob, e.g. `request.jwt.claim.sub` -> claim path `sub`
                // (dotted suffix maps to nested keys).
                return inner
                    .jwt_claims
                    .as_ref()
                    .and_then(|c| canonical_lookup(c, suffix.split('.')));
            }
            lookup_guc(inner, name)
        }
    }
}

/// Resolve a GUC name through the precedence Postgres `current_setting` uses: transaction-local
/// (`SET LOCAL`), then session (`SET`), then the login-time role default. The role-default tier lets
/// an unset session GUC project the value Postgres would substitute into the policy rather than
/// `None`.
fn lookup_guc(inner: &SessionInner, name: &str) -> Option<String> {
    inner
        .trx_local_gucs
        .get(name)
        .or_else(|| inner.session_gucs.get(name))
        .or_else(|| inner.role_default_gucs.get(name))
        .cloned()
}

/// Convert claim path segments into the flattened GUC name: the segments joined with `.` under the
/// `request.jwt.claim.` root.
fn flattened_claim_name(segments: &[Box<str>]) -> String {
    let mut out = String::from("request.jwt.claim.");
    for (i, seg) in segments.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(seg);
    }
    out
}

fn parse_jwt_claims(raw: &str) -> Option<Arc<JsonValue>> {
    serde_json::from_str::<JsonValue>(raw).ok().map(Arc::new)
}

/// Walk `value` along the claim path `segments` and return a canonical string representation of the
/// value found there. Numbers normalise via `serde_json::Number::to_string`; strings pass through;
/// nested arrays/objects serialise with sorted keys so semantically-equal claim values hash equal.
fn canonical_lookup<'a>(
    value: &JsonValue,
    segments: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let mut cur = value;
    for segment in segments {
        cur = cur.get(segment)?;
    }
    Some(canonicalise(cur))
}

fn canonicalise(value: &JsonValue) -> String {
    let mut buf = String::new();
    write_canonical(&mut buf, value);
    buf
}

fn write_canonical(buf: &mut String, value: &JsonValue) {
    use std::fmt::Write;
    match value {
        JsonValue::Null => buf.push_str("null"),
        JsonValue::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
        JsonValue::Number(n) => {
            let _ = write!(buf, "{n}");
        }
        JsonValue::String(s) => {
            buf.push('"');
            for c in s.chars() {
                match c {
                    '"' => buf.push_str("\\\""),
                    '\\' => buf.push_str("\\\\"),
                    _ => buf.push(c),
                }
            }
            buf.push('"');
        }
        JsonValue::Array(items) => {
            buf.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_canonical(buf, item);
            }
            buf.push(']');
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            buf.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                buf.push('"');
                buf.push_str(key);
                buf.push_str("\":");
                write_canonical(buf, &map[*key]);
            }
            buf.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Arc<SessionContext> {
        SessionContext::new(SqlIdentifier::from("authenticator"))
    }

    fn guc(name: &str) -> Arc<[SessionInputType]> {
        Arc::from(vec![SessionInputType::guc(name)])
    }

    fn claim(segments: &[&str]) -> Arc<[SessionInputType]> {
        Arc::from(vec![SessionInputType::jwt_claim(segments)])
    }

    /// Unwrap a keyed (non-bypass, trusted) partition for a session and
    /// input set.
    fn part(s: &SessionContext, g: &Arc<[SessionInputType]>) -> Arc<[SessionInputValue]> {
        s.rls_input_values(g).expect("session should be keyable")
    }

    #[test]
    fn fresh_session_partition_matches_across_calls() {
        let s = ctx();
        let g = claim(&["sub"]);
        assert_eq!(part(&s, &g), part(&s, &g));
    }

    #[test]
    fn distinct_jwt_claims_produce_distinct_partitions() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"sub":"alice","role":"authenticated","exp":4102444800}"#,
        );
        b.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"sub":"bob","role":"authenticated","exp":4102444800}"#,
        );

        let g = claim(&["sub"]);
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    /// Two JWTs with the same `sub` but different rotated timestamps
    /// (`iat`/`exp`) project to equal partitions when only `sub` is
    /// keyed.
    #[test]
    fn rotated_jwt_with_same_keyed_claim_partitions_equal() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc("request.jwt.claims", r#"{"sub":"alice","iat":1,"exp":2}"#);
        b.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"sub":"alice","iat":999,"exp":1000}"#,
        );

        let g = claim(&["sub"]);
        assert_eq!(part(&a, &g), part(&b, &g));
    }

    /// Object-key reorder in the raw JWT canonicalises to the same
    /// projected value.
    #[test]
    fn jwt_object_key_order_canonicalises() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"sub":"alice","role":"authenticated"}"#,
        );
        b.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"role":"authenticated","sub":"alice"}"#,
        );

        let g = claim(&["sub"]);
        assert_eq!(part(&a, &g), part(&b, &g));
    }

    /// Empty-string GUC and unset GUC must produce distinct partitions.
    #[test]
    fn empty_guc_vs_unset_guc_partitions_distinctly() {
        let empty = ctx();
        let unset = ctx();
        empty.set_session_guc("app.tenant_id", "");

        let g = guc("app.tenant_id");
        assert_ne!(part(&empty, &g), part(&unset, &g));
    }

    /// A login-time role default fills an unset session GUC, and the explicit
    /// SET tiers (session, then trx-local) take precedence over it.
    #[test]
    fn role_default_guc_fills_and_is_overridden() {
        let mut defaults = HashMap::new();
        defaults.insert("app.tenant_id".to_string(), "acme".to_string());
        let s = SessionContext::with_role_defaults(
            SqlIdentifier::from("authenticator"),
            defaults,
            true,
        );
        assert!(s.role_defaults_available());

        let g = guc("app.tenant_id");

        // Unset session GUC resolves to the role default: the same partition as
        // an explicit `SET app.tenant_id = 'acme'` on an otherwise-equal session.
        let from_default = part(&s, &g);
        let explicit = ctx();
        explicit.set_session_guc("app.tenant_id", "acme");
        assert_eq!(from_default, part(&explicit, &g));

        // A session SET overrides the default.
        s.set_session_guc("app.tenant_id", "globex");
        let from_session = part(&s, &g);
        assert_ne!(from_default, from_session);

        // A trx-local SET overrides the session value.
        s.set_trx_local_guc("app.tenant_id", "initech");
        assert_ne!(from_session, part(&s, &g));
    }

    /// Two caches with disjoint keyed input sets project to distinct
    /// partitions on the same session because each cache's partition
    /// is built over its own input set.
    #[test]
    fn disjoint_input_sets_do_not_alias() {
        let s = ctx();
        s.set_session_guc("a", "1");
        s.set_session_guc("b", "2");

        let just_a = guc("a");
        let just_b = guc("b");
        assert_ne!(part(&s, &just_a), part(&s, &just_b));
    }

    #[test]
    fn bypass_rls_uses_bypass_partition() {
        let s = ctx();
        s.set_effective_role(SqlIdentifier::from("postgres"), true);
        let g = claim(&["sub"]);
        let p = part(&s, &g);
        assert_eq!(&*p, &[SessionInputValue::Bypass]);
    }

    /// A session mutation between two lookups changes the projected
    /// values, so the two lookups produce distinct partitions.
    #[test]
    fn session_mutation_changes_partition() {
        let s = ctx();
        let g = guc("app.x");
        s.set_session_guc("app.x", "before");
        let before = part(&s, &g);
        s.set_session_guc("app.x", "after");
        let after = part(&s, &g);
        assert_ne!(before, after);
    }

    /// An untrusted state mirror routes off-cache: `rls_input_values`
    /// returns `None` until the trust gap is cleared.
    #[test]
    fn untrusted_state_routes_off_cache() {
        let s = ctx();
        let g = guc("app.x");
        s.set_session_guc("app.x", "v");
        assert!(s.rls_input_values(&g).is_some());
        s.mark_session_untrusted();
        assert!(s.rls_input_values(&g).is_none());
        s.discard_all();
        assert!(s.rls_input_values(&g).is_some());
    }

    /// `SET NAMES` does not touch anything a policy reads, so it is ignored
    /// and leaves the session cacheable.
    #[test]
    fn set_names_is_ignored_and_keeps_session_trusted() {
        let s = ctx();
        let g = guc("app.x");
        s.set_session_guc("app.x", "v");
        let effect = s.apply_set_statement(&SetStatement::Names(readyset_sql::ast::SetNames {
            charset: "utf8".to_string(),
            collation: None,
        }));
        assert!(matches!(effect, SetSessionEffect::None));
        assert!(s.rls_input_values(&g).is_some());
    }

    /// An unmodeled SET could change a policy-relevant identity we cannot
    /// mirror, so the session fails closed and routes off-cache.
    #[test]
    fn unmodeled_set_marks_session_untrusted() {
        let s = ctx();
        let g = guc("app.x");
        s.set_session_guc("app.x", "v");
        assert!(s.rls_input_values(&g).is_some());
        let effect =
            s.apply_set_statement(&SetStatement::Variable(readyset_sql::ast::SetVariables {
                variables: vec![],
            }));
        assert!(matches!(effect, SetSessionEffect::None));
        assert!(s.rls_input_values(&g).is_none());
    }

    /// `SET [LOCAL] SESSION AUTHORIZATION` is handled by the dispatch's
    /// `mirror_session_authorization`, which scopes the untrust correctly.
    /// `apply_set_statement` must leave the session alone -- a session-scoped
    /// untrust here would outlive the transaction and break the `SET LOCAL`
    /// case, whose trust must return at COMMIT.
    #[test]
    fn session_authorization_does_not_untrust_here() {
        for local in [false, true] {
            let s = ctx();
            let g = guc("app.x");
            s.set_session_guc("app.x", "v");
            let stmt =
                SetStatement::SessionAuthorization(readyset_sql::ast::SetSessionAuthorization {
                    local,
                    value: readyset_sql::ast::SessionAuthorizationValue::Default,
                });
            assert!(matches!(
                s.apply_set_statement(&stmt),
                SetSessionEffect::None
            ));
            assert!(s.rls_input_values(&g).is_some(), "local={local}");
        }
    }

    /// A transaction-scoped trust gap is cleared when the transaction
    /// ends -- even when the malformed batch applied no trx-local mirror
    /// writes, so `on_trx_end` must clear it before its early return.
    #[test]
    fn on_trx_end_clears_transaction_scoped_untrust() {
        let s = ctx();
        let g = guc("app.x");
        s.set_session_guc("app.x", "v");
        s.mark_transaction_untrusted();
        assert!(s.is_untrusted());
        assert!(s.rls_input_values(&g).is_none());
        s.on_trx_end();
        assert!(!s.is_untrusted());
        assert!(s.rls_input_values(&g).is_some());
    }

    /// A session-scoped trust gap (`SET SESSION AUTHORIZATION`) survives both
    /// transaction end and a clean batch; only `DISCARD ALL` / `RESET ALL`
    /// (modeled by `discard_all`) clears it.
    #[test]
    fn session_scoped_untrust_clears_only_on_discard_all() {
        let s = ctx();
        s.mark_session_untrusted();
        s.on_trx_end();
        assert!(s.is_untrusted());
        s.discard_all();
        assert!(!s.is_untrusted());
    }

    /// Both scopes can be set at once (session-scoped authorization change
    /// then a malformed trx-local batch). `on_trx_end` clears only the
    /// transaction-scoped gap; the session-scoped one persists until
    /// `discard_all`.
    #[test]
    fn on_trx_end_clears_only_transaction_scope() {
        let s = ctx();
        s.mark_session_untrusted();
        s.mark_transaction_untrusted();
        assert!(s.is_untrusted());
        s.on_trx_end();
        assert!(s.is_untrusted());
        s.discard_all();
        assert!(!s.is_untrusted());
    }

    /// JWT claim keys that contain `.` or `/` survive projection
    /// without aliasing. Two sessions with different OIDC-namespace
    /// claim values produce distinct partitions.
    #[test]
    fn jwt_claim_with_dotted_key_does_not_alias() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"https://example.com/role":"admin"}"#,
        );
        b.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"https://example.com/role":"viewer"}"#,
        );
        // Analyzer-emitted form for `auth.jwt() ->> 'https://example.com/role'`
        let g = claim(&["https://example.com/role"]);
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    /// Two distinct claim keys must NOT collide when one is the prefix
    /// of the other after dot-splitting. A key literally named
    /// `app_metadata.tenant` stays distinct from the nested
    /// `app_metadata` -> `tenant` path because the claim path segments
    /// address them separately.
    #[test]
    fn jwt_claim_nested_and_flat_keys_are_distinct() {
        let g_nested = claim(&["app_metadata", "tenant"]);
        let g_flat = claim(&["app_metadata.tenant"]);
        let mix = ctx();
        mix.set_trx_local_guc(
            "request.jwt.claims",
            r#"{"app_metadata":{"tenant":"nested"},"app_metadata.tenant":"flat"}"#,
        );
        let nested_id = part(&mix, &g_nested);
        let flat_id = part(&mix, &g_flat);
        assert_ne!(nested_id, flat_id);
    }

    /// Policy keyed on the claim-path form (`auth.uid()` ->
    /// `JwtClaim(["sub"])`) but the session populated only the
    /// flattened GUC `request.jwt.claim.sub`. Without reconciliation
    /// both sessions would project `None` and collapse onto one
    /// partition (cross-tenant leak); the fallback keeps them distinct.
    #[test]
    fn jwt_claim_policy_resolves_flattened_guc_fallback() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc("request.jwt.claim.sub", "alice");
        b.set_trx_local_guc("request.jwt.claim.sub", "bob");
        let g = claim(&["sub"]);
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    /// Policy keyed on the flattened form
    /// (`current_setting('request.jwt.claim.sub', true)`) but the
    /// session populated only the `request.jwt.claims` JSON blob. The
    /// fallback resolves through the blob so the partitions stay
    /// distinct rather than collapsing.
    #[test]
    fn flattened_policy_resolves_json_blob_fallback() {
        let a = ctx();
        let b = ctx();
        a.set_trx_local_guc("request.jwt.claims", r#"{"sub":"alice"}"#);
        b.set_trx_local_guc("request.jwt.claims", r#"{"sub":"bob"}"#);
        let g = guc("request.jwt.claim.sub");
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    /// Invariant: a scoped partition always keys on the effective role, even
    /// when the keyed input set reads nothing about the role. The analyzer
    /// emits no role signal precisely because this holds unconditionally; pin
    /// it so a future "key on role only when the policy reads it" optimization
    /// cannot silently merge distinct roles onto one partition.
    #[test]
    fn partition_always_keys_on_effective_role() {
        let g = guc("app.x");
        let a = ctx();
        let b = ctx();
        a.set_session_guc("app.x", "v");
        b.set_session_guc("app.x", "v");
        a.set_effective_role(SqlIdentifier::from("role_a"), false);
        b.set_effective_role(SqlIdentifier::from("role_b"), false);
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    /// Invariant: a scoped partition always keys on the session user, so two
    /// sessions with the same effective role but different login identities do
    /// not share a partition.
    #[test]
    fn partition_always_keys_on_session_user() {
        let g = guc("app.x");
        let a = SessionContext::new(SqlIdentifier::from("login_a"));
        let b = SessionContext::new(SqlIdentifier::from("login_b"));
        a.set_session_guc("app.x", "v");
        b.set_session_guc("app.x", "v");
        a.set_effective_role(SqlIdentifier::from("shared_role"), false);
        b.set_effective_role(SqlIdentifier::from("shared_role"), false);
        assert_ne!(part(&a, &g), part(&b, &g));
    }

    #[test]
    fn discard_all_reverts_role_and_clears_gucs() {
        let s = ctx();
        s.set_session_guc("a", "1");
        s.set_trx_local_guc("b", "2");
        s.set_effective_role(SqlIdentifier::from("authenticated"), false);
        s.discard_all();

        assert_eq!(s.effective_role().as_str(), "authenticator");
        assert!(!s.has_session_gucs());
        assert!(!s.has_trx_local_gucs());
    }

    #[test]
    fn trx_local_gucs_cleared_on_trx_end() {
        let s = ctx();
        s.set_trx_local_guc("a", "1");
        s.on_trx_end();
        assert!(!s.has_trx_local_gucs());
    }

    /// A transaction-local role (SET LOCAL ROLE / set_config with
    /// is_local=true) reverts to the session-scope baseline on
    /// transaction end, taking its bypass flag with it. Without this
    /// the role and its bypass would leak onto the next query on a
    /// pooled connection.
    #[test]
    fn trx_local_role_reverts_on_trx_end() {
        let s = ctx();
        s.set_effective_role(SqlIdentifier::from("app_user"), false);
        s.set_effective_role_scoped(SqlIdentifier::from("tenant_a"), true, true);
        assert_eq!(s.effective_role().as_str(), "tenant_a");
        assert!(s.bypass_rls());

        s.on_trx_end();
        assert_eq!(
            s.effective_role().as_str(),
            "app_user",
            "trx-local role must revert to the session baseline"
        );
        assert!(!s.bypass_rls(), "bypass must revert with the role");
    }

    /// A session-scope role write (no LOCAL) survives transaction end.
    #[test]
    fn session_scope_role_persists_across_trx_end() {
        let s = ctx();
        s.set_effective_role_scoped(SqlIdentifier::from("app_user"), false, false);
        s.on_trx_end();
        assert_eq!(s.effective_role().as_str(), "app_user");
        assert!(!s.bypass_rls());
    }

    #[test]
    fn reset_guc_changes_partition() {
        let s = ctx();
        s.set_session_guc("a", "1");
        let g = guc("a");
        let before = part(&s, &g);
        s.reset_guc("a");
        let after = part(&s, &g);
        assert_ne!(before, after);
    }

    fn pg_set(name: &str, scope: Option<PostgresParameterScope>, value: &str) -> SetStatement {
        SetStatement::PostgresParameter(SetPostgresParameter {
            scope,
            name: SqlIdentifier::from(name),
            value: SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                PostgresParameterValueInner::Literal(Literal::String(value.to_string())),
            )),
        })
    }

    #[test]
    fn apply_set_session_writes_session_guc() {
        let s = ctx();
        let stmt = pg_set("app.tenant_id", None, "t1");
        assert!(matches!(
            s.apply_set_statement(&stmt),
            SetSessionEffect::None
        ));
        assert_eq!(s.session_guc("app.tenant_id"), Some("t1".to_string()));
    }

    #[test]
    fn apply_set_local_writes_trx_local_guc() {
        let s = ctx();
        let stmt = pg_set("app.tenant_id", Some(PostgresParameterScope::Local), "t2");
        s.apply_set_statement(&stmt);
        assert_eq!(s.trx_local_guc("app.tenant_id"), Some("t2".to_string()));
        assert!(s.session_guc("app.tenant_id").is_none());
    }

    #[test]
    fn apply_set_role_returns_role_set_effect() {
        let s = ctx();
        let stmt = pg_set("role", None, "authenticated");
        match s.apply_set_statement(&stmt) {
            SetSessionEffect::RoleSet { role, scope } => {
                assert_eq!(role.as_str(), "authenticated");
                assert_eq!(scope, None);
            }
            other => panic!("unexpected effect {other:?}"),
        }
    }

    #[test]
    fn apply_set_role_default_reverts_to_startup_user_and_clears_bypass() {
        let s = ctx();
        // Arm with bypass=true so the post-revert assertion is
        // actually load-bearing: if the revert code regresses to
        // leaving bypass at its prior value, this test fails.
        s.set_effective_role(SqlIdentifier::from("postgres"), true);
        assert!(s.bypass_rls());

        let stmt = SetStatement::PostgresParameter(SetPostgresParameter {
            scope: None,
            name: SqlIdentifier::from("role"),
            value: SetPostgresParameterValue::Default,
        });
        assert!(matches!(
            s.apply_set_statement(&stmt),
            SetSessionEffect::RoleReset
        ));
        assert_eq!(s.effective_role().as_str(), "authenticator");
        assert!(
            !s.bypass_rls(),
            "SET ROLE DEFAULT must clear the bypass flag along with the role"
        );
    }

    #[test]
    fn apply_set_default_resets_keyed_guc() {
        let s = ctx();
        s.set_session_guc("app.tenant_id", "t1");
        assert_eq!(s.session_guc("app.tenant_id"), Some("t1".to_string()));

        // `RESET app.tenant_id` is modeled as `SET app.tenant_id = DEFAULT`;
        // applying it must clear the mirrored value so a subsequent lookup no
        // longer keys against the prior tenant's partition.
        let stmt = SetStatement::PostgresParameter(SetPostgresParameter {
            scope: Some(PostgresParameterScope::Session),
            name: SqlIdentifier::from("app.tenant_id"),
            value: SetPostgresParameterValue::Default,
        });
        assert!(matches!(
            s.apply_set_statement(&stmt),
            SetSessionEffect::None
        ));
        assert!(s.session_guc("app.tenant_id").is_none());
    }
}
