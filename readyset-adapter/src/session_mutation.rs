//! Recognise prepared statements that mutate per-connection session state, and apply the mutation
//! at execute time.
//!
//! The shallow-cache and proxy prepare paths null out `PreparedStatement::parsed_query`, so a hook
//! at execute time cannot see the AST. That hides session-mutating statements from the cache key
//! path, a correctness problem under RLS: PostgREST sets the bound JWT via such a prepared
//! statement before every user query, and missing it serves one tenant's rows to another.
//!
//! So we recognise the shape at prepare time (where the parsed AST is still in hand) into a typed
//! [`SessionMutationTemplate`] and call [`apply`] at execute time once the bind parameters are
//! known. The enum is the extension point: other Postgres shapes plug in as new variants without
//! changing the prepare/execute plumbing.
//!
//! PostgREST is the only client that exercises this path today; it sends a `SELECT set_config(...)`
//! batch:
//!
//! ```sql
//! SELECT set_config('search_path',        $1, true),
//!        set_config('role',               $2, true),
//!        set_config('request.jwt.claims', $3, true),
//!        ...
//! ```

use std::sync::Arc;

use readyset_data::DfValue;
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, FunctionExpr, ItemPlaceholder, LimitClause, Literal,
    SelectStatement, SqlQuery,
};

use crate::session_context::SessionContext;

/// A recognised prepared-statement shape that mutates [`SessionContext`] when executed. The
/// recognizer produces one at prepare time; the applier consumes it at execute time with the bound
/// parameter vector.
#[derive(Debug, Clone)]
pub enum SessionMutationTemplate {
    /// PostgREST `SELECT set_config(name, $N, is_local), ...` batch.
    SetConfig(Arc<[SetConfigCall]>),
}

/// A single `set_config('<NAME>', <value>, <is_local>)` call in the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetConfigCall {
    /// The GUC name (first argument, must be a string literal).
    pub guc_name: String,
    /// The `value` argument: a bound parameter (extended protocol, the
    /// PostgREST form) or an inline string literal (simple/text protocol,
    /// the form a `psql` user types).
    pub value: ValueSource,
    /// The `is_local` flag, either a constant or another bound
    /// parameter index.
    pub is_local: IsLocalSpec,
}

/// The source of a `set_config` argument that resolves to a value at
/// apply time: a bound parameter index or an inline string literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueSource {
    Param(usize),
    Literal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsLocalSpec {
    Literal(bool),
    Param(usize),
}

/// Decide whether `query` is a recognised session-mutating shape. `None` means the caller falls
/// through to normal evaluation and stores nothing on the prepared statement.
pub fn recognize(query: &SqlQuery) -> Option<SessionMutationTemplate> {
    extract_set_config_batch(query).map(SessionMutationTemplate::SetConfig)
}

/// Recognise the `SELECT set_config(...)` batch: returns the per-call vector when every projected
/// expression is `set_config('<name>', $N|'literal', <bool>|$M)` and the SELECT has no FROM clause;
/// `None` otherwise.
fn extract_set_config_batch(query: &SqlQuery) -> Option<Arc<[SetConfigCall]>> {
    let SqlQuery::Select(SelectStatement {
        fields,
        tables,
        where_clause,
        group_by,
        having,
        order,
        limit_clause,
        distinct,
        ..
    }) = query
    else {
        return None;
    };

    // PostgREST's batch is a flat projection. Anything with FROM/WHERE/GROUP BY/DISTINCT/LIMIT/
    // ORDER is a hand-crafted query that happens to mention set_config; play safe and refuse.
    let limit_clause_present = match limit_clause {
        LimitClause::LimitOffset { limit, offset } => limit.is_some() || offset.is_some(),
        LimitClause::OffsetCommaLimit { .. } => true,
    };
    if !tables.is_empty()
        || where_clause.is_some()
        || group_by.is_some()
        || having.is_some()
        || order.is_some()
        || limit_clause_present
        || *distinct
    {
        return None;
    }

    let mut calls = Vec::with_capacity(fields.len());
    for field in fields {
        let FieldDefinitionExpr::Expr { expr, .. } = field else {
            return None;
        };
        let call = extract_call(expr)?;
        calls.push(call);
    }

    if calls.is_empty() {
        return None;
    }

    Some(Arc::from(calls.into_boxed_slice()))
}

fn extract_call(expr: &Expr) -> Option<SetConfigCall> {
    let Expr::Call(FunctionExpr::Udf {
        schema,
        name,
        arguments,
    }) = expr
    else {
        return None;
    };

    // Accept unqualified `set_config(...)` and schema-qualified `pg_catalog.set_config(...)` (many
    // drivers emit the latter); reject any other schema.
    if let Some(s) = schema.as_ref()
        && !s.as_str().eq_ignore_ascii_case("pg_catalog")
    {
        return None;
    }
    if !name.as_str().eq_ignore_ascii_case("set_config") {
        return None;
    }
    if arguments.len() != 3 {
        return None;
    }

    let guc_name = match &arguments[0] {
        Expr::Literal(Literal::String(s)) => s.clone(),
        _ => return None,
    };
    let value = value_source(&arguments[1])?;
    let is_local = is_local_spec(&arguments[2])?;

    Some(SetConfigCall {
        guc_name,
        value,
        is_local,
    })
}

fn value_source(expr: &Expr) -> Option<ValueSource> {
    match expr {
        Expr::Literal(Literal::String(s)) => Some(ValueSource::Literal(s.clone())),
        _ => placeholder_index(expr).map(ValueSource::Param),
    }
}

fn placeholder_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(n))) => {
            usize::try_from(*n).ok().and_then(|n| n.checked_sub(1))
        }
        _ => None,
    }
}

fn is_local_spec(expr: &Expr) -> Option<IsLocalSpec> {
    match expr {
        Expr::Literal(Literal::Boolean(b)) => Some(IsLocalSpec::Literal(*b)),
        Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(n))) => {
            usize::try_from(*n)
                .ok()
                .and_then(|n| n.checked_sub(1))
                .map(IsLocalSpec::Param)
        }
        _ => None,
    }
}

/// Apply a recognised template to the session using the execute-time parameter vector.
///
/// Two-pass: validate every call before applying any. A well-formed batch applies its calls in
/// order but does not re-trust the session (an existing untrusted gap clears only at its scope
/// boundary). A malformed batch leaves the mirror unchanged and marks the session untrusted so
/// cache lookups route off-cache -- upstream may have applied some calls anyway, leaving our mirror
/// inconsistent with what RLS sees. The gap is transaction-scoped when every call is a resolved
/// transaction-local write, session-scoped otherwise.
///
/// `policy_registry` resolves the bypass-RLS flag for a `set_config('role', ...)` call.
pub fn apply(
    template: &SessionMutationTemplate,
    params: &[DfValue],
    session: &Arc<SessionContext>,
    policy_registry: &readyset_rls::PolicyRegistry,
) {
    match template {
        SessionMutationTemplate::SetConfig(calls) => {
            apply_set_config(calls, params, session, policy_registry)
        }
    }
}

/// Resolved form of one `set_config(name, value, is_local)` after parameter substitution.
struct ResolvedSetConfig {
    guc_name: String,
    value: String,
    is_local: bool,
}

fn validate_set_config(
    calls: &[SetConfigCall],
    params: &[DfValue],
) -> Option<Vec<ResolvedSetConfig>> {
    let mut resolved = Vec::with_capacity(calls.len());
    for call in calls {
        let value_str = match &call.value {
            ValueSource::Literal(s) => s.clone(),
            ValueSource::Param(idx) => render_dfvalue(params.get(*idx)?),
        };
        let is_local = match &call.is_local {
            IsLocalSpec::Literal(b) => *b,
            IsLocalSpec::Param(idx) => params.get(*idx).and_then(coerce_bool)?,
        };
        resolved.push(ResolvedSetConfig {
            guc_name: call.guc_name.clone(),
            value: value_str,
            is_local,
        });
    }
    Some(resolved)
}

fn apply_set_config(
    calls: &[SetConfigCall],
    params: &[DfValue],
    session: &Arc<SessionContext>,
    policy_registry: &readyset_rls::PolicyRegistry,
) {
    let Some(resolved) = validate_set_config(calls, params) else {
        // Malformed batch. Upstream may have applied some calls; our mirror has nothing applied.
        // Mark the session untrusted so cache lookups route off-cache until a clean batch (or
        // DISCARD ALL) restores a known state. Scope by `is_local`: an all-transaction-local batch
        // is transaction-scoped (un-mirrored writes roll back at transaction end); any session
        // write, or any unresolvable `is_local`, fails closed to session scope.
        let all_trx_local = calls
            .iter()
            .all(|c| matches!(c.is_local, IsLocalSpec::Literal(true)));
        tracing::warn!(
            calls = calls.len(),
            transaction_scoped = all_trx_local,
            "set_config batch had out-of-range or unparseable parameters; \
             marking session state untrusted and routing off-cache"
        );
        if all_trx_local {
            session.mark_transaction_untrusted();
        } else {
            session.mark_session_untrusted();
        }
        return;
    };

    for call in resolved {
        match call.guc_name.to_ascii_lowercase().as_str() {
            "role" => {
                // PostgREST sends `role` as a string. Resolve bypass via the policy registry so a
                // role carrying `rolsuper`/`rolbypassrls` (e.g. `service_role`) lands on the shared
                // bypass partition rather than its own per-role slot.
                let bypass = policy_registry.bypass_rls_for_role(call.value.as_str());
                let role_id: readyset_sql::ast::SqlIdentifier = call.value.as_str().into();
                session.set_effective_role_scoped(role_id, bypass, call.is_local);
            }
            _ => {
                if call.is_local {
                    session.set_trx_local_guc(call.guc_name, call.value);
                } else {
                    session.set_session_guc(call.guc_name, call.value);
                }
            }
        }
    }
    // A clean batch does not re-trust the session. It sets only the GUCs it names; it cannot
    // reverse an unmirrorable identity change (SET SESSION AUTHORIZATION leaves `session_user`
    // diverged and set_config never touches it), nor vouch for session writes a malformed batch
    // left applied upstream. A transaction-scoped gap clears at the transaction boundary; a
    // session-scoped gap only at DISCARD ALL / RESET ALL.
}

fn render_dfvalue(v: &DfValue) -> String {
    match v {
        DfValue::None | DfValue::Default => String::new(),
        DfValue::Text(_)
        | DfValue::TinyText(_)
        | DfValue::ByteArray(_)
        | DfValue::PassThrough(_) => v.to_string(),
        other => other.to_string(),
    }
}

fn coerce_bool(v: &DfValue) -> Option<bool> {
    match v {
        DfValue::Int(i) => Some(*i != 0),
        DfValue::UnsignedInt(i) => Some(*i != 0),
        DfValue::Text(s) => parse_bool_text(s.as_str()),
        DfValue::TinyText(s) => parse_bool_text(s.as_str()),
        _ => None,
    }
}

fn parse_bool_text(s: &str) -> Option<bool> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("t")
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed == "1"
    {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("f")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed == "0"
    {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql::ast::SqlIdentifier;

    use super::*;

    fn parse(query: &str) -> SqlQuery {
        readyset_sql_parsing::parse_query(Dialect::PostgreSQL, query).expect("parse")
    }

    /// Parse with the production preset, which handles schema-qualified function calls such as
    /// `pg_catalog.set_config(...)`.
    fn parse_prod(query: &str) -> SqlQuery {
        readyset_sql_parsing::parse_query_with_config(
            readyset_sql_parsing::ParsingPreset::for_prod(),
            Dialect::PostgreSQL,
            query,
        )
        .expect("parse")
    }

    fn unwrap_set_config(t: &SessionMutationTemplate) -> &[SetConfigCall] {
        match t {
            SessionMutationTemplate::SetConfig(calls) => calls,
        }
    }

    #[test]
    fn recognises_postgrest_batch() {
        let q = parse(
            "SELECT set_config('search_path', $1, true), \
                    set_config('role', $2, true), \
                    set_config('request.jwt.claims', $3, true)",
        );
        let tpl = recognize(&q).expect("template");
        let calls = unwrap_set_config(&tpl);
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].guc_name, "search_path");
        assert_eq!(calls[0].value, ValueSource::Param(0));
        assert_eq!(calls[0].is_local, IsLocalSpec::Literal(true));
        assert_eq!(calls[1].guc_name, "role");
        assert_eq!(calls[1].value, ValueSource::Param(1));
        assert_eq!(calls[2].guc_name, "request.jwt.claims");
        assert_eq!(calls[2].value, ValueSource::Param(2));
    }

    #[test]
    fn recognises_schema_qualified_pg_catalog_set_config() {
        let q = parse_prod(
            "SELECT pg_catalog.set_config('role', $1, true), \
                    pg_catalog.set_config('request.jwt.claims', $2, true)",
        );
        let tpl = recognize(&q).expect("template");
        let calls = unwrap_set_config(&tpl);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].guc_name, "role");
        assert_eq!(calls[1].guc_name, "request.jwt.claims");
    }

    #[test]
    fn refuses_other_schema_qualified_set_config() {
        let q = parse_prod("SELECT public.set_config('role', $1, true)");
        assert!(recognize(&q).is_none());
    }

    #[test]
    fn refuses_select_with_from_clause() {
        let q = parse("SELECT set_config('x', $1, true) FROM pg_namespace");
        assert!(recognize(&q).is_none());
    }

    #[test]
    fn refuses_non_set_config_projection() {
        let q = parse("SELECT set_config('x', $1, true), now()");
        assert!(recognize(&q).is_none());
    }

    #[test]
    fn refuses_non_literal_name_arg() {
        let q = parse("SELECT set_config($1, $2, true)");
        assert!(recognize(&q).is_none());
    }

    #[test]
    fn applies_role_and_local_gucs() {
        let q = parse(
            "SELECT set_config('role', $1, true), \
                    set_config('request.jwt.claims', $2, true)",
        );
        let tpl = recognize(&q).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        let params = vec![
            DfValue::from("authenticated"),
            DfValue::from(r#"{"sub":"alice"}"#),
        ];
        apply(&tpl, &params, &session, &registry);

        assert_eq!(session.effective_role().as_str(), "authenticated");
        assert!(!session.bypass_rls());
        assert_eq!(
            session.trx_local_guc("request.jwt.claims").as_deref(),
            Some(r#"{"sub":"alice"}"#),
        );
    }

    /// `set_config('role', 'service_role', ...)` against a registry
    /// that knows `service_role` carries `rolbypassrls=true` collapses
    /// the session onto the shared bypass partition.
    #[test]
    fn applies_role_with_bypass_resolution() {
        let q = parse("SELECT set_config('role', $1, true)");
        let tpl = recognize(&q).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        registry.set_role_name("service_role", 16384);
        registry.set_role(
            16384,
            readyset_rls::policy_registry::RoleAttrs {
                rolsuper: false,
                rolbypassrls: true,
            },
        );

        apply(&tpl, &[DfValue::from("service_role")], &session, &registry);

        assert_eq!(session.effective_role().as_str(), "service_role");
        assert!(session.bypass_rls());
    }

    #[test]
    fn session_scope_when_is_local_false() {
        let q = parse("SELECT set_config('app.tenant_id', $1, false)");
        let tpl = recognize(&q).unwrap();
        let session = SessionContext::new(SqlIdentifier::from("u"));
        let registry = readyset_rls::PolicyRegistry::new();
        apply(&tpl, &[DfValue::from("t1")], &session, &registry);
        assert_eq!(session.session_guc("app.tenant_id").as_deref(), Some("t1"));
        assert!(session.trx_local_guc("app.tenant_id").is_none());
    }

    /// The simple/text-protocol form carries inline literal values (no bound
    /// params). It is recognized and applied with an empty parameter vector,
    /// mirroring the role and claims the same way the parameterized form does.
    #[test]
    fn applies_literal_simple_protocol_batch() {
        let q = parse(
            "SELECT set_config('role', 'authenticated', false), \
                    set_config('request.jwt.claims', '{\"sub\":\"alice\"}', false)",
        );
        let tpl = recognize(&q).expect("literal batch recognized");
        let calls = unwrap_set_config(&tpl);
        assert_eq!(calls[0].value, ValueSource::Literal("authenticated".into()));
        assert_eq!(
            calls[1].value,
            ValueSource::Literal(r#"{"sub":"alice"}"#.into())
        );

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        // No bound parameters in the simple protocol.
        apply(&tpl, &[], &session, &registry);

        assert_eq!(session.effective_role().as_str(), "authenticated");
        assert_eq!(
            session.session_guc("request.jwt.claims").as_deref(),
            Some(r#"{"sub":"alice"}"#),
        );
    }

    /// A malformed batch (out-of-range placeholder) leaves the
    /// session mirror unchanged and marks the session untrusted so
    /// subsequent cache lookups route off-cache.
    #[test]
    fn malformed_batch_marks_session_untrusted() {
        // $99 is out of range for the supplied params slice; the
        // batch is malformed.
        let q = parse(
            "SELECT set_config('role', $1, true), \
                    set_config('request.jwt.claims', $99, true)",
        );
        let tpl = recognize(&q).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        // Single param supplied; the second call's $99 is out of
        // range.
        apply(&tpl, &[DfValue::from("authenticated")], &session, &registry);

        // Mirror was NOT updated.
        assert_eq!(session.effective_role().as_str(), "authenticator");
        assert!(session.trx_local_guc("request.jwt.claims").is_none());
        // Session is now untrusted; rls_input_values will route
        // off-cache until the next clean batch.
        assert!(session.is_untrusted());
        // Every call was transaction-local, so the gap is
        // transaction-scoped and clears when the transaction ends.
        session.on_trx_end();
        assert!(!session.is_untrusted());
    }

    /// A malformed batch carrying a session-scoped call fails closed to
    /// session scope: the gap survives transaction end, since upstream may
    /// have applied a session-level write our mirror never saw.
    #[test]
    fn malformed_session_scoped_batch_survives_trx_end() {
        let q = parse(
            "SELECT set_config('role', $1, false), \
                    set_config('request.jwt.claims', $99, true)",
        );
        let tpl = recognize(&q).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        apply(&tpl, &[DfValue::from("authenticated")], &session, &registry);

        assert!(session.is_untrusted());
        session.on_trx_end();
        assert!(session.is_untrusted());
    }

    /// A clean batch does NOT clear a session-scoped gap: it applies its
    /// GUCs but cannot reverse the unmirrorable session-level divergence, so
    /// the session stays untrusted until `DISCARD ALL` / `RESET ALL`.
    #[test]
    fn clean_batch_does_not_clear_session_scoped_gap() {
        // `is_local = false` makes the malformed batch session-scoped.
        let bad = parse(
            "SELECT set_config('role', $1, false), \
                    set_config('request.jwt.claims', $99, false)",
        );
        let good = parse(
            "SELECT set_config('role', $1, true), \
                    set_config('request.jwt.claims', $2, true)",
        );
        let tpl_bad = recognize(&bad).unwrap();
        let tpl_good = recognize(&good).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        apply(
            &tpl_bad,
            &[DfValue::from("authenticated")],
            &session,
            &registry,
        );
        assert!(session.is_untrusted());

        apply(
            &tpl_good,
            &[
                DfValue::from("authenticated"),
                DfValue::from(r#"{"sub":"alice"}"#),
            ],
            &session,
            &registry,
        );
        // GUCs were re-applied, but the session-scoped gap is not cleared
        // by a clean batch.
        assert!(session.is_untrusted());
        assert_eq!(session.effective_role().as_str(), "authenticated");
        assert_eq!(
            session.trx_local_guc("request.jwt.claims").as_deref(),
            Some(r#"{"sub":"alice"}"#),
        );

        // A full reset is what restores trust.
        session.discard_all();
        assert!(!session.is_untrusted());
    }

    /// A clean batch does NOT clear a transaction-scoped gap: an all
    /// transaction-local malformed batch stays untrusted across a
    /// subsequent clean batch, and only the transaction boundary clears it.
    #[test]
    fn clean_batch_leaves_transaction_scoped_gap() {
        let bad = parse(
            "SELECT set_config('role', $1, true), \
                    set_config('request.jwt.claims', $99, true)",
        );
        let good = parse("SELECT set_config('request.jwt.claims', $1, true)");
        let tpl_bad = recognize(&bad).unwrap();
        let tpl_good = recognize(&good).unwrap();

        let session = SessionContext::new(SqlIdentifier::from("authenticator"));
        let registry = readyset_rls::PolicyRegistry::new();
        apply(
            &tpl_bad,
            &[DfValue::from("authenticated")],
            &session,
            &registry,
        );
        assert!(session.is_untrusted());

        apply(
            &tpl_good,
            &[DfValue::from(r#"{"sub":"alice"}"#)],
            &session,
            &registry,
        );
        // Clean batch resolved session scope, but the transaction-scoped
        // gap persists until the transaction ends.
        assert!(session.is_untrusted());
        session.on_trx_end();
        assert!(!session.is_untrusted());
    }
}
