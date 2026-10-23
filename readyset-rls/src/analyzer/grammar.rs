//! Grammar checker for Postgres RLS policy expressions.
//!
//! Walks a parsed policy expression and decides whether it is built only from the allowlisted
//! vocabulary. Anything else produces [`Cacheability::Refuse`] with a typed [`RefuseReason`].
//!
//! The analyzer is a whitelist checker, not a recogniser of "anything that looks safe". Adding a
//! new construct is a deliberate code change.

use std::sync::Arc;

use crate::Oid;
use readyset_sql::Dialect;
use readyset_sql::ast::{
    BinaryOperator, Expr, FieldDefinitionExpr, FunctionExpr, Literal, SqlQuery, UnaryOperator,
};
use smallvec::SmallVec;

use super::allowlist::{is_allowed_guc, lookup_function};
use super::{CacheSessionDeps, Cacheability, RefuseReason};
use crate::policy_registry::{OidKind, PolicyRegistry};
use crate::types::SessionInputType;

/// Walk the policies attached to `referenced_relations` and produce the merged
/// [`CacheSessionDeps`]. A single policy outside the grammar refuses the entire cache.
pub fn analyze(
    registry: &Arc<PolicyRegistry>,
    referenced_relations: &[Oid],
    snapshot_generation: u64,
) -> CacheSessionDeps {
    let mut deps = CacheSessionDeps {
        snapshot_generation,
        ..Default::default()
    };
    let mut inputs: SmallVec<[SessionInputType; 4]> = SmallVec::new();

    // Worklist so a view contributes its underlying base relations. RLS
    // lives on tables, not on the view, so a query against a view must be
    // analyzed against what the view reads. `seen` guards view dependency
    // cycles and repeated work.
    let mut work: Vec<(OidKind, Oid, bool)> = referenced_relations
        .iter()
        .map(|r| (OidKind::Rel, *r, false))
        .collect::<Vec<_>>();
    let mut seen: std::collections::HashSet<(OidKind, Oid, bool)> =
        std::collections::HashSet::new();

    while let Some(obj) = work.pop() {
        if !seen.insert(obj) {
            continue;
        }

        match obj.0 {
            OidKind::Rel => {
                let relid = obj.1;

                deps.visited_relations.push(relid);

                let flags = registry.flags_for_rel(relid).unwrap_or_default();

                // Views and materialized views carry no policies of their own. With `security_invoker`
                // the underlying policies run as the querying role, so expanding to the base relations
                // and partitioning by them is correct. Without it the view runs as its owner and
                // bypasses the caller's RLS, which is not representable per-session. A view whose
                // dependencies the registry has not loaded yet fails closed rather than being treated
                // as a plain table.
                if matches!(flags.relkind, b'v' | b'm') {
                    let Some(underlying) = registry.view_underlying(relid) else {
                        deps.cacheability = Cacheability::Refuse(RefuseReason::PolicyTooDynamic(
                            format!("view oid={relid} dependencies not loaded into the registry"),
                        ));
                        return deps;
                    };

                    let owner = (obj.2) | !flags.security_invoker;
                    if owner {
                        for &(kind, oid) in underlying.iter() {
                            if kind == OidKind::Rel {
                                let uf = registry.flags_for_rel(oid).unwrap_or_default();
                                if uf.relrowsecurity {
                                    deps.cacheability = Cacheability::Refuse(
                                        RefuseReason::RlsTableInOwnerContext(oid),
                                    );
                                    return deps;
                                }
                            }
                            // Only the SECURITY DEFINER flag propagates: it changes `current_user`,
                            // while a view only changes how its direct relations are checked.
                            work.push((kind, oid, obj.2));
                        }
                    } else {
                        work.extend(underlying.iter().map(|d| (d.0, d.1, obj.2)));
                    }
                }

                if !flags.relrowsecurity {
                    continue;
                }

                // Beneath a SECURITY DEFINER function `current_user` is the function's owner, so this
                // table's policies do not run as the caller.
                if obj.2 {
                    deps.cacheability =
                        Cacheability::Refuse(RefuseReason::RlsTableInOwnerContext(relid));
                    return deps;
                }

                // Partitioned tables refuse outright.
                if flags.relkind == b'p' {
                    deps.cacheability = Cacheability::Refuse(RefuseReason::PartitionedTable(relid));
                    return deps;
                }

                deps.rls_active_for_tables.push(relid);

                let Some(policies) = registry.policies_for(relid) else {
                    // RLS enabled but no policies: Postgres returns zero rows for any non-owner role.
                    // Safe to cache (same answer for every session), but we conservatively refuse.
                    deps.cacheability = Cacheability::Refuse(RefuseReason::PolicyTooDynamic(
                        format!("rls_enabled_but_no_policies_in_registry table_oid={relid}"),
                    ));
                    return deps;
                };

                for policy in policies.iter() {
                    let Some(expr_text) = policy.using_expr.as_deref() else {
                        continue;
                    };
                    match check_policy_expr(expr_text) {
                        Ok(walk) => merge(&mut inputs, walk),
                        Err(reason) => {
                            deps.cacheability = Cacheability::Refuse(reason);
                            return deps;
                        }
                    }
                }
            }
            OidKind::Func => {
                let funoid = obj.1;

                // Visited before any check, so a function loaded or changed later reaches the
                // caches that call it.
                deps.visited_functions.push(funoid);

                let Some(func) = registry.func_info(funoid) else {
                    deps.cacheability = Cacheability::Refuse(RefuseReason::PolicyTooDynamic(
                        format!("function oid={funoid} not loaded into the registry"),
                    ));
                    return deps;
                };

                if func.provolatile == b'v' {
                    deps.cacheability = Cacheability::Refuse(RefuseReason::VolatileFunction(
                        format!("{}.{}", func.schema, func.name),
                    ));
                    return deps;
                }

                let owner = obj.2;

                work.extend(
                    func.deps
                        .iter()
                        .map(|dep| (dep.0, dep.1, func.security_definer | owner)),
                );
            }
        }
    }

    deps.session_rls_inputs = Arc::from(inputs.into_vec());
    deps.cacheability = Cacheability::Cacheable;
    deps
}

/// Result of a successful walk over a single policy expression. The
/// caller merges this into the per-cache input set.
#[derive(Debug, Default)]
struct PolicyWalk {
    session_rls_inputs: SmallVec<[SessionInputType; 4]>,
}

fn merge(inputs: &mut SmallVec<[SessionInputType; 4]>, walk: PolicyWalk) {
    for input in walk.session_rls_inputs {
        if !inputs.contains(&input) {
            inputs.push(input);
        }
    }
}

/// Parse a `pg_get_expr(polqual, polrelid)` result by wrapping it in
/// a SELECT projection so the SQL parser can handle it. Returns the
/// extracted [`Expr`].
fn parse_policy_expr(expr_text: &str) -> Result<Expr, RefuseReason> {
    let wrapped = format!("SELECT ({expr_text}) AS x");
    // The nom-sql parser does not handle schema-qualified function calls (`auth.uid()`);
    // use sqlparser-only so the grammar can recognise the canonical pattern.
    let parsed = readyset_sql_parsing::parse_query_with_config(
        readyset_sql_parsing::ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        &wrapped,
    )
    .map_err(|_| RefuseReason::PolicyTooDynamic(format!("parse_failed: {expr_text}")))?;
    let SqlQuery::Select(select) = parsed else {
        return Err(RefuseReason::PolicyTooDynamic(format!(
            "parse_not_select: {expr_text}"
        )));
    };
    let mut fields = select.fields;
    if fields.len() != 1 {
        return Err(RefuseReason::PolicyTooDynamic(format!(
            "parse_unexpected_projection: {expr_text}"
        )));
    }
    let FieldDefinitionExpr::Expr { expr, .. } = fields.swap_remove(0) else {
        return Err(RefuseReason::PolicyTooDynamic(format!(
            "parse_non_expr_field: {expr_text}"
        )));
    };
    Ok(expr)
}

fn check_policy_expr(expr_text: &str) -> Result<PolicyWalk, RefuseReason> {
    let expr = parse_policy_expr(expr_text)?;
    let mut walk = PolicyWalk::default();
    match check_expr(&expr, &mut walk) {
        Ok(()) => Ok(walk),
        // Append the policy text so the EXPLAIN / log line names both the
        // rejected construct and the policy it came from.
        Err(RefuseReason::PolicyTooDynamic(detail)) => Err(RefuseReason::PolicyTooDynamic(
            format!("{detail} in policy `{expr_text}`"),
        )),
        Err(other) => Err(other),
    }
}

fn check_expr(expr: &Expr, walk: &mut PolicyWalk) -> Result<(), RefuseReason> {
    // Recognise `auth.jwt() ->> '<key>'` (and chains like `auth.jwt() -> 'a' ->> 'b'`) and
    // record the narrowed claim path instead of the whole `request.jwt.claims` blob, so two
    // JWTs with the same keyed value but different rotated `iat`/`exp` collapse to one cache
    // partition.
    if let Some(input) = match_jwt_claim(expr) {
        walk.session_rls_inputs.push(input);
        return Ok(());
    }

    match expr {
        Expr::Literal(_) => Ok(()),
        Expr::Column(_) => Ok(()),
        Expr::BinaryOp { lhs, op, rhs } => {
            check_binary_op(*op)?;
            check_expr(lhs, walk)?;
            check_expr(rhs, walk)?;
            Ok(())
        }
        Expr::UnaryOp { op, rhs } => {
            check_unary_op(*op)?;
            check_expr(rhs, walk)
        }
        Expr::Cast { expr, .. } => check_expr(expr, walk),
        Expr::Call(func) => check_function_call(func, walk),
        Expr::NestedSelect(sel) => {
            // Only `(SELECT <allowlisted_call>)` is permitted; this is
            // the canonical pattern `(SELECT auth.uid())`.
            if !sel.tables.is_empty()
                || sel.where_clause.is_some()
                || sel.group_by.is_some()
                || sel.having.is_some()
                || sel.distinct
                || sel.order.is_some()
                || sel.fields.len() != 1
            {
                return Err(RefuseReason::PolicyTooDynamic(
                    "subquery is not a simple single-expression projection".into(),
                ));
            }
            let FieldDefinitionExpr::Expr { expr, .. } = &sel.fields[0] else {
                return Err(RefuseReason::PolicyTooDynamic(
                    "subquery projects a non-expression field".into(),
                ));
            };
            check_expr(expr, walk)
        }
        other => Err(RefuseReason::PolicyTooDynamic(format!(
            "unsupported expression node: {other:?}"
        ))),
    }
}

/// Peel any cast layers to reach an underlying string literal.
/// `pg_get_expr` renders policy string literals with an explicit cast
/// (`'request.jwt.claims'::text`, `'sub'::text`), so the grammar must
/// look through casts wherever it expects a literal.
fn as_string_literal(expr: &Expr) -> Option<&str> {
    let mut cur = expr;
    loop {
        match cur {
            Expr::Literal(Literal::String(s)) => return Some(s.as_str()),
            Expr::Cast { expr, .. } => cur = expr,
            _ => return None,
        }
    }
}

/// True when `expr` is the JWT claims object at the root of a claim
/// chain: either `auth.jwt()` or the inlined
/// `current_setting('request.jwt.claims', true)` form that the
/// `auth.*` helpers expand to (and that `pg_get_expr` emits). Casts
/// (`::json` / `::jsonb`) are peeled. The `current_setting` form
/// requires the explicit `missing_ok = true` argument, matching the
/// standalone `current_setting` rule.
fn is_jwt_claims_root(expr: &Expr) -> bool {
    let mut cur = expr;
    while let Expr::Cast { expr, .. } = cur {
        cur = expr;
    }
    let Expr::Call(FunctionExpr::Udf {
        schema,
        name,
        arguments,
    }) = cur
    else {
        return false;
    };
    let schema_str = schema.as_ref().map(|s| s.as_str()).unwrap_or("");
    if schema_str.eq_ignore_ascii_case("auth")
        && name.as_str().eq_ignore_ascii_case("jwt")
        && arguments.is_empty()
    {
        return true;
    }
    if name.as_str().eq_ignore_ascii_case("current_setting")
        && (schema_str.is_empty() || schema_str.eq_ignore_ascii_case("pg_catalog"))
    {
        let guc_is_claims = arguments
            .first()
            .and_then(as_string_literal)
            .is_some_and(|g| g.eq_ignore_ascii_case("request.jwt.claims"));
        let missing_ok_true = matches!(
            arguments.get(1),
            Some(Expr::Literal(Literal::Boolean(true)))
        );
        if guc_is_claims && missing_ok_true {
            return true;
        }
    }
    false
}

/// Recognise the JWT-claim-extraction shape:
///
///   `auth.jwt()` (root)
///   `auth.jwt() ->> '<key>'` (->> returns text)
///   `auth.jwt() -> 'a' ->> 'b'` (nested)
///   `current_setting('request.jwt.claims', true)::json ->> '<key>'`
///     (the inlined `auth.uid()` / `auth.role()` form)
///
/// Returns the [`SessionInputType::JwtClaim`] dependency: a bare root (no
/// chain) yields empty segments (the whole blob); a chain yields its
/// keys in navigation order, carried verbatim. Keys may be
/// cast-wrapped (`'sub'::text`). Returns `None` if the expression is
/// not a JWT-claim chain or contains anything outside string-key
/// navigation.
fn match_jwt_claim(expr: &Expr) -> Option<SessionInputType> {
    let mut keys: Vec<&str> = Vec::new();
    let mut cur = expr;
    loop {
        match cur {
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::Arrow1 | BinaryOperator::Arrow2,
                rhs,
            } => {
                keys.push(as_string_literal(rhs)?);
                cur = lhs;
            }
            _ if is_jwt_claims_root(cur) => {
                return Some(SessionInputType::JwtClaim(
                    keys.iter().rev().map(|k| Box::from(*k)).collect(),
                ));
            }
            _ => return None,
        }
    }
}

fn check_binary_op(op: BinaryOperator) -> Result<(), RefuseReason> {
    match op {
        BinaryOperator::And
        | BinaryOperator::Or
        | BinaryOperator::Equal
        | BinaryOperator::NotEqual
        | BinaryOperator::Greater
        | BinaryOperator::GreaterOrEqual
        | BinaryOperator::Less
        | BinaryOperator::LessOrEqual
        | BinaryOperator::Is
        | BinaryOperator::IsNot
        | BinaryOperator::Arrow1
        | BinaryOperator::Arrow2 => Ok(()),
        other => Err(RefuseReason::PolicyTooDynamic(format!(
            "unsupported binary operator: {other:?}"
        ))),
    }
}

fn check_unary_op(op: UnaryOperator) -> Result<(), RefuseReason> {
    match op {
        UnaryOperator::Not => Ok(()),
        other => Err(RefuseReason::PolicyTooDynamic(format!(
            "unsupported unary operator: {other:?}"
        ))),
    }
}

fn check_function_call(func: &FunctionExpr, walk: &mut PolicyWalk) -> Result<(), RefuseReason> {
    match func {
        // Bare `current_user` / `session_user` / `user` are accepted: the
        // scoped partition unconditionally folds in the effective role and
        // session user, so a policy reading role identity is already keyed by
        // it without any extra signal.
        FunctionExpr::CurrentUser | FunctionExpr::SessionUser | FunctionExpr::SqlUser => Ok(()),

        FunctionExpr::Udf {
            schema,
            name,
            arguments,
        } => {
            let lower_name = name.as_str().to_ascii_lowercase();
            let qualified = match schema {
                Some(s) => format!("{}.{}", s.as_str().to_ascii_lowercase(), lower_name),
                None => format!("pg_catalog.{lower_name}"),
            };
            let meta = lookup_function(&qualified);
            let Some(meta) = meta else {
                return Err(RefuseReason::NonStableFunctionInPolicy(qualified));
            };

            // `current_setting(name[, missing_ok])`: GUC name must be
            // an allowlisted literal string.
            if meta
                .qualified_name
                .eq_ignore_ascii_case("pg_catalog.current_setting")
            {
                let Some(first) = arguments.first() else {
                    return Err(RefuseReason::PolicyTooDynamic(
                        "current_setting() called with no arguments".into(),
                    ));
                };
                let Some(guc) = as_string_literal(first) else {
                    return Err(RefuseReason::PolicyTooDynamic(
                        "current_setting() GUC name is not a string literal".into(),
                    ));
                };
                if !is_allowed_guc(guc) {
                    return Err(RefuseReason::PolicyTooDynamic(format!(
                        "current_setting() reads non-allowlisted GUC `{guc}`"
                    )));
                }
                walk.session_rls_inputs.push(SessionInputType::guc(guc));
                // The one-arg form errors on a missing GUC; refuse unless `missing_ok` is
                // explicitly true, so we never silently change behaviour.
                match arguments.get(1) {
                    Some(Expr::Literal(Literal::Boolean(true))) => Ok(()),
                    None => Err(RefuseReason::PolicyTooDynamic(format!(
                        "current_setting('{guc}') needs explicit missing_ok=true (one-arg form errors on unset GUC)"
                    ))),
                    _ => Err(RefuseReason::PolicyTooDynamic(format!(
                        "current_setting('{guc}') second argument must be the boolean literal true"
                    ))),
                }
            } else {
                // Allowlisted functions: record their declared session-input reads.
                for input in meta.reads.iter() {
                    walk.session_rls_inputs.push(input.clone());
                }
                // Allowlisted functions are nullary; any argument signals a non-standard shape.
                if !arguments.is_empty() {
                    return Err(RefuseReason::PolicyTooDynamic(format!(
                        "allowlisted function `{}` called with arguments",
                        meta.qualified_name
                    )));
                }
                Ok(())
            }
        }
        _ => Err(RefuseReason::NonStableFunctionInPolicy(format!("{func:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use crate::policy_registry::{FuncInfo, Policy, RelationFlags};

    use super::*;

    fn registry() -> Arc<PolicyRegistry> {
        Arc::new(PolicyRegistry::new())
    }

    fn policy_table(r: &Arc<PolicyRegistry>, relid: Oid, using_expr: &str) {
        r.set_flags(
            relid,
            RelationFlags {
                relrowsecurity: true,
                relkind: b'r',
                ..Default::default()
            },
        );
        r.set_policies(
            relid,
            vec![Policy {
                oid: 1,
                name: "owner".into(),
                permissive: true,
                cmd: 'r',
                roles: vec![],
                using_expr: Some(using_expr.into()),
                check_expr: None,
            }],
        );
    }

    #[test]
    fn auth_uid_policy_is_cacheable_with_claims_sub_key() {
        let r = registry();
        policy_table(&r, 100, "user_id = (SELECT auth.uid())");
        let deps = analyze(&r, &[100], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["sub"]))
        );
    }

    /// A policy reading `current_user` is in-grammar and cacheable: the
    /// scoped partition always keys on the effective role, so no extra
    /// role signal is needed.
    #[test]
    fn current_user_policy_is_cacheable() {
        let r = registry();
        policy_table(&r, 101, "owner = current_user");
        let deps = analyze(&r, &[101], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
    }

    #[test]
    fn out_of_grammar_subtraction_is_refused() {
        let r = registry();
        policy_table(&r, 102, "user_id - 1 = 0");
        let deps = analyze(&r, &[102], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PolicyTooDynamic(_))
        ));
    }

    #[test]
    fn unknown_function_call_is_refused() {
        let r = registry();
        policy_table(&r, 103, "my_schema.my_func() = 1");
        let deps = analyze(&r, &[103], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::NonStableFunctionInPolicy(_))
        ));
    }

    #[test]
    fn current_setting_one_arg_form_is_refused() {
        let r = registry();
        policy_table(&r, 104, "tenant = current_setting('request.jwt.claim.sub')");
        let deps = analyze(&r, &[104], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PolicyTooDynamic(_))
        ));
    }

    #[test]
    fn current_setting_two_arg_with_allowed_guc_is_cacheable() {
        let r = registry();
        policy_table(
            &r,
            105,
            "tenant = current_setting('request.jwt.claim.sub', true)",
        );
        let deps = analyze(&r, &[105], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::guc("request.jwt.claim.sub"))
        );
    }

    #[test]
    fn current_setting_unknown_guc_is_refused() {
        let r = registry();
        policy_table(&r, 106, "x = current_setting('app.secret', true)");
        let deps = analyze(&r, &[106], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PolicyTooDynamic(_))
        ));
    }

    #[test]
    fn boolean_combination_of_allowlisted_constructs_is_cacheable() {
        let r = registry();
        policy_table(&r, 107, "user_id = (SELECT auth.uid()) AND deleted = false");
        let deps = analyze(&r, &[107], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
    }

    #[test]
    fn non_rls_table_is_plain_cacheable() {
        let r = registry();
        r.set_flags(
            200,
            RelationFlags {
                relrowsecurity: false,
                relkind: b'r',
                ..Default::default()
            },
        );
        let deps = analyze(&r, &[200], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(deps.rls_active_for_tables.is_empty());
    }

    #[test]
    fn partitioned_rls_table_refuses() {
        let r = registry();
        r.set_flags(
            201,
            RelationFlags {
                relrowsecurity: true,
                relkind: b'p',
                ..Default::default()
            },
        );
        let deps = analyze(&r, &[201], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PartitionedTable(201))
        ));
    }

    #[test]
    fn jwt_claim_path_is_recorded_narrowed() {
        let r = registry();
        policy_table(&r, 110, "org_id = ((auth.jwt() ->> 'org_id'))");
        let deps = analyze(&r, &[110], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["org_id"]))
        );
        assert!(
            !deps
                .session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&[]))
        );
    }

    #[test]
    fn nested_jwt_path_records_all_segments() {
        let r = registry();
        policy_table(&r, 111, "x = (auth.jwt() -> 'app_metadata' ->> 'tenant')");
        let deps = analyze(&r, &[111], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["app_metadata", "tenant"]))
        );
    }

    /// Claim keys containing `.`, `/`, or `~` are carried verbatim as
    /// a single segment, so an OIDC-namespace key like
    /// `https://example.com/role` cannot alias a nested path.
    #[test]
    fn jwt_claim_keys_with_special_chars_stay_single_segments() {
        let r = registry();
        policy_table(&r, 112, "x = (auth.jwt() ->> 'https://example.com/role')");
        let deps = analyze(&r, &[112], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["https://example.com/role"]))
        );
    }

    /// The inlined `auth.uid()` form as `pg_get_expr` renders it: string literals carry `::text`
    /// casts and the JWT root is the expanded `current_setting('request.jwt.claims', true)::json`.
    /// It must key on the narrowed claim path, not the whole `request.jwt.claims` blob.
    #[test]
    fn inlined_current_setting_jwt_claim_is_cacheable_with_narrowed_path() {
        let r = registry();
        policy_table(
            &r,
            120,
            "(owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'sub'::text))",
        );
        let deps = analyze(&r, &[120], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["sub"]))
        );
        assert!(
            !deps
                .session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&[]))
        );
    }

    /// A standalone `current_setting` with a cast-wrapped GUC-name
    /// literal (`'...'::text`) still resolves the allowlisted GUC.
    #[test]
    fn current_setting_cast_wrapped_guc_name_is_allowed() {
        let r = registry();
        policy_table(
            &r,
            121,
            "tenant = current_setting('request.jwt.claim.sub'::text, true)",
        );
        let deps = analyze(&r, &[121], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::guc("request.jwt.claim.sub"))
        );
    }

    /// The inlined JWT form still honours the missing_ok rule: the one-arg `current_setting`
    /// (no `missing_ok`) is not a recognised JWT root and falls through to the standalone
    /// rule, which refuses.
    #[test]
    fn inlined_current_setting_jwt_one_arg_is_refused() {
        let r = registry();
        policy_table(
            &r,
            122,
            "(owner_id = ((current_setting('request.jwt.claims'::text))::json ->> 'sub'::text))",
        );
        let deps = analyze(&r, &[122], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PolicyTooDynamic(_))
        ));
    }

    /// Registers `view_oid` as a view over `base_oid` with the given
    /// `security_invoker` setting. The view itself carries no RLS flag.
    fn view_over(r: &Arc<PolicyRegistry>, view_oid: Oid, base_oid: Oid, security_invoker: bool) {
        r.set_flags(
            view_oid,
            RelationFlags {
                relrowsecurity: false,
                relkind: b'v',
                security_invoker,
                ..Default::default()
            },
        );
        r.set_view_underlying(view_oid, vec![(OidKind::Rel, base_oid)]);
    }

    /// A `security_invoker` view over an RLS table is cacheable and keys on the base table's
    /// policy, exactly as a direct query would. The view's own relrowsecurity is false, so it
    /// must be expanded to the base relation rather than treated as a plain table.
    #[test]
    fn security_invoker_view_over_rls_table_is_scoped() {
        let r = registry();
        policy_table(&r, 300, "user_id = (SELECT auth.uid())");
        view_over(&r, 301, 300, true);
        let deps = analyze(&r, &[301], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(deps.rls_active_for_tables.contains(&300));
        assert!(
            deps.session_rls_inputs
                .contains(&SessionInputType::jwt_claim(&["sub"]))
        );
    }

    /// A non-`security_invoker` view over an RLS table runs as the view
    /// owner and cannot be partitioned per-caller: refuse, do not cache.
    #[test]
    fn view_without_security_invoker_over_rls_table_refuses() {
        let r = registry();
        policy_table(&r, 300, "user_id = (SELECT auth.uid())");
        view_over(&r, 302, 300, false);
        let deps = analyze(&r, &[302], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::RlsTableInOwnerContext(300))
        ));
    }

    /// A non-`security_invoker` view over a non-RLS table is identical
    /// for every session, so it remains plain-cacheable.
    #[test]
    fn view_without_security_invoker_over_plain_table_is_cacheable() {
        let r = registry();
        r.set_flags(
            310,
            RelationFlags {
                relrowsecurity: false,
                relkind: b'r',
                ..Default::default()
            },
        );
        view_over(&r, 311, 310, false);
        let deps = analyze(&r, &[311], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(deps.rls_active_for_tables.is_empty());
    }

    /// A view whose dependencies the registry has not loaded yet fails
    /// closed rather than being treated as a plain table.
    #[test]
    fn view_with_unloaded_dependencies_refuses() {
        let r = registry();
        r.set_flags(
            320,
            RelationFlags {
                relrowsecurity: false,
                relkind: b'v',
                security_invoker: true,
                ..Default::default()
            },
        );
        // No set_view_underlying call.
        let deps = analyze(&r, &[320], r.generation());
        assert!(matches!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::PolicyTooDynamic(_))
        ));
    }

    fn plain_table(r: &Arc<PolicyRegistry>, relid: Oid) {
        r.set_flags(
            relid,
            RelationFlags {
                relkind: b'r',
                ..Default::default()
            },
        );
    }

    /// A cache over a view hierarchy must register against every level and the plain base table,
    /// so redefining any view or enabling RLS on the table reaches it.
    #[test]
    fn visited_relations_cover_every_view_level_and_plain_base() {
        let r = registry();
        plain_table(&r, 500);
        view_over(&r, 501, 500, true);
        view_over(&r, 502, 501, true);
        let deps = analyze(&r, &[502], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(deps.rls_active_for_tables.is_empty());
        for relid in [502, 501, 500] {
            assert!(deps.visited_relations.contains(&relid), "missing {relid}");
        }
    }

    /// Only RLS-active tables decide scoping, but every base table is visited.
    #[test]
    fn visited_relations_include_plain_and_rls_bases() {
        let r = registry();
        policy_table(&r, 510, "user_id = (SELECT auth.uid())");
        plain_table(&r, 512);
        view_over(&r, 511, 510, true);
        r.set_view_underlying(511, vec![(OidKind::Rel, 510), (OidKind::Rel, 512)]);
        let deps = analyze(&r, &[511], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert_eq!(deps.rls_active_for_tables.as_slice(), &[510]);
        for relid in [511, 510, 512] {
            assert!(deps.visited_relations.contains(&relid), "missing {relid}");
        }
    }

    /// An owner-context view is not expanded, but its base table is still visited so enabling RLS
    /// on that table reaches the plain cache over the view.
    #[test]
    fn visited_relations_include_owner_context_view_base() {
        let r = registry();
        plain_table(&r, 520);
        view_over(&r, 521, 520, false);
        let deps = analyze(&r, &[521], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert!(deps.visited_relations.contains(&521));
        assert!(deps.visited_relations.contains(&520));
    }

    /// A catalog change elsewhere leaves a cache's dependencies intact; redefining a view beneath
    /// it or enabling RLS on its base table does not.
    #[test]
    fn same_dependencies_ignores_unrelated_changes() {
        let r = registry();
        plain_table(&r, 530);
        view_over(&r, 531, 530, true);
        let before = analyze(&r, &[531], r.generation());

        policy_table(&r, 539, "user_id = (SELECT auth.uid())");
        r.bump_generation();
        assert!(analyze(&r, &[531], r.generation()).same_dependencies(&before));

        policy_table(&r, 532, "user_id = (SELECT auth.uid())");
        r.set_view_underlying(531, vec![(OidKind::Rel, 532)]);
        assert!(!analyze(&r, &[531], r.generation()).same_dependencies(&before));

        r.set_view_underlying(531, vec![(OidKind::Rel, 530)]);
        policy_table(&r, 530, "user_id = (SELECT auth.uid())");
        assert!(!analyze(&r, &[531], r.generation()).same_dependencies(&before));
    }

    fn stable_func(
        r: &Arc<PolicyRegistry>,
        oid: Oid,
        security_definer: bool,
        deps: Vec<(OidKind, Oid)>,
    ) {
        r.set_func_info(
            oid,
            FuncInfo {
                schema: "public".into(),
                name: format!("f{oid}"),
                provolatile: b's',
                security_definer,
                deps,
            },
        );
    }

    /// A function called from a view without `security_invoker` still runs as the caller, so an
    /// RLS-active table it reads is scoped by its policy.
    #[test]
    fn view_without_security_invoker_over_invoker_function_is_scoped() {
        let r = registry();
        policy_table(&r, 650, "user_id = (SELECT auth.uid())");
        stable_func(&r, 651, false, vec![(OidKind::Rel, 650)]);
        view_over(&r, 652, 650, false);
        r.set_view_underlying(652, vec![(OidKind::Func, 651)]);
        let deps = analyze(&r, &[652], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert_eq!(deps.rls_active_for_tables.as_slice(), &[650]);
    }

    /// A `security_invoker` view checks its relations as the current user even when a view without
    /// `security_invoker` reads it, so the RLS-active table beneath it is scoped.
    #[test]
    fn invoker_view_nested_in_view_without_security_invoker_is_scoped() {
        let r = registry();
        policy_table(&r, 680, "user_id = (SELECT auth.uid())");
        view_over(&r, 681, 680, true);
        view_over(&r, 682, 681, false);
        let deps = analyze(&r, &[682], r.generation());
        assert_eq!(deps.cacheability, Cacheability::Cacheable);
        assert_eq!(deps.rls_active_for_tables.as_slice(), &[680]);
    }

    /// A SECURITY DEFINER function reads as its owner, so an RLS-active table beneath it is refused
    /// even under a `security_invoker` view.
    #[test]
    fn security_definer_function_reading_rls_table_refuses() {
        let r = registry();
        policy_table(&r, 660, "user_id = (SELECT auth.uid())");
        stable_func(&r, 661, true, vec![(OidKind::Rel, 660)]);
        view_over(&r, 662, 660, true);
        r.set_view_underlying(662, vec![(OidKind::Func, 661)]);
        let deps = analyze(&r, &[662], r.generation());
        assert_eq!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::RlsTableInOwnerContext(660))
        );
    }

    /// Everything beneath a SECURITY DEFINER function runs as its owner, including an invoker
    /// function it calls, so the RLS-active table that one reads is refused.
    #[test]
    fn invoker_function_beneath_security_definer_function_refuses() {
        let r = registry();
        policy_table(&r, 690, "user_id = (SELECT auth.uid())");
        stable_func(&r, 691, false, vec![(OidKind::Rel, 690)]);
        stable_func(&r, 692, true, vec![(OidKind::Func, 691)]);
        view_over(&r, 693, 690, true);
        r.set_view_underlying(693, vec![(OidKind::Func, 692)]);
        let deps = analyze(&r, &[693], r.generation());
        assert_eq!(
            deps.cacheability,
            Cacheability::Refuse(RefuseReason::RlsTableInOwnerContext(690))
        );
    }
}
