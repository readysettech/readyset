//! Initial REPEATABLE READ snapshot loader for the policy registry.
//!
//! Runs the catalog SELECTs inside a single `REPEATABLE READ READ ONLY`
//! transaction so a concurrent DDL cannot leave the registry with a mismatch
//! between, e.g., `pg_class.relrowsecurity` and the `pg_policy` rows for the
//! same relation.
//!
//! Uses `pg_roles` rather than `pg_authid`: `pg_authid` is privilege-restricted
//! on many managed platforms and exposes password hashes the registry has no
//! business reading.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;
use tokio_postgres::types::Oid as PgOid;
use tracing::trace;

use crate::policy_registry::{FuncInfo, OidKind, Policy, PolicyRegistry, RelationFlags, RoleAttrs};

/// SQL for the snapshot queries. `pub(crate)` so the poller can reuse the same
/// query strings when reloading a single relation/role.
pub(crate) mod sql {
    pub(crate) const POLICIES: &str = "\
        SELECT p.oid, p.polname, p.polrelid, p.polcmd, p.polpermissive, \
               p.polroles, \
               pg_get_expr(p.polqual,      p.polrelid) AS using_expr, \
               pg_get_expr(p.polwithcheck, p.polrelid) AS check_expr \
        FROM   pg_catalog.pg_policy p \
        JOIN   pg_catalog.pg_class  c ON c.oid = p.polrelid \
        WHERE  c.relnamespace NOT IN \
               (SELECT oid FROM pg_catalog.pg_namespace \
                WHERE  nspname IN ('pg_catalog','information_schema','pg_toast'))";

    pub(crate) const RELATIONS: &str = "\
        SELECT c.oid, c.relkind, c.relrowsecurity, c.relforcerowsecurity, \
               c.reloptions, c.relname, n.nspname AS schema_name \
        FROM   pg_catalog.pg_class c \
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
        WHERE  c.relkind IN ('r','p') \
          AND  n.nspname NOT IN \
               ('pg_catalog','information_schema','pg_toast')";

    pub(crate) const ROLES: &str = "\
        SELECT oid, rolname, rolsuper, rolbypassrls \
        FROM   pg_catalog.pg_roles";

    macro_rules! view_deps {
        () => {
            "\
            SELECT DISTINCT v.oid AS view_oid, v.relkind, v.reloptions, v.relname, \
                   n.nspname AS schema_name, d.refobjid AS dep_oid, \
                   CASE \
                       WHEN d.refclassid = 'pg_class'::regclass THEN 'R' \
                       WHEN d.refclassid = 'pg_proc'::regclass THEN 'F' \
                       ELSE 'U' \
                   END AS dep_cat \
            FROM   pg_catalog.pg_class     v \
            JOIN   pg_catalog.pg_namespace n  ON n.oid = v.relnamespace \
            JOIN   pg_catalog.pg_rewrite   rw ON rw.ev_class = v.oid AND rw.rulename = '_RETURN' \
            LEFT JOIN pg_catalog.pg_depend d \
                   ON  d.classid = 'pg_rewrite'::regclass AND d.objid = rw.oid \
                   AND d.refclassid IN ('pg_class'::regclass, 'pg_proc'::regclass) \
                   AND d.refobjid <> v.oid \
            WHERE  v.relkind IN ('v','m') \
              AND  n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
        };
    }

    pub(crate) const VIEW_DEPS: &str = view_deps!();

    pub(crate) const VIEW_DEPS_FOR_VIEW: &str = concat!(view_deps!(), " AND v.oid = $1");

    macro_rules! func_deps {
        () => {
            "\
            SELECT DISTINCT f.oid AS func_oid, f.provolatile, f.prosecdef, f.proname, \
                   n.nspname AS schema_name, d.refobjid AS dep_oid, \
                   CASE \
                       WHEN d.refclassid = 'pg_class'::regclass THEN 'R' \
                       WHEN d.refclassid = 'pg_proc'::regclass THEN 'F' \
                       ELSE 'U' \
                   END AS dep_cat \
            FROM   pg_catalog.pg_proc      f \
            JOIN   pg_catalog.pg_namespace n ON n.oid = f.pronamespace \
            LEFT JOIN pg_catalog.pg_depend d \
                   ON  d.classid = 'pg_proc'::regclass AND d.objid = f.oid \
                   AND d.refclassid IN ('pg_class'::regclass, 'pg_proc'::regclass) \
                   AND d.refobjid <> f.oid \
            WHERE  n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
        };
    }

    pub(crate) const FUNC_DEPS: &str = func_deps!();

    pub(crate) const FUNC_DEPS_FOR_FUNC: &str = concat!(func_deps!(), " AND f.oid = $1");

    // Order by `setdatabase` so the global default (`setdatabase = 0`) is applied
    // before the database-specific default, letting the latter overwrite the
    // former. Matches Postgres precedence for the current database.
    pub(crate) const ROLE_DEFAULT_GUCS: &str = "\
        SELECT r.oid, r.rolname, d.datname, s.setconfig \
        FROM   pg_catalog.pg_db_role_setting s \
        JOIN   pg_catalog.pg_roles r    ON r.oid = s.setrole \
        LEFT JOIN pg_catalog.pg_database d ON d.oid = s.setdatabase \
        WHERE  d.datname = current_database() OR s.setdatabase = 0 \
        ORDER BY s.setdatabase";

    /// Names every `pg_catalog` relation the snapshot and poller read that the
    /// connection role cannot `SELECT`, so the caller fails closed with an
    /// actionable error instead of a mid-snapshot permission failure.
    pub(crate) const PRIVILEGE_CHECK: &str = "\
        SELECT array_remove(ARRAY[ \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_policy',           'SELECT') THEN 'pg_policy'           END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_class',            'SELECT') THEN 'pg_class'            END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_roles',            'SELECT') THEN 'pg_roles'            END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_namespace',        'SELECT') THEN 'pg_namespace'        END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_rewrite',          'SELECT') THEN 'pg_rewrite'          END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_depend',           'SELECT') THEN 'pg_depend'           END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_proc',             'SELECT') THEN 'pg_proc'             END, \
            CASE WHEN NOT has_table_privilege('pg_catalog.pg_db_role_setting',  'SELECT') THEN 'pg_db_role_setting'  END \
        ]::text[], NULL) AS missing";
}

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("upstream query failed: {0}")]
    Upstream(#[from] tokio_postgres::Error),
    /// The snapshot's `COMMIT` failed after all SELECTs completed. The registry
    /// data is correct (REPEATABLE READ guarantees snapshot consistency) but the
    /// connection is in an undefined state and must be dropped, not reused.
    #[error("snapshot COMMIT failed: {0}")]
    CommitFailed(tokio_postgres::Error),
    /// The connection role cannot `SELECT` one or more `pg_catalog` relations the
    /// snapshot reads. Fatal: without them the analyzer cannot tell which tables
    /// are RLS-active, and caching them unpartitioned is a cross-tenant leak.
    #[error("insufficient catalog privileges: connection role is missing SELECT on {0}")]
    InsufficientPrivileges(String),
}

/// Load a full catalog snapshot through `client` and populate `registry`. Wraps
/// the SELECTs in a single `REPEATABLE READ READ ONLY` transaction so the loader
/// sees a single point-in-time view of the catalogs.
///
/// COMMIT failure surfaces as [`LoaderError::CommitFailed`]: the registry data is
/// fine but the connection is unsafe to reuse, so the caller replaces it.
pub async fn load_snapshot(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    // Check catalog grants before opening the snapshot. A missing core catalog is
    // fatal; a missing pg_db_role_setting only disables role-default GUCs, and the
    // registry records it so the partition path can fail closed.
    let role_defaults_available = verify_privileges(client).await?;
    registry.set_role_defaults_available(role_defaults_available);

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;

    if let Err(e) = load_inner(client, registry, role_defaults_available).await {
        // Best-effort rollback so we don't leak an open transaction; surface the
        // original error regardless of whether the rollback succeeds.
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(e);
    }

    client
        .batch_execute("COMMIT")
        .await
        .map_err(LoaderError::CommitFailed)?;

    Ok(())
}

/// Confirm the connection role can read the `pg_catalog` relations the snapshot
/// and poller depend on. Missing a core catalog (anything but
/// `pg_db_role_setting`) is fatal: Readyset cannot tell whether a table is
/// RLS-protected, so caching it would risk a cross-tenant leak. Missing only
/// `pg_db_role_setting` returns `Ok(false)`; the caller skips role-default GUCs
/// and the partition path fails closed for any cache whose keyed GUC is unset.
async fn verify_privileges(client: &tokio_postgres::Client) -> Result<bool, LoaderError> {
    let row = client.query_one(sql::PRIVILEGE_CHECK, &[]).await?;
    let missing: Vec<String> = row.try_get("missing")?;
    let role_defaults_missing = missing.iter().any(|c| c == "pg_db_role_setting");
    let core_missing: Vec<String> = missing
        .into_iter()
        .filter(|c| c != "pg_db_role_setting")
        .collect();
    if !core_missing.is_empty() {
        return Err(LoaderError::InsufficientPrivileges(core_missing.join(", ")));
    }
    Ok(!role_defaults_missing)
}

async fn load_inner(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    role_defaults_available: bool,
) -> Result<(), LoaderError> {
    load_relations(client, registry).await?;
    load_policies(client, registry).await?;
    load_roles(client, registry).await?;
    load_view_deps(client, registry).await?;
    load_func_deps(client, registry).await?;
    if role_defaults_available {
        load_role_default_gucs(client, registry).await?;
    }
    trace!("RLS catalog snapshot loaded");
    Ok(())
}

async fn load_relations(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    for row in client.query(sql::RELATIONS, &[]).await? {
        let oid: PgOid = row.try_get("oid")?;
        let relkind: i8 = row.try_get("relkind")?;
        let relrowsecurity: bool = row.try_get("relrowsecurity")?;
        let relforcerowsecurity: bool = row.try_get("relforcerowsecurity")?;
        let reloptions: Option<Vec<String>> = row.try_get("reloptions").ok();
        let relname: String = row.try_get("relname")?;
        let schema_name: String = row.try_get("schema_name")?;

        let security_invoker = is_security_invoker(reloptions.as_deref());

        registry.set_flags(
            oid,
            RelationFlags {
                relrowsecurity,
                relforcerowsecurity,
                relkind: relkind as u8,
                security_invoker,
            },
        );
        registry.set_relation_name(&schema_name, &relname, oid);
    }
    Ok(())
}

async fn load_policies(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    let mut by_rel: HashMap<PgOid, Vec<Policy>> = HashMap::new();
    for row in client.query(sql::POLICIES, &[]).await? {
        let oid: PgOid = row.try_get("oid")?;
        let polname: String = row.try_get("polname")?;
        let polrelid: PgOid = row.try_get("polrelid")?;
        let polcmd: i8 = row.try_get("polcmd")?;
        let polpermissive: bool = row.try_get("polpermissive")?;
        let polroles: Vec<PgOid> = row.try_get("polroles").unwrap_or_default();
        let using_expr: Option<String> = row.try_get("using_expr").ok();
        let check_expr: Option<String> = row.try_get("check_expr").ok();

        by_rel.entry(polrelid).or_default().push(Policy {
            oid,
            name: polname,
            permissive: polpermissive,
            cmd: polcmd as u8 as char,
            roles: polroles,
            using_expr,
            check_expr,
        });
    }
    for (relid, policies) in by_rel {
        registry.set_policies(relid, policies);
    }
    Ok(())
}

async fn load_roles(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    for row in client.query(sql::ROLES, &[]).await? {
        let oid: PgOid = row.try_get("oid")?;
        let rolname: String = row.try_get("rolname")?;
        let rolsuper: bool = row.try_get("rolsuper")?;
        let rolbypassrls: bool = row.try_get("rolbypassrls")?;
        registry.set_role(
            oid,
            RoleAttrs {
                rolsuper,
                rolbypassrls,
            },
        );
        registry.set_role_name(&rolname, oid);
    }
    Ok(())
}

async fn load_view_deps(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    let rows = client.query(sql::VIEW_DEPS, &[]).await?;
    apply_view_rows(registry, &rows)
}

async fn load_func_deps(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    let rows = client.query(sql::FUNC_DEPS, &[]).await?;
    apply_func_rows(registry, &rows)
}

/// Load each function's attributes and dependencies from [`sql::FUNC_DEPS`]-shaped rows.
fn apply_func_rows(
    registry: &Arc<PolicyRegistry>,
    rows: &[tokio_postgres::Row],
) -> Result<(), LoaderError> {
    let mut by_func: HashMap<PgOid, FuncInfo> = HashMap::new();
    for row in rows {
        let func_oid: PgOid = row.try_get("func_oid")?;
        let provolatile: i8 = row.try_get("provolatile")?;
        let prosecdef: bool = row.try_get("prosecdef")?;
        let proname: String = row.try_get("proname")?;
        let schema_name: String = row.try_get("schema_name")?;

        let info = by_func.entry(func_oid).or_insert_with(|| FuncInfo {
            schema: schema_name,
            name: proname,
            provolatile: provolatile as u8,
            security_definer: prosecdef,
            ..Default::default()
        });
        if let Some(dep) = dep_from_row(row)? {
            info.deps.push(dep);
        }
    }
    for (func, info) in by_func {
        registry.set_func_info(func, info);
    }
    Ok(())
}

/// Parse a row's `dep_cat` / `dep_oid` pair. `None` when the row carries no dependency (the
/// `LEFT JOIN` found none).
fn dep_from_row(row: &tokio_postgres::Row) -> Result<Option<(OidKind, PgOid)>, LoaderError> {
    let Some(dep_oid) = row.try_get::<_, Option<PgOid>>("dep_oid")? else {
        return Ok(None);
    };
    Ok(match row.try_get::<_, &str>("dep_cat")? {
        "R" => Some((OidKind::Rel, dep_oid)),
        "F" => Some((OidKind::Func, dep_oid)),
        _ => None,
    })
}

/// Load each view's flags, name, and relation and function dependencies from
/// [`sql::VIEW_DEPS`]-shaped rows. A view with no relation dependency is left without a dependency
/// entry: it may still read through functions or session settings, so the analyzer must refuse it
/// rather than treat it as reading nothing.
fn apply_view_rows(
    registry: &Arc<PolicyRegistry>,
    rows: &[tokio_postgres::Row],
) -> Result<(), LoaderError> {
    let mut by_view: HashMap<PgOid, Vec<(OidKind, PgOid)>> = HashMap::new();
    for row in rows {
        let view_oid: PgOid = row.try_get("view_oid")?;
        let relkind: i8 = row.try_get("relkind")?;
        let reloptions: Option<Vec<String>> = row.try_get("reloptions")?;
        let relname: String = row.try_get("relname")?;
        let schema_name: String = row.try_get("schema_name")?;
        let dep = dep_from_row(row)?;

        let security_invoker = is_security_invoker(reloptions.as_deref());
        registry.set_flags(
            view_oid,
            RelationFlags {
                relkind: relkind as u8,
                security_invoker,
                ..Default::default()
            },
        );
        registry.set_relation_name(&schema_name, &relname, view_oid);

        let deps = by_view.entry(view_oid).or_default();
        if let Some(dep) = dep {
            deps.push(dep);
        }
    }
    for (view, deps) in by_view {
        if deps.is_empty() {
            registry.remove_view_deps(view);
        } else {
            registry.set_view_underlying(view, deps);
        }
    }
    Ok(())
}

/// Whether `reloptions` turns `security_invoker` on. Postgres stores a reloption's value as
/// written, so `on`, `1`, or `yes` are as valid as `true`.
fn is_security_invoker(reloptions: Option<&[String]>) -> bool {
    reloptions.into_iter().flatten().any(|opt| {
        opt.split_once('=').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("security_invoker") && parse_pg_bool(value) == Some(true)
        })
    })
}

/// Parse a boolean the way Postgres's `parse_bool` does: `true`/`false`, `yes`/`no`, `on`/`off`
/// (or `of`), `1`/`0`, case-insensitive, plus any prefix of `true`, `false`, `yes`, or `no`.
fn parse_pg_bool(value: &str) -> Option<bool> {
    let prefix_of = |word: &str| {
        !value.is_empty()
            && word
                .get(..value.len())
                .is_some_and(|p| p.eq_ignore_ascii_case(value))
    };
    if prefix_of("true") || prefix_of("yes") || value == "1" || value.eq_ignore_ascii_case("on") {
        Some(true)
    } else if prefix_of("false")
        || prefix_of("no")
        || value.eq_ignore_ascii_case("of")
        || value.eq_ignore_ascii_case("off")
        || value == "0"
    {
        Some(false)
    } else {
        None
    }
}

/// Reload a single relation's policies and flags. Used by the poller when a
/// fingerprint change targets one relation, avoiding a full catalog re-read.
pub async fn reload_relation(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    relid: crate::Oid,
) -> Result<(), LoaderError> {
    let flags_rows = client
        .query(
            "SELECT c.oid, c.relkind, c.relrowsecurity, c.relforcerowsecurity, c.reloptions, \
             c.relname, n.nspname AS schema_name \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = $1",
            &[&(relid as PgOid)],
        )
        .await?;
    if flags_rows.is_empty() {
        // Relation dropped since the fingerprint diff observed it. Purge its
        // catalog state rather than leaving stale entries in the registry.
        registry.remove_relation(relid);
        return Ok(());
    }
    for row in flags_rows {
        let relkind: i8 = row.try_get("relkind")?;
        let relrowsecurity: bool = row.try_get("relrowsecurity")?;
        let relforcerowsecurity: bool = row.try_get("relforcerowsecurity")?;
        let reloptions: Option<Vec<String>> = row.try_get("reloptions").ok();
        let relname: String = row.try_get("relname")?;
        let schema_name: String = row.try_get("schema_name")?;
        let security_invoker = is_security_invoker(reloptions.as_deref());
        registry.set_flags(
            relid,
            RelationFlags {
                relrowsecurity,
                relforcerowsecurity,
                relkind: relkind as u8,
                security_invoker,
            },
        );
        registry.set_relation_name(&schema_name, &relname, relid);
    }

    let policy_rows = client
        .query(
            "SELECT p.oid, p.polname, p.polrelid, p.polcmd, p.polpermissive, p.polroles, \
             pg_get_expr(p.polqual, p.polrelid) AS using_expr, \
             pg_get_expr(p.polwithcheck, p.polrelid) AS check_expr \
             FROM pg_catalog.pg_policy p WHERE p.polrelid = $1",
            &[&(relid as PgOid)],
        )
        .await?;
    let mut policies = Vec::new();
    for row in policy_rows {
        let oid: PgOid = row.try_get("oid")?;
        let polname: String = row.try_get("polname")?;
        let polcmd: i8 = row.try_get("polcmd")?;
        let polpermissive: bool = row.try_get("polpermissive")?;
        let polroles: Vec<PgOid> = row.try_get("polroles").unwrap_or_default();
        let using_expr: Option<String> = row.try_get("using_expr").ok();
        let check_expr: Option<String> = row.try_get("check_expr").ok();
        policies.push(Policy {
            oid,
            name: polname,
            permissive: polpermissive,
            cmd: polcmd as u8 as char,
            roles: polroles,
            using_expr,
            check_expr,
        });
    }
    registry.set_policies(relid, policies);
    Ok(())
}

/// Reload a single view's flags, name, and relation dependencies in one query. Used by the poller
/// when a view's dependency fingerprint changes, appears, or disappears.
pub async fn reload_view(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    view_oid: crate::Oid,
) -> Result<(), LoaderError> {
    let rows = client
        .query(sql::VIEW_DEPS_FOR_VIEW, &[&(view_oid as PgOid)])
        .await?;
    if rows.is_empty() {
        // View dropped since the fingerprint diff observed it. Purge its catalog state rather
        // than leaving stale entries in the registry.
        registry.remove_relation(view_oid);
        registry.remove_view_deps(view_oid);
        return Ok(());
    }
    apply_view_rows(registry, &rows)
}

/// Reload a single function's attributes and dependencies in one query. Used by the poller when a
/// function's fingerprint changes, appears, or disappears.
pub async fn reload_func(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    func_oid: crate::Oid,
) -> Result<(), LoaderError> {
    let rows = client
        .query(sql::FUNC_DEPS_FOR_FUNC, &[&(func_oid as PgOid)])
        .await?;
    if rows.is_empty() {
        registry.remove_func_info(func_oid);
        return Ok(());
    }
    apply_func_rows(registry, &rows)
}

/// Reload a single role's attributes from pg_roles.
pub async fn reload_role(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    roleid: crate::Oid,
) -> Result<(), LoaderError> {
    let rows = client
        .query(
            "SELECT oid, rolname, rolsuper, rolbypassrls FROM pg_catalog.pg_roles WHERE oid = $1",
            &[&(roleid as PgOid)],
        )
        .await?;
    if rows.is_empty() {
        // Role dropped since the fingerprint diff observed it. Purge its
        // attributes and name entry.
        registry.remove_role(roleid);
        return Ok(());
    }
    for row in rows {
        let rolname: String = row.try_get("rolname")?;
        let rolsuper: bool = row.try_get("rolsuper")?;
        let rolbypassrls: bool = row.try_get("rolbypassrls")?;
        registry.set_role(
            roleid,
            RoleAttrs {
                rolsuper,
                rolbypassrls,
            },
        );
        registry.set_role_name(&rolname, roleid);
    }
    Ok(())
}

async fn load_role_default_gucs(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
) -> Result<(), LoaderError> {
    let mut by_role: HashMap<PgOid, HashMap<String, String>> = HashMap::new();
    for row in client.query(sql::ROLE_DEFAULT_GUCS, &[]).await? {
        let oid: PgOid = row.try_get("oid")?;
        let setconfig: Option<Vec<String>> = row.try_get("setconfig").ok();
        let entry = by_role.entry(oid).or_default();
        if let Some(settings) = setconfig {
            for s in settings {
                if let Some((name, value)) = s.split_once('=') {
                    entry.insert(name.to_owned(), value.to_owned());
                }
            }
        }
    }
    for (role, gucs) in by_role {
        registry.set_role_default_gucs(role, gucs);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_sql_contains_expected_fingerprint_inputs() {
        // The poller's reload path reuses these query strings; dropping a column
        // would lose a hash input and break change detection. Lock the columns in.
        assert!(sql::POLICIES.contains("pg_get_expr(p.polqual"));
        assert!(sql::POLICIES.contains("pg_get_expr(p.polwithcheck"));
        assert!(sql::RELATIONS.contains("relrowsecurity"));
        assert!(sql::RELATIONS.contains("reloptions"));
        assert!(sql::ROLES.contains("rolbypassrls"));
        assert!(sql::VIEW_DEPS.contains("pg_rewrite"));
        assert!(sql::ROLE_DEFAULT_GUCS.contains("pg_db_role_setting"));
        // Must not read pg_authid.
        assert!(!sql::ROLES.contains("pg_authid"));
    }

    #[test]
    fn security_invoker_accepts_every_postgres_boolean_spelling() {
        let set = |opts: &[&str]| {
            let opts: Vec<String> = opts.iter().map(|o| o.to_string()).collect();
            is_security_invoker(Some(&opts))
        };
        for on in ["true", "TRUE", "t", "tr", "on", "ON", "1", "yes", "y", "Ye"] {
            assert!(set(&[&format!("security_invoker={on}")]), "{on}");
        }
        for off in ["false", "f", "off", "OF", "0", "no", "n", "o", ""] {
            assert!(!set(&[&format!("security_invoker={off}")]), "{off}");
        }
        assert!(set(&["check_option=local", "security_invoker=on"]));
        assert!(!set(&["security_barrier=true"]));
        assert!(!is_security_invoker(None));
    }
}
