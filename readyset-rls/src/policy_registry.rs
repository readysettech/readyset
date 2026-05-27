//! In-memory snapshot of `pg_policy` / `pg_class` / `pg_roles`.
//!
//! Owned by the catalog poller and consulted by the analyzer at `CREATE CACHE`
//! time. Mutated only by the poller's reload path; the analyzer reads under an
//! RCU-style guard.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::Oid;
use papaya::HashMap as ConcurrentMap;
use seize::Collector;

/// Per-relation parsed RLS policy. Stores the raw expression text
/// from `pg_get_expr(polqual, polrelid)` plus the `pg_policy`
/// metadata. The analyzer re-parses the expression on demand.
#[derive(Debug, Clone)]
pub struct Policy {
    pub oid: Oid,
    pub name: String,
    /// Permissive (`true`) vs restrictive (`false`).
    pub permissive: bool,
    /// `polcmd`: `'r'` for SELECT, `'a'` for INSERT, etc.; only `'r'`
    /// affects cache hit-side correctness.
    pub cmd: char,
    /// Roles the policy applies to. Empty slice == PUBLIC.
    pub roles: Vec<Oid>,
    /// `pg_get_expr(polqual, polrelid)`. May be empty for INSERT-only
    /// policies with only a WITH CHECK expression.
    pub using_expr: Option<String>,
    /// `pg_get_expr(polwithcheck, polrelid)`.
    pub check_expr: Option<String>,
}

/// Per-table flags from `pg_class`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationFlags {
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    /// Kind: 'r' (table), 'p' (partitioned), 'v' (view), 'm' (matview).
    /// Stored as a byte so we can refuse partitioned tables early.
    pub relkind: u8,
    /// For views, whether `security_invoker = true` is set (PG 15+).
    pub security_invoker: bool,
}

/// Per-role attributes from `pg_roles`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoleAttrs {
    pub rolsuper: bool,
    pub rolbypassrls: bool,
}

/// Key into the relation name index. Stored verbatim: the loader
/// writes `pg_class.relname` / `pg_namespace.nspname` directly, and
/// the caller folds unquoted identifiers to lowercase per Postgres's
/// rule before lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RelationName {
    schema: String,
    name: String,
}

/// Snapshot of the catalog tables that drive RLS evaluation.
///
/// Independent typed maps share one `seize::Collector` so each getter is
/// type-safe. Read-side methods return cloned values to avoid holding the
/// internal locks across analyzer work.
#[derive(Debug)]
pub struct PolicyRegistry {
    policies: ConcurrentMap<Oid, Arc<Vec<Policy>>>,
    flags: ConcurrentMap<Oid, RelationFlags>,
    roles: ConcurrentMap<Oid, RoleAttrs>,
    view_underlying: ConcurrentMap<Oid, Arc<Vec<Oid>>>,
    role_gucs: ConcurrentMap<Oid, Arc<HashMap<String, String>>>,
    relations_by_name: ConcurrentMap<RelationName, Oid>,
    roles_by_name: ConcurrentMap<String, Oid>,
    generation: AtomicU64,
    /// Whether the connection role could read `pg_db_role_setting` at the last
    /// snapshot. When false, role-default GUCs are unknown, so the partition path
    /// fails closed for any cache whose keyed GUC the session left unset: a role
    /// default could be hiding behind it.
    role_defaults_available: AtomicBool,
}

fn new_table<K, V>() -> ConcurrentMap<K, V>
where
    K: std::hash::Hash + Eq + Send + Sync,
    V: Send + Sync,
{
    let gc = Collector::new().batch_size(1);
    ConcurrentMap::builder().collector(gc).build()
}

impl Default for PolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyRegistry {
    pub fn new() -> Self {
        Self {
            policies: new_table(),
            flags: new_table(),
            roles: new_table(),
            view_underlying: new_table(),
            role_gucs: new_table(),
            relations_by_name: new_table(),
            roles_by_name: new_table(),
            generation: AtomicU64::new(1),
            role_defaults_available: AtomicBool::new(true),
        }
    }

    /// Whether role-default GUCs (`pg_db_role_setting`) were loadable at the
    /// last snapshot. `false` means the partition path must fail closed for a
    /// cache whose keyed GUC the session left unset.
    pub fn role_defaults_available(&self) -> bool {
        self.role_defaults_available.load(Ordering::Acquire)
    }

    /// Record whether `pg_db_role_setting` was readable at snapshot time.
    /// Set by the loader after the privilege preflight.
    pub fn set_role_defaults_available(&self, available: bool) {
        self.role_defaults_available
            .store(available, Ordering::Release);
    }

    /// Current generation number. Stamped into shallow-cache keys so a
    /// poll-detected change invalidates the previous entries.
    ///
    /// Acquire-ordered: a reader observing a generation also observes every
    /// registry mutation that preceded the matching `bump_generation` release.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Bump the generation. Called by the poller after a reload completes.
    /// Release-ordered so a reader observing the new generation also observes
    /// every prior registry mutation that contributed to it.
    pub fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Release) + 1
    }

    /// Look up the policies attached to a relation.
    pub fn policies_for(&self, relid: Oid) -> Option<Arc<Vec<Policy>>> {
        self.policies.pin().get(&relid).map(Arc::clone)
    }

    pub fn flags_for(&self, relid: Oid) -> Option<RelationFlags> {
        self.flags.pin().get(&relid).copied()
    }

    pub fn role_attrs(&self, roleid: Oid) -> Option<RoleAttrs> {
        self.roles.pin().get(&roleid).copied()
    }

    pub fn view_underlying(&self, view_oid: Oid) -> Option<Arc<Vec<Oid>>> {
        self.view_underlying.pin().get(&view_oid).map(Arc::clone)
    }

    /// Resolve a `(schema, table)` reference to an OID.
    ///
    /// `schema = None` falls back to a `public` lookup, matching Postgres's
    /// default search-path semantics. Lookups are case-sensitive against names
    /// stored verbatim from `pg_class.relname`; the caller folds unquoted
    /// identifiers to lowercase first, so `"Customers"` does not alias
    /// `customers`.
    pub fn relid_for(&self, schema: Option<&str>, name: &str) -> Option<Oid> {
        let lookup = RelationName {
            schema: schema.unwrap_or("public").to_owned(),
            name: name.to_owned(),
        };
        self.relations_by_name.pin().get(&lookup).copied()
    }

    /// Install or update a name -> OID entry. Names are stored verbatim from
    /// `pg_class.relname` / `pg_namespace.nspname`; no case-folding here.
    pub fn set_relation_name(&self, schema: &str, name: &str, oid: Oid) {
        self.relations_by_name.pin().insert(
            RelationName {
                schema: schema.to_owned(),
                name: name.to_owned(),
            },
            oid,
        );
    }

    pub fn role_default_gucs(&self, roleid: Oid) -> Option<Arc<HashMap<String, String>>> {
        self.role_gucs.pin().get(&roleid).map(Arc::clone)
    }

    /// Resolve a role name to its login-time default GUCs (`ALTER ROLE ... SET`,
    /// `ALTER ROLE ... IN DATABASE ... SET`). `None` when the role is unknown or
    /// has no defaults.
    pub fn role_default_gucs_for(&self, name: &str) -> Option<Arc<HashMap<String, String>>> {
        self.role_oid_for(name)
            .and_then(|oid| self.role_default_gucs(oid))
    }

    /// Replace the policies for `relid`. The caller bumps the generation once a
    /// coherent set of updates lands.
    pub fn set_policies(&self, relid: Oid, policies: Vec<Policy>) {
        self.policies.pin().insert(relid, Arc::new(policies));
    }

    pub fn set_flags(&self, relid: Oid, flags: RelationFlags) {
        self.flags.pin().insert(relid, flags);
    }

    /// Drop every trace of a relation: its policies, flags, view dependencies,
    /// and any name-index entry that resolves to it, so a dropped table cannot
    /// leave stale `relrowsecurity` flags or a name mapping behind.
    pub fn remove_relation(&self, relid: Oid) {
        self.policies.pin().remove(&relid);
        self.flags.pin().remove(&relid);
        self.view_underlying.pin().remove(&relid);
        self.relations_by_name.pin().retain(|_, &oid| oid != relid);
    }

    pub fn set_role(&self, roleid: Oid, attrs: RoleAttrs) {
        self.roles.pin().insert(roleid, attrs);
    }

    /// Resolve a role name to its OID. `None` when the role has not yet been
    /// observed by the loader (typical at boot before the first snapshot). Names
    /// are matched case-exact against `pg_roles.rolname`.
    pub fn role_oid_for(&self, name: &str) -> Option<Oid> {
        self.roles_by_name.pin().get(name).copied()
    }

    /// Install or update a role name -> OID entry.
    pub fn set_role_name(&self, name: &str, oid: Oid) {
        self.roles_by_name.pin().insert(name.to_owned(), oid);
    }

    /// Drop every trace of a role: its attributes, login-time default GUCs, and
    /// any name-index entry that resolves to it, so a dropped role cannot leave
    /// a stale `rolbypassrls` attribute behind.
    pub fn remove_role(&self, roleid: Oid) {
        self.roles.pin().remove(&roleid);
        self.role_gucs.pin().remove(&roleid);
        self.roles_by_name.pin().retain(|_, &oid| oid != roleid);
    }

    /// `true` when role `name` is known and carries `rolsuper` or
    /// `rolbypassrls`. `false` when the role is absent (registry not seeded yet)
    /// or has neither attribute, letting the caller partition by role identity
    /// rather than collapse onto the bypass slot.
    pub fn bypass_rls_for_role(&self, name: &str) -> bool {
        self.role_oid_for(name)
            .and_then(|oid| self.role_attrs(oid))
            .map(|attrs| attrs.rolsuper || attrs.rolbypassrls)
            .unwrap_or(false)
    }

    pub fn set_view_underlying(&self, view_oid: Oid, underlying: Vec<Oid>) {
        self.view_underlying
            .pin()
            .insert(view_oid, Arc::new(underlying));
    }

    pub fn set_role_default_gucs(&self, roleid: Oid, gucs: HashMap<String, String>) {
        self.role_gucs.pin().insert(roleid, Arc::new(gucs));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_starts_at_one_and_bumps_monotonically() {
        let r = PolicyRegistry::new();
        assert_eq!(r.generation(), 1);
        assert_eq!(r.bump_generation(), 2);
        assert_eq!(r.bump_generation(), 3);
        assert_eq!(r.generation(), 3);
    }

    #[test]
    fn round_trip_policies_flags_and_role() {
        let r = PolicyRegistry::new();
        r.set_policies(
            100,
            vec![Policy {
                oid: 999,
                name: "owner_policy".into(),
                permissive: true,
                cmd: 'r',
                roles: vec![10],
                using_expr: Some("user_id = (SELECT auth.uid())".into()),
                check_expr: None,
            }],
        );
        r.set_flags(
            100,
            RelationFlags {
                relrowsecurity: true,
                relforcerowsecurity: false,
                relkind: b'r',
                security_invoker: false,
            },
        );
        r.set_role(
            10,
            RoleAttrs {
                rolsuper: false,
                rolbypassrls: false,
            },
        );

        let policies = r.policies_for(100).expect("policies");
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "owner_policy");
        assert!(r.flags_for(100).unwrap().relrowsecurity);
        assert!(!r.role_attrs(10).unwrap().rolbypassrls);
    }

    #[test]
    fn remove_relation_purges_all_relation_state() {
        let r = PolicyRegistry::new();
        r.set_flags(100, RelationFlags::default());
        r.set_policies(100, vec![]);
        r.set_view_underlying(100, vec![7]);
        r.set_relation_name("public", "widgets", 100);

        r.remove_relation(100);

        assert!(r.flags_for(100).is_none());
        assert!(r.policies_for(100).is_none());
        assert!(r.view_underlying(100).is_none());
        assert!(r.relid_for(Some("public"), "widgets").is_none());
    }

    #[test]
    fn remove_role_purges_all_role_state() {
        let r = PolicyRegistry::new();
        r.set_role(10, RoleAttrs::default());
        r.set_role_name("tenant_user", 10);
        r.set_role_default_gucs(10, HashMap::from([("app.tenant".into(), "1".into())]));

        r.remove_role(10);

        assert!(r.role_attrs(10).is_none());
        assert!(r.role_oid_for("tenant_user").is_none());
        assert!(r.role_default_gucs(10).is_none());
    }
}
