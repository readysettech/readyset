//! The adapter-side cache key for shallow caches.
//!
//! The key pairs the bound query parameters with the session-derived values the cached result
//! depends on ([`SessionInputValues`]). A session-independent cache carries an empty value set, so
//! every session shares its entries; an RLS-scoped lookup folds in the policy-registry generation
//! plus the session's identity and policy-read values, so each security context gets its own
//! partition. Equality and hashing over this key keep one session's cached rows from being served
//! to another.

use readyset_data::DfValue;
use readyset_rls::SessionInputType;
use readyset_sql::ast::SqlIdentifier;
use readyset_util::SizeOf;

/// One session-derived fact a cached result depends on, resolved against the calling session.
/// `Ord` provides the canonical ordering [`SessionInputValues`] keeps its contents in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionInputValue {
    /// Marker for a `BYPASSRLS` session. A scoped lookup under bypass carries only this and the
    /// generation: the policy is not evaluated, so the result is independent of the rest of the
    /// session state and every bypass session shares one partition.
    Bypass,
    /// Policy-registry generation stamped into every RLS-scoped key. A catalog change bumps it,
    /// orphaning keys stamped under the old one.
    RlsGeneration(u64),
    /// Effective role. Folded into every non-bypass scoped key.
    Role(SqlIdentifier),
    /// Session user. Folded into every non-bypass scoped key.
    SessionUser(SqlIdentifier),
    /// A policy-read input and the value the session resolved for it. A `None` value distinguishes
    /// an unset input from an empty one.
    Rls(SessionInputType, Option<Box<str>>),
}

/// The session half of a [`ShallowKey`]: the set of session-derived values the cached result
/// depends on. Contents stay sorted and deduplicated through [`SessionInputValues::add`], so the
/// derived equality and hashing are independent of the order contributors added values in. Empty
/// means the result is session-independent (a plain cache).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SessionInputValues {
    /// Sorted, deduplicated.
    values: Vec<SessionInputValue>,
}

impl SessionInputValues {
    /// Insert `value`, keeping the contents sorted and ignoring an exact duplicate.
    pub fn add(&mut self, value: SessionInputValue) {
        if let Err(pos) = self.values.binary_search(&value) {
            self.values.insert(pos, value);
        }
    }

    /// `true` when no session-derived value was contributed: the key is session-independent and
    /// every session shares its entries.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Opaque cache key the adapter instantiates `readyset-shallow` with.
/// The cache treats it as an opaque `K`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShallowKey {
    pub params: Vec<DfValue>,
    pub session: SessionInputValues,
}

impl ShallowKey {
    /// Key for a session-independent lookup: params only.
    pub fn plain(params: Vec<DfValue>) -> Self {
        Self {
            params,
            session: SessionInputValues::default(),
        }
    }
}

impl SizeOf for ShallowKey {
    fn deep_size_of(&self) -> usize {
        // The session values are a handful of identifiers and short claim
        // values; charge their inline size and let the params dominate.
        size_of::<Self>()
            + self.params.deep_size_of()
            + self.session.values.capacity() * size_of::<SessionInputValue>()
    }

    fn size_is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rls_val(name: &str, value: Option<&str>) -> SessionInputValue {
        SessionInputValue::Rls(SessionInputType::guc(name), value.map(Box::from))
    }

    fn values(items: &[SessionInputValue]) -> SessionInputValues {
        let mut out = SessionInputValues::default();
        for item in items {
            out.add(item.clone());
        }
        out
    }

    fn std_hash(v: &SessionInputValues) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    #[test]
    fn add_order_does_not_affect_equality_or_hash() {
        let a = values(&[rls_val("b", Some("2")), rls_val("a", Some("1"))]);
        let b = values(&[rls_val("a", Some("1")), rls_val("b", Some("2"))]);
        assert_eq!(a, b);
        assert_eq!(std_hash(&a), std_hash(&b));
    }

    #[test]
    fn add_deduplicates_exact_values() {
        let once = values(&[rls_val("a", Some("1"))]);
        let twice = values(&[rls_val("a", Some("1")), rls_val("a", Some("1"))]);
        assert_eq!(once, twice);
    }

    #[test]
    fn distinct_values_are_unequal() {
        assert_ne!(
            values(&[rls_val("k", Some("alice"))]),
            values(&[rls_val("k", Some("bob"))]),
        );
    }

    #[test]
    fn unset_vs_empty_value_are_distinct() {
        assert_ne!(
            values(&[rls_val("k", None)]),
            values(&[rls_val("k", Some(""))]),
        );
    }

    #[test]
    fn empty_is_plain_and_distinct_from_any_contribution() {
        let plain = SessionInputValues::default();
        assert!(plain.is_empty());
        assert_ne!(plain, values(&[SessionInputValue::Bypass]));
        assert_ne!(plain, values(&[SessionInputValue::RlsGeneration(1)]));
    }

    #[test]
    fn role_and_user_and_generation_partition_the_key() {
        let role = |r: &str| SessionInputValue::Role(SqlIdentifier::from(r));
        let base = &[
            role("authenticated"),
            SessionInputValue::SessionUser(SqlIdentifier::from("login")),
            SessionInputValue::RlsGeneration(1),
        ];
        let mut other_role = base.to_vec();
        other_role[0] = role("anon");
        assert_ne!(values(base), values(&other_role));

        let mut other_gen = base.to_vec();
        other_gen[2] = SessionInputValue::RlsGeneration(2);
        assert_ne!(values(base), values(&other_gen));
    }

    #[test]
    fn shallow_keys_differ_by_session_values() {
        let ka = ShallowKey {
            params: vec![],
            session: values(&[rls_val("k", Some("a"))]),
        };
        let kb = ShallowKey {
            params: vec![],
            session: values(&[rls_val("k", Some("b"))]),
        };
        assert_ne!(ka, kb);
        assert_eq!(ka, ka.clone());
    }

    #[test]
    fn plain_keys_compare_on_params_alone() {
        let a = ShallowKey::plain(vec![DfValue::from(1)]);
        let b = ShallowKey::plain(vec![DfValue::from(1)]);
        let c = ShallowKey::plain(vec![DfValue::from(2)]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
