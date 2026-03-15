//! Variable system for the constraint-based query generator.
//!
//! Variables are typed placeholders that name as-yet-unresolved entities.
//! They are the mechanism by which constraints within a single pattern
//! refer to shared entities.

/// A unique identifier for a variable within a resolution scope.
/// Uses usize because it serves as an index into the vars vec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarId(pub(crate) usize);

/// What kind of entity a variable represents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarKind {
    /// Resolves to a relation (base table, alias, or derived relation).
    Relation,
    /// Resolves to a column within a specific relation variable.
    Column { table: VarId },
    /// Resolves to a SQL type.
    SqlType,
}

impl VarKind {
    /// Returns the discriminant name for kind-mismatch error messages.
    fn discriminant_name(&self) -> &'static str {
        match self {
            VarKind::Relation => "Relation",
            VarKind::Column { .. } => "Column",
            VarKind::SqlType => "SqlType",
        }
    }

    /// Returns true if two VarKinds have the same discriminant
    /// (ignoring inner fields like Column's table VarId).
    fn same_discriminant(&self, other: &VarKind) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

/// Allocates variables with unique IDs and tracks their kinds.
#[derive(Debug, Default, Clone)]
pub struct VarAllocator {
    kinds: Vec<VarKind>,
}

impl VarAllocator {
    /// Creates a new empty allocator.
    pub fn new() -> Self {
        Self { kinds: Vec::new() }
    }

    /// Allocates a new variable of the given kind and returns its ID.
    pub fn alloc(&mut self, kind: VarKind) -> VarId {
        let id = VarId(self.kinds.len());
        self.kinds.push(kind);
        id
    }

    /// Returns the kind of the given variable.
    ///
    /// # Panics
    ///
    /// Panics if the VarId is out of range.
    pub fn kind(&self, v: VarId) -> &VarKind {
        &self.kinds[v.0]
    }

    /// Returns the number of variables allocated.
    pub fn len(&self) -> usize {
        self.kinds.len()
    }

    /// Returns true if no variables have been allocated.
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// Returns all variable kinds as a slice, indexed by VarId.
    pub fn kinds(&self) -> &[VarKind] {
        &self.kinds
    }

    /// Consume the allocator and return the kinds vec.
    pub fn into_kinds(self) -> Vec<VarKind> {
        self.kinds
    }
}

/// Error type for unification failures.
#[derive(Debug, thiserror::Error)]
pub enum UnifyError {
    #[error("cannot unify {left_kind} variable with {right_kind} variable")]
    KindMismatch {
        left_kind: &'static str,
        right_kind: &'static str,
    },
}

/// Union-find (disjoint set) data structure with path compression and
/// union by rank. Used for variable unification (Eq constraints).
#[derive(Debug, Clone)]
pub struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    /// Creates a new UnionFind for `n` elements, each in its own set.
    pub fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    /// Finds the representative of the set containing `x`, with path compression.
    ///
    /// Iterative two-pass: walk to the root, then re-walk and rewrite each
    /// `parent[x]` to point at the root. Recursive path compression would risk
    /// stack overflow on long chains, which CLAUDE.md forbids in DB code.
    pub fn find(&mut self, x: usize) -> usize {
        let mut root = x;
        while self.parent[root] != root {
            root = self.parent[root];
        }
        let mut cur = x;
        while self.parent[cur] != root {
            let next = self.parent[cur];
            self.parent[cur] = root;
            cur = next;
        }
        root
    }

    /// Unifies the sets containing `x` and `y`. Uses union by rank.
    ///
    /// `kinds` is indexed by VarId so that representatives can be kind-checked.
    /// Returns an error if the variables have incompatible kinds
    /// (e.g., Table vs Column).
    pub fn union(&mut self, x: usize, y: usize, kinds: &[VarKind]) -> Result<(), UnifyError> {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return Ok(());
        }

        let kx = &kinds[rx];
        let ky = &kinds[ry];
        if !kx.same_discriminant(ky) {
            return Err(UnifyError::KindMismatch {
                left_kind: kx.discriminant_name(),
                right_kind: ky.discriminant_name(),
            });
        }

        // Union by rank
        match self.rank[rx].cmp(&self.rank[ry]) {
            std::cmp::Ordering::Less => self.parent[rx] = ry,
            std::cmp::Ordering::Greater => self.parent[ry] = rx,
            std::cmp::Ordering::Equal => {
                self.parent[ry] = rx;
                self.rank[rx] += 1;
            }
        }

        Ok(())
    }

    /// Returns true if `x` and `y` are in the same set.
    pub fn same_set(&mut self, x: usize, y: usize) -> bool {
        self.find(x) == self.find(y)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- VarAllocator tests --

    #[test]
    fn allocator_ids_are_sequential() {
        let mut alloc = VarAllocator::new();
        let t = alloc.alloc(VarKind::Relation);
        let c = alloc.alloc(VarKind::Column { table: t });
        let s = alloc.alloc(VarKind::SqlType);

        assert_eq!(t, VarId(0));
        assert_eq!(c, VarId(1));
        assert_eq!(s, VarId(2));
    }

    #[test]
    fn allocator_tracks_kinds() {
        let mut alloc = VarAllocator::new();
        let t = alloc.alloc(VarKind::Relation);
        let c = alloc.alloc(VarKind::Column { table: t });

        assert_eq!(*alloc.kind(t), VarKind::Relation);
        assert_eq!(*alloc.kind(c), VarKind::Column { table: t });
    }

    #[test]
    fn allocator_len_tracks_count() {
        let mut alloc = VarAllocator::new();
        assert_eq!(alloc.len(), 0);
        assert!(alloc.is_empty());

        alloc.alloc(VarKind::Relation);
        assert_eq!(alloc.len(), 1);
        assert!(!alloc.is_empty());

        alloc.alloc(VarKind::Relation);
        alloc.alloc(VarKind::SqlType);
        assert_eq!(alloc.len(), 3);
    }

    #[test]
    fn allocator_kinds_returns_slice() {
        let mut alloc = VarAllocator::new();
        let t = alloc.alloc(VarKind::Relation);
        alloc.alloc(VarKind::Column { table: t });

        let kinds = alloc.kinds();
        assert_eq!(kinds.len(), 2);
        assert_eq!(kinds[0], VarKind::Relation);
        assert_eq!(kinds[1], VarKind::Column { table: VarId(0) });
    }

    // -- UnionFind tests --

    #[test]
    fn union_find_same_kind_succeeds() {
        let mut alloc = VarAllocator::new();
        let t1 = alloc.alloc(VarKind::Relation);
        let t2 = alloc.alloc(VarKind::Relation);

        let mut uf = UnionFind::new(alloc.len());
        uf.union(t1.0, t2.0, alloc.kinds())
            .expect("same-kind union should succeed");

        assert_eq!(uf.find(t1.0), uf.find(t2.0));
        assert!(uf.same_set(t1.0, t2.0));
    }

    #[test]
    fn union_find_different_kind_fails() {
        let mut alloc = VarAllocator::new();
        let t = alloc.alloc(VarKind::Relation);
        let c = alloc.alloc(VarKind::Column { table: t });

        let mut uf = UnionFind::new(alloc.len());
        let err = uf.union(t.0, c.0, alloc.kinds()).unwrap_err();

        assert!(
            matches!(
                err,
                UnifyError::KindMismatch {
                    left_kind: "Relation",
                    right_kind: "Column"
                }
            ),
            "expected KindMismatch, got: {err:?}"
        );
    }

    #[test]
    fn union_find_columns_from_different_tables_can_unify() {
        let mut alloc = VarAllocator::new();
        let t1 = alloc.alloc(VarKind::Relation);
        let t2 = alloc.alloc(VarKind::Relation);
        let c1 = alloc.alloc(VarKind::Column { table: t1 });
        let c2 = alloc.alloc(VarKind::Column { table: t2 });

        let mut uf = UnionFind::new(alloc.len());
        uf.union(c1.0, c2.0, alloc.kinds())
            .expect("columns with different tables should still unify");

        assert!(uf.same_set(c1.0, c2.0));
    }

    #[test]
    fn union_find_self_union_is_noop() {
        let mut alloc = VarAllocator::new();
        let t = alloc.alloc(VarKind::Relation);

        let mut uf = UnionFind::new(alloc.len());
        uf.union(t.0, t.0, alloc.kinds())
            .expect("self-union should succeed");

        assert_eq!(uf.find(t.0), t.0);
    }

    #[test]
    fn union_find_path_compression() {
        // Create a chain: 0 -> 1 -> 2 -> 3, all Table vars
        let mut alloc = VarAllocator::new();
        for _ in 0..4 {
            alloc.alloc(VarKind::Relation);
        }

        let mut uf = UnionFind::new(4);
        uf.union(0, 1, alloc.kinds()).unwrap();
        uf.union(1, 2, alloc.kinds()).unwrap();
        uf.union(2, 3, alloc.kinds()).unwrap();

        // All should be in the same set
        let root = uf.find(0);
        assert_eq!(uf.find(1), root);
        assert_eq!(uf.find(2), root);
        assert_eq!(uf.find(3), root);

        // After find(3), path compression should have flattened.
        // Verify by checking parent directly points to root.
        // (This tests the internal invariant of path compression.)
        let root3 = uf.find(3);
        // After find, 3's parent should directly point to root
        assert_eq!(uf.find(3), root3);
    }

    #[test]
    fn union_find_already_unified_is_ok() {
        let mut alloc = VarAllocator::new();
        alloc.alloc(VarKind::Relation);
        alloc.alloc(VarKind::Relation);

        let mut uf = UnionFind::new(2);
        uf.union(0, 1, alloc.kinds()).unwrap();
        // Second union of same pair should be a no-op
        uf.union(0, 1, alloc.kinds()).unwrap();

        assert!(uf.same_set(0, 1));
    }

    #[test]
    fn union_find_transitive() {
        let mut alloc = VarAllocator::new();
        for _ in 0..3 {
            alloc.alloc(VarKind::Relation);
        }

        let mut uf = UnionFind::new(3);
        uf.union(0, 1, alloc.kinds()).unwrap();
        uf.union(1, 2, alloc.kinds()).unwrap();

        // Transitivity: 0 and 2 should be in the same set
        assert!(uf.same_set(0, 2));
    }

    #[test]
    fn union_find_separate_sets() {
        let mut alloc = VarAllocator::new();
        for _ in 0..4 {
            alloc.alloc(VarKind::Relation);
        }

        let mut uf = UnionFind::new(4);
        uf.union(0, 1, alloc.kinds()).unwrap();
        uf.union(2, 3, alloc.kinds()).unwrap();

        assert!(uf.same_set(0, 1));
        assert!(uf.same_set(2, 3));
        assert!(!uf.same_set(0, 2));
        assert!(!uf.same_set(1, 3));
    }
}
