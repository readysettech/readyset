//! Walk a [`ShallowCacheQuery`], collect the relations it references, and resolve each against the
//! [`readyset_rls::PolicyRegistry`] so the analyzer can decide whether the query is RLS-aware.
//!
//! Lives in `readyset-adapter` rather than `readyset-rls` because the shallow-cache AST type is
//! sqlparser-flavoured while the registry is Postgres-flavoured; keeping AST traversal out of the
//! registry crate leaves it testable without parser dependencies.

use std::collections::HashSet;
use std::sync::Arc;

use readyset_rls::Oid;
use readyset_sql::ast::{ShallowCacheQuery, SqlIdentifier};
use sqlparser::ast::{Query, SetExpr, TableFactor};

use readyset_rls::PolicyRegistry;

/// An unresolved relation name seen while walking the query. Collected so the caller can decide
/// whether to defer recovery (registry not seeded yet) or skip permanently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownRelation {
    pub schema: Option<String>,
    pub name: String,
}

impl UnknownRelation {
    /// `<schema>.<name>`, or just `<name>` for unqualified references.
    pub fn qualified(&self) -> String {
        match &self.schema {
            Some(s) => format!("{s}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// Resolve every relation referenced by `query` against `registry`.
///
/// Fail-closed: returns `Err(Vec<UnknownRelation>)` listing every referenced relation that has no
/// OID in the registry, rather than an empty OID list that would let the analyzer treat the query
/// as non-RLS and build a `Plain` cache over a possibly RLS-protected table. The caller can then
/// defer (registry not seeded yet) or skip permanently (e.g. a typo).
///
/// Unqualified names resolve against `schema_search_path` in order, first match wins, as Postgres
/// does: `search_path = private, public` reading `documents` resolves `private.documents`, not a
/// same-named `public.documents` decoy. Hard-coding `public` instead could build a `Plain` cache
/// over an RLS-protected table and leak rows across tenants.
pub fn extract_referenced_relation_oids(
    query: &ShallowCacheQuery,
    registry: &Arc<PolicyRegistry>,
    schema_search_path: &[SqlIdentifier],
) -> Result<Vec<Oid>, Vec<UnknownRelation>> {
    let mut names = Vec::new();
    walk_query(query, &HashSet::new(), &mut names);

    let mut oids = Vec::with_capacity(names.len());
    let mut seen_oids = std::collections::HashSet::with_capacity(names.len());
    let mut unknown = Vec::new();
    let mut seen_unknown: std::collections::HashSet<(Option<String>, String)> =
        std::collections::HashSet::new();
    for (schema, name) in names {
        let resolved = match &schema {
            Some(s) => registry.relid_for(Some(s), &name),
            None => resolve_unqualified(registry, &name, schema_search_path),
        };
        match resolved {
            Some(oid) => {
                if seen_oids.insert(oid) {
                    oids.push(oid);
                }
            }
            None => {
                if seen_unknown.insert((schema.clone(), name.clone())) {
                    unknown.push(UnknownRelation { schema, name });
                }
            }
        }
    }
    if !unknown.is_empty() {
        return Err(unknown);
    }
    Ok(oids)
}

/// Resolve an unqualified relation name against the search path, first match wins. An empty path
/// falls back to `public`; schemas holding no such relation (or nonexistent, e.g. an unexpanded
/// `$user`) are skipped.
fn resolve_unqualified(
    registry: &Arc<PolicyRegistry>,
    name: &str,
    schema_search_path: &[SqlIdentifier],
) -> Option<Oid> {
    if schema_search_path.is_empty() {
        return registry.relid_for(None, name);
    }
    schema_search_path
        .iter()
        .find_map(|schema| registry.relid_for(Some(schema.as_str()), name))
}

/// Walk `query`, collecting catalog relation references into `out`.
///
/// `cte_scope` carries names bound by enclosing `WITH` clauses. An unqualified reference to such a
/// name is a query-local CTE alias (e.g. PostgREST's `pgrst_source`), not a catalog relation, so it
/// is skipped, matching how Postgres shadows a table with a same-named CTE. Names are folded with
/// the same rule as table references so quoted/unquoted forms compare correctly.
fn walk_query(query: &Query, cte_scope: &HashSet<String>, out: &mut Vec<(Option<String>, String)>) {
    let Some(with) = &query.with else {
        walk_set_expr(&query.body, cte_scope, out);
        return;
    };

    // CTE visibility follows lexical order: each CTE body sees the
    // enclosing scope plus the CTEs declared before it, and a recursive
    // CTE additionally sees its own name. The query body sees every CTE
    // in this clause.
    let mut scope = cte_scope.clone();
    for cte in &with.cte_tables {
        let name = fold_identifier(&cte.alias.name);
        if with.recursive {
            let mut self_scope = scope.clone();
            self_scope.insert(name.clone());
            walk_query(&cte.query, &self_scope, out);
        } else {
            walk_query(&cte.query, &scope, out);
        }
        scope.insert(name);
    }
    walk_set_expr(&query.body, &scope, out);
}

fn walk_set_expr(
    expr: &SetExpr,
    cte_scope: &HashSet<String>,
    out: &mut Vec<(Option<String>, String)>,
) {
    match expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                walk_table_factor(&table_with_joins.relation, cte_scope, out);
                for join in &table_with_joins.joins {
                    walk_table_factor(&join.relation, cte_scope, out);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left, cte_scope, out);
            walk_set_expr(right, cte_scope, out);
        }
        SetExpr::Query(q) => walk_query(q, cte_scope, out),
        _ => {}
    }
}

/// Apply Postgres identifier folding: unquoted identifiers fold to lowercase, double-quoted ones
/// preserve case, matching what the upstream stored in `pg_class`.
fn fold_identifier(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_none() {
        ident.value.to_ascii_lowercase()
    } else {
        ident.value.clone()
    }
}

fn walk_table_factor(
    factor: &TableFactor,
    cte_scope: &HashSet<String>,
    out: &mut Vec<(Option<String>, String)>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            // ObjectName parts may be Identifier or Function; only Identifier names a table.
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| p.as_ident().map(fold_identifier))
                .collect();
            match parts.as_slice() {
                // An unqualified name bound by an enclosing WITH is a CTE alias, not a catalog
                // relation; skip it. CTE names are never schema-qualified, so qualified forms
                // always refer to real relations.
                [name] if cte_scope.contains(name) => {}
                [name] => out.push((None, name.clone())),
                [schema, name] => out.push((Some(schema.clone()), name.clone())),
                // `db.schema.table`: ignore the database part, use the rightmost two.
                [_db, schema, name] => out.push((Some(schema.clone()), name.clone())),
                _ => {}
            }
        }
        TableFactor::Derived { subquery, .. } => walk_query(subquery, cte_scope, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_factor(&table_with_joins.relation, cte_scope, out);
            for join in &table_with_joins.joins {
                walk_table_factor(&join.relation, cte_scope, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;

    fn parse_shallow(sql: &str) -> ShallowCacheQuery {
        // Parse via the workspace's sqlparser entry point for the same AST flavour the shallow
        // cache sees.
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let _ = Dialect::PostgreSQL;
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse");
        let sqlparser::ast::Statement::Query(query) = statements.into_iter().next().unwrap() else {
            panic!("expected query")
        };
        ShallowCacheQuery::from(*query)
    }

    #[test]
    fn resolves_single_table_against_default_public_schema() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "documents", 42);
        let q = parse_shallow("SELECT * FROM documents");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![42]
        );
    }

    #[test]
    fn resolves_schema_qualified_table() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("auth", "users", 7);
        let q = parse_shallow("SELECT * FROM auth.users");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![7]
        );
    }

    #[test]
    fn deduplicates_repeated_references_and_joins() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "a", 1);
        registry.set_relation_name("public", "b", 2);
        let q =
            parse_shallow("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN a a2 ON a2.id = b.a_id");
        let mut got = extract_referenced_relation_oids(&q, &registry, &[]).unwrap();
        got.sort();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn unknown_relation_fails_closed() {
        let registry = Arc::new(PolicyRegistry::new());
        let q = parse_shallow("SELECT * FROM unknown_table");
        let err = extract_referenced_relation_oids(&q, &registry, &[]).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].name, "unknown_table");
        assert!(err[0].schema.is_none());
    }

    #[test]
    fn all_unknown_relations_reported_for_partially_resolvable_query() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "known", 1);
        let q = parse_shallow(
            "SELECT * FROM known JOIN missing_a ON known.id = missing_a.id \
             JOIN missing_b ON missing_a.id = missing_b.id",
        );
        let err = extract_referenced_relation_oids(&q, &registry, &[]).unwrap_err();
        let names: Vec<&str> = err.iter().map(|u| u.name.as_str()).collect();
        assert_eq!(names, vec!["missing_a", "missing_b"]);
    }

    /// Each unknown name is reported once even across repeated references (e.g. self-joins).
    #[test]
    fn unknown_relations_are_deduplicated() {
        let registry = Arc::new(PolicyRegistry::new());
        let q = parse_shallow("SELECT * FROM ghost g1 JOIN ghost g2 ON g1.id = g2.id");
        let err = extract_referenced_relation_oids(&q, &registry, &[]).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].name, "ghost");
    }

    /// Unquoted identifiers fold to lowercase: `Customers` resolves to the `customers` entry.
    #[test]
    fn unquoted_identifier_folds_to_lowercase() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "customers", 42);
        let q = parse_shallow("SELECT * FROM Customers");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![42]
        );
    }

    /// Quoted identifiers preserve case: `"Customers"` resolves to `Customers`, not `customers`.
    #[test]
    fn quoted_identifier_preserves_case() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "Customers", 42);
        registry.set_relation_name("public", "customers", 43);
        let q = parse_shallow("SELECT * FROM \"Customers\"");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![42]
        );
    }

    /// Quoted and unquoted forms address distinct relations when both exist.
    #[test]
    fn quoted_and_unquoted_do_not_alias() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "Customers", 42);
        registry.set_relation_name("public", "customers", 43);
        let q1 = parse_shallow("SELECT * FROM \"Customers\"");
        let q2 = parse_shallow("SELECT * FROM customers");
        assert_eq!(
            extract_referenced_relation_oids(&q1, &registry, &[]).unwrap(),
            vec![42]
        );
        assert_eq!(
            extract_referenced_relation_oids(&q2, &registry, &[]).unwrap(),
            vec![43]
        );
    }

    /// A CTE name is a query-local alias: the real table inside the CTE body resolves, and the CTE
    /// name itself is not reported as unknown.
    #[test]
    fn cte_name_is_not_treated_as_a_relation() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("api", "todos", 99);
        let q = parse_shallow("WITH src AS (SELECT * FROM api.todos) SELECT * FROM src");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![99]
        );
    }

    /// The canonical PostgREST read shape: a quoted `pgrst_source` CTE over a real table, selected
    /// through a derived-table wrapper. Only the inner table resolves.
    #[test]
    fn postgrest_pgrst_source_wrapper_resolves_inner_table_only() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("api", "todos", 99);
        let q = parse_shallow(
            "WITH \"pgrst_source\" AS (SELECT \"api\".\"todos\".* FROM \"api\".\"todos\") \
             SELECT count(*) AS page_total \
             FROM (SELECT * FROM \"pgrst_source\") AS \"_postgrest_t\"",
        );
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![99]
        );
    }

    /// A later non-recursive sibling CTE is not visible to an earlier CTE's body, so an earlier
    /// reference to that name binds to the real table and must still resolve.
    #[test]
    fn earlier_cte_does_not_see_later_sibling_name() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "b", 5);
        // `a`'s body references `b` before the `b` CTE is declared, so it binds to the table `b`.
        let q = parse_shallow("WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![5]
        );
    }

    /// Schema qualifier and table name fold independently: `"Auth".users` keeps `Auth`'s case while
    /// unquoted `users` folds.
    #[test]
    fn schema_and_name_fold_independently() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("Auth", "users", 7);
        let q = parse_shallow("SELECT * FROM \"Auth\".users");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![7]
        );
    }

    /// An unqualified name present in two schemas resolves to the first schema on the search path,
    /// not a hard-coded `public`. With `search_path = private, public`, `documents` must resolve
    /// `private.documents`, never the same-named `public.documents` decoy.
    #[test]
    fn unqualified_resolves_first_schema_on_search_path() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("private", "documents", 1);
        registry.set_relation_name("public", "documents", 2);
        let q = parse_shallow("SELECT * FROM documents");
        let search_path: Vec<SqlIdentifier> = vec!["private".into(), "public".into()];
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &search_path).unwrap(),
            vec![1],
        );
    }

    /// A schema earlier on the path that lacks the relation is skipped; the
    /// next schema that has it wins.
    #[test]
    fn unqualified_falls_through_to_next_schema_on_path() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "documents", 2);
        let q = parse_shallow("SELECT * FROM documents");
        // `private` has no `documents`; resolution falls through to `public`.
        let search_path: Vec<SqlIdentifier> = vec!["private".into(), "public".into()];
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &search_path).unwrap(),
            vec![2],
        );
    }

    /// An empty search path falls back to `public`, so callers without a resolved search path are
    /// unaffected.
    #[test]
    fn empty_search_path_defaults_to_public() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("public", "documents", 2);
        let q = parse_shallow("SELECT * FROM documents");
        assert_eq!(
            extract_referenced_relation_oids(&q, &registry, &[]).unwrap(),
            vec![2],
        );
    }

    /// A relation that exists only outside the search path is unresolvable (fail closed), matching
    /// Postgres, which would not find it either.
    #[test]
    fn relation_outside_search_path_fails_closed() {
        let registry = Arc::new(PolicyRegistry::new());
        registry.set_relation_name("private", "documents", 1);
        let q = parse_shallow("SELECT * FROM documents");
        // Search path is `public` only; `private.documents` is not visible.
        let search_path: Vec<SqlIdentifier> = vec!["public".into()];
        let err = extract_referenced_relation_oids(&q, &registry, &search_path).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].name, "documents");
        assert!(err[0].schema.is_none());
    }
}
