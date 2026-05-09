//! CTE patterns: simple_cte, cte_with_join, cte_with_param.

use readyset_sql::ast::BinaryOperator;

use crate::constraint::Constraint;
use crate::pattern::{Pattern, PatternBuilder};

/// WITH cte0 AS (SELECT t.c FROM t) SELECT cte0.c FROM cte0
pub fn simple_cte() -> Pattern {
    let mut b = PatternBuilder::new("simple_cte");

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c, t_inner);
    let cte_alias = sq.commit_as_cte();

    // Outer query references the CTE alias, not the underlying base table.
    // The column var `c` keeps its name from the inner scope, but is
    // qualified by the CTE alias name on the outer side.
    b.from(cte_alias);
    b.project_column(c, cte_alias);

    b.tags(&["cte"]);
    b.build()
}

/// WITH cte0 AS (SELECT t0.c0 FROM t0) SELECT cte0.c0, t1.c1 FROM cte0 INNER JOIN t1 ON ...
pub fn cte_with_join() -> Pattern {
    let mut b = PatternBuilder::new("cte_with_join");

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_cte = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c_cte, t_inner);
    let cte_alias = sq.commit_as_cte();

    // Outer: join CTE with another table. The CTE body's inner table is
    // hidden behind the alias here, so we don't need a cross-scope NotEq
    // to keep them distinct — a self-join through `cte0` is semantically
    // fine even when the CTE happens to wrap the same base table.
    let t_join = b.table();
    let c_join = b.column(t_join);
    let c_join_key = b.column(t_join);

    b.from(cte_alias);
    b.project_column(c_cte, cte_alias);
    b.project_column(c_join, t_join);
    b.join_table(
        readyset_sql::ast::JoinOperator::InnerJoin,
        t_join,
        c_cte,
        c_join_key,
    );

    b.tags(&["cte", "join"]);
    b.build()
}

/// WITH cte0 AS (SELECT t.c0 FROM t WHERE t.c1 = ?) SELECT cte0.c0 FROM cte0
///
/// The CTE body contains a parameterized filter. When Readyset compiles this,
/// the CTE is compiled with `LeafBehavior::Anonymous`, producing a `ViewKey`
/// MIR node that is then pushed down to the outer query's `Leaf` by the
/// `pull_view_keys_to_leaf` rewrite pass. This exercises the ViewKey creation
/// code path that plain cached SELECTs (which use `LeafBehavior::Leaf`) never
/// reach.
pub fn cte_with_param() -> Pattern {
    let mut b = PatternBuilder::new("cte_with_param");

    let mut sq = b.subquery();
    let t = sq.table();
    let c_proj = sq.column(t);
    let c_filter = sq.column(t);
    sq.constraint(Constraint::NotEq(c_proj, c_filter));
    sq.from(t);
    sq.project_column(c_proj, t);
    sq.where_param(c_filter, t, BinaryOperator::Equal);
    let cte_alias = sq.commit_as_cte();

    // Outer query selects from the CTE alias.
    b.from(cte_alias);
    b.project_column(c_proj, cte_alias);

    b.tags(&["cte", "parameter"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn simple_cte_builds() {
        let p = simple_cte();
        assert_eq!(p.name, "simple_cte");
        assert!(p.tags.contains(&"cte"));
        assert!(p.min_depth >= 1);

        // Should have a Cte constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryRelation { .. }))
        );
    }

    #[test]
    fn cte_with_join_builds() {
        let p = cte_with_join();
        assert_eq!(p.name, "cte_with_join");
        assert!(p.tags.contains(&"cte"));
        assert!(p.tags.contains(&"join"));

        // Should have both Cte and Join constraints
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryRelation { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::Join { .. }))
        );
    }

    #[test]
    fn simple_cte_resolves() {
        let p = simple_cte();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
        // The outer query must reference the CTE alias, not the underlying
        // base table.
        assert!(
            sql.contains("FROM `cte0`"),
            "outer FROM should use CTE alias: {sql}"
        );
    }

    #[test]
    fn cte_with_join_resolves() {
        let p = cte_with_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("JOIN"), "sql: {sql}");
        // The outer FROM must reference the CTE alias `cte0`, not the
        // underlying inner table.
        assert!(
            sql.contains("FROM `cte0`"),
            "outer FROM should use CTE alias: {sql}"
        );
    }

    #[test]
    fn cte_with_param_builds() {
        let p = cte_with_param();
        assert_eq!(p.name, "cte_with_param");
        assert!(p.tags.contains(&"cte"));
        assert!(p.tags.contains(&"parameter"));

        // Should have a SubqueryRelation { kind: Cte } constraint
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::SubqueryRelation {
                kind: crate::constraint::SubqueryRelationKind::Cte,
                ..
            }
        )));
    }

    #[test]
    fn cte_with_param_resolves() {
        let p = cte_with_param();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("= ?"), "expected parameter in sql: {sql}");
    }

    /// Regression: when a CTE pattern is composed with a partner that
    /// introduces additional columns on the unified primary table, the
    /// partner's column references end up qualified by the CTE alias even
    /// though the CTE body never projects them. MySQL rejects the resulting
    /// SQL with `ERROR 42S22: Unknown column 'cteN.cX' in 'field list'`.
    ///
    /// Repro from the seed=42 / seed=43 / seed=256 dante-oracle soak runs
    /// (2026-04-30): the CTE body is `SELECT t.c0 FROM t` but outer refs
    /// like `cte0.c1`, `cte0.c2`, etc. show up because partner column
    /// resolution unifies the partner's primary with the CTE alias var,
    /// then `Cte` build re-binds the union-find rep to `cte0`,
    /// retroactively retargeting every partner column reference to it.
    ///
    /// Drives the full generator (registry + composition partners) for many
    /// seeds, mimicking the soak run that exposed the bug.
    #[test]
    fn generator_never_references_unprojected_cte_columns() {
        use rand::SeedableRng;
        use rand::rngs::SmallRng;
        use readyset_sql::DialectDisplay;
        use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

        use crate::entropy::Entropy;
        use crate::generator::Generator;
        use crate::state::GeneratorConfig;

        let config = GeneratorConfig {
            reuse_preference: 0.9,
            ..Default::default()
        };
        let mut g = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let mut bad_examples: Vec<String> = Vec::new();
        for _ in 0..200 {
            let Ok(out) = g.generate_with_ddl(&mut entropy) else {
                continue;
            };
            let sql = out.query.display(Dialect::MySQL).to_string();
            // Skip queries we can't parse — those are unrelated generator
            // bugs (e.g. `CROSS JOIN ... ON ...`) that this test doesn't
            // cover.
            let Ok(parsed) = parse_query_with_config(
                ParsingPreset::for_tests().into_config(),
                Dialect::MySQL,
                &sql,
            ) else {
                continue;
            };
            let select = match parsed {
                readyset_sql::ast::SqlQuery::Select(s) => s,
                _ => continue,
            };

            let mut cte_projections: std::collections::HashMap<
                String,
                std::collections::HashSet<String>,
            > = Default::default();
            for cte in &select.ctes {
                let mut cols = std::collections::HashSet::new();
                for f in &cte.statement.fields {
                    if let readyset_sql::ast::FieldDefinitionExpr::Expr {
                        expr: readyset_sql::ast::Expr::Column(c),
                        alias,
                    } = f
                    {
                        cols.insert(
                            alias
                                .as_ref()
                                .map(|a| a.to_string())
                                .unwrap_or_else(|| c.name.to_string()),
                        );
                    }
                }
                cte_projections.insert(cte.name.to_string(), cols);
            }
            if cte_projections.is_empty() {
                continue;
            }

            let mut bad: Vec<(String, String)> = Vec::new();
            let mut stack: Vec<&readyset_sql::ast::Expr> = Vec::new();
            for f in &select.fields {
                if let readyset_sql::ast::FieldDefinitionExpr::Expr { expr, .. } = f {
                    stack.push(expr);
                }
            }
            if let Some(w) = &select.where_clause {
                stack.push(w);
            }
            while let Some(expr) = stack.pop() {
                use readyset_sql::ast::Expr::*;
                match expr {
                    Column(c) => {
                        if let Some(table) = &c.table
                            && let Some(allowed) = cte_projections.get(&table.name.to_string())
                            && !allowed.contains(&c.name.to_string())
                        {
                            bad.push((table.name.to_string(), c.name.to_string()));
                        }
                    }
                    BinaryOp { lhs, rhs, .. } => {
                        stack.push(lhs);
                        stack.push(rhs);
                    }
                    UnaryOp { rhs, .. } => stack.push(rhs),
                    Call(call) => {
                        for arg in call.arguments() {
                            stack.push(arg);
                        }
                    }
                    _ => {}
                }
            }
            if !bad.is_empty() {
                bad_examples.push(format!("refs unprojected CTE cols {bad:?}\n{sql}"));
            }
        }

        assert!(
            bad_examples.is_empty(),
            "{} queries had invalid CTE refs. First few:\n\n{}",
            bad_examples.len(),
            bad_examples
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n\n----\n\n")
        );
    }
}
