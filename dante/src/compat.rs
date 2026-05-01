//! Compatibility rules: tag-based rules and custom predicates.
//!
//! The compatibility system prevents conflicting patterns from being
//! combined in the same query.

use crate::constraint::Constraint;
use crate::var::VarId;

/// A rule that declares when constraint sets are incompatible.
#[derive(Debug, Clone)]
pub struct CompatibilityRule {
    /// Stable identifier for the rule. Use this to look the rule up in
    /// tests instead of relying on its position in `default_rules()`.
    pub name: &'static str,
    /// The condition that triggers incompatibility.
    pub condition: CompatCondition,
    /// Human-readable reason for the incompatibility.
    pub reason: &'static str,
}

/// What condition makes a constraint set incompatible.
#[derive(Debug, Clone)]
pub enum CompatCondition {
    /// Two tags cannot coexist in the same recipe.
    TagConflict(&'static str, &'static str),
    /// A specific tag must not be present.
    TagPresent(&'static str),
    /// A tag can appear at most N times in a recipe.
    MaxOccurrences(&'static str, usize),
    /// Custom predicate over the full constraint set.
    /// Returns true if the constraint set is INCOMPATIBLE (should be rejected).
    Custom(fn(&[Constraint]) -> bool),
}

/// Filter for selecting patterns from the registry.
#[derive(Debug, Clone, Default)]
pub struct SelectionFilter {
    /// Tags that must be present on the pattern.
    pub required_tags: Vec<&'static str>,
    /// Tags that must NOT be present on the pattern.
    pub excluded_tags: Vec<&'static str>,
    /// Maximum allowed `min_depth` for a pattern. `None` means unlimited.
    pub max_depth: Option<usize>,
}

impl SelectionFilter {
    /// Check if a pattern passes this filter.
    pub fn matches(&self, tags: &[&'static str], min_depth: usize) -> bool {
        for req in &self.required_tags {
            if !tags.contains(req) {
                return false;
            }
        }
        for exc in &self.excluded_tags {
            if tags.contains(exc) {
                return false;
            }
        }
        if let Some(max) = self.max_depth
            && min_depth > max
        {
            return false;
        }
        true
    }
}

impl CompatibilityRule {
    /// Check if the given constraint set violates this rule.
    /// Returns true if incompatible (i.e., the set should be rejected).
    pub fn is_violated(&self, constraints: &[Constraint], tags: &[&str]) -> bool {
        match &self.condition {
            CompatCondition::TagConflict(a, b) => tags.contains(a) && tags.contains(b),
            CompatCondition::TagPresent(tag) => tags.contains(tag),
            CompatCondition::MaxOccurrences(tag, max) => {
                let count = tags.iter().filter(|t| *t == tag).count();
                count > *max
            }
            CompatCondition::Custom(pred) => pred(constraints),
        }
    }
}

/// Check if any constraint in the set (non-recursively) matches a predicate.
pub fn has_constraint(constraints: &[Constraint], pred: fn(&Constraint) -> bool) -> bool {
    constraints.iter().any(pred)
}

/// Check if any subquery or CTE nested within these constraints
/// contains a constraint matching the predicate. Recurses into all nested scopes.
pub fn any_nested_contains(constraints: &[Constraint], pred: fn(&Constraint) -> bool) -> bool {
    for c in constraints {
        if let Constraint::SubqueryExpr {
            constraints: inner, ..
        }
        | Constraint::SubqueryRelation {
            constraints: inner, ..
        } = c
            && (inner.iter().any(pred) || any_nested_contains(inner, pred))
        {
            return true;
        }
    }
    false
}

/// Check if any top-level constraint is a SubqueryExpr/SubqueryRelation whose
/// inner constraints match the predicate. One level of nesting only.
pub fn has_subquery_containing(
    constraints: &[Constraint],
    pred: fn(&[Constraint]) -> bool,
) -> bool {
    for c in constraints {
        if let Constraint::SubqueryExpr {
            constraints: inner, ..
        }
        | Constraint::SubqueryRelation {
            constraints: inner, ..
        } = c
            && pred(inner)
        {
            return true;
        }
    }
    false
}

/// Count occurrences of constraints matching a predicate.
pub fn count_constraints(constraints: &[Constraint], pred: fn(&Constraint) -> bool) -> usize {
    constraints.iter().filter(|c| pred(c)).count()
}

// --- Default incompatibility rules ---

/// Returns the default set of incompatibility rules.
pub fn default_rules() -> Vec<CompatibilityRule> {
    vec![
        CompatibilityRule {
            name: "distinct_with_bare_aggregate",
            condition: CompatCondition::TagConflict("distinct", "aggregate_without_group_by"),
            reason: "DISTINCT with bare aggregate (no GROUP BY) is semantically dubious",
        },
        CompatibilityRule {
            name: "max_subquery_nesting",
            condition: CompatCondition::MaxOccurrences("subquery", 2),
            reason: "limit subquery nesting depth to 2",
        },
        CompatibilityRule {
            name: "window_in_subquery",
            condition: CompatCondition::Custom(|constraints| {
                has_subquery_containing(constraints, |inner| {
                    has_constraint(inner, |c| matches!(c, Constraint::WindowFunction { .. }))
                })
            }),
            reason: "window functions not supported inside subqueries",
        },
        // Composition safety rules
        CompatibilityRule {
            name: "multiple_limits",
            condition: CompatCondition::Custom(|constraints| {
                count_constraints(constraints, |c| matches!(c, Constraint::Limit { .. })) > 1
            }),
            reason: "multiple LIMIT clauses",
        },
        CompatibilityRule {
            name: "aggregate_with_ungrouped_projection",
            condition: CompatCondition::Custom(|constraints| {
                // Reject if there are aggregate projections AND non-grouped
                // plain column projections. This catches composed queries like
                // SELECT COUNT(c1), c2 FROM t (without GROUP BY c2) which
                // are non-deterministic in MySQL and errors in Postgres.
                let has_aggregate = has_constraint(constraints, |c| {
                    matches!(c, Constraint::ProjectAggregate { .. })
                });
                if !has_aggregate {
                    return false;
                }
                // Collect grouped column VarIds
                let grouped: std::collections::HashSet<VarId> = constraints
                    .iter()
                    .filter_map(|c| match c {
                        Constraint::GroupBy { col, .. } => Some(*col),
                        _ => None,
                    })
                    .collect();
                // Check if any non-aggregate projection references ungrouped columns
                constraints.iter().any(|c| match c {
                    Constraint::ProjectColumn { col, .. } => !grouped.contains(col),
                    Constraint::ProjectFunction { args, .. } => {
                        args.iter().any(|(col, _)| !grouped.contains(col))
                    }
                    _ => false,
                })
            }),
            reason: "aggregate with non-grouped projected column",
        },
        CompatibilityRule {
            name: "max_cte_nesting",
            condition: CompatCondition::MaxOccurrences("cte", 2),
            reason: "limit CTE nesting depth to 2",
        },
        // MySQL error 3065: ORDER BY columns must be in the SELECT list
        // when DISTINCT is used.
        CompatibilityRule {
            name: "distinct_with_unprojected_order_by",
            condition: CompatCondition::Custom(|constraints| {
                let has_distinct =
                    has_constraint(constraints, |c| matches!(c, Constraint::Distinct));
                if !has_distinct {
                    return false;
                }
                // Collect all projected column VarIds
                let projected: std::collections::HashSet<VarId> = constraints
                    .iter()
                    .filter_map(|c| match c {
                        Constraint::ProjectColumn { col, .. } => Some(*col),
                        _ => None,
                    })
                    .collect();
                // Check if any ORDER BY column is not projected
                constraints.iter().any(|c| match c {
                    Constraint::OrderBy { col, .. } => !projected.contains(col),
                    _ => false,
                })
            }),
            reason: "DISTINCT with ORDER BY on unprojected column (MySQL error 3065)",
        },
    ]
}

/// Returns incompatibility rules for Readyset deep-cache compatibility.
///
/// These rules reject query patterns that Readyset cannot deep-cache (i.e.,
/// queries that would fall back to shallow caching or upstream proxying).
/// Enable these when the goal is to maximize dataflow/reader-map coverage.
///
/// Known unsupported features for deep caching:
/// - Self-joins with same-column conditions (ENG-411)
/// - CROSS JOIN (no join condition for dataflow)
/// - DISTINCT combined with parameterized IN lists
/// - Window functions inside subqueries (already in default_rules)
pub fn readyset_compat_rules() -> Vec<CompatibilityRule> {
    vec![
        // Self-joins trigger problematic self-join detection (ENG-411).
        CompatibilityRule {
            name: "readyset_no_self_join",
            condition: CompatCondition::TagPresent("self_join"),
            reason: "Readyset: self-join not supported for deep caching",
        },
        // CROSS JOIN has no join condition, can't be represented in dataflow.
        CompatibilityRule {
            name: "readyset_no_cross_join",
            condition: CompatCondition::TagPresent("cross_join"),
            reason: "Readyset: cross join not supported for deep caching",
        },
        // DISTINCT + parameterized IN list triggers adapter rewrite error.
        CompatibilityRule {
            name: "readyset_no_distinct_in_param",
            condition: CompatCondition::Custom(|constraints| {
                let has_distinct =
                    has_constraint(constraints, |c| matches!(c, Constraint::Distinct));
                let has_in_param = has_constraint(constraints, |c| {
                    matches!(c, Constraint::WhereInParam { .. })
                });
                has_distinct && has_in_param
            }),
            reason: "Readyset: DISTINCT with parameterized IN list not supported",
        },
    ]
}

/// Check a constraint set against all provided rules.
/// Returns the first violated rule's reason, or None if all pass.
pub fn check_rules(
    rules: &[CompatibilityRule],
    constraints: &[Constraint],
    tags: &[&str],
) -> Option<&'static str> {
    for rule in rules {
        if rule.is_violated(constraints, tags) {
            return Some(rule.reason);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraint::{SubqueryExprKind, WindowFn};

    #[test]
    fn tag_conflict_rejects_conflicting_tags() {
        let rule = CompatibilityRule {
            name: "test_tag_conflict",
            condition: CompatCondition::TagConflict("foo", "bar"),
            reason: "test conflict",
        };

        assert!(rule.is_violated(&[], &["foo", "bar"]));
        assert!(rule.is_violated(&[], &["baz", "foo", "bar"]));
        assert!(!rule.is_violated(&[], &["foo"]));
        assert!(!rule.is_violated(&[], &["bar"]));
        assert!(!rule.is_violated(&[], &[]));
    }

    #[test]
    fn max_occurrences_limits_tag_count() {
        let rule = CompatibilityRule {
            name: "test_max_occurrences",
            condition: CompatCondition::MaxOccurrences("subquery", 2),
            reason: "too many subqueries",
        };

        assert!(!rule.is_violated(&[], &["subquery"]));
        assert!(!rule.is_violated(&[], &["subquery", "subquery"]));
        assert!(rule.is_violated(&[], &["subquery", "subquery", "subquery"]));
        assert!(!rule.is_violated(&[], &[]));
    }

    #[test]
    fn custom_predicate_inspects_constraints() {
        let rule = CompatibilityRule {
            name: "test_custom",
            condition: CompatCondition::Custom(|constraints| {
                has_constraint(constraints, |c| matches!(c, Constraint::Distinct))
            }),
            reason: "no distinct",
        };

        assert!(rule.is_violated(&[Constraint::Distinct], &[]));
        assert!(!rule.is_violated(
            &[Constraint::Limit {
                limit: 10,
                offset: None
            }],
            &[]
        ));
    }

    #[test]
    fn selection_filter_required_tags() {
        let filter = SelectionFilter {
            required_tags: vec!["aggregate"],
            ..Default::default()
        };

        assert!(filter.matches(&["aggregate", "group_by"], 0));
        assert!(filter.matches(&["aggregate"], 0));
        assert!(!filter.matches(&["filter"], 0));
        assert!(!filter.matches(&[], 0));
    }

    #[test]
    fn selection_filter_excluded_tags() {
        let filter = SelectionFilter {
            excluded_tags: vec!["join"],
            ..Default::default()
        };

        assert!(filter.matches(&["filter"], 0));
        assert!(filter.matches(&[], 0));
        assert!(!filter.matches(&["join"], 0));
        assert!(!filter.matches(&["filter", "join"], 0));
    }

    #[test]
    fn selection_filter_combined() {
        let filter = SelectionFilter {
            required_tags: vec!["filter"],
            excluded_tags: vec!["join"],
            ..Default::default()
        };

        assert!(filter.matches(&["filter"], 0));
        assert!(filter.matches(&["filter", "string"], 0));
        assert!(!filter.matches(&["filter", "join"], 0));
        assert!(!filter.matches(&["join"], 0));
        assert!(!filter.matches(&[], 0));
    }

    #[test]
    fn selection_filter_max_depth() {
        let filter = SelectionFilter {
            max_depth: Some(1),
            ..Default::default()
        };

        assert!(filter.matches(&[], 0));
        assert!(filter.matches(&[], 1));
        assert!(!filter.matches(&[], 2));
    }

    #[test]
    fn has_constraint_finds_match() {
        let constraints = vec![
            Constraint::Distinct,
            Constraint::Limit {
                limit: 10,
                offset: None,
            },
        ];

        assert!(has_constraint(&constraints, |c| matches!(
            c,
            Constraint::Distinct
        )));
        assert!(has_constraint(&constraints, |c| matches!(
            c,
            Constraint::Limit { .. }
        )));
        assert!(!has_constraint(&constraints, |c| matches!(
            c,
            Constraint::BaseTable(_)
        )));
    }

    #[test]
    fn any_nested_contains_finds_in_subquery() {
        let constraints = vec![Constraint::SubqueryExpr {
            kind: SubqueryExprKind::ExistsUncorrelated,
            constraints: vec![Constraint::Distinct],
            shared_vars: vec![],
        }];

        assert!(any_nested_contains(&constraints, |c| matches!(
            c,
            Constraint::Distinct
        )));
        // Top-level doesn't have Distinct directly
        assert!(!has_constraint(&constraints, |c| matches!(
            c,
            Constraint::Distinct
        )));
    }

    #[test]
    fn cross_scope_window_in_subquery_detected() {
        let constraints = vec![Constraint::SubqueryExpr {
            kind: SubqueryExprKind::ExistsUncorrelated,
            constraints: vec![Constraint::WindowFunction {
                function: WindowFn::RowNumber,
                partition_col: None,
                order_col: None,
                order_type: None,
            }],
            shared_vars: vec![],
        }];

        let rules = default_rules();
        let rule = rules
            .iter()
            .find(|r| r.name == "window_in_subquery")
            .expect("window_in_subquery rule should be registered");
        assert!(rule.is_violated(&constraints, &[]));
    }

    #[test]
    fn window_function_at_top_level_allowed() {
        let constraints = vec![Constraint::WindowFunction {
            function: WindowFn::RowNumber,
            partition_col: None,
            order_col: None,
            order_type: None,
        }];

        let rules = default_rules();
        let rule = rules
            .iter()
            .find(|r| r.name == "window_in_subquery")
            .expect("window_in_subquery rule should be registered");
        assert!(!rule.is_violated(&constraints, &[]));
    }

    #[test]
    fn check_rules_returns_first_violation() {
        let rules = default_rules();

        // DISTINCT + aggregate_without_group_by should trigger TagConflict
        let result = check_rules(&rules, &[], &["distinct", "aggregate_without_group_by"]);
        assert!(result.is_some());
        assert!(result.expect("should have violation").contains("DISTINCT"));
    }

    #[test]
    fn check_rules_passes_clean_set() {
        let rules = default_rules();
        let result = check_rules(&rules, &[Constraint::Distinct], &["filter", "ordering"]);
        assert!(result.is_none());
    }

    #[test]
    fn distinct_with_unprojected_order_by_rejected() {
        use readyset_sql::ast::OrderType;

        let rules = default_rules();

        // ORDER BY col (VarId(2)) is NOT in the projection — should be rejected
        let constraints = vec![
            Constraint::ProjectColumn {
                col: VarId(1),
                table: VarId(0),
            },
            Constraint::OrderBy {
                col: VarId(2),
                table: VarId(0),
                direction: OrderType::OrderDescending,
                null_order: None,
            },
            Constraint::Distinct,
        ];
        let result = check_rules(&rules, &constraints, &["distinct", "ordering"]);
        assert!(
            result.is_some(),
            "DISTINCT + ORDER BY with unprojected column should be rejected"
        );
    }

    #[test]
    fn distinct_with_projected_order_by_allowed() {
        use readyset_sql::ast::OrderType;

        let rules = default_rules();

        // ORDER BY col (VarId(1)) IS in the projection — should be allowed
        let constraints = vec![
            Constraint::ProjectColumn {
                col: VarId(1),
                table: VarId(0),
            },
            Constraint::OrderBy {
                col: VarId(1),
                table: VarId(0),
                direction: OrderType::OrderDescending,
                null_order: None,
            },
            Constraint::Distinct,
        ];
        let result = check_rules(&rules, &constraints, &["distinct", "ordering"]);
        assert!(
            result.is_none(),
            "DISTINCT + ORDER BY with projected column should be allowed"
        );
    }

    #[test]
    fn count_constraints_works() {
        let constraints = vec![
            Constraint::Distinct,
            Constraint::Distinct,
            Constraint::Limit {
                limit: 10,
                offset: None,
            },
        ];

        assert_eq!(
            count_constraints(&constraints, |c| matches!(c, Constraint::Distinct)),
            2
        );
        assert_eq!(
            count_constraints(&constraints, |c| matches!(c, Constraint::Limit { .. })),
            1
        );
        assert_eq!(
            count_constraints(&constraints, |c| matches!(c, Constraint::BaseTable(_))),
            0
        );
    }

    // --- Readyset compatibility rules ---

    #[test]
    fn readyset_compat_rejects_self_join() {
        let rules = readyset_compat_rules();
        // self_join tag should be rejected
        let result = check_rules(&rules, &[], &["join", "self_join"]);
        assert!(result.is_some());
        assert!(
            result.expect("should have violation").contains("self-join"),
            "reason should mention self-join"
        );
    }

    #[test]
    fn readyset_compat_allows_regular_join() {
        let rules = readyset_compat_rules();
        let result = check_rules(&rules, &[], &["join", "two_table"]);
        assert!(result.is_none(), "regular joins should be allowed");
    }

    #[test]
    fn readyset_compat_rejects_cross_join() {
        let rules = readyset_compat_rules();
        let result = check_rules(&rules, &[], &["join", "cross_join"]);
        assert!(result.is_some());
        assert!(
            result
                .expect("should have violation")
                .contains("cross join"),
            "reason should mention cross join"
        );
    }

    #[test]
    fn readyset_compat_rejects_distinct_with_in_param() {
        use crate::constraint::Constraint;
        let rules = readyset_compat_rules();
        let constraints = vec![
            Constraint::Distinct,
            Constraint::WhereInParam {
                col: VarId(1),
                table: VarId(0),
                num_values: 3,
            },
        ];
        let result = check_rules(&rules, &constraints, &["distinct"]);
        assert!(result.is_some());
        assert!(
            result.expect("should have violation").contains("DISTINCT"),
            "reason should mention DISTINCT"
        );
    }

    #[test]
    fn readyset_compat_allows_distinct_without_in_param() {
        let rules = readyset_compat_rules();
        let constraints = vec![Constraint::Distinct];
        let result = check_rules(&rules, &constraints, &["distinct"]);
        assert!(result.is_none(), "DISTINCT alone should be fine");
    }

    #[test]
    fn readyset_compat_combined_with_default_rules() {
        // When both default + readyset rules are active, both should apply
        let mut rules = default_rules();
        rules.extend(readyset_compat_rules());

        // Default rule: DISTINCT + aggregate_without_group_by
        let result = check_rules(&rules, &[], &["distinct", "aggregate_without_group_by"]);
        assert!(result.is_some());

        // Readyset rule: self_join
        let result = check_rules(&rules, &[], &["self_join"]);
        assert!(result.is_some());

        // Neither: basic filter
        let result = check_rules(&rules, &[], &["filter"]);
        assert!(result.is_none());
    }

    #[test]
    fn readyset_compat_allows_subquery() {
        let rules = readyset_compat_rules();
        let result = check_rules(&rules, &[], &["subquery"]);
        assert!(
            result.is_none(),
            "subqueries should be allowed in readyset compat mode"
        );
    }

    #[test]
    fn readyset_compat_allows_group_concat() {
        let rules = readyset_compat_rules();
        let result = check_rules(&rules, &[], &["aggregate", "group_concat"]);
        assert!(
            result.is_none(),
            "GROUP_CONCAT should be allowed now that truncation is implemented"
        );
    }
}
