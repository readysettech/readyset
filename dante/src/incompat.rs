//! Incompatibility detection: tag-based rules and custom predicates.
//!
//! The incompatibility system prevents conflicting patterns from being
//! combined in the same query.

use crate::constraint::Constraint;

/// A rule that declares when constraint sets are incompatible.
#[derive(Debug, Clone)]
pub struct IncompatibilityRule {
    /// Stable identifier for the rule. Use this to look the rule up in
    /// tests instead of relying on its position in `default_rules()`.
    pub name: &'static str,
    /// The condition that triggers incompatibility.
    pub condition: IncompatCondition,
    /// Human-readable reason for the incompatibility.
    pub reason: &'static str,
}

/// What condition makes a constraint set incompatible.
#[derive(Debug, Clone)]
pub enum IncompatCondition {
    /// Two tags cannot coexist in the same recipe.
    TagConflict(&'static str, &'static str),
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

impl IncompatibilityRule {
    /// Check if the given constraint set violates this rule.
    /// Returns true if incompatible (i.e., the set should be rejected).
    pub fn is_violated(&self, constraints: &[Constraint], tags: &[&str]) -> bool {
        match &self.condition {
            IncompatCondition::TagConflict(a, b) => tags.contains(a) && tags.contains(b),
            IncompatCondition::MaxOccurrences(tag, max) => {
                let count = tags.iter().filter(|t| *t == tag).count();
                count > *max
            }
            IncompatCondition::Custom(pred) => pred(constraints),
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
        match c {
            Constraint::Subquery {
                constraints: inner, ..
            }
            | Constraint::Cte {
                constraints: inner, ..
            } if (inner.iter().any(pred) || any_nested_contains(inner, pred)) => {
                return true;
            }
            Constraint::Join {
                right:
                    crate::constraint::JoinRight::Subquery {
                        constraints: inner, ..
                    },
                ..
            }
            | Constraint::Join {
                right:
                    crate::constraint::JoinRight::Cte {
                        constraints: inner, ..
                    },
                ..
            } if (inner.iter().any(pred) || any_nested_contains(inner, pred)) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Check if any top-level constraint is a Subquery/CTE whose inner
/// constraints match the predicate. One level of nesting only.
pub fn has_subquery_containing(
    constraints: &[Constraint],
    pred: fn(&[Constraint]) -> bool,
) -> bool {
    for c in constraints {
        match c {
            Constraint::Subquery {
                constraints: inner, ..
            }
            | Constraint::Cte {
                constraints: inner, ..
            } if pred(inner) => {
                return true;
            }
            Constraint::Join {
                right:
                    crate::constraint::JoinRight::Subquery {
                        constraints: inner, ..
                    },
                ..
            }
            | Constraint::Join {
                right:
                    crate::constraint::JoinRight::Cte {
                        constraints: inner, ..
                    },
                ..
            } if pred(inner) => {
                return true;
            }
            _ => {}
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
pub fn default_rules() -> Vec<IncompatibilityRule> {
    vec![
        IncompatibilityRule {
            name: "distinct_with_bare_aggregate",
            condition: IncompatCondition::TagConflict("distinct", "aggregate_without_group_by"),
            reason: "DISTINCT with bare aggregate (no GROUP BY) is semantically dubious",
        },
        IncompatibilityRule {
            name: "max_subquery_nesting",
            condition: IncompatCondition::MaxOccurrences("subquery", 2),
            reason: "limit subquery nesting depth to 2",
        },
        IncompatibilityRule {
            name: "window_in_subquery",
            condition: IncompatCondition::Custom(|constraints| {
                has_subquery_containing(constraints, |inner| {
                    has_constraint(inner, |c| matches!(c, Constraint::WindowFunction { .. }))
                })
            }),
            reason: "window functions not supported inside subqueries",
        },
    ]
}

/// Check a constraint set against all provided rules.
/// Returns the first violated rule's reason, or None if all pass.
pub fn check_rules(
    rules: &[IncompatibilityRule],
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
    use crate::constraint::{SubqueryPosition, WindowFn};

    #[test]
    fn tag_conflict_rejects_conflicting_tags() {
        let rule = IncompatibilityRule {
            name: "test_tag_conflict",
            condition: IncompatCondition::TagConflict("foo", "bar"),
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
        let rule = IncompatibilityRule {
            name: "test_max_occurrences",
            condition: IncompatCondition::MaxOccurrences("subquery", 2),
            reason: "too many subqueries",
        };

        assert!(!rule.is_violated(&[], &["subquery"]));
        assert!(!rule.is_violated(&[], &["subquery", "subquery"]));
        assert!(rule.is_violated(&[], &["subquery", "subquery", "subquery"]));
        assert!(!rule.is_violated(&[], &[]));
    }

    #[test]
    fn custom_predicate_inspects_constraints() {
        let rule = IncompatibilityRule {
            name: "test_custom",
            condition: IncompatCondition::Custom(|constraints| {
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
        let constraints = vec![Constraint::Subquery {
            position: SubqueryPosition::ExistsUncorrelated,
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
        let constraints = vec![Constraint::Subquery {
            position: SubqueryPosition::ExistsUncorrelated,
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
}
