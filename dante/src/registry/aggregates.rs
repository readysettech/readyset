//! Aggregate patterns: count, sum, avg, min/max, count_distinct, group_by,
//! and dialect-specific aggregates (group_concat, array_agg).

use readyset_sql::ast::{BinaryOperator, OrderType};

use crate::constraint::{AggregateFn, DialectSupport, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT COUNT(t.c) FROM t
pub fn count() -> Pattern {
    let mut b = PatternBuilder::new("count");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT SUM(t.c) FROM t (c must be numeric)
pub fn sum() -> Pattern {
    let mut b = PatternBuilder::new("sum");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c, t);
    b.tags(&["aggregate", "numeric"]);
    b.build()
}

/// SELECT AVG(t.c) FROM t (c must be numeric)
pub fn avg() -> Pattern {
    let mut b = PatternBuilder::new("avg");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Avg { distinct: false }, c, t);
    b.tags(&["aggregate", "numeric"]);
    b.build()
}

/// SELECT MIN(t.c) FROM t
pub fn min_max() -> Pattern {
    let mut b = PatternBuilder::new("min_max");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Orderable);
    b.from(t);
    b.project_aggregate(AggregateFn::Min, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT COUNT(DISTINCT t.c) FROM t
pub fn count_distinct() -> Pattern {
    let mut b = PatternBuilder::new("count_distinct");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: true }, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT t.c1, COUNT(t.c2) FROM t GROUP BY t.c1
pub fn aggregate_with_group_by() -> Pattern {
    let mut b = PatternBuilder::new("aggregate_with_group_by");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.group_by(c_group, t);
    b.tags(&["aggregate", "group_by"]);
    b.build()
}

/// SELECT t.c1, COUNT(t.c2) FROM t GROUP BY t.c1 HAVING COUNT(t.c2) > ?
pub fn having_clause() -> Pattern {
    let mut b = PatternBuilder::new("having_clause");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.group_by(c_group, t);
    b.having(
        AggregateFn::Count { distinct: false },
        c_agg,
        t,
        BinaryOperator::Greater,
    );
    b.tags(&["aggregate", "group_by", "having"]);
    b.build()
}

/// SELECT GROUP_CONCAT(t.c) FROM t (MySQL only)
pub fn group_concat() -> Pattern {
    let mut b = PatternBuilder::new("group_concat");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::GroupConcat, c, t);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "group_concat"]);
    b.build()
}

/// SELECT t.c0, GROUP_CONCAT(t.c1) FROM t GROUP BY t.c0 ORDER BY t.c0 ASC
/// (MySQL only)
///
/// Deterministic-output variant of [`group_concat`]: GROUP BY on the
/// auto-allocated PK (`c0` is `Unique`) means each group has exactly one
/// source row, so the order of values within each `GROUP_CONCAT` string is
/// trivially fixed. ORDER BY on the same PK then fixes the outer row
/// order. The underlying MIR planner still emits `MirNodeInner::Accumulator`
/// for the `GROUP_CONCAT`, so the `Create dataflow node::Accumulator`
/// Antithesis assertion fires on MySQL just like `array_agg` does on
/// PostgreSQL.
pub fn group_concat_grouped() -> Pattern {
    let mut b = PatternBuilder::new("group_concat_grouped");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::GroupConcat, c_agg, t);
    b.group_by(c_group, t);
    b.order_by(c_group, t, OrderType::OrderAscending, None);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "group_concat", "accumulator"]);
    b.build()
}

/// SELECT t.c3, COUNT(t.c1), SUM(t.c2) FROM t GROUP BY t.c3
///
/// Multiple aggregates in one query trigger JoinAggregates dataflow nodes.
pub fn multi_aggregate() -> Pattern {
    let mut b = PatternBuilder::new("multi_aggregate");
    let t = b.table();
    let c_count = b.column(t);
    let c_sum = b.column(t);
    let c_group = b.column(t);
    b.column_type_class(c_sum, TypeClass::Numeric);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_count, t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c_sum, t);
    b.group_by(c_group, t);
    b.tags(&["aggregate", "group_by", "multi_aggregate"]);
    b.build()
}

/// SELECT COUNT(t.c1), SUM(t.c2) FROM t
///
/// Multiple aggregates with no GROUP BY and no WHERE parameters. The absence
/// of both group-by columns and parameter columns leaves JoinAggregates with
/// an empty group_by, targeting the
/// "JoinAggregates with empty group_by (no GROUP BY or parameter columns)"
/// Antithesis assertion in mir_to_flow.rs.
///
/// A WHERE placeholder would be folded into group_cols by param_cols in
/// grouped.rs, so the pattern is fully unparameterized.
pub fn multi_aggregate_no_group_by() -> Pattern {
    let mut b = PatternBuilder::new("multi_aggregate_no_group_by");
    let t = b.table();
    let c_count = b.column(t);
    let c_sum = b.column(t);
    b.column_type_class(c_sum, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_count, t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c_sum, t);
    b.tags(&["aggregate", "multi_aggregate"]);
    b.build()
}

/// SELECT SUM(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Combining an aggregate with WHERE IN triggers post-lookup aggregation
/// in Readyset, where partial results from multiple point lookups are
/// re-aggregated after the cache read.
pub fn in_list_aggregate() -> Pattern {
    let mut b = PatternBuilder::new("in_list_aggregate");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_agg, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT ARRAY_AGG(t.c) FROM t (PostgreSQL only)
pub fn array_agg() -> Pattern {
    let mut b = PatternBuilder::new("array_agg");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::ArrayAgg, c, t);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT MAX(t.c) FROM t
///
/// `min_max` currently emits only `Min`; without a sibling for `Max` the
/// `Aggregation::Max` dataflow-node assertion in Readyset never fires, and
/// no path exists for `Post-lookup aggregate::Max` either. Pair this with
/// `max_in_list` to cover both the aggregation and post-lookup paths.
pub fn max() -> Pattern {
    let mut b = PatternBuilder::new("max");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Orderable);
    b.from(t);
    b.project_aggregate(AggregateFn::Max, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT MAX(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Combines `Max` with `WHERE IN` so the planner emits a post-lookup
/// `Max` aggregator after the multi-key cache read.
pub fn max_in_list() -> Pattern {
    let mut b = PatternBuilder::new("max_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.column_type_class(c_agg, TypeClass::Orderable);
    b.project_aggregate(AggregateFn::Max, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT MIN(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Sibling of [`max_in_list`] for the `Min` post-lookup path. `min_max`
/// already drives the bare-aggregation `Min` node; this variant
/// specifically targets the post-lookup re-aggregation step.
pub fn min_in_list() -> Pattern {
    let mut b = PatternBuilder::new("min_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.column_type_class(c_agg, TypeClass::Orderable);
    b.project_aggregate(AggregateFn::Min, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT AVG(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Post-lookup average. `c1` is numeric to match `AVG`'s domain; `c2` is
/// the IN-list filter.
pub fn avg_in_list() -> Pattern {
    let mut b = PatternBuilder::new("avg_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_agg, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Avg { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup", "numeric"]);
    b.build()
}

/// SELECT t.c0, JSON_OBJECT_AGG(t.c1) FROM t GROUP BY t.c0 ORDER BY t.c0 ASC
/// (MySQL only)
///
/// Deterministic-output variant of json_object_agg: GROUP BY on the
/// auto-allocated PK (`c0` is `Unique`) means each group has exactly one
/// source row, so the JSON object produced per group is a single key-value
/// pair with a fixed key. ORDER BY on the same PK then fixes the outer row
/// order. The underlying MIR planner emits `MirNodeInner::Accumulator` for
/// the `JSON_OBJECT_AGG`, so the `Accumulation: JsonObjectAgg` Antithesis
/// assertion fires on MySQL.
pub fn json_object_agg_grouped() -> Pattern {
    let mut b = PatternBuilder::new("json_object_agg_grouped");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::JsonObjectAgg, c_agg, t);
    b.group_by(c_group, t);
    b.order_by(c_group, t, OrderType::OrderAscending, None);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "json_object_agg", "accumulator"]);
    b.build()
}

/// SELECT JSON_OBJECT_AGG(t.c1) FROM t WHERE t.c2 IN (?, ?, ?) (MySQL only)
///
/// Combines `JsonObjectAgg` with `WHERE IN` so the planner emits a
/// post-lookup `JsonObjectAgg` re-aggregation after the multi-key cache
/// read, firing the `Post-lookup aggregate: JsonObjectAgg` assertion.
/// The filter column is promoted to a unique key to bound the match, and the
/// aggregated column is too: it serves as both key and value, so distinct
/// values keep the JSON keys distinct -- a duplicate key would collapse
/// nondeterministically and diverge from the upstream DB.
pub fn json_object_agg_in_list() -> Pattern {
    let mut b = PatternBuilder::new("json_object_agg_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_unique_not_null(c_agg);
    b.column_unique_not_null(c_filter);
    b.from(t);
    b.project_aggregate(AggregateFn::JsonObjectAgg, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "filter", "post_lookup", "json_object_agg"]);
    b.build()
}

/// SELECT t.c0, STRING_AGG(t.c1, ',') FROM t GROUP BY t.c0 ORDER BY t.c0 ASC
/// (PostgreSQL only)
///
/// PostgreSQL analogue of [`group_concat_grouped`]: GROUP BY on the
/// auto-allocated PK (`c0` is `Unique`) means each group has exactly one
/// source row, so the order of values within each `STRING_AGG` string is
/// trivially fixed. ORDER BY on the same PK fixes the outer row order.
/// The MIR planner emits `MirNodeInner::Accumulator` for `STRING_AGG`,
/// so the `Accumulation::StringAgg` Antithesis assertion fires.
pub fn string_agg_grouped() -> Pattern {
    let mut b = PatternBuilder::new("string_agg_grouped");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::StringAgg, c_agg, t);
    b.group_by(c_group, t);
    b.order_by(c_group, t, OrderType::OrderAscending, None);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate", "group_concat", "accumulator"]);
    b.build()
}

/// SELECT STRING_AGG(t.c0, ',') FROM t WHERE t.c1 IN ($1)
/// (PostgreSQL only)
///
/// `STRING_AGG` with `WHERE IN` drives the post-lookup re-aggregation path
/// for `Post-lookup aggregate: StringAgg`. The filter column is promoted to a
/// unique key (`column_unique_not_null`) and the single `IN` value matches at
/// most one row, so `STRING_AGG` sees at most one element. Without that bound
/// the concatenation order is implementation-defined and diverges between
/// Readyset and the upstream DB, producing spurious oracle mismatches.
pub fn string_agg_in_list() -> Pattern {
    let mut b = PatternBuilder::new("string_agg_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_unique_not_null(c_filter);
    b.from(t);
    b.project_aggregate(AggregateFn::StringAgg, c_agg, t);
    b.where_in_param(c_filter, t, 1);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate", "filter", "post_lookup", "group_concat"]);
    b.build()
}

/// SELECT COUNT(t.c1) FROM t ORDER BY COUNT(t.c1) DESC LIMIT 10
///
/// Exercises the `normalize_topk_with_aggregate` pass: when an aggregate
/// appears in the SELECT list with no GROUP BY, the query always returns
/// exactly one row. The pass removes the ORDER BY and LIMIT as no-ops.
/// Determinism: COUNT over a table returns one row regardless of order.
pub fn aggregate_no_group_by_with_topk() -> Pattern {
    let mut b = PatternBuilder::new("aggregate_no_group_by_with_topk");
    let t = b.table();
    let c_agg = b.column(t);
    b.column_type_class(c_agg, TypeClass::Integer);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.order_by(c_agg, t, OrderType::OrderDescending, None);
    b.limit(10, None);
    b.tags(&["aggregate", "topk", "normalize_topk"]);
    b.build()
}

/// SELECT GROUP_CONCAT(t.c0) FROM t WHERE t.c1 IN (?) (MySQL only)
///
/// Bare `GROUP_CONCAT` with `WHERE IN` drives the post-lookup
/// re-aggregation path for `Post-lookup aggregate: GroupConcat`. The filter
/// column is promoted to a unique key (`column_unique_not_null`) and the
/// single `IN` value matches at most one row, so `GROUP_CONCAT` sees at most
/// one element. Without that bound the concatenation order is
/// implementation-defined and diverges between Readyset and the upstream DB,
/// producing spurious oracle mismatches.
pub fn group_concat_in_list() -> Pattern {
    let mut b = PatternBuilder::new("group_concat_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_unique_not_null(c_filter);
    b.from(t);
    b.project_aggregate(AggregateFn::GroupConcat, c_agg, t);
    b.where_in_param(c_filter, t, 1);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "filter", "post_lookup", "group_concat"]);
    b.build()
}

/// SELECT COUNT(*) FROM t WHERE t.c0 IN (?, ?, ?)
///
/// `COUNT(*)` with `WHERE IN` drives the post-lookup re-aggregation path
/// for `Post-lookup aggregate::CountStar`. Readyset's grouped.rs has a
/// dedicated `CountStar` arm separate from `Count(col)`, so this pattern
/// is required to reach that coverage point. Dialect: both MySQL and
/// PostgreSQL.
pub fn count_star_in_list() -> Pattern {
    let mut b = PatternBuilder::new("count_star_in_list");
    let t = b.table();
    let c_filter = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::CountStar, c_filter, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT ARRAY_AGG(t.c0) FROM t WHERE t.c1 IN ($1)
/// (PostgreSQL only)
///
/// `ARRAY_AGG` with `WHERE IN` drives the post-lookup re-aggregation path
/// for `Post-lookup aggregate::ArrayAgg`. The aggregated column is
/// constrained to Integer or String to avoid producing TIMESTAMP_ARRAY,
/// which the oracle's PG decode allowlist does not include. The filter
/// column (`c1`) is promoted to a unique key and the single `IN` value
/// matches at most one row, so `ARRAY_AGG` sees at most one element. Without
/// that bound the array element order is implementation-defined and diverges
/// between Readyset and the upstream DB, producing spurious oracle mismatches.
pub fn array_agg_in_list() -> Pattern {
    let mut b = PatternBuilder::new("array_agg_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_agg, TypeClass::Integer);
    b.column_unique_not_null(c_filter);
    b.from(t);
    b.project_aggregate(AggregateFn::ArrayAgg, c_agg, t);
    b.where_in_param(c_filter, t, 1);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT array_to_string(ARRAY_AGG(t.c), ',') FROM t WHERE t.k IN ($1)
/// (PostgreSQL only)
///
/// Exercises the scalar-over-aggregate composition path: ARRAY_AGG is the
/// grouped node; array_to_string is applied as a post-aggregate projection.
/// The aggregated column is constrained to a non-array scalar type (String)
/// so the resulting element type is unambiguous. `k` is promoted to a unique
/// key and the single IN value matches at most one row, so ARRAY_AGG sees at
/// most one element. Without that bound the array element order is
/// implementation-defined and diverges between Readyset and the upstream DB,
/// producing spurious oracle mismatches.
pub fn array_to_string_agg() -> Pattern {
    let mut b = PatternBuilder::new("array_to_string_agg");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::String);
    let c_key = b.column(t);
    b.column_unique_not_null(c_key);
    b.from(t);
    b.project_array_to_string_agg(c, t);
    b.where_in_param(c_key, t, 1);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate", "string"]);
    b.build()
}

/// SELECT COUNT(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Mirror of [`avg_in_list`] for `Count`. We already drive
/// `Post-lookup aggregate::Count` indirectly through the `Sum` path of
/// `in_list_aggregate`, but a dedicated COUNT-only variant keeps the
/// path covered if other patterns shift.
pub fn count_in_list() -> Pattern {
    let mut b = PatternBuilder::new("count_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::{Constraint, DialectSupport};
    use crate::test_util::resolve_pattern;

    /// The WHERE-IN filter column of an order-sensitive post-lookup aggregate
    /// must be promoted to a unique key, so each IN value matches at most one
    /// row and the concatenated/array output is deterministic for the oracle.
    #[track_caller]
    fn assert_where_in_filter_is_unique(p: &crate::pattern::Pattern) {
        let where_in_col = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::WhereInParam { col, .. } => Some(*col),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{} has no WHERE IN filter", p.name));
        assert!(
            p.constraints.iter().any(
                |c| matches!(c, Constraint::ColumnUniqueNotNull { col } if *col == where_in_col)
            ),
            "WHERE-IN filter column of {} must be ColumnUniqueNotNull for determinism",
            p.name,
        );
    }

    #[test]
    fn order_sensitive_in_list_aggregates_have_unique_filters() {
        for p in [
            string_agg_in_list(),
            json_object_agg_in_list(),
            group_concat_in_list(),
            array_agg_in_list(),
            array_to_string_agg(),
        ] {
            assert_where_in_filter_is_unique(&p);
        }
    }

    /// Concat/array post-lookup aggregates have no inner ORDER BY, so the order
    /// of elements in their output is implementation-defined and diverges
    /// between Readyset and the upstream DB. A single IN value bounds the match
    /// to one row, so the aggregate sees at most one element and the output is
    /// trivially deterministic. (JSON_OBJECT_AGG is exempt: the oracle compares
    /// objects by sorted key, so several distinct keys stay deterministic.)
    #[track_caller]
    fn assert_single_in_value(p: &crate::pattern::Pattern) {
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 1, .. })),
            "{} must use a single IN value for deterministic element order",
            p.name,
        );
    }

    #[test]
    fn concat_and_array_in_list_aggregates_use_single_in_value() {
        for p in [
            string_agg_in_list(),
            group_concat_in_list(),
            array_agg_in_list(),
            array_to_string_agg(),
        ] {
            assert_single_in_value(&p);
        }
    }

    #[test]
    fn json_object_agg_in_list_constrains_distinct_keys() {
        // JSON_OBJECT_AGG uses the aggregated column as both key and value, and
        // a duplicate key collapses nondeterministically. The key column must
        // generate distinct values so the aggregated object is deterministic.
        let p = json_object_agg_in_list();
        let agg_col = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::ProjectAggregate {
                    function: AggregateFn::JsonObjectAgg,
                    col,
                    ..
                } => Some(*col),
                _ => None,
            })
            .expect("json_object_agg_in_list must project a JsonObjectAgg");
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ColumnUniqueNotNull { col } if *col == agg_col)),
            "json_object_agg_in_list must mark its aggregated key column unique for distinct keys",
        );
    }

    #[test]
    fn min_max_patterns_constrain_operand_orderable() {
        use crate::constraint::{Constraint, TypeClass};
        for p in [min_max(), max(), min_in_list(), max_in_list()] {
            assert!(
                p.constraints.iter().any(|c| matches!(
                    c,
                    Constraint::ColumnTypeClass {
                        type_class: TypeClass::Orderable,
                        ..
                    }
                )),
                "pattern `{}` must constrain its MIN/MAX operand to Orderable so PG \
                 never sees min/max(boolean)",
                p.name
            );
        }
    }

    #[test]
    fn count_builds() {
        let p = count();
        assert_eq!(p.name, "count");
        assert!(p.tags.contains(&"aggregate"));
        assert_eq!(p.num_vars(), 2); // table + column
    }

    #[test]
    fn sum_builds() {
        let p = sum();
        assert_eq!(p.name, "sum");
        assert!(p.tags.contains(&"numeric"));

        // Should have a ColumnTypeClass(Numeric) constraint
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ColumnTypeClass {
                type_class: TypeClass::Numeric,
                ..
            }
        )));
    }

    #[test]
    fn avg_builds() {
        let p = avg();
        assert_eq!(p.name, "avg");
        assert!(p.tags.contains(&"numeric"));
    }

    #[test]
    fn min_max_builds() {
        let p = min_max();
        assert_eq!(p.name, "min_max");
        assert!(p.tags.contains(&"aggregate"));
    }

    #[test]
    fn count_distinct_builds() {
        let p = count_distinct();
        assert_eq!(p.name, "count_distinct");
    }

    #[test]
    fn aggregate_with_group_by_builds() {
        let p = aggregate_with_group_by();
        assert_eq!(p.name, "aggregate_with_group_by");
        assert!(p.tags.contains(&"group_by"));

        // Should have a GroupBy constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
    }

    #[test]
    fn having_clause_builds() {
        let p = having_clause();
        assert_eq!(p.name, "having_clause");
        assert!(p.tags.contains(&"having"));
    }

    #[test]
    fn count_resolves() {
        let p = count();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "sql: {sql}"
        );
    }

    #[test]
    fn sum_resolves() {
        let p = sum();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("SUM(") || sql.contains("sum("), "sql: {sql}");
    }

    #[test]
    fn aggregate_with_group_by_resolves() {
        let p = aggregate_with_group_by();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("GROUP BY"), "sql: {sql}");
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "sql: {sql}"
        );
    }

    #[test]
    fn group_concat_builds() {
        let p = group_concat();
        assert_eq!(p.name, "group_concat");
        assert_eq!(p.dialect_support, DialectSupport::MySqlOnly);
    }

    #[test]
    fn group_concat_resolves() {
        let p = group_concat();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("GROUP_CONCAT("),
            "expected GROUP_CONCAT in sql: {sql}"
        );
    }

    #[test]
    fn array_agg_builds() {
        let p = array_agg();
        assert_eq!(p.name, "array_agg");
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }

    #[test]
    fn array_agg_resolves() {
        let p = array_agg();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("ARRAY_AGG("),
            "expected ARRAY_AGG in sql: {sql}"
        );
    }

    #[test]
    fn multi_aggregate_builds() {
        let p = multi_aggregate();
        assert_eq!(p.name, "multi_aggregate");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"group_by"));

        // Should have exactly 2 ProjectAggregate constraints
        let agg_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ProjectAggregate { .. }))
            .count();
        assert_eq!(agg_count, 2, "expected 2 ProjectAggregate constraints");

        // Should have a GroupBy constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
    }

    #[test]
    fn multi_aggregate_resolves() {
        let p = multi_aggregate();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
    }

    #[test]
    fn in_list_aggregate_builds() {
        let p = in_list_aggregate();
        assert_eq!(p.name, "in_list_aggregate");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"filter"));

        // Should have a ProjectAggregate constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ProjectAggregate { .. }))
        );

        // Should have a WhereInParam constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 3, .. }))
        );
    }

    #[test]
    fn in_list_aggregate_resolves() {
        let p = in_list_aggregate();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn json_object_agg_grouped_builds() {
        let p = json_object_agg_grouped();
        assert_eq!(p.name, "json_object_agg_grouped");
        assert!(p.tags.contains(&"accumulator"));
        assert_eq!(p.dialect_support, DialectSupport::MySqlOnly);
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::JsonObjectAgg,
                ..
            }
        )));
    }

    #[test]
    fn json_object_agg_grouped_resolves() {
        let p = json_object_agg_grouped();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // MySQL renders FunctionExpr::JsonObjectAgg as `json_objectagg`
        assert!(
            sql.contains("json_objectagg("),
            "expected json_objectagg in sql: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
    }

    // A GROUP BY query must NOT get the table primary key appended to ORDER BY as a tiebreaker:
    // the PK is not a grouped column, so MySQL rejects the query under only_full_group_by
    // (ERROR 1055), the upstream rejects it, and the cache (hence the Accumulation assertion)
    // is never created. The grouped column already establishes a total order over the
    // one-row-per-group result, so no tiebreaker is needed. The seeded PK is c0.
    #[test]
    fn grouped_order_by_omits_pk_tiebreaker() {
        for p in [group_concat_grouped(), json_object_agg_grouped()] {
            let sql = resolve_pattern(&p, Dialect::MySQL);
            let order = sql.split_once("ORDER BY").expect("has ORDER BY").1;
            assert!(
                !order.contains("c0"),
                "GROUP BY query must not append the PK (c0) to ORDER BY: {sql}"
            );
        }
    }

    #[test]
    fn json_object_agg_in_list_builds() {
        let p = json_object_agg_in_list();
        assert_eq!(p.name, "json_object_agg_in_list");
        assert!(p.tags.contains(&"post_lookup"));
        assert_eq!(p.dialect_support, DialectSupport::MySqlOnly);
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::JsonObjectAgg,
                ..
            }
        )));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 3, .. }))
        );
    }

    #[test]
    fn json_object_agg_in_list_resolves() {
        let p = json_object_agg_in_list();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // MySQL renders FunctionExpr::JsonObjectAgg as `json_objectagg`
        assert!(
            sql.contains("json_objectagg("),
            "expected json_objectagg in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn group_concat_in_list_builds() {
        let p = group_concat_in_list();
        assert_eq!(p.name, "group_concat_in_list");
        assert!(p.tags.contains(&"post_lookup"));
        assert_eq!(p.dialect_support, DialectSupport::MySqlOnly);
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::GroupConcat,
                ..
            }
        )));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 1, .. }))
        );
    }

    #[test]
    fn group_concat_in_list_resolves() {
        let p = group_concat_in_list();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("GROUP_CONCAT("),
            "expected GROUP_CONCAT in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn string_agg_grouped_builds() {
        let p = string_agg_grouped();
        assert_eq!(p.name, "string_agg_grouped");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"group_concat"));
        assert!(p.tags.contains(&"accumulator"));
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::StringAgg,
                ..
            }
        )));
    }

    #[test]
    fn string_agg_grouped_resolves() {
        let p = string_agg_grouped();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("string_agg("),
            "expected string_agg in sql: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
        assert!(sql.contains("','"), "expected separator ',' in sql: {sql}");
    }

    #[test]
    fn string_agg_in_list_builds() {
        let p = string_agg_in_list();
        assert_eq!(p.name, "string_agg_in_list");
        assert!(p.tags.contains(&"post_lookup"));
        assert!(p.tags.contains(&"group_concat"));
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::StringAgg,
                ..
            }
        )));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 1, .. }))
        );
    }

    #[test]
    fn string_agg_in_list_resolves() {
        let p = string_agg_in_list();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("string_agg("),
            "expected string_agg in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
        assert!(sql.contains("','"), "expected separator ',' in sql: {sql}");
    }

    #[test]
    fn string_agg_grouped_is_postgres_only() {
        let p = string_agg_grouped();
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }

    #[test]
    fn string_agg_in_list_is_postgres_only() {
        let p = string_agg_in_list();
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }

    #[test]
    fn count_star_in_list_builds() {
        let p = count_star_in_list();
        assert_eq!(p.name, "count_star_in_list");
        assert!(p.tags.contains(&"post_lookup"));
        assert_eq!(p.dialect_support, DialectSupport::Both);
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::CountStar,
                ..
            }
        )));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 3, .. }))
        );
    }

    #[test]
    fn count_star_in_list_resolves_mysql() {
        let p = count_star_in_list();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("count(*)") || sql.contains("COUNT(*)"),
            "expected COUNT(*) in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn count_star_in_list_resolves_pg() {
        let p = count_star_in_list();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("count(*)") || sql.contains("COUNT(*)"),
            "expected COUNT(*) in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn array_agg_in_list_builds() {
        let p = array_agg_in_list();
        assert_eq!(p.name, "array_agg_in_list");
        assert!(p.tags.contains(&"post_lookup"));
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ProjectAggregate {
                function: AggregateFn::ArrayAgg,
                ..
            }
        )));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 1, .. }))
        );
    }

    #[test]
    fn array_agg_in_list_resolves() {
        let p = array_agg_in_list();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("ARRAY_AGG(") || sql.contains("array_agg("),
            "expected ARRAY_AGG in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }

    #[test]
    fn array_agg_in_list_is_postgres_only() {
        let p = array_agg_in_list();
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }

    #[test]
    fn array_agg_in_list_constrains_non_timestamp_column() {
        use crate::constraint::TypeClass;
        let p = array_agg_in_list();
        // The aggregated column (c_agg) must be constrained to a non-timestamp
        // type class so PG's TIMESTAMP_ARRAY is never produced.
        let has_type_constraint = p.constraints.iter().any(|c| {
            matches!(
                c,
                Constraint::ColumnTypeClass {
                    type_class: TypeClass::Integer | TypeClass::String,
                    ..
                }
            )
        });
        assert!(
            has_type_constraint,
            "array_agg_in_list must constrain aggregated column to non-timestamp type"
        );
    }

    #[test]
    fn multi_aggregate_no_group_by_builds() {
        let p = multi_aggregate_no_group_by();
        assert_eq!(p.name, "multi_aggregate_no_group_by");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"multi_aggregate"));

        // Exactly 2 ProjectAggregate constraints
        let agg_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ProjectAggregate { .. }))
            .count();
        assert_eq!(agg_count, 2, "expected 2 ProjectAggregate constraints");

        // No GroupBy constraint — this pattern targets the empty group_by path
        assert!(
            !p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. })),
            "multi_aggregate_no_group_by must not have GroupBy"
        );

        // No parameter constraints — a WHERE placeholder would fill group_by
        // via param_cols and prevent the JoinAggregates empty-group_by path
        assert!(
            !p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereParam { .. })),
            "multi_aggregate_no_group_by must not have WHERE parameters"
        );

        // SUM column must be numeric
        assert!(
            p.constraints.iter().any(|c| matches!(
                c,
                Constraint::ColumnTypeClass {
                    type_class: TypeClass::Numeric,
                    ..
                }
            )),
            "multi_aggregate_no_group_by must constrain SUM column to Numeric"
        );
    }

    #[test]
    fn multi_aggregate_no_group_by_resolves_mysql() {
        let p = multi_aggregate_no_group_by();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "must not contain GROUP BY: {sql}"
        );
        assert!(!sql.contains("WHERE"), "must not contain WHERE: {sql}");
    }

    #[test]
    fn multi_aggregate_no_group_by_resolves_pg() {
        let p = multi_aggregate_no_group_by();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "must not contain GROUP BY: {sql}"
        );
        assert!(!sql.contains("WHERE"), "must not contain WHERE: {sql}");
    }

    #[test]
    fn aggregate_no_group_by_with_topk_builds() {
        let p = aggregate_no_group_by_with_topk();
        assert_eq!(p.name, "aggregate_no_group_by_with_topk");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"topk"));
        assert!(p.tags.contains(&"normalize_topk"));

        // Must have a ProjectAggregate constraint (COUNT)
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ProjectAggregate { .. })),
            "expected ProjectAggregate constraint"
        );

        // Must have an OrderBy constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::OrderBy { .. })),
            "expected OrderBy constraint"
        );

        // Must have a Limit constraint
        assert!(
            p.constraints.iter().any(|c| matches!(
                c,
                Constraint::Limit {
                    limit: 10,
                    offset: None
                }
            )),
            "expected Limit {{ limit: 10, offset: None }} constraint"
        );

        // Must NOT have a GroupBy constraint — the pass fires only without GROUP BY
        assert!(
            !p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. })),
            "aggregate_no_group_by_with_topk must not have GroupBy"
        );
    }

    #[test]
    fn aggregate_no_group_by_with_topk_resolves() {
        let p = aggregate_no_group_by_with_topk();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(sql.contains("ORDER BY"), "expected ORDER BY in sql: {sql}");
        assert!(sql.contains("LIMIT"), "expected LIMIT in sql: {sql}");
        assert!(
            !sql.contains("GROUP BY"),
            "must not contain GROUP BY: {sql}"
        );
    }

    #[test]
    fn array_to_string_agg_builds() {
        let p = array_to_string_agg();
        assert_eq!(p.name, "array_to_string_agg");
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"string"));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ProjectArrayToStringAgg { .. }))
        );
    }

    #[test]
    fn array_to_string_agg_resolves() {
        let p = array_to_string_agg();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("array_to_string(") || sql.contains("ARRAY_TO_STRING("),
            "expected array_to_string in sql: {sql}"
        );
        assert!(
            sql.contains("ARRAY_AGG(") || sql.contains("array_agg("),
            "expected ARRAY_AGG in sql: {sql}"
        );
        assert!(
            sql.contains("','"),
            "expected comma delimiter in sql: {sql}"
        );
    }

    #[test]
    fn array_to_string_agg_is_postgres_only() {
        let p = array_to_string_agg();
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }
}
