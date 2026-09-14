use std::collections::HashSet;
use std::mem;

use readyset_data::DfValue;
use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{BinaryOperator, Expr, InValue, ItemPlaceholder, Literal, SelectStatement};
use readyset_sql::{Dialect, TryFromDialect};

use crate::rewrite_utils::{
    iter_and_conjuncts, predicate_caps_row_number, preserve_row_number_caps,
};

/// The key a read looks its cache up by: what it carries at each position the cache
/// parameterized, against the cache parameter that position fills.
/// See [`LiteralSlots::match_read`].
pub type CacheLookupKey = Vec<(usize, Literal)>;

/// Collect top-level WHERE conjuncts that cap a `ROW_NUMBER()` projection, returning a set
/// keyed by value-equality on `Expr`. Both orientations of each cap predicate are inserted
/// so the gate at the top of `visit_expr` also short-circuits the literal-on-left shape
/// that the swap arms produce by recursively revisiting the (now-swapped) expression.
/// Returns an empty set when `preserve_row_number_caps()` is false — the gate is then a
/// no-op and auto-parameterize handles cap predicates with its default literal-swap arms.
fn collect_top_level_caps(query: &SelectStatement) -> HashSet<Expr> {
    let mut set = HashSet::new();
    if !preserve_row_number_caps() {
        return set;
    }
    if let Some(where_clause) = query.where_clause.as_ref() {
        for conjunct in iter_and_conjuncts(where_clause) {
            if predicate_caps_row_number(conjunct, query).is_some() {
                set.insert(conjunct.clone());
                if let Some(flipped) = flip_binary_operands(conjunct) {
                    set.insert(flipped);
                }
            }
        }
    }
    set
}

/// For a `BinaryOp` whose operands the auto-parameterize swap arms would mutate
/// in-place, return the post-swap form. Used to pre-populate the cap-predicate set
/// with both orientations.
///
/// Equal: swap lhs/rhs (operator unchanged, equality is symmetric). Ordering
/// comparisons: swap lhs/rhs and flip the operator (e.g. `10 >= rn` -> `rn <= 10`).
/// Any other shape returns `None`.
fn flip_binary_operands(expr: &Expr) -> Option<Expr> {
    let Expr::BinaryOp { lhs, op, rhs } = expr else {
        return None;
    };
    let new_op = match op {
        BinaryOperator::Equal => *op,
        op if op.is_ordering_comparison() => op.flip_ordering_comparison().ok()?,
        _ => return None,
    };
    Some(Expr::BinaryOp {
        lhs: rhs.clone(),
        op: new_op,
        rhs: lhs.clone(),
    })
}

/// What the autoparameterization pass found at each canonical parameter position: the literal it
/// lifted (parameterized) there, or `None` where the query already held a placeholder.
///
/// A canonical parameter position is one [`auto_parameterize_query`] takes: a literal it lifts, or
/// a placeholder already there. They are numbered from zero in walk order, and the numbering runs
/// across both rewrite phases, since the second walks the query again.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct LiteralSlots {
    /// The literal at each canonical position, or `None` where the query already held a
    /// placeholder.
    slots: Box<[Option<Literal>]>,
    /// Where each `IN` list's positions start and how many it took, in walk order.
    /// `collapse_where_in` folds every list into a single predicate, so two queries whose lists
    /// hold the same literals reach the same shape; the runs are what tell them apart. The start
    /// matters as much as the length, since a run of the same length sitting at a different
    /// position is a different query.
    in_list_runs: Box<[(usize, usize)]>,
}

impl LiteralSlots {
    /// How many canonical positions the query has.
    pub fn positions(&self) -> usize {
        self.slots.len()
    }

    /// How many positions hold a literal. A cache holding more of them answers fewer reads, so
    /// this is how specific it is.
    pub fn inline_positions(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_some()).count()
    }

    /// Match a read against these slots, which are a cache's.
    ///
    /// A position where the cache holds a literal is one it kept inline, so the read has to carry
    /// the same literal there. A position holding `None` is one the cache parameterized, which any
    /// value satisfies -- including a placeholder whose value arrives with the client's parameters.
    ///
    /// `Some` carries the read's lookup key: what it holds at the positions the cache
    /// parameterized, each against the cache parameter it fills.
    ///
    /// `None` when the read belongs to another cache: a literal that differs, a different number
    /// of positions, or a placeholder where this cache baked a literal in.
    pub fn match_read(&self, read: &Self) -> Option<CacheLookupKey> {
        // Grouping first: the shape a read hashes to says nothing about how its positions
        // partition across `IN` lists, so two queries holding the same literals in different
        // groupings reach this point looking alike.
        if self.in_list_runs != read.in_list_runs || self.slots.len() != read.slots.len() {
            return None;
        }
        let mut lookup = Vec::new();
        // Which of the cache's own parameters the next parameterized position is.
        let mut parameter = 0;
        for (cached, found) in self.slots.iter().zip(read.slots.iter()) {
            match (cached, found) {
                // Kept inline: the read spells out the same literal, which says which cache over
                // this shape it belongs to rather than keying a lookup into it.
                (Some(cached), Some(found)) if cached == found => {}
                (Some(_), _) => return None,
                // Parameterized: whatever the read carries here keys the lookup. A placeholder
                // carries nothing, since its value arrives with the client's parameters, so it
                // leaves this parameter for them to fill.
                (None, found) => {
                    if let Some(found) = found {
                        lookup.push((parameter, found.clone()));
                    }
                    parameter += 1;
                }
            }
        }
        Some(lookup)
    }

    /// These slots with each literal converted to the value a client would bind there, which is
    /// what a prepared statement's execute matches with.
    pub fn to_values(&self, dialect: Dialect) -> ReadySetResult<ValueSlots> {
        let values = self
            .slots
            .iter()
            .map(|slot| {
                slot.as_ref()
                    .map(|lit| DfValue::try_from_dialect(lit, dialect))
                    .transpose()
            })
            .collect::<Result<Box<[Option<DfValue>]>, _>>()?;
        Ok(ValueSlots {
            values,
            in_list_runs: self.in_list_runs.clone(),
        })
    }
}

/// A cache's [`LiteralSlots`] as values, the form an execute's bound and spelled-out values take.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueSlots {
    values: Box<[Option<DfValue>]>,
    in_list_runs: Box<[(usize, usize)]>,
}

impl ValueSlots {
    /// Match an execute holding `values` at its canonical positions against these slots, which
    /// are a cache's; `read` is the statement's own slots, for its `IN` grouping. `Some` carries
    /// the values at the positions the cache parameterized, in its parameter order.
    pub fn match_values(&self, read: &LiteralSlots, values: &[DfValue]) -> Option<Vec<DfValue>> {
        if self.in_list_runs != read.in_list_runs || self.values.len() != values.len() {
            return None;
        }
        let mut parameters = Vec::new();
        for (cached, found) in self.values.iter().zip(values) {
            let Some(cached) = cached else {
                parameters.push(found.clone());
                continue;
            };
            let Ok(found) = found.coerce_for_comparison(&cached.infer_dataflow_type()) else {
                return None;
            };
            if *cached != found {
                return None;
            }
        }
        Some(parameters)
    }
}

/// Slots with no `IN` grouping recorded, which only a test can mean: a query's grouping comes
/// from the walk, and claiming none of it would let two different groupings match.
#[cfg(test)]
impl From<Vec<Option<Literal>>> for LiteralSlots {
    fn from(slots: Vec<Option<Literal>>) -> Self {
        Self {
            slots: slots.into_boxed_slice(),
            in_list_runs: Box::new([]),
        }
    }
}

/// What one run of [`auto_parameterize_query`] produced.
#[derive(Debug, Default)]
pub struct AutoParameters {
    /// `(position in the emitted form, literal)` for every literal the run lifted, following
    /// whatever parameters the caller passed in.
    pub params: Vec<(usize, Literal)>,
    /// What the walk found at each canonical position, carrying whatever an earlier phase found
    /// at the positions it passed again.
    pub slots: LiteralSlots,
}

#[derive(Default)]
struct AutoParameterizeVisitor {
    autoparameterize_equals: bool,
    autoparameterize_ranges: bool,
    out: Vec<(usize, Literal)>,
    in_supported_position: bool,
    /// What the walk found at each canonical position it has passed. Its length is both the
    /// number of positions taken and the index the next parameter takes, since every position
    /// the walk takes becomes a parameter.
    slots: Vec<Option<Literal>>,
    /// What an earlier phase found at the positions this walk passes again. A placeholder this
    /// walk reaches may be one that phase lifted from a literal, which the query alone no longer
    /// says.
    prev_slots: Vec<Option<Literal>>,
    /// Where each `IN` list's positions start and how many it took, in walk order.
    in_list_runs: Vec<(usize, usize)>,
    query_depth: u8,
    visit_limit_clause: bool,
    /// Top-level WHERE conjuncts (in both orientations) that cap a `ROW_NUMBER()`
    /// projection. The integer literal in such a predicate is the cardinality signal
    /// CBJR consumes downstream; replacing it with a placeholder would destroy the
    /// signal, so the gate at the top of `visit_expr` short-circuits the walk when
    /// the current expression appears in this set.
    cap_predicates: HashSet<Expr>,
}

/// Whether the literals a row is compared against, by `=` or in an `IN` list, can be
/// parameterized: each lifted literal keys the lookup on the column it faces, so every position of
/// the row has to be one.
fn is_row_of_columns(exprs: &[Expr]) -> bool {
    exprs.iter().all(|e| matches!(e, Expr::Column(_)))
}

/// Whether a row equality can be parameterized: its left is all columns and its right all
/// literals, so that every position becomes a column keyed by a placeholder.
fn is_parameterizable_row_equality(lhs: &[Expr], rhs: &[Expr]) -> bool {
    is_row_of_columns(lhs) && rhs.iter().all(|e| matches!(e, Expr::Literal(_)))
}

/// Replace a `Literal::Preserved(inner)` marker with its inner literal, in place. No-op for any
/// other literal.
fn unwrap_preserved(literal: &mut Literal) {
    if matches!(literal, Literal::Preserved(_))
        && let Literal::Preserved(inner) = mem::replace(literal, Literal::Null)
    {
        *literal = *inner;
    }
}

/// Sweep that unwraps every `Literal::Preserved` marker to its inner literal. Run once at the
/// end of the Readyset rewrite, after both autoparameterization phases have honored the markers,
/// so none survives into the stored or executed form.
pub(super) fn unwrap_all_preserved(query: &mut SelectStatement) {
    struct UnwrapPreservedVisitor;

    impl<'ast> VisitorMut<'ast> for UnwrapPreservedVisitor {
        type Error = std::convert::Infallible;

        fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
            unwrap_preserved(literal);
            Ok(())
        }
    }

    let Ok(()) = UnwrapPreservedVisitor.visit_select_statement(query);
}

impl AutoParameterizeVisitor {
    /// Account for a placeholder already in the query: it holds a canonical position, and a
    /// position in the form the pass emits. Call this exactly once per canonical position.
    ///
    /// Indexing `prev_slots` by the count so far holds because every position the second phase
    /// adds trails the ones the first phase took. The only positions it adds are the top-level
    /// limit clause's: the first phase does not walk a limit clause, the walk reaches that clause
    /// after the rest of the query, and `visit_limit_clause` keeps a subquery's out of the walk.
    fn count_placeholder(&mut self) {
        let lifted_earlier = self.prev_slots.get(self.slots.len()).cloned().flatten();
        self.slots.push(lifted_earlier);
    }

    fn replace_literal(&mut self, literal: &mut Literal) {
        // A literal marked by the exclusion pre-pass must not be parameterized. The marker stays
        // in place so the second autoparameterization phase preserves it too; the final sweep at
        // the end of the Readyset rewrite unwraps it. It holds no canonical position, which is
        // why a slot cannot express a scope's marks -- see `autoparam_exclusions`.
        if matches!(literal, Literal::Preserved(_)) {
            return;
        }
        let literal = mem::replace(literal, Literal::Placeholder(ItemPlaceholder::QuestionMark));
        self.out.push((self.slots.len(), literal.clone()));
        self.slots.push(Some(literal));
    }

    /// The walk proper. `visit_expr` wraps this to measure the positions an `IN` list takes.
    fn visit_expr_taking_positions(&mut self, expression: &mut Expr) -> ReadySetResult<()> {
        let was_supported = self.in_supported_position;
        // Preserve row-number cap literals: the integer bound is a cardinality signal
        // that CBJR reads from the AST. Skipping the walk here keeps the literal
        // intact rather than rewriting it to a placeholder.
        if was_supported && self.cap_predicates.contains(expression) {
            return Ok(());
        }
        if was_supported {
            match expression {
                Expr::BinaryOp { lhs, op, rhs } => match (lhs.as_mut(), op, rhs.as_mut()) {
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(Literal::Placeholder(_))) => {}
                    (Expr::Row { exprs: lhs_exprs, .. }, BinaryOperator::Equal, Expr::Row { exprs, .. })
                        if is_parameterizable_row_equality(lhs_exprs, exprs) =>
                    {
                        for expr in exprs {
                            if let Expr::Literal(lit) = expr {
                                match lit {
                                    // A placeholder holds a parameter position of its own, so
                                    // the positions after it are numbered past it.
                                    Literal::Placeholder(_) => self.count_placeholder(),
                                    _ if self.autoparameterize_equals => self.replace_literal(lit),
                                    _ => {}
                                }
                            }
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(Literal::Placeholder(_))) if op.is_ordering_comparison() => {}
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(lit)) => {
                        if self.autoparameterize_equals {
                            self.replace_literal(lit);
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(lit)) if op.is_ordering_comparison() => {
                        if self.autoparameterize_ranges {
                            self.replace_literal(lit);
                        }
                        return Ok(());
                    }
                    (Expr::Literal(_), BinaryOperator::Equal | BinaryOperator::NotEqual, Expr::Column(_)) => {
                        // for lit = col and lit != col, swap the equality first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Row { exprs: lhs_exprs, .. }, BinaryOperator::Equal, Expr::Row { exprs: rhs_exprs, .. })
                        if is_parameterizable_row_equality(rhs_exprs, lhs_exprs) =>
                    {
                        // for (lit, ..) = (col, ..), swap the rows first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Literal(_), op, Expr::Column(_)) if op.is_ordering_comparison() => {
                        // for lit <ordering op> col, swap operands and flip operator, then revisit
                        mem::swap(lhs, rhs);
                        // this shouldn't fail as we just did the `op.is_ordering_comparison()`
                        // check
                        *op = op.flip_ordering_comparison().unwrap();
                        return self.visit_expr(expression);
                    }
                    (lhs, BinaryOperator::And, rhs) => {
                        self.visit_expr(lhs)?;
                        self.in_supported_position = true;
                        self.visit_expr(rhs)?;
                        self.in_supported_position = true;
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::In {
                    lhs,
                    rhs: InValue::List(exprs),
                    negated: false,
                } => match lhs.as_ref() {
                    // Case 1: Single-column IN (a IN (1,2,3))
                    Expr::Column(_)
                        if exprs
                            .iter()
                            .all(|e| matches!( e, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)))) =>
                    {
                        if self.autoparameterize_equals {
                            for expr in exprs.iter_mut() {
                                if let Expr::Literal(lit) = expr {
                                    self.replace_literal(lit);
                                }
                            }
                        }
                        return Ok(());
                    }

                    // Case 2: Tuple IN ((a, b) IN ((1,2), (3,4)))
                    Expr::Row { exprs: lhs_exprs, .. }
                        if is_row_of_columns(lhs_exprs) && exprs.iter().all(|e| {
                            match e {
                                Expr::Row { exprs, .. } => exprs.iter().all(
                                    |e| matches!(e, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))),
                                ),
                                // FIXME(sqlparser): This is a special case because nom parses `(a, b) IN ((1,2))` as
                                // `(a, b) IN (1,2)` instead of `((a, b)) IN ((1,2))`.
                                // This case should be removed once migration to sqlparser is
                                // finalized.
                                // To fix this, we readd the removed parens before proceeding.
                                Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)) => true,
                                _ => false,
                            }
                        }) =>
                    {
                        if self.autoparameterize_equals {
                            // FIXME(sqlparser): this handles the special case mentioned in the comment
                            // just before this
                            if !exprs.is_empty() && matches!(exprs[0], Expr::Literal(_)) {
                                let _ = mem::replace(
                                    exprs,
                                    vec![Expr::Row {
                                        exprs: exprs.clone(),
                                        explicit: false,
                                    }],
                                );
                            };

                            for row in exprs.iter_mut() {
                                // The guard admits only rows, apart from the parse workaround
                                // just applied.
                                let Expr::Row { exprs, .. } = row else {
                                    unsupported!("Expected a ROW of literals");
                                };
                                for expr in exprs.iter_mut() {
                                    if let Expr::Literal(lit) = expr {
                                        self.replace_literal(lit);
                                    }
                                }
                            }
                        }
                        return Ok(());
                    }

                    _ => self.in_supported_position = false,
                },
                _ => self.in_supported_position = false,
            }
        }

        visit_mut::walk_expr(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }
}

impl<'ast> VisitorMut<'ast> for AutoParameterizeVisitor {
    type Error = ReadySetError;

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if matches!(literal, Literal::Placeholder(_)) {
            self.count_placeholder();
        }
        Ok(())
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.query_depth = self.query_depth.saturating_add(1);
        visit_mut::walk_select_statement(self, select_statement)?;
        self.query_depth = self.query_depth.saturating_sub(1);
        Ok(())
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        // We can only support parameters in the WHERE clause of the top-level query, not any
        // subqueries it contains.
        self.in_supported_position = self.query_depth <= 1;
        self.visit_expr(expression)?;
        self.in_supported_position = false;
        Ok(())
    }

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        // An `IN` list takes a contiguous run of canonical positions, and `collapse_where_in`
        // folds the whole list into one predicate later, so the run's length is the only record
        // that the grouping was there. Take it here rather than where literals are lifted, since
        // a list the author wrote as placeholders is grouped the same way and lifts nothing.
        if matches!(
            expression,
            Expr::In {
                rhs: InValue::List(_),
                ..
            }
        ) {
            let before = self.slots.len();
            let walked = self.visit_expr_taking_positions(expression);
            if self.slots.len() > before {
                self.in_list_runs.push((before, self.slots.len() - before));
            }
            return walked;
        }
        self.visit_expr_taking_positions(expression)
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(offset, Literal::Placeholder(_))
            && self.autoparameterize_equals
            && self.query_depth <= 1
        {
            // `replace_literal` has accounted for the position, so the walk stops here rather
            // than reaching it again.
            self.replace_literal(offset);
            return Ok(());
        }

        visit_mut::walk_offset(self, offset)
    }

    fn visit_limit_clause(
        &mut self,
        limit_clause: &'ast mut readyset_sql::ast::LimitClause,
    ) -> Result<(), Self::Error> {
        if self.visit_limit_clause && self.query_depth <= 1 {
            visit_mut::walk_limit_clause(self, limit_clause)
        } else {
            Ok(())
        }
    }
}

/// Walks through the query to determine whether the query has equals comparisons, range
/// comparisons, equals placeholders, and range placeholders in positions that support
/// autoparameterization.
#[derive(Default)]
struct AnalyzeLiteralsVisitor {
    contains_equal: bool,
    contains_range: bool,
    contains_equal_placeholder: bool,
    contains_range_placeholder: bool,
    query_depth: u8,
    in_supported_position: bool,
    has_aggregates: bool,
    /// Same gate as `AutoParameterizeVisitor::cap_predicates`: skipping cap predicates
    /// here prevents the mode-decision in `auto_parameterize_query` from biasing toward
    /// range-mode when the only range comparison in the query is an RN cap.
    cap_predicates: HashSet<Expr>,
}

impl<'ast> VisitorMut<'ast> for AnalyzeLiteralsVisitor {
    type Error = std::convert::Infallible;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.query_depth = self.query_depth.saturating_add(1);
        visit_mut::walk_select_statement(self, select_statement)?;
        self.query_depth = self.query_depth.saturating_sub(1);
        Ok(())
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        // We can only support parameters in the WHERE clause of the top-level query, not any
        // subqueries it contains.
        self.in_supported_position = self.query_depth <= 1;
        self.visit_expr(expression)?;
        self.in_supported_position = false;
        Ok(())
    }

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        let was_supported = self.in_supported_position;
        // Skip cap predicates so they don't count toward the range/equal classification
        // used to pick the autoparameterize mode below.
        if was_supported && self.cap_predicates.contains(expression) {
            return Ok(());
        }
        if was_supported {
            match expression {
                Expr::BinaryOp { lhs, op, rhs } => match (lhs.as_mut(), op, rhs.as_mut()) {
                    // A literal marked by the exclusion pre-pass won't be parameterized, so it
                    // must not influence the equals/range mixing gate: fall through as
                    // unsupported.
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(lit))
                        if !matches!(lit, Literal::Preserved(_)) =>
                    {
                        self.contains_equal = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_equal_placeholder = true;
                        }
                        return Ok(());
                    }
                    (Expr::Row { exprs: lhs_exprs, .. }, BinaryOperator::Equal, Expr::Row { exprs, .. })
                        if is_parameterizable_row_equality(lhs_exprs, exprs) =>
                    {
                        self.contains_equal = true;
                        for expr in exprs {
                            if let Expr::Literal(Literal::Placeholder(_)) = expr {
                                self.contains_equal_placeholder = true;
                            }
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(lit))
                        if op.is_ordering_comparison()
                            && !matches!(lit, Literal::Preserved(_)) =>
                    {
                        self.contains_range = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_range_placeholder = true;
                        }
                        return Ok(());
                    }
                    (Expr::Literal(_), BinaryOperator::Equal | BinaryOperator::NotEqual, Expr::Column(_)) => {
                        // for lit = col and lit != col, swap the equality first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Row { exprs: lhs_exprs, .. }, BinaryOperator::Equal, Expr::Row { exprs: rhs_exprs, .. })
                        if is_parameterizable_row_equality(rhs_exprs, lhs_exprs) =>
                    {
                        // for (lit, ..) = (col, ..), swap the rows first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Literal(_), op, Expr::Column(_)) if op.is_ordering_comparison() => {
                        // for lit <ordering op> col, swap operands and flip operator, then revisit
                        mem::swap(lhs, rhs);
                        // this shouldn't fail as we just did the `op.is_ordering_comparison()`
                        // check
                        *op = op.flip_ordering_comparison().unwrap();
                        return self.visit_expr(expression);
                    }
                    (lhs, BinaryOperator::And, rhs) => {
                        self.visit_expr(lhs)?;
                        self.in_supported_position = true;
                        self.visit_expr(rhs)?;
                        self.in_supported_position = true;
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::Between { min, max, .. } => match (min.as_ref(), max.as_ref()) {
                    (Expr::Literal(lit), _) | (_, Expr::Literal(lit)) => {
                        self.contains_range = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_range_placeholder = true;
                        }
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::In {
                    lhs,
                    rhs: InValue::List(exprs),
                    negated: false,
                } if exprs.iter().all(|e| {
                    match e {
                        // Case 1: Single-column IN (a IN (1,2,3))
                        Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)) => true,
                        // Case 2: Multi-column IN ((a,b) IN ((1,2), (3,4)))
                        Expr::Row { exprs, .. }
                            if exprs.iter().all(
                                |inner| matches!(inner, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))),
                            ) =>
                        {
                            true
                        }
                        _ => false,
                    }
                }) && !self.has_aggregates =>
                {
                    match lhs.as_ref() {
                        Expr::Row { exprs, .. } if !is_row_of_columns(exprs) => {
                            self.in_supported_position = false
                        }
                        Expr::Column(_) | Expr::Row { .. } => {
                            self.contains_equal = true;
                            return Ok(());
                        }
                        _ => self.in_supported_position = false,
                    }
                }
                _ => self.in_supported_position = false,
            }
        }

        visit_mut::walk_expr(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(offset, Literal::Placeholder(_)) {
            self.contains_equal = true;
        }

        visit_mut::walk_offset(self, offset)
    }
}

/// Replace all literals in positions we support with placeholders, extracting the literals as
/// parameters in a parameter list of (placeholder position, value).
///
/// `prev` and `prev_slots` come from an earlier phase's run over the same query, so this walk
/// numbers its positions in one space with that one and reports what was originally at each.
pub fn auto_parameterize_query(
    query: &mut SelectStatement,
    prev: Vec<(usize, Literal)>,
    prev_slots: LiteralSlots,
    autoparameterize: bool,
    server_supports_mixed_comparisons: bool,
    visit_limit_clause: bool,
) -> ReadySetResult<AutoParameters> {
    let cap_predicates = collect_top_level_caps(query);

    // Don't try to auto-parameterize equal-queries that already contain range params for now, since
    // we don't yet allow mixing range and equal parameters in the same query
    let mut visitor = AnalyzeLiteralsVisitor {
        cap_predicates: cap_predicates.clone(),
        ..Default::default()
    };
    visitor.visit_select_statement(query).unwrap();

    let (autoparameterize_equals, autoparameterize_ranges) = if !autoparameterize {
        // Every literal the author wrote stays inline, which is the form a cache created with
        // `AUTOPARAM OFF` takes. The walk still runs, so it counts the placeholders already there.
        (false, false)
    } else if server_supports_mixed_comparisons {
        (true, true)
    } else if !visitor.contains_range {
        // If a query contains no range comparisons in positions that support
        // autoparameterization, we can just proceed with autoparameterizing equals
        // comparisons
        (true, false)
    } else if !visitor.contains_equal {
        // If a query contains no equals comparisons in positions that support
        // autoparameterization, we can just proceed with autoparameterizing range
        // comparisons
        (false, true)
    } else {
        // If we're here, it means the query has both range and equals comparisons in
        // positions that support autoparameterization

        match (
            visitor.contains_equal_placeholder,
            visitor.contains_range_placeholder,
        ) {
            // If the query contains only equals placeholders, we try to autoparameterize the rest
            // of the equals comparisons in the query
            (true, false) => (true, false),
            // If the query contains only range placeholders, we try to autoparameterize the rest
            // of the range comparisons in the query
            (false, true) => (false, true),
            // If the query contains no placeholderse, we try to autoparameterize the equals
            // comparisons only, since we don't support mixed comparisons yet
            (false, false) => (true, false),
            // A query that already mixes equal and range placeholders gets neither, since we
            // don't support mixed comparisons yet. The walk still runs, so it counts the
            // placeholders already there and reports the query's canonical positions.
            (true, true) => (false, false),
        }
    };

    let mut visitor = AutoParameterizeVisitor {
        autoparameterize_equals,
        autoparameterize_ranges,
        out: prev,
        prev_slots: prev_slots.slots.into_vec(),
        visit_limit_clause,
        cap_predicates,
        ..Default::default()
    };
    visitor.visit_select_statement(query)?;
    Ok(AutoParameters {
        params: visitor.out,
        slots: LiteralSlots {
            slots: visitor.slots.into_boxed_slice(),
            in_list_runs: visitor.in_list_runs.into_boxed_slice(),
        },
    })
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;

    fn try_parse_select_statement(q: &str, dialect: Dialect) -> Result<SelectStatement, String> {
        readyset_sql_parsing::parse_select(dialect, q).map_err(|e| e.to_string())
    }

    fn parse_select_statement(q: &str, dialect: Dialect) -> SelectStatement {
        try_parse_select_statement(q, dialect).unwrap()
    }

    #[test]
    fn a_subquery_limit_placeholder_takes_no_position() {
        let dialect = readyset_sql::Dialect::MySQL;
        let both_phases = |sql: &str| {
            let mut first_query = parse_select_statement(sql, dialect);
            let first = auto_parameterize_query(
                &mut first_query,
                Vec::new(),
                LiteralSlots::default(),
                true,
                false,
                false,
            )
            .unwrap();
            let mut second_query = parse_select_statement(sql, dialect);
            let second = auto_parameterize_query(
                &mut second_query,
                first.params.clone(),
                first.slots.clone(),
                true,
                false,
                true,
            )
            .unwrap();
            (first.slots, second.slots)
        };

        for sql in [
            "SELECT * FROM (SELECT x FROM t WHERE a = 1 LIMIT ?) sub WHERE sub.x = 2",
            "WITH c AS (SELECT x FROM t WHERE a = 1 LIMIT ?) SELECT * FROM c WHERE c.x = 2",
        ] {
            let (first, second) = both_phases(sql);
            assert_eq!(
                first.slots, second.slots,
                "a subquery's limit placeholder took a position for {sql}"
            );
        }

        // The top-level clause is walked, so its placeholder takes the position after the literal
        // the first phase lifted.
        let (_, second) = both_phases("SELECT x FROM t WHERE a = 1 LIMIT ?");
        assert_eq!(second.slots.to_vec(), vec![Some(Literal::Integer(1)), None]);
    }

    fn test_auto_parameterize(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
        dialect: readyset_sql::Dialect,
        server_supports_mixed_comparisons: bool,
    ) {
        let mut query = parse_select_statement(query, dialect);
        let expected = parse_select_statement(expected_query, dialect);
        let res = auto_parameterize_query(
            &mut query,
            Vec::new(),
            LiteralSlots::default(),
            true,
            server_supports_mixed_comparisons,
            true,
        )
        .unwrap()
        .params;
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(res, expected_added_parameters);
    }

    fn test_auto_parameterize_mysql(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parameterize(
            query,
            expected_query,
            expected_added_parameters,
            readyset_sql::Dialect::MySQL,
            false,
        )
    }

    fn test_auto_parameterize_postgres(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parameterize(
            query,
            expected_query,
            expected_added_parameters,
            readyset_sql::Dialect::PostgreSQL,
            false,
        )
    }

    #[test]
    fn no_literals() {
        test_auto_parameterize_mysql("SELECT * FROM users", "SELECT * FROM users", vec![]);
        test_auto_parameterize_postgres("SELECT * FROM users", "SELECT * FROM users", vec![]);
    }

    #[test]
    fn simple_parameter() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1",
            "SELECT id FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn and_parameters() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND name = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    /// A literal frozen by the autoparam-exclusion machinery (wrapped in `Literal::Preserved`, as
    /// the pre-pass produces) is kept inline by `auto_parameterize_query` while sibling literals
    /// are still parameterized, and the final sweep unwraps the marker to a plain literal.
    #[test]
    fn autoparam_keeps_preserved_literal_inline() {
        struct FreezeStrings;
        impl<'ast> VisitorMut<'ast> for FreezeStrings {
            type Error = std::convert::Infallible;
            fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
                if matches!(literal, Literal::String(_)) {
                    let inner = mem::replace(literal, Literal::Null);
                    *literal = Literal::Preserved(Box::new(inner));
                }
                Ok(())
            }
        }

        let dialect = Dialect::MySQL;
        let mut query = parse_select_statement(
            "SELECT id FROM users WHERE name = \"frozen\" AND id = 5",
            dialect,
        );
        FreezeStrings.visit_select_statement(&mut query).unwrap();

        let params = auto_parameterize_query(
            &mut query,
            Vec::new(),
            LiteralSlots::default(),
            true,
            false,
            true,
        )
        .unwrap()
        .params;
        unwrap_all_preserved(&mut query);

        // `name` stays an inline constant; `id` is autoparameterized. Equality also proves no
        // `Literal::Preserved` survived the sweep (it would not equal the plain `String`).
        let expected = parse_select_statement(
            "SELECT id FROM users WHERE name = \"frozen\" AND id = ?",
            dialect,
        );
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(params, vec![(0, 5.into())]);
    }

    /// A frozen literal in a range position is likewise preserved, and (via the analyze-gate
    /// guard) does not flip the query into the unsupported mixed-comparison bail.
    #[test]
    fn autoparam_keeps_preserved_range_literal_inline() {
        struct FreezeIntoPreserved;
        impl<'ast> VisitorMut<'ast> for FreezeIntoPreserved {
            type Error = std::convert::Infallible;
            fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
                // Freeze the range bound `10`, leaving the equality literal `5` to parameterize.
                if matches!(literal, Literal::Integer(10) | Literal::UnsignedInteger(10)) {
                    let inner = mem::replace(literal, Literal::Null);
                    *literal = Literal::Preserved(Box::new(inner));
                }
                Ok(())
            }
        }

        let dialect = Dialect::MySQL;
        let mut query =
            parse_select_statement("SELECT id FROM users WHERE id = 5 AND age > 10", dialect);
        FreezeIntoPreserved
            .visit_select_statement(&mut query)
            .unwrap();

        let params = auto_parameterize_query(
            &mut query,
            Vec::new(),
            LiteralSlots::default(),
            true,
            false,
            true,
        )
        .unwrap()
        .params;
        unwrap_all_preserved(&mut query);

        let expected =
            parse_select_statement("SELECT id FROM users WHERE id = ? AND age > 10", dialect);
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(params, vec![(0, 5.into())]);
    }

    #[test]
    fn existing_param_before() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE x = ? AND id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE x = ? AND id = ? AND name = ?",
            vec![(1, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn existing_param_after() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\" AND x = ?",
            "SELECT id FROM users WHERE id = ? AND name = ? AND x = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    #[test]
    fn existing_param_between() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND x = ? AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND x = ? AND name = ?",
            vec![(0, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn literal_in_or() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = \"bob\"",
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = ?",
            vec![(0, "bob".into())],
        )
    }

    #[test]
    fn literal_in_subquery_where() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn literal_in_field() {
        test_auto_parameterize_mysql(
            "SELECT id + 1 FROM users WHERE id = 1",
            "SELECT id + 1 FROM users WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn row_in_predicate() {
        // FIXME(sqlparser): Read the FIXME above, the expected query gets parsed incorrectly
        // because of nom, but the actual query itself works as expected because of the hardcoded
        // check above
        // test_auto_parameterize_mysql(
        //     "SELECT * FROM t WHERE (a, b) IN ((1, 10))",
        //     "SELECT * FROM t WHERE (a, b) IN ((?, ?))",
        //     vec![(0, 1.into()), (1, 10.into())],
        // );

        test_auto_parameterize_mysql(
            "SELECT * FROM t WHERE (a, b) IN ((1, 'str'),(2, 'string'))",
            "SELECT * FROM t WHERE (a, b) IN ((?, ?), (?, ?))",
            vec![
                (0, 1.into()),
                (1, "str".into()),
                (2, 2.into()),
                (3, "string".into()),
            ],
        );
    }

    #[test]
    fn literal_in_in_rhs() {
        test_auto_parameterize_mysql(
            "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (10,20,31)",
            "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (?,?,?)",
            vec![(0, 10.into()), (1, 20.into()), (2, 31.into())],
        );
    }

    #[test]
    fn mixed_in_with_equality() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id in (1, 2) AND name = 'bob'",
            "SELECT id FROM users WHERE id in (?, ?) AND name = ?",
            vec![(0, 1.into()), (1, 2.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn equal_in_equal() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE x = 'foo' AND id in (1, 2) AND name = 'bob'",
            "SELECT id FROM users WHERE x = ? AND id in (?, ?) AND name = ?",
            vec![
                (0, "foo".into()),
                (1, 1.into()),
                (2, 2.into()),
                (3, "bob".into()),
            ],
        );
    }

    #[test]
    fn in_with_aggregates() {
        test_auto_parameterize_mysql(
            "SELECT count(*) FROM users WHERE id = 1 AND x IN (1, 2)",
            "SELECT count(*) FROM users WHERE id = ? AND x IN (?, ?)",
            vec![(0, 1.into()), (1, 1.into()), (2, 2.into())],
        );
    }

    #[test]
    fn literal_equals_column() {
        test_auto_parameterize_mysql(
            "SELECT * FROM users WHERE 1 = id",
            "SELECT * FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn literal_not_equals_column() {
        test_auto_parameterize_mysql(
            "SELECT * FROM users WHERE 1 != id",
            "SELECT * FROM users WHERE id != 1",
            vec![],
        );
    }

    #[test]
    fn existing_range_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            vec![],
        )
    }

    #[test]
    fn offset() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 ORDER BY SCORE ASC LIMIT 3 OFFSET 6",
            "SELECT * FROM posts WHERE id = ? ORDER BY SCORE ASC LIMIT 3 OFFSET ?",
            vec![(0, 1.into()), (1, 6.into())],
        );
    }

    #[test]
    fn constant_filter_with_param_betwen() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            vec![],
        );
    }

    #[test]
    fn range_query_literals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10",
            "SELECT * FROM posts WHERE score > ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score <= 10",
            "SELECT * FROM posts WHERE score >= ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive_exclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score < 10",
            "SELECT * FROM posts WHERE score >= ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_exclusive_inclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score <= 10",
            "SELECT * FROM posts WHERE score > ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn equals_then_range() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > 0 AND score < 10",
            "SELECT * FROM posts WHERE id = ? AND score > 0 AND score < 10",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_then_equals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = 1",
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_with_or() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            vec![],
        );
    }

    #[test]
    fn nested_range() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            vec![],
        );
    }

    #[test]
    fn less_than() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id < 10",
            "SELECT * FROM posts WHERE id < ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn less_than_equal() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id <= 10",
            "SELECT * FROM posts WHERE id <= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id > 10",
            "SELECT * FROM posts WHERE id > ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than_equal() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id >= 10",
            "SELECT * FROM posts WHERE id >= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn range_with_pre_existing_equals_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            vec![],
        );
    }

    #[test]
    fn equals_with_pre_existing_range_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 10",
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn ranges_only() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id > 10 AND views > 2",
            "SELECT * FROM posts WHERE id > ? AND views > ?",
            vec![(0, 10.into()), (1, 2.into())],
        );
    }

    #[test]
    fn some_equals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views = 10",
            "SELECT * FROM posts WHERE id = ? AND views = ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn some_equals_with_ranges() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND date = ? AND views > 10",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn supported_equals_with_unsupported_ranges() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_with_unsupported_equals() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_equals() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_ranges() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    mod mixed_comparisons {
        use super::*;

        fn test_auto_parameterize_mysql(
            query: &str,
            expected_query: &str,
            // These are parameters that are expected to have been added by the
            // autoparameterization rewrite pass
            expected_added_parameters: Vec<(usize, Literal)>,
        ) {
            test_auto_parameterize(
                query,
                expected_query,
                expected_added_parameters,
                readyset_sql::Dialect::MySQL,
                true,
            )
        }

        #[test]
        fn some_equals_with_ranges() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 9",
                "SELECT * FROM posts WHERE id = ? AND date = ? AND views > ?",
                vec![(1, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn range_with_pre_existing_equals_param() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = ? AND views > 10",
                "SELECT * FROM posts WHERE id = ? AND views > ?",
                vec![(1, 10.into())],
            );
        }

        #[test]
        fn equals_with_pre_existing_range_param() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 9",
                "SELECT * FROM posts WHERE id = ? AND views > ? AND date > ?",
                vec![(0, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn supported_equals_with_unsupported_ranges() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_with_unsupported_equals() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_equals() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_ranges() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }

        #[test]
        fn supported_row_equality_predicates() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE (name, age) = ('Bob', 27)",
                "SELECT id FROM users WHERE (name, age) = (?, ?)",
                vec![(0, "Bob".into()), (1, 27.into())],
            )
        }
    }

    /// Row-number cap predicates (`rn op K` where `rn` aliases a `ROW_NUMBER()` projection)
    /// must keep their integer literal intact: CBJR reads that literal as a cardinality
    /// signal. These tests pin to sqlparser-only on the MySQL dialect: nom-sql doesn't
    /// support `ROW_NUMBER`, and MySQL's `?` placeholder shape is what the
    /// auto-parameterizer emits.
    mod row_number_caps {
        use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

        use super::*;

        fn parse_mysql(q: &str) -> SelectStatement {
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, q).unwrap()
        }

        fn test_auto_parameterize_rn(
            query: &str,
            expected_query: &str,
            expected_added_parameters: Vec<(usize, Literal)>,
        ) {
            let mut query = parse_mysql(query);
            let expected = parse_mysql(expected_query);
            let res = auto_parameterize_query(
                &mut query,
                Vec::new(),
                LiteralSlots::default(),
                true,
                false,
                true,
            )
            .unwrap()
            .params;
            assert_eq!(
                query,
                expected,
                "\n  left: {}\n right: {}",
                query.display(Dialect::MySQL),
                expected.display(Dialect::MySQL),
            );
            assert_eq!(res, expected_added_parameters);
        }

        #[test]
        fn cap_predicate_literal_preserved() {
            // The outer WHERE references `rn` which resolves to the inner ROW_NUMBER()
            // projection. The literal 10 is the cardinality cap and must survive.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10",
                vec![],
            );
        }

        #[test]
        fn cap_and_regular_filter_mixed() {
            // The cap stays put; the unrelated `id = 5` equality parameterizes normally.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10 AND id = 5",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10 AND id = ?",
                vec![(0, 5.into())],
            );
        }

        #[test]
        fn flipped_cap_literal_left_preserved() {
            // `10 >= rn` is the swap-arm shape; the dual-orientation cap set must catch
            // the post-swap expression on the recursive visit.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 10 >= rn",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 10 >= rn",
                vec![],
            );
        }

        #[test]
        fn flipped_cap_equality_literal_left_preserved() {
            // `1 = rn` exercises the equality swap arm (Equal/NotEqual branch).
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 1 = rn",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 1 = rn",
                vec![],
            );
        }

        #[test]
        fn synthetic_underscore_rn_preserved() {
            // The synthetic `__rn` alias produced by TOP-K rewrite must also be recognized.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS __rn FROM t) s \
                 WHERE __rn <= 100",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS __rn FROM t) s \
                 WHERE __rn <= 100",
                vec![],
            );
        }

        #[test]
        fn qualified_cap_reference_preserved() {
            // `s.rn <= 5` resolves via the FROM-alias `s` to the inner subquery's RN
            // projection. The cap literal stays intact.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE s.rn <= 5",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE s.rn <= 5",
                vec![],
            );
        }

        #[test]
        fn non_rn_range_filter_still_parameterized() {
            // Negative control: a plain integer column with the same operator/literal
            // shape still autoparameterizes — the gate is targeted to RN caps only.
            test_auto_parameterize_rn(
                "SELECT id FROM posts WHERE id <= 10",
                "SELECT id FROM posts WHERE id <= ?",
                vec![(0, 10.into())],
            );
        }
    }

    /// Every parameter the pass emits is indexed by how many placeholders precede it, so an
    /// index has to account for the placeholders already in the query -- both the ones the
    /// user wrote and the ones an earlier phase lifted.
    mod parameter_indexing {
        use super::*;

        /// The Readyset rewrite runs the pass twice: once without the limit clause, then again
        /// over the result carrying the first run's parameters. The second run's own parameters
        /// follow the first run's, with no gap.
        #[test]
        fn second_phase_continues_first_phase_numbering() {
            let mut query = parse_select_statement(
                "SELECT * FROM posts WHERE id = 1 LIMIT 3 OFFSET 6",
                Dialect::MySQL,
            );
            let one = auto_parameterize_query(
                &mut query,
                Vec::new(),
                LiteralSlots::default(),
                true,
                false,
                false,
            )
            .unwrap();
            assert_eq!(one.params, vec![(0, 1.into())]);
            assert_eq!(one.slots, vec![Some(1.into())].into());

            // The second walk sees the first's lifted literal as a placeholder, and its slots say
            // it was a literal all the same. The OFFSET it reaches is a position the first never
            // took.
            let two = auto_parameterize_query(&mut query, one.params, one.slots, true, false, true)
                .unwrap();
            assert_eq!(two.params, vec![(0, 1.into()), (1, 6.into())]);
            assert_eq!(two.slots, vec![Some(1.into()), Some(6.into())].into());
        }

        /// A placeholder inside a row comparison occupies a parameter position, so the literal
        /// beside it is the second parameter, not the first.
        #[test]
        fn row_equality_counts_an_existing_placeholder() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE (name, age) = (?, 27)",
                "SELECT id FROM users WHERE (name, age) = (?, ?)",
                vec![(1, 27.into())],
            );
        }

        /// A range placeholder puts the pass in range-only mode. A row equality is an equality,
        /// so it stays inline rather than joining the range parameters in one query.
        #[test]
        fn row_equality_respects_range_only_mode() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE (name, age) = ('Bob', 27) AND score > ?",
                "SELECT id FROM users WHERE (name, age) = ('Bob', 27) AND score > ?",
                vec![],
            );
        }

        /// A lifted literal keys a lookup on the column it faces, so only a row of columns compared
        /// to literals is parameterized. Any other keeps its literals inline, and is not an
        /// equality that competes with the query's ranges for the parameter mode.
        #[test]
        fn row_that_cannot_key_a_lookup_keeps_its_literals_inline() {
            for query in [
                "SELECT id FROM users WHERE ('', name) = ('A', 's')",
                "SELECT id FROM users WHERE (name, '') = ('A', 's')",
                "SELECT id FROM users WHERE (name, age) = ('Bob', score)",
                "SELECT id FROM users WHERE (name, '') IN (('A', ''), ('B', 'x'))",
            ] {
                test_auto_parameterize_mysql(query, query, vec![]);
                let with_range = format!("{query} AND score > 5");
                test_auto_parameterize_mysql(
                    &with_range,
                    &with_range.replace("> 5", "> ?"),
                    vec![(0, 5.into())],
                );
            }
        }

        /// A row of literals compared to a row of columns is turned around first, as a scalar
        /// comparison is, so its literals key the lookup on the columns they face and count as an
        /// equality when choosing the parameter mode. A row that would still not key a lookup
        /// turned around stays as written.
        #[test]
        fn row_equality_with_literals_on_the_left_is_turned_around() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE ('Bob', 27) = (name, age)",
                "SELECT id FROM users WHERE (name, age) = (?, ?)",
                vec![(0, "Bob".into()), (1, 27.into())],
            );
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE (?, 27) = (name, age)",
                "SELECT id FROM users WHERE (name, age) = (?, ?)",
                vec![(1, 27.into())],
            );
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE ('Bob', 27) = (name, age) AND score > 5",
                "SELECT id FROM users WHERE (name, age) = (?, ?) AND score > 5",
                vec![(0, "Bob".into()), (1, 27.into())],
            );
            let kept = "SELECT id FROM users WHERE (name, 'x') = (age, score)";
            test_auto_parameterize_mysql(kept, kept, vec![]);
        }
    }

    /// A query's slots say what it held at each canonical position, which is what lets a read
    /// reach a cache whose form it does not produce on its own.
    mod slots {
        use super::*;

        fn slots_of(query: &str, dialect: Dialect) -> LiteralSlots {
            let mut query = parse_select_statement(query, dialect);
            auto_parameterize_query(
                &mut query,
                Vec::new(),
                LiteralSlots::default(),
                true,
                false,
                true,
            )
            .unwrap()
            .slots
        }

        /// Every query is its own cache: read by the text it was created from, each literal it
        /// spells out identifies it and nothing keys a lookup.
        #[test]
        fn a_query_matches_itself() {
            for query in [
                "SELECT * FROM t WHERE a = 1",
                "SELECT * FROM t WHERE a = ?",
                "SELECT * FROM t WHERE a = 1 AND b = ?",
                "SELECT * FROM t WHERE a = ? AND b = 2",
                "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3",
                "SELECT * FROM t WHERE a > 1 AND b < 2",
                "SELECT * FROM t WHERE a IN (1, 2, 3)",
                "SELECT * FROM t WHERE a IN (?, ?, ?)",
                "SELECT * FROM t WHERE (x, y) = ('a', 1)",
                "SELECT * FROM t WHERE (x, y) = (?, 1)",
                "SELECT * FROM t WHERE a = 1 LIMIT 3 OFFSET 6",
                "SELECT * FROM t WHERE a = 1 ORDER BY b LIMIT 3 OFFSET ?",
            ] {
                let slots = slots_of(query, Dialect::MySQL);
                let matched = slots
                    .match_read(&slots)
                    .unwrap_or_else(|| panic!("`{query}` does not match itself"));
                assert!(
                    matched.is_empty(),
                    "`{query}` keys a lookup with {:?}",
                    matched,
                );
            }
        }

        /// The behaviour the whole mechanism exists for: a read spelling out a value where the
        /// cache parameterized reaches it, and that value keys the lookup.
        #[test]
        fn a_spelled_out_value_keys_the_lookup() {
            let cache = slots_of("SELECT * FROM t WHERE a = ? AND b = 2", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE a = 1 AND b = 2", Dialect::MySQL);
            let matched = cache
                .match_read(&read)
                .expect("the read belongs to the cache");
            assert_eq!(matched, vec![(0, 1.into())]);
        }

        /// The key indexes the cache's own parameters rather than canonical positions, so a
        /// literal the cache keeps ahead of one it parameterized must not shift what follows.
        #[test]
        fn an_inline_literal_before_a_parameter_keys_from_zero() {
            let cache = slots_of("SELECT * FROM t WHERE b = 2 AND a = ?", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE b = 2 AND a = 1", Dialect::MySQL);
            let matched = cache
                .match_read(&read)
                .expect("the read belongs to the cache");
            assert_eq!(matched, vec![(0, 1.into())]);
        }

        /// A different value where the cache keeps a literal is a different query.
        #[test]
        fn a_differing_literal_belongs_to_another_cache() {
            let cache = slots_of("SELECT * FROM t WHERE a = ? AND b = 2", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE a = 1 AND b = 3", Dialect::MySQL);
            assert!(cache.match_read(&read).is_none());
        }

        /// A cache with a literal baked in cannot serve a client that wants to bind that position
        /// at execution.
        #[test]
        fn a_baked_literal_cannot_serve_a_bind_position() {
            let cache = slots_of("SELECT * FROM t WHERE a = ? AND b = 2", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE a = 1 AND b = ?", Dialect::MySQL);
            assert!(cache.match_read(&read).is_none());
        }

        /// A fully parameterized cache takes every value of every position, and all of them key
        /// the lookup.
        #[test]
        fn a_parameterized_cache_takes_any_value() {
            let cache = slots_of("SELECT * FROM t WHERE a = ? AND b = ?", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE a = 1 AND b = 2", Dialect::MySQL);
            let matched = cache
                .match_read(&read)
                .expect("every position takes a value");
            assert_eq!(matched, vec![(0, 1.into()), (1, 2.into())]);
        }

        /// `collapse_where_in` folds every `IN` arity into one shape, so a read can reach this
        /// point carrying more positions than the cache has. The position count is what says the
        /// two are different queries.
        #[test]
        fn a_differing_in_arity_belongs_to_another_cache() {
            let cache = slots_of("SELECT * FROM t WHERE a IN (1, 2)", Dialect::MySQL);
            let read = slots_of("SELECT * FROM t WHERE a IN (1, 2, 3)", Dialect::MySQL);
            assert_ne!(cache.positions(), read.positions());
            assert!(cache.match_read(&read).is_none());
        }

        /// `collapse_where_in` folds every list into one predicate, so two queries whose lists
        /// hold the same literals in different groupings reach the same shape and the same
        /// position count. The grouping is what separates them.
        #[test]
        fn a_regrouped_in_list_belongs_to_another_cache() {
            let cache = slots_of(
                "SELECT * FROM t WHERE a IN (1, 2) AND b IN (3)",
                Dialect::MySQL,
            );
            let read = slots_of(
                "SELECT * FROM t WHERE a IN (1) AND b IN (2, 3)",
                Dialect::MySQL,
            );
            assert_eq!(cache.positions(), read.positions());
            assert!(cache.match_read(&read).is_none());
        }

        /// A run of the same length sitting at a different position is a different query, so the
        /// run's start has to count as much as its length. Both of these hold the same three
        /// literals and reach the same shape.
        #[test]
        fn a_shifted_in_list_belongs_to_another_cache() {
            let cache = slots_of(
                "SELECT * FROM t WHERE x IN (1, 2) AND y = 3",
                Dialect::MySQL,
            );
            let read = slots_of(
                "SELECT * FROM t WHERE x = 1 AND y IN (2, 3)",
                Dialect::MySQL,
            );
            assert_eq!(cache.positions(), read.positions());
            assert!(cache.match_read(&read).is_none());
        }

        /// The same, with the run shifting past a plain equality between two lists.
        #[test]
        fn a_run_shifting_past_an_equality_belongs_to_another_cache() {
            let cache = slots_of(
                "SELECT * FROM t WHERE a IN (1, 2) AND b = 3 AND c IN (4, 5)",
                Dialect::MySQL,
            );
            let read = slots_of(
                "SELECT * FROM t WHERE a = 1 AND b IN (2, 3) AND c IN (4, 5)",
                Dialect::MySQL,
            );
            assert_eq!(cache.positions(), read.positions());
            assert!(cache.match_read(&read).is_none());
        }

        /// A list the author wrote as placeholders lifts nothing, so its grouping is taken where
        /// the walk sees the list rather than where a literal is lifted.
        #[test]
        fn a_placeholder_in_list_is_grouped_like_a_literal_one() {
            let cache = slots_of(
                "SELECT * FROM t WHERE a IN (?, ?) AND b IN (?)",
                Dialect::MySQL,
            );
            let read = slots_of(
                "SELECT * FROM t WHERE a IN (1, 2) AND b IN (3)",
                Dialect::MySQL,
            );
            assert!(cache.match_read(&read).is_some());
            let regrouped = slots_of(
                "SELECT * FROM t WHERE a IN (1) AND b IN (2, 3)",
                Dialect::MySQL,
            );
            assert!(cache.match_read(&regrouped).is_none());
        }
    }
}
