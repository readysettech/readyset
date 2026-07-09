//! Pre-unnest position normalization for subqueries in HAVING and ORDER BY.
//!
//! This pass reshapes a SELECT so that any subquery originally living in
//! HAVING or in an ORDER BY item ends up in a canonical position the
//! existing `unnest_subqueries` pass already handles: HAVING-subquery
//! predicates land in the wrapper's WHERE; ORDER BY-subquery items project
//! to the inner SELECT-list with synthetic aliases referenced from the
//! wrapper's ORDER BY.  Correlated and uncorrelated subqueries are treated
//! uniformly - the pass's trigger is structural, not correlational.
//!
//! ## Coherent post-GROUP-BY tail migration
//!
//! HAVING, ORDER BY, LIMIT, and OFFSET form a causally-chained semantic
//! pipeline: HAVING filters before ORDER BY sorts; ORDER BY sorts before
//! LIMIT clamps.  When the wrap fires (any trigger), the whole tail moves
//! to the wrapper; the inner has none of `{ORDER BY, LIMIT, OFFSET}` post-
//! wrap.  Splitting the chain would be unsound when LIMIT is present:
//! inner LIMIT clamping rows before the wrapper's moved HAVING applies
//! produces a different result set than the original semantics.
//!
//! ## Return contract
//!
//! Returns the set of aliases minted for the wrapper derived tables.  The
//! adapter pipeline threads that set into `inline_leading_derived_table`,
//! which skips inlining any FROM subquery whose alias is in the set:
//! the wrap is load-bearing until `unnest_subqueries` consumes the moved
//! predicates.  Post-unnest, the pipeline re-invokes
//! `inline_leading_derived_table` with an empty set to recover inlining
//! opportunities the wraps temporarily blocked.
//!
//! ## Supported HAVING-subquery shapes
//!
//! Only shapes accepted by `is_supported_subquery_predicate` are wrapped.
//! Shapes outside that set (top-level OR with a subquery-bearing operand,
//! ANY / ALL / SOME, BETWEEN with subquery bounds, IN with a nested-SELECT
//! LHS) are left in place, and the `unnest_all_subqueries` backstop gate
//! rejects them cleanly rather than letting them drift downstream as
//! latent wrong-results potential.  `SELECT DISTINCT` is supported: the
//! DISTINCT is lifted from the inner to the wrapper so it dedupes on the
//! user-original projection tuple (matching the original semantics) rather
//! than the synthetically-widened inner tuple.
//!
//! Correlated subquery predicates inside INNER JOIN ON clauses are the
//! separate `move_correlated_constraints_from_join_to_where` mechanism
//! (Stage 3 of the extended-subquery-decorrelation project).

use std::collections::HashSet;
use std::convert::Infallible;
use std::mem;

use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias, collect_local_from_items, conjoin_all_dedup,
    contains_select, deep_columns_visitor_mut_in_set,
    denormalize_having_and_group_by_for_statement, expect_field_as_expr,
    expect_only_subquery_from_with_alias_mut, fix_duplicate_aliases, for_each_window_function,
    is_aggregation_or_grouped, normalize_having_and_group_by_for_statement,
    project_columns_if_not_exist_fix_duplicate_aliases, resolve_field_reference,
};
use crate::unnest_subqueries::collect_subquery_predicates;
use crate::{contains_wf, is_window_function_expr};
use readyset_errors::{ReadySetError, ReadySetResult, invariant};
use readyset_sql::analysis::is_aggregate;
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::analysis::visit_mut::{
    VisitorMut, walk_expr, walk_function_expr, walk_select_statement,
};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, OrderBy, OrderClause, Relation,
    SelectStatement, SqlIdentifier, SqlQuery, TableExpr, TableExprInner,
};
/// Prefix for aliases minted by the wrap for its inserted derived tables.
/// The full alias is `{WRAP_ALIAS_PREFIX}{N}` where `N` is a counter unique
/// to each `WrapVisitor` traversal and chosen to avoid collision with any
/// subquery alias already present in the input statement.
const WRAP_ALIAS_PREFIX: &str = "_NSP_W_";

pub(crate) trait NormalizeSubqueryPositions: Sized {
    /// Wrap SELECT bodies whose HAVING or ORDER BY contains a subquery so
    /// the subquery migrates to a position the existing `unnest_subqueries`
    /// pass can decorrelate.  When fired, HAVING-subquery predicates move
    /// to the wrapper's WHERE, and ORDER BY + LIMIT + OFFSET migrate to
    /// the wrapper alongside (mandatory for soundness: leaving LIMIT in
    /// the inner would clamp rows before the wrapper's moved HAVING
    /// applies, producing a different result set than the original).
    ///
    /// Returns the set of aliases the wrap assigned to its inserted derived
    /// tables (empty if no wrap fired).  The adapter pipeline threads that
    /// set into `inline_leading_derived_table`, which uses it to skip
    /// inlining wrap outputs until `unnest_subqueries` consumes them.
    fn normalize_subquery_positions(&mut self) -> ReadySetResult<HashSet<SqlIdentifier>>;
}

impl NormalizeSubqueryPositions for SelectStatement {
    fn normalize_subquery_positions(&mut self) -> ReadySetResult<HashSet<SqlIdentifier>> {
        // Precondition: `expand_stars` must have run.  The wrap projects
        // fields through `expect_field_as_expr`, which panics on
        // `FieldDefinitionExpr::All`.  Enforced in release too, so
        // out-of-order pipeline wiring returns a clean internal error
        // instead of panicking downstream.
        invariant!(
            !contains_star_projection(self),
            "normalize_subquery_positions requires expand_stars to have run",
        );
        // Bottom-up: descend into nested SELECTs first, then process this
        // level.  Per-statement entry/exit alias hygiene happens INSIDE
        // the visitor -- fires only when HAVING is present, denormalizes
        // HAVING + GROUP BY + ORDER BY around the wrap attempt, and
        // renormalizes the inner (if wrap fired) or stmt (if not).
        let mut visitor = WrapVisitor::new(self);
        visitor.visit_select_statement(self)?;
        Ok(visitor.emitted_aliases)
    }
}

/// True when any `SelectStatement` reachable from `stmt` still carries a
/// wildcard select item (`SELECT *` or `SELECT t.*`).  Used only by the
/// debug-assert entry check.
fn contains_star_projection(stmt: &SelectStatement) -> bool {
    struct StarCheck {
        found: bool,
    }
    impl<'ast> Visitor<'ast> for StarCheck {
        type Error = Infallible;
        fn visit_field_definition_expr(
            &mut self,
            fde: &'ast FieldDefinitionExpr,
        ) -> Result<(), Self::Error> {
            if matches!(
                fde,
                FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_)
            ) {
                self.found = true;
            }
            visit::walk_field_definition_expr(self, fde)
        }
    }
    let mut check = StarCheck { found: false };
    let _ = check.visit_select_statement(stmt);
    check.found
}

impl NormalizeSubqueryPositions for SqlQuery {
    fn normalize_subquery_positions(&mut self) -> ReadySetResult<HashSet<SqlIdentifier>> {
        match self {
            SqlQuery::Select(stmt) => stmt.normalize_subquery_positions(),
            SqlQuery::CompoundSelect(csq) => {
                let mut emitted = HashSet::new();
                for (_op, stmt) in &mut csq.selects {
                    emitted.extend(stmt.normalize_subquery_positions()?);
                }
                Ok(emitted)
            }
            _ => Ok(HashSet::new()),
        }
    }
}

/// Bottom-up visitor: walks nested SELECTs first, then processes the current
/// level via [`wrap_post_groupby_positions`].  Stage 2 will extend the wrap
/// trigger to ORDER BY-subquery detection (same migration mechanism);
/// Stage 3's INNER JOIN ON correlated-subquery move lives in a separate
/// pass.
///
/// Aliases minted for each wrap output are counter-based (`_NSP_W_0`,
/// `_NSP_W_1`, ...) and skip any value that would collide with a subquery
/// alias already present in the input statement -- the visitor snapshots
/// those into `reserved_aliases` at entry.  `emitted_aliases` collects the
/// aliases actually assigned so the adapter pipeline can pass them into
/// `inline_leading_derived_table` as the "don't-inline-these" set.
struct WrapVisitor {
    reserved_aliases: HashSet<SqlIdentifier>,
    next_alias_id: usize,
    emitted_aliases: HashSet<SqlIdentifier>,
}

impl WrapVisitor {
    fn new(stmt: &SelectStatement) -> Self {
        let mut reserved_aliases = HashSet::new();
        collect_subquery_aliases(stmt, &mut reserved_aliases);
        Self {
            reserved_aliases,
            next_alias_id: 0,
            emitted_aliases: HashSet::new(),
        }
    }

    /// Return the next wrap alias that collides with neither the input
    /// statement's pre-existing subquery aliases nor any wrap alias this
    /// visitor has already minted.  Records the choice in `emitted_aliases`.
    fn mint_wrap_alias(&mut self) -> SqlIdentifier {
        loop {
            let candidate =
                SqlIdentifier::from(format!("{WRAP_ALIAS_PREFIX}{}", self.next_alias_id));
            self.next_alias_id += 1;
            if !self.reserved_aliases.contains(&candidate)
                && !self.emitted_aliases.contains(&candidate)
            {
                self.emitted_aliases.insert(candidate.clone());
                return candidate;
            }
        }
    }
}

impl<'ast> VisitorMut<'ast> for WrapVisitor {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        stmt: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Bottom-up: descend first.
        walk_select_statement(self, stmt)?;

        // Per-statement alias hygiene: denormalize HAVING + GROUP BY + ORDER
        // BY on `stmt`, attempt wrap, renormalize.
        //
        // ORDER BY is denormalized at entry because the unified wrap may
        // project its items to the inner SELECT-list and that projection
        // helper does expression-equality lookups against `inner.fields`
        // -- alias-resolved form is the canonical input for that.
        //
        // The renormalize target depends on whether the wrap fired:
        //
        // - **No wrap**: `stmt` is unchanged; renormalize symmetrically.
        // - **Wrap fired**: `stmt` is the WRAPPER, which has no HAVING /
        //   GROUP BY / ORDER BY in alias-ref form (everything at the
        //   wrapper level is fully-qualified `_NSP_W.<alias>` by
        //   construction).  The INNER, however, carries the denormalized
        //   GROUP BY + remaining-non-subq HAVING; renormalize those there.
        //   ORDER BY was migrated to the wrapper and the inner has none.
        //
        // Triggers (precise check happens INSIDE `wrap_post_groupby_positions`
        // after denormalize, where SELECT-list alias references have been
        // resolved to underlying expressions):
        //
        // - HAVING-subquery (via `collect_subquery_predicates`).
        // - ORDER BY-subquery (via `order_by_has_subquery`).
        //
        // Outer gate is intentionally broad -- `stmt.having.is_some() ||
        // stmt.order.is_some()` -- so the denormalize step runs FIRST and
        // alias-ref-to-subquery patterns (`HAVING j_count > 5` where
        // `j_count` is a SELECT-list alias for `(SELECT ...)`) are caught
        // by the post-denormalize precise check.  A `contains_select`-based
        // prefilter on the pre-denormalize HAVING / ORDER BY would miss
        // those patterns because `contains_select` walks only the visible
        // Expr tree, not following alias refs.
        //
        // ORDER BY trigger is a pre-denormalize check: an ORDER BY item that
        // syntactically contains a subquery in the user query.  Alias-refs
        // to SELECT-list subquery expressions (e.g., `SELECT (subq) AS m ...
        // ORDER BY m`) do NOT trigger the wrap -- the SELECT-list already
        // projects the subquery once per row and ORDER BY on the alias
        // sorts on that projection without any wrap layer.  Checked here,
        // before denormalize, because denormalize would substitute the
        // alias with the underlying expression and mask the distinction.
        let order_by_trigger = order_by_has_subquery(stmt);

        // Cost: denormalize+renormalize cycle for statements with HAVING /
        // ORDER-BY-with-subquery but no HAVING subquery.  Cheap -- bounded
        // by the size of those clauses, idempotent net effect.
        if stmt.having.is_some() || order_by_trigger {
            // HAVING-only pre-wrap CSE short-circuit.  Try normalizing
            // HAVING against SELECT-list-item aliases first (via
            // `normalize_having_and_group_by_for_statement`, which
            // structurally matches sub-expressions against SELECT-list
            // exprs and rewrites matches to alias refs).  If the CSE
            // eliminates every subquery from HAVING AND ORDER BY has no
            // subquery (its original state, `order_by_trigger`), the
            // wrap is unnecessary: the pre-existing SELECT-list-subquery
            // decorrelation handles the aliased subqueries and HAVING
            // ends up as a plain non-subquery expression referencing
            // the resulting aliases.
            //
            // Restricted to HAVING because ORDER BY / GROUP BY alias
            // refs are unconditionally denormalized (expanded to raw
            // expressions) by several downstream passes via
            // `resolve_field_reference` / `normalize_field_reference`
            // (ILDT's `normalize_group_and_order_by`, DTR's ORDER BY
            // resolve, `inline_subquery`'s prepared-stmt GROUP BY
            // resolve, `fix_correlated_columns_in_group_by_and_having`
            // for GROUP BY).  Leaving an ORDER BY / GROUP BY alias ref
            // pointing at a SELECT-list subquery risks exposing the raw
            // subquery when one of those denormalizers runs against an
            // inner DT that carries it up.  HAVING has no analogous
            // downstream denormalizer, so the short-circuit is safe
            // there.  See REA-6724 for the ORDER BY / GROUP BY
            // extension follow-up.
            normalize_having_and_group_by_for_statement(stmt)?;
            let having_has_subq = stmt.having.as_ref().is_some_and(contains_select);
            // The short-circuit additionally requires that the outer stmt
            // is genuinely aggregating (DISTINCT / aggregate in SELECT /
            // GROUP BY).  A HAVING without aggregation is a downstream
            // pipeline invariant violation (`inline_subquery`'s
            // `check_group_by_compatibility` explicitly asserts against
            // it): those queries need the wrap's HAVING-to-WHERE move
            // to reach a shape downstream can handle.  Falling through
            // preserves pre-change behavior for that class.
            let outer_is_aggregating = is_aggregation_or_grouped(stmt)?;
            if !having_has_subq && !order_by_trigger && outer_is_aggregating {
                return Ok(());
            }

            denormalize_having_and_group_by_for_statement(stmt)?;
            let wrapped = wrap_post_groupby_positions(stmt, order_by_trigger, self)?;
            if wrapped {
                let (inner_stmt, _) = expect_only_subquery_from_with_alias_mut(stmt)?;
                normalize_having_and_group_by_for_statement(inner_stmt)?;
            } else {
                normalize_having_and_group_by_for_statement(stmt)?;
            }
        }
        Ok(())
    }
}

/// Collect the alias of every subquery `TableExpr` reachable from `stmt` into
/// `out`.  Base tables and unaliased subqueries are ignored -- the collected
/// set is later used to seed `WrapVisitor::reserved_aliases`, and only
/// subquery aliases can collide with wrap-minted derived-table aliases at
/// the ILDT skip check.
fn collect_subquery_aliases(stmt: &SelectStatement, out: &mut HashSet<SqlIdentifier>) {
    struct Collector<'a> {
        out: &'a mut HashSet<SqlIdentifier>,
    }
    impl<'ast, 'a> Visitor<'ast> for Collector<'a> {
        type Error = Infallible;
        fn visit_table_expr(&mut self, te: &'ast TableExpr) -> Result<(), Self::Error> {
            if let Some((_, alias)) = as_sub_query_with_alias(te) {
                self.out.insert(alias);
            }
            visit::walk_table_expr(self, te)
        }
    }
    let _ = Collector { out }.visit_select_statement(stmt);
}

/// Trigger predicate: does the statement's ORDER BY contain a subquery?
/// `contains_select` (not `is_supported_subquery_predicate`) -- items are
/// projected wholesale, so the predicate-shape distinction the unnester
/// cares about doesn't apply at this gate.
fn order_by_has_subquery(stmt: &SelectStatement) -> bool {
    let Some(order_clause) = &stmt.order else {
        return false;
    };
    order_clause
        .order_by
        .iter()
        .any(|item| matches!(&item.field, FieldReference::Expr(e) if contains_select(e)))
}

/// Wrap a SELECT whose post-GROUP-BY pipeline contains a subquery -- in
/// either HAVING (HAVING trigger) or an ORDER BY item (ORDER BY trigger).
/// When the wrap fires (either trigger), the full post-GROUP-BY tail
/// migrates COHERENTLY to the wrapper:
///
/// - HAVING-subquery predicates move to the wrapper's WHERE, referencing
///   the inner's projected aggregate / GROUP BY values via the `_NSP_W`
///   alias.  Non-subquery HAVING conjuncts stay in inner HAVING (per-
///   conjunct split).  Skipped when the HAVING trigger doesn't fire.
/// - ORDER BY items are projected to the inner SELECT-list with synthetic
///   aliases; the wrapper's ORDER BY references those aliases with
///   original direction modifiers preserved.  Inner ORDER BY is removed.
///   Skipped only when the original had no ORDER BY at all.
/// - LIMIT and OFFSET move to the wrapper.  Inner has neither.
///
/// Migrating the whole TOP-K tail (ORDER BY + LIMIT + OFFSET) alongside
/// HAVING is mandatory for soundness -- see design memo sec.5.0.  Leaving
/// LIMIT in the inner would clamp rows BEFORE the wrapper's moved HAVING
/// applies, producing a different result set than the original semantics.
///
/// Returns `Ok(true)` if a wrap was applied.
fn wrap_post_groupby_positions(
    stmt: &mut SelectStatement,
    order_by_trigger: bool,
    visitor: &mut WrapVisitor,
) -> ReadySetResult<bool> {
    // HAVING trigger: take HAVING off so the no-fire path can restore it;
    // the fire path keeps only the non-subquery remainder.  Detects both
    // direct subqueries in HAVING and alias-refs to SELECT-list subqueries
    // (visible post-denormalize).
    let mut subquery_preds = Vec::new();
    if let Some(having_expr) = stmt.having.take() {
        let (preds, remaining) = collect_subquery_predicates(&having_expr)?;
        if preds.is_empty() {
            stmt.having = Some(having_expr);
        } else {
            subquery_preds = preds;
            stmt.having = remaining;
        }
    }

    // If neither trigger fired, no wrap.  `order_by_trigger` is pre-
    // denormalize (see call site) so it stays accurate under this
    // post-denormalize position.
    if subquery_preds.is_empty() && !order_by_trigger {
        return Ok(false);
    }

    // WF migration -- fires only when HAVING moves at least one
    // conjunct AND the outer SELECT-list contains a window function.
    // The moved HAVING would otherwise land in the wrapper's WHERE,
    // evaluated AFTER the inner's WF materialization -- but standard
    // SQL logical order puts HAVING BEFORE WF, so leaving WFs in the
    // inner would compute them over an unfiltered groupset.  When
    // this path fires we partition WF-carrying SELECT-list items out
    // of the stmt (they will be spliced back into the wrapper's
    // SELECT-list at their original positions, with internal refs
    // rebound to reference inner-projected aliases).  When only the
    // ORDER BY trigger fires (no HAVING to move), WFs stay in the
    // inner soundly: ORDER BY / LIMIT / OFFSET all sit after WFs in
    // the SQL logical order, so moving them to the wrapper doesn't
    // reorder anything relative to WFs.
    let migrate_wfs = !subquery_preds.is_empty() && contains_wf!(stmt);
    let mut wf_slots: Vec<(usize, Expr, SqlIdentifier)> = Vec::new();
    if migrate_wfs {
        let all_fields = mem::take(&mut stmt.fields);
        let mut kept = Vec::with_capacity(all_fields.len());
        for (pos, fde) in all_fields.into_iter().enumerate() {
            let (expr_ref, alias_ref) = expect_field_as_expr(&fde);
            if is_window_function_expr!(expr_ref) {
                let outer_alias = alias_for_expr(expr_ref, alias_ref);
                // Consume `fde` to extract the owned `Expr` for later
                // in-place rebinding.  The `Expr` variant is guaranteed
                // by the pass-entry `invariant!` on `expand_stars`.
                match fde {
                    FieldDefinitionExpr::Expr { expr, .. } => {
                        wf_slots.push((pos, expr, outer_alias));
                    }
                    _ => unreachable!("expand_stars invariant"),
                }
            } else {
                kept.push(fde);
            }
        }
        stmt.fields = kept;
    }

    // Snapshot user-original outer aliases BEFORE deduping the inner.
    // Duplicate aliases in the user's SELECT-list are legal SQL (both
    // implicit -- `SELECT s.city, j.city` -- and explicit -- `SELECT
    // s.city AS foo, j.city AS foo`), and the client expects those
    // duplicates preserved in the response.  The wrapper's outer `AS
    // <alias>` uses these snapshots; the inner alias (post-dedup) is
    // used only to disambiguate the `<wrap_alias>.<inner>` ref.
    // Post-WF-partition: `stmt.fields` contains only non-WF items;
    // `outer_aliases` therefore holds one entry per non-WF slot.
    let outer_aliases: Vec<SqlIdentifier> = stmt
        .fields
        .iter()
        .map(|fde| {
            let (expr, alias) = expect_field_as_expr(fde);
            alias_for_expr(expr, alias)
        })
        .collect();

    // Dedup inner SELECT-list aliases so `<wrap_alias>.<inner>` refs are
    // unambiguous.  Later duplicates get suffixed in place; the first
    // occurrence is unchanged.  Downstream projection helpers keep the
    // invariant that inner aliases stay unique.
    fix_duplicate_aliases(&mut stmt.fields);

    // When the wrap fires, ALL of {ORDER BY, LIMIT, OFFSET} migrate to the
    // wrapper.  This is mandatory for soundness: HAVING / ORDER BY / LIMIT
    // / OFFSET form a causally-chained post-GROUP-BY pipeline, and leaving
    // LIMIT in the inner would let the inner clamp rows BEFORE the
    // wrapper's moved HAVING applies -- producing a different result set
    // than the original semantics.  See design memo sec.5.0.  Inner has none
    // of {ORDER BY, LIMIT, OFFSET} post-wrap.
    let saved_order = stmt.order.take();
    let saved_limit_clause = mem::take(&mut stmt.limit_clause);

    // Remember the user-visible field-list length before we add synthetic
    // projections.  `project_columns_if_not_exist_fix_duplicate_aliases`
    // appends new items at the end and renames later duplicates only, so
    // the first N entries (user-originals, already deduped above) keep
    // their order and aliases intact.  We use this boundary post-processing
    // to read back the original-field basis for the wrapper SELECT-list.
    let original_field_count = stmt.fields.len();

    // Snapshot the to-be-wrapped statement's local relations (for both the
    // LHS-rebind and the subquery-body correlation-rebind filters).  Once
    // the wrap step finishes, these are exactly the relations local to the
    // (post-wrap) inner.
    let stmt_rels = collect_local_from_items(stmt)?;

    let wrap_alias = visitor.mint_wrap_alias();

    // Per moved predicate: rebind LHS sub-expressions against `stmt`'s fields,
    // and rebind correlation refs inside the subquery body.
    for pred in &mut subquery_preds {
        rebind_predicate_against_inner(pred, stmt, &stmt_rels, &wrap_alias)?;
    }

    // Project ORDER BY items to the inner SELECT-list with synthetic
    // aliases; build the wrapper ORDER BY from the aliases with original
    // direction modifiers preserved.  Skipped entirely when the original
    // had no ORDER BY.
    let wrapper_order = build_wrapper_order(saved_order, stmt, &wrap_alias)?;

    // Rebind each partitioned WF slot's expression against the (now
    // inner-shape) `stmt`.  `LhsRebindVisitor` walks the WF expression;
    // at `Expr::WindowFunction` it descends into function args +
    // partition_by + order_by exprs, applying the standard match /
    // project-as-whole / descend rebind rules to each sub-slot.  The
    // WF's own function is left as the windowed operation (a windowed
    // aggregate stays windowed; a ranking function stays a rank fn).
    // Group-level aggregates and local-column refs inside the WF get
    // projected into `stmt.fields` (past the user-original slots) and
    // rebound to `<wrap_alias>.<projected_alias>` for the wrapper.
    if migrate_wfs {
        let mut lhs_rebind = LhsRebindVisitor {
            stmt,
            stmt_rels: &stmt_rels,
            wrap_alias: &wrap_alias,
        };
        for (_pos, wf_expr, _outer_alias) in &mut wf_slots {
            lhs_rebind.visit_expr(wf_expr)?;
        }
    }

    // Take ownership of the inner (the original body, now stripped of
    // {ORDER BY, LIMIT, OFFSET}, with HAVING-subquery conjuncts split out
    // and synthetic projections appended for HAVING / ORDER BY rebinds).
    // The wrap alias is recorded in `visitor.emitted_aliases` -- the adapter
    // pipeline threads that set into `inline_leading_derived_table` so it
    // leaves this body intact until `unnest_subqueries` consumes the wrap.
    let mut inner = mem::take(stmt);

    // Lift DISTINCT from the inner to the wrapper.  The inner's SELECT-list
    // now carries synthetic projections (for HAVING LHS rebinds and ORDER BY
    // items) beyond the user-original fields; running DISTINCT on that
    // widened tuple would admit rows the original semantics collapse.  The
    // wrapper projects only the user-original fields (via `build_wrapper_
    // fields`), so DISTINCT at the wrapper level dedupes on the same tuple
    // the original query does.  Rows sharing user-original values come from
    // the same grouping key and therefore have identical synthetic values,
    // so the wrapper's WHERE (which references synthetic projections) makes
    // the same admit/reject decision for all such rows -- WHERE-then-DISTINCT
    // is semantically equivalent to HAVING-then-DISTINCT in the original.
    let was_distinct = mem::replace(&mut inner.distinct, false);

    // Lift LATERAL from inner to wrapper.  The wrapper takes the pre-wrap
    // statement's syntactic position; if that position was LATERAL (i.e.,
    // the pre-wrap statement is a `LATERAL (...)` FROM subquery of an
    // outer scope), the wrapper inherits LATERAL so the outer's correlation
    // resolution still works.  The inner, now a plain derived table inside
    // the wrapper's own FROM, is not at a LATERAL position.
    let was_lateral = mem::replace(&mut inner.lateral, false);

    // Metadata invariant: nothing in the current pipeline populates
    // `SelectMetadata` before this pass (`collapse_where_in` and every MIR
    // -side `CollapsedWhereIn` set run downstream).  Enforcing empty at the
    // wrap boundary means any future pre-NSP metadata-setter tripped here
    // MUST explicitly decide wrapper vs. inner placement -- silently
    // defaulting either way would risk mis-attributing a client-level
    // annotation to a derived subquery, or vice versa.
    invariant!(
        inner.metadata.is_empty(),
        "normalize_subquery_positions wrap expects empty metadata at entry",
    );

    // Build the wrapper: SELECT <wrap_alias>.<inner_alias> AS <outer_alias>,
    // ... FROM (inner) AS <wrap_alias> WHERE <moved preds AND-folded>.  The
    // qualified ref uses the (post-dedup) inner alias from the first
    // `original_field_count` entries of `inner.fields`; the outer `AS` uses
    // the pre-dedup `outer_aliases` snapshot so user-visible duplicates are
    // preserved.  When WFs migrated, the rebound WF expressions are spliced
    // back at their original positions (via `wf_slots`) so the wrapper's
    // SELECT-list preserves the user's field ordering.  When no WFs
    // migrated, `wf_slots` is empty and `build_wrapper_fields` degenerates
    // to plain projection refs in order.
    let wrapper_fields = build_wrapper_fields(
        &inner.fields[..original_field_count],
        &outer_aliases,
        wf_slots,
        &wrap_alias,
    );
    let wrapper_where = conjoin_all_dedup(subquery_preds);

    // Enumerate every SelectStatement field explicitly.  No `..Default::default()`
    // -- adding a new field to `SelectStatement` in the future must force a
    // compile error here so the placement (wrapper vs. inner) is decided
    // deliberately, not silently defaulted.
    *stmt = SelectStatement {
        ctes: vec![],           // invariant: no CTEs survive validate_pipeline_invariants
        distinct: was_distinct, // lifted from inner (client-visible level)
        lateral: was_lateral,   // lifted from inner (wrapper takes pre-wrap syntactic position)
        fields: wrapper_fields,
        tables: vec![TableExpr {
            inner: TableExprInner::Subquery(Box::new(inner)),
            alias: Some(wrap_alias),
            column_aliases: vec![],
        }],
        join: vec![],                // single-DT FROM
        where_clause: wrapper_where, // moved HAVING subquery predicates
        group_by: None,              // aggregation happens in inner
        having: None,                // moved out (or CSE'd) upstream
        order: wrapper_order,        // migrated ORDER BY, or None
        limit_clause: saved_limit_clause,
        metadata: vec![], // invariant-checked empty at entry
    };

    Ok(true)
}

/// Project every ORDER BY item's expression into `inner`'s SELECT-list
/// (with synthetic aliases via `project_columns_if_not_exist_fix_duplicate_aliases`,
/// which dedups against existing fields) and return a fresh `OrderClause`
/// whose entries reference the projected aliases via `<wrap_alias>.<proj_alias>`
/// -- direction modifiers (`ASC`/`DESC`, `NULLS FIRST`/`LAST`) preserved.
///
/// Returns `Ok(None)` when `saved_order` is `None` (caller had no ORDER BY).
///
/// `FieldReference::Numeric(N)` items resolve against `inner.fields` at
/// 1-based position `N` BEFORE projection.  Out-of-range or zero positions
/// fail the rewrite (downstream pipeline falls back to upstream).
fn build_wrapper_order(
    saved_order: Option<OrderClause>,
    inner: &mut SelectStatement,
    wrap_alias: &SqlIdentifier,
) -> ReadySetResult<Option<OrderClause>> {
    let Some(order_clause) = saved_order else {
        return Ok(None);
    };

    // Resolve each item's `field` into a concrete `Expr` ready to project.
    // `resolve_field_reference` handles both `Numeric(N)` (1-based index into
    // `inner.fields`, with `invalid_query_err!` on out-of-bounds) and `Expr`
    // (with belt-and-suspenders SELECT-list alias resolution -- idempotent
    // here since entry hygiene already denormalized ORDER BY).
    let item_exprs: Vec<Expr> = order_clause
        .order_by
        .iter()
        .map(|item| resolve_field_reference(&inner.fields, &item.field))
        .collect::<ReadySetResult<_>>()?;

    // Project all items uniformly.  Returns one (field_idx, alias) per item;
    // dedups against existing fields automatically.
    let projected = project_columns_if_not_exist_fix_duplicate_aliases(inner, &item_exprs);

    // Build the wrapper's ORDER BY: bare `_NSP_W.<alias>` refs with original
    // direction modifiers.
    let order_by = order_clause
        .order_by
        .into_iter()
        .zip(projected)
        .map(|(item, (_, alias))| OrderBy {
            field: FieldReference::Expr(wrap_column_ref(alias, wrap_alias)),
            order_type: item.order_type,
            null_order: item.null_order,
        })
        .collect();

    Ok(Some(OrderClause { order_by }))
}

/// Rebind references in a moved HAVING subquery predicate against the to-be-
/// inner stmt's projection set.  Operates in two parts:
///
/// 1. **LHS handling (and any other parts of the predicate that aren't a
///    nested SELECT)** -- a unified top-down visitor matches each sub-
///    expression against existing `stmt.fields` (reuses projections);
///    otherwise, for aggregates and local-column refs, projects whole and
///    rebinds; everything else descends.  The visitor's
///    `visit_select_statement` returns without descending so the subquery
///    body is left to step 2.
///
/// 2. **Subquery body** -- `deep_columns_visitor_mut_in_set` walks every
///    `Expr::Column` whose table is in `stmt_rels`, descending into
///    nested SELECTs except those that shadow our local relations.  Each
///    matched column is projected into `stmt.fields` (idempotent if already
///    present) and rebound to `<wrap_alias>.<projected_alias>`.
fn rebind_predicate_against_inner(
    pred: &mut Expr,
    stmt: &mut SelectStatement,
    stmt_rels: &HashSet<Relation>,
    wrap_alias: &SqlIdentifier,
) -> ReadySetResult<()> {
    // Step 1: LHS/outer-level rebind via unified top-down walk.
    let mut lhs_visitor = LhsRebindVisitor {
        stmt,
        stmt_rels,
        wrap_alias,
    };
    lhs_visitor.visit_expr(pred)?;

    // Step 2: subquery-body correlation rebind.  Generic outermost-SELECT
    // walk -- applies the deep-columns-with-shadow-set rebind to each
    // subquery body reachable in `pred` without coupling to the structural
    // catalog of supported subquery predicate shapes.  For a supported
    // predicate, this fires exactly once (the predicate has one outermost
    // subquery by definition).
    let mut body_rebinder = SubqueryBodyRebinder {
        stmt,
        stmt_rels,
        wrap_alias,
    };
    body_rebinder.visit_expr(pred)?;
    Ok(())
}

/// Visitor that finds each outermost `SelectStatement` reachable in an
/// `Expr` and applies a deep-columns-with-shadow-set walk to it, projecting
/// and rebinding any local-column refs to `<wrap_alias>.<projected_alias>`.
/// Does not descend into the subquery body itself -- `visit_select_statement`
/// runs the inner walk and stops.
struct SubqueryBodyRebinder<'a> {
    stmt: &'a mut SelectStatement,
    stmt_rels: &'a HashSet<Relation>,
    wrap_alias: &'a SqlIdentifier,
}

impl<'a, 'ast> VisitorMut<'ast> for SubqueryBodyRebinder<'a> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        body: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Outermost subquery body -- apply the deep-columns rebind, don't
        // descend further (the outer visitor's default would walk into
        // the body via this callback again, but our override stops here).
        //
        // Body-level shadowing: when the body's own FROM declares an alias
        // matching one of `self.stmt_rels` (e.g. `FROM qa.spj AS s` while
        // outer is `qa.s`), column refs in the body with `table = s`
        // resolve to the body's local FROM, NOT to a correlation against
        // outer.  The `deep_columns_visitor_mut_in_set` entry point treats
        // its top-level stmt as the scope where the passed outer set lives
        // (its own FROM does not shadow), so the caller subtracts the
        // body's own locals up-front.  Nested-subquery shadows deeper in
        // the body are tracked per-level by the visitor itself.
        let stmt = &mut *self.stmt;
        let wrap_alias = self.wrap_alias;
        let body_locals = collect_local_from_items(body)?;
        let effective_outer: HashSet<Relation> =
            self.stmt_rels.difference(&body_locals).cloned().collect();
        deep_columns_visitor_mut_in_set(body, &effective_outer, &mut |col_expr| {
            *col_expr = project_into_wrap_ref(stmt, col_expr, wrap_alias);
        })
    }
}

/// Unified top-down LHS/outer-level rebind visitor (Step 1 of
/// `rebind_predicate_against_inner`).  At each `Expr` node:
///
/// 0. If the node is an `Expr::WindowFunction`, descend into its function
///    arguments (via `walk_function_expr`, which walks children through
///    `visit_expr`) and each `partition_by` / `order_by` expression.  Do
///    NOT treat the WF itself as a candidate for match-and-rebind or
///    project-as-whole -- the WF's own function is a windowed operation,
///    not a group-level aggregate; capturing it as an alias would move
///    computation into the inner and produce wrong-results semantics
///    (see REA-6724).  This explicit arm avoids relying on the current
///    `walk_expr` implementation-detail of dispatching WF's own function
///    via `visit_function_expr` -- the invariant is documented here at
///    the use site.
/// 1. Try a structural match against `stmt.fields` -- if found, replace
///    with `<wrap_alias>.<existing_alias>` and stop descending.
/// 2. Otherwise, if the node is an aggregate or a local-column ref, project
///    it whole into `stmt.fields`, rebind, stop descending.
/// 3. Otherwise descend into children.
///
/// `visit_select_statement` is overridden to return without descending so
/// nested subquery bodies are left untouched -- those are handled by Step 2
/// of `rebind_predicate_against_inner` via the deep-columns visitor.
struct LhsRebindVisitor<'a> {
    stmt: &'a mut SelectStatement,
    stmt_rels: &'a HashSet<Relation>,
    wrap_alias: &'a SqlIdentifier,
}

impl<'a, 'ast> VisitorMut<'ast> for LhsRebindVisitor<'a> {
    type Error = ReadySetError;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        // 0. Handle Expr::WindowFunction explicitly -- see the doc-comment
        //    on `LhsRebindVisitor` for the invariant.
        if let Expr::WindowFunction {
            function,
            partition_by,
            order_by,
        } = expr
        {
            walk_function_expr(self, function)?;
            for pb in partition_by {
                self.visit_expr(pb)?;
            }
            for (ob, _, _) in order_by {
                self.visit_expr(ob)?;
            }
            return Ok(());
        }

        // 1. Match against existing stmt.fields.
        if let Some(alias) = match_existing_field(expr, &self.stmt.fields) {
            *expr = wrap_column_ref(alias, self.wrap_alias);
            return Ok(());
        }

        // 2. Project-as-whole if aggregate or local-column ref.
        let is_agg = matches!(expr, Expr::Call(fe) if is_aggregate(fe));
        let is_local_col = matches!(expr, Expr::Column(col)
            if col.table.as_ref()
                .is_some_and(|t| self.stmt_rels.contains(t)));
        if is_agg || is_local_col {
            *expr = project_into_wrap_ref(self.stmt, expr, self.wrap_alias);
            return Ok(());
        }

        // 3. Descend.
        walk_expr(self, expr)
    }

    fn visit_select_statement(
        &mut self,
        _stmt: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Don't descend into nested subquery bodies -- handled by the
        // deep-columns visitor in Step 2.
        Ok(())
    }
}

/// Build a column reference `<wrap_alias>.<projected_alias>` -- the canonical
/// shape rebound predicates use to address the inner's projected items
/// after the wrap.
fn wrap_column_ref(projected_alias: SqlIdentifier, wrap_alias: &SqlIdentifier) -> Expr {
    Expr::Column(Column {
        name: projected_alias,
        table: Some(Relation::from(wrap_alias.clone())),
    })
}

/// Project `expr` into `stmt.fields` (idempotent -- `project_columns_if_not_
/// exist_fix_duplicate_aliases` reuses an existing structurally-equal field
/// when present) and return a `<wrap_alias>.<projected_alias>` column
/// reference for the caller to install.
fn project_into_wrap_ref(
    stmt: &mut SelectStatement,
    expr: &Expr,
    wrap_alias: &SqlIdentifier,
) -> Expr {
    let mut pairs =
        project_columns_if_not_exist_fix_duplicate_aliases(stmt, std::slice::from_ref(expr));
    let alias = pairs
        .pop()
        .expect("project_columns returns one entry per input")
        .1;
    wrap_column_ref(alias, wrap_alias)
}

/// Look up `expr` (by structural equality) in `fields`.  Returns the alias
/// if found (using `alias_for_expr` to derive a default when the matched
/// field has no explicit alias).
fn match_existing_field(expr: &Expr, fields: &[FieldDefinitionExpr]) -> Option<SqlIdentifier> {
    for fe in fields {
        let (fe_expr, fe_alias) = expect_field_as_expr(fe);
        if expr == fe_expr {
            return Some(alias_for_expr(fe_expr, fe_alias));
        }
    }
    None
}

/// Build the wrapper's SELECT-list by iterating through the outer's
/// original field positions and emitting one entry per position.
///
/// Emission rules per position:
///
/// - **Non-WF position**: emit `<wrap_alias>.<inner_alias> AS
///   <outer_alias>` -- a projection ref into the inner's SELECT-list.
///   `inner_alias` is read from `non_wf_fields` (post-dedup, unique
///   per-slot); `outer_alias` comes from `non_wf_outer_aliases`, a
///   snapshot taken before dedup that preserves user-visible
///   duplicates.
/// - **WF position**: emit the rebound WF `Expr` `AS <outer_alias>`
///   directly.  The WF's internal aggregate and GB-key refs have
///   already been rewritten by `LhsRebindVisitor` to reference
///   `<wrap_alias>.<projected_alias>`, so the WF gets computed at
///   wrapper level -- after the wrapper's WHERE filters, preserving
///   the SQL logical order `HAVING -> WF -> SELECT`.
///
/// Degenerate case (no WFs migrated): `wf_slots` is empty, the loop
/// simply emits projection refs in order, and behavior collapses to
/// the pre-WF-preserving wrap semantics.  Callers on that path pass
/// `vec![]` for `wf_slots`.
///
/// `wf_slots` MUST be sorted by original position (ascending) so the
/// front-peek suffices to detect a WF at each iteration.
fn build_wrapper_fields(
    non_wf_fields: &[FieldDefinitionExpr],
    non_wf_outer_aliases: &[SqlIdentifier],
    wf_slots: Vec<(usize, Expr, SqlIdentifier)>,
    wrap_alias: &SqlIdentifier,
) -> Vec<FieldDefinitionExpr> {
    let total = non_wf_fields.len() + wf_slots.len();
    let mut out = Vec::with_capacity(total);
    let mut non_wf_idx: usize = 0;
    let mut wf_iter = wf_slots.into_iter().peekable();
    for pos in 0..total {
        // `wf_slots` is sorted by original position, so a front-peek
        // tells us whether the current position is occupied by a WF.
        if wf_iter.peek().is_some_and(|(p, _, _)| *p == pos) {
            let (_pos, expr, outer_alias) = wf_iter.next().expect("peeked");
            out.push(FieldDefinitionExpr::Expr {
                expr,
                alias: Some(outer_alias),
            });
        } else {
            // Non-WF slot -- emit a projection ref into the inner.
            let inner_fde = &non_wf_fields[non_wf_idx];
            let (inner_expr, inner_alias_opt) = expect_field_as_expr(inner_fde);
            let inner_alias = alias_for_expr(inner_expr, inner_alias_opt);
            out.push(FieldDefinitionExpr::Expr {
                expr: wrap_column_ref(inner_alias, wrap_alias),
                alias: Some(non_wf_outer_aliases[non_wf_idx].clone()),
            });
            non_wf_idx += 1;
        }
    }
    out
}
