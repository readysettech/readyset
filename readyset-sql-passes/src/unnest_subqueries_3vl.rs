use crate::infer_nullability::infer_select_field_nullability;
use crate::rewrite_utils::{
    and_predicates_skip_true, collect_local_from_items, construct_is_not_null_expr,
    construct_scalar_expr, ensure_first_field_alias, expect_only_subquery_from_with_alias_mut,
    expect_sub_query_with_alias, expect_sub_query_with_alias_mut, get_first_field_expr,
    get_unique_alias,
};
use crate::unnest_subqueries::{
    AsJoinableOpts, DeriveTableJoinKind, NonNullSchema, PRESENT_COL_NAME, SubqueryContext,
    as_joinable_derived_table_with_opts, join_derived_table,
};
use readyset_errors::{ReadySetResult, internal_err, invariant};
use readyset_sql::ast::JoinOperator::LeftOuterJoin;
use readyset_sql::ast::{
    BinaryOperator, CaseWhenBranch, Column, Expr, Literal, Relation, SelectStatement, SqlIdentifier,
};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem;

/// What we cache: for a given RHS + correlation, we maintain *once*:
///  - NP probe (null-present):   EXISTS(rhs WHERE first_field IS NULL)
///  - EP probe (existence):      EXISTS(rhs)
///
/// Both are installed as LEFT LATERAL joins that project a `present_` flag.
/// We reference them with `present_ IS [NOT] NULL` where needed.
#[derive(Default, Clone)]
pub(crate) struct ProbeInfo {
    pub has_null: Option<(Relation /*alias*/, SqlIdentifier /*present_*/)>,
    pub non_empty: Option<(Relation /*alias*/, SqlIdentifier /*present_*/)>,
}

/// Structural key hash: normalized RHS body + the correlation ON predicate we apply for that RHS.
/// We hash *post* normalization that `as_joinable_derived_table` performs (DISTINCT, ORDER removal,
/// correlation hoist, TOP-K normalized earlier, etc.). This avoids cache misses caused by superficial
/// differences (alias names, whitespace, etc.).
#[derive(Hash, Eq, PartialEq, Clone)]
struct ProbeKeyHash {
    rhs_hash: u64, // normalized RHS body
    on_hash: u64,  // normalized ON predicate we’ll apply
}

impl ProbeKeyHash {
    fn new(rhs_stmt: &SelectStatement, on_expr: &Option<Expr>) -> Self {
        fn hash_select(stmt: &SelectStatement) -> u64 {
            let mut h = DefaultHasher::new();
            stmt.hash(&mut h);
            h.finish()
        }

        fn hash_expr_opt(on: &Option<Expr>) -> u64 {
            let mut h = DefaultHasher::new();
            if let Some(e) = on {
                e.hash(&mut h);
            }
            h.finish()
        }

        Self {
            rhs_hash: hash_select(rhs_stmt),
            on_hash: hash_expr_opt(on_expr),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
enum ProbeKey {
    NullableRhs(ProbeKeyHash),
    NullFreeRhs(ProbeKeyHash),
}

pub(crate) struct ProbeRegistry {
    map: HashMap<ProbeKey, ProbeInfo>,
}

impl ProbeRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Ensure the RHS subquery is usable for 3VL null-probe and derive its probe context.
    ///
    /// This routine validates or prepares the **RHS side** needed by 3VL guards for
    /// `NOT IN` and select-list subqueries, and returns the information the guard builder
    /// needs to generate correct SQL under three-valued logic.
    ///
    /// Specifically, it determines:
    /// - whether the RHS comparison column is **null-free** under the current schema
    ///   (`NonNullSchema`) and shape;
    /// - whether we need a **window function** in the probe to detect existence of
    ///   *non-null* rows per correlation partition (e.g., `ROW_NUMBER() OVER (PARTITION BY ...)`
    ///   in presence of TOP-K), which becomes part of `RhsContext`;
    /// - the **probe shape** invariants required by the 3VL builders:
    ///   - a stable **anchor top** (single FROM item wrapper),
    ///   - a projected `present_` column for presence checks,
    ///   - (optionally) the original first field’s alias available at the anchor top.
    ///
    /// ### Inputs and dependencies
    /// This function shapes the RHS into a predictable EXISTS-form using as_joinable_derived_table_with_opts.
    /// It preserves TOP-K where needed and, in the NP, requests a stable wrapper and bubbles the first-field alias
    /// to the anchor top so we can apply IS NULL.
    ///
    /// ### Output
    /// Returns a ProbeInfo describing which probes have been materialized: has_null (NP) and/or non_empty (EP) as (Relation, present_col) pairs.
    /// The function uses the provided RhsContext (null-freeness) to decide which probes to build.
    ///
    /// ### Errors & limits
    /// - The function can reject shapes where we cannot establish a stable probe (e.g., missing wrapper,
    ///   required alias not bubbled, or unsupported correlation).
    /// - This function performs **probe shaping** internally via `as_joinable_derived_table_with_opts` and
    ///   mutates `base_stmt` only by joining the probes. The original `rhs_stmt_original` is consumed/cloned for shaping;
    ///   the caller does not need to pre-shape it.
    pub fn ensure_for_rhs(
        &mut self,
        base_stmt: &mut SelectStatement,
        mut rhs_stmt_original: SelectStatement,
        rhs_ctx: &RhsContext,
        need_np: bool,
        need_ep: bool,
    ) -> ReadySetResult<ProbeInfo> {
        macro_rules! shape_dt_as_exists_preserve_top_k {
            ($stmt:expr, $stmt_alias:expr) => {
                as_joinable_derived_table_with_opts(
                    SubqueryContext::Exists,
                    &mut $stmt,
                    $stmt_alias,
                    AsJoinableOpts {
                        preserve_top_k_for_exists: true,
                        ..Default::default()
                    },
                )?
            };
        }

        macro_rules! shape_dt_as_exists_with_opts {
            ($stmt:expr, $stmt_alias:expr, $opts:expr) => {
                as_joinable_derived_table_with_opts(
                    SubqueryContext::Exists,
                    &mut $stmt,
                    $stmt_alias,
                    $opts,
                )?
            };
        }

        macro_rules! join_dt {
            ($base_stmt:expr, $dt:expr, $join_on:expr) => {
                invariant!(join_derived_table(
                    $base_stmt,
                    None,
                    $dt,
                    $join_on,
                    DeriveTableJoinKind::Join(LeftOuterJoin),
                )?)
            };
        }

        // Collect locals once for stable alias generation in probes (EP/NP).
        let locals = collect_local_from_items(base_stmt)?;

        // Normalize RHS & ON:
        // TODO: we really need actual semantic normalization here: aliases, comparison sides, etc
        // PROBE-KEY SHAPE: we normalize through `as_joinable_derived_table_with_opts(.., Exists, ..)`
        // to get a predictable SELECT/FROM/ON shape (DISTINCT/order/limit handled earlier),
        // so the registry can hash and reuse probes across identical RHS+ON shapes.
        let (rhs_dt_for_key, on_for_key) = shape_dt_as_exists_preserve_top_k!(
            rhs_stmt_original.clone(),
            get_unique_alias(&locals, "PRB_K")
        );
        let (rhs_norm_stmt_for_key, _) = expect_sub_query_with_alias(&rhs_dt_for_key);
        let key_hash = ProbeKeyHash::new(rhs_norm_stmt_for_key, &on_for_key);

        let key = if rhs_ctx.is_null_free() {
            ProbeKey::NullFreeRhs(key_hash)
        } else {
            ProbeKey::NullableRhs(key_hash)
        };

        // Look up or initialize an empty entry; we may "upgrade" it by adding missing probes.
        let mut entry = self.map.get(&key).cloned().unwrap_or_default();
        // We may upgrade a partially-populated entry (e.g., add EP later if LHS can be NULL).
        // This keeps probe construction lazy and avoids duplicate joins.

        // Helper to (re)collect locals on demand
        // Using a consistent alias space helps us key + cache probes deterministically.
        let mut locals = Some(locals); // captured earlier
        let mut get_locals = |stmt: &SelectStatement| {
            if let Some(l) = locals.take() {
                Ok(l)
            } else {
                collect_local_from_items(stmt)
            }
        };

        // Lazily build EP (existence) only if requested and absent
        if need_ep && entry.non_empty.is_none() {
            // EP probe: LEFT join of `EXISTS(RHS)` variant that projects `present_`.
            let ep_alias = get_unique_alias(&get_locals(base_stmt)?, "EP_3VL");
            // EP does not need TOP-K preservation: presence after LIMIT/OFFSET is equivalent
            // to presence before it (for LIMIT > 0). Avoid wrapper here.
            let (ep_dt, ep_on) = shape_dt_as_exists_with_opts!(
                &mut rhs_stmt_original.clone(),
                ep_alias.clone(),
                AsJoinableOpts {
                    preserve_top_k_for_exists: false, // do not preserve TOP-K for EP
                    force_wrapper: false,             // no need for a single-anchor wrapper for EP
                    bubble_alias_to_anchor_top: None, // EP doesn’t use the first-field alias
                }
            );
            join_dt!(base_stmt, ep_dt, ep_on);
            // Join materializes `present_` for EP_3VL; consumers check it with IS [NOT] NULL.
            entry.non_empty = Some((ep_alias.into(), PRESENT_COL_NAME.into()));
        }

        // Lazily build NP (null-present) only if requested/possible and absent
        if need_np && !rhs_ctx.is_null_free() && entry.has_null.is_none() {
            // 1) Ensure the *original* first field has a stable alias on the original RHS
            let original_ff_alias = ensure_first_field_alias(&mut rhs_stmt_original);
            // This alias is bubbled to the probe’s anchor top so we can reference it in NP’s WHERE.

            // 2) Shape probe DT with stable wrapper and bubbled alias
            let np_alias = get_unique_alias(&get_locals(base_stmt)?, "NP_3VL");
            let (mut np_dt, np_on) = shape_dt_as_exists_with_opts!(
                &mut rhs_stmt_original,
                np_alias.clone(),
                AsJoinableOpts {
                    preserve_top_k_for_exists: true,
                    force_wrapper: true, // <— guarantee stable wrapper
                    bubble_alias_to_anchor_top: Some(original_ff_alias.clone()), // <— bubble alias
                }
            );
            // The `Exists` + `force_wrapper` shape guarantees a single anchor subquery in FROM.
            // `bubble_alias_to_anchor_top` makes `<anchor>.<original_ff_alias>` available here.

            // 3) At the NP level, add WHERE <anchor>.<original_ff_alias> IS NULL
            let (np_stmt, _) = expect_sub_query_with_alias_mut(&mut np_dt);
            let (_, anchor_alias) = expect_only_subquery_from_with_alias_mut(np_stmt)?;
            // NP filter: anchor.<first_field_alias> IS NULL
            // This detects null-producing RHS rows per outer correlation partition.
            np_stmt.where_clause = and_predicates_skip_true(
                mem::take(&mut np_stmt.where_clause),
                construct_is_not_null_expr(
                    Expr::Column(Column {
                        table: Some(anchor_alias.into()),
                        name: original_ff_alias,
                    }),
                    /* IS NULL? */ true,
                ),
            );

            // 4) LEFT JOIN NP as usual
            join_dt!(base_stmt, np_dt, np_on);
            // Join materializes `present_` for NP_3VL; consumers check it with IS [NOT] NULL.
            entry.has_null = Some((np_alias.into(), PRESENT_COL_NAME.into()));
        }

        self.map.insert(key, entry.clone());

        Ok(entry)
    }
}

impl ProbeInfo {
    pub fn has_null_present_col(&self) -> Option<Expr> {
        self.has_null.as_ref().map(|(rel, name)| {
            Expr::Column(Column {
                table: Some(rel.clone()),
                name: name.clone(),
            })
        })
    }

    pub fn non_empty_present_col(&self) -> Option<Expr> {
        self.non_empty.as_ref().map(|(rel, name)| {
            Expr::Column(Column {
                table: Some(rel.clone()),
                name: name.clone(),
            })
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RhsContext {
    is_null_free: bool,
}

impl RhsContext {
    pub(crate) fn new(is_null_free: bool) -> Self {
        Self { is_null_free }
    }

    pub(crate) fn is_null_free(&self) -> bool {
        self.is_null_free
    }
}

pub(crate) fn is_select_expr_null_free(
    expr: &Expr,
    stmt: &SelectStatement,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<bool> {
    infer_select_field_nullability(expr, stmt, schema)
}

pub(crate) fn is_first_field_null_free(
    subquery_stmt: &SelectStatement,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<bool> {
    is_select_expr_null_free(get_first_field_expr(subquery_stmt)?, subquery_stmt, schema)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SelectList3vlFlags {
    pub is_lhs_null_free: bool,
    pub is_not_in: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectList3vlInput {
    pub lhs: Expr,
    pub rhs: Expr,
    pub preserved_rhs_stmt: SelectStatement,
    pub rhs_ctx: RhsContext,
    pub flags: SelectList3vlFlags,
}

pub(crate) fn add_3vl_for_not_in_where_subquery(
    base_stmt: &mut SelectStatement,
    lhs: Expr,
    is_lhs_null_free: bool,
    preserved_rhs_stmt: SelectStatement,
    rhs_ctx: RhsContext,
    probes: &mut ProbeRegistry,
) -> ReadySetResult<Expr> {
    // We only need NP unless LHS can be NULL (EP used only then).
    let need_np = !rhs_ctx.is_null_free();
    let need_ep = false;

    // Ensure NP only (if needed)
    let info = probes.ensure_for_rhs(
        base_stmt,
        preserved_rhs_stmt.clone(),
        &rhs_ctx,
        need_np,
        need_ep,
    )?;

    // rhs_not_null := (NP.present_ IS NULL) or TRUE if RHS null-free
    let rhs_not_null = if rhs_ctx.is_null_free() {
        Expr::Literal(true.into())
    } else {
        let has_null_present = info
            .has_null_present_col()
            .ok_or_else(|| internal_err!("NP_3VL present_ should exist"))?;
        construct_is_not_null_expr(has_null_present, /* IS NULL? */ true)
    };

    // If LHS is provably non-null, the guard is just `rhs_not_null`
    if is_lhs_null_free {
        return Ok(rhs_not_null);
    }

    // Otherwise we also need EP for the (… OR is_empty) branch — upgrade lazily.
    let info = probes.ensure_for_rhs(base_stmt, preserved_rhs_stmt, &rhs_ctx, false, true)?;
    let rhs_is_empty = {
        let ep_present = info
            .non_empty_present_col()
            .ok_or_else(|| internal_err!("EP_3VL present_ should exist"))?;
        construct_is_not_null_expr(ep_present, /* IS NULL? */ true)
    };

    let lhs_not_null = construct_is_not_null_expr(lhs, /* IS NULL? */ false);
    let check = construct_scalar_expr(
        if rhs_ctx.is_null_free() {
            lhs_not_null
        } else {
            construct_scalar_expr(lhs_not_null, BinaryOperator::And, rhs_not_null)
        },
        BinaryOperator::Or,
        rhs_is_empty,
    );

    Ok(check)
}

fn construct_3vl_case_expr(
    is_not_in: bool,
    lhs_has_null: Expr,
    equal_match: Expr,
    rhs_has_null: Expr,
    rhs_non_empty: Expr,
) -> Expr {
    //   WHEN equal_match                      THEN <NOT IN -> FALSE, IN -> TRUE>
    //   WHEN (lhs_has_null AND rhs_non_empty) THEN NULL
    //   WHEN rhs_has_null                     THEN NULL
    //   ELSE                                  <NOT IN -> TRUE, IN -> FALSE>
    let mut branches = vec![CaseWhenBranch {
        condition: equal_match,
        body: Expr::Literal((!is_not_in).into()),
    }];
    if !matches!(lhs_has_null, Expr::Literal(Literal::Boolean(false))) {
        branches.push(CaseWhenBranch {
            condition: Expr::BinaryOp {
                lhs: Box::new(lhs_has_null),
                op: BinaryOperator::And,
                rhs: Box::new(rhs_non_empty),
            },
            body: Expr::Literal(Literal::Null),
        });
    }
    branches.push(CaseWhenBranch {
        condition: rhs_has_null,
        body: Expr::Literal(Literal::Null),
    });

    Expr::CaseWhen {
        branches,
        else_expr: Some(Box::new(Expr::Literal(is_not_in.into()))),
    }
}

pub(crate) fn add_3vl_for_select_list_in_subquery(
    base_stmt: &mut SelectStatement,
    input: SelectList3vlInput,
    probes: &mut ProbeRegistry,
) -> ReadySetResult<Expr> {
    // Request NP only if RHS can produce NULL; request EP only if LHS can be NULL (CASE branch)
    let need_np = !input.rhs_ctx.is_null_free();
    let need_ep = !input.flags.is_lhs_null_free;

    let info = probes.ensure_for_rhs(
        base_stmt,
        input.preserved_rhs_stmt.clone(),
        &input.rhs_ctx,
        need_np,
        need_ep,
    )?;

    let equal_match = construct_is_not_null_expr(input.rhs, /* IS NULL? */ false);

    // rhs_has_null := NP.present_ IS NOT NULL  (or FALSE when RHS null-free)
    let rhs_has_null = if input.rhs_ctx.is_null_free() {
        Expr::Literal(false.into())
    } else {
        let has_null_present = info
            .has_null_present_col()
            .expect("NP_3VL present_ should exist");
        construct_is_not_null_expr(has_null_present, /* IS NULL? */ false)
    };

    // rhs_non_empty := EP.present_ IS NOT NULL  (or FALSE when not requested)
    let rhs_non_empty = if input.flags.is_lhs_null_free {
        Expr::Literal(false.into())
    } else {
        let ep_present = info
            .non_empty_present_col()
            .expect("EP_3VL present_ should exist");
        construct_is_not_null_expr(ep_present, /* IS NULL? */ false)
    };

    // lhs_has_null := (lhs IS NULL) or FALSE if known non-null
    let lhs_has_null = if input.flags.is_lhs_null_free {
        Expr::Literal(false.into())
    } else {
        construct_is_not_null_expr(input.lhs, /* IS NULL? */ true)
    };

    Ok(construct_3vl_case_expr(
        input.flags.is_not_in,
        lhs_has_null,
        equal_match,
        rhs_has_null,
        rhs_non_empty,
    ))
}
