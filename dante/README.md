# dante — constraint-based SQL pattern generator

`dante` produces random SQL queries from a registry of declarative
**patterns**. A pattern is a set of `Constraint`s over shared variables;
the resolver binds variables to concrete tables, columns, and parameters
and emits a `ResolverOutput { query, ddl, params, examples }`. The
[`readyset-dante-oracle`](../readyset-dante-oracle/) harness consumes the
output and diffs Readyset against an upstream reference engine.

This document is the **pattern-authoring guide**. If you are running the
oracle (and not writing patterns), read
[`public/readyset-dante-oracle/README.md`](../readyset-dante-oracle/README.md).

## Contents

1. [Mental model](#mental-model)
2. [Where patterns live](#where-patterns-live)
3. [Authoring a new pattern](#authoring-a-new-pattern)
4. [`PatternBuilder` reference](#patternbuilder-reference)
5. [Constraint catalogue](#constraint-catalogue)
6. [Type classes](#type-classes)
7. [Dialect support](#dialect-support)
8. [Tags and the selection filter](#tags-and-the-selection-filter)
9. [Example pinning (`Constraint::Example`)](#example-pinning-constraintexample)
10. [Composition](#composition)
11. [Compatibility rules](#compatibility-rules)
12. [Testing patterns locally](#testing-patterns-locally)
13. [Running a pattern end-to-end](#running-a-pattern-end-to-end)
14. [Best practices](#best-practices)

## Mental model

```
PatternBuilder              ConstraintRegistry
   |                                |
   v                                v
Pattern  --register-->  Generator  --pick+compose-->  Recipe
                          |                              |
                          v                              v
                       (rules)                      try_resolve
                                                         |
                                                         v
                                                ResolverOutput
                                              { query, ddl, params,
                                                examples }
                                                         |
                                                         v
                                              QueryOutput (dialect-filtered)
```

Key types:

- **`VarId`** — a numeric handle for a logical entity (relation, column,
  parameter, SQL type) that the resolver will bind to a concrete value.
- **`VarKind`** — what a `VarId` means at allocation time: `Relation`,
  `DerivedRelation`, `Column { table }`, `SqlType`, or
  `Param { col }` (placeholder bound to the type of `col`).
- **`Constraint`** — a single declarative assertion about the query
  (e.g. `From(t)`, `ProjectColumn { col, table }`,
  `WhereParam { col, table, op, param }`,
  `Example { note, dialect, cells }`).
- **`Pattern`** — a named bundle of constraints plus metadata (tags,
  weight, dialect support).
- **`Recipe`** — what the resolver actually consumes. Either one
  pattern's constraints with a 0 offset, or the result of composing
  several patterns.
- **`Generator`** — owns the `ConstraintRegistry`, picks a pattern per
  iteration, composes it, runs `try_resolve`, and produces
  `QueryOutput`. The harness wraps `Generator` and adds DDL / row / probe
  execution.

## Where patterns live

```
public/dante/src/registry/
  advanced.rs       windows, three-table joins, multi-feature
  aggregates.rs     COUNT/SUM/AVG/HAVING and friends
  basic.rs          SELECT col FROM t, single-parameter
  compound.rs       UNION ALL / UNION DISTINCT shapes
  ctes.rs           WITH ... AS (...) SELECT
  examples.rs       example-pinned probes (Constraint::Example)
  expressions.rs    expression-evaluation patterns (tagged expr_eval)
  filters.rs        WHERE-only shapes
  functions.rs      scalar function calls
  hoisting.rs       HAVING-on-group-key hoisting cases
  joins.rs          JOIN shapes
  ordering.rs       ORDER BY / LIMIT / OFFSET
  subqueries.rs     scalar / EXISTS / IN subqueries
```

New patterns either extend an existing file or get a new module added to
`registry/mod.rs` and registered in `ConstraintRegistry::default_registry`.

## Authoring a new pattern

Each pattern is a `pub fn name() -> Pattern` that builds and returns the
pattern from a `PatternBuilder`. The recipe is:

```rust
use readyset_sql::ast::BinaryOperator;

use crate::constraint::TypeClass;
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c1, t.c2 FROM t WHERE t.c1 = ?
pub fn single_parameter_two_cols() -> Pattern {
    let mut b = PatternBuilder::new("single_parameter_two_cols");

    // 1. Allocate vars.
    let t  = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);

    // 2. Constrain types (optional but usually helpful).
    b.column_type_class(c1, TypeClass::Integer);

    // 3. Structure.
    b.from(t);
    b.project_column(c1, t);
    b.project_column(c2, t);
    let _param = b.where_param(c1, t, BinaryOperator::Equal);

    // 4. Metadata: tags drive the SelectionFilter; weight tunes pick
    //    probability; dialect_support gates entirely-unsupported dialects.
    b.tags(&["filter", "parameter"]);

    b.build()
}
```

Then register it in `ConstraintRegistry::default_registry` (see
`public/dante/src/generator.rs`):

```rust
reg.register(basic::single_parameter_two_cols());
```

The `b.where_param` helper returns the `VarId` of the placeholder. Keep
that handle if you intend to address the placeholder later from a
`Constraint::Example`.

## `PatternBuilder` reference

Defined in `src/pattern.rs`. Grouped by purpose:

### Variable allocation

| Method | Purpose |
|---|---|
| `b.table() -> VarId` | Allocate a fresh relation var. Most patterns start here. |
| `b.alias_of(orig) -> VarId` | Same physical table, different SQL alias (for self-joins). |
| `b.column(table) -> VarId` | Allocate a fresh column var bound to `table`. |
| `b.sql_type() -> VarId` | Allocate a `SqlType` var (rare; used by polymorphic functions). |

### Type pinning

| Method | Purpose |
|---|---|
| `b.column_type_class(c, TypeClass::...)` | Constrain `c` to a type class. |
| `b.type_compatible(a, b)` | Force two columns to share a compatible type. |
| `b.eq(a, b)` / `b.not_eq(a, b)` | Force vars to (not) resolve to the same value. |

### Structure

| Method | Purpose |
|---|---|
| `b.from(t)` | `FROM t`. |
| `b.join_table(...)` | Add a JOIN clause. |
| `b.project_column(c, t)` | `SELECT t.c`. |
| `b.project_aggregate(fn, c, t)` | `SELECT COUNT(t.c)` etc. |
| `b.project_function(fn, args)` | Scalar function in SELECT. |
| `b.project_binary_op(left, op, right)` | `SELECT lt.lc op rt.rc`. |
| `b.project_cast(c, t, target_ty)` | `SELECT CAST(t.c AS target_ty)`. |
| `b.project_literal(LiteralKind::...)` | `SELECT 1`, `'foo'`, `NULL`, ... |
| `b.group_by(c, t)` | `GROUP BY t.c`. |
| `b.having(fn, c, t, op) -> VarId` | `HAVING <agg> op ?`; returns the param var. |
| `b.having_key_filter(c, t, op) -> VarId` | `HAVING <group-by col> op ?`; hoistable. |
| `b.order_by(...)` | `ORDER BY ...`. |
| `b.limit(limit, offset)` | `LIMIT n [OFFSET k]`. |
| `b.distinct()` | `SELECT DISTINCT`. |
| `b.window_function(...)` | Window function in SELECT. |
| `b.subquery() -> SubqueryBuilder` | Nested SELECT scope; commit as WHERE/FROM/JOIN/CTE. |
| `b.compound_select(op, branches)` | `UNION` / `UNION ALL` / etc. |

### Filters (all parameter-emitting helpers return the placeholder `VarId`)

| Method | Emits | Returns |
|---|---|---|
| `b.where_param(c, t, op)` | `WHERE t.c op ?` | `param: VarId` |
| `b.where_in_param(c, t, n)` | `WHERE t.c IN (?, ?, ...)` | `Vec<VarId>` (one per slot) |
| `b.where_range_param(c, t)` | `WHERE t.c >= ? AND t.c <= ?` | `(lo, hi)` |
| `b.where_between_param(c, t)` | `WHERE t.c BETWEEN ? AND ?` | `(lo, hi)` |
| `b.where_like(c, t, negated)` | `WHERE t.c [NOT] LIKE ?` | `param: VarId` |
| `b.where_is_null(c, t, negated)` | `WHERE t.c IS [NOT] NULL` | — |
| `b.where_column_compare(left, op, right)` | `WHERE l op r` (column vs column) | — |
| `b.where_lookup_binary_op(lookup, cmp, left, op, right)` | `WHERE lookup_col cmp (left op right)` | — |

### Metadata

| Method | Purpose |
|---|---|
| `b.tags(&["t1", "t2"])` | Static tag list. |
| `b.set_weight(w)` | Pick weight (> 0). |
| `b.set_dialect_support(DialectSupport::...)` | Gate by MySQL / PG / both. |
| `b.example(note, dialect, cells)` | Attach a `Constraint::Example`; see below. |

`b.build()` finalises and returns the `Pattern`.

## Constraint catalogue

The full enum lives in `src/constraint.rs`. The variants you reach most
often via `PatternBuilder`:

- **Schema-level:** `BaseTable`, `AliasOf`, `ColumnExists`,
  `ColumnTypeClass`, `TypeCompatible`.
- **Equality:** `Eq`, `NotEq`.
- **Structural:** `From`, `Join`, `ProjectColumn`, `ProjectAggregate`,
  `ProjectFunction`, `ProjectBinaryOp`, `ProjectCast`, `ProjectLiteral`,
  `GroupBy`, `Having`, `HavingKeyFilter`, `OrderBy`, `Limit`, `Distinct`,
  `WindowFunction`, `CompoundSelect`.
- **Filters with placeholders:** `WhereParam`, `WhereInParam`,
  `WhereRangeParam`, `WhereLike`, `WhereBetweenParam`,
  `WhereLookupBinaryOp`. Every placeholder-emitting variant carries a
  `param` (or `params`/`lo`/`hi`) `VarId` of kind `VarKind::Param { col
  }` so `Constraint::Example` cells can address the placeholder.
- **Filters without placeholders:** `WhereIsNull`,
  `WhereColumnCompare`.
- **Subqueries:** `SubqueryExpr`, `SubqueryRelation`.
- **Metadata / probes:** `Example { note, dialect, cells }`.

If you need a SQL shape that no current variant covers, add a new
variant. Mechanical follow-ups: update `var_ids`, `map_var_ids`, the
`forbids_compound_compose` match in `compat.rs`, and the resolver's
`ast_builder` arm.

## Type classes

`TypeClass` lets you constrain a column to a family rather than an exact
SQL type. The resolver picks a concrete type from the class, taking the
running dialect into account.

| Class | Concrete types it draws from |
|---|---|
| `Any` | Anything the synthesiser supports. |
| `Integer` | `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT` (signed and unsigned where dialect supports). |
| `Numeric` | Integer types plus `DECIMAL(p, s)`, `FLOAT`, `REAL`, `DOUBLE`, `NUMERIC`. |
| `String` | `VARCHAR`, `TEXT`, `CHAR` and dialect-specific text. |
| `DateTime` | `DATE`, `DATETIME`/`TIMESTAMP`, `TIME`. |
| `Exact(SqlType)` | The exact type you name. |

When you want the precise type — e.g. `SqlType::Decimal(10, 4)` for a
lookup-key coercion test — use `Exact`. When the bug class spans a
family — e.g. any integer divided by any integer — use the broad class.

## Dialect support

Each pattern has a `DialectSupport` that gates whether it even *enters*
the random pick:

```rust
b.set_dialect_support(DialectSupport::MySqlOnly);   // only when dialect == MySQL
b.set_dialect_support(DialectSupport::PostgresOnly); // only when dialect == PostgreSQL
b.set_dialect_support(DialectSupport::Both);        // default
```

`DialectSupport` also gates **individual examples** independently of the
pattern's gate — a `Both` pattern can carry an `MySqlOnly` example that
only runs against MySQL.

Mark patterns whose SQL is dialect-specific (e.g. MySQL `GROUP_CONCAT`,
PG `ARRAY_AGG`). The harness filters them out of the wrong-dialect run
rather than emitting unsupported SQL.

## Tags and the selection filter

Tags are `&'static str`s that the harness's `--required-tags` /
`--excluded-tags` flags filter against. They are not a fixed vocabulary;
add new tags freely. Common conventions in the current tree:

- **Layer:** `base`, `filter`, `aggregate`, `cte`, `compound`, `join`,
  `subquery`.
- **Dialect:** `mysql_only`, `postgres_only`.
- **Feature class:** `parameter`, `literal`, `string`, `numeric`,
  `datetime`, `window`, `distinct`.
- **Expression evaluation:** `expr_eval` and sub-tags (`binary_op`,
  `arithmetic`, `cast`, `mixed_type`, `int_int`, `lookup_key`,
  `float`, `precision`).
- **Example:** `example` whenever the pattern carries
  `Constraint::Example` cells.

Browse current tags:

```bash
grep -rh 'b\.tags(' public/dante/src/registry/ | sort -u
```

`SelectionFilter::matches` requires *all* `required_tags` to be present
and *none* of the `excluded_tags`.

## Example pinning (`Constraint::Example`)

When a bug class only surfaces for specific inputs (e.g. `8 / 3` for the
int-int divide coercion), attach an example to the pattern so the
oracle can probe deterministically:

```rust
use crate::constraint::{DialectSupport, ExampleCell, ExampleValue};

let c1 = b.column(t);
let c2 = b.column(t);
b.column_type_class(c1, TypeClass::Integer);
b.column_type_class(c2, TypeClass::Integer);
b.from(t);
b.project_column(c1, t);
b.project_column(c2, t);
let param = b.where_param(c1, t, BinaryOperator::Equal);

b.example(
    "int/int divide: c1=8, c2=3, param=2",
    DialectSupport::MySqlOnly,
    vec![
        ExampleCell { var: c1,    value: ExampleValue::Literal("8") },
        ExampleCell { var: c2,    value: ExampleValue::Literal("3") },
        ExampleCell { var: param, value: ExampleValue::Literal("2") },
    ],
);

b.tags(&["expr_eval", "binary_op", "int_int", "example"]);
```

Rules:

- `ExampleCell.var` must be a `VarKind::Column { .. }` (row override
  at INSERT) or `VarKind::Param { .. }` (param override at SELECT).
  Any other kind aborts at registration via `validate_examples`.
- `ExampleValue::Literal(&'static str)` is the SQL text as it would
  appear inline (`"'2025-01-01'"`, `"8"`, `"NULL"`). The dialect-shaped
  literal is the author's responsibility.
- A pattern can carry many examples; each becomes its own SELECT probe
  with its own row insertion (in addition to the random-fill probe).
  Each example's `note` ends up in any divergence log line it triggers.

See `src/registry/examples.rs` for the canonical example-pinned pattern
(`int_int_divide_bait`) and `src/registry/expressions.rs` for
`where_lookup_decimal_eq_int_div_int`, which carries three examples to
cover RS-only, MySQL-only, and both-match rows.

## Composition

By default the generator composes patterns to make queries deeper.
`Pattern::compose(other)` glues two patterns into one `Recipe` by:

- merging their var ranges with a fresh offset,
- attempting to unify their primary tables (so the inner pattern's
  filter sits on the outer pattern's table),
- concatenating constraints.

A pattern that should never be composed (e.g. a complete `UNION ALL`)
sets `is_compound()` to true via `Constraint::CompoundSelect`; the
generator skips composition for those.

When you are debugging a single pattern, run the oracle with
`--single-pattern` — composition partners frequently introduce `self_join`
or unsupported placeholder positions that block `CREATE DEEP CACHE` and
hide bugs you are trying to surface.

## Compatibility rules

`ConstraintRegistry::add_rule` lets you declare global rejection rules:

- `TagConflict(a, b)` — reject when both tags are present together.
- `TagPresent(t)` — reject any pattern that has tag `t`.
- `MaxOccurrences(t, n)` — reject when the composed tag list has more
  than `n` instances of `t`.
- `Custom(fn(&[Constraint]) -> bool)` — arbitrary predicate; returns
  `true` for incompatible.

Rules are checked after composition and reject the composed recipe; the
generator retries up to `MAX_RETRIES` times before giving up.

## Testing patterns locally

Run the dante test suite (lib + crate-integration tests):

```bash
cd public/
cargo nextest run -p dante
```

Doctests:

```bash
cargo test --doc -p dante
```

**Never use bare `cargo test`** for the non-doc suite in this repo. Some
developers configure local `cargo nt` / `cargo t` aliases that wrap
`nextest` with Readyset's test environment; those are personal
`~/.cargo/config.toml` aliases, not repo-provided commands.

A pattern should have at least one test in its module's `#[cfg(test)]
mod tests` block that asserts:

- the pattern builds without panic,
- it resolves to a SQL string containing the syntactic markers you
  expect (`assert!(sql.contains(" + "))` and similar), and
- example pinning, if any, produces the expected number of resolved
  examples (use the `Generator::generate_with_ddl` path and inspect
  `out.examples`).

For SQL-level invariants that span many shapes (e.g. an op-coercion
property), drive many generated shapes through `readyset-dante-oracle`
against both engines and inspect the divergence report.

## Running a pattern end-to-end

Once the pattern resolves and tests pass, exercise it against a live
Readyset using the oracle harness. The targeted-testing combo is:

```bash
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://root:noria@127.0.0.1:3306/cfuzz \
  --readyset-url mysql://root:noria@127.0.0.1:3307/cfuzz \
  --seed 42 -n 50 \
  --required-tags <your-pattern-tag> \
  --single-pattern
```

For complete setup (fresh database, Readyset start, dialect-specific
recipes, repro capture, multi-seed orchestration), see
[`public/readyset-dante-oracle/README.md`](../readyset-dante-oracle/README.md).

## Best practices

- **Allocate vars before constraining them.** Build the var graph
  first, then layer constraints over it. Mixing the two makes
  composition harder to reason about.
- **Use the broad `TypeClass` when the bug class is broad.** Only pin
  to `Exact(SqlType)` when the divergence is intrinsic to that exact
  type (e.g. `DECIMAL(10, 4)` for lookup-key coercion).
- **Keep param `VarId`s.** Always bind the return value of
  `b.where_param` / `b.where_like` / `b.having` etc. when you might
  add an example later — it is the only way to address that
  placeholder from `ExampleCell`.
- **Tag thoroughly.** A pattern with vague tags is invisible to
  `--required-tags`. Layer tags (one for the SQL shape, one for the
  bug class, one for the dialect) so multiple filter combinations can
  find it.
- **Pin examples for known bugs.** Random fill rarely lands on the
  exact `8 / 3` shape that surfaces the int-int divide bug.
  `Constraint::Example` is the deterministic probe; use it whenever
  the bug class has a specific witness.
- **Mark dialect support honestly.** `MySqlOnly`/`PostgresOnly` keeps
  the wrong-dialect harness run silent instead of producing
  unsupported-SQL noise.
- **Tests for new constraint variants.** When you add a new
  `Constraint` variant, also extend `var_ids`, `map_var_ids`, the
  `compat::forbids_compound_compose` match, and the AST-builder arm in
  one atomic commit. Type-class-and-test discipline catches the
  cascade.
- **Don't reach for `_` matches on `Constraint`.** Exhaustive `match`
  is the codebase convention — it forces an explicit decision every
  time a variant is added.
- **Doc each pattern with the SQL it emits.** A one-line doc comment
  saying `SELECT t.c1 + t.c2 FROM t` makes the registry browsable. The
  `expressions.rs` patterns are a good model.
