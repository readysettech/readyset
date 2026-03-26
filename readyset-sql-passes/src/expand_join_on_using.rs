//! Desugars SQL `JOIN ... USING(col1, col2, ...)` into an equivalent `JOIN ... ON (...)`
//! by synthesizing an AND-conjunction of equality predicates between the left-hand side (LHS)
//! and right-hand side (RHS) relations for each USING column.
//!
//! Scope & guarantees (predicate-only rewrite):
//! - Preserves row-production semantics for INNER and LEFT joins (our supported join types).
//! - Handles chains of USING over a multi-table LHS *without* forcing wrappers by tracking a
//!   single visible/merged LHS representative per USING column across the join chain.
//! - Works for base tables and derived tables (subqueries) via schema introspection.
//! - Emits clear errors when a USING column is missing on either side, or is ambiguous on the LHS
//!   without having been merged by a prior USING.
//!
//! Out of scope (by design, handled elsewhere):
//! - Projection semantics of `USING` (merged visible columns and their names, esp. for `SELECT *`).
//!   This pass only rewrites the *predicate*; the projection/star expansion pass should run later.
//! - Parenthesized/multi-table RHS groups (`JoinRightSide::Tables`) are not supported yet.
//!
//! Reviewer notes:
//! - The `merged_lhs_repr` map records, per column name, the LHS relation that represents the
//!   merged USING column after a successful `A JOIN B USING (x)`. A subsequent `... JOIN C USING (x)`
//!   will then build `rep.x = C.x` rather than erroring due to multiple matches in the LHS.
//! - If there was *no* prior USING merge and multiple LHS items expose the same column, we surface
//!   an ambiguity error (aligned with PostgreSQL).
use crate::rewrite_utils::{and_predicates, construct_scalar_expr, get_from_item_reference_name};
use crate::{star_expansion::StarExpansionContext, util};
use readyset_errors::{ReadySetError, ReadySetResult, invalid_query, unsupported};
use readyset_sql::DialectDisplay;
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_select_statement};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, JoinConstraint, JoinRightSide, Relation, SelectStatement,
    SqlIdentifier, TableExpr, TableExprInner,
};
use std::collections::HashMap;

/// Applies the USING→ON desugaring to this statement and all immediate subqueries in `FROM`.
///
/// This pass is *predicate-only*: it does not change the projection/visible columns. It should
/// run before star/projection expansion that implements `USING`'s merged-column visibility rules.
///
/// Errors if a USING column is absent on either side, or if the LHS is ambiguous for that column
/// without a prior USING merge that picks a representative.
pub trait ExpandJoinOnUsing: Sized {
    fn expand_join_on_using<C: StarExpansionContext>(
        &mut self,
        context: C,
    ) -> ReadySetResult<&mut Self>;
}

impl ExpandJoinOnUsing for SelectStatement {
    fn expand_join_on_using<C: StarExpansionContext>(
        &mut self,
        context: C,
    ) -> ReadySetResult<&mut Self> {
        ExpandUsingVisitor { context: &context }.visit_select_statement(self)?;
        Ok(self)
    }
}

/// Visitor that recursively expands `JOIN ... USING` → `JOIN ... ON` in every
/// `SelectStatement` it encounters.  The default `walk_select_statement` handles
/// recursion into all subquery positions (FROM, WHERE, HAVING, SELECT, JOIN ON,
/// IN, ARRAY, etc.) automatically.
struct ExpandUsingVisitor<'a, C> {
    context: &'a C,
}

impl<'ast, C: StarExpansionContext> VisitorMut<'ast> for ExpandUsingVisitor<'_, C> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Recurse into nested subqueries first (bottom-up), so schema introspection
        // on derived tables sees already-expanded inner joins.
        walk_select_statement(self, select_statement)?;
        // Then expand this statement's own USING clauses.
        rewrite_using_to_on_predicates(select_statement, self.context)?;
        Ok(())
    }
}

/// Cache key for retrieving field/column names:
/// - `Alias(_)` for derived tables (subqueries with alias)
/// - `Relation(_)` for base tables
///
/// We key by alias for subqueries since their visible schema can be renamed/reshaped.
#[derive(Hash, Eq, PartialEq, Clone)]
enum FieldKey {
    Alias(SqlIdentifier),
    Relation(Relation),
}

/// Memoizes schema lookups for `FROM` items during a single pass.
///
/// Avoids repeated `schema_for_relation` and derived-table schema walks while expanding multiple
/// USING columns across a join chain.
struct FieldNamesCache<'a, C: StarExpansionContext> {
    cache: HashMap<FieldKey, Vec<SqlIdentifier>>,
    subquery_schemas: &'a HashMap<SqlIdentifier, Vec<SqlIdentifier>>,
    context: &'a C,
}

impl<'a, C: StarExpansionContext> FieldNamesCache<'a, C> {
    fn new(
        context: &'a C,
        subquery_schemas: &'a HashMap<SqlIdentifier, Vec<SqlIdentifier>>,
    ) -> Self {
        Self {
            cache: HashMap::new(),
            context,
            subquery_schemas,
        }
    }

    /// Return (and memoize) the visible column names for a `FROM` item, keyed either by its alias
    /// or by its base relation depending on the table kind.
    ///
    /// Note: We intentionally reject unsupported `FROM` items up front (e.g., subqueries without alias).
    fn get_or_load(&mut self, from_item: &TableExpr) -> ReadySetResult<&Vec<SqlIdentifier>> {
        let key = match from_item {
            TableExpr {
                inner: TableExprInner::Subquery(_),
                alias: Some(alias),
                ..
            } => FieldKey::Alias(alias.clone()),
            TableExpr {
                inner: TableExprInner::Values { .. },
                alias: Some(alias),
                ..
            } => FieldKey::Alias(alias.clone()),
            TableExpr {
                inner: TableExprInner::Table(table),
                ..
            } => FieldKey::Relation(table.clone()),
            _ => {
                unsupported!(
                    "Could not derive field names for {}",
                    from_item.display(self.context.dialect().into())
                )
            }
        };
        if !self.cache.contains_key(&key) {
            self.cache
                .insert(key.clone(), self.get_from_item_field_names(&key)?);
        }
        Ok(self.cache.get(&key).unwrap())
    }

    fn get_from_item_field_names(
        &self,
        field_key: &FieldKey,
    ) -> ReadySetResult<Vec<SqlIdentifier>> {
        Ok(match field_key {
            FieldKey::Alias(alias) => {
                if let Some(field_names) = self.subquery_schemas.get(alias) {
                    field_names.clone()
                } else {
                    unsupported!(
                        "Could not find field names for derived table {} ",
                        alias.as_str()
                    );
                }
            }
            FieldKey::Relation(r) => {
                if let Some(columns) = self
                    .context
                    .schema_for_relation(r)
                    .map(|cols| cols.into_iter().collect::<Vec<_>>())
                {
                    columns
                } else if let Some(field_names) = self.subquery_schemas.get(&r.name) {
                    // CTE references appear as plain table names but their schemas
                    // are registered in subquery_schemas by name.
                    field_names.clone()
                } else {
                    unsupported!(
                        "Could not find column names for table {} ",
                        r.display_unquoted()
                    );
                }
            }
        })
    }
}

/// Core implementation: rewrites all `JOIN ... USING(...)` in `stmt` into equivalent `JOIN ... ON(...)`.
///
/// Algorithm sketch:
/// 1) Collect visible schemas for base/derived tables into `subquery_schemas`.
/// 2) Walk the join list left-to-right, maintaining:
///    - `lhs_tables`: the sequence of `FROM` items visible on the LHS up to current join,
///    - `merged_lhs_repr`: map `col_name → LHS representative relation` for columns merged by prior USING.
/// 3) For each `USING(col)`:
///    - Ensure RHS exposes `col` (error if not).
///    - Prefer an existing LHS representative from `merged_lhs_repr`; otherwise, search `lhs_tables`:
///      * 0 matches ⇒ error (missing on LHS)
///      * 1 match  ⇒ use it
///      * >1 matches ⇒ error (ambiguous LHS; not merged by prior USING)
///    - Synthesize `lhs.col = rhs.col` and AND-accumulate across multiple USING columns.
///    - Record/refresh `merged_lhs_repr[col] = chosen_lhs_relation` for subsequent joins.
///
/// Limitations:
/// - Does not handle `JoinRightSide::Tables` (parenthesized multi-item RHS).
/// - Does not implement `USING` projection rules; another pass should handle visible columns.
fn rewrite_using_to_on_predicates<C: StarExpansionContext>(
    stmt: &mut SelectStatement,
    context: &C,
) -> ReadySetResult<()> {
    if stmt.tables.is_empty() || stmt.join.is_empty() {
        return Ok(());
    }

    // Visible schema for derived tables (subqueries), used to resolve USING column names by alias.
    let subquery_schemas: HashMap<SqlIdentifier, Vec<SqlIdentifier>> = util::subquery_schemas(
        &mut stmt.tables,
        &mut stmt.ctes,
        &mut stmt.join,
        context.dialect().into(),
    )?
    .into_iter()
    .map(|(k, v)| (k.clone(), v.into_iter().cloned().collect()))
    .collect();

    // Memoize field name lookups per visible from-item (by alias or base relation)
    let mut field_names_cache = FieldNamesCache::<C>::new(context, &subquery_schemas);

    // Tracks merged visible columns from prior USING joins:
    //   column name -> representative LHS relation (the relation whose column name represents the merged column)
    // Example:
    //   FROM a JOIN b USING (x)    => merged_lhs_repr["x"] = a   (or b, depending on policy)
    //   ... JOIN c USING (x)       => we build  a.x = c.x   (not ambiguous), then refresh the representative.
    // If there was no prior USING merge and multiple LHS items expose "x", we error out as ambiguous.
    let mut merged_lhs_representatives: HashMap<SqlIdentifier, Relation> = HashMap::new();

    let mut lhs_tables = stmt.tables.iter().collect::<Vec<_>>();

    for jc in stmt.join.iter_mut() {
        let rhs_table = match &jc.right {
            JoinRightSide::Table(tab) => tab,
            JoinRightSide::Tables(_) => {
                unsupported!("JoinRightSide::Tables not yet implemented")
            }
        };

        let on_expr = if let JoinConstraint::Using(using_columns) = &jc.constraint {
            if using_columns.is_empty() {
                invalid_query!("Empty USING clause");
            }

            let mut acc_expr = None;
            // Resolve LHS/RHS sources for this USING column, preferring a prior LHS representative when present.
            for using_col in using_columns {
                let mut rhs_rel: Option<Relation> = None;
                let mut lhs_rel: Option<Relation> = None;

                // Prefer a previously merged LHS representative for this column
                if let Some(rep) = merged_lhs_representatives.get(&using_col.name) {
                    lhs_rel = Some(rep.clone());
                } else {
                    // Otherwise, search the current LHS tables for matches
                    for lhs_tab in lhs_tables.iter() {
                        if field_names_cache
                            .get_or_load(lhs_tab)?
                            .contains(&using_col.name)
                        {
                            if lhs_rel.is_some() {
                                invalid_query!(
                                    "USING column {} is ambiguous on the left (multiple sources; no prior USING merge)",
                                    using_col.name
                                );
                            }
                            lhs_rel = Some(get_from_item_reference_name(lhs_tab)?);
                        }
                    }
                }

                // RHS must expose the USING column
                if field_names_cache
                    .get_or_load(rhs_table)?
                    .contains(&using_col.name)
                {
                    rhs_rel = Some(get_from_item_reference_name(rhs_table)?);
                }

                // Defensive: either side missing the USING column is a user query error (aligns with PG).
                if rhs_rel.is_none() || lhs_rel.is_none() {
                    invalid_query!(
                        "USING column {} not present on both sides of the join",
                        using_col.name
                    );
                }

                // Build `lhs.col = rhs.col` and chain with previous USING columns via AND.
                acc_expr = and_predicates(
                    acc_expr,
                    construct_scalar_expr(
                        Expr::Column(Column {
                            name: using_col.name.clone(),
                            table: lhs_rel.clone(),
                        }),
                        BinaryOperator::Equal,
                        Expr::Column(Column {
                            name: using_col.name.clone(),
                            table: rhs_rel.clone(),
                        }),
                    ),
                );

                // Record/refresh merged representative for subsequent USING joins
                merged_lhs_representatives.insert(using_col.name.clone(), lhs_rel.unwrap());
                // Subsequent joins that use USING(col) will reuse this representative, avoiding spurious LHS ambiguity.
            }
            if acc_expr.is_none() {
                invalid_query!("Columns referenced in USING not present in both tables");
            }
            acc_expr
        } else {
            None
        };

        if let Some(on_expr) = on_expr {
            // Replace USING(...) with the synthesized ON(...) predicate(s).
            jc.constraint = JoinConstraint::On(on_expr);
        }

        lhs_tables.push(rhs_table);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RewriteDialectContext;
    use readyset_data::Dialect as DataDialect;
    use readyset_sql::Dialect;
    use readyset_sql::ast::Relation;
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    struct TestJoinOnUsingContext {
        schema: HashMap<Relation, Vec<SqlIdentifier>>,
    }

    impl RewriteDialectContext for TestJoinOnUsingContext {
        fn dialect(&self) -> DataDialect {
            DataDialect::DEFAULT_POSTGRESQL
        }
    }

    impl StarExpansionContext for TestJoinOnUsingContext {
        fn schema_for_relation(
            &self,
            relation: &Relation,
        ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
            self.schema.get(relation).cloned()
        }

        fn is_relation_non_replicated(&self, _relation: &Relation) -> bool {
            false
        }
    }

    fn spj_schema() -> HashMap<Relation, Vec<SqlIdentifier>> {
        HashMap::from([
            (
                "s".into(),
                vec![
                    "sn".into(),
                    "sname".into(),
                    "status".into(),
                    "city".into(),
                    "pn".into(),
                    "jn".into(),
                ],
            ),
            (
                "p".into(),
                vec![
                    "pn".into(),
                    "pname".into(),
                    "color".into(),
                    "weight".into(),
                    "city".into(),
                    "sn".into(),
                    "jn".into(),
                ],
            ),
            (
                "j".into(),
                vec![
                    "jn".into(),
                    "jname".into(),
                    "city".into(),
                    "sn".into(),
                    "pn".into(),
                ],
            ),
            (
                "spj".into(),
                vec!["sn".into(), "pn".into(), "jn".into(), "qty".into()],
            ),
        ])
    }

    const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        let rewritten_stmt =
            match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, original_text) {
                Ok(mut stmt) => {
                    let context = TestJoinOnUsingContext {
                        schema: spj_schema(),
                    };
                    match stmt.expand_join_on_using(context) {
                        Ok(_) => {
                            println!(
                                ">>>>>> Desugared USING: {}",
                                stmt.display(Dialect::PostgreSQL)
                            );
                            Ok(stmt.clone())
                        }
                        Err(err) => Err(err),
                    }
                }
                Err(e) => panic!("> {test_name}: ORIGINAL STATEMENT PARSE ERROR: {e}"),
            };

        match rewritten_stmt {
            Ok(expect_stmt) => {
                assert_eq!(
                    expect_stmt,
                    match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, expect_text)
                    {
                        Ok(stmt) => stmt,
                        Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                    }
                );
            }
            Err(e) => {
                println!("> {test_name}: REWRITE ERROR: {e}");
                assert!(expect_text.is_empty(), "> {test_name}: REWRITE ERROR: {e}")
            }
        }
    }

    #[test]
    fn test1() {
        let original =
            r#"select spj.qty, s.pn from spj join s using (sn, jn) where s.city = 'LONDON'"#;

        let expected = r#"SELECT "spj"."qty", "s"."pn" FROM
        "spj" JOIN "s" ON (("spj"."sn" = "s"."sn") AND ("spj"."jn" = "s"."jn"))
        WHERE ("s"."city" = 'LONDON')"#;

        test_it("test1", original, expected);
    }

    #[test]
    fn test2() {
        let original = r#"
select
    spj.qty,
    t1.jn
from
    (
        select
            j.jn,
            p.sn
        from
            p
            join j using (pn, sn)
        where
            p.weight = 17
            and j.sn = 'S33333'
    ) as t1 join spj using (jn)
where
    t1.sn = 'S33333';"#;

        let expected = r#"SELECT "spj"."qty", "t1"."jn" FROM
        (SELECT "j"."jn", "p"."sn" FROM "p" JOIN "j" ON (("p"."pn" = "j"."pn") AND ("p"."sn" = "j"."sn"))
        WHERE (("p"."weight" = 17) AND ("j"."sn" = 'S33333'))) AS "t1" JOIN "spj" ON ("t1"."jn" = "spj"."jn")
        WHERE ("t1"."sn" = 'S33333')"#;

        test_it("test2", original, expected);
    }

    #[test]
    fn test3() {
        let original = r#"
select
    spj.qty,
    t1.jn,
    p.weight
from
    (
        select
            j.jn,
            p.sn
        from
            p
            join j using (pn, sn)
        where
            p.weight = 17
            and j.sn = 'S33333'
    ) as t1 join spj using (jn)
    join p on t1.sn = p.sn
where
    t1.sn = 'S33333';"#;

        let expected = r#"SELECT "spj"."qty", "t1"."jn", "p"."weight" FROM
        (SELECT "j"."jn", "p"."sn" FROM "p" JOIN "j" ON (("p"."pn" = "j"."pn") AND ("p"."sn" = "j"."sn"))
        WHERE (("p"."weight" = 17) AND ("j"."sn" = 'S33333'))) AS "t1" JOIN "spj" ON ("t1"."jn" = "spj"."jn")
        JOIN "p" ON ("t1"."sn" = "p"."sn") WHERE ("t1"."sn" = 'S33333')"#;

        test_it("test3", original, expected);
    }

    #[test]
    fn test4() {
        let original = r#"select spj.qty, s.pn from s join spj using (sn, jn) join p using(city) where s.city = 'LONDON'"#;

        let expected = r#"SELECT "spj"."qty", "s"."pn" FROM "s" JOIN "spj" ON (("s"."sn" = "spj"."sn") AND ("s"."jn" = "spj"."jn"))
        JOIN "p" ON ("s"."city" = "p"."city") WHERE ("s"."city" = 'LONDON')"#;

        test_it("test4", original, expected);
    }

    // Negative test: column `qty` is present only in `spj` table
    #[test]
    fn test5() {
        let original = r#"select spj.qty, s.pn from s join spj using (sn, qty) join p using(city) where s.city = 'LONDON'"#;

        // Expect error
        let expected = r#""#;

        test_it("test5", original, expected);
    }

    // Chained USING over multi-table LHS should succeed without wrapping
    #[test]
    fn test6() {
        let original = r#"SELECT s.sname FROM s JOIN spj USING (sn) JOIN p USING (sn)"#;

        // Representative ON uses the merged LHS column (leftmost representative: "s"."sn")
        let expected = r#"SELECT "s"."sname" FROM "s" JOIN "spj" ON ("s"."sn" = "spj"."sn") JOIN "p" ON ("s"."sn" = "p"."sn")"#;

        test_it("test6", original, expected);
    }

    // True ambiguity (no prior USING merge): should error
    #[test]
    fn test7() {
        // Here both "s" and "spj" expose column "jn" on LHS, but they were NOT merged by USING(jn)
        // prior to joining "j" USING(jn); this should be ambiguous.
        let original = r#"SELECT 1 FROM s JOIN spj ON (s.sn = spj.sn) JOIN j USING (jn)"#;

        let expected = r#""#; // expect rewrite error

        test_it("test7", original, expected);
    }

    // VALUES clause with USING should resolve column aliases
    #[test]
    fn test_values_using() {
        let original = r#"
            SELECT c.product_category, s.sname
            FROM (VALUES ('London'), ('Paris')) AS c(city)
            JOIN s USING (city)
        "#;

        let expected = r#"
            SELECT "c"."product_category", "s"."sname"
            FROM (VALUES ('London'), ('Paris')) AS "c"("city")
            JOIN "s" ON ("c"."city" = "s"."city")
        "#;

        test_it("test_values_using", original, expected);
    }

    // Subquery with explicit column aliases should use those for USING resolution
    #[test]
    fn test_subquery_column_aliases_using() {
        let original = r#"
            SELECT t.sn, s.sname
            FROM (SELECT spj.sn, spj.qty FROM spj WHERE spj.qty > 100) AS t(sn, amount)
            JOIN s USING (sn)
        "#;

        let expected = r#"
            SELECT "t"."sn", "s"."sname"
            FROM (SELECT "spj"."sn", "spj"."qty" FROM "spj" WHERE ("spj"."qty" > 100)) AS "t"("sn", "amount")
            JOIN "s" ON ("t"."sn" = "s"."sn")
        "#;

        test_it("test_subquery_column_aliases_using", original, expected);
    }

    // Subquery column alias renames a field; USING should match the alias, not the original name
    #[test]
    fn test_subquery_column_alias_rename_using() {
        let original = r#"
            SELECT t.city, s.sname
            FROM (SELECT p.city FROM p WHERE p.weight = 17) AS t(city)
            JOIN s USING (city)
        "#;

        let expected = r#"
            SELECT "t"."city", "s"."sname"
            FROM (SELECT "p"."city" FROM "p" WHERE ("p"."weight" = 17)) AS "t"("city")
            JOIN "s" ON ("t"."city" = "s"."city")
        "#;

        test_it(
            "test_subquery_column_alias_rename_using",
            original,
            expected,
        );
    }

    // VALUES with multiple columns and USING
    #[test]
    fn test_values_multi_column_using() {
        let original = r#"
            SELECT v.sn, s.sname
            FROM (VALUES ('S1', 10), ('S2', 20)) AS v(sn, qty)
            JOIN s USING (sn)
        "#;

        let expected = r#"
            SELECT "v"."sn", "s"."sname"
            FROM (VALUES ('S1', 10), ('S2', 20)) AS "v"("sn", "qty")
            JOIN "s" ON ("v"."sn" = "s"."sn")
        "#;

        test_it("test_values_multi_column_using", original, expected);
    }

    // CTE referenced as a plain table in JOIN ... USING should resolve columns
    #[test]
    fn test_cte_using() {
        let original = r#"
            WITH filtered AS (SELECT s.sn, s.city FROM s WHERE s.status > 10)
            SELECT p.pname, filtered.city
            FROM p
            JOIN filtered USING (sn)
        "#;

        let expected = r#"
            WITH "filtered" AS (SELECT "s"."sn", "s"."city" FROM "s" WHERE ("s"."status" > 10))
            SELECT "p"."pname", "filtered"."city"
            FROM "p"
            JOIN "filtered" ON ("p"."sn" = "filtered"."sn")
        "#;

        test_it("test_cte_using", original, expected);
    }

    // CTE + VALUES with USING
    #[test]
    fn test_cte_values_using() {
        let original = r#"
            WITH category_filter AS (SELECT DISTINCT s.city FROM s WHERE s.status > 10)
            SELECT c.city, CASE WHEN f.city IS NULL THEN 0 ELSE 1 END AS available
            FROM (VALUES ('London'), ('Paris'), ('Athens')) AS c(city)
            LEFT JOIN category_filter f USING (city)
        "#;

        let expected = r#"
            WITH "category_filter" AS (SELECT DISTINCT "s"."city" FROM "s" WHERE ("s"."status" > 10))
            SELECT "c"."city", CASE WHEN ("f"."city" IS NULL) THEN 0 ELSE 1 END AS "available"
            FROM (VALUES ('London'), ('Paris'), ('Athens')) AS "c"("city")
            LEFT JOIN "category_filter" AS "f" ON ("c"."city" = "f"."city")
        "#;

        test_it("test_cte_values_using", original, expected);
    }
}
