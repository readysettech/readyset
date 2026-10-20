use std::collections::HashMap;

use criterion::{BatchSize, Bencher, Criterion, black_box, criterion_group, criterion_main};
use readyset_sql::Dialect;
use readyset_sql::ast::{CreateTableBody, Relation, SqlIdentifier};
use readyset_sql_parsing::{
    ParsingPreset, parse_create_table, parse_select, parse_select_with_config,
};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteContext, AdapterRewriteParams};
use readyset_sql_passes::{
    BaseSchemasContext, CanQuery, ImpliedTablesContext, ResolveSchemasContext,
    RewriteDialectContext, StarExpansionContext,
};

fn auto_parameterize_query(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, src: &str| {
        let q = parse_select(Dialect::MySQL, src).unwrap();
        b.iter_batched(
            || q.clone(),
            |mut q| {
                adapter_rewrites::auto_parameterize_query(
                    &mut q,
                    Vec::new(),
                    adapter_rewrites::LiteralSlots::default(),
                    true,
                    false,
                    true,
                )
                .unwrap();
                black_box(q)
            },
            BatchSize::SmallInput,
        )
    };

    c.benchmark_group("auto_parameterize_query")
        .bench_with_input("trivial", "SELECT * FROM t", run_benchmark)
        .bench_with_input(
            "simple",
            "SELECT customer_id, amount, account_name FROM payment WHERE customer_id = 1",
            run_benchmark,
        )
        .bench_with_input(
            "moderate",
            "SELECT * FROM t \
                 WHERE x = 1 \
                 AND ( \
                    y = 4 \
                    AND ( \
                      z = 5 \
                      AND q = 7 \
                      AND (\
                        x = 80 \
                        AND w = ? \
                        AND xx = 43 \
                        AND yz IN (x + 4 - 8))))",
            run_benchmark,
        );
}

/// A schema catalog of `tables` tables in schema `s`, laid out like the adapter's own catalog:
/// base schemas and column lists keyed by relation.
struct Catalog {
    base_schemas: HashMap<Relation, CreateTableBody>,
    view_schemas: HashMap<Relation, Vec<SqlIdentifier>>,
    search_path: Vec<SqlIdentifier>,
}

impl Catalog {
    fn new(tables: usize) -> Self {
        let mut base_schemas = HashMap::new();
        let mut view_schemas = HashMap::new();
        for i in 0..tables {
            let create = parse_create_table(
                Dialect::MySQL,
                format!(
                    "CREATE TABLE s.t{i} (id INT PRIMARY KEY, a INT NOT NULL, b INT, \
                     c VARCHAR(255) NOT NULL, d VARCHAR(255), e INT UNIQUE, ts INT NOT NULL, \
                     f TEXT, c0 INT NOT NULL UNIQUE, c1 JSON, c2 SMALLINT, c3 VARCHAR(255), \
                     c4 INT, c5 INT, c6 DOUBLE, c7 SMALLINT, c8 BIGINT)"
                ),
            )
            .unwrap();
            let body = create.body.unwrap();
            let columns = body.fields.iter().map(|f| f.column.name.clone()).collect();
            view_schemas.insert(create.table.clone(), columns);
            base_schemas.insert(create.table, body);
        }
        Self {
            base_schemas,
            view_schemas,
            search_path: vec!["s".into()],
        }
    }
}

impl AdapterRewriteContext for Catalog {}

impl BaseSchemasContext for Catalog {
    fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
        Box::new(self.base_schemas.iter())
    }

    fn base_schema(&self, relation: &Relation) -> Option<&CreateTableBody> {
        self.base_schemas.get(relation)
    }
}

impl ResolveSchemasContext for Catalog {
    fn add_invalidating_table(&self, _table: Relation) {}

    fn can_query_table(&self, schema: &SqlIdentifier, table: &SqlIdentifier) -> Option<CanQuery> {
        let relation = Relation {
            schema: Some(schema.clone()),
            name: table.clone(),
        };
        self.base_schemas
            .contains_key(&relation)
            .then_some(CanQuery::Yes)
    }

    fn search_path(&self) -> &[SqlIdentifier] {
        &self.search_path
    }

    fn schema_contains_custom_type(&self, _: &SqlIdentifier, _: &SqlIdentifier) -> bool {
        false
    }
}

impl StarExpansionContext for Catalog {
    fn schema_for_relation(
        &self,
        relation: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
        self.view_schemas.get(relation).cloned()
    }

    fn is_relation_non_replicated(&self, _relation: &Relation) -> bool {
        false
    }
}

impl RewriteDialectContext for Catalog {
    fn dialect(&self) -> readyset_data::Dialect {
        Dialect::MySQL.into()
    }
}

impl ImpliedTablesContext for Catalog {
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
        self.view_schemas.clone()
    }
}

/// The full adapter rewrite of reads, from single-table lookups to joins and subqueries, against
/// catalogs of 10 and of 500 tables, to show which per-query costs grow with the catalog. The
/// `dg_` statements are shapes from the general corpus of the adapter benchmark.
fn rewrite_query(c: &mut Criterion) {
    let queries = [
        ("point_lookup", "SELECT t0.a, t0.b FROM t0 WHERE t0.id = ?"),
        (
            "filter",
            "SELECT t0.a, t0.c FROM t0 WHERE t0.a > ? AND t0.b IN (?, ?, ?)",
        ),
        (
            "top_k",
            "SELECT t0.a, t0.ts FROM t0 WHERE t0.a = ? ORDER BY t0.ts DESC LIMIT 10",
        ),
        (
            "join",
            "SELECT t0.a, t1.c FROM t0 JOIN t1 ON t0.b = t1.a WHERE t0.id = ?",
        ),
        (
            "in_subquery",
            "SELECT t0.a FROM t0 WHERE t0.b IN (SELECT t1.a FROM t1 WHERE t1.c = ?)",
        ),
        (
            "exists",
            "SELECT t0.a FROM t0 WHERE EXISTS (SELECT 1 FROM t1 WHERE t1.a = t0.b AND t1.c = ?)",
        ),
        (
            "dg_substring_in_list",
            "SELECT SUBSTRING(`t2`.`c3`, 1, 10) FROM `t2` WHERE `t2`.`c0` IN (?, ?, ?)",
        ),
        (
            "dg_functions_top_1",
            "SELECT SUBSTRING(`t3`.`c3`, 1, 10), MONTH(`t3`.`c2`), ASCII(`t3`.`c3`), `t3`.`c1`, \
             LEAST(`t3`.`c1`, `t3`.`c0`) FROM `t3` \
             WHERE (`t3`.`c3` IN (?, ?, ?) AND (`t3`.`c0` = 1)) \
             ORDER BY `t3`.`c1` ASC, `t3`.`c0` ASC LIMIT 1",
        ),
        (
            "dg_group_by",
            "SELECT `t1`.`c7`, count(`t1`.`c0`), sum(`t1`.`c0`) FROM `t1` GROUP BY `t1`.`c7`",
        ),
        (
            "dg_three_way_join",
            "SELECT `t2`.`c7`, `t6`.`c3`, `t8`.`c3`, DAYOFWEEK(`t2`.`c2`) FROM `t2` \
             INNER JOIN `t6` ON (`t2`.`c0` = `t6`.`c4`) INNER JOIN `t8` ON (`t6`.`c7` = `t8`.`c0`)",
        ),
        (
            "dg_in_and_not_in_subqueries",
            "SELECT CONCAT(`t4`.`c3`, `t4`.`c3`), CAST(`t4`.`c4` as DOUBLE), `t4`.`c4`, \
             `t4`.`c2`, (`t4`.`c0` % `t4`.`c5`), ASCII(`t4`.`c3`) FROM `t4` \
             WHERE (`t4`.`c5` NOT IN (SELECT `t7`.`c1` FROM `t7`) \
             AND `t4`.`c0` IN (SELECT `t0`.`c8` FROM `t0`))",
        ),
    ];
    let mut group = c.benchmark_group("rewrite_query");
    for tables in [10, 500] {
        let catalog = Catalog::new(tables);
        for (name, src) in queries {
            let q = parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, src)
                .unwrap();
            group.bench_function(format!("{name}/{tables}_tables"), |b| {
                b.iter_batched(
                    || q.clone(),
                    |mut q| {
                        let params = adapter_rewrites::rewrite_query(
                            &mut q,
                            AdapterRewriteParams::new(Dialect::MySQL),
                            &catalog,
                        )
                        .unwrap();
                        black_box((q, params))
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(benches, auto_parameterize_query, rewrite_query);
criterion_main!(benches);
