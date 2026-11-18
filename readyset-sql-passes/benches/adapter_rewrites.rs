//! Benchmark the times of each rewrite pass.
//!
//! Below are variations of running the benchmark on various subsets of queries and rewrite passes.
//!
//! ```text
//! # Each pass of rewrite_query, for each query:
//! cargo bench -p readyset-sql-passes
//!
//! # Each pass of rewrite_equivalent_deep, for each query:
//! cargo bench -p readyset-sql-passes -- rewrite_equivalent_deep
//!
//! # The pass number_placeholders, in both rewrite_equivalent_deep and rewrite_for_readyset:
//! cargo bench -p readyset-sql-passes -- number_placeholders
//!
//! # The pass number_placeholders, in rewrite_for_readyset only:
//! cargo bench -p readyset-sql-passes -- rewrite_for_readyset/number_placeholders
//!
//! # Each pass of rewrite_query, for the query in_subquery:
//! cargo bench -p readyset-sql-passes -- in_subquery
//!
//! # The pass number_placeholders in rewrite_for_readyset, for the query in_subquery:
//! cargo bench -p readyset-sql-passes -- rewrite_for_readyset/number_placeholders/in_subquery
//! ```

use std::collections::HashMap;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, black_box, criterion_group, criterion_main,
};
use readyset_sql::Dialect;
use readyset_sql::ast::{CreateTableBody, Relation, SqlIdentifier};
use readyset_sql_parsing::{ParsingPreset, parse_create_table, parse_select_with_config};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteContext, AdapterRewriteParams};
use readyset_sql_passes::{
    BaseSchemasContext, CanQuery, ImpliedTablesContext, ResolveSchemasContext,
    RewriteDialectContext, StarExpansionContext, pass_timing,
};

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
        let relation = Relation::new(Some(schema.clone()), table.clone());
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

const QUERIES: &[(&str, &str)] = &[
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

fn rewrite_equivalent_deep(c: &mut Criterion) {
    let catalog = Catalog::new(500);
    let flags = AdapterRewriteParams::new(Dialect::MySQL);
    let mut group = c.benchmark_group("rewrite_equivalent_deep");
    for &(name, src) in QUERIES {
        let q =
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, src).unwrap();
        bench_passes(
            &mut group,
            name,
            || q.clone(),
            |mut q| {
                let params =
                    adapter_rewrites::rewrite_equivalent_deep(&mut q, flags, &catalog).unwrap();
                (q, params)
            },
        );
    }
    group.finish();
}

fn rewrite_for_readyset(c: &mut Criterion) {
    let catalog = Catalog::new(500);
    let flags = AdapterRewriteParams::new(Dialect::MySQL);
    let mut group = c.benchmark_group("rewrite_for_readyset");
    for &(name, src) in QUERIES {
        let mut q =
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, src).unwrap();
        let params = adapter_rewrites::rewrite_equivalent_deep(&mut q, flags, &catalog).unwrap();
        bench_passes(
            &mut group,
            name,
            || (q.clone(), params.clone()),
            |(mut q, params)| {
                let params = adapter_rewrites::rewrite_for_readyset(&mut q, flags, params).unwrap();
                (q, params)
            },
        );
    }
    group.finish();
}

fn bench_passes<I, O>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    query: &str,
    setup: impl Fn() -> I,
    rewrite: impl Fn(I) -> O,
) {
    let record = |input| {
        pass_timing::start();
        let output = rewrite(input);
        (pass_timing::drain(), output)
    };
    let (passes, _) = record(setup());
    for (pass, _) in passes {
        group.bench_function(BenchmarkId::new(pass, query), |b| {
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| {
                        let (passes, output) = record(setup());
                        black_box(output);
                        passes.into_iter().find(|&(p, _)| p == pass).unwrap().1
                    })
                    .sum()
            })
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = rewrite_equivalent_deep, rewrite_for_readyset
}
criterion_main!(benches);
