//! Differential equivalence tests for the deep-cache rewrite pipeline.
//!
//! The pipeline's contract is that the query it emits answers the same as the one the user
//! wrote.  Nothing else checks that: the validators either side of the rewrites establish
//! well-formedness, not equivalence, so a pass that changes cardinality produces valid SQL with
//! the wrong rows and no test fails.  Here both statements run against the upstream and their
//! result multisets are compared, which is the only check that can see such a difference.
//!
//! Cases belong here when they carry something a rewrite could mishandle -- a NULL on the inner
//! side of `NOT IN`, a filter that matches nothing, an aggregate with no rows to aggregate,
//! duplicates across a join.  A query with tidy data proves very little; the fixtures below are
//! built to be awkward.
//!
//! Requires a Postgres reachable through `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`, defaulting to
//! the `docker-compose.yml` configuration.

use std::collections::HashMap;
use std::env;

use readyset_sql::ast::{
    CreateTableBody, Expr, JoinConstraint, JoinOperator, Literal, Relation, SelectStatement,
    SqlIdentifier, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config, parse_select_with_config};
use test_utils::{tags, upstream};
use tokio_postgres::NoTls;

use crate::adapter_rewrites::{
    AdapterRewriteContext, AdapterRewriteParams, rewrite_equivalent_deep,
};
use crate::resolve_schemas::ResolveSchemasContext;
use crate::star_expansion::StarExpansionContext;
use crate::{BaseSchemasContext, CanQuery, ImpliedTablesContext, RewriteDialectContext};

const SCHEMA: &str = "rw_equiv";
const PARSING: ParsingPreset = ParsingPreset::OnlySqlparser;

/// Fixtures chosen for what they can break, not for what they represent: `b.k` and `a.k` carry
/// NULLs so `IN`/`NOT IN` exercise three-valued logic, both tables carry duplicate keys so joins
/// fan out, and `empty` is never populated so aggregates run over no rows at all.
const DDL: &[(&str, &str)] = &[
    ("a", "CREATE TABLE rw_equiv.a (k int, v int, g int, s text)"),
    ("b", "CREATE TABLE rw_equiv.b (k int, w int, t text)"),
    ("empty", "CREATE TABLE rw_equiv.empty (k int, z int)"),
    (
        "keyed",
        "CREATE TABLE rw_equiv.keyed (id int PRIMARY KEY, k int NOT NULL, label text)",
    ),
];

const FIXTURES: &[&str] = &[
    "INSERT INTO rw_equiv.a VALUES (1,10,1,'x'),(1,20,1,'y'),(2,NULL,1,'x'),(3,30,2,NULL),(NULL,40,2,'z')",
    "INSERT INTO rw_equiv.b VALUES (1,100,'p'),(1,101,'p'),(NULL,102,'q'),(4,103,NULL)",
    "INSERT INTO rw_equiv.keyed VALUES (1,1,'one'),(2,2,'two'),(3,4,'four')",
];

/// Cases the pipeline is expected to refuse.  A case outside this list that stops rewriting is
/// the pipeline losing a shape it used to handle; a case inside it that starts rewriting is a
/// rewrite appearing where none is correct.  Both fail.
const DECLINED: &[&str] = &["subquery_projected_beside_ungrouped_aggregate"];

/// Every case runs twice: as written, and as the pipeline rewrites it.  Both must answer the
/// same.
const CASES: &[(&str, &str)] = &[
    (
        "not_in_with_null_inner",
        "SELECT a.k FROM rw_equiv.a WHERE a.k NOT IN (SELECT b.k FROM rw_equiv.b)",
    ),
    (
        "not_in_over_empty",
        "SELECT a.g, COUNT(*) AS n FROM rw_equiv.a \
         WHERE a.k NOT IN (SELECT empty.k FROM rw_equiv.empty) GROUP BY a.g",
    ),
    (
        "in_with_null_inner",
        "SELECT a.k FROM rw_equiv.a WHERE a.k IN (SELECT b.k FROM rw_equiv.b)",
    ),
    (
        "in_with_group_by_having",
        "SELECT a.k FROM rw_equiv.a WHERE a.k IN \
         (SELECT b.k FROM rw_equiv.b GROUP BY b.k HAVING COUNT(*) > 1)",
    ),
    (
        "exists_over_empty",
        "SELECT a.k FROM rw_equiv.a WHERE EXISTS \
         (SELECT 1 FROM rw_equiv.empty WHERE empty.k = a.k)",
    ),
    (
        "exists_with_having_no_group_by",
        "SELECT a.k FROM rw_equiv.a WHERE EXISTS \
         (SELECT 1 FROM rw_equiv.b WHERE b.k = a.k HAVING COUNT(*) > 1)",
    ),
    (
        "correlated_count_no_match",
        "SELECT a.k, (SELECT COUNT(*) FROM rw_equiv.b WHERE b.k = a.k) AS n FROM rw_equiv.a",
    ),
    (
        "correlated_scalar_with_limit",
        "SELECT a.k, (SELECT b.w FROM rw_equiv.b WHERE b.k = a.k ORDER BY b.w LIMIT 1) AS f \
         FROM rw_equiv.a",
    ),
    (
        "ungrouped_aggregate_filter_matches_nothing",
        "SELECT a.k, s.n FROM rw_equiv.a CROSS JOIN \
         (SELECT COUNT(*) AS n FROM rw_equiv.b WHERE b.k = 99) s",
    ),
    // Declined today, and named in `DECLINED` so the loop expects that.  It earns its place as a
    // tripwire: the shape has no correct rewrite through a join under the aggregate, so a later
    // change that starts emitting one fails this test rather than quietly passing.
    (
        "subquery_projected_beside_ungrouped_aggregate",
        "SELECT COUNT(*) AS n, (SELECT COUNT(*) FROM rw_equiv.b) AS c \
         FROM rw_equiv.a WHERE a.s = 'nope'",
    ),
    (
        "grouped_subquery_filter_hoistable",
        "SELECT a.k, s.n FROM rw_equiv.a JOIN \
         (SELECT COUNT(*) AS n, b.k FROM rw_equiv.b WHERE b.w = 100 GROUP BY b.k) s ON a.k = s.k",
    ),
    (
        "left_join_predicate_on_nullable_side",
        "SELECT a.k, d.w FROM rw_equiv.a LEFT JOIN \
         (SELECT b.k, b.w FROM rw_equiv.b WHERE b.w > 100) d ON a.k = d.k",
    ),
    (
        "anti_join_idiom",
        "SELECT a.k, a.v FROM rw_equiv.a LEFT JOIN rw_equiv.b ON a.k = b.k WHERE b.w IS NULL",
    ),
    (
        "distinct_over_fan_out",
        "SELECT DISTINCT a.g FROM rw_equiv.a JOIN rw_equiv.b ON a.k = b.k",
    ),
    (
        "derived_table_with_limit_joined",
        "SELECT d.k, b.w FROM (SELECT a.k FROM rw_equiv.a ORDER BY a.k LIMIT 2) d \
         JOIN rw_equiv.b ON d.k = b.k",
    ),
    (
        "having_with_uncorrelated_subquery",
        "SELECT a.g, SUM(a.v) AS s FROM rw_equiv.a GROUP BY a.g \
         HAVING SUM(a.v) > (SELECT COUNT(*) FROM rw_equiv.b)",
    ),
    (
        "group_by_positional_with_correlated_subquery",
        "SELECT a.k, a.g, (SELECT COUNT(*) FROM rw_equiv.b WHERE b.k = a.k) AS c \
         FROM rw_equiv.a GROUP BY 1, 2",
    ),
    (
        "group_by_alias_with_correlated_subquery",
        "SELECT a.k AS kk, (SELECT COUNT(*) FROM rw_equiv.b WHERE b.k = a.k) AS c \
         FROM rw_equiv.a GROUP BY kk",
    ),
    (
        "self_join_on_primary_key",
        "SELECT k1.label FROM rw_equiv.keyed k1 JOIN rw_equiv.keyed k2 ON k1.id = k2.id",
    ),
    (
        "nested_derived_tables",
        "SELECT x.k FROM (SELECT y.k FROM (SELECT a.k FROM rw_equiv.a WHERE a.v > 10) y) x",
    ),
    (
        "scalar_subquery_in_or",
        "SELECT a.k FROM rw_equiv.a WHERE a.v > (SELECT AVG(b.w) FROM rw_equiv.b) OR a.g = 2",
    ),
];

/// Supplies the real `CREATE TABLE` bodies, so passes that consult keys and nullability take the
/// same decisions here as they do against a live upstream.
struct SchemaContext {
    dialect: Dialect,
    schemas: HashMap<Relation, CreateTableBody>,
}

impl SchemaContext {
    fn new(dialect: Dialect) -> Self {
        let schemas = DDL
            .iter()
            .map(|(name, ddl)| {
                let body = match parse_query_with_config(PARSING, dialect, *ddl) {
                    Ok(readyset_sql::ast::SqlQuery::CreateTable(stmt)) => {
                        stmt.body.expect("fixture DDL has a body")
                    }
                    other => panic!("fixture DDL for {name} does not parse: {other:?}"),
                };
                (
                    Relation {
                        schema: Some(SCHEMA.into()),
                        name: (*name).into(),
                    },
                    body,
                )
            })
            .collect();
        Self { dialect, schemas }
    }
}

impl AdapterRewriteContext for SchemaContext {}

impl RewriteDialectContext for SchemaContext {
    fn dialect(&self) -> readyset_data::Dialect {
        self.dialect.into()
    }
}

impl ResolveSchemasContext for SchemaContext {
    fn add_invalidating_table(&self, _table: Relation) {}

    fn can_query_table(&self, schema: &SqlIdentifier, table: &SqlIdentifier) -> Option<CanQuery> {
        self.schemas
            .keys()
            .any(|r| r.schema.as_ref() == Some(schema) && r.name == *table)
            .then_some(CanQuery::Yes)
    }

    fn search_path(&self) -> &[SqlIdentifier] {
        &[]
    }

    fn schema_contains_custom_type(&self, _schema: &SqlIdentifier, _ty: &SqlIdentifier) -> bool {
        false
    }
}

impl StarExpansionContext for SchemaContext {
    fn schema_for_relation(
        &self,
        relation: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
        self.schemas
            .iter()
            .find(|(r, _)| {
                r.name == relation.name
                    && (relation.schema.is_none() || r.schema == relation.schema)
            })
            .map(|(_, body)| {
                body.fields
                    .iter()
                    .map(|f| f.column.name.clone())
                    .collect::<Vec<_>>()
            })
    }

    fn is_relation_non_replicated(&self, _relation: &Relation) -> bool {
        false
    }
}

impl ImpliedTablesContext for SchemaContext {
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
        self.schemas
            .iter()
            .map(|(rel, body)| {
                (
                    rel.clone(),
                    body.fields.iter().map(|f| f.column.name.clone()).collect(),
                )
            })
            .collect::<Vec<_>>()
    }
}

impl BaseSchemasContext for SchemaContext {
    fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
        Box::new(self.schemas.iter())
    }

    fn base_schema(&self, relation: &Relation) -> Option<&CreateTableBody> {
        self.schemas.get(relation)
    }
}

fn rewrite_params(dialect: Dialect) -> AdapterRewriteParams {
    AdapterRewriteParams {
        dialect,
        server_supports_topk: false,
        server_supports_pagination: false,
        server_supports_mixed_comparisons: false,
        // Literals stay literal, so the emitted statement runs as-is.  Auto-parameterization
        // happens after the passes under test and does not affect them.
        autoparameterize: false,
    }
}

/// Decorrelation joins a derived table with nothing to match on, which the AST records as an
/// absent constraint and prints as an absent `ON`.  Readyset consumes the AST directly; an
/// upstream needs the constraint spelled out -- except after `CROSS JOIN`, which does not take
/// one.
fn spell_out_empty_join_constraints(stmt: &mut SelectStatement) {
    for join in stmt.join.iter_mut() {
        if matches!(join.constraint, JoinConstraint::Empty)
            && join.operator != JoinOperator::CrossJoin
        {
            join.constraint = JoinConstraint::On(Expr::Literal(Literal::Boolean(true)));
        }
        if let readyset_sql::ast::JoinRightSide::Table(t) = &mut join.right
            && let TableExprInner::Subquery(sub) = &mut t.inner
        {
            spell_out_empty_join_constraints(sub);
        }
    }
    for table in stmt.tables.iter_mut() {
        if let TableExprInner::Subquery(sub) = &mut table.inner {
            spell_out_empty_join_constraints(sub);
        }
    }
}

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, String> {
    // Render every row through the server so comparison is insensitive to column types and to
    // the labels the two statements give their outputs.
    let wrapped = format!("SELECT ROW(q.*)::text FROM ({sql}) q");
    let result = client.query(wrapped.as_str(), &[]).await;
    match result {
        Ok(rows) => {
            let mut out: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            out.sort();
            Ok(out)
        }
        Err(e) => Err(e.to_string()),
    }
}

#[tags(serial)]
#[upstream(postgres)]
#[tokio::test]
async fn rewrites_answer_the_same_as_the_query_they_replace() {
    let mut config = tokio_postgres::Config::new();
    config
        .host(env::var("PGHOST").as_deref().unwrap_or("localhost"))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .expect("PGPORT must be a port number"),
        )
        .user(env::var("PGUSER").as_deref().unwrap_or("postgres"))
        .password(env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()))
        .dbname(env::var("PGDATABASE").as_deref().unwrap_or("postgres"));

    let (client, connection) = config.connect(NoTls).await.expect("upstream is reachable");
    tokio::spawn(connection);

    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA};"
        ))
        .await
        .expect("fixture schema is creatable");
    for (_, ddl) in DDL {
        client
            .batch_execute(ddl)
            .await
            .expect("fixture DDL applies");
    }
    for insert in FIXTURES {
        client
            .batch_execute(insert)
            .await
            .expect("fixture rows insert");
    }

    let mut failures = Vec::new();
    for (name, sql) in CASES {
        let mut rewritten = parse_select_with_config(PARSING, Dialect::PostgreSQL, *sql)
            .unwrap_or_else(|e| panic!("{name} does not parse: {e}"));
        if let Err(e) = rewrite_equivalent_deep(
            &mut rewritten,
            rewrite_params(Dialect::PostgreSQL),
            SchemaContext::new(Dialect::PostgreSQL),
        ) {
            // Each case below pins one hazard, so a refusal is the pipeline losing a shape it
            // used to handle -- the regression this test exists to catch -- unless the case is
            // one the pipeline is meant to refuse.  A generated query the pipeline declines
            // would be a coverage question instead, and belongs to whatever feeds those in.
            if !DECLINED.contains(name) {
                failures.push(format!("{name}: no longer rewritten ({e})\n  query: {sql}"));
            }
            continue;
        }
        if DECLINED.contains(name) {
            failures.push(format!(
                "{name}: expected to be declined, but the pipeline rewrote it\n  query: {sql}"
            ));
            continue;
        }
        spell_out_empty_join_constraints(&mut rewritten);
        let rewritten_sql = rewritten.display(Dialect::PostgreSQL).to_string();

        match (
            rows(&client, sql).await,
            rows(&client, &rewritten_sql).await,
        ) {
            (Ok(original), Ok(emitted)) if original == emitted => {}
            (Ok(original), Ok(emitted)) => failures.push(format!(
                "{name}: {} rows before the rewrite, {} after\n  query:    {sql}\n  \
                 rewritten: {rewritten_sql}\n  only before: {:?}\n  only after:  {:?}",
                original.len(),
                emitted.len(),
                original
                    .iter()
                    .filter(|r| !emitted.contains(r))
                    .collect::<Vec<_>>(),
                emitted
                    .iter()
                    .filter(|r| !original.contains(r))
                    .collect::<Vec<_>>(),
            )),
            (Err(e), _) => failures.push(format!("{name}: the case itself does not run: {e}")),
            (_, Err(e)) => failures.push(format!(
                "{name}: the rewritten query does not run: {e}\n  rewritten: {rewritten_sql}"
            )),
        }
    }

    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        .await
        .ok();

    assert!(
        failures.is_empty(),
        "the pipeline changed what {} of {} queries answer:\n\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n\n")
    );
}
