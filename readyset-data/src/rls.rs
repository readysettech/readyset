use crate::DfValue;
use itertools::Either;
use readyset_errors::{
    internal, internal_err, invalid_query, unsupported, unsupported_err, ReadySetError,
    ReadySetResult,
};
use readyset_sql::analysis::visit;
use readyset_sql::analysis::visit::Visitor;
use readyset_sql::analysis::visit_mut::{walk_select_statement, VisitorMut};
use readyset_sql::ast::Expr::BinaryOp;
use readyset_sql::ast::Literal::Placeholder;
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, ItemPlaceholder, JoinRightSide, Literal, Relation,
    SelectStatement, SqlIdentifier,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, iter, mem};

#[derive(Serialize, Deserialize, Debug)]
pub struct RlsPersistentState {
    rls_repo: String,
    rls_applied_queries: String,
    cached_queries: String,
}

impl RlsPersistentState {
    pub fn get() -> ReadySetResult<RlsPersistentState> {
        macro_rules! serialize {
            ($guard:expr) => {
                serde_json::to_string(&(*$guard)).map_err(|e| internal_err!("{e}"))
            };
        }
        match (
            RLS_REPO.read_guard(),
            RLS_APPLIED_QUERIES.read_guard(),
            CACHED_QUERIES_REPO.read_guard(),
        ) {
            (Ok(rls_repo_guard), Ok(rls_applied_queries_guard), Ok(cached_queries_guard)) => {
                Ok(RlsPersistentState {
                    rls_repo: serialize!(rls_repo_guard)?,
                    rls_applied_queries: serialize!(rls_applied_queries_guard)?,
                    cached_queries: serialize!(cached_queries_guard)?,
                })
            }
            _ => internal!("Internal lock is poisoned"),
        }
    }

    pub fn apply(&self) -> ReadySetResult<()> {
        macro_rules! deserialize {
            ($guard:expr, $src:expr) => {
                mem::replace(
                    &mut (*$guard),
                    match serde_json::from_slice($src.as_bytes()) {
                        Ok(v) => v,
                        Err(e) => internal!("{e}"),
                    },
                )
            };
        }
        match (
            RLS_REPO.write_guard(),
            RLS_APPLIED_QUERIES.write_guard(),
            CACHED_QUERIES_REPO.write_guard(),
        ) {
            (
                Ok(mut rls_repo_guard),
                Ok(mut rls_applied_queries_guard),
                Ok(mut cached_queries_guard),
            ) => {
                let _ = deserialize!(rls_repo_guard, self.rls_repo);
                let _ = deserialize!(rls_applied_queries_guard, self.rls_applied_queries);
                let _ = deserialize!(cached_queries_guard, self.cached_queries);
                Ok(())
            }
            _ => internal!("Internal lock is poisoned"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SharedRepoElement<T>
where
    T: DeserializeOwned + Serialize,
{
    #[serde(serialize_with = "serialize_hashmap")]
    #[serde(deserialize_with = "deserialize_hashmap")]
    map: HashMap<Relation, T>,
}

fn serialize_hashmap<S, V>(map: &HashMap<Relation, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    let converted: HashMap<String, &V> = map
        .iter()
        .map(|(k, v)| (k.display_unquoted().to_string(), v))
        .collect();
    converted.serialize(serializer)
}

fn deserialize_hashmap<'de, D, V>(deserializer: D) -> Result<HashMap<Relation, V>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    fn from_str(s: String) -> ReadySetResult<Relation> {
        let parts: Vec<&str> = s.split(".").collect();
        if parts.is_empty() {
            invalid_query!("Invalid serialized Relation: {s}")
        }
        Ok(if parts.len() == 1 {
            Relation {
                name: parts[0].into(),
                schema: None,
            }
        } else {
            Relation {
                name: parts[1].into(),
                schema: Some(parts[0].into()),
            }
        })
    }

    let string_map: HashMap<String, V> = HashMap::deserialize(deserializer)?;
    string_map
        .into_iter()
        .map(|(k, v)| Ok((from_str(k).map_err(serde::de::Error::custom)?, v)))
        .collect()
}

struct SharedRepo<T>
where
    T: DeserializeOwned + Serialize,
{
    inner: LazyLock<Arc<RwLock<SharedRepoElement<T>>>>,
}

impl<T: Serialize + for<'a> Deserialize<'a>> SharedRepo<T> {
    const fn new() -> Self {
        Self {
            inner: LazyLock::new(|| {
                Arc::new(RwLock::new(SharedRepoElement {
                    map: HashMap::new(),
                }))
            }),
        }
    }

    fn write_guard(
        &'static self,
    ) -> ReadySetResult<RwLockWriteGuard<'static, SharedRepoElement<T>>> {
        self.inner.write().map_err(|e| internal_err!("{e}"))
    }

    fn read_guard(&'static self) -> ReadySetResult<RwLockReadGuard<'static, SharedRepoElement<T>>> {
        self.inner.read().map_err(|e| internal_err!("{e}"))
    }
}

// RLS rule type.
// The inner tuple is a semantic equivalent of (col = var)
type RLSRule = Vec<(SqlIdentifier, SqlIdentifier)>;

/*
* Table which contains the RLS rules.
*/
static RLS_REPO: SharedRepo<RLSRule> = SharedRepo::new();

/// Add RLS rule on `table`
/// Overwrite previously added policy or error out, depending on `if_not_exists` flag.
/// Return `true` if the new content is distinct from existing, `false` otherwise
///
pub fn create_rls(
    table: Relation,
    policy: Vec<(Column, SqlIdentifier)>,
    if_not_exists: bool,
) -> ReadySetResult<bool> {
    let mut guard = RLS_REPO.write_guard()?;
    if if_not_exists && guard.map.contains_key(&table) {
        invalid_query!("RLS already defined on table {}", table.display_unquoted())
    } else if let Some(rls_rule) = guard.map.insert(
        table,
        policy
            .iter()
            .map(|(col, var)| (col.name.clone(), var.clone()))
            .collect(),
    ) {
        Ok(!policy
            .iter()
            .zip(rls_rule)
            .all(|((col1, var1), (col2, var2))| col1.name.eq(&col2) && var1.eq(&var2)))
    } else {
        Ok(true)
    }
}

/// Drop RLS policy on `Some(table)`, or all existing RLS policies if `None`
/// Synch up with `RLS applied` repository
pub fn drop_rls(table: Option<Relation>) -> ReadySetResult<Option<Vec<Relation>>> {
    if let Some(table) = table {
        Ok(if RLS_REPO.write_guard()?.map.remove(&table).is_some() {
            drop_rls_applied_queries_for_table(table)?
        } else {
            None
        })
    } else {
        RLS_REPO.write_guard()?.map.clear();
        let queries = get_all_rls_applied_queries()?;
        drop_all_rls_applied_queries();
        Ok(queries)
    }
}

pub fn get_rls(table: Option<Relation>) -> ReadySetResult<Option<Vec<Vec<DfValue>>>> {
    fn rls_rule_to_string(value: &RLSRule) -> String {
        let mut col_text: String = String::new();
        let mut var_text: String = String::new();
        value.iter().all(|(col, var)| {
            if !col_text.is_empty() {
                col_text.push_str(", ");
            }
            col_text.push_str(col.as_str());
            if !var_text.is_empty() {
                var_text.push_str(", ");
            }
            var_text.push_str(var.as_str());
            true
        });
        format!("({}) = ({})", col_text, var_text).to_string()
    }

    macro_rules! rls_policy_row {
        ($table:expr, $rls_rule:expr) => {
            vec![
                $table.display_unquoted().to_string().as_str().into(),
                $rls_rule.into(),
            ]
        };
    }

    let guard = RLS_REPO.read_guard()?;
    if let Some(table) = table {
        if let Some(rls_rule) = guard.map.get(&table) {
            Ok(Some(vec![rls_policy_row!(
                table,
                rls_rule_to_string(rls_rule)
            )]))
        } else {
            Ok(None)
        }
    } else {
        let mut rows: Vec<Vec<DfValue>> = Vec::with_capacity(guard.map.len());
        for (table, rls_rule) in guard.map.iter() {
            rows.push(rls_policy_row!(table, rls_rule_to_string(rls_rule)));
        }
        Ok(Some(rows))
    }
}

// Return RLS policy associated with `table` or None
fn get_rls_for_table(table: &Relation) -> Option<RLSRule> {
    if let Ok(guard) = RLS_REPO.read_guard() {
        guard.map.get(table).cloned()
    } else {
        None
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct CachedQueryInfo {
    tables: Option<HashSet<Relation>>,
    resolved: bool,
    status: ReadySetResult<()>,
}

impl CachedQueryInfo {
    fn new() -> Self {
        CachedQueryInfo {
            tables: None,
            resolved: false,
            status: Ok(()),
        }
    }

    fn is_valid(&self) -> bool {
        self.resolved && self.status.is_ok()
    }

    fn contains(&self, table: &Relation) -> bool {
        if let Some(tables) = &self.tables {
            tables.contains(table)
        } else {
            false
        }
    }
}

static CACHED_QUERIES_REPO: SharedRepo<CachedQueryInfo> = SharedRepo::new();

pub fn add_cached_query<F>(
    cached_name: &Relation,
    stmt: SelectStatement,
    schema_resolver: F,
) -> ReadySetResult<bool>
where
    F: Fn(SelectStatement) -> ReadySetResult<SelectStatement>,
{
    let mut qi = CachedQueryInfo::new();
    match schema_resolver(stmt) {
        Ok(resolved_stmt) => {
            qi.resolved = true;
            qi.tables = Some(get_referenced_tables(&resolved_stmt));
            if contains_self_join(&resolved_stmt) {
                qi.status = Err(unsupported_err!("RLS not yet supported for self-joins"))
            }
        }
        Err(e) => {
            qi.status = Err(e);
        }
    }

    match CACHED_QUERIES_REPO
        .write_guard()?
        .map
        .insert(cached_name.clone(), qi)
    {
        None => Ok(true),
        Some(_) => internal!(
            "Cached query {} already registered;",
            cached_name.display_unquoted(),
        ),
    }
}

pub fn drop_cached_query(cached_name: &Relation) -> ReadySetResult<bool> {
    drop_rls_applied_query(cached_name)?;
    Ok(CACHED_QUERIES_REPO
        .write_guard()?
        .map
        .remove(cached_name)
        .is_some())
}

pub fn get_cached_queries_referencing_table(table: &Relation) -> ReadySetResult<HashSet<Relation>> {
    Ok(CACHED_QUERIES_REPO
        .read_guard()?
        .map
        .iter()
        .filter_map(|(cached_name, qi)| {
            if qi.contains(table) {
                print!(" {} ", cached_name.display_unquoted());
                Some(cached_name.clone())
            } else {
                None
            }
        })
        .collect())
}

fn get_referenced_tables(stmt: &SelectStatement) -> HashSet<Relation> {
    struct TableVisitor {
        tables: HashSet<Relation>,
    }

    impl<'ast> Visitor<'ast> for TableVisitor {
        type Error = std::convert::Infallible;
        fn visit_table(&mut self, table: &'ast Relation) -> Result<(), Self::Error> {
            self.tables.insert(table.clone());
            Ok(())
        }
    }

    let mut vis = TableVisitor {
        tables: HashSet::new(),
    };
    vis.visit_select_statement(stmt).expect("Just checked");

    vis.tables
}

fn contains_self_join(stmt: &SelectStatement) -> bool {
    struct TableVisitor {
        contains_self_joins: bool,
    }

    impl<'ast> Visitor<'ast> for TableVisitor {
        type Error = std::convert::Infallible;

        fn visit_select_statement(
            &mut self,
            select_statement: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            let mut distinct_tables: HashSet<Relation> = HashSet::new();
            if !select_statement
                .tables
                .iter()
                .chain(
                    select_statement
                        .join
                        .iter()
                        .flat_map(|join| match &join.right {
                            JoinRightSide::Table(table) => Either::Left(iter::once(table)),
                            JoinRightSide::Tables(tables) => Either::Right(tables.iter()),
                        }),
                )
                .all(|t| {
                    if let Some(table) = t.inner.as_table() {
                        distinct_tables.insert(table.clone())
                    } else {
                        true
                    }
                })
            {
                self.contains_self_joins = true;
            }
            visit::walk_select_statement(self, select_statement)
        }
    }

    let mut vis = TableVisitor {
        contains_self_joins: false,
    };
    vis.visit_select_statement(stmt).expect("Just checked");
    vis.contains_self_joins
}

// Contains RLS managed queries mapped against the RLS tables and session variables,
// which have been injected into the original query as extra placeholders.
// During upquery execution, the values of these variables should be added to the auto-parameters.
static RLS_APPLIED_QUERIES: SharedRepo<(Vec<Relation>, Vec<SqlIdentifier>)> = SharedRepo::new();

// Takes statement `stmt`, and applies the existing RLS rules to the outermost tables.
// Returns `true` if the statement was RLS managed, and `false` otherwise.
// Note, it errors out if any RLS enabled tables are referenced anywhere but the outermost tables.
// It will add the RLS rules in the form of explicitly parametrized eq predicates, where placeholder
// indexes start right after the maximum placeholder in the original input statement, assuming that
// adapter-rewrite::rewrite() logic has been already executed against the input statement,
// and the existing placeholders are contiguous. So that we just continue with increasing
// the placeholder indexes.
pub fn try_apply_rls(cached_name: Relation, stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    // Counts the maximum placeholder index, asserting that all placeholders ended up being contiguous.
    let placeholders_count = number_placeholders(stmt)?;

    // Walk over the input statement, visiting the tables referenced at in the FROM or JOIN clauses.
    let mut rls_apply = RLSApplyVisitor::new(placeholders_count + 1);
    rls_apply.visit_select_statement(stmt)?;

    // If we did not add any new placeholders, then no RLS enabled tables found.
    let rls_placeholders_count = rls_apply.get_rls_placeholders_count();
    if rls_placeholders_count == 0 {
        Ok(false)
    } else {
        // Get the RLS session variables names, in the order of the corresponding placeholder indexes.
        let rsl_vars = rls_apply.get_rls_vars();
        if rsl_vars.len() as u32 != rls_placeholders_count {
            internal!("Discrepancy between calculated amounts of the RLS placeholders and the session variables")
        }
        // The RLS is applied to `stmt`, let's check if the original statement,
        // associated with `cached_name` is valid
        match CACHED_QUERIES_REPO.read_guard()?.map.get(&cached_name) {
            Some(qi) => {
                if !qi.is_valid() {
                    return Err(if let Err(e) = &qi.status {
                        e.clone()
                    } else {
                        unsupported_err!("Currently RLS only supported for CREATE CACHE statements")
                    });
                }
            }
            None => unsupported!("Currently RLS only supported for CREATE CACHE statements"),
        };
        // Add the query name along with the RLS tables and session variables to `RLS_APPLIED_QUERIES`
        register_rls_applied_query(cached_name, rls_apply.get_rls_tables(), rsl_vars)?;
        Ok(true)
    }
}

/// Add the RLS managed query `cached_name` to the repo, along with
/// the RLS involved tables from the query and the RLS variables
fn register_rls_applied_query(
    cached_name: Relation,
    rls_tables: Vec<Relation>,
    rls_vars: Vec<SqlIdentifier>,
) -> ReadySetResult<()> {
    match RLS_APPLIED_QUERIES
        .write_guard()?
        .map
        .insert(cached_name.clone(), (rls_tables.clone(), rls_vars.clone()))
    {
        None => Ok(()),
        Some((existing_tabs, existing_vars)) => {
            // Handle the situation when the same query was managed multiple times.
            // This is valid, as we build the query graph for queries, even we are not going to cache.
            // Assert that existing content previously linked to `cached_name` is the same
            // as what we are adding now.
            if !(existing_tabs.eq(&rls_tables) && existing_vars.eq(&rls_vars)) {
                Err(internal_err!(
                    "Different RLS rules have been applied for cached query {:?};",
                    cached_name
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// Return variables associated with `cached_name` query as Some,
/// or as None if `cached_name` has not been RLS managed.
pub fn get_rls_vars_for_query(
    cached_name: &Relation,
) -> ReadySetResult<Option<Vec<SqlIdentifier>>> {
    Ok(RLS_APPLIED_QUERIES
        .read_guard()?
        .map
        .get(cached_name)
        .map(|(_, vars)| vars.clone()))
}

/// Drop from the repository the RLS applied query by name
fn drop_rls_applied_query(cached_name: &Relation) -> ReadySetResult<bool> {
    Ok(RLS_APPLIED_QUERIES
        .write_guard()?
        .map
        .remove(cached_name)
        .is_some())
}

/// Drop from the repository, and return names of the RLS applied queries,
/// in which `table` was involved
fn drop_rls_applied_queries_for_table(table: Relation) -> ReadySetResult<Option<Vec<Relation>>> {
    let mut queries_to_remove: Vec<Relation> = Vec::new();
    let mut guard = RLS_APPLIED_QUERIES.write_guard()?;
    guard.map.iter().all(|(query_name, (tabs, _))| {
        if tabs.contains(&table) {
            queries_to_remove.push(query_name.clone());
        }
        true
    });
    for query_name in &queries_to_remove {
        guard.map.remove(query_name);
    }
    Ok(if queries_to_remove.is_empty() {
        None
    } else {
        Some(queries_to_remove)
    })
}

/// Drop all RLS applied queries from the repository
fn drop_all_rls_applied_queries() {
    if let Ok(mut guard) = RLS_APPLIED_QUERIES.write_guard() {
        guard.map.clear();
    }
}

/// Return all RLS applied queries
fn get_all_rls_applied_queries() -> ReadySetResult<Option<Vec<Relation>>> {
    let query_names: Vec<Relation> = RLS_APPLIED_QUERIES
        .read_guard()?
        .map
        .keys()
        .cloned()
        .collect();
    Ok(if query_names.is_empty() {
        None
    } else {
        Some(query_names)
    })
}

// Visitor used to determine the maximum placeholder index in the input statement.
// The input statement should've already been properly handled, but since having
// them deduplicated and contiguous is invariant for our RLS implementation,
// we verify it here as well.
struct NumberPlaceholdersVisitor {
    max_placeholder_index: u32,
    placeholders_indexes: HashSet<u32>,
}

impl NumberPlaceholdersVisitor {
    fn new() -> Self {
        NumberPlaceholdersVisitor {
            max_placeholder_index: 0,
            placeholders_indexes: HashSet::new(),
        }
    }
}

impl<'ast> Visitor<'ast> for NumberPlaceholdersVisitor {
    type Error = ReadySetError;
    fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
        if let Placeholder(item) = literal {
            match item {
                ItemPlaceholder::DollarNumber(n) => {
                    if !self.placeholders_indexes.insert(*n) {
                        internal!("Duplicate placeholder index {}", n)
                    }
                    self.max_placeholder_index = cmp::max(*n, self.max_placeholder_index);
                }
                _ => {
                    internal!("Unexpected unnumbered placeholder")
                }
            }
        }
        Ok(())
    }
}

fn number_placeholders(query: &SelectStatement) -> ReadySetResult<u32> {
    let mut visitor = NumberPlaceholdersVisitor::new();
    visitor.visit_select_statement(query)?;
    if visitor.placeholders_indexes.len() != visitor.max_placeholder_index as usize {
        internal!("The placeholder indexes are not contiguous")
    }
    Ok(visitor.max_placeholder_index)
}

// Visitor used to walk over the input statement, and apply the RLS rules to
// the RLS enabled tables.
// We only allow the RLS enabled tables to be the outermost ones.
// We error out if the RLS enabled tables are referenced at inside CTE, or
// any nested statement. This sounds aggressive, but it will error out
// later on anyway, as we do not parametrize constraints inside sub-queries
// and CTE.
// We also do not allow the RLS enabled tables to be LEFT OUTER joined.
struct RLSApplyVisitor {
    // Stores already manged RLS enabled tables
    applied: HashSet<Relation>,
    // Track down the nested statements
    depth: u32,
    // Is set to `true` if RLS enabled tables are allowed at the current `depth`.
    // Will error out if an RLS enabled table is discovered while `ok_to_rls` is false.
    ok_to_rls: bool,
    // The expression we are adding the RLS rules
    add_rls_to: Option<Expr>,
    // Contains the RLS session variables, corresponding to the RLS placeholders in the correct order.
    rls_vars: Vec<SqlIdentifier>,
    // The starting index for the RLS placeholders
    start_placeholder_index: u32,
    // The running index for the RLS placeholders
    placeholder_index: u32,
    // The tables which we could not apply the RLS rules, and the reason why
    erroneous_tables: Vec<(Relation, RLSFailureReason)>,
}

enum RLSFailureReason {
    OuterJoin,
    Subquery,
}

impl Display for RLSFailureReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RLSFailureReason::OuterJoin => write!(f, "OUTER JOIN"),
            RLSFailureReason::Subquery => write!(f, "sub-query"),
        }
    }
}

impl RLSApplyVisitor {
    fn new(placeholder_index: u32) -> Self {
        RLSApplyVisitor {
            applied: HashSet::new(),
            depth: 0,
            ok_to_rls: false,
            add_rls_to: None,
            rls_vars: vec![],
            start_placeholder_index: placeholder_index,
            placeholder_index,
            erroneous_tables: vec![],
        }
    }

    fn get_rls_placeholders_count(&self) -> u32 {
        self.placeholder_index - self.start_placeholder_index
    }

    fn get_rls_vars(&self) -> Vec<SqlIdentifier> {
        self.rls_vars.clone()
    }

    fn get_rls_tables(&self) -> Vec<Relation> {
        self.applied.iter().cloned().collect()
    }

    // Construct RLS predicate, based on the given column names `rls_cols`.
    // The output expression is of format: col1 = $i and col2 = $i+1 and ...
    // where `i` is `self.placeholder_index`, which is updated accordingly.
    fn construct_rls_predicate(
        &mut self,
        table: &Relation,
        rls_rule: &[(SqlIdentifier, SqlIdentifier)],
    ) -> Option<Expr> {
        let mut rls_pred = None;
        for (col_name, _) in rls_rule {
            let rls = BinaryOp {
                lhs: Box::new(Expr::Column(Column {
                    name: col_name.clone(),
                    table: Some(table.clone()),
                })),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(Placeholder(ItemPlaceholder::DollarNumber(
                    self.placeholder_index,
                )))),
            };
            self.placeholder_index += 1;
            rls_pred = if rls_pred.is_none() {
                Some(rls)
            } else {
                Some(BinaryOp {
                    lhs: Box::new(rls_pred?),
                    op: BinaryOperator::And,
                    rhs: Box::new(rls),
                })
            };
        }
        rls_pred
    }
}

impl<'ast> VisitorMut<'ast> for RLSApplyVisitor {
    type Error = ReadySetError;

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        if let Some(rls) = get_rls_for_table(table) {
            if self.ok_to_rls {
                let Some(rls_pred) = self.construct_rls_predicate(table, &rls) else {
                    return Err(unsupported_err!(
                        "Empty RLS rules for table {}",
                        table.display_unquoted()
                    ));
                };

                self.applied.insert(table.clone());

                self.add_rls_to = Some(match self.add_rls_to.take() {
                    None => rls_pred,
                    Some(existing_expr) => BinaryOp {
                        lhs: Box::new(existing_expr),
                        op: BinaryOperator::And,
                        rhs: Box::new(rls_pred),
                    },
                });

                self.rls_vars.extend(
                    rls.iter()
                        .map(|(_, var)| var.clone())
                        .collect::<Vec<SqlIdentifier>>(),
                );
            } else {
                self.erroneous_tables.push((
                    table.clone(),
                    if self.depth == 1 {
                        RLSFailureReason::OuterJoin
                    } else {
                        RLSFailureReason::Subquery
                    },
                ));
            }
        }

        Ok(())
    }

    fn visit_column(&mut self, _: &'ast mut Column) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_select_statement(
        &mut self,
        stmt: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.depth += 1;
        self.ok_to_rls = self.depth == 1 && stmt.join.iter().all(|j| j.operator.is_inner_join());
        self.add_rls_to = stmt.where_clause.take();
        walk_select_statement(self, stmt)?;
        stmt.where_clause = self.add_rls_to.take();
        if let Some(where_clause) = &mut stmt.where_clause {
            self.visit_where_clause(where_clause)?;
        }
        if self.erroneous_tables.is_empty() {
            Ok(())
        } else {
            let mut error_msg: String = "Cannot apply RLS policy: ".to_string();
            for (i, (table, reason)) in self.erroneous_tables.iter().enumerate() {
                if i > 0 {
                    error_msg.push_str("; ");
                }
                error_msg.push_str(table.display_unquoted().to_string().as_str());
                error_msg.push_str(" referenced inside ");
                error_msg.push_str(reason.to_string().as_str());
            }
            unsupported!("{error_msg}")
        }
    }
}
