use crate::DfValue;
use readyset_errors::{
    internal, internal_err, invalid_query, unsupported, unsupported_err, ReadySetError,
    ReadySetResult,
};
use readyset_sql::analysis::visit::Visitor;
use readyset_sql::analysis::visit_mut::{walk_select_statement, VisitorMut};
use readyset_sql::ast::Expr::BinaryOp;
use readyset_sql::ast::Literal::Placeholder;
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, ItemPlaceholder, Literal, Relation, SelectStatement,
    SqlIdentifier,
};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

// RLS rule type.
// The inner tuple is a semantic equivalent of (col = var)
type RLSRule = Vec<(SqlIdentifier, SqlIdentifier)>;

impl From<&RLSRule> for DfValue {
    fn from(value: &RLSRule) -> Self {
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
        format!("({}) = ({})", col_text, var_text)
            .to_string()
            .into()
    }
}

/*
* Table which contains the RLS rules.
*/
static RLS_REPO: LazyLock<Arc<RwLock<HashMap<Relation, RLSRule>>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert(
        Relation {
            schema: Some("public".into()),
            name: "rls_test".into(),
        },
        vec![("auth_id".into(), "my.var_auth_id".into())],
    );
    Arc::new(RwLock::new(map))
});

fn get_rls_repo_write_guard(
) -> ReadySetResult<RwLockWriteGuard<'static, HashMap<Relation, RLSRule>>> {
    match RLS_REPO.write() {
        Ok(guard) => Ok(guard),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

fn get_rls_repo_read_guard() -> ReadySetResult<RwLockReadGuard<'static, HashMap<Relation, RLSRule>>>
{
    match RLS_REPO.read() {
        Ok(guard) => Ok(guard),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

pub fn create_rls(
    table: Relation,
    policy: Vec<(Column, SqlIdentifier)>,
    if_not_exists: bool,
) -> ReadySetResult<()> {
    let mut guard = get_rls_repo_write_guard()?;
    if if_not_exists && guard.contains_key(&table) {
        invalid_query!("RLS already defined on table {}", table.display_unquoted())
    } else {
        guard.insert(
            table,
            policy
                .into_iter()
                .map(|(col, var)| (col.name, var))
                .collect(),
        );
        Ok(())
    }
}

pub fn drop_rls(table: Option<Relation>) -> ReadySetResult<Option<Vec<Relation>>> {
    if let Some(table) = table {
        Ok(if get_rls_repo_write_guard()?.remove(&table).is_some() {
            drop_rls_applied_queries_for_table(table)?
        } else {
            None
        })
    } else {
        get_rls_repo_write_guard()?.clear();
        Ok(get_all_rls_applied_queries()?)
    }
}

pub fn get_rls(table: Option<Relation>) -> ReadySetResult<Option<Vec<Vec<DfValue>>>> {
    macro_rules! rls_policy_row {
        ($table:expr, $rls_rule:expr) => {
            vec![
                $table.display_unquoted().to_string().as_str().into(),
                $rls_rule.into(),
            ]
        };
    }

    let guard = get_rls_repo_read_guard()?;
    if let Some(table) = table {
        if let Some(rls_rule) = guard.get(&table) {
            Ok(Some(vec![rls_policy_row!(table, rls_rule)]))
        } else {
            Ok(None)
        }
    } else {
        let mut rows: Vec<Vec<DfValue>> = Vec::with_capacity(guard.len());
        for (table, rls_rule) in guard.iter() {
            rows.push(rls_policy_row!(table, rls_rule));
        }
        Ok(Some(rows))
    }
}

// Return RLS policy associated with `table` or None
fn get_rls_for_table(table: &Relation) -> Option<RLSRule> {
    if let Ok(guard) = get_rls_repo_read_guard() {
        guard.get(table).cloned()
    } else {
        None
    }
}

type RlsAppliedQueriesMap = HashMap<Relation, (Vec<Relation>, Vec<SqlIdentifier>)>;

// Contains RLS managed queries mapped against the RLS tables and session variables,
// which have been injected into the original query as extra placeholders.
// During upquery execution, the values of these variables should be added to the auto-parameters.
static RLS_APPLIED_QUERIES: LazyLock<Arc<RwLock<RlsAppliedQueriesMap>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

fn get_rls_applied_queries_read_guard(
) -> ReadySetResult<RwLockReadGuard<'static, RlsAppliedQueriesMap>> {
    match RLS_APPLIED_QUERIES.read() {
        Ok(guard) => Ok(guard),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

fn get_rls_applied_queries_write_guard(
) -> ReadySetResult<RwLockWriteGuard<'static, RlsAppliedQueriesMap>> {
    match RLS_APPLIED_QUERIES.write() {
        Ok(guard) => Ok(guard),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

// Takes the statement `stmt`, and applies the existing RLS rules for the outermost tables.
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
        // Add the query name along with the RLS tables and session variables to `RLS_APPLIED_QUERIES`
        register_rls_applied_query(cached_name, rls_apply.get_rls_tables(), rsl_vars)?;
        Ok(true)
    }
}

fn register_rls_applied_query(
    cached_name: Relation,
    rls_tables: Vec<Relation>,
    rls_vars: Vec<SqlIdentifier>,
) -> ReadySetResult<()> {
    match get_rls_applied_queries_write_guard()?
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

// Return variables associated with `cached_name` query as Some,
// or as None if `cached_name` has not been RLS managed.
pub fn get_rls_vars_for_query(
    cached_name: &Relation,
) -> ReadySetResult<Option<Vec<SqlIdentifier>>> {
    Ok(get_rls_applied_queries_read_guard()?
        .get(cached_name)
        .map(|(_, vars)| vars.clone()))
}

// Return cached queries, in which `table` was involved
fn drop_rls_applied_queries_for_table(
    drop_rls_for_table: Relation,
) -> ReadySetResult<Option<Vec<Relation>>> {
    let mut queries_to_remove: Vec<Relation> = Vec::new();
    let mut guard = get_rls_applied_queries_write_guard()?;
    guard.iter().all(|(query_name, (tabs, _))| {
        if tabs.contains(&drop_rls_for_table) {
            queries_to_remove.push(query_name.clone());
        }
        true
    });
    for query_name in &queries_to_remove {
        guard.remove(query_name);
    }
    Ok(if queries_to_remove.is_empty() {
        None
    } else {
        Some(queries_to_remove)
    })
}

// Return all cached queries
fn get_all_rls_applied_queries() -> ReadySetResult<Option<Vec<Relation>>> {
    let query_names: Vec<Relation> = get_rls_applied_queries_read_guard()?
        .iter()
        .map(|(query_name, _)| query_name.clone())
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
                    if self.depth == 0 {
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
        self.ok_to_rls = self.depth == 0 && stmt.join.iter().all(|j| j.operator.is_inner_join());
        self.depth += 1;
        self.add_rls_to = stmt.where_clause.take();
        walk_select_statement(self, stmt)?;
        stmt.where_clause = self.add_rls_to.take();
        if let Some(where_clause) = &mut stmt.where_clause {
            self.visit_where_clause(where_clause)?;
        }
        if self.erroneous_tables.is_empty() {
            Ok(())
        } else {
            let mut error_msg: String = "Cannot apply RLS policy to tables: ".to_string();
            for (table, reason) in &self.erroneous_tables {
                error_msg.push_str(table.display_unquoted().to_string().as_str());
                error_msg.push_str(" referenced inside ");
                error_msg.push_str(reason.to_string().as_str());
            }
            unsupported!("{error_msg}")
        }
    }
}
