use readyset_errors::{
    internal, internal_err, unsupported, unsupported_err, ReadySetError, ReadySetResult,
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
use std::sync::{Arc, LazyLock, RwLock};

// RLS rule type.
// The inner tuple is semantic equivalent of col = var
type RLSRule = Vec<(SqlIdentifier, SqlIdentifier)>;

/*
* Table which contains the RLS rules.
* Currently, it contains hardcoded values, for testing purpose, until REA-5339 will be implemented.
* Here is the definition and the content of tables, used for testing:
*
* -- required for supporting UUID
* vassilizarouba=# CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
*
* -- Table `rls_test` contains column `auth_id` which is assumed to be the RLS field associated with
* -- `my.var_auth_id` custom variable, which should be set to some valid UUID value.
* -- For ex.:
* vassilizarouba=# set my.var_auth_id = '82abb461-3448-4a10-9ffa-eee8f4038582';
  SET
*
* vassilizarouba=# create table rls_test (auth_id uuid, i int, b int, t text, dt date);
* vassilizarouba=# insert into rls_test (auth_id, i, b, t, dt) values ('1c75691c-b02b-4fde-a049-bd6848dbf0d4', 10, 11, 'aa', '2025-02-18');
* vassilizarouba=# insert into rls_test (auth_id, i, b, t, dt) values ('b7474fb5-007a-4d11-9a09-c062253189f1', 11, 10, 'aa', '2025-02-18');
* vassilizarouba=# insert into rls_test (auth_id, i, b, t, dt) values ('8757f95f-4cb7-437b-9688-d11e5c943b4d', 11, 11, 'ab', '2025-02-17');
* vassilizarouba=# insert into rls_test (auth_id, i, b, t, dt) values ('82abb461-3448-4a10-9ffa-eee8f4038582', 12, 11, 'bb', '2025-02-19');
* vassilizarouba=# select * from rls_test;
               auth_id                | i  | b  | t  |     dt
--------------------------------------+----+----+----+------------
 1c75691c-b02b-4fde-a049-bd6848dbf0d4 | 10 | 11 | aa | 2025-02-18
 b7474fb5-007a-4d11-9a09-c062253189f1 | 11 | 10 | aa | 2025-02-18
 8757f95f-4cb7-437b-9688-d11e5c943b4d | 11 | 11 | ab | 2025-02-17
 82abb461-3448-4a10-9ffa-eee8f4038582 | 12 | 11 | bb | 2025-02-19
(4 rows)
*
*
* The current mock is the equivalent of our custom syntax statements (REA-5339):
* CREATE RLS POLICY ON public.rls_test USING (auth_id) = ('my.var_auth_id');
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

type RlsAppliedQueriesMap = HashMap<Relation, Vec<SqlIdentifier>>;

// Contains RLS managed queries mapped against the RLS session variables, which have been injected into
// the original query as extra placeholders.
// During upquery execution, the values of these variables should be added to the auto-parameters.
static RLS_APPLIED_QUERIES: LazyLock<Arc<RwLock<RlsAppliedQueriesMap>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

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
        // Add the query name along with the RLS session variables to `RLS_APPLIED_QUERIES`
        register_rls_applied_query(cached_name, rsl_vars)?;
        Ok(true)
    }
}

fn register_rls_applied_query(
    cached_name: Relation,
    rls_vars: Vec<SqlIdentifier>,
) -> ReadySetResult<()> {
    match RLS_APPLIED_QUERIES.write() {
        Ok(mut guard) => match guard.insert(cached_name.clone(), rls_vars.clone()) {
            None => Ok(()),
            Some(old_rls_vars) => {
                // Handle the situation when the same query was managed multiple times.
                // This is valid, as we build the query graph for queries, even we are not going to cache.
                // Assert that existing RLS variables previously linked to `cached_name` are the same
                // as the ones we are adding now.
                if !rls_vars
                    .iter()
                    .zip(old_rls_vars.iter())
                    .all(|(new_var, old_var)| new_var.eq(old_var))
                {
                    Err(internal_err!(
                        "Different RLS rules have been applied for cached query {:?};",
                        cached_name
                    ))
                } else {
                    Ok(())
                }
            }
        },
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

// Return variables associated with `cached_name` query as Some,
// or as None if `cached_name` has not been RLS managed.
pub fn get_rls_vars_for_query(
    cached_name: &Relation,
) -> ReadySetResult<Option<Vec<SqlIdentifier>>> {
    match RLS_APPLIED_QUERIES.read() {
        Ok(guard) => Ok(guard.get(cached_name).cloned()),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

// Return RLS rule associated with `tab_name` or None
fn get_rls_for_table(tab_name: &Relation) -> Option<RLSRule> {
    let guard = match RLS_REPO.read() {
        Ok(guard) => guard,
        Err(_) => return None,
    };
    guard.get(tab_name).cloned()
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
    pub fn new(placeholder_index: u32) -> Self {
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

    pub fn get_rls_placeholders_count(&self) -> u32 {
        self.placeholder_index - self.start_placeholder_index
    }

    pub fn get_rls_vars(&self) -> Vec<SqlIdentifier> {
        self.rls_vars.clone()
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
            for (tab_name, reason) in &self.erroneous_tables {
                error_msg.push_str(tab_name.display_unquoted().to_string().as_str());
                error_msg.push_str(" referenced inside ");
                error_msg.push_str(reason.to_string().as_str());
            }
            unsupported!("{error_msg}")
        }
    }
}
