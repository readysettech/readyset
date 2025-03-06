use readyset_errors::{internal, internal_err, unsupported_err, ReadySetError, ReadySetResult};
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
use std::sync::{Arc, LazyLock, RwLock};

// RLS rule instance: col_name_1 = 'var_name_1' [and col_name_* = 'var_name_*']
// `columns` contains the column names, which should compare against variables from `sys_vars`.
#[derive(Debug, Clone)]
struct RLSRule {
    columns: Vec<SqlIdentifier>,
    sys_vars: Vec<SqlIdentifier>,
}

/*
* Table which contains the RLS rules.
* Currently, it contains hardcoded values, for testing purpose, until REA-5339 will be implemented.
* Here is the definition and the content of tables, used for testing:
*
* -- required for supporting UUID
* vassilizarouba=# CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
*
* -- Table `rls_test` contains column `auth_id` which is assumed to be the RLS filed associated with
* -- `my.var_auth_id` custom variable, which should be set to some existing UUID value.
* -- For ex.:
* vassilizarouba=# select set_config('my.var_auth_id', '82abb461-3448-4a10-9ffa-eee8f4038582', false);
              set_config
--------------------------------------
 82abb461-3448-4a10-9ffa-eee8f4038582
(1 row)
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

* -- Table `rls_test_user` contains columns `auth_id` and `user_id` which are assumed to be the RLS fileds
* -- associated with `my.var_auth_id` and `USER` variables.
* -- Note, `my.var_auth_id` should be set to some existing UUID value, whereas `USER` is defined at login.
* -- For ex.:
* vassilizarouba=# select set_config('my.var_auth_id', '8757f95f-4cb7-437b-9688-d11e5c943b4d', false);
              set_config
--------------------------------------
 8757f95f-4cb7-437b-9688-d11e5c943b4d
(1 row)

* vassilizarouba=# select USER;
   user
----------
 postgres
(1 row)

* vassilizarouba=# create table rls_test_user (auth_id uuid, user_id text, i int, b int, t text, dt date);
* vassilizarouba=# insert into rls_test_user (auth_id, user_id, i, b, t, dt) values ('1c75691c-b02b-4fde-a049-bd6848dbf0d4', 'postgres', 10, 11, 'aa', '2025-02-18');
* vassilizarouba=# insert into rls_test_user (auth_id, user_id, i, b, t, dt) values ('b7474fb5-007a-4d11-9a09-c062253189f1', 'postgres', 11, 10, 'aa', '2025-02-18');
* vassilizarouba=# insert into rls_test_user (auth_id, user_id, i, b, t, dt) values ('8757f95f-4cb7-437b-9688-d11e5c943b4d', 'postgres', 11, 11, 'ab', '2025-02-17');
* vassilizarouba=# insert into rls_test_user (auth_id, user_id, i, b, t, dt) values ('82abb461-3448-4a10-9ffa-eee8f4038582', 'John',     12, 11, 'bb', '2025-02-19');
* vassilizarouba=# select * from rls_test_user;
               auth_id                | user_id  | i  | b  | t  |     dt
--------------------------------------+----------+----+----+----+------------
 1c75691c-b02b-4fde-a049-bd6848dbf0d4 | postgres | 10 | 11 | aa | 2025-02-18
 b7474fb5-007a-4d11-9a09-c062253189f1 | postgres | 11 | 10 | aa | 2025-02-18
 8757f95f-4cb7-437b-9688-d11e5c943b4d | postgres | 11 | 11 | ab | 2025-02-17
 82abb461-3448-4a10-9ffa-eee8f4038582 | John     | 12 | 11 | bb | 2025-02-19
(4 rows)
*
*
* The current mock is the equivalent of our custom syntax statements (REA-5339):
* CREATE RLS POLICY ON public.rls_test USING (auth_id) = ('my.var_auth_id');
* CREATE RLS POLICY ON public.rls_test_user USING (auth_id, auth_user) = ('my.var_auth_id', USER);
*/
static RLS_REPO: LazyLock<Arc<RwLock<HashMap<Relation, RLSRule>>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert(
        Relation {
            schema: Some("public".into()),
            name: "rls_test".into(),
        },
        RLSRule {
            columns: vec!["auth_id".into()],
            sys_vars: vec!["my.var_auth_id".into()],
        },
    );
    map.insert(
        Relation {
            schema: Some("public".into()),
            name: "rls_test_user".into(),
        },
        RLSRule {
            columns: vec!["auth_id".into(), "user_id".into()],
            sys_vars: vec!["my.var_auth_id".into(), "USER".into()],
        },
    );
    Arc::new(RwLock::new(map))
});

type RlsAppliedQueriesMap = HashMap<Relation, Vec<SqlIdentifier>>;

// Contains RLS managed queries mapped against the system variables, which have been injected into
// the original query as extra placeholders.
// During upquery execution, these system variables values should be added to the auto-parameters.
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
        // Get the names of the system variables, in the order of the corresponding placeholder indexes.
        let rsl_sys_vars = rls_apply.get_rls_sys_vars();
        if rsl_sys_vars.len() as u32 != rls_placeholders_count {
            internal!("Discrepancy between calculated amounts of the RLS placeholders and system variables")
        }
        // Add the query name along with the system variables to `RLS_APPLIED_QUERIES`
        register_rls_applied_query(cached_name, rsl_sys_vars)?;
        Ok(true)
    }
}

fn register_rls_applied_query(
    cached_name: Relation,
    sys_vars: Vec<SqlIdentifier>,
) -> ReadySetResult<()> {
    match RLS_APPLIED_QUERIES.write() {
        Ok(mut guard) => match guard.insert(cached_name.clone(), sys_vars.clone()) {
            None => Ok(()),
            Some(old_sys_vars) => {
                // Handle the situation when the same query was managed multiple times.
                // This is valid, as we build the query graph for queries, even we are not going to cache.
                // Assert that existing system variables previously linked to `cached_name` are the same
                // as the ones we are adding now.
                if !sys_vars
                    .iter()
                    .zip(old_sys_vars.iter())
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

// Return the system variables associated with `cached_name` query as Some,
// or as None if `cached_name` has not been RLS managed.
pub fn get_rls_sys_vars_for_query(
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

// Constant for the maximum number of placeholders in the statement.
// This is used as part of implementation of the verification that
// the placeholder indexes are contiguous
const MAX_PLACEHOLDERS_NUMBER: usize = 32768;

// Visitor used to determine the maximum placeholder index in the input statement.
// The input statement should've already been properly handled, but since having
// them contiguous is invariant for our RLS implementation, we verify it here as well,
// in a very simple and efficient way.
// `NumberPlaceholdersVisitor.contiguous_indexes` is a bitset, with bit indexes
// corresponding to the placeholder indexes. So that, having all bits set,
// means the placeholder indexes are contiguous.
struct NumberPlaceholdersVisitor {
    max_placeholder_index: u32,
    contiguous_indexes: Vec<u32>,
}

macro_rules! div_32 {
    ($v:expr) => {
        (($v) >> 5)
    };
}

macro_rules! mod_32 {
    ($v:expr) => {
        (($v) & 31)
    };
}
impl NumberPlaceholdersVisitor {
    fn new() -> Self {
        NumberPlaceholdersVisitor {
            max_placeholder_index: 0,
            contiguous_indexes: vec![0; MAX_PLACEHOLDERS_NUMBER / 32 /*number of bits in u32*/],
        }
    }

    fn add_placeholder(&mut self, placeholder_index: usize) -> ReadySetResult<()> {
        if placeholder_index >= MAX_PLACEHOLDERS_NUMBER {
            internal!(
                "Too big placeholder index. Expected maximum {}, found {}.",
                MAX_PLACEHOLDERS_NUMBER,
                placeholder_index
            );
        }
        let bit_index = placeholder_index - 1;
        if (self.contiguous_indexes[div_32!(bit_index)] >> mod_32!(bit_index)) & 1 == 1 {
            internal!("Duplicate placeholder ${}", placeholder_index);
        }
        self.contiguous_indexes[div_32!(bit_index)] |= 1u32 << mod_32!(bit_index);
        Ok(())
    }

    fn assert_all_placeholders_contiguous(&self) -> ReadySetResult<()> {
        let bit_count = self.max_placeholder_index;

        if bit_count == 0 {
            return Ok(());
        }

        let full_words = div_32!(bit_count) as usize;
        let remaining_bits = mod_32!(bit_count);

        for i in 0..full_words {
            if self.contiguous_indexes[i] != u32::MAX {
                internal!("The placeholder indexes are not contiguous")
            }
        }

        if remaining_bits > 0 {
            let last_word_mask = (1u32 << remaining_bits) - 1;
            if (self.contiguous_indexes[full_words] & last_word_mask) == last_word_mask {
                return Ok(());
            } else {
                internal!("The placeholder indexes are not contiguous")
            }
        }

        Ok(())
    }
}

impl<'ast> Visitor<'ast> for NumberPlaceholdersVisitor {
    type Error = ReadySetError;
    fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
        if let Placeholder(item) = literal {
            match item {
                ItemPlaceholder::DollarNumber(n) => {
                    self.add_placeholder(*n as usize)?;
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
    visitor.assert_all_placeholders_contiguous()?;
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
    depth: i32,
    // Is set to `true` if RLS enabled tables are allowed at the current `depth`.
    // Will error out, if it's `false` and RLS enabled table is found.
    ok_to_rls: bool,
    // The expression we are adding the RLS rules
    add_rls_to: Option<Expr>,
    // Contains the system variables corresponding to the RLS placeholders in the correct order.
    rls_vars: Vec<SqlIdentifier>,
    // The starting index for the RLS placeholders
    start_placeholder_index: u32,
    // The running index for the RLS placeholders
    placeholder_index: u32,
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
        }
    }

    pub fn get_rls_placeholders_count(&self) -> u32 {
        self.placeholder_index - self.start_placeholder_index
    }

    pub fn get_rls_sys_vars(&self) -> Vec<SqlIdentifier> {
        self.rls_vars.clone()
    }

    // Construct RLS predicate, based on the given column names `rls_cols`.
    // The output expression is of format: col1 = $i and col2 = $i+1 and ...
    // where `i` is `self.placeholder_index`, which is updated accordingly.
    fn construct_rls_predicate(
        &mut self,
        table: &Relation,
        rls_cols: &[SqlIdentifier],
    ) -> Option<Expr> {
        let mut rls_pred = None;
        for col_name in rls_cols.iter() {
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
                let Some(rls_pred) = self.construct_rls_predicate(table, &rls.columns) else {
                    return Err(unsupported_err!(
                        "Empty RLS rules for table {}",
                        table.display_unquoted()
                    ));
                };

                if self.applied.contains(table) {
                    return Err(unsupported_err!(
                        "RLS-ed table {} referenced multiple times",
                        table.display_unquoted()
                    ));
                }
                self.applied.insert(table.clone());

                self.add_rls_to = Some(match self.add_rls_to.take() {
                    None => rls_pred,
                    Some(existing_expr) => BinaryOp {
                        lhs: Box::new(existing_expr),
                        op: BinaryOperator::And,
                        rhs: Box::new(rls_pred),
                    },
                });

                self.rls_vars.extend(rls.sys_vars.clone());
            } else {
                return Err(unsupported_err!(
                    "Can not parametrize RLS constraint for table {}",
                    table.display_unquoted()
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
        Ok(())
    }
}
