use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::{ReadySetResult, ReuseConfigType};

use nom_sql::{parser as sql_parser, Dialect, SqlQuery};
use noria::ActivationResult;
use petgraph::graph::NodeIndex;
use tracing::{debug, error, warn};

use nom_sql::CreateTableStatement;
use noria_errors::{internal, internal_err, ReadySetError};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::str;
use std::vec::Vec;

use super::sql;

/// The canonical SQL dialect used for central Noria server recipes. All direct clients of
/// noria-server must use this dialect for their SQL recipes, and all adapters and client libraries
/// must translate into this dialect as part of handling requests from users
pub const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

type QueryID = u64;

/// Represents a Soup recipe.
#[derive(Clone, Debug)]
// crate viz for tests
pub(crate) struct Recipe {
    /// SQL queries represented in the recipe. Value tuple is (name, query, public).
    expressions: HashMap<QueryID, (Option<String>, SqlQuery, bool)>,
    /// Addition order for the recipe expressions
    expression_order: Vec<QueryID>,
    /// Named read/write expression aliases, mapping to queries in `expressions`.
    aliases: HashMap<String, QueryID>,

    /// Recipe revision.
    version: usize,
    /// Preceding recipe.
    prior: Option<Box<Recipe>>,

    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    ///
    /// NOTE: this is an Option so that we can call [`Option::take`] on it, but will
    /// never actually be None externally.
    inc: Option<SqlIncorporator>,
}

impl Display for Recipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for query_id in &self.expression_order {
            let (name, query, public) = &self.expressions[query_id];
            if *public {
                write!(f, "query")?;
            }
            if let Some(name) = name {
                if *public {
                    write!(f, " ")?;
                }
                write!(f, "{}", name)?;
            }
            if *public || name.is_some() {
                write!(f, ": ")?;
            }
            writeln!(f, "{};", query)?;
        }

        Ok(())
    }
}

unsafe impl Send for Recipe {}

impl PartialEq for Recipe {
    /// Equality for recipes is defined in terms of all members apart from `inc`.
    fn eq(&self, other: &Recipe) -> bool {
        self.expressions == other.expressions
            && self.expression_order == other.expression_order
            && self.aliases == other.aliases
            && self.version == other.version
            && self.prior == other.prior
    }
}

#[derive(Debug)]
pub(super) enum Schema {
    Table(CreateTableStatement),
    View(Vec<String>),
}

fn hash_query(q: &SqlQuery) -> QueryID {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

#[inline]
fn ident(input: &str) -> nom::IResult<&str, &str> {
    use nom::InputTakeAtPosition;
    input.split_at_position_complete(|chr| {
        !(chr.is_ascii() && (nom::character::is_alphanumeric(chr as u8) || chr == '_'))
    })
}

fn query_prefix(input: &str) -> nom::IResult<&str, (bool, Option<&str>)> {
    use nom::branch::alt;
    use nom::bytes::complete::tag_no_case;
    use nom::character::complete::{char, multispace0, space1};
    use nom::combinator::opt;
    use nom::sequence::{pair, terminated};
    let (input, public) = opt(pair(
        alt((tag_no_case("query"), tag_no_case("view"))),
        space1,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, name) = opt(terminated(ident, multispace0))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':')(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, (public.is_some(), name)))
}

fn query_expr(input: &str) -> nom::IResult<&str, (bool, Option<&str>, SqlQuery)> {
    use nom::character::complete::multispace0;
    use nom::combinator::opt;
    let (input, prefix) = opt(query_prefix)(input)?;
    // NOTE: some massaging since nom_sql operates on &[u8], not &str
    let (input, expr) = match sql_parser::sql_query(CANONICAL_DIALECT)(input.as_bytes()) {
        Ok((i, e)) => Ok((std::str::from_utf8(i).unwrap(), e)),
        Err(nom::Err::Incomplete(n)) => Err(nom::Err::Incomplete(n)),
        Err(nom::Err::Error((i, e))) => Err(nom::Err::Error((std::str::from_utf8(i).unwrap(), e))),
        Err(nom::Err::Failure((i, e))) => {
            Err(nom::Err::Error((std::str::from_utf8(i).unwrap(), e)))
        }
    }?;
    let (input, _) = multispace0(input)?;
    Ok((
        input,
        match prefix {
            None => (false, None, expr),
            Some((public, name)) => (public, name, expr),
        },
    ))
}

fn query_exprs(input: &str) -> nom::IResult<&str, Vec<(bool, Option<&str>, SqlQuery)>> {
    nom::multi::many1(query_expr)(input)
}

#[allow(unused)]
impl Recipe {
    /// Get the id associated with an alias
    pub(crate) fn id_from_alias(&self, alias: &str) -> Option<&QueryID> {
        self.aliases.get(alias)
    }
    /// Get the SqlQuery associated with a query ID
    pub(crate) fn expression(&self, id: &QueryID) -> Option<&SqlQuery> {
        self.expressions.get(id).map(|expr| &expr.1)
    }
    /// Return active aliases for expressions
    fn aliases(&self) -> Vec<&str> {
        self.aliases.keys().map(String::as_str).collect()
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    fn blank() -> Recipe {
        Recipe {
            expressions: HashMap::default(),
            expression_order: Vec::default(),
            aliases: HashMap::default(),
            version: 0,
            prior: None,
            inc: Some(SqlIncorporator::new()),
        }
    }

    /// Creates a new blank recipe with the given SQL configuration and MIR configuration
    pub(crate) fn with_config(sql_config: sql::Config, mir_config: sql::mir::Config) -> Self {
        let mut res = Recipe::blank();
        res.set_sql_config(sql_config);
        res.set_mir_config(mir_config);
        res
    }

    /// Creates a blank recipe with config taken from `other`
    pub(crate) fn blank_with_config_from(other: &Recipe) -> Recipe {
        let mut res = Recipe::blank();
        res.clone_config_from(other);
        res
    }

    /// Creates a blank recipe with the given `version`, and config taken from `other`
    pub(super) fn with_version_and_config_from(version: usize, other: &Recipe) -> Recipe {
        let mut res = Recipe {
            version,
            ..Self::blank()
        };
        res.clone_config_from(other);
        res
    }

    /// Clone the MIR and SQL configuration from `other` into `self`
    pub(crate) fn clone_config_from(&mut self, other: &Recipe) {
        self.set_sql_config(other.sql_config().clone());
        self.set_mir_config(other.mir_config().clone());
    }

    /// Set the MIR configuration for this recipe
    pub(crate) fn set_mir_config(&mut self, mir_config: sql::mir::Config) {
        self.inc.as_mut().unwrap().set_mir_config(mir_config)
    }

    /// Get a shared reference to this recipe's MIR configuration
    pub(crate) fn mir_config(&self) -> &sql::mir::Config {
        self.inc.as_ref().unwrap().mir_config()
    }

    /// Set the SQL configuration for this recipe
    pub(crate) fn set_sql_config(&mut self, sql_config: sql::Config) {
        self.inc.as_mut().unwrap().config = sql_config;
    }

    /// Get a shared reference to this recipe's SQL configuration
    pub(crate) fn sql_config(&self) -> &sql::Config {
        &self.inc.as_ref().unwrap().config
    }

    /// Disable node reuse.
    // crate viz for tests
    pub(crate) fn disable_reuse(&mut self) {
        self.inc.as_mut().unwrap().disable_reuse();
    }

    /// Enable node reuse.
    // crate viz for tests
    pub(crate) fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.inc.as_mut().unwrap().enable_reuse(reuse_type)
    }

    pub(in crate::controller) fn resolve_alias(&self, alias: &str) -> Option<&str> {
        self.aliases.get(alias).map(|qid| {
            let (ref internal_qn, _, _) = self.expressions[qid];
            internal_qn.as_ref().unwrap().as_str()
        })
    }

    /// Obtains the `NodeIndex` for the node corresponding to a named query or a write type.
    pub(in crate::controller) fn node_addr_for(&self, name: &str) -> Result<NodeIndex, String> {
        match self.inc {
            Some(ref inc) => {
                // `name` might be an alias for another identical query, so resolve if needed
                let na = match self.resolve_alias(name) {
                    None => inc.get_query_address(name),
                    Some(internal_qn) => inc.get_query_address(internal_qn),
                };
                match na {
                    None => Err(format!(
                        "No query endpoint for \"{}\" exists at v{}.",
                        name, self.version
                    )),
                    Some(na) => Ok(na),
                }
            }
            None => Err("Recipe not applied".to_string()),
        }
    }

    /// Get schema for a base table or view in the recipe.
    pub(super) fn schema_for(&self, name: &str) -> Option<Schema> {
        let inc = self.inc.as_ref().expect("Recipe not applied");
        match inc.get_base_schema(name) {
            None => {
                let s = match self.resolve_alias(name) {
                    None => inc.get_view_schema(name),
                    Some(internal_qn) => inc.get_view_schema(internal_qn),
                };
                s.map(Schema::View)
            }
            Some(s) => Some(Schema::Table(s)),
        }
    }

    /// Remove comments from queries and aggregate lines into singular queries
    pub(crate) fn clean_queries(recipe_text: &str) -> Vec<String> {
        recipe_text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("--"))
            .map(|l| {
                // remove inline comments, too
                match l.find('#') {
                    None => l.trim(),
                    Some(pos) => l[0..pos - 1].trim(),
                }
            })
            .fold(Vec::<String>::new(), |mut acc, l| {
                match acc.last_mut() {
                    Some(s) if !s.ends_with(';') => {
                        s.push(' ');
                        s.push_str(l);
                    }
                    _ => acc.push(l.to_string()),
                }
                acc
            })
    }

    /// Creates a recipe from a set of SQL queries in a string (e.g., read from a file).
    /// Note that the recipe is not backed by a Soup data-flow graph until `activate` is called on
    /// it.
    // crate viz for tests
    pub(crate) fn from_str(recipe_text: &str) -> ReadySetResult<Recipe> {
        // parse and compute differences to current recipe
        let parsed_queries = Recipe::parse(recipe_text)?;
        Recipe::from_queries(parsed_queries)
    }

    /// Creates a recipe from a set of pre-parsed `SqlQuery` structures.
    /// Note that the recipe is not backed by a Soup data-flow graph until `activate` is called on
    /// it.
    fn from_queries(qs: Vec<(Option<String>, SqlQuery, bool)>) -> ReadySetResult<Recipe> {
        let mut aliases = HashMap::default();
        let mut expression_order = Vec::new();
        let mut duplicates = 0;
        let mut expressions = HashMap::new();
        for (mut n, q, mut is_leaf) in qs.into_iter() {
            let qid = hash_query(&q);
            if !expression_order.contains(&qid) {
                expression_order.push(qid);
            } else {
                duplicates += 1;
            }

            // Treat views created using CREATE VIEW as leaf views too
            if let (None, SqlQuery::CreateView(query)) = (&n, &q) {
                n = Some(query.name.clone());
                is_leaf = true;
            }

            if let Some(ref name) = n {
                if aliases.contains_key(name) && aliases[name] != qid {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Query name exists but existing query is different: {}",
                        name
                    )));
                }
                aliases.insert(name.clone(), qid);
            }
            expressions.insert(qid, (n, q, is_leaf));
        }

        let inc = SqlIncorporator::new();

        debug!(duplicate_queries = duplicates, version = 0);

        Ok(Recipe {
            expressions,
            expression_order,
            aliases,
            version: 0,
            prior: None,
            inc: Some(inc),
        })
    }

    /// Activate the recipe by migrating the Soup data-flow graph wrapped in `mig` to the recipe.
    /// This causes all necessary changes to said graph to be applied; however, it is the caller's
    /// responsibility to call `mig.commit()` afterwards.
    // crate viz for tests
    pub(crate) fn activate(
        &mut self,
        mig: &mut Migration,
    ) -> Result<ActivationResult, ReadySetError> {
        debug!(
            num_queries = self.expressions.len(),
            named_queries = self.aliases.len(),
            version = self.version,
        );

        let (added, removed) = match self.prior {
            None => self.compute_delta(&Recipe::blank()),
            Some(ref pr) => {
                // compute delta over prior recipe
                self.compute_delta(pr)
            }
        };

        let mut result = ActivationResult {
            new_nodes: HashMap::default(),
            removed_leaves: Vec::default(),
            expressions_added: added.len(),
            expressions_removed: removed.len(),
        };

        // upgrade schema version *before* applying changes, so that new queries are correctly
        // tagged with the new version. If this recipe was just created, there is no need to
        // upgrade the schema version, as the SqlIncorporator's version will still be at zero.
        if self.version > 0 {
            self.inc.as_mut().unwrap().upgrade_schema(self.version)?;
        }

        // add new queries to the Soup graph carried by `mig`, and reflect state in the
        // incorporator in `inc`. `NodeIndex`es for new nodes are collected in `new_nodes` to be
        // returned to the caller (who may use them to obtain mutators and getters)
        for qid in added {
            let (n, q, is_leaf) = self.expressions[&qid].clone();

            // add the query
            let qfp = self
                .inc
                .as_mut()
                .unwrap()
                .add_parsed_query(q, n.clone(), is_leaf, mig)?;

            if qfp.reused_nodes.get(0) == Some(&qfp.query_leaf) && qfp.reused_nodes.len() == 1 {
                if let Some(ref name) = n {
                    self.alias_query(&qfp.name, name.clone()).map_err(|_| {
                        internal_err(
                            "SqlIncorporator told recipe about a query it doesn't know about!",
                        )
                    })?;
                }
            }

            // If the user provided us with a query name, use that.
            // If not, use the name internally used by the QFP.
            let query_name = n.unwrap_or_else(|| qfp.name.clone());
            result.new_nodes.insert(query_name, qfp.query_leaf);
        }

        result.removed_leaves = removed
            .iter()
            .map(|qid| {
                let (ref n, ref q, _) = self.prior.as_ref().unwrap().expressions[qid];
                Ok(match q {
                    SqlQuery::CreateTable(ref ctq) => {
                        // a base may have many dependent queries, including ones that also lost
                        // nodes; the code handling `removed_leaves` therefore needs to take care
                        // not to remove bases while they still have children, or to try removing
                        // them twice.
                        self.inc.as_mut().unwrap().remove_base(&ctq.table.name)?;
                        match self.node_addr_for(&ctq.table.name) {
                            Ok(ni) => Some(ni),
                            Err(e) => {
                                error!(
                                    err = %e,
                                    name = %ctq.table.name,
                                    "failed to remove base whose address could not be resolved",
                                );
                                internal!(
                                    "failed to remove base {} whose address could not be resolved",
                                    ctq.table.name
                                );
                            }
                        }
                    }
                    _ => self
                        .inc
                        .as_mut()
                        .unwrap()
                        .remove_query(n.as_ref().unwrap(), mig)?,
                })
            })
            // FIXME(eta): error handling impl overhead
            .collect::<ReadySetResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(result)
    }

    /// Work out the delta between two recipes.
    /// Returns two sets of `QueryID` -> `SqlQuery` mappings:
    /// (1) those queries present in `self`, but not in `other`; and
    /// (2) those queries present in `other` , but not in `self`.
    fn compute_delta(&self, other: &Recipe) -> (Vec<QueryID>, Vec<QueryID>) {
        let mut added_queries: Vec<QueryID> = Vec::new();
        let mut removed_queries = Vec::new();
        for qid in self.expression_order.iter() {
            if !other.expressions.contains_key(qid) {
                added_queries.push(*qid);
            }
        }
        for qid in other.expression_order.iter() {
            if !self.expressions.contains_key(qid) {
                removed_queries.push(*qid);
            }
        }

        (added_queries, removed_queries)
    }

    /// Returns the query expressions in the recipe.
    // crate viz for tests
    pub(crate) fn expressions(&self) -> Vec<(Option<&String>, &SqlQuery)> {
        self.expressions
            .values()
            .map(|&(ref n, ref q, _)| (n.as_ref(), q))
            .collect()
    }

    /// Append the queries in the `additions` argument to this recipe. This will attempt to parse
    /// `additions`, and if successful, will extend the recipe. No expressions are removed from the
    /// recipe; use `replace` if removal of unused expressions is desired.
    /// Consumes `self` and returns a replacement recipe.
    // crate viz for tests
    pub(crate) fn extend(mut self, additions: &str) -> Result<Recipe, (Recipe, ReadySetError)> {
        // parse and compute differences to current recipe
        let add_rp = match Recipe::from_str(additions) {
            Ok(rp) => rp,
            Err(e) => return Err((self, e)),
        };
        let (added, _) = add_rp.compute_delta(&self);

        // move the incorporator state from the old recipe to the new one
        let prior_inc = self.inc.take();

        // build new recipe as clone of old one
        let mut new = Recipe {
            expressions: self.expressions.clone(),
            expression_order: self.expression_order.clone(),
            aliases: self.aliases.clone(),
            version: self.version + 1,
            inc: prior_inc,
            prior: None,
        };

        // apply changes
        for qid in added {
            let q = add_rp.expressions[&qid].clone();
            new.expressions.insert(qid, q);
            new.expression_order.push(qid);
        }

        for (n, qid) in &add_rp.aliases {
            if new.aliases.contains_key(n) && new.aliases[n] != *qid {
                self.inc = new.inc.take();
                return Err((
                    self,
                    ReadySetError::RecipeInvariantViolated(format!(
                        "Query name exists but existing query is different: {}",
                        n
                    )),
                ));
            }
        }
        new.aliases.extend(add_rp.aliases);
        // retain the old recipe for future reference
        new.prior = Some(Box::new(self));

        // return new recipe as replacement for self
        Ok(new)
    }

    pub fn clear_prior(&mut self) {
        self.prior = None;
    }

    /// Helper method to reparent a recipe. This is needed for the recovery logic to build
    /// recovery and original recipe (see `make_recovery`).
    pub(in crate::controller) fn set_prior(&mut self, new_prior: Recipe) {
        self.prior = Some(Box::new(new_prior));
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(super) fn sql_inc(&self) -> &SqlIncorporator {
        self.inc.as_ref().unwrap()
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(in crate::controller) fn set_sql_inc(&mut self, new_inc: SqlIncorporator) {
        self.inc = Some(new_inc);
    }

    fn parse(recipe_text: &str) -> ReadySetResult<Vec<(Option<String>, SqlQuery, bool)>> {
        let query_strings = Recipe::clean_queries(recipe_text);

        let parsed_queries = query_strings.iter().fold(
            Vec::new(),
            |mut acc: Vec<ReadySetResult<(bool, Option<&str>, SqlQuery)>>, query| {
                match query_exprs(query) {
                    Result::Err(e) => {
                        // we got a parse error
                        acc.push(Err(ReadySetError::UnparseableQuery {
                            query: query.clone(),
                        }));
                    }
                    Result::Ok((remainder, parsed)) => {
                        // should have consumed all input
                        if !remainder.is_empty() {
                            acc.push(Err(ReadySetError::UnparseableQuery {
                                query: query.clone(),
                            }));
                            return acc;
                        }
                        acc.extend(parsed.into_iter().map(|p| Ok(p)).collect::<Vec<_>>());
                    }
                }
                acc
            },
        );

        parsed_queries
            .into_iter()
            .map(|pr| {
                let (is_leaf, r, q) = pr?;
                Ok((r.map(String::from), q, is_leaf))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Returns the predecessor from which this `Recipe` was migrated to.
    // crate viz for tests
    pub(crate) fn prior(&self) -> Option<&Recipe> {
        self.prior.as_deref()
    }

    /// Remove the query with the given `name` from the recipe.
    pub(super) fn remove_query(&mut self, qname: &str) -> bool {
        let qid = self.aliases.get(qname).cloned();
        if qid.is_none() {
            warn!(%qname, "Query not found in expressions");
            return false;
        }
        let qid = qid.unwrap();

        self.aliases.remove(qname);
        if self.expressions.remove(&qid).is_some() {
            if let Some(i) = self.expression_order.iter().position(|&q| q == qid) {
                self.expression_order.remove(i);
                return true;
            }
        }
        false
    }

    /// Alias `query` as `alias`. Subsequent calls to `node_addr_for(alias)` will return the node
    /// addr for `query`.
    ///
    /// Returns an Err if `query` is not found in the recipe
    pub(super) fn alias_query(&mut self, query: &str, alias: String) -> Result<(), String> {
        // NOTE: this is (consciously) O(n) because we don't have a reverse index from query name to
        // QueryID and I don't feel like it's worth the time-space tradeoff given this is only
        // called on migration
        let qid = self
            .expressions
            .iter()
            .find(|(_, (name, _, _))| name.as_ref().map(|n| n.as_str()) == Some(query))
            .ok_or_else(|| "Query not found".to_string())?
            .0;
        self.aliases.insert(alias, *qid);
        Ok(())
    }

    /// Replace this recipe with a new one, retaining queries that exist in both. Any queries only
    /// contained in `new` (but not in `self`) will be added; any contained in `self`, but not in
    /// `new` will be removed.
    /// Consumes `self` and returns a replacement recipe.
    pub(super) fn replace(mut self, mut new: Recipe) -> Recipe {
        // generate replacement recipe with correct version and lineage
        new.version = self.version + 1;
        // retain the old incorporator but move it to the new recipe
        let prior_inc = self.inc.take();
        // retain the old recipe for future reference
        new.prior = Some(Box::new(self));
        // retain the previous `SqlIncorporator` state
        new.inc = prior_inc;

        // return new recipe as replacement for self
        new
    }

    /// Increments the version of a recipe. Returns the new version number.
    pub(super) fn next(&mut self) -> usize {
        self.version += 1;
        self.version
    }

    /// Returns the version number of this recipe.
    // crate viz for tests
    pub(crate) fn version(&self) -> usize {
        self.version
    }

    /// Reverts to prior version of recipe
    pub(super) fn revert(self) -> Recipe {
        if let Some(prior) = self.prior {
            *prior
        } else {
            Recipe::blank_with_config_from(&self)
        }
    }

    pub(super) fn queries_for_nodes(&self, nodes: Vec<NodeIndex>) -> Vec<String> {
        nodes
            .iter()
            .flat_map(|ni| {
                self.inc
                    .as_ref()
                    .expect("need SQL incorporator")
                    .get_queries_for_node(*ni)
            })
            .collect()
    }

    pub(super) fn make_recovery(&self, mut affected_queries: Vec<String>) -> (Recipe, Recipe) {
        affected_queries.sort();
        affected_queries.dedup();

        let mut recovery = self.clone();
        recovery.prior = Some(Box::new(self.clone()));
        recovery.next();

        // remove from recipe
        for q in affected_queries {
            warn!(query = %q, "query affected by failure");
            if !recovery.remove_query(&q) {
                warn!(query = %q, "Call to Recipe::remove_query() failed");
            }
        }

        let mut original = self.clone();
        original.next();
        original.next();

        (recovery, original)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_computes_delta() {
        let r0 = Recipe::blank();
        let q0 = sql_parser::parse_query(Dialect::MySQL, "SELECT a FROM b;").unwrap();
        let q1 =
            sql_parser::parse_query(Dialect::MySQL, "SELECT a, c FROM b WHERE x = 42;").unwrap();

        let q0_id = hash_query(&q0);
        let q1_id = hash_query(&q1);

        let pq_a = vec![(None, q0.clone(), true), (None, q1, true)];
        let r1 = Recipe::from_queries(pq_a).unwrap();

        // delta from empty recipe
        let (added, removed) = r1.compute_delta(&r0);
        assert_eq!(added.len(), 2);
        assert_eq!(removed.len(), 0);
        assert_eq!(added[0], q0_id);
        assert_eq!(added[1], q1_id);

        // delta with oneself should be nothing
        let (added, removed) = r1.compute_delta(&r1);
        assert_eq!(added.len(), 0);
        assert_eq!(removed.len(), 0);

        // bring on a new query set
        let q2 = sql_parser::parse_query(Dialect::MySQL, "SELECT c FROM b;").unwrap();
        let q2_id = hash_query(&q2);
        let pq_b = vec![(None, q0, true), (None, q2, true)];
        let r2 = Recipe::from_queries(pq_b).unwrap();

        // delta should show addition and removal
        let (added, removed) = r2.compute_delta(&r1);
        assert_eq!(added.len(), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(added[0], q2_id);
        assert_eq!(removed[0], q1_id);
    }

    #[test]
    fn it_replaces() {
        let r0 = Recipe::blank();
        assert_eq!(r0.version, 0);
        assert_eq!(r0.expressions.len(), 0);
        assert_eq!(r0.prior, None);

        let r0_copy = r0.clone();

        let r1_txt = "SELECT a FROM b;\nSELECT a, c FROM b WHERE x = 42;";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 2);
        assert_eq!(r1.prior, Some(Box::new(r0_copy)));

        let r1_copy = r1.clone();

        let r2_txt = "SELECT c FROM b;\nSELECT a, c FROM b;";
        let r2_t = Recipe::from_str(r2_txt).unwrap();
        let r2 = r1.replace(r2_t);
        assert_eq!(r2.version, 2);
        assert_eq!(r2.expressions.len(), 2);
        assert_eq!(r2.prior, Some(Box::new(r1_copy)));
    }

    #[test]
    fn it_handles_aliasing() {
        let r0 = Recipe::blank();

        let r1_txt = "q_0: SELECT a FROM b;\nq_1: SELECT a FROM b;";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 1);
        assert_eq!(r1.aliases.len(), 2);
        assert_eq!(r1.resolve_alias("q_1"), r1.resolve_alias("q_0"));
    }

    #[test]
    #[should_panic(expected = "Query name exists but existing query is different")]
    fn it_avoids_spurious_aliasing() {
        let r0 = Recipe::blank();

        let r1_txt = "q_0: SELECT a FROM b;\nq_1: SELECT a, c FROM b WHERE x = 42;";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 2);

        let r2_txt = "q_0: SELECT a, c FROM b WHERE x = 21;\nq_1: SELECT c FROM b;";
        // we expect this to panic, since both q_0 and q_1 already exist with a different
        // definition
        let r2 = r1.extend(r2_txt).unwrap();
        assert_eq!(r2.version, 2);
        assert_eq!(r2.expressions.len(), 4);
    }

    #[test]
    fn it_handles_multiple_statements_per_line() {
        let r0 = Recipe::blank();

        let r1_txt = "  QUERY q_0: SELECT a FROM b; QUERY q_1: SELECT x FROM y;";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.expressions.len(), 2);
    }

    #[test]
    fn it_handles_spaces() {
        let r0 = Recipe::blank();

        let r1_txt = "  QUERY q_0: SELECT a FROM b;\
                      QUERY q_1: SELECT x FROM y;";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.expressions.len(), 2);
    }

    #[test]
    fn it_handles_missing_semicolon() {
        let r0 = Recipe::blank();

        let r1_txt = "QUERY q_0: SELECT a FROM b;\nVIEW q_1: SELECT x FROM y";
        let r1_t = Recipe::from_str(r1_txt).unwrap();
        let r1 = r0.replace(r1_t);
        assert_eq!(r1.expressions.len(), 2);
    }

    #[test]
    fn display_parses() {
        let recipe =
            Recipe::from_str("CREATE TABLE b (a INT);\nQUERY q_0: SELECT a FROM b;").unwrap();
        let recipe_s = recipe.to_string();
        let res = Recipe::from_str(&recipe_s).unwrap();
        assert_eq!(res.expressions().len(), 2);
    }
}
