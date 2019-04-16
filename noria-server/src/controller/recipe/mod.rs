use crate::controller::security::SecurityConfig;
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::ReuseConfigType;
use dataflow::ops::trigger::Trigger;
use dataflow::ops::trigger::TriggerEvent;
use dataflow::prelude::DataType;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use noria::ActivationResult;
use petgraph::graph::NodeIndex;

use nom::{self, is_alphanumeric, multispace};
use nom_sql::CreateTableStatement;
use slog;
use std::collections::HashMap;
use std::str;
use std::vec::Vec;

type QueryID = u64;

/// Represents a Soup recipe.
#[derive(Clone, Debug)]
// crate viz for tests
crate struct Recipe {
    /// SQL queries represented in the recipe. Value tuple is (name, query, public).
    expressions: HashMap<QueryID, (Option<String>, SqlQuery, bool)>,
    /// Addition order for the recipe expressions
    expression_order: Vec<QueryID>,
    /// Named read/write expression aliases, mapping to queries in `expressions`.
    aliases: HashMap<String, QueryID>,
    /// Security configuration
    security_config: Option<SecurityConfig>,

    /// Recipe revision.
    version: usize,
    /// Preceding recipe.
    prior: Option<Box<Recipe>>,

    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: Option<SqlIncorporator>,

    log: slog::Logger,
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
fn is_ident(chr: u8) -> bool {
    is_alphanumeric(chr) || chr as char == '_'
}

named!(query_expr<&[u8], (bool, Option<String>, SqlQuery)>,
    do_parse!(
        prefix: opt!(do_parse!(
            public: opt!(alt_complete!(tag_no_case!("query") | tag_no_case!("view"))) >>
            opt!(complete!(multispace)) >>
            name: opt!(terminated!(map_res!(take_while1!(is_ident), str::from_utf8),
                                   opt!(complete!(multispace)))) >>
            tag!(":") >>
            opt!(complete!(multispace)) >>
            (public, name)
        )) >>
        expr: apply!(sql_parser::sql_query,) >>
        (match prefix {
            None => (false, None, expr),
            Some(p) => (p.0.is_some(), p.1.map(ToOwned::to_owned), expr)
        })
    )
);

#[allow(unused)]
impl Recipe {
    /// Return security groups in the recipe
    pub(in crate::controller) fn security_groups(&self) -> Vec<String> {
        match self.security_config {
            Some(ref config) => config.groups.keys().cloned().collect(),
            None => vec![],
        }
    }

    /// Return active aliases for expressions
    fn aliases(&self) -> Vec<&str> {
        self.aliases.keys().map(String::as_str).collect()
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    // crate viz for tests
    crate fn blank(log: Option<slog::Logger>) -> Recipe {
        Recipe {
            expressions: HashMap::default(),
            expression_order: Vec::default(),
            aliases: HashMap::default(),
            version: 0,
            prior: None,
            inc: match log {
                None => Some(SqlIncorporator::default()),
                Some(ref log) => Some(SqlIncorporator::new(log.clone())),
            },
            log: match log {
                None => slog::Logger::root(slog::Discard, o!()),
                Some(log) => log,
            },
            security_config: None,
        }
    }

    pub(super) fn with_version(version: usize, log: Option<slog::Logger>) -> Recipe {
        Recipe {
            version,
            ..Self::blank(log)
        }
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Disable node reuse.
    // crate viz for tests
    crate fn disable_reuse(&mut self) {
        self.inc.as_mut().unwrap().disable_reuse();
    }

    /// Enable reuse
    pub(super) fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.inc.as_mut().unwrap().enable_reuse(reuse_type)
    }

    fn resolve_alias(&self, alias: &str) -> Option<&str> {
        self.aliases.get(alias).map(|ref qid| {
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
                    Some(ref internal_qn) => inc.get_query_address(internal_qn),
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
                    Some(ref internal_qn) => inc.get_view_schema(internal_qn),
                };
                s.map(Schema::View)
            }
            Some(s) => Some(Schema::Table(s)),
        }
    }

    /// Set recipe's security configuration
    pub(in crate::controller) fn set_security_config(&mut self, config_text: &str) {
        let mut config = SecurityConfig::parse(config_text);
        self.security_config = Some(config);
    }

    /// Creates a recipe from a set of SQL queries in a string (e.g., read from a file).
    /// Note that the recipe is not backed by a Soup data-flow graph until `activate` is called on
    /// it.
    // crate viz for tests
    crate fn from_str(recipe_text: &str, log: Option<slog::Logger>) -> Result<Recipe, String> {
        // remove comment lines
        let lines: Vec<String> = recipe_text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("--"))
            .map(String::from)
            .collect();
        let cleaned_recipe_text = lines.join("\n");

        // parse and compute differences to current recipe
        let parsed_queries = Recipe::parse(&cleaned_recipe_text)?;

        Ok(Recipe::from_queries(parsed_queries, log))
    }

    /// Creates a recipe from a set of pre-parsed `SqlQuery` structures.
    /// Note that the recipe is not backed by a Soup data-flow graph until `activate` is called on
    /// it.
    fn from_queries(
        qs: Vec<(Option<String>, SqlQuery, bool)>,
        log: Option<slog::Logger>,
    ) -> Recipe {
        let mut aliases = HashMap::default();
        let mut expression_order = Vec::new();
        let mut duplicates = 0;
        let expressions = qs
            .into_iter()
            .map(|(n, q, is_leaf)| {
                let qid = hash_query(&q);
                if !expression_order.contains(&qid) {
                    expression_order.push(qid);
                } else {
                    duplicates += 1;
                }
                match n {
                    None => (),
                    Some(ref name) => {
                        assert!(
                            !aliases.contains_key(name) || aliases[name] == qid,
                            "Query name exists but existing query is different: {}",
                            name
                        );
                        aliases.insert(name.clone(), qid);
                    }
                }
                (qid, (n, q, is_leaf))
            })
            .collect::<HashMap<QueryID, (Option<String>, SqlQuery, bool)>>();

        let inc = match log {
            None => SqlIncorporator::default(),
            Some(ref log) => SqlIncorporator::new(log.clone()),
        };

        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(log) => log,
        };

        debug!(log, "{} duplicate queries", duplicates; "version" => 0);

        Recipe {
            expressions,
            expression_order,
            aliases,
            security_config: None,
            version: 0,
            prior: None,
            inc: Some(inc),
            log,
        }
    }

    /// Creates a new security universe
    pub(in crate::controller) fn create_universe(
        &mut self,
        mig: &mut Migration,
        universe_groups: HashMap<String, Vec<DataType>>,
    ) -> Result<ActivationResult, String> {
        use crate::controller::sql::security::Multiverse;

        let mut result = ActivationResult {
            new_nodes: HashMap::default(),
            removed_leaves: Vec::default(),
            expressions_added: 0,
            expressions_removed: 0,
        };

        if self.security_config.is_some() {
            let qfps = self.inc.as_mut().unwrap().prepare_universe(
                &self.security_config.clone().unwrap(),
                universe_groups,
                mig,
            )?;

            for qfp in qfps {
                result.new_nodes.insert(qfp.name.clone(), qfp.query_leaf);
            }
        }

        for expr in self.expressions.values() {
            let (n, q, is_leaf) = expr.clone();

            // add the universe-specific query
            // don't use query name to avoid conflict with global queries
            let (id, group) = mig.universe();
            let new_name = if n.is_some() {
                match group {
                    Some(ref g) => Some(format!(
                        "{}_{}{}",
                        n.clone().unwrap(),
                        g.to_string(),
                        id.to_string()
                    )),
                    None => Some(format!("{}_u{}", n.clone().unwrap(), id.to_string())),
                }
            } else {
                None
            };

            let is_leaf = if group.is_some() { false } else { is_leaf };

            let qfp = self
                .inc
                .as_mut()
                .unwrap()
                .add_parsed_query(q, new_name, is_leaf, mig)?;

            // If the user provided us with a query name, use that.
            // If not, use the name internally used by the QFP.
            let query_name = match n {
                Some(name) => name,
                None => qfp.name.clone(),
            };

            result.new_nodes.insert(query_name, qfp.query_leaf);
        }

        Ok(result)
    }

    /// Activate the recipe by migrating the Soup data-flow graph wrapped in `mig` to the recipe.
    /// This causes all necessary changes to said graph to be applied; however, it is the caller's
    /// responsibility to call `mig.commit()` afterwards.
    // crate viz for tests
    crate fn activate(&mut self, mig: &mut Migration) -> Result<ActivationResult, String> {
        debug!(self.log, "{} queries, {} of which are named",
                                 self.expressions.len(),
                                 self.aliases.len(); "version" => self.version);

        let (added, removed) = match self.prior {
            None => self.compute_delta(&Recipe::blank(None)),
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
            self.inc.as_mut().unwrap().upgrade_schema(self.version);
        }

        // create nodes to enforce security configuration
        if self.security_config.is_some() {
            info!(
                self.log,
                "Found a security configuration, bootstrapping groups..."
            );
            let config = self.security_config.take().unwrap();
            for group in config.groups.values() {
                info!(
                    self.log,
                    "Creating membership view for group {}",
                    group.name()
                );
                let qfp = self.inc.as_mut().unwrap().add_parsed_query(
                    group.membership(),
                    Some(group.name()),
                    true,
                    mig,
                )?;

                /// Add trigger node below group membership views
                let group_creation = TriggerEvent::GroupCreation {
                    group: group.name(),
                };
                let trigger = Trigger::new(qfp.query_leaf, group_creation, 1);
                mig.add_ingredient(
                    &format!("{}-trigger", group.name()),
                    &["uid", "gid"],
                    trigger,
                );

                result.new_nodes.insert(group.name(), qfp.query_leaf);
            }

            self.security_config = Some(config);
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

            // If the user provided us with a query name, use that.
            // If not, use the name internally used by the QFP.
            let query_name = match n {
                Some(name) => name,
                None => qfp.name.clone(),
            };

            result.new_nodes.insert(query_name, qfp.query_leaf);
        }

        result.removed_leaves = removed
            .iter()
            .filter_map(|qid| {
                let (ref n, ref q, _) = self.prior.as_ref().unwrap().expressions[qid];
                match q {
                    SqlQuery::CreateTable(ref ctq) => {
                        // a base may have many dependent queries, including ones that also lost
                        // nodes; the code handling `removed_leaves` therefore needs to take care
                        // not to remove bases while they still have children, or to try removing
                        // them twice.
                        self.inc.as_mut().unwrap().remove_base(&ctq.table.name);
                        match self.prior.as_ref().unwrap().node_addr_for(&ctq.table.name) {
                            Ok(ni) => Some(ni),
                            Err(e) => {
                                crit!(
                                    self.log,
                                    "failed to remove base {} whose  address could not be resolved",
                                    ctq.table.name
                                );
                                unimplemented!()
                            }
                        }
                    }
                    _ => self
                        .inc
                        .as_mut()
                        .unwrap()
                        .remove_query(n.as_ref().unwrap(), mig),
                }
            })
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
    crate fn expressions(&self) -> Vec<(Option<&String>, &SqlQuery)> {
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
    crate fn extend(mut self, additions: &str) -> Result<Recipe, (Recipe, String)> {
        // parse and compute differences to current recipe
        let add_rp = match Recipe::from_str(additions, None) {
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
            log: self.log.clone(),
            security_config: self.security_config.clone(),
            // retain the old recipe for future reference
            prior: Some(Box::new(self)),
        };

        // apply changes
        for qid in added {
            let q = add_rp.expressions[&qid].clone();
            new.expressions.insert(qid, q);
            new.expression_order.push(qid);
        }

        for (n, qid) in &add_rp.aliases {
            assert!(
                !new.aliases.contains_key(n) || new.aliases[n] == *qid,
                "Query name exists but existing query is different: {}",
                n
            );
        }
        new.aliases.extend(add_rp.aliases);

        // return new recipe as replacement for self
        Ok(new)
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

    fn parse(recipe_text: &str) -> Result<Vec<(Option<String>, SqlQuery, bool)>, String> {
        let lines: Vec<&str> = recipe_text
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                // remove inline comments, too
                match l.find('#') {
                    None => l.trim(),
                    Some(pos) => &l[0..pos - 1].trim(),
                }
            })
            .collect();
        let mut query_strings = Vec::new();
        let mut q = String::new();
        for l in lines {
            if !l.ends_with(';') {
                q.push_str(l);
                q.push_str(" ");
            } else {
                // end of query
                q.push_str(l);
                query_strings.push(q);
                q = String::new();
            }
        }

        let parsed_queries = query_strings
            .iter()
            .map(|q| (q, query_expr(q.as_bytes())))
            .collect::<Vec<_>>();

        if !parsed_queries.iter().all(|pq| pq.1.is_done()) {
            for pq in parsed_queries {
                match pq.1 {
                    nom::IResult::Error(e) => {
                        return Err(format!("Query \"{}\", parse error: {}", pq.0, e));
                    }
                    nom::IResult::Done(_, _) => (),
                    nom::IResult::Incomplete(_) => unreachable!(),
                }
            }
            return Err("Failed to parse recipe!".to_string());
        }

        Ok(parsed_queries
            .into_iter()
            .map(|(_, t)| {
                let pr = t.unwrap().1;
                (pr.1, pr.2, pr.0)
            })
            .collect::<Vec<_>>())
    }

    /// Returns the predecessor from which this `Recipe` was migrated to.
    // crate viz for tests
    crate fn prior(&self) -> Option<&Recipe> {
        self.prior.as_ref().map(|p| &**p)
    }

    fn remove_query(&mut self, qname: &str) -> bool {
        let qid = self.aliases.get(qname).cloned();
        if qid.is_none() {
            warn!(self.log, "Query {} not found in expressions", qname);
            return false;
        }
        let qid = qid.unwrap();

        self.aliases.remove(qname);
        self.expressions.remove(&qid).is_some() && self.expression_order.remove_item(&qid).is_some()
    }

    /// Replace this recipe with a new one, retaining queries that exist in both. Any queries only
    /// contained in `new` (but not in `self`) will be added; any contained in `self`, but not in
    /// `new` will be removed.
    /// Consumes `self` and returns a replacement recipe.
    pub(super) fn replace(mut self, mut new: Recipe) -> Result<Recipe, String> {
        // generate replacement recipe with correct version and lineage
        new.version = self.version + 1;
        // retain the old incorporator but move it to the new recipe
        let prior_inc = self.inc.take();
        // retain security configuration
        new.security_config = self.security_config.take();
        // retain the old recipe for future reference
        new.prior = Some(Box::new(self));
        // retain the previous `SqlIncorporator` state
        new.inc = prior_inc;

        // return new recipe as replacement for self
        Ok(new)
    }

    /// Increments the version of a recipe. Returns the new version number.
    pub(super) fn next(&mut self) -> usize {
        self.version += 1;
        self.version
    }

    /// Returns the version number of this recipe.
    // crate viz for tests
    crate fn version(&self) -> usize {
        self.version
    }

    /// Reverts to prior version of recipe
    pub(super) fn revert(self) -> Recipe {
        if let Some(prior) = self.prior {
            *prior
        } else {
            Recipe::blank(Some(self.log))
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
            warn!(self.log, "query {} affected by failure", q);
            if !recovery.remove_query(&q) {
                warn!(self.log, "Call to Recipe::remove_query() failed for {}", q);
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
        let r0 = Recipe::blank(None);
        let q0 = sql_parser::parse_query("SELECT a FROM b;").unwrap();
        let q1 = sql_parser::parse_query("SELECT a, c FROM b WHERE x = 42;").unwrap();

        let q0_id = hash_query(&q0);
        let q1_id = hash_query(&q1);

        let pq_a = vec![(None, q0.clone(), true), (None, q1.clone(), true)];
        let r1 = Recipe::from_queries(pq_a, None);

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
        let q2 = sql_parser::parse_query("SELECT c FROM b;").unwrap();
        let q2_id = hash_query(&q2);
        let pq_b = vec![(None, q0, true), (None, q2.clone(), true)];
        let r2 = Recipe::from_queries(pq_b, None);

        // delta should show addition and removal
        let (added, removed) = r2.compute_delta(&r1);
        assert_eq!(added.len(), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(added[0], q2_id);
        assert_eq!(removed[0], q1_id);
    }

    #[test]
    fn it_replaces() {
        let r0 = Recipe::blank(None);
        assert_eq!(r0.version, 0);
        assert_eq!(r0.expressions.len(), 0);
        assert_eq!(r0.prior, None);

        let r0_copy = r0.clone();

        let r1_txt = "SELECT a FROM b;\nSELECT a, c FROM b WHERE x = 42;";
        let r1_t = Recipe::from_str(r1_txt, None).unwrap();
        let r1 = r0.replace(r1_t).unwrap();
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 2);
        assert_eq!(r1.prior, Some(Box::new(r0_copy)));

        let r1_copy = r1.clone();

        let r2_txt = "SELECT c FROM b;\nSELECT a, c FROM b;";
        let r2_t = Recipe::from_str(r2_txt, None).unwrap();
        let r2 = r1.replace(r2_t).unwrap();
        assert_eq!(r2.version, 2);
        assert_eq!(r2.expressions.len(), 2);
        assert_eq!(r2.prior, Some(Box::new(r1_copy)));
    }

    #[test]
    #[should_panic(expected = "Query name exists but existing query is different")]
    fn it_avoids_spurious_aliasing() {
        let r0 = Recipe::blank(None);

        let r1_txt = "q_0: SELECT a FROM b;\nq_1: SELECT a, c FROM b WHERE x = 42;";
        let r1_t = Recipe::from_str(r1_txt, None).unwrap();
        let r1 = r0.replace(r1_t).unwrap();
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 2);

        let r2_txt = "q_0: SELECT a, c FROM b WHERE x = 21;\nq_1: SELECT c FROM b;";
        // we expect this to panic, since both q_0 and q_1 already exist with a different
        // definition
        let r2 = r1.extend(r2_txt).unwrap();
        assert_eq!(r2.version, 2);
        assert_eq!(r2.expressions.len(), 4);
    }
}
