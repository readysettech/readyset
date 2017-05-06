use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use {SqlIncorporator, Migration, NodeAddress};

use slog;
use std::collections::HashMap;
use std::str;
use std::vec::Vec;

type QueryID = u64;

/// Represents the result of a recipe activation.
#[derive(Clone, Debug)]
pub struct ActivationResult {
    /// Map of query names to `NodeAddress` handles for reads/writes.
    pub new_nodes: HashMap<String, NodeAddress>,
    /// Number of expressions the recipe added compared to the prior recipe.
    pub expressions_added: usize,
    /// Number of expressions the recipe removed compared to the prior recipe.
    pub expressions_removed: usize,
}

/// Represents a Soup recipe.
#[derive(Clone, Debug)]
pub struct Recipe {
    /// SQL queries represented in the recipe. Value tuple is (name, query).
    expressions: HashMap<QueryID, (Option<String>, SqlQuery)>,
    /// Addition order for the recipe expressions
    expression_order: Vec<QueryID>,
    /// Named read/write expression aliases, mapping to queries in `expressions`.
    aliases: HashMap<String, QueryID>,

    /// Recipe revision.
    version: usize,
    /// Preceding recipe.
    prior: Option<Box<Recipe>>,

    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: Option<SqlIncorporator>,

    log: slog::Logger,
}

impl PartialEq for Recipe {
    /// Equality for recipes is defined in terms of all members apart from `inc`.
    fn eq(&self, other: &Recipe) -> bool {
        self.expressions == other.expressions && self.expression_order == other.expression_order &&
        self.aliases == other.aliases && self.version == other.version &&
        self.prior == other.prior
    }
}

fn hash_query(q: &SqlQuery) -> QueryID {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

impl Recipe {
    /// Return active aliases for expressions
    pub fn aliases(&self) -> Vec<&str> {
        self.aliases.keys().map(String::as_str).collect()
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    pub fn blank(log: Option<slog::Logger>) -> Recipe {
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
        }
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Obtains the `NodeAddress` for the node corresponding to a named query or a write type.
    pub fn node_addr_for(&self, name: &str) -> Result<NodeAddress, String> {
        match self.inc {
            Some(ref inc) => {
                // `name` might be an alias for another identical query, so resolve via QID here
                // TODO(malte): better error handling
                let na = match self.aliases.get(name) {
                    None => inc.get_query_address(name),
                    Some(ref qid) => {
                        let (ref internal_qn, _) = self.expressions[qid];
                        inc.get_query_address(internal_qn.as_ref().unwrap())
                    }
                };
                match na {
                    None => {
                        Err(format!("No flow graph node for \"{}\" exists at v{}",
                                    name,
                                    self.version))
                    }
                    Some(na) => Ok(na),
                }
            }
            None => Err(format!("Recipe not applied")),
        }
    }

    /// Creates a recipe from a set of SQL queries in a string (e.g., read from a file).
    /// Note that the recipe is not backed by a Soup data-flow graph until `activate` is called on
    /// it.
    pub fn from_str(recipe_text: &str, log: Option<slog::Logger>) -> Result<Recipe, String> {
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
    pub fn from_queries(qs: Vec<(Option<String>, SqlQuery)>, log: Option<slog::Logger>) -> Recipe {
        let mut aliases = HashMap::default();
        let mut expression_order = Vec::new();
        let mut duplicates = 0;
        let expressions = qs.into_iter()
            .map(|(n, q)| {
                let qid = hash_query(&q);
                if !expression_order.contains(&qid) {
                    expression_order.push(qid);
                } else {
                    duplicates += 1;
                }
                match n {
                    None => (),
                    Some(ref name) => {
                        aliases.insert(name.clone(), qid);
                    }
                }
                (qid.into(), (n, q))
            })
            .collect::<HashMap<QueryID, (Option<String>, SqlQuery)>>();

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
            expressions: expressions,
            expression_order: expression_order,
            aliases: aliases,
            version: 0,
            prior: None,
            inc: Some(inc),
            log: log,
        }
    }

    /// Activate the recipe by migrating the Soup data-flow graph wrapped in `mig` to the recipe.
    /// This causes all necessary changes to said graph to be applied; however, it is the caller's
    /// responsibility to call `mig.commit()` afterwards.
    pub fn activate(&mut self,
                    mig: &mut Migration,
                    transactional_base_nodes: bool)
                    -> Result<ActivationResult, String> {
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
            expressions_added: added.len(),
            expressions_removed: removed.len(),
        };

        // upgrade schema version *before* applying changes, so that new queries are correctly
        // tagged with the new version. If this recipe was just created, there is no need to
        // upgrade the schema version, as the SqlIncorporator's version will still be at zero.
        if self.version > 0 {
            self.inc.as_mut().unwrap().upgrade_schema(self.version);
        }

        self.inc
            .as_mut()
            .unwrap()
            .set_transactional(transactional_base_nodes);

        // add new queries to the Soup graph carried by `mig`, and reflect state in the
        // incorporator in `inc`. `NodeAddress`es for new nodes are collected in `new_nodes` to be
        // returned to the caller (who may use them to obtain mutators and getters)
        for qid in added {
            let (n, q) = self.expressions[&qid].clone();

            // add the query
            let qfp = self.inc.as_mut().unwrap().add_parsed_query(q, n, mig)?;

            // we currently use a domain per query
            let d = mig.add_domain();
            for na in qfp.new_nodes.iter() {
                mig.assign_domain(na.clone(), d);
            }
            result.new_nodes.insert(qfp.name.clone(), qfp.query_leaf);
        }

        // TODO(malte): deal with removal.
        for qid in removed {
            error!(self.log, "Unhandled query removal of {:?}", qid; "version" => self.version);
            //unimplemented!()
        }

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
    pub fn expressions(&self) -> Vec<(Option<&String>, &SqlQuery)> {
        self.expressions
            .values()
            .map(|&(ref n, ref q)| (n.as_ref(), q))
            .collect()
    }

    /// Append the queries in the `additions` argument to this recipe. This will attempt to parse
    /// `additions`, and if successful, will extend the recipe. No expressions are removed from the
    /// recipe; use `replace` if removal of unused expressions is desired.
    /// Consumes `self` and returns a replacement recipe.
    pub fn extend(mut self, additions: &str) -> Result<Recipe, String> {
        // parse and compute differences to current recipe
        let add_rp = Recipe::from_str(additions, None)?;
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
            // retain the old recipe for future reference
            prior: Some(Box::new(self)),
        };

        // apply changes
        for qid in added {
            let q = add_rp.expressions[&qid].clone();
            new.expressions.insert(qid, q);
            new.expression_order.push(qid);
        }

        // return new recipe as replacement for self
        Ok(new)
    }

    fn parse(recipe_text: &str) -> Result<Vec<(Option<String>, SqlQuery)>, String> {
        let lines: Vec<&str> = recipe_text
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| {
                     // remove inline comments, too
                     match l.find("#") {
                         None => l.trim(),
                         Some(pos) => &l[0..pos - 1].trim(),
                     }
                 })
            .collect();
        let mut query_strings = Vec::new();
        let mut q = String::new();
        for l in lines {
            if !l.ends_with(";") {
                q.push_str(l);
            } else {
                // end of query
                q.push_str(l);
                query_strings.push(q);
                q = String::new();
            }
        }

        let parsed_queries = query_strings
            .iter()
            .map(|ref q| {
                let r: Vec<&str> = q.splitn(2, ":").map(|s| s.trim()).collect();
                if r.len() == 2 {
                    // named query
                    let q = r[1];
                    let name = Some(String::from(r[0]));
                    (name, q.clone(), sql_parser::parse_query(q))
                } else {
                    // unnamed query
                    let q = r[0];
                    (None, q.clone(), sql_parser::parse_query(q))
                }
            })
            .collect::<Vec<_>>();

        if !parsed_queries.iter().all(|pq| pq.2.is_ok()) {
            for pq in parsed_queries {
                match pq.2 {
                    Err(e) => return Err(format!("Query \"{}\", parse error: {}", pq.1, e)),
                    Ok(_) => (),
                }
            }
            return Err(format!("Failed to parse recipe!"));
        }

        Ok(parsed_queries
               .into_iter()
               .map(|t| (t.0, t.2.unwrap()))
               .collect::<Vec<_>>())
    }

    /// Returns the predecessor from which this `Recipe` was migrated to.
    pub fn prior(&self) -> Option<&Box<Recipe>> {
        self.prior.as_ref()
    }

    /// Replace this recipe with a new one, retaining queries that exist in both. Any queries only
    /// contained in `new` (but not in `self`) will be added; any contained in `self`, but not in
    /// `new` will be removed.
    /// Consumes `self` and returns a replacement recipe.
    pub fn replace(mut self, mut new: Recipe) -> Result<Recipe, String> {
        // generate replacement recipe with correct version and lineage
        new.version = self.version + 1;
        // retain the old incorporator but move it to the new recipe
        let prior_inc = self.inc.take();
        // retain the old recipe for future reference
        new.prior = Some(Box::new(self));
        // retain the previous `SqlIncorporator` state
        new.inc = prior_inc;

        // return new recipe as replacement for self
        Ok(new)
    }

    /// Returns the version number of this recipe.
    pub fn version(&self) -> usize {
        self.version
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

        let pq_a = vec![(None, q0.clone()), (None, q1.clone())];
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
        let pq_b = vec![(None, q0), (None, q2.clone())];
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
}
