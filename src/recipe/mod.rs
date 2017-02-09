use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use {SqlIncorporator, Migration};

use std::collections::HashMap;
use std::str;
use std::vec::Vec;

type QueryID = u64;

#[derive(Clone, Debug, PartialEq)]
pub struct Recipe {
    /// SQL queries represented in the recipe
    expressions: HashMap<QueryID, SqlQuery>,
    /// Named read/write expression aliases, mapping to queries in `expressions`.
    aliases: HashMap<String, QueryID>,
    /// Recipe revision.
    version: usize,
    /// Preceding recipe.
    prior: Option<Box<Recipe>>,
    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: Option<SqlIncorporator>,
}

fn hash_query(q: &SqlQuery) -> QueryID {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

impl Recipe {
    pub fn blank() -> Recipe {
        Recipe {
            expressions: HashMap::default(),
            aliases: HashMap::default(),
            version: 0,
            prior: None,
            inc: None,
        }
    }

    pub fn from_str(recipe_text: &str) -> Result<Recipe, String> {
        // parse and compute differences to current recipe
        let parsed_queries = Recipe::parse(recipe_text)?;
        Ok(Recipe::from_queries(parsed_queries))
    }

    pub fn from_queries(qs: Vec<SqlQuery>) -> Recipe {
        let expressions = qs.iter()
            .map(|q| {
                let qid = hash_query(q);
                (qid.into(), q.clone())
            })
            .collect::<HashMap<QueryID, SqlQuery>>();

        Recipe {
            expressions: expressions,
            aliases: HashMap::default(),
            version: 0,
            prior: None,
            inc: None,
        }
    }

    pub fn activate(&mut self, mig: &mut Migration) -> Result<(), String> {
        let (added, _removed) = match self.prior {
            None => self.compute_delta(&Recipe::blank()),
            Some(ref pr) => {
                // compute delta over prior recipe
                self.compute_delta(pr)
            }
        };

        // lazily instantiate `SqlIncorporator` if we don't have one already
        match self.inc {
            None => self.inc = Some(SqlIncorporator::default()),
            Some(_) => (),
        }

        // add new queries to the Soup graph carried by `mig`, and reflect state in the
        // incorporator in `inc`
        for (_, q) in added {
            self.inc.as_mut().unwrap().add_parsed_query(q, None, mig)?;
        }

        // TODO(malte): deal with removal.
        Ok(())
    }

    /// Work out the delta between two recipes.
    /// Returns two sets of `QueryID` -> `SqlQuery` mappings:
    /// (1) those queries present in `self`, but not in `other`; and
    /// (2) those queries present in `other` , but not in `self`.
    fn compute_delta(&self,
                     other: &Recipe)
                     -> (HashMap<QueryID, SqlQuery>, HashMap<QueryID, SqlQuery>) {
        let mut added_queries: HashMap<QueryID, SqlQuery> = HashMap::new();
        let mut removed_queries = HashMap::new();
        for (qid, q) in self.expressions.iter() {
            if !other.expressions.contains_key(qid) {
                added_queries.insert(*qid, q.clone());
            }
        }
        for (qid, q) in other.expressions.iter() {
            if !self.expressions.contains_key(qid) {
                removed_queries.insert(*qid, q.clone());
            }
        }

        (added_queries, removed_queries)
    }

    pub fn extend(mut self, additions: &str) -> Result<Recipe, String> {
        // parse and compute differences to current recipe
        let add_rp = Recipe::from_str(additions)?;
        let (added, _) = add_rp.compute_delta(&self);

        // move the incorporator state from the old recipe to the new one
        let prior_inc = self.inc.take();

        // build new recipe as clone of old one
        let mut new = Recipe {
            expressions: self.expressions.clone(),
            aliases: self.aliases.clone(),
            version: self.version + 1,
            // retain the old recipe for future reference
            prior: Some(Box::new(self)),
            inc: prior_inc,
        };

        // apply changes
        for (qid, q) in added {
            new.expressions.insert(qid, q);
        }

        // return new recipe as replacement for self
        Ok(new)
    }

    fn parse(recipe_text: &str) -> Result<Vec<SqlQuery>, String> {
        let lines: Vec<String> = recipe_text.lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| if !(l.ends_with("\n") || l.ends_with(";")) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            })
            .collect();

        let parsed_queries = lines.iter()
            .map(|ref q| (q.clone(), sql_parser::parse_query(q)))
            .collect::<Vec<_>>();

        if !parsed_queries.iter().all(|pq| pq.1.is_ok()) {
            println!("Failed to parse recipe!");
            for pq in parsed_queries {
                match pq.1 {
                    Err(e) => println!("Query \"{}\", parse error: {}", pq.0, e),
                    Ok(_) => (),
                }
            }
            return Err(String::from("Failed to parse recipe!"));
        }

        Ok(parsed_queries.into_iter().map(|t| t.1.unwrap()).collect::<Vec<_>>())
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_computes_delta() {
        let r0 = Recipe::blank();
        let q0 = sql_parser::parse_query("SELECT a FROM b;").unwrap();
        let q1 = sql_parser::parse_query("SELECT a, c FROM b WHERE x = 42;").unwrap();

        let q0_id = hash_query(&q0);
        let q1_id = hash_query(&q1);

        let pq_a = vec![q0.clone(), q1.clone()];
        let r1 = Recipe::from_queries(pq_a);

        // delta from empty recipe
        let (added, removed) = r1.compute_delta(&r0);
        assert_eq!(added.len(), 2);
        assert_eq!(removed.len(), 0);
        assert_eq!(added[&q0_id], q0);
        assert_eq!(added[&q1_id], q1);

        // delta with oneself should be nothing
        let (added, removed) = r1.compute_delta(&r1);
        assert_eq!(added.len(), 0);
        assert_eq!(removed.len(), 0);

        // bring on a new query set
        let q2 = sql_parser::parse_query("SELECT c FROM b;").unwrap();
        let q2_id = hash_query(&q2);
        let pq_b = vec![q0, q2.clone()];
        let r2 = Recipe::from_queries(pq_b);

        // delta should show addition and removal
        let (added, removed) = r2.compute_delta(&r1);
        assert_eq!(added.len(), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(added[&q2_id], q2);
        assert_eq!(removed[&q1_id], q1);
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
        let r1 = r0.replace(r1_t).unwrap();
        assert_eq!(r1.version, 1);
        assert_eq!(r1.expressions.len(), 2);
        assert_eq!(r1.prior, Some(Box::new(r0_copy)));

        let r1_copy = r1.clone();

        let r2_txt = "SELECT c FROM b;\nSELECT a, c FROM b;";
        let r2_t = Recipe::from_str(r2_txt).unwrap();
        let r2 = r1.replace(r2_t).unwrap();
        assert_eq!(r2.version, 2);
        assert_eq!(r2.expressions.len(), 2);
        assert_eq!(r2.prior, Some(Box::new(r1_copy)));
    }

    #[test]
    fn it_activates() {
        use Blender;

        let r_txt = "INSERT INTO b (a, c, x) VALUES (?, ?, ?);\n";
        let mut r = Recipe::from_str(r_txt).unwrap();
        assert_eq!(r.version, 0);
        assert_eq!(r.expressions.len(), 1);
        assert_eq!(r.prior, None);

        let mut g = Blender::new();
        {
            let mut mig = g.start_migration();
            assert!(r.activate(&mut mig).is_ok());
            mig.commit();
        }
        // source, base, ingress, ts-ingress, ts-egress
        assert_eq!(g.graph().node_count(), 5);
        println!("{}", g);
    }
}
