use nom_sql::Column;
use nom_sql::ConditionExpression::*;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::controller::sql::query_graph::{OutputColumn, QueryGraph, QueryGraphEdge};

pub trait Signature {
    fn signature(&self) -> QuerySignature;
}

#[derive(Clone, Debug)]
pub struct QuerySignature<'a> {
    pub relations: HashSet<&'a str>,
    pub attributes: HashSet<&'a Column>,
    pub hash: u64,
}

impl<'a> PartialEq for QuerySignature<'a> {
    fn eq(&self, other: &QuerySignature) -> bool {
        self.hash == other.hash
    }
}

impl<'a> Eq for QuerySignature<'a> {}

impl<'a> Hash for QuerySignature<'a> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        state.write_u64(self.hash)
    }
}

impl<'a> QuerySignature<'a> {
    pub fn is_generalization_of(&self, other: &QuerySignature) -> bool {
        // if the queries are the same, they are (non-strict) generalizations of each other
        if self.hash == other.hash {
            return true;
        }

        // to be a generalization, we must have
        // 1) either the same relations as in `other`, or a subset of them
        if !self.relations.is_subset(&other.relations) {
            return false;
        }

        // 2) either the same attributes as in `other`, or a subset of them
        if !self.attributes.is_subset(&other.attributes) {
            return false;
        }

        true
    }

    // Checks if a query is a weak generalization of the other by analyzing their
    // relations.
    pub fn is_weak_generalization_of(&self, other: &QuerySignature) -> bool {
        // if the queries are the same, they are (non-strict) generalizations of each other
        if self.hash == other.hash {
            return true;
        }

        // to be a generalization, we must have
        // 1) either the same relations as in `other`, or a subset of them
        if self.relations.is_disjoint(&other.relations) {
            return false;
        }

        true
    }
}

impl Signature for QueryGraph {
    /// Used to get a concise signature for a query graph. The `hash` member can be used to check
    /// for identical sets of relations and attributes covered (as per Finkelstein algorithm),
    /// while `relations` and `attributes` as `HashSet`s that allow for efficient subset checks.
    ///
    /// *N.B.:* Equal query signatures do *NOT* imply that queries are identical! Instead, it
    /// merely means that the queries:
    ///  1) refer to the same relations
    ///  2) mention the same columns as attributes
    /// Importantly, this does *NOT* say anything about the operators used in comparisons, literal
    /// values compared against, or even which columns are compared. It is the responsibilty of the
    /// caller to do a deeper comparison of the queries.
    fn signature(&self) -> QuerySignature {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        let rels = self.relations.keys().map(|r| String::as_str(r)).collect();

        // Compute relations part of hash
        let mut r_vec: Vec<&str> = self.relations.keys().map(String::as_str).collect();
        r_vec.sort();
        for r in &r_vec {
            r.hash(&mut hasher);
        }

        // Collect attributes from predicates and projected columns
        let mut attrs = HashSet::<&Column>::new();
        let mut attrs_vec = Vec::<&Column>::new();
        for n in self.relations.values() {
            for p in &n.predicates {
                match *p {
                    ComparisonOp(ref ct) | LogicalOp(ref ct) => {
                        for c in &ct.contained_columns() {
                            attrs_vec.push(c);
                            attrs.insert(c);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        for e in self.edges.values() {
            match *e {
                QueryGraphEdge::Join(ref join_predicates)
                | QueryGraphEdge::LeftJoin(ref join_predicates) => {
                    for p in join_predicates {
                        for c in &p.contained_columns() {
                            attrs_vec.push(c);
                            attrs.insert(c);
                        }
                    }
                }
                QueryGraphEdge::GroupBy(ref cols) => {
                    for c in cols {
                        attrs_vec.push(c);
                        attrs.insert(c);
                    }
                }
            }
        }

        // Global predicates are part of the attributes too
        for p in &self.global_predicates {
            match *p {
                ComparisonOp(ref ct) | LogicalOp(ref ct) => {
                    for c in &ct.contained_columns() {
                        attrs_vec.push(c);
                        attrs.insert(c);
                    }
                }
                _ => unreachable!(),
            }
        }

        // Compute attributes part of hash
        attrs_vec.sort();
        for a in &attrs_vec {
            a.hash(&mut hasher);
        }

        let proj_columns: Vec<&OutputColumn> = self.columns.iter().collect();
        // Compute projected columns part of hash. In the strict definition of the Finkelstein
        // query graph equivalence problem, we should not sort the columns here, since their order
        // doesn't matter in the query graph. However, we would like to avoid spurious ExactMatch
        // reuse cases and reproject incorrectly ordered columns, so we actually reflect the
        // column order in the query signature.
        for c in proj_columns {
            c.hash(&mut hasher);
        }

        QuerySignature {
            relations: rels,
            attributes: attrs,
            hash: hasher.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_does_subsets() {
        let a_c1 = Column::from("a.c1");
        let b_c3 = Column::from("b.c3");
        let a_c1_2 = Column::from("a.c1");

        {
            let mut a = QuerySignature {
                relations: HashSet::new(),
                attributes: HashSet::new(),
                hash: 0, // bogus value, but must be != to b's
            };
            a.relations.insert("r_a");
            a.relations.insert("r_b");
            a.attributes.insert(&a_c1);
            a.attributes.insert(&b_c3);

            let mut b = QuerySignature {
                relations: HashSet::new(),
                attributes: HashSet::new(),
                hash: 1, // bogus value, but must be != to a's
            };
            b.relations.insert("r_a");
            b.relations.insert("r_b");
            b.attributes.insert(&a_c1_2);

            assert!(b.is_generalization_of(&a));
        }
    }

    #[test]
    fn it_generalizes() {
        use crate::controller::sql::query_graph::to_query_graph;
        use nom_sql::parser::{parse_query, SqlQuery};

        let qa =
            parse_query("SELECT a.c1, b.c3 FROM a, b WHERE a.c1 = b.c1 AND a.c2 = 42;").unwrap();
        let qb = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 = b.c1;").unwrap();
        let qc = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 = b.c1 AND b.c4 = 21;").unwrap();

        let qga = match qa {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgb = match qb {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgc = match qc {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };

        let qsa = qga.signature();
        let qsb = qgb.signature();
        let qsc = qgc.signature();

        // b is more general than a
        assert!(qsb.is_generalization_of(&qsa));
        // but not vice versa
        assert!(!qsa.is_generalization_of(&qsb));

        // c is NOT more general than a
        assert!(!qsc.is_generalization_of(&qsa));
        // and neither vice versa because they have disjoint attribute sets
        assert!(!qsa.is_generalization_of(&qsc));
    }

    #[test]
    fn it_compares_signatures() {
        use crate::controller::sql::query_graph::to_query_graph;
        use nom_sql::parser::{parse_query, SqlQuery};

        let qa = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 = 42;").unwrap();
        let qb = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 > 42;").unwrap();
        let qc = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 = 42 AND b.c4 = a.c2;").unwrap();
        let qd = parse_query("SELECT b.c3 FROM a, b WHERE a.c1 = 21 AND b.c4 = a.c2;").unwrap();

        let qga = match qa {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgb = match qb {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgc = match qc {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgd = match qd {
            SqlQuery::Select(ref q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };

        let qsa = qga.signature();
        let qsb = qgb.signature();
        let qsc = qgc.signature();
        let qsd = qgd.signature();

        // identical queries = identical signatures
        assert_eq!(qsa, qsa);
        // even if operators differ
        assert_eq!(qsa, qsb);
        // ... or if literals differ
        assert_eq!(qsc, qsd);
        // ... but not if additional predicates exist
        assert_ne!(qsa, qsc);
    }
}
