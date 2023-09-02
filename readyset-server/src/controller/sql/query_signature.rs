use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use nom_sql::analysis::ReferredColumns;
use nom_sql::Column;

use crate::controller::sql::query_graph::{OutputColumn, QueryGraph, QueryGraphEdge};

pub trait Signature {
    fn signature(&self) -> QuerySignature;
}

// TODO: Change relations to Hashset<&'a Relation>
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
    /// values compared against, or even which columns are compared. It is the responsibility of the
    /// caller to do a deeper comparison of the queries.
    fn signature(&self) -> QuerySignature {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        let rels = self.relations.keys().map(|r| r.name.as_str()).collect();

        // Compute relations part of hash
        let mut r_vec: Vec<_> = self.relations.keys().collect();
        r_vec.sort_unstable();
        for r in r_vec {
            r.hash(&mut hasher);
        }

        // Collect attributes from predicates and projected columns
        let mut attrs = HashSet::<&Column>::new();
        let mut attrs_vec = Vec::<&Column>::new();
        let mut record_column = |c| {
            attrs_vec.push(c);
            attrs.insert(c);
        };
        self.relations
            .values()
            .flat_map(|n| &n.predicates)
            .flat_map(|p| p.referred_columns())
            .for_each(&mut record_column);

        for e in self.edges.values() {
            match e {
                QueryGraphEdge::Join { on } => {
                    on.iter()
                        .flat_map(|p| vec![&p.left, &p.right])
                        .for_each(&mut record_column);
                }
                QueryGraphEdge::LeftJoin { on, extra_preds } => {
                    on.iter()
                        .flat_map(|p| vec![&p.left, &p.right])
                        .for_each(&mut record_column);

                    extra_preds
                        .iter()
                        .flat_map(|expr| expr.referred_columns())
                        .for_each(&mut record_column);
                }
            }
        }

        self.group_by
            .iter()
            .flat_map(|e| e.referred_columns())
            .for_each(&mut record_column);

        // Global predicates are part of the attributes too
        self.global_predicates
            .iter()
            .flat_map(|p| p.referred_columns())
            .for_each(record_column);

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
    use nom_sql::{parse_query, Dialect, SqlQuery};

    use super::*;
    use crate::controller::sql::query_graph::to_query_graph;

    /// Parse a SQL query that is expected to be a SelectQuery. Returns None if
    /// parsing fails *or* if the query is something other than a Select
    pub fn parse_select<T: AsRef<str>>(input: T) -> Option<nom_sql::SelectStatement> {
        match parse_query(Dialect::MySQL, input) {
            Ok(SqlQuery::Select(sel)) => Some(sel),
            _ => None,
        }
    }

    #[test]
    #[allow(clippy::eq_op)]
    fn it_compares_signatures() {
        use nom_sql::parser::{parse_query, SqlQuery};

        use crate::controller::sql::query_graph::to_query_graph;

        let qa = parse_query(Dialect::MySQL, "SELECT b.c3 FROM a, b WHERE a.c1 = 42;").unwrap();
        let qb = parse_query(Dialect::MySQL, "SELECT b.c3 FROM a, b WHERE a.c1 > 42;").unwrap();
        let qc = parse_query(
            Dialect::MySQL,
            "SELECT b.c3 FROM a, b WHERE a.c1 = 42 AND b.c4 = a.c2;",
        )
        .unwrap();
        let qd = parse_query(
            Dialect::MySQL,
            "SELECT b.c3 FROM a, b WHERE a.c1 = 21 AND b.c4 = a.c2;",
        )
        .unwrap();

        let qga = match qa {
            SqlQuery::Select(q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgb = match qb {
            SqlQuery::Select(q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgc = match qc {
            SqlQuery::Select(q) => to_query_graph(q).unwrap(),
            _ => panic!(),
        };
        let qgd = match qd {
            SqlQuery::Select(q) => to_query_graph(q).unwrap(),
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

    #[test]
    fn topk_hashes_are_inequal() {
        use std::collections::hash_map::DefaultHasher;

        let without_topk = parse_select("SELECT a.id FROM a").unwrap();
        let with_topk = parse_select("SELECT a.id FROM a ORDER BY n LIMIT 3").unwrap();

        let without_topk_qg = to_query_graph(without_topk).unwrap();
        let with_topk_qg = to_query_graph(with_topk).unwrap();

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        without_topk_qg.hash(&mut h1);
        with_topk_qg.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }
}
