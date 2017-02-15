use nom_sql::Column;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

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
        where H: Hasher
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

        return true;
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
        use flow::sql::query_graph::{to_query_graph, QueryGraph};
        use nom_sql::parser::{parse_query, SqlQuery};

        let qa = parse_query("SELECT a.c1, b.c3 FROM a, b WHERE a.c1 = b.c1 AND a.c2 = 42;")
            .unwrap();
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

}
