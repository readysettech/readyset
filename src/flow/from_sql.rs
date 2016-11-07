use nom_sql::parser as sql_parser;
use nom_sql::parser::SqlQuery;
use nom_sql::{InsertStatement, SelectStatement};
use FlowGraph;
use ops;
use ops::Node;
use ops::base::Base;
use ops::identity::Identity;
use query::{DataType, Query};

use petgraph;
use std::str;
use std::iter::FromIterator;
use std::vec::Vec;

trait FromSql {
    fn incorporate_query(&mut self, &str) -> Result<Vec<petgraph::graph::NodeIndex>, String>;
    fn lookup_node(&self, &str) -> petgraph::graph::NodeIndex;
    fn make_base_node(&self, &InsertStatement) -> Node;
    fn make_filter_node(&self, &SelectStatement) -> Node;
    fn nodes_for_query(&self, SqlQuery) -> Vec<Node>;
}

impl FromSql for FlowGraph<Query, ops::Update, Vec<DataType>> {
    fn incorporate_query(&mut self, q: &str) -> Result<Vec<petgraph::graph::NodeIndex>, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(q);

        // if ok, manufacture a node for the query structure we got
        let nodes = match parsed_query {
            Ok(q) => self.nodes_for_query(q),
            Err(e) => return Err(String::from(e)),
        };

        // finally, let's hook in the node
        Ok(nodes.into_iter()
            .map(|n| self.incorporate(n))
            .collect())
    }

    fn lookup_node(&self, vn: &str) -> petgraph::graph::NodeIndex {
        self.named[vn]
    }

    fn make_base_node(&self, st: &InsertStatement) -> Node {
        ops::new(st.table.clone(),
                 Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
                 true,
                 Base {})
    }

    fn make_filter_node(&self, st: &SelectStatement) -> Node {
        // XXX(malte): we need a custom name/identifier scheme for filter nodes, since the table
        // name is already used for the base node. Maybe this is where identifiers based on query
        // prefixes come in.
        ops::new(st.table.clone(),
                 Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
                 true,
                 Identity::new(self.lookup_node(&st.table)))
    }

    fn nodes_for_query(&self, q: SqlQuery) -> Vec<Node> {
        println!("{:#?}", q);

        match q {
            SqlQuery::Insert(iq) => vec![self.make_base_node(&iq)],
            SqlQuery::Select(sq) => vec![self.make_filter_node(&sq)],
        }
    }
}

#[cfg(test)]
mod tests {
    use FlowGraph;
    use ops::new;
    use ops::base::Base;
    use super::FromSql;

    #[test]
    fn it_parses() {
        // set up graph
        let mut g = FlowGraph::new();

        // Must have a base node for type inference to work, so make one manually
        let _ = g.incorporate(new("users", &["id", "username"], true, Base {}));
        // Should have two nodes: source and "users" base table
        assert_eq!(g.graph.node_count(), 2);

        assert!(g.incorporate_query("SELECT * from users;").is_ok());
        // Should now have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);

        // Invalid query should fail parsing and add no nodes
        assert!(g.incorporate_query("foo bar from whatever;").is_err());
        // Should still only have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);
    }

    #[test]
    fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = FlowGraph::new();

        // Establish a base write type
        assert!(g.incorporate_query("INSERT INTO users VALUES (?, ?);").is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!((*g.graph[g.named["users"]].as_ref().unwrap()).name(),
                   "users");

        // Try a simple query
        assert!(g.incorporate_query("SELECT * from users;").is_ok());
    }
}
