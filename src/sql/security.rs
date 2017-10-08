use flow::core::DataType;
use flow::Migration;
use std::collections::HashMap;
use nom_sql::SqlQuery;
use security::Policy;
use sql::{QueryFlowParts, SqlIncorporator};
use sql::query_graph::{to_query_graph, QueryGraph};
use std::collections::hash_map::Entry;

pub type UniverseId = DataType;

pub trait ManyUniverses {
    /// Bootstraps a new security universe
    fn start_universe(
        &mut self,
        policies: &HashMap<u64, Policy>,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String>;
}

impl ManyUniverses for SqlIncorporator {
    fn start_universe(
        &mut self,
        policies: &HashMap<u64, Policy>,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        // First, we need to create a UserContext base node.
        let uid = mig.universe();
        let context = mig.context();

        info!(self.log, "Starting user universe {}", uid);

        let name = format!("UserContext_{}", uid);
        let mut s = String::new();
        s.push_str(&format!("CREATE TABLE `{}` (", name));
        for k in context.keys() {
            s.push_str("\n");
            s.push_str(&format!("`{}` text NOT NULL,", k));
        }
        s.push_str("\n");
        s.push_str(") ENGINE=MyISAM DEFAULT CHARSET=utf8;");

        let res = self.add_query(&s, Some(name), mig);

        // Then, we need to transform policies' predicates into QueryGraphs.
        // We do this in a per-universe base, instead of once per policy,
        // because predicates can have nested subqueries, which will trigger
        // a view creation and these views might be unique to each universe
        // e.g. if they reference UserContext.

        self.mir_converter.clear_policies(&uid);
        let mut policies_qg: HashMap<(DataType, String), Vec<QueryGraph>> = HashMap::new();
        for policy in policies.values() {
            trace!(self.log, "Adding policy {:?}", policy);
            // Policies should have access to all the data in graph, because of that we set
            // policy_enhanced to false, so any subviews also have access to all the data.
            let predicate = self.rewrite_query(policy.predicate.clone(), mig);
            let st = match predicate {
                SqlQuery::Select(ref st) => st,
                _ => unreachable!(),
            };

            // TODO(larat): currently we only support policies with a single predicate. These can be
            // represented as a query graph. This will change for more complex policies eg. column
            // replacement and aggregation permission.

            let qg = match to_query_graph(st) {
                Ok(qg) => qg,
                Err(e) => panic!(e),
            };

            match policies_qg.entry((uid.clone(), policy.table.clone()))
            {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(qg);
                }
                Entry::Vacant(e) => {
                    e.insert(vec![qg]);
                }
            };

        }

        self.mir_converter.set_policies(policies_qg);

        res
    }
}