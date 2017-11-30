use dataflow::prelude::DataType;
use dataflow::ops::base::Base;
use controller::Migration;
use std::collections::HashMap;
use nom_sql::SqlQuery;
use controller::security::SecurityConfig;
use controller::sql::{QueryFlowParts, SqlIncorporator};
use controller::sql::query_graph::{to_query_graph, QueryGraph};
use std::collections::hash_map::Entry;

pub type UniverseId = DataType;

pub trait Multiverse {
    /// Prepare a new security universe.
    /// It creates universe-specific nodes like UserContext and UserGroups and
    /// it derives universe-specific policies from the security configuration.
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String>;

    fn add_base(
        &mut self,
        name: String,
        fields: Vec<&String>,
        mig: &mut Migration
    ) -> Result<QueryFlowParts, String>;
}

impl Multiverse for SqlIncorporator {
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        // First, create the UserContext base node.
        let uid = mig.universe();
        let context = mig.context();

        info!(self.log, "Starting user universe {}", uid);

        let name = format!("UserContext_{}", uid);
        let fields: Vec<_> = context.keys().collect();

        let res = self.add_base(name, fields, mig);

        // Then, we need to transform policies' predicates into QueryGraphs.
        // We do this in a per-universe base, instead of once per policy,
        // because predicates can have nested subqueries, which will trigger
        // a view creation and these views might be unique to each universe
        // e.g. if they reference UserContext.
        self.mir_converter.clear_policies(&uid);
        let mut policies_qg: HashMap<(DataType, String), Vec<QueryGraph>> = HashMap::new();
        for policy in config.policies() {
            trace!(self.log, "Adding policy {:?}", policy.name);
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

            let e = policies_qg.entry((uid.clone(), policy.table.clone())).or_insert_with(Vec::new);
            e.push(qg);
        }

        self.mir_converter.set_policies(policies_qg);

        res
    }

    fn add_base(
        &mut self,
        name: String,
        fields: Vec<&String>,
        mig: &mut Migration
    ) -> Result<QueryFlowParts, String> {
        // Unfortunately, we can't add the base directly to the graph, because we needd
        // it to be recorded in the MIR level, so other queries can reference it.
        let mut s = String::new();
        s.push_str(&format!("CREATE TABLE `{}` (", name));
        for k in fields {
            s.push_str("\n");
            s.push_str(&format!("`{}` text NOT NULL,", k));
        }
        s.push_str("\n");
        s.push_str(") ENGINE=MyISAM DEFAULT CHARSET=utf8;");

        self.add_query(&s, Some(name), mig)
    }
}
