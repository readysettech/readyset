use dataflow::prelude::DataType;
use controller::Migration;
use std::collections::HashMap;
use nom_sql::SqlQuery;
use controller::security::SecurityConfig;
use controller::sql::{QueryFlowParts, SqlIncorporator};
use controller::sql::query_graph::{to_query_graph, QueryGraph};
use nom_sql::parser as sql_parser;

pub type UniverseId = DataType;

pub trait Multiverse {
    /// Prepare a new security universe.
    /// It creates universe-specific nodes like UserContext and UserGroups and
    /// it derives universe-specific policies from the security configuration.
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        group: Option<String>,
        mig: &mut Migration,
    ) -> Vec<QueryFlowParts>;

    fn add_base(
        &mut self,
        name: String,
        fields: Vec<&String>,
        mig: &mut Migration
    ) -> QueryFlowParts;

    fn add_user_group(
        &mut self,
        name: String,
        user_context: String,
        group_membership: String,
        mig: &mut Migration,
    ) -> QueryFlowParts;
}

impl Multiverse for SqlIncorporator {
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        group: Option<String>,
        mig: &mut Migration,
    ) -> Vec<QueryFlowParts> {
        // First, create the UserContext base node.

        let universe_policies;
        let uid = mig.universe();
        let mut qfps = Vec::new();
        if group.is_none() {
            info!(self.log, "Starting user universe {}", uid);
            let context = mig.context();

            let uc_name = format!("UserContext_{}", uid);
            let fields: Vec<_> = context.keys().collect();

            let base = self.add_base(uc_name.clone(), fields, mig);
            qfps.push(base);

            // Then, create the UserGroup views.
            for group in config.groups.values() {
                let name = format!("{}_{}", group.name(), uid);
                let group = self.add_user_group(name, uc_name.clone(), group.name(), mig);
                qfps.push(group);
            }

            universe_policies = config.policies();

        } else {
            info!(self.log, "Starting group universe {}", uid);
            let group_name = group.unwrap();

            universe_policies = config.get_group_policies(group_name);
        }


        // Then, we need to transform policies' predicates into QueryGraphs.
        // We do this in a per-universe base, instead of once per policy,
        // because predicates can have nested subqueries, which will trigger
        // a view creation and these views might be unique to each universe
        // e.g. if they reference UserContext.
        self.mir_converter.clear_policies(&uid);
        let mut policies_qg: HashMap<(DataType, String), Vec<QueryGraph>> = HashMap::new();
        for policy in universe_policies {
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

        qfps
    }

    fn add_user_group(
        &mut self,
        name: String,
        user_context: String,
        group_membership: String,
        mig: &mut Migration,
    ) -> QueryFlowParts {
        let mut s = String::new();
        s.push_str(&format!("select uid, gid FROM {}, {} WHERE id = uid;",
                        user_context,
                        group_membership,
                    ));

        let parsed_query = sql_parser::parse_query(&s).unwrap();

        self.add_parsed_query(parsed_query, Some(name), false, mig).unwrap()
    }

    fn add_base(
        &mut self,
        name: String,
        fields: Vec<&String>,
        mig: &mut Migration
    ) -> QueryFlowParts {
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

        let parsed_query = sql_parser::parse_query(&s).unwrap();

        self.add_parsed_query(parsed_query, Some(name), false, mig).unwrap()
    }
}
