use dataflow::prelude::DataType;
use controller::Migration;
use std::collections::HashMap;
use nom_sql::SqlQuery;
use controller::security::SecurityConfig;
use controller::sql::{QueryFlowParts, SqlIncorporator};
use controller::sql::query_graph::{to_query_graph, QueryGraph};
use nom_sql::parser as sql_parser;

#[derive(Clone, Debug)]
pub struct Universe {
    pub id: DataType,
    pub from_group: Option<String>,
    pub member_of: HashMap<String, Vec<DataType>>,
    pub policies: HashMap<(DataType, String), Vec<QueryGraph>>,
}

impl Universe {
    pub fn default() -> Universe {
        Universe {
            id: "".into(),
            from_group: None,
            member_of: HashMap::default(),
            policies: HashMap::default(),
        }
    }
}

pub trait Multiverse {
    /// Prepare a new security universe.
    /// It creates universe-specific nodes like UserContext and UserGroups and
    /// it derives universe-specific policies from the security configuration.
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        group: Option<String>,
        universe_groups: HashMap<String, Vec<DataType>>,
        mig: &mut Migration,
    ) -> Vec<QueryFlowParts>;

    fn add_base(
        &mut self,
        name: String,
        fields: &mut Vec<&String>,
        mig: &mut Migration
    ) -> QueryFlowParts;
}

impl Multiverse for SqlIncorporator {
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        group: Option<String>,
        universe_groups: HashMap<String, Vec<DataType>>,
        mig: &mut Migration,
    ) -> Vec<QueryFlowParts> {
        let mut qfps = Vec::new();

        let mut universe = Universe {
            id: mig.universe(),
            from_group: group.clone(),
            member_of: universe_groups,
            policies: HashMap::new(),
        };

        // Create the UserContext base node.
        let context = mig.context();
        let mut fields: Vec<_> = context.keys().collect();

        let (uc_name, universe_policies) = if group.is_none() {
            info!(self.log, "Starting user universe {}", universe.id);
            let uc_name = format!("UserContext_{}", universe.id);

            (uc_name, config.policies())
        } else {
            info!(self.log, "Starting group universe {}", universe.id);
            let group_name = group.unwrap();
            let uc_name = format!("GroupContext_{}_{}", group_name, universe.id);

            (uc_name, config.get_group_policies(group_name))
        };

        let base = self.add_base(uc_name.clone(), &mut fields, mig);
        qfps.push(base);

        // Then, we need to transform policies' predicates into QueryGraphs.
        // We do this in a per-universe base, instead of once per policy,
        // because predicates can have nested subqueries, which will trigger
        // a view creation and these views might be unique to each universe
        // e.g. if they reference UserContext.
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

            let e = policies_qg.entry((universe.id.clone(), policy.table.clone())).or_insert_with(Vec::new);
            e.push(qg);
        }

        universe.policies = policies_qg;

        self.mir_converter.set_universe(universe);

        qfps
    }

    fn add_base(
        &mut self,
        name: String,
        fields: &mut Vec<&String>,
        mig: &mut Migration
    ) -> QueryFlowParts {
        // Unfortunately, we can't add the base directly to the graph, because we needd
        // it to be recorded in the MIR level, so other queries can reference it.

        fields.sort();
        fields.dedup();

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
