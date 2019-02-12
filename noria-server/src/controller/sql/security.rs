use crate::controller::security::SecurityConfig;
use crate::controller::sql::query_graph::{to_query_graph, QueryGraph};
use crate::controller::sql::{QueryFlowParts, SqlIncorporator};
use crate::controller::Migration;
use dataflow::prelude::DataType;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Universe {
    pub id: DataType,
    pub from_group: Option<DataType>,
    pub member_of: HashMap<String, Vec<DataType>>,
    pub row_policies: HashMap<String, Vec<QueryGraph>>,
    pub rewrite_policies: HashMap<String, Vec<RewritePolicy>>,
}

impl Universe {
    pub fn default() -> Universe {
        Universe {
            id: "".into(),
            from_group: None,
            member_of: HashMap::default(),
            row_policies: HashMap::default(),
            rewrite_policies: HashMap::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RewritePolicy {
    pub value: String,
    pub column: String,
    pub key: String,
    pub rewrite_view: String,
}

pub trait Multiverse {
    /// Prepare a new security universe.
    /// It creates universe-specific nodes like UserContext and UserGroups and
    /// it derives universe-specific policies from the security configuration.
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        universe_groups: HashMap<String, Vec<DataType>>,
        mig: &mut Migration,
    ) -> Result<Vec<QueryFlowParts>, String>;

    fn add_base(
        &mut self,
        name: String,
        fields: &mut Vec<String>,
        mig: &mut Migration,
    ) -> QueryFlowParts;
}

impl Multiverse for SqlIncorporator {
    fn prepare_universe(
        &mut self,
        config: &SecurityConfig,
        universe_groups: HashMap<String, Vec<DataType>>,
        mig: &mut Migration,
    ) -> Result<Vec<QueryFlowParts>, String> {
        let mut qfps = Vec::new();

        self.mir_converter.clear_universe();

        let (id, group) = mig.universe();
        let mut universe = Universe {
            id: id.clone(),
            from_group: group.clone(),
            member_of: universe_groups,
            row_policies: HashMap::new(),
            rewrite_policies: HashMap::new(),
        };

        // Create the UserContext base node.
        let mut fields: Vec<String> = mig.context().keys().cloned().collect();

        let (uc_name, universe_policies) = if group.is_none() {
            info!(self.log, "Starting user universe {}", universe.id);
            let uc_name = format!("UserContext_{}", universe.id.to_string());

            (uc_name, config.policies())
        } else {
            info!(self.log, "Starting group universe {}", universe.id);
            let group_name: DataType = group.clone().unwrap();
            let uc_name = format!(
                "GroupContext_{}_{}",
                group_name.to_string(),
                universe.id.to_string()
            );

            (uc_name, config.get_group_policies(group_name.to_string()))
        };

        let base = self.add_base(uc_name.clone(), &mut fields, mig);
        qfps.push(base);

        // Then, we need to transform policies' predicates into QueryGraphs.
        // We do this in a per-universe base, instead of once per policy,
        // because predicates can have nested subqueries, which will trigger
        // a view creation and these views might be unique to each universe
        // e.g. if they reference UserContext.
        let mut row_policies_qg: HashMap<String, Vec<QueryGraph>> = HashMap::new();
        for policy in universe_policies {
            if !policy.is_row_policy() {
                let qfp = self
                    .add_parsed_query(policy.predicate(), None, false, mig)
                    .unwrap();
                let rewrite_view = qfp.name.clone();
                let rw_pol = RewritePolicy {
                    value: policy.value(),
                    column: policy.column(),
                    key: policy.key(),
                    rewrite_view,
                };

                let e = universe
                    .rewrite_policies
                    .entry(policy.table().clone())
                    .or_insert_with(Vec::new);
                e.push(rw_pol);
                qfps.push(qfp);
                continue;
            }

            trace!(self.log, "Adding row policy {:?}", policy.name());
            let predicate = self.rewrite_query(policy.predicate(), mig)?;
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

            let e = row_policies_qg
                .entry(policy.table().clone())
                .or_insert_with(Vec::new);
            e.push(qg);
        }

        universe.row_policies = row_policies_qg;

        let e = self.universes.entry(group.clone()).or_insert_with(Vec::new);
        e.push((id, group));

        self.mir_converter.set_universe(universe);

        Ok(qfps)
    }

    fn add_base(
        &mut self,
        name: String,
        fields: &mut Vec<String>,
        mig: &mut Migration,
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

        self.add_parsed_query(parsed_query, Some(name), false, mig)
            .unwrap()
    }
}
