use std::collections::HashMap;
use std::vec::Vec;

use nom_sql::Table;
use noria::ReadySetError;

use crate::controller::sql::query_graph::QueryGraph;
use crate::controller::sql::reuse::join_order::reorder_joins;
use crate::{ReadySetResult, ReuseConfigType};

mod finkelstein;
mod full;
mod helpers;
mod join_order;
mod relaxed;

#[derive(Clone, Debug)]
pub(in crate::controller) enum ReuseType {
    DirectExtension,
    PrefixReuse,
    #[allow(dead_code)]
    BackjoinRequired(Vec<Table>),
}

pub(in crate::controller) struct ReuseConfig {
    config: ReuseConfigType,
}

impl ReuseConfig {
    pub(in crate::controller) fn reuse_candidates<'a>(
        &self,
        qg: &mut QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Result<Vec<(ReuseType, (u64, &'a QueryGraph))>, ReadySetError> {
        let reuse_candidates = match self.config {
            ReuseConfigType::Finkelstein => {
                finkelstein::Finkelstein::reuse_candidates(qg, query_graphs)?
            }
            ReuseConfigType::Relaxed => relaxed::Relaxed::reuse_candidates(qg, query_graphs)?,
            ReuseConfigType::Full => full::Full::reuse_candidates(qg, query_graphs)?,
        };

        self.reorder_joins(qg, &reuse_candidates)?;
        Ok(reuse_candidates)
    }

    fn reorder_joins(
        &self,
        qg: &mut QueryGraph,
        reuse_candidates: &[(ReuseType, (u64, &QueryGraph))],
    ) -> ReadySetResult<()> {
        reorder_joins(qg, reuse_candidates)?;
        Ok(())
    }

    pub(in crate::controller) fn new(reuse_type: ReuseConfigType) -> ReuseConfig {
        match reuse_type {
            ReuseConfigType::Finkelstein => ReuseConfig::finkelstein(),
            ReuseConfigType::Relaxed => ReuseConfig::relaxed(),
            ReuseConfigType::Full => ReuseConfig::full(),
        }
    }

    fn full() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Full,
        }
    }

    fn finkelstein() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Finkelstein,
        }
    }

    fn relaxed() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Relaxed,
        }
    }
}

trait ReuseConfiguration {
    fn reuse_candidates<'a>(
        qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Result<Vec<(ReuseType, (u64, &'a QueryGraph))>, ReadySetError>;
}
