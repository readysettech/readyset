use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::iter::{Cycle, Iterator};

use dataflow::prelude::DomainIndex;
use flow::{WorkerEndpoint, WorkerIdentifier};

pub trait DomainPlacementStrategy {
    fn place_domain(&mut self, d: &DomainIndex, s: usize) -> Option<WorkerIdentifier>;
}

pub(crate) struct RoundRobinPlacer<'a> {
    iter: Cycle<Iter<'a, WorkerIdentifier, WorkerEndpoint>>,
}

impl<'a> RoundRobinPlacer<'a> {
    pub fn new(workers: &'a HashMap<WorkerIdentifier, WorkerEndpoint>) -> Self {
        RoundRobinPlacer {
            iter: workers.iter().cycle(),
        }
    }
}

impl<'a> DomainPlacementStrategy for RoundRobinPlacer<'a> {
    fn place_domain(&mut self, _: &DomainIndex, _: usize) -> Option<WorkerIdentifier> {
        self.iter.next().map(|ref w| w.0.clone())
    }
}

pub(crate) struct ShardIdPlacer {
    ids: Vec<WorkerIdentifier>,
}

impl ShardIdPlacer {
    pub fn new(workers: &HashMap<WorkerIdentifier, WorkerEndpoint>) -> Self {
        ShardIdPlacer {
            ids: workers.iter().map(|(wi, _)| wi.clone()).collect(),
        }
    }
}

impl DomainPlacementStrategy for ShardIdPlacer {
    fn place_domain(&mut self, _: &DomainIndex, si: usize) -> Option<WorkerIdentifier> {
        self.ids.get(si % self.ids.len()).cloned()
    }
}
