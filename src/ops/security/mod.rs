use std::fmt;
use std::collections::HashMap;

use flow::prelude::*;

pub mod filter;

pub trait SecurityOperation: fmt::Debug + Clone {
	fn description(&self) -> String;

	fn on_input(
        &mut self,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ProcessingResult;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityOperator<T: SecurityOperation> {
	src: IndexPair,
    inner: T,
}

impl<T: SecurityOperation> SecurityOperator<T> {
	pub fn new(src: NodeIndex, op: T) -> SecurityOperator<T> {
		SecurityOperator {
			src: src.into(),
			inner: op,
		}
	}
}

impl<T: SecurityOperation + Send> Ingredient for SecurityOperator<T> 
where
	Self: Into<NodeOperator>,
{
	fn take(&mut self) -> NodeOperator {
		Clone::clone(self).into()
	}

	fn ancestors(&self) -> Vec<NodeIndex> {
		vec![self.src.as_global()]
	}

	fn should_materialize(&self) -> bool {
		false
	}

	fn will_query(&self, _: bool) -> bool {
		false
	}

	fn suggest_indexes(&self, _:NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
		HashMap::new()
	}

	fn resolve(&self, col:usize) -> Option<Vec<(NodeIndex, usize)>> {
		Some(vec![(self.src.as_global(), col)])
	}

	fn description(&self) -> String {
		self.inner.description()
	}

	fn on_connected(&mut self, g: &Graph) {
		// TODO(larat): implement
	}	

	fn on_commit(&mut self, _:NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
		self.src.remap(remap);
	}

	fn on_input(
        &mut self,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ProcessingResult {
		self.inner.on_input(from, data, tracer, domain, states)
		
	}

	fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}

