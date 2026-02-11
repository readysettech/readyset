use serde::{Deserialize, Serialize};

use crate::prelude::*;

/// A constant table node that emits fixed rows.
/// Used for VALUES clauses - acts as a tiny fully-materialized source.
/// Like Base nodes, Constant does NOT implement Ingredient - it's a special source node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constant {
    /// The constant rows to emit
    rows: Vec<Vec<DfValue>>,
}

impl Constant {
    /// Construct a new constant node with the given rows.
    pub fn new(rows: Vec<Vec<DfValue>>) -> Self {
        Self { rows }
    }

    /// Get the constant rows.
    pub fn rows(&self) -> &[Vec<DfValue>] {
        &self.rows
    }

    /// Take ownership of this Constant, leaving an empty one in its place.
    pub(in crate::node) fn take(&mut self) -> Self {
        Self {
            rows: std::mem::take(&mut self.rows),
        }
    }

    /// Returns a short description for debugging/visualization.
    pub fn description(&self) -> String {
        format!("C[{}]", self.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_describes() {
        let rows = vec![
            vec![DfValue::from(1), DfValue::from("a")],
            vec![DfValue::from(2), DfValue::from("b")],
            vec![DfValue::from(3), DfValue::from("c")],
        ];
        let c = Constant::new(rows);
        assert_eq!(c.description(), "C[3]");
    }

    #[test]
    fn it_stores_rows() {
        let rows = vec![
            vec![DfValue::from(1), DfValue::from("a")],
            vec![DfValue::from(2), DfValue::from("b")],
        ];
        let c = Constant::new(rows.clone());
        assert_eq!(c.rows(), &rows);
    }
}
