use nom_sql::Operator;
use sql::query_graph::QueryGraph;

use slog;
use std::collections::HashMap;
use std::str;
use std::vec::Vec;

fn direct_elimination(op1: &Operator, op2: &Operator) -> Option<Operator> {
    match *op1 {
        Operator::Equal => {
            match *op2 {
                Operator::Equal => Some(Operator::Equal),
                Operator::Less => Some(Operator::Less),
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        Operator::NotEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::NotEqual),
                Operator::Less => None,
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::Less => {
            match *op2 {
                Operator::Equal => Some(Operator::Less),
                Operator::Less => Some(Operator::Less),
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::LessOrEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::LessOrEqual),
                Operator::Less => Some(Operator::LessOrEqual),
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::Greater => {
            match *op2 {
                Operator::Equal => Some(Operator::Greater),
                Operator::Less => None,
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        Operator::GreaterOrEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::GreaterOrEqual),
                Operator::Less => None,
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        _ => unimplemented!(),
    }
}

pub fn check_compatibility(new_qg: &QueryGraph, existing_qg: &QueryGraph) -> bool {
    // 1. NQG's nodes is subset of EQG's nodes
    // -- already established via signature check
    // 2. NQG's attributes is subset of NQG's edges
    // -- already established via signature check
    assert!(existing_qg.signature().is_generalization_of(&new_qg.signature()));

    // 3. NQC's edges are superset of EQG's

    // 4. NQG's predicates imply EQG's
    //   4a. on nodes
    //   4b. on edges

    false
}
