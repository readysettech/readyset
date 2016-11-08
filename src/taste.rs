use std::str;
use std::path::Path;
use std::process::{Command, Stdio, Output};

pub fn taste_commit(id: &String) {
    println!("Tasting commit {}", id);
}
