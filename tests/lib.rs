extern crate nom_sqlp;

use std::fs::File;
use std::io::Read;

fn parse_queryset(queries: Vec<String>) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;
    for query in queries.iter() {
        match nom_sqlp::parser::parse_query(&query) {
            Ok(q) => {
                parsed_ok.push(query);
            }
            Err(_) => parsed_err += 1,
        }
    }

    println!("Parsing failed: {} queries", parsed_err);
    println!("Parsed successfully: {} queries", parsed_ok.len());
    println!("\nSuccessfully parsed queries:");
    for q in parsed_ok.iter() {
        println!("{:?}", q);
    }
}

#[test]
fn hotcrp_queries() {
    let mut f = File::open("tests/hotcrp-queries.txt").unwrap();
    let mut s = String::new();

    // Load HotCRP queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s.lines()
        .filter(|l| !l.starts_with("#"))
        .map(|l| {
            if !(l.ends_with("\n") || l.ends_with(";")) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            }
        })
        .collect();
    println!("Loaded {} HotCRP queries", lines.len());

    // Try parsing them all
    parse_queryset(lines);
}

#[test]
fn hyrise_test_queries() {
    let mut f = File::open("tests/hyrise-test-queries.txt").unwrap();
    let mut s = String::new();

    // Load HyRise queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s.lines()
        .filter(|l| !l.starts_with("#"))
        .map(|l| {
            if !(l.ends_with("\n") || l.ends_with(";")) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            }
        })
        .collect();
    println!("Loaded {} Hyrise queries", lines.len());

    // Try parsing them all
    parse_queryset(lines);
}
