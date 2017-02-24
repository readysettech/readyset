extern crate nom_sql;

use std::fs::File;
use std::path::Path;
use std::io::Read;

fn parse_queryset(queries: Vec<String>) -> (i32, i32) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;
    for query in queries.iter() {
        println!("Trying to parse '{}': ", &query);
        match nom_sql::parser::parse_query(&query) {
            Ok(_) => {
                println!("ok");
                parsed_ok.push(query);
            }
            Err(_) => {
                println!("failed");
                parsed_err += 1;
            }
        }
    }

    println!("Parsing failed: {} queries", parsed_err);
    println!("Parsed successfully: {} queries", parsed_ok.len());
    println!("\nSuccessfully parsed queries:");
    for q in parsed_ok.iter() {
        println!("{:?}", q);
    }

    (parsed_ok.len() as i32, parsed_err)
}

fn test_queries_from_file(f: &Path, name: &str) -> Result<i32, i32> {
    let mut f = File::open(f).unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s.lines()
        .filter(|l| !l.is_empty() && !l.starts_with("#"))
        .map(|l| if !(l.ends_with("\n") || l.ends_with(";")) {
            String::from(l) + "\n"
        } else {
            String::from(l)
        })
        .collect();
    println!("Loaded {} {} queries", lines.len(), name);

    // Try parsing them all
    let (ok, _) = parse_queryset(lines);

    // For the moment, we're always good
    Ok(ok)
}

#[test]
fn hotcrp_queries() {
    assert!(test_queries_from_file(Path::new("tests/hotcrp-queries.txt"), "HotCRP").is_ok());
}

#[test]
fn hyrise_test_queries() {
    assert!(test_queries_from_file(Path::new("tests/hyrise-test-queries.txt"), "HyRise").is_ok());
}

#[test]
fn tpcw_test_queries() {
    assert!(test_queries_from_file(Path::new("tests/tpc-w-queries.txt"), "TPC-W").is_ok());
}

#[test]
fn tpcw_test_tables() {
    let res = test_queries_from_file(Path::new("tests/tpc-w-tables.txt"), "TPC-W tables");
    assert!(res.is_ok());
    // There are 10 tables
    assert_eq!(res.unwrap(), 10);
}

#[test]
fn finkelstein82_test_queries() {
    assert!(test_queries_from_file(Path::new("tests/finkelstein82.txt"), "Finkelstein 1982")
        .is_ok())
}

#[test]
fn mediawiki_schema() {
    let mut f = File::open(Path::new("tests/mediawiki-schema.txt")).unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<&str> = s.lines()
        .map(str::trim)
        .filter(|l| {
            !l.is_empty() && !l.starts_with("#") && !l.starts_with("--") && !l.starts_with("DROP")
        })
        .collect();
    let mut q = String::new();
    let mut queries = Vec::new();
    for l in lines {
        if !l.ends_with(";") {
            q.push_str(l);
        } else {
            // end of query
            q.push_str(l);
            queries.push(q.clone());
            q = String::new();
        }
    }
    println!("Loaded {} table definitions", queries.len());

    // Try parsing them all
    let (ok, fail) = parse_queryset(queries);

    // There are 17 CREATE TABLE queries in the schema
    assert_eq!(ok, 17);
    assert_eq!(fail, 0);
}
