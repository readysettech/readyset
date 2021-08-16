//! Tests for query coverage analysis and testing support in the adapter binaries
//!
//! see: <https://docs.google.com/document/d/1i2HYLxANhJX4BxBnYeEzLO6sTecE4HkLoN31vXDlFCM/edit>

mod common;
use common::{sleep, Deployment};

use mysql::prelude::Queryable;
use noria_client::backend::BackendBuilder;

fn setup(deployment: &Deployment) -> mysql::Opts {
    common::setup(
        BackendBuilder::new()
            .require_authentication(false)
            .race_reads(true),
        deployment,
        true,
        true,
    )
}

#[test]
fn race_reads_with_supported_query() {
    let d = Deployment::new("race_reads_with_supported_query");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();

    conn.query_drop("CREATE TABLE Cats(id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    let res: Vec<u32> = conn
        .query("SELECT id FROM Cats WHERE name = \"Bob\"")
        .unwrap();

    assert_eq!(res, vec![1])
}

#[test]
#[ignore] // waiting on fallback for prepared statements
fn race_reads_with_unsupported_query() {
    let d = Deployment::new("race_reads_with_unsupported_query");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();

    conn.query_drop("CREATE TABLE Cats(id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    // this query will likely always be unsupported, so should fail in noria but succeed in mysql
    // (and return the results from mysql)
    let res: Vec<(u32, String)> = conn.exec(
        "WITH bobs AS (SELECT id, name FROM Cats WHERE lower(name) LIKE CONCAT('%', lower(?), '%'))
         SELECT * FROM bobs",
        ("Bob",)
    ).unwrap();

    assert_eq!(res, vec![(1, "Bob".to_owned())])
}
