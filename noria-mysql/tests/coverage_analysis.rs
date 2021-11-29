//! Tests for query coverage analysis and testing support in noria-mysql
//!
//! see: <https://docs.google.com/document/d/1i2HYLxANhJX4BxBnYeEzLO6sTecE4HkLoN31vXDlFCM/edit>

use mysql::prelude::Queryable;
use noria_client_test_helpers::sleep;

mod common;
use common::setup;

#[test]
fn mirror_reads_with_supported_query() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();

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
fn mirror_reads_with_unsupported_query() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();

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
