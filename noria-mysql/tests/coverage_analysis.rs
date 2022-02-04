//! Tests for query coverage analysis and testing support in noria-mysql
//!
//! see: <https://docs.google.com/document/d/1i2HYLxANhJX4BxBnYeEzLO6sTecE4HkLoN31vXDlFCM/edit>

use mysql_async::prelude::Queryable;
use noria_client_test_helpers::mysql_helpers::setup;
use noria_client_test_helpers::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn mirror_reads_with_supported_query() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats(id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    let res: Vec<u32> = conn
        .query("SELECT id FROM Cats WHERE name = \"Bob\"")
        .await
        .unwrap();

    assert_eq!(res, vec![1])
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // waiting on fallback for prepared statements
async fn mirror_reads_with_unsupported_query() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats(id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    // this query will likely always be unsupported, so should fail in noria but succeed in mysql
    // (and return the results from mysql)
    let res: Vec<(u32, String)> = conn.exec(
        "WITH bobs AS (SELECT id, name FROM Cats WHERE lower(name) LIKE CONCAT('%', lower(?), '%'))
         SELECT * FROM bobs",
        ("Bob",)
    ).await.unwrap();

    assert_eq!(res, vec![(1, "Bob".to_owned())])
}
