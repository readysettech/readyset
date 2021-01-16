use noria::consensus::ZookeeperAuthority;
use noria::Builder;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

// This exists for testing purposes. It is an easy script to install a noria recipe. It assumes the deployment
// is named `myapp` and that its listening on address 127.0.0.1
#[tokio::main]
async fn main() {
    let test_sql_string = "
            CREATE TABLE employees (
                emp_no      INT             NOT NULL,
                first_name  VARCHAR(14)     NOT NULL,
                last_name   VARCHAR(16)     NOT NULL,
                gender      VARCHAR(1)  NOT NULL,    
                PRIMARY KEY (emp_no)
            );

            QUERY testQuery : \
                SELECT * FROM employees;
        ";

    let authority = ZookeeperAuthority::new(&"127.0.0.1:2181/myapp");
    let mut builder = Builder::default();
    builder.set_listen_addr(IpAddr::from_str("127.0.0.1").unwrap());
    let (mut noria_authority, _) = builder.start(Arc::new(authority.unwrap())).await.unwrap();

    println!("Waiting for noria");
    noria_authority
        .install_recipe(test_sql_string)
        .await
        .unwrap();
    let mut getter = noria_authority.view(&"testQuery").await.unwrap();

    let results = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Results: {:?}", results);
}
