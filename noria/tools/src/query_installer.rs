use noria::consensus::{Authority, ZookeeperAuthority};
use noria::ControllerHandle;

// This exists for testing purposes. It is an easy script to install a noria recipe. It assumes the deployment
// is named `myapp` and that its listening on address 127.0.0.1
#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let authority = Authority::from(ZookeeperAuthority::new("127.0.0.1:2181/myapp")?);
    let mut noria = ControllerHandle::new(authority).await;

    println!("Waiting for noria");
    noria.install_recipe(test_sql_string).await.unwrap();
    let mut getter = noria.view("testQuery").await.unwrap();

    let results = getter.lookup(&[0.into()], true).await.unwrap();
    println!("Results: {:?}", results);
    Ok(())
}
