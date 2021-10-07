//! Tool to install a test query view in noria. This tool
//! creates a table in Noria and creates a view for the SELECT
//! * query for that table. It then performs a lookup of the
//! view.
use clap::Clap;
use noria::consensus::AuthorityType;
use noria::ControllerHandle;

#[derive(Clap)]
#[clap(name = "query_installer")]
struct QueryInstaller {
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,

    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    #[clap(short, long, env("NORIA_DEPLOYMENT"))]
    deployment: String,

    /// Name of the view to use in the install_recipe statement.
    #[clap(short, long)]
    query: String,
}

impl QueryInstaller {
    pub async fn run(self) -> anyhow::Result<()> {
        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.deployment)
            .await;

        let mut handle: ControllerHandle = ControllerHandle::new(authority).await;
        handle.ready().await.unwrap();

        let test_sql_string = format!(
            "
            CREATE TABLE employees (
                emp_no      INT             NOT NULL,
                first_name  VARCHAR(14)     NOT NULL,
                last_name   VARCHAR(16)     NOT NULL,
                gender      VARCHAR(1)  NOT NULL,
                PRIMARY KEY (emp_no)
            );
            QUERY {} : \
                SELECT * FROM employees;
        ",
            self.query.clone()
        );

        println!("Waiting for noria");
        handle.install_recipe(&test_sql_string).await.unwrap();
        let mut getter = handle.view(&self.query).await.unwrap();

        let results = getter.lookup(&[0.into()], true).await.unwrap();
        println!("Results: {:?}", results);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let query_installer = QueryInstaller::parse();
    query_installer.run().await
}
