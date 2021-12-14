use benchmarks::utils::generate::parallel_load;
use benchmarks::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use clap::{Parser, ValueHint};
use noria_logictest::upstream::DatabaseURL;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;

#[derive(Parser)]
#[clap(name = "data_generator")]
struct DataGenerator {
    /// Path to the desired database SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: DatabaseURL,

    /// Change or assign values to user variables in the provided schema.
    /// The format is a json map, for example "{ 'user_rows': '10000', 'article_rows': '100' }"
    #[clap(long, default_value = "{}")]
    var_overrides: serde_json::Value,
}

impl DataGenerator {
    pub async fn run(self) -> anyhow::Result<()> {
        let user_vars: HashMap<String, String> = self
            .var_overrides
            .as_object()
            .expect("var-overrides should be formatted as a json map")
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.as_str().unwrap().to_owned()))
            .collect();

        let schema = DatabaseSchema::try_from((self.schema, user_vars))?;

        let database_spec = DatabaseGenerationSpec::new(schema);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut conn = self.database_url.connect().await?;
        // Prioir to data injection we want to increase the innodb pool size for faster writes
        let old_size: usize = conn.query("SELECT @@innodb_buffer_pool_size").await?[0][0]
            .to_string()
            .parse()
            .unwrap();

        let new_size = old_size * 8;
        let _ = conn
            .query_drop(format!("SET GLOBAL innodb_buffer_pool_size={}", new_size))
            .await;

        let ret = runtime
            .spawn(parallel_load(self.database_url.clone(), database_spec))
            .await;

        // Restore the original buffer size
        let _ = conn
            .query_drop(format!("SET GLOBAL innodb_buffer_pool_size={}", old_size))
            .await;

        runtime.shutdown_background();

        ret.unwrap()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGenerator::parse();
    data_generator.run().await
}
