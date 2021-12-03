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

    /// The number of threads to generate data with.
    #[clap(long, default_value = "1")]
    threads: usize,
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

        let mut database_spec = DatabaseGenerationSpec::new(schema);
        parallel_load(self.database_url.clone(), &mut database_spec, self.threads).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGenerator::parse();
    data_generator.run().await
}
