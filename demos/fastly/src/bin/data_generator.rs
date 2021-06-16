use clap::{Clap, ValueHint};
use fastly_demo::generate::load;
use fastly_demo::spec::{DatabaseGenerationSpec, DatabaseSchema};
use noria::DataType;
use noria_logictest::generate::DatabaseURL;
use query_generator::ColumnGenerationSpec;
use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;

#[derive(Clap)]
#[clap(name = "data_generator")]
struct DataGenerator {
    /// The number of rows to generate for the article table.
    #[clap(long, default_value = "10")]
    article_table_rows: usize,

    /// The number of rows to generate for the users table.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// The number of rows to generate for the recommendations table per user.
    #[clap(long, default_value = "10")]
    per_user_recs: usize,

    /// Path to the fastly data model SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: Option<PathBuf>,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: DatabaseURL,
}

impl DataGenerator {
    pub async fn run(self) -> anyhow::Result<()> {
        let fastly_schema = DatabaseSchema::try_from(self.schema.unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/fastly_db.sql")
        }))?;
        let mut database_spec = DatabaseGenerationSpec::new(fastly_schema)
            .table_rows("articles", self.article_table_rows)
            .table_rows("users", self.user_table_rows)
            .table_rows("recommendations", self.user_table_rows * self.per_user_recs);

        // Article table overrides.
        let table = database_spec.table_spec("articles");
        table.set_column_generator_specs(&[
            ("id".into(), ColumnGenerationSpec::Unique),
            (
                "priority".into(),
                ColumnGenerationSpec::Uniform(DataType::Int(0), DataType::Int(128)),
            ),
        ]);

        let table = database_spec.table_spec("users");
        table.set_column_generator_specs(&[("id".into(), ColumnGenerationSpec::Unique)]);

        let table = database_spec.table_spec("recommendations");
        table.set_column_generator_specs(&[
            (
                "user_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.user_table_rows as u32),
                ),
            ),
            (
                "article_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.article_table_rows as u32),
                ),
            ),
        ]);

        let mut conn = self.database_url.connect().await?;
        load(&mut conn, database_spec).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGenerator::parse();
    data_generator.run().await
}
