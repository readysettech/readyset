use benchmarks::utils::generate::load;
use benchmarks::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use clap::{Parser, ValueHint};
use noria::DataType;
use noria_logictest::upstream::DatabaseURL;
use query_generator::ColumnGenerationSpec;
use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;

#[derive(Parser)]
#[clap(name = "data_generator")]
struct DataGenerator {
    /// The number of rows to generate for the article table.
    #[clap(long, default_value = "10")]
    article_table_rows: usize,

    /// The number of rows to generate for the users table.
    #[clap(long, default_value = "10")]
    user_table_rows: usize,

    /// The number of rows to generate for the authors table.
    #[clap(long, default_value = "10")]
    author_table_rows: usize,

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
            .table_rows("recommendations", self.user_table_rows * self.per_user_recs)
            .table_rows("authors", self.author_table_rows);

        // Article table overrides.
        let table = database_spec.table_spec("articles");
        table.set_column_generator_specs(&[
            ("id".into(), ColumnGenerationSpec::Unique),
            (
                "priority".into(),
                ColumnGenerationSpec::Uniform(DataType::Int(0), DataType::Int(128)),
            ),
            (
                "author_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.author_table_rows as u32),
                ),
            ),
            ("keywords".into(), ColumnGenerationSpec::Random),
            ("title".into(), ColumnGenerationSpec::Random),
            ("full_text".into(), ColumnGenerationSpec::Random),
            ("short_text".into(), ColumnGenerationSpec::Random),
            ("image_url".into(), ColumnGenerationSpec::Random),
            ("url".into(), ColumnGenerationSpec::Random),
            ("type".into(), ColumnGenerationSpec::Random),
        ]);

        let table = database_spec.table_spec("users");
        table.set_column_generator_specs(&[("id".into(), ColumnGenerationSpec::Unique)]);

        let table = database_spec.table_spec("authors");
        table.set_column_generator_specs(&[
            ("id".into(), ColumnGenerationSpec::Unique),
            ("name".into(), ColumnGenerationSpec::Random),
            ("image_url".into(), ColumnGenerationSpec::Random),
        ]);

        let table = database_spec.table_spec("recommendations");
        // For each user we will generate `self.per_user_recs` rows. Recommendations for
        // each user are pulled from a non-repeated uniform distribution over the set of
        // articles.
        table.set_column_generator_specs(&[
            (
                "user_id".into(),
                ColumnGenerationSpec::UniqueRepeated(self.per_user_recs as u32),
            ),
            (
                "article_id".into(),
                ColumnGenerationSpec::UniformWithoutReplacement {
                    min: DataType::UnsignedInt(0),
                    max: DataType::UnsignedInt(self.article_table_rows as u32),
                    batch_size: Some(self.per_user_recs as u32),
                },
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
