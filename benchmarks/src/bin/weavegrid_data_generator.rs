use benchmarks::utils::generate::parallel_load;
use benchmarks::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use clap::{Parser, ValueHint};
use noria_data::DataType;
use noria_logictest::upstream::DatabaseURL;
use query_generator::ColumnGenerationSpec;
use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;

#[derive(Parser)]
#[clap(name = "data_generator")]
struct DataGenerator {
    /// Path to the fastly data model SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: Option<PathBuf>,

    /// The number of rows to generate for each table.
    #[clap(long, default_value = "100")]
    table_rows: usize,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: DatabaseURL,
}

impl DataGenerator {
    pub async fn run(self) -> anyhow::Result<()> {
        let weavegrid_schema = DatabaseSchema::try_from(self.schema.unwrap_or_else(|| {
            PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap() + "/weavegrid_db.sql")
        }))?;
        let mut database_spec = DatabaseGenerationSpec::new(weavegrid_schema)
            .table_rows("users", self.table_rows)
            .table_rows("utility_peak_periods", self.table_rows)
            .table_rows("utilities", self.table_rows)
            .table_rows("utility_holidays", 2)
            .table_rows("vehicle_load_profiles", self.table_rows)
            .table_rows("registrations", self.table_rows)
            .table_rows("vehicles", self.table_rows);

        let table = database_spec.table_spec("users");
        table.set_column_generator_specs(&[
            ("user_id".into(), ColumnGenerationSpec::Unique),
            (
                "utility_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.table_rows as _),
                ),
            ),
        ]);

        let table = database_spec.table_spec("registrations");
        table.set_column_generator_specs(&[
            ("registration_id".into(), ColumnGenerationSpec::Unique),
            (
                "user_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.table_rows as _),
                ),
            ),
        ]);

        let table = database_spec.table_spec("utilities");
        table.set_column_generator_specs(&[("utility_id".into(), ColumnGenerationSpec::Unique)]);

        let table = database_spec.table_spec("utility_peak_periods");
        table.set_column_generator_specs(&[
            ("peak_periods_id".into(), ColumnGenerationSpec::Unique),
            (
                "utility_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.table_rows as _),
                ),
            ),
        ]);

        let table = database_spec.table_spec("vehicles");
        table.set_column_generator_specs(&[
            ("vehicle_id".into(), ColumnGenerationSpec::Unique),
            (
                "registration_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.table_rows as _),
                ),
            ),
        ]);

        let table = database_spec.table_spec("vehicle_load_profiles");
        table.set_column_generator_specs(&[
            (
                "vehicle_load_profile_id".into(),
                ColumnGenerationSpec::Unique,
            ),
            (
                "vehicle_id".into(),
                ColumnGenerationSpec::Uniform(
                    DataType::UnsignedInt(0),
                    DataType::UnsignedInt(self.table_rows as _),
                ),
            ),
        ]);

        parallel_load(self.database_url, database_spec).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGenerator::parse();
    data_generator.run().await
}
