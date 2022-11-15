use benchmarks::utils::generate::DataGenerator as DataGeneratorUtil;
use clap::Parser;

#[derive(Parser)]
#[clap(name = "data_generator")]
struct DataGenerator {
    /// Path to the desired database SQL schema.
    #[clap(flatten)]
    generator: DataGeneratorUtil,

    /// MySQL database connection string.
    #[clap(long)]
    database_url: String,
}

impl DataGenerator {
    pub async fn run(self) -> anyhow::Result<()> {
        self.generator.generate(&self.database_url).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGenerator::parse();
    data_generator.run().await
}
