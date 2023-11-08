use benchmarks::utils::generate::DataGenerator as DataGeneratorUtil;
use clap::Parser;

#[derive(Parser)]
#[command(name = "data_generator")]
struct DataGeneratorTool {
    /// Path to the desired database SQL schema.
    #[command(flatten)]
    generator: DataGeneratorUtil,

    /// Upstream database connection string.
    #[arg(long)]
    database_url: String,
}

impl DataGeneratorTool {
    pub async fn run(self) -> anyhow::Result<()> {
        self.generator.generate(&self.database_url).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_generator = DataGeneratorTool::parse();
    data_generator.run().await
}
