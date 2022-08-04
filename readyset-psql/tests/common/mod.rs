use tokio_postgres::{Client, Config, NoTls};

pub async fn connect(config: Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}
