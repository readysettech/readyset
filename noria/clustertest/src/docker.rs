//! Utilities for managing an external zooekeeper container through
//! the docker daemon API.

use anyhow::Result;
use shiplift::{ContainerOptions, Docker, RmContainerOptions};

/// Translates from a `prefix` to the name of the zookeeper container
/// including the prefix.
pub fn prefix_to_zookeeper_container(prefix: &str) -> String {
    prefix.to_string() + "-zookeeper"
}

/// Translates from a `prefix` to the name of the mysql container.
pub fn prefix_to_mysql_container(prefix: &str) -> String {
    prefix.to_string() + "-mysql"
}

/// Creates and starts a new zookeeper container, this does not
/// check for container conflicts and will error if a container
/// with the same name '`name_prefix`-zookeeper' already exist.
pub async fn start_zookeeper(name_prefix: &str, port: u16) -> Result<()> {
    let docker = Docker::new();
    let image = "zookeeper";
    let container_name = prefix_to_zookeeper_container(name_prefix);

    docker
        .containers()
        .create(
            &ContainerOptions::builder(image)
                .name(&container_name)
                .restart_policy("always", 10)
                .expose(2181, "tcp", port as u32)
                .build(),
        )
        .await?;

    docker.containers().get(container_name).start().await?;
    Ok(())
}

/// Stops an existing zookeeper instance and removes the container.
/// This function returns an `Ok` result if the container does not
/// exist or was not running.
pub async fn kill_zookeeper(name_prefix: &str) -> Result<()> {
    let docker = Docker::new();
    let container_name = prefix_to_zookeeper_container(name_prefix);
    let container = docker.containers().get(container_name);
    if let Ok(d) = container.inspect().await {
        if d.state.running {
            container.stop(None).await?;
        }

        container
            .remove(RmContainerOptions::builder().force(true).build())
            .await?;
    }
    Ok(())
}

/// Creates and starts a mysql container.
pub async fn start_mysql(name_prefix: &str, port: u16) -> Result<()> {
    let docker = Docker::new();
    let image = "mysql";
    let container_name = prefix_to_mysql_container(name_prefix);

    docker
        .containers()
        .create(
            &ContainerOptions::builder(image)
                .name(&container_name)
                .restart_policy("always", 10)
                .expose(3306, "tcp", port as u32)
                .env(&["MYSQL_ALLOW_EMPTY_PASSWORD=true"])
                .cmd(vec![
                    "mysqld",
                    "--default-authentication-plugin=mysql_native_password",
                ])
                .build(),
        )
        .await?;

    docker.containers().get(container_name).start().await?;
    Ok(())
}

/// Kills a container with `name_prefix` created by `start_mysql`.
pub async fn kill_mysql(name_prefix: &str) -> Result<()> {
    let docker = Docker::new();
    let container_name = prefix_to_mysql_container(name_prefix);
    let container = docker.containers().get(container_name);
    if let Ok(d) = container.inspect().await {
        if d.state.running {
            container.stop(None).await?;
        }

        container
            .remove(RmContainerOptions::builder().force(true).build())
            .await?;
    }
    Ok(())
}

/// Creates a new zookeeper client that checks if a container, `name`,
/// is running.
#[cfg(test)]
pub async fn container_running(name: &str) -> bool {
    let docker = Docker::new();
    match docker.containers().get(name).inspect().await {
        Err(e) => {
            eprintln!("Error inspecting container: {}", e);
            false
        }
        Ok(d) => d.state.running,
    }
}

/// Checks if the container already exists.
#[cfg(test)]
pub async fn container_exists(name: &str) -> bool {
    let docker = Docker::new();
    // Inspects a container, this will return a 404 error
    // if the container does not exist.
    !docker.containers().get(name).inspect().await.is_err()
}

// These tests currently require that a docker daemon is already setup
// and accessible by the user calling cargo test. As these tests interact
// with a stateful external component, the docker daemon, each test is
// responsible for cleaning up its own external state.
#[cfg(test)]
mod tests {
    use super::*;

    // This test verifies that we can create and teardown a zookeeper
    // docker container. It does not verify that the container is
    // set up properly.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn zookeeper_operations() {
        let container_prefix = "start_zookeeper_test";
        // Kill the container first to prevent conflicts with previous
        // test runs that may not have cleaned up the container.
        let _ = kill_zookeeper(container_prefix).await;

        // Create a zookeeper container, verify that it is running.
        let name = prefix_to_zookeeper_container(container_prefix);
        let res = start_zookeeper(container_prefix, 2184).await;
        assert!(
            !res.is_err(),
            "Error starting zookeeper: {}",
            res.err().unwrap()
        );
        // This only verifies that the zookeeper container is running, it
        // does not verify connectivity or that zookeeper is running
        // successfully in the container.
        assert!(container_running(&name).await);

        // Kill and remove the zookeeper container,
        let res = kill_zookeeper(container_prefix).await;
        assert!(
            !res.is_err(),
            "Error killing zookeeper: {}",
            res.err().unwrap()
        );
        assert!(!container_exists(&name).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn mysql_operations() {
        let container_prefix = "start_mysql_test";
        // Kill the container first to prevent conflicts with previous
        // test runs that may not have cleaned up the container.
        let _ = kill_mysql(container_prefix).await;

        // Create a zookeeper container, verify that it is running.
        let name = prefix_to_mysql_container(container_prefix);
        let res = start_mysql(container_prefix, 3308).await;
        assert!(
            !res.is_err(),
            "Error starting mysql: {}",
            res.err().unwrap()
        );
        // This only verifies that the zookeeper container is running, it
        // does not verify connectivity or that zookeeper is running
        // successfully in the container.
        assert!(container_running(&name).await);

        // Kill and remove the zookeeper container,
        let res = kill_mysql(container_prefix).await;
        assert!(!res.is_err(), "Error killing mysql: {}", res.err().unwrap());
        assert!(!container_exists(&name).await);
    }
}
