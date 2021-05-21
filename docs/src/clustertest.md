# Clustertest

The clustertest framework provides a programatic way to locally build and
test a full noria deployment. It handles building ReadySet binaries,
creating cluster management resources and starting ReadySet workers
in separate processes on the local machine. 

Design: [Google Doc](https://docs.google.com/document/d/1goz9jJQSc8vDZsuPrQDuNq_B9CxuKhGl6ESoliPH7wY/edit?usp=sharing)

## When to use clustertest
**Clustertest**, tries to be as close to a real-world deployment as possible by deploying
each noria-server as a separate process and using a zookeeper node to perform
cluster management. This allows us to:
- Test that a specific behavior occurs on specific workers.
- Evaluate real-world system behavior such as failures, coordination via zookeeper, etc.

However, it would not be a good test infrastructure for evaluating behavior
that would change with inter-server communication.

## How to use clustertest

### Defining a deployment
A deployment is defined by a `DeploymentParams` object ([clustertest/src/lib.rs](https://github.com/readysettech/readyset/blob/master/noria/clustertest/src/lib.rs)).

Here's a snippet of what the `DeploymentParams` object looks like:
```rust
struct DeploymentParams {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,
    /// Source of the noria-server binary.
    noria_server_source: NoriaServerSource,
    /// Number of shards for dataflow nodes.
    sharding: Option<usize>,
    /// The primary region of the noria cluster.
    primary_region: Option<String>,
    /// Parameters for the set of noria-server instances in the deployment.
    servers: Vec<ServerParams>,

}
```

This object defines parameters for the test deployment. Each server that is meant
to be added to the system should have its own `ServerParams` object added to `servers`.
The `NoriaServerSource` object, determines where the clustertest will source the
`noria-server` binary from. This can be built from source, or given a path to
a pre-built binary.


### Starting and interacting with a deployment.
```rust
sync fn start_multi_process(params: DeploymentParams) 
	-> anyhow::Result<DeploymentHandle>
```

Starting the deployment requires calling `start_multi_process` with a `DeploymentParams` object, 
which returns a handle to the created deployment. This handle lets you interact with the controller 
through a `ControllerHandle` and request metrics via a `MetricsClient`.

Example of requesting a metrics dump:
```rust
let metrics = deployment.metrics.get_metrics().await.unwrap();
```

Example of calling a controller rpc:
```rust
let workers = deployment.handle.workers().await.unwrap();
```

The `DeploymentHandle` object also has cluster management functions that can be used
to start new servers, and kill existing servers. See `DeploymentHandle`'s impl block for
more details.

### Clean-up
```rust
let res = deployment.teardown().await;
```

Cleaning up resources must be done at the end of each test. Resources may not
be cleaned up when tests panic (there is a Drop implementation that occasionally
chooses not to work). In this case, noria-server processes should be manually
killed.
