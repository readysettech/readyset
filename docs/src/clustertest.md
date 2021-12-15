# Clustertest

A local multi-process deployment test framework for ReadySet. It enables 
local blackbox testing on multi-server deployments with support for 
programatically modifying the deployment, i.e. introducing faults, adding new
servers, replicating readers.

## When to use clustertest
The clustertest framework well suited for:
 - Testing fault tolerance and failure recovery behavior. 
 - Testing behavior that is localized to specific workers. Each worker in a deployment can be queried for metrics separately via a metrics client.

It is not well suited for:
 - Evaluating regular query behavior in the absence of failures.
 - Evaluating behavior that can be tested locally on a single server.

## How to use clustertest
See rustdocs for the `noria/clustertest` crate.

## Resources 
Design: [Google Doc](https://docs.google.com/document/d/1goz9jJQSc8vDZsuPrQDuNq_B9CxuKhGl6ESoliPH7wY/edit?usp=sharing)


