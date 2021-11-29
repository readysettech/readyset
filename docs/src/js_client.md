# Node.js Client Library

## Overview

The Node.js Client Library is a native ReadySet client library designed for use in Node.js applications.

The library is packaged as a native Node.js package and exposes a `Client` class.

## Motivation

1. Having a native Node.js client library is _cool_!
2. Avoids the need to rely on third-party clients.
3. Avoids bottleneck caused by MySQL adapter.

   A bit more on that third point: all MySQL clients that run through the MySQL adapter need to connect to the same adapter that pretends to be a MySQL server. This is a bottlneck that native Node.js clients avoid, because each Node.js client has a direct connection to Noria itself.

## Design

### Neon

[Neon](https://docs.rs/neon/0.7.1-napi/neon/) is a crate that allows Rust functions to be easily called from JavaScript as part of a native node package. [Neon](https://docs.rs/neon/0.7.1-napi/neon/) also allows JavaScript to access smart pointers to Rust structs (which are cleaned-up by the JS garbage collector!!).

### A Tale of Two Wrappers

The Node.js Client Library consists of two wrappers.

#### Rust Wrapper

The [first wrapper](../../js-client/src/lib.rs) is a Rust wrapper around the [noria-client](./noria_client.md) that provides the ability to call, from javascript, the [noria-client](./noria_client.md)'s underlying Rust functions that carry out queries. This wrapper uses Neon to expose 4 functions:

- a function to create a new `Backend` struct in rust and return a JavaScript-managed pointer to this struct
- 3 asynchronous functions that allow `Backend`'s core 3 functions (`prepare`, `execute`, and `query`) to be called from JavaScript. These functions spaw their own threads in Rust to process queries and then trigger a callback when they are done to deliver their results to Node.js and tell Node.js that the promise can be resolved.

This API is compiled into a [native c++ addon](https://nodejs.org/api/addons.html) with the help of Neon. Basically, this is a shared object file that can be imported like a normal node package.

### JavaScript Wrapper

The [second wrapper](../../js-client/lib/index.js) is a JavaScript class that imports the rust functions that were created with Neon. This wrapper exposes a `Client` class whose API is specifically designed to mirror the popular [MySQL 2](https://www.npmjs.com/package/mysql2) Node.js client library. The functions that allow users to query are `async`.

## To Compile the Rust Library Into the Addon:

Run `yarn run build`.

This will compile the Rust code and also rename and move the shared object file to `index.node` in the top level of the js-client crate.

## To Use the Node.js Client Library Locally:

See [this example](../../js-client/examples/basic_demo.js).

## Test Suite

An end-to-end test suite is located in the [tests directory](../../js-client/tests). The tests use the `jest` test framework. The tests run automatically in CI.

### To Run the Test Suite Locally:

- Ensure docker-compose is [installed](https://docs.docker.com/compose/install/)
- Pull the [`noria-server`](build/Dockerfile.noria-server) docker image from our aws ECR registry. The image to pull is: `069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-server:latest`
  - This can be done easily from the [aws CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv1.html) (after installing run `aws configure`, ensure you have [credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html), and set `Default region name` to `us-east-2` and `Default output format` to `json`). Then you can run `aws ecr get-login-password --region us-east-2 | sudo docker login --username AWS --password-stdin 069491470376.dkr.ecr.us-east-2.amazonaws.com` to authenticate with the ECR registry.
- `cd` into the [tests/build directory](../../js-client/tests/build)
- run `docker-compose run node` to run the test suite
- run `docker-compose down` to clean up
