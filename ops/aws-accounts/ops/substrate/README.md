# aws-accounts (to be renamed to substrate)
Substrate and Terraform configuration for ReadySet

## Introduction
ReadySet AWS accounts are managed in an
[AWS Organization](https://aws.amazon.com/organizations/) using
[Substrate](https://src-bin.co/substrate/manual/) which generates
[Terraform](https://terraform.io). In addition, we are using
[SOPS](https://github.com/mozilla/sops) for secrets management.

## Deployment

We are moving towards using Buildkite to deploy everything in here using a
runner written in Rust using the
[`cargo xtask`](https://github.com/matklad/cargo-xtask/) free form automation
system.
