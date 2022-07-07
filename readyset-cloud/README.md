# readyset-cloud

## Getting Started

Run `pnpm install` and everything will be configured and set up correctly
including fetching CDK providers. This will need to be run before any of
the other commands.

The primary thing which needs to be run will be the CDK synthesis. This can be
done with `pnpm -C apps/control-plane cdktf:synth`. The output will be in
`apps/control-plane/cdktf.out`

Run `pnpm run typecheck` to verify that all the packages continue to typecheck.

Run `pnpm run format` to format all the files with Prettier.

Run `pnpm run lint` to verify that all the configured linting rules pass.

Run `pnpm run test` to run all the Jest tests.

## Design Summary

This overall system is designed as a series of translation steps to go from
what a customer desires in terms of clusters to cloud provider and Kubernetes
API calls. We are currently using Terraform through the CDK project to handle
make those API calls.

The `data-plane` package exposes a repository and types around the information
the customer provides. The repository is an object that can be pointed at a
data store and has functions that returns different queries upon that data
store.

The `control-plane` package iterates over the various queries from the
repository and uses that to create Terraform definitions for the customer
requirements.

This design is very much inspired by the design of compilers. Compilers are
generally designed as a series of compiler passes which are just functions that
take an input in a data structure and produce a new data structure as the output
and each pass performs just transform of the input. Currently, the two
transforms are located as outputs of queries from the data plane and transforms
from those outputs to the paramaters to pass to the various Terraform
constructs.

## File Structure

`packages/\*`: Contains various TypeScript packages that can be included in
multiple applications or as dependencies for other packages.

`packages/data-plane`: Interface to the database of what we want to configure
for our customers.

`apps/\*`: Contains various TypeScript applications from Terraform CDK to the
Console and other tools.

`apps/control-plane`: Terraform CDK code using the data-plane as direction.
