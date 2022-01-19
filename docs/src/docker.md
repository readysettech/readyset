# Docker Configuration

## Docker Desktop

For those using Docker Desktop, e.g. macOS users, our Docker builds
involve use large amounts of RAM. Since Docker Desktop uses a virtual machine to
run Docker on it only, by default, acquires 2 GB of RAM to run Docker with. You
should increase this to avoid build and runtime failures. You can
find [instructions](https://docs.docker.com/desktop/mac/#resources) on the
Docker documentation website.

## Amazon ECR

Due to rate limiting we have migrated all of the Dockerfiles and Docker Compose
files to reference images from our private AWS Elastic Container Registry (ECR).

ECR uses Amazon's IAM system for authentication which is not compatible with how
Docker does configuration by default. Docker does have support for plugging in
different authentication backends for different purposes.

### Configuration for ReadySet

1. Install `[amazon-ecr-credential-helper][0]` using the instructions for your
operating system.

1. Add to `~/.docker/config.json` the following configuration:

  ```json
  {
    "credHelpers": {
      "305232526136.dkr.ecr.us-east-2.amazonaws.com": "ecr-login",
      "public.ecr.aws": "ecr-login"
    }
  }
  ```

[0]: https://github.com/awslabs/amazon-ecr-credential-helper

### Automated Configuration for ReadySet (BETA)

There is an `cargo xtask` which attempts to do the install procedure as
documented above. If you have configured the monorepo and got Rust working
`cargo xtask install-docker-credential-ecr-login` should do the above steps.
on Linux.
