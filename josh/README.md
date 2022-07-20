# JOSH

[JOSH](https://github.com/josh-project/josh) is a tool for subdividing git repositories while maintaining a cohesive
git commit history. At Readyset we use JOSH as a way split our codebase between an internal version and an open source
version that we call [Readyset Core](https://github.com/readysettech/readyset) which is hosted on Github.
This is accomplished by taking the internal version of the codebase and using JOSH to filter
out whatever we don't want to include in Readyset Core.

JOSH can be used in two modes; as a command line tool and as a Git proxy. In either mode JOSH relies on
[filters](https://josh-project.github.io/josh/reference/filters.html)--statements that define how a repository should be
filtered, remapped, etc. At the moment we use JOSH in CLI mode and have plans to use it as a proxy in the future.

## Command Line

As a command line tool JOSH takes a local repository and applies filters to it and stores the results in a ref
called `FILTERED_HEAD`. JOSH can be somewhat cumbersome to use in this mode and as such there's a provided wrapper
script
called `joshua.sh` that provides an opinionated subset of JOSH functionality while eliminating common "gotchas" with
using JOSH.

### Install JOSH

JOSH as a CLI tool is intended to be built and installed with `cargo`.

```shell
git clone https://github.com/josh-project/josh.git
cd josh
cargo install --path ./
# Check that josh is installed and working
josh-filter --help
```

### Wrapper Script Usage

The wrapper script is designed to do three things:

- Apply a JOSH filter to a local Git repository and create a filtered repository.
- Display the Git diffs between the filtered repository and a remote repository.
- Push the filtered repository to a remote branch.

As applied to the Readyset codebase the intended workflow is:

- Generate the Readyset Core repo from the internal version of the Readyset codebase.
- Check Git diffs between the Readyset Core repo locally and what's hosted on Github.
- Once diffs have been checked and approval has been granted Readyset Core code is pushed to Github.
- Changes from Github are merged back into the internal codebase, if necessary.

To accomplish these goals the CLI wrapper script accepts the following command line arguments:

```text
  Usage:
    | Flag | Long Name | Description                   | Example          |
    |---------------------------------------------------------------------|
    | -f   | filter    | JOSH filter statement         | :/tests          |
    | -r   | ref       | git refs to push to           | refs/heads/main  |
    | -m   | remote    | git remote branch to push to  | origin           |
    | -p   | push      | push changes to remote branch | true             |
```

Note that the `-p` flag if not set to "true" will display Git diffs.

### Wrapper Script  Examples

Use Case:

Filter out only the `/tests` directory from a local repo then generate Git diffs between what's stored in
`FILTERED_HEAD` and the repository located in the remote `tests` using the refs `refs/heads/main`.

```shell
./joshua.sh -f ":/tests" -r "refs/heads/main" -m "tests" -p "false"
```

Use Case:

Filter out only the `/tests` directory from a local repo then push to a repository located in the remote `tests`
using the refs `refs/heads/main`.

```shell
./joshua.sh -f ":/tests" -r "refs/heads/main" -m "tests" -p "true"
```

## Proxy

JOSH can act as a Git proxy in so far as it can be run as an idempotent service and intercept Git commands.
When acting in this way JOSH acts as a read/write capable shim over Git;
that is you can interact with a JOSH generated filtered repository as if it were any other repository.

Note that JOSH is
[not intended to be used with SSH](https://josh-project.github.io/josh/reference/proxy.html?highlight=ssh#josh-proxy).

### Example Usage

Use Case:

Use JOSH as a proxy to the HTTP API for the Readyset Gerrit instance.

```shell
git clone http://<proxy-host>/a/readyset.git<josh-filters>.git
```

Note that the cloned repository will be a filtered version of the `readyset` repository filtered by `<josh-filters>`.

## Future Work

While JOSH is suitable for generating the Readyset Core codebase as a CLI tool it would be more flexible
and easier to maintain as a proxy. As such we plan to switch to deploying both Gerrit and JOSH as internal Kubernetes
services in the near future.

There's a provided `docker-compose.yml` file that serves as a reference for future Kubernetes development work
which is reproduced here for convenience.

```yaml
version: '3.6'

services:

  josh:
    image: joshproject/josh-proxy:r22.06.22
    ports:
      - "80:8000"
    volumes:
      - "josh-vol:/data/git"
    environment:
      JOSH_REMOTE: "https://gerrit.readyset.name"

volumes:
  josh-vol:
```