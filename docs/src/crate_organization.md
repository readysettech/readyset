# Cargo.toml

The manifest file, `Cargo.toml`, describes how to build crates within a
package. Cargo.toml files describe binaries, libraries, package dependencies,
compile-time feature flags, and more. See 
[Cargo.toml](https://doc.rust-lang.org/cargo/reference/manifest.html)
for more information on the manifest format. 

## Adding dependencies to an existing package.
Rust has a growing ecosystem of libraries we can use [crates.io](https://crates.io/).
When adding a new dependency it is always worth considering:
  * Do we already import a dependency that can perform the job?
  * Do we import this dependency already in other crates? If so, prefer to keep the version consistent
    with the existing dependencies.
  * Is this crate likely to be broken? The number of dependant crates, downloads, and
    if any big-name crates use it as a dependency may be useful signals.
  * Is this crate still maintained? Unmaintained crates may cause headaches if we have
    to make changes or update dependencies.
  * Does it have a license we are allowed to use?
  * Does it have any security vulnurabilities.

> The latter two are checked in CI via `cargo deny`. See `//deny.toml` for more
> information on what checks are performed by `cargo deny`.

## Removing dependencies
If a dependency can be removed because it is unused or can be replaced by another
crate, create a CL. Always feel free to put CLs that improve code health up for
review.

## Adding a new crate
When new functionality or features make more sense as a separate crate, make sure
you ensure a few things:
  * Add the crate to the workspace root `Cargo.toml`. 
  * Setup the Cargo.toml file with naming, versioning, and rust edition set to `2018`.

```toml
[package]
name = "readyset-logging"
version = "0.1.0"
publish = false
edition = "2018"
```

If you do not intend to release the crate publicly, set `publish = false` in the
Cargo.toml so that `cargo-deny` is aware that this is an internal crate.

<!-- TODO: Global denys to set up, etc. -->

## Notes about crates

**Changes in dependant crates require recompilation of your crate**.
For example, `noria/server` is dependant on `noria/noria`. As a result any change
to the `noria/noria` code will cause `noria/server` to be recompiled next time it,
or any crate that depends on it, is used. As a result, it is important to be aware
of crate dependencies you are introducing, some crates take an unfortunately long
time to compile, and should be added as dependencies with caution. Some examples:
  * Any crate that depends on librocksdb.
  * noria/server


## Cargo.lock 
The [`Cargo.lock`](https://doc.rust-lang.org/cargo/guide/cargo-toml-vs-cargo-lock.html) 
file describes the state of the world at the time of a successful
build: the exact dependencies and versions used at the time when it was
generated. This allows us to deterministically build and test ReadySet
with reproducible dependencies and versions.

> We use the `--locked` when performing building, linting, and testing
> in our CI pipeline. If the Cargo.lock file is not up to date, the CI
> pipeline will fail the build. As such, always commit `Cargo.lock` when
> making changes to dependencies.

When performing a build, `cargo` attempts to use the latest compatible
version of a package according to 
[`SemVer` compatibility](https://doc.rust-lang.org/cargo/reference/resolver.html#semver-compatibility).

## Updating git dependencies with `cargo update`

Git dependencies in `Cargo.toml` are locked to the version pulled when
the dependency was added. Bumping the version of the git dependency can
be performed with `cargo update`.

```
cargo update -p [dependency]
```

## Working with dependencies treewide via [`cargo-edit`](https://crates.io/crates/cargo-edit)

`cargo-edit` enables you to modify `Cargo.toml` files from the
command-line.  It can be used to add (`cargo add`), remove (`cargo rm`),
bump the Cargo.toml version (`cargo set-version`), and upgrade
dependencies (`cargo update`).

```
# Upgrade the dependency in the Cargo.toml file.
cargo upgrade [FLAGS] [OPTIONS] [dependency]...
```

> The `--workspace` flag can be used to apply an upgrade across the
> entire workspace.

See [`cargo-edit`](https://crates.io/crates/cargo-edit) for other
commands, options, and flags.



