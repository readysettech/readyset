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

#TODO: Global denys to set up, etc.

## Notes about crates

**Changes in dependant crates require recompilation of your crate**.
For example, `noria/server` is dependant on `noria/noria`. As a result any change
to the `noria/noria` code will cause `noria/server` to be recompiled next time it,
or any crate that depends on it, is used. As a result, it is important to be aware
of crate dependencies you are introducing, some crates take an unfortunately long
time to compile, and should be added as dependencies with caution. Some examples:
  * Any crate that depends on librocksdb.
  * noria/server
