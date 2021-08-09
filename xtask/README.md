# Readyset xtasks

[xtask][1] is a polyfill for an eventual `cargo` workflows feature. This allows
us to write arbitrary tasks using Rust and use `cargo` to execute them using
standard and stable features. This is similar to `npm run` or `rake`.

This crate contains just a single binary. The only other part of this is an
alias setup in `.cargo/config` which allows you to run `cargo xtask <command>`.

[1]: https://github.com/matklad/cargo-xtask/

## Currently defined xtasks

### `install-commit-msg-hook`

As an example, running this command will download the `commit-msg` hook from
Gerrit and install it in the working directory.
