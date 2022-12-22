# Launchpad

This crate provides miscellanious utilities and extensions to the Rust standard
library, for use in all crates in this workspace.

## What belongs here?

Code in this crate should remain *generally* useful to *all* crates that use the
rust standard library, and should never depend on other crates in this
workspace. The idea is that this crate should always remain at the root of the
dependency chain of all crates in this workspace.
