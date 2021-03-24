//! This crate provides miscellanious utilities and extensions to the Rust standard library, for use
//! in all crates in this workspace.
#![deny(missing_docs, missing_crate_level_docs)]
#![feature(or_patterns)]

pub mod arbitrary;
pub mod hash;
pub mod intervals;

/// If `a` is `Some`, wrap it in `Ok`. If `a` is `None`, run `b` and return it.
pub fn or_else_result<T, E, F>(a: Option<T>, b: F) -> Result<Option<T>, E>
where
    F: FnOnce() -> Result<Option<T>, E>,
{
    match a {
        Some(a) => Ok(Some(a)),
        None => b(),
    }
}
