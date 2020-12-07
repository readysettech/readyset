//! The types in this module _cannot_ be public, and you must _never_ implement a trait for one but
//! not the other.
//!
//! See the module-level documentation on [`left_right::aliasing`] for details.

use left_right::aliasing::DropBehavior;

/// If you can see this type outside of `evmap`, that is a severe unsoundness bug.
/// Please report it at https://github.com/jonhoo/rust-evmap/issues.
#[allow(missing_debug_implementations)]
// NOTE: This type is pub so that we can expose it in a couple of trait bounds of the form:
//
//   Aliased<T, NoDrop>: Trait
//
// However, it remains private because this module is private.
pub struct NoDrop;

pub(crate) struct DoDrop;

impl DropBehavior for NoDrop {
    fn do_drop() -> bool {
        false
    }
}
impl DropBehavior for DoDrop {
    fn do_drop() -> bool {
        true
    }
}
