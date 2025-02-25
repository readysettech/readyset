//! Simple helper to easily propagate write-once configuration flags or parameters around the
//! system

use std::sync::OnceLock;

static FLAGS: OnceLock<Flags> = OnceLock::new();

/// Readyset runtime configuration flags
///
/// If something is a flag here, it should be flexible and changeable on a run-to-run basis.
/// Setting a value once should not require that it be set the same way every time forever.
///
/// # Example
///
/// ```
/// use readyset_util::flags::{self, Flags};
///
/// let mut flags = Flags::default();
/// flags.set_reuse(true);
/// flags.commit();
/// assert!(flags::get().reuse());
/// ```
#[derive(Debug, Default)]
pub struct Flags {
    reuse: bool,
}

macro_rules! impl_bool_flag {
    ($name:ident) => {
        paste::paste! {
            #[allow(missing_docs)]
            pub fn [<set_ $name>](&mut self, val: bool) {
                self.$name = val;
            }

        }

        // outside of paste! for better autocomplete
        #[allow(missing_docs)]
        pub fn $name(&self) -> bool {
            self.$name
        }
    };
}

impl Flags {
    fn commit_impl(self, flags: &OnceLock<Flags>) {
        flags
            .set(self)
            .expect("global flags are already initialized!");
    }

    /// Install the current flags as the global flags.
    ///
    /// This can only be called once per process.  Once installed, the flags are immutable for
    /// the lifetime of the process.
    pub fn commit(self) {
        self.commit_impl(&FLAGS);
    }

    impl_bool_flag!(reuse);
}

fn get_impl(flags: &OnceLock<Flags>) -> &Flags {
    flags.get().expect("global flags aren't initialized yet!")
}

/// Get the global flags.
pub fn get() -> &'static Flags {
    get_impl(&FLAGS)
}

#[cfg(test)]
mod test {
    // We can't test the top-level get/commit functions due to their use of static state.
    use super::*;

    #[test]
    #[should_panic]
    fn not_init() {
        let flags = OnceLock::new();
        get_impl(&flags);
    }

    #[test]
    #[should_panic]
    fn init_twice() {
        let flags = OnceLock::new();
        Flags::default().commit_impl(&flags);
        Flags::default().commit_impl(&flags);
    }
}
