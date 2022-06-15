pub use failpoint_proc_macros::failpoint;

/// Creates a [`fail::fail_point`] which can be used to programatically
/// introduce failures from tests. This macro wraps [`fail::fail_point`]
/// with our `failure_injection` feature.
///
/// Example: Creating a simple failpoint named `rpc-failed`. This failpoint
/// can not return values using the `return` action.
/// ```rust
/// use failpoint_macros::set_failpoint;
/// set_failpoint!("rpc-failed");
/// ```
///
/// Example: Creating a failpoint that may return a ReadySet error when
/// passed the `return` action.
/// ```rust
/// use failpoint_macros::set_failpoint;
/// set_failpoint!("rpc-failed", |_| ReadySetError::Internal("failpoint"));
/// ```
///
/// Example: Creating a failpoint that can return values and be conditioned
/// on the failpoint enabling + a boolean passed in as an argument.
/// ```rust
/// use failpoint_macros::set_failpoint;
/// let is_leader_rpc = false;
/// set_failpoint!("rpc-failed", is_leader_rpc, |_| ReadySetError::Internal(
///     "thrown from failpoint"
/// ));
/// ```
#[macro_export]
macro_rules! set_failpoint {
    ($name:expr) => {{
        #[cfg(feature = "failure_injection")]
        fail::fail_point!($name);
    }};
    ($name:expr, $e:expr) => {{
        #[cfg(feature = "failure_injection")]
        fail::fail_point!($name, $e);
    }};
    ($name:expr, $cond:expr, $e:expr) => {{
        #[cfg(feature = "failure_injection")]
        fail::fail_point!($name, $cond, $e);
    }};
}
