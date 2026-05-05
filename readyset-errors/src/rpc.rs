//! Helpers for converting `readyset-multiplex` errors into [`ReadySetError`].

use crate::{ReadySetError, internal_err};

/// Make a new [`ReadySetError::RpcFailed`] with the provided string-able `during` value
/// and the provided `err` as cause.
///
/// This attempts to downcast `err` into a `Box<ReadySetError>`. If that fails, the error is
/// formatted as a [`ReadySetError::Internal`].
pub fn rpc_err<T, E>(during: T, err: E) -> ReadySetError
where
    T: Into<String>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let err: Box<dyn std::error::Error + Send + Sync> = err.into();
    let source = if let Some(err) = err.downcast_ref::<readyset_multiplex::Error>() {
        Box::new(err.into())
    } else {
        err.downcast::<ReadySetError>()
            .unwrap_or_else(|e| Box::new(internal_err!("{e}")))
    };
    ReadySetError::RpcFailed {
        during: during.into(),
        source,
    }
}

impl From<&readyset_multiplex::Error> for ReadySetError {
    fn from(err: &readyset_multiplex::Error) -> Self {
        match err {
            readyset_multiplex::Error::TransportFull => ReadySetError::TransportFull,
            readyset_multiplex::Error::ClientDropped => ReadySetError::ClientDropped,
            readyset_multiplex::Error::Desynchronized => ReadySetError::Desynchronized,
            readyset_multiplex::Error::BrokenTransportSend(e) => {
                ReadySetError::TransportSendFailed(e.to_string())
            }
            readyset_multiplex::Error::BrokenTransportRecv(_) => ReadySetError::TransportRecvFailed,
        }
    }
}

/// Generates a closure, suitable as an argument to `.map_err()`, that maps the provided error
/// argument into a [`ReadySetError::RpcFailed`] with the given `during` argument (anything
/// that implements `Display`).
///
/// When building in debug mode, the `during` argument generated also captures file, line, and
/// column information for further debugging purposes.
///
/// # Example
///
/// ```ignore
/// let rpc_result = do_rpc_call()
///     .map_err(rpc_err!("do_rpc_call"));
/// ```
#[macro_export]
macro_rules! rpc_err {
    ($during:expr_2021) => {
        |e| $crate::rpc::rpc_err(format!("{}{}", $during, $crate::__location_info!()), e)
    };
}
