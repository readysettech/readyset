use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream, TryStream};

use crate::{internal_err, ReadySetError};

/// A type that implements [`Sink`] and [`TryStream`] for use as a placeholder associated type for
/// [`tokio_tower::Error`]'s type parameters when we aren't trying to downcast to a
/// [`tokio_tower::Error`] during calls to `rpc_err`
pub struct NullSink;

impl<I> Sink<I> for NullSink {
    type Error = !;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _item: I) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

impl Stream for NullSink {
    type Item = Result<(), !>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

/// Make a new [`ReadySetError::RpcFailed`] with the provided string-able `during` value
/// and the provided `err` as cause.
///
/// This attempts to downcast the `err` into a `Box<ReadySetError>`. If the downcasting
/// fails, the error is formatted as a [`ReadySetError::Internal`].
pub fn rpc_err<T, S, I>(during: T, err: Box<dyn std::error::Error>) -> ReadySetError
where
    T: Into<String>,
    S: Sink<I> + TryStream + 'static,
    <S as Sink<I>>::Error: std::error::Error,
    <S as TryStream>::Error: std::error::Error,
    I: 'static,
{
    let source = if let Some(err) = err.downcast_ref::<tokio_tower::Error<S, I>>() {
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

impl<S, I> From<&tokio_tower::Error<S, I>> for ReadySetError
where
    S: Sink<I> + TryStream + 'static,
    <S as Sink<I>>::Error: std::error::Error,
    <S as TryStream>::Error: std::error::Error,
    I: 'static,
{
    fn from(err: &tokio_tower::Error<S, I>) -> Self {
        match err {
            tokio_tower::Error::TransportFull => ReadySetError::TransportFull,
            tokio_tower::Error::ClientDropped => ReadySetError::ClientDropped,
            tokio_tower::Error::Desynchronized => ReadySetError::Desynchronized,
            tokio_tower::Error::BrokenTransportSend(e) => {
                ReadySetError::TransportSendFailed(e.to_string())
            }
            tokio_tower::Error::BrokenTransportRecv(_) => ReadySetError::TransportRecvFailed,
        }
    }
}

/// Generates a closure, suitable as an argument to `.map_err()`, that maps the provided error
/// argument into a [`ReadySetError::RpcFailed`] with the given `during` argument (anything
/// that implements `Display`).
///
/// Optionally, the transport and request type parameters for [`tokio_tower::Error`] can be given,
/// to attempt downcasting to that error type for more detailed error values.
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
    ($during:expr) => {
        rpc_err!($during, $crate::rpc::NullSink, ())
    };
    ($during:expr, $s: ty, $i: ty $(,)?) => {
        |e| {
            $crate::rpc::rpc_err::<_, $s, $i>(
                format!("{}{}", $during, $crate::__location_info!()),
                e,
            )
        }
    };
}
