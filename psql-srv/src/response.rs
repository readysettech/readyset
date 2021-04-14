use crate::codec::EncodeError;
use crate::error::Error;
use crate::message::{BackendMessage, CommandCompleteTag, TransferFormat};
use crate::value::Value;
use futures::prelude::*;
use std::convert::TryInto;
use std::sync::Arc;

/// An encapsulation of a complete response produced by a Postgresql backend in response to a
/// request. The response will be sent to the frontend as a sequence of zero or more
/// `BackendMessage`s.
pub enum Response<R, S> {
    Empty,
    Message(BackendMessage<R>),
    Message2(BackendMessage<R>, BackendMessage<R>),

    /// `Select` is the most complex variant, containing data rows to be sent to the frontend in
    /// response to a select query.
    Select {
        header: Option<BackendMessage<R>>,
        resultset: S,
        result_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
        trailer: Option<BackendMessage<R>>,
    },
}

impl<R, S> Response<R, S>
where
    R: IntoIterator<Item: TryInto<Value, Error = Error>>,
    S: IntoIterator<Item = R>,
{
    pub async fn write<K>(self, sink: &mut K) -> Result<(), EncodeError>
    where
        K: Sink<BackendMessage<R>, Error = EncodeError> + Unpin,
    {
        use Response::*;
        match self {
            Empty => Ok(()),

            Message(m) => sink.send(m).await,

            Message2(m1, m2) => {
                sink.feed(m1).await?;
                sink.send(m2).await
            }

            Select {
                header,
                resultset,
                result_transfer_formats,
                trailer,
            } => {
                if let Some(header) = header {
                    sink.feed(header).await?;
                }

                let mut n_rows = 0;
                for r in resultset {
                    sink.feed(BackendMessage::DataRow {
                        values: r,
                        explicit_transfer_formats: result_transfer_formats.clone(),
                    })
                    .await?;
                    n_rows += 1;
                }

                sink.feed(BackendMessage::CommandComplete {
                    tag: CommandCompleteTag::Select(n_rows),
                })
                .await?;

                if let Some(trailer) = trailer {
                    sink.feed(trailer).await?;
                }

                sink.flush().await
            }
        }
    }
}
