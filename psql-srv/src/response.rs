use std::sync::Arc;

use futures::prelude::*;
use smallvec::SmallVec;

use crate::codec::EncodeError;
use crate::error::Error;
use crate::message::{BackendMessage, CommandCompleteTag, TransferFormat};
use crate::value::PsqlValue;

/// An encapsulation of a complete response produced by a Postgresql backend in response to a
/// request. The response will be sent to the frontend as a sequence of zero or more
/// `BackendMessage`s.
#[derive(Debug, PartialEq)]
#[warn(variant_size_differences)]
pub enum Response<S> {
    Empty,
    Message(BackendMessage),
    /// Send multiple messages at once
    Messages(SmallVec<[BackendMessage; 2]>),

    /// `Select` is the most complex variant, containing data rows to be sent to the frontend in
    /// response to a select query.
    Select {
        header: Option<BackendMessage>,
        resultset: S,
        result_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
        trailer: Option<BackendMessage>,
    },
}

impl<S> Response<S>
where
    S: Stream<Item = Result<Vec<PsqlValue>, Error>> + Unpin,
{
    pub async fn write<K>(self, sink: &mut K) -> Result<(), EncodeError>
    where
        K: Sink<BackendMessage, Error = EncodeError> + Unpin,
    {
        use Response::*;
        match self {
            Empty => Ok(()),

            Message(m) => sink.feed(m).await,

            Messages(ms) => {
                for m in ms {
                    sink.feed(m).await?
                }
                Ok(())
            }

            Select {
                header,
                mut resultset,
                result_transfer_formats,
                trailer,
            } => {
                if let Some(header) = header {
                    sink.feed(header).await?;
                }

                let mut n_rows = 0;
                while let Some(r) = resultset.next().await {
                    match r {
                        Ok(row) => {
                            sink.feed(BackendMessage::DataRow {
                                values: row,
                                explicit_transfer_formats: result_transfer_formats.clone(),
                            })
                            .await?;
                            n_rows += 1;
                        }
                        Err(e) => {
                            sink.feed(e.into()).await?;
                        }
                    }
                }

                sink.feed(BackendMessage::CommandComplete {
                    tag: CommandCompleteTag::Select(n_rows),
                })
                .await?;

                if let Some(trailer) = trailer {
                    sink.feed(trailer).await?;
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use smallvec::smallvec;
    use tokio_test::block_on;

    use super::*;
    use crate::value::PsqlValue;

    type TestResponse = Response<stream::Iter<vec::IntoIter<Result<Vec<PsqlValue>, Error>>>>;

    #[test]
    fn write_empty() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |_i, _m: BackendMessage| {
            async move {
                // No messages are expected.
                panic!();
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_message() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert_eq!(m, BackendMessage::BindComplete),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_message2() {
        let response = TestResponse::Messages(smallvec![
            BackendMessage::BindComplete,
            BackendMessage::CloseComplete,
        ]);
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert_eq!(m, BackendMessage::BindComplete),
                    1 => assert_eq!(m, BackendMessage::CloseComplete),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_select_simple_empty() {
        let response = TestResponse::Select {
            header: None,
            resultset: stream::iter(vec![]),
            result_transfer_formats: None,
            trailer: None,
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert_eq!(
                        m,
                        BackendMessage::CommandComplete {
                            tag: CommandCompleteTag::Select(0)
                        }
                    ),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_select() {
        let response = Response::Select {
            header: Some(BackendMessage::RowDescription {
                field_descriptions: vec![],
            }),
            resultset: stream::iter(vec![
                Ok(vec![PsqlValue::Int(5), PsqlValue::Double(0.123)]),
                Ok(vec![PsqlValue::Int(99), PsqlValue::Double(0.456)]),
            ]),
            result_transfer_formats: Some(Arc::new(vec![
                TransferFormat::Text,
                TransferFormat::Binary,
            ])),
            trailer: Some(BackendMessage::ready_for_query_idle()),
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert_eq!(
                        m,
                        BackendMessage::RowDescription {
                            field_descriptions: vec![]
                        }
                    ),
                    1 => assert_eq!(
                        m,
                        BackendMessage::DataRow {
                            values: vec![PsqlValue::Int(5), PsqlValue::Double(0.123)],
                            explicit_transfer_formats: Some(Arc::new(vec![
                                TransferFormat::Text,
                                TransferFormat::Binary
                            ]))
                        }
                    ),
                    2 => assert_eq!(
                        m,
                        BackendMessage::DataRow {
                            values: vec![PsqlValue::Int(99), PsqlValue::Double(0.456)],
                            explicit_transfer_formats: Some(Arc::new(vec![
                                TransferFormat::Text,
                                TransferFormat::Binary
                            ]))
                        }
                    ),
                    3 => assert_eq!(
                        m,
                        BackendMessage::CommandComplete {
                            tag: CommandCompleteTag::Select(2)
                        }
                    ),
                    4 => assert_eq!(m, BackendMessage::ready_for_query_idle()),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }
}
